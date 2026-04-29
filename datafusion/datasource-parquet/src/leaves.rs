use arrow::datatypes::{DataType, Field, SchemaRef};
use datafusion_common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion_common::{ScalarValue, internal_err};
use datafusion_physical_expr::ScalarFunctionExpr;
use datafusion_physical_expr::expressions::{CastExpr, Column, Literal};
use datafusion_physical_expr::projection::ProjectionExprs;
use datafusion_physical_expr_common::physical_expr::PhysicalExpr;
use log::error;
use parquet::schema::types::SchemaDescriptor;
use std::cmp::min;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;

/// Combines the current projection (numeric indices of top-level columns) with
/// the deep projection - "paths" inside a top-level column
pub fn splat_columns(
    src: &SchemaRef,
    projection: &[usize],
    projection_deep: &HashMap<usize, Vec<String>>,
) -> Vec<String> {
    projection
        .iter()
        .map(|pi| {
            let f = src.field(*pi);
            match projection_deep.get(pi) {
                None => {
                    vec![f.name().to_owned()]
                }
                Some(rests) => {
                    if !rests.is_empty() {
                        rests
                            .iter()
                            .map(|rest| format!("{}.{}", f.name(), rest))
                            .collect::<Vec<_>>()
                    } else {
                        vec![f.name().to_owned()]
                    }
                }
            }
        })
        .flatten()
        .collect::<Vec<_>>()
}

// FIXME: ACTUALLY look at the arrow schema and handle map types correctly
// Right now, we are matching "map-like" parquet leaves like "key_value.key" etc
// But, we neeed to walk through both the arrow schema (which KNOWS about the map type)
// and the parquet leaves to do this correctly.
fn equivalent_projection_paths_from_parquet_schema(
    _arrow_schema: SchemaRef,
    parquet_schema: &SchemaDescriptor,
) -> Vec<(usize, (String, String))> {
    let mut output: Vec<(usize, (String, String))> = vec![];
    for (i, col) in parquet_schema.columns().iter().enumerate() {
        let original_path = col.path().string();
        let converted_path = {
            // we convert the path in the parquet schema to ignore stuff related to maps, entries, lists
            let parquet_path = original_path.as_str();
            if parquet_path.contains(".key_value.key")
                || parquet_path.contains(".key_value.value")
                || parquet_path.contains(".entries.keys")
                || parquet_path.contains(".entries.values")
                || parquet_path.contains(".list.element")
            {
                parquet_path
                    .replace("key_value.key", "*")
                    .replace("key_value.value", "*")
                    .replace("entries.keys", "*")
                    .replace("entries.values", "*")
                    .replace("list.element", "*")
            } else {
                parquet_path.to_string()
            }
        };
        output.push((i, (original_path.clone(), converted_path)));
    }
    output
}

#[allow(clippy::ptr_arg)]
pub fn parquet_leaf_paths(
    arrow_schema: SchemaRef,
    parquet_schema: &SchemaDescriptor,
    projection: &Vec<usize>,
    projection_deep: &HashMap<usize, Vec<String>>,
) -> Vec<usize> {
    let actual_projection = if projection.is_empty() {
        (0..arrow_schema.fields().len()).collect()
    } else {
        projection.clone()
    };
    let splatted = splat_columns(&arrow_schema, &actual_projection, projection_deep);

    let mut out: Vec<usize> = vec![];
    for (i, (_original, converted)) in
        equivalent_projection_paths_from_parquet_schema(arrow_schema, parquet_schema)
    {
        // FIXME
        //  for map fields, the actual parquet paths look like x.y.z.key_value.key, x.y.z.key_value.value
        //  since we are ignoring these names in the paths, we need to actually collapse this access to a *
        //  so we can filter for them
        //  also, we need BOTH the key and the value for maps otherwise we run into an arrow-rs error
        //  "partial projection of MapArray is not supported"

        let mut found = false;
        for filter in splatted.iter() {
            // check if this filter matches this leaf path
            let filter_pieces = filter.split(".").collect::<Vec<&str>>();
            let col_pieces = converted.split(".").collect::<Vec<_>>();
            let mut filter_found = true;
            for i in 0..min(filter_pieces.len(), col_pieces.len()) {
                if i >= filter_pieces.len() {
                    //  we are at the end of the filter, and we matched until now, so we break, we match !
                    break;
                }
                if i >= col_pieces.len() {
                    // we have a longer filter, we matched until now, we match !
                    break;
                }
                // we can actually check
                if !(col_pieces[i] == filter_pieces[i] || filter_pieces[i] == "*") {
                    filter_found = false;
                    break;
                }
            }
            if filter_found {
                found = true;
                break;
            }
        }
        if found {
            out.push(i);
        }
    }
    out
}

pub fn has_simplified_parquet_columns(possible: &HashMap<usize, Vec<String>>) -> bool {
    !(possible.is_empty() || possible.iter().all(|(_k, v)| v.is_empty()))
}

pub fn projection_specifier(
    logical_file_schema: SchemaRef,
    _projection: &ProjectionExprs,
    projection_hints: Option<&ProjectionExprs>,
    _projection_hints_indices: &[usize],
) -> HashMap<usize, Vec<String>> {
    // info!(target: "deep", "leaves::projection_specifier >>>>>>>>>>>>>>>>>>>>>>>>>>>");
    let mut deep_column_map: HashMap<usize, HashSet<String>> = HashMap::new();

    let source_schema = logical_file_schema.clone();
    // for expr in projection
    //     .iter()
    //     .map(|pe| extract_expressions_containing_column(&pe.expr))
    //     .flatten()
    // {
    //     info!("leaves::projection_specifier XXX expr: {:?}, indices: {:?}", expr.to_string(), projection_hints_indices);
    //     let col_arg = find_column_in_expr(&expr).unwrap();
    //     let col_index = source_schema
    //         .index_of(col_arg.name())
    //         .expect("Col in table");
    //     if !projection_hints_indices.contains(&col_index) {
    //         // let marker = format!("xx: {} {}", col_index, expr_to_deep_projection(&pexpr));
    //         let marker = simplified_parquet_column_path(&expr);
    //         let entry = deep_column_map.entry(col_index).or_default();
    //         if entry.contains("") {
    //             // already full column !!!!!!!
    //         } else {
    //             entry.insert(marker);
    //         }
    //     }
    // }
    // info!("leaves::projection_specifier deep column map after handling projection: {:?}", &deep_column_map);

    for pexpr in projection_hints.into_iter().flatten() {
        let expr = pexpr.clone().expr;
        // info!(target: "deep", "leaves::projection_specifier projection hint: {}", expr.to_string());
        let col_arg = get_first_column_from_expr(&expr).unwrap();
        let col_index = source_schema
            .index_of(col_arg.name())
            .expect("Col in table");
        let marker = simplified_parquet_column_path(&expr);
        // info!(target: "deep", "    > marker: {}", marker.as_str());
        let entry = deep_column_map.entry(col_index).or_default();
        if entry.contains("") {
            // already full column !!!!!!!
        } else {
            entry.insert(marker);
        }
    }
    // info!(target: "deep", "leaves::projection_specifier deep column map after handling projection hints: {:?}", &deep_column_map);

    let final_map: HashMap<usize, Vec<String>> = deep_column_map
        .iter()
        .map(|(k, v)| {
            let k = k.clone();
            let mut newv = v
                .into_iter()
                // clone
                .map(|s| s.clone())
                // // remove empty specifiers
                // .filter(|v| v != "")
                // fix fake field names which are actually map names
                .map(|v| {
                    // info!("fix_deep for {}", v.as_str());
                    fix_simplified_column_path(v.as_str(), source_schema.field(k))
                        .unwrap()
                    // let pieces = v.split(".").collect::<Vec<_>>();
                })
                .collect::<Vec<String>>();
            newv.sort_by_key(|s| (s.split('.').count(), s.len()));
            let mut kept: Vec<String> = Vec::new();
            newv.retain(|s| {
                let is_extension = kept.iter().any(|k| {
                    k.is_empty() || s == k || s.starts_with(&format!("{k}."))
                });
                if !is_extension {
                    kept.push(s.clone());
                    true
                } else {
                    false
                }
            });
            let newv = newv.into_iter().filter(|v| v != "").collect::<Vec<_>>();
            (k, newv)
        })
        .into_iter()
        .collect();
    // info!(target: "deep", "leaves::projection_specifier final map: {:?}", &final_map);
    // info!(target: "deep", "leaves::projection_specifier <<<<<<<<<<<<<<<<<<<<<<<<<<");
    final_map
}

/// returns true if the expr is a combination of get_field, array_elements, CAST, or column reference
pub fn expr_is_only_get_field_or_array_or_cast_and_contains_column(
    input: &Arc<dyn PhysicalExpr>,
) -> bool {
    let mut has_invalid_expr_type = false;
    let mut has_column = false;
    let _ = input.apply(|pe| {
        if let Some(sfe) = pe.as_any().downcast_ref::<ScalarFunctionExpr>()
            && sfe.name() == "get_field"
        {
            return Ok(TreeNodeRecursion::Continue);
        };
        if let Some(sfe) = pe.as_any().downcast_ref::<ScalarFunctionExpr>()
            && sfe.name() == "array_element"
        {
            return Ok(TreeNodeRecursion::Continue);
        };
        if let Some(_) = pe.as_any().downcast_ref::<CastExpr>() {
            return Ok(TreeNodeRecursion::Continue);
        };
        if let Some(_) = pe.as_any().downcast_ref::<Column>() {
            has_column = true;
            return Ok(TreeNodeRecursion::Stop);
        };
        has_invalid_expr_type = true;
        return Ok(TreeNodeRecursion::Stop);
    });
    !has_invalid_expr_type && has_column
}

pub fn extract_expressions_for_deep_projection(input: &Arc<dyn PhysicalExpr>) -> Vec<Arc<dyn PhysicalExpr>> {
    let mut out: Vec<Arc<dyn PhysicalExpr>> = vec![];
    let _ = input.apply(|pe| {
        if expr_is_only_get_field_or_array_or_cast_and_contains_column(pe) {
            out.push(pe.clone());
            return Ok(TreeNodeRecursion::Jump);
        }
        Ok(TreeNodeRecursion::Continue)
    });
    out
}

/// returns true if the expr is a combination of get_field, array_elements, CAST, or column reference
pub fn expr_has_get_field_or_array_element(
    input: &Arc<dyn PhysicalExpr>,
) -> bool {
    let mut has_invalid_expr_type = false;
    let _ = input.apply(|pe| {
        if let Some(sfe) = pe.as_any().downcast_ref::<ScalarFunctionExpr>()
            && sfe.name() == "get_field"
        {
            has_invalid_expr_type = true;
            return Ok(TreeNodeRecursion::Stop);
        };
        if let Some(sfe) = pe.as_any().downcast_ref::<ScalarFunctionExpr>()
            && sfe.name() == "array_element"
        {
            has_invalid_expr_type = true;
            return Ok(TreeNodeRecursion::Stop);
        };
        return Ok(TreeNodeRecursion::Continue);
    });
    has_invalid_expr_type
}


/// extracts a list of expression that contain a small set of operations from a larger column
/// This drills down in expressions until it reaches branches of the tree that only contain get_field, array_element, CAST, or column
/// linearizes that and return them
pub fn get_expressions_amenable_to_deep_projection(
    input: &Arc<dyn PhysicalExpr>,
) -> Vec<Arc<dyn PhysicalExpr>> {
    let mut out = vec![];
    let _ = input.apply(|pe| {
        if expr_is_only_get_field_or_array_or_cast_and_contains_column(pe) {
            out.push(pe.clone());
            return Ok(TreeNodeRecursion::Jump);
        }
        Ok(TreeNodeRecursion::Continue)
    });
    out
}

/// finds a column inside a larger expression that may contain columns
pub fn get_first_column_from_expr(expr: &Arc<dyn PhysicalExpr>) -> Option<Column> {
    let mut out: Option<Column> = None;
    let _ = expr.apply(|expr| {
        if let Some(column) = expr.as_any().downcast_ref::<Column>() {
            out = Some(column.clone());
            return Ok(TreeNodeRecursion::Stop);
        }
        Ok(TreeNodeRecursion::Continue)
    });
    out
}

pub fn simplified_parquet_column_path(expr: &Arc<dyn PhysicalExpr>) -> String {
    let mut accum: VecDeque<String> = VecDeque::new();
    let mut out: Vec<String> = vec![];
    let mut in_other_literal_call: bool = false;
    let _ = expr.apply(|e| {
        if let Some(_) = e.as_any().downcast_ref::<Column>() {
            // This used to have a .rev at the end, because we were getting nested get_field expressions
            // now, get_field adds all the parameters, so we can
            // out.push(column.name().to_string());
            for f in accum.iter().rev() {
                out.push(f.to_owned());
            }
            return Ok(TreeNodeRecursion::Stop);
        }
        if let Some(sfe) = e.as_any().downcast_ref::<ScalarFunctionExpr>()
            && sfe.name() == "get_field"
        {
            if sfe.args().len() == 2 {
                if let Some(literal) = sfe.args()[1].as_any().downcast_ref::<Literal>() {
                    match literal.value() {
                        ScalarValue::Utf8(Some(str)) => {
                            accum.push_back(str.clone());
                        }
                        _ => {
                            error!("Can't handle expression 1 {:?}", sfe.args()[1]);
                            in_other_literal_call = true;
                        }
                    }
                } else {
                    error!("Can't handle expression 2 {:?}", sfe.args()[1]);
                }
            } else {
                sfe.args()
                    .iter()
                    .skip(1)
                    .enumerate()
                    .map(|(_i, arg)| {
                        if let Some(literal) = arg.as_any().downcast_ref::<Literal>() {
                            match literal.value() {
                                ScalarValue::Utf8(Some(str)) => Ok(str.clone()),
                                _ => {
                                    error!("Can't handle expression {:?}", sfe.args()[1]);
                                    in_other_literal_call = true;
                                    internal_err!("field argument")
                                }
                            }
                        } else {
                            error!("Can't handle expression {:?}", sfe.args()[1]);
                            internal_err!("field argument")
                        }
                    })
                    .map_while(datafusion_common::Result::ok)
                    .collect::<Vec<_>>()
                    .into_iter()
                    .rev()
                    .for_each(|arg| {
                        accum.push_back(arg);
                    });
            }
        }
        if let Some(sfe) = e.as_any().downcast_ref::<ScalarFunctionExpr>()
            && sfe.name() == "array_element"
        {
            accum.push_back("*".to_string());
        }
        Ok(TreeNodeRecursion::Continue)
    });
    out.join(".")
}

pub fn fix_simplified_column_path(
    specifier: &str,
    field: &Field,
) -> datafusion_common::Result<String> {
    if specifier == "" {
        return Ok("".to_string());
    }
    let pieces = specifier.split(".").collect::<Vec<_>>();
    let mut idx = 0;
    let mut out_pieces: Vec<String> = vec![];
    let mut current = field;
    'outer: loop {
        if idx > pieces.len() - 1 {
            break;
        }
        // not at the end
        match current.data_type() {
            DataType::Map(map_struct, _) => {
                out_pieces.push("*".to_string());
                idx += 1;
                if let DataType::Struct(map_struct_fields) = map_struct.data_type() {
                    let (_idx, field) = map_struct_fields
                        .find("value")
                        .expect("map struct should have value");
                    current = field;
                    continue 'outer;
                }
            }
            DataType::List(inner)
            | DataType::ListView(inner)
            | DataType::FixedSizeList(inner, _)
            | DataType::LargeList(inner)
            | DataType::LargeListView(inner) => {
                if pieces[idx] != "*" {
                    // this was a get_field call, we need to insert an access
                    out_pieces.push("*".to_string());
                    current = inner;
                    continue 'outer;
                } else {
                    // this was an array_element call
                    out_pieces.push("*".to_string());
                    idx += 1;
                    current = inner;
                    continue 'outer;
                }
            }
            DataType::Struct(inner) => {
                out_pieces.push(pieces[idx].to_string());
                let (_idx, field) = inner
                    .find(pieces[idx])
                    .expect("map struct should have value");
                current = field;
                idx += 1;
                continue 'outer;
            }
            _ => {
                error!(
                    "fix_deep_column_specifier_for_field at index {} for specifier {} - cannot handle {:?}",
                    idx,
                    specifier,
                    current.data_type()
                );
                return internal_err!(
                    "fix_deep_column_specifier_for_field not at end, but non-nested field"
                );
            }
        }
    }
    Ok(out_pieces.join("."))
}
