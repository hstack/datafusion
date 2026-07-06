use crate::{project_schema, Result, cast_column};
use arrow::array::{ArrayRef, RecordBatch, RecordBatchOptions, StructArray};
use arrow::compute::{CastOptions, can_cast_types};
use arrow::datatypes::{DataType, Field, FieldRef, Fields, Schema, SchemaRef};
use log::{error, trace};
use std::collections::HashMap;
use std::fs::OpenOptions;
use std::io::Write;
use std::sync::Arc;
use crate::cast::as_struct_array;

/// Check whether an Arrow [DataType] is recursive in the sense that we need to
/// look inside and continue unpacking the data
/// This is used when creating a schema based on a deep projection
pub fn data_type_recurs(dt: &DataType) -> bool {
    match dt {
        // scalars
        DataType::Null
        | DataType::Boolean
        | DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64
        | DataType::Float16
        | DataType::Float32
        | DataType::Float64
        | DataType::Timestamp(_, _)
        | DataType::Date32
        | DataType::Date64
        | DataType::Time32(_)
        | DataType::Time64(_)
        | DataType::Duration(_)
        | DataType::Interval(_)
        | DataType::Binary
        | DataType::FixedSizeBinary(_)
        | DataType::LargeBinary
        | DataType::BinaryView
        | DataType::Utf8
        | DataType::LargeUtf8
        | DataType::Utf8View
        | DataType::Decimal32(_, _)
        | DataType::Decimal64(_, _)
        | DataType::Decimal128(_, _)
        | DataType::Decimal256(_, _)
        | DataType::Dictionary(_, _) => false,
        // containers
        DataType::RunEndEncoded(_, val) => data_type_recurs(val.data_type()),
        DataType::Union(_, _) => true,
        DataType::List(f) => data_type_recurs(f.data_type()),
        DataType::ListView(f) => data_type_recurs(f.data_type()),
        DataType::FixedSizeList(f, _) => data_type_recurs(f.data_type()),
        DataType::LargeList(f) => data_type_recurs(f.data_type()),
        DataType::LargeListView(f) => data_type_recurs(f.data_type()),
        // list of struct
        DataType::Map(_, _) => true,
        DataType::Struct(_) => true,
    }
}

/// Mutually recursive with [can_rewrite_field]
/// checks whether we can rewrite a source [Fields] object to a destination one
/// the missing fields in the source behavior can be changed with the [`fill_missing_source_field`]
/// parameter.
pub fn can_rewrite_fields(
    dst_fields: &Fields,
    src_fields: &Fields,
    fill_missing_source_fields: bool,
) -> bool {
    let mut out = true;
    for i in 0..dst_fields.len() {
        let dst_field = Arc::clone(&dst_fields[i]);
        let dst_name = dst_field.name();

        let src_field_opt = src_fields
            .iter()
            .enumerate()
            .find(|(_idx, b)| b.name() == dst_name);

        // if the field exists in the source
        if let Some((_src_idx, src_field)) = src_field_opt {
            let src_field = Arc::clone(src_field);
            let can_cast =
                can_rewrite_field(&dst_field, &src_field, fill_missing_source_fields);
            out = out && can_cast;
        } else {
            out = out && fill_missing_source_fields;
        }
    }
    out
}

/// Mutually recursive with [can_rewrite_fielda]
/// checks whether we can rewrite a source [FieldRef] object to a destination one
/// the missing fields in the source behavior can be changed with the [`fill_missing_source_field`]
/// parameter.
pub fn can_rewrite_field(
    dst_field: &FieldRef,
    src_field: &FieldRef,
    fill_missing_source_fields: bool,
) -> bool {
    let can_cast_by_arrow = !data_type_recurs(dst_field.data_type())
        && !data_type_recurs(src_field.data_type());
    if can_cast_by_arrow {
        return can_cast_types(src_field.data_type(), dst_field.data_type());
    }
    match (src_field.data_type(), dst_field.data_type()) {
        (DataType::List(src_inner), DataType::List(dst_inner))
        | (DataType::List(src_inner), DataType::LargeList(dst_inner))
        | (DataType::LargeList(src_inner), DataType::LargeList(dst_inner)) => {
            if data_type_recurs(src_inner.data_type())
                && data_type_recurs(dst_inner.data_type())
            {
                can_rewrite_field(
                    dst_inner,
                    src_inner,
                    fill_missing_source_fields,
                )
            } else {
                can_cast_types(src_inner.data_type(), dst_inner.data_type())
            }
        }
        (
            DataType::FixedSizeList(src_inner, src_sz),
            DataType::FixedSizeList(dst_inner, dst_sz),
        ) => {
            if src_sz != dst_sz {
                return false;
            }
            if data_type_recurs(src_inner.data_type())
                && data_type_recurs(dst_inner.data_type())
            {
                can_rewrite_field(
                    dst_inner,
                    src_inner,
                    fill_missing_source_fields,
                )
            } else {
                can_cast_types(src_inner.data_type(), dst_inner.data_type())
            }
        }
        (DataType::Map(src_inner, _), DataType::Map(dst_inner, _)) => {
            match (src_inner.data_type(), dst_inner.data_type()) {
                (DataType::Struct(src_inner_f), DataType::Struct(dst_inner_f)) => {
                    can_rewrite_field(
                        &dst_inner_f[1],
                        &src_inner_f[1],
                        fill_missing_source_fields,
                    )
                }
                _ => false,
            }
        }
        (DataType::Struct(src_inner), DataType::Struct(dst_inner)) => {
            can_rewrite_fields(dst_inner, src_inner, fill_missing_source_fields)
        }
        (DataType::Union(src_fields, src_mode), DataType::Union(dst_fields, dst_mode)) => {
            src_fields == dst_fields && src_mode == dst_mode
        }
        (_src, _dest) => {
            error!(
                    target: "deepschema",
                    "  can_rewrite_field: Unhandled src dest field: src {}={:?}, dst {}={:?}",
                    src_field.name(),
                    src_field.data_type(),
                    dst_field.name(),
                    dst_field.data_type()
            );
            false
        }
    }
}

pub fn can_cast_datatype_deep(from: &DataType, to: &DataType, fill_missing_source_field: bool) -> bool {
    let ffrom = Field::new("f1", from.clone(), true);
    let fto = Field::new("f1", to.clone(), true);
    can_rewrite_field(&Arc::new(fto), &Arc::new(ffrom), fill_missing_source_field)
}

/// Deep projections are represented using a HashMap<usize, Vec<String>>
/// for backwards compatibility (current projections are represented using a [`Vec<usize>`]
/// Currently, deep projections are represented (even if there's some duplicated information as a
/// [`HashMap<usize, Vec<String>>`]
/// the key is the source field id of the top-level field
/// the value is a list of "paths" inside the top-level field
/// Examples:
///   Scalar fields - no representations of paths inside the field possible
///   List<Scalar> fields - same thing
///   List<Struct<id, name, address>> - possible paths may be "*.id", "*.name", "*.address"
///   List<Map<String, Map<String, Struct<id, name, address>>>
///     - possible paths may be "*.*", "*.*.*.id", "*.*.*.name", "*.*.*.address"
pub fn has_deep_projection(possible: &HashMap<usize, Vec<String>>) -> bool {
    !(possible.is_empty() || possible.iter().all(|(_k, v)| v.is_empty()))
}

/// Combines the current projection (numeric indices of top-level columns) with
/// the deep projection - "paths" inside a top-level column
pub fn splat_columns(
    src: &SchemaRef,
    projection: &[usize],
    projection_deep: &HashMap<usize, Vec<String>>,
) -> Vec<String> {
    let mut out: Vec<String> = vec![];
    for pi in projection.iter() {
        let f = src.field(*pi);
        match projection_deep.get(pi) {
            None => {
                out.push(f.name().to_owned());
            }
            Some(rests) => {
                if !rests.is_empty() {
                    for rest in rests.iter() {
                        out.push(format!("{}.{}", f.name(), rest))
                    }
                } else {
                    out.push(f.name().to_owned());
                }
            }
        }
    }
    out
}

pub fn try_rewrite_schema_opt(
    src: SchemaRef,
    projection_opt: Option<&Vec<usize>>,
    projection_deep_opt: Option<&HashMap<usize, Vec<String>>>,
) -> Result<SchemaRef> {
    match projection_opt {
        None => Ok(src),
        Some(projection) => match projection_deep_opt {
            None => project_schema(&src, projection_opt),
            Some(projection_deep) => Ok(rewrite_schema(&src, projection, projection_deep)),
        },
    }
}

pub fn rewrite_field_projection(
    src: &SchemaRef,
    projected_field_idx: usize,
    projection_deep: &HashMap<usize, Vec<String>>,
) -> FieldRef {
    let original_field = Arc::new(src.field(projected_field_idx).clone());
    let single_field_schema = Arc::new(Schema::new(vec![original_field]));
    // rewrite projection, deep projection to use 0
    let projected_vec = vec![0];
    let mut projected_deep_vec = HashMap::new();
    let empty_vec: Vec<String> = vec![];
    projected_deep_vec.insert(
        0usize,
        projection_deep
            .get(&projected_field_idx)
            .unwrap_or(&empty_vec)
            .clone(),
    );

    let rewritten_single_field_schema =
        rewrite_schema(&single_field_schema, &projected_vec, &projected_deep_vec);
    Arc::new(rewritten_single_field_schema.field(0).clone())
}

fn make_path(parent: &str, name: &str) -> String {
    if parent.is_empty() {
        name.to_owned()
    } else {
        format!("{parent}.{name}")
    }
}

fn path_prefix_exists(filters: &[String], path: &String) -> bool {
    filters.iter().any(|f| {
        let tmp = f.find(path);
        tmp.is_some() && tmp.unwrap() == 0
    })
}

fn path_included(filters: &[String], path: &str) -> bool {
    filters.iter().any(|f| {
        let tmp = path.find(f);
        tmp.is_some() && tmp.unwrap() == 0
    })
}

pub fn rewrite_schema(
    src: &SchemaRef,
    projection: &Vec<usize>,
    projection_deep: &HashMap<usize, Vec<String>>,
) -> SchemaRef {
    trace!(target: "deepschema", "rewrite_schema: projection={projection:?}, projection_deep={projection_deep:?}, input schema={src:#?}");

    const FLAG_PARENT_IS_LIST: u8 = 0x2;
    const FLAG_PARENT_IS_MAP: u8 = 0x4;

    fn rewrite_schema_fields(
        parent: &str,
        parent_flags: u8,
        src_fields: &Fields,
        filters: &Vec<String>,
    ) -> Vec<FieldRef> {
        let mut out_fields: Vec<FieldRef> = vec![];
        for i in 0..src_fields.len() {
            let src_field = Arc::clone(&src_fields[i]);
            let src_field_path = make_path(parent, src_field.name());
            // trace!(target:"deep", "rewrite_schema_fields: parent={}, src_field_path={}, filters={:?}, field={:#?}", parent, src_field_path, filters, src_field);
            trace!(target:"deepschema", "rewrite_schema_fields: parent={parent}, src_field_path={src_field_path}, filters={filters:?}, parent_flags: {parent_flags}");

            let field_path_included = path_included(filters, &src_field_path); //filters.contains(&src_field_path);
            let mut field_path_with_star_included = false;
            if parent_flags & FLAG_PARENT_IS_LIST > 0 {
                let mut src_field_path_with_star = src_field_path.clone();
                src_field_path_with_star.push_str(".*");
                field_path_with_star_included =
                    path_included(filters, &src_field_path_with_star);
            }

            if field_path_included || field_path_with_star_included {
                out_fields.push(Arc::clone(&src_field));
            } else if data_type_recurs(src_field.data_type())
                && path_prefix_exists(filters, &src_field_path)
            {
                match rewrite_schema_field(parent, 0, &src_field, filters) {
                    None => {}
                    Some(f) => out_fields.push(f),
                }
            }
        }
        out_fields
    }

    fn rewrite_schema_field(
        parent: &str,
        parent_flags: u8,
        src_field: &FieldRef,
        filters: &Vec<String>,
    ) -> Option<FieldRef> {
        let src_field_name = src_field.name();
        // FIXME: @HStack
        //  if we already navigated to this field and the accessor is "*"
        //  that means we don't care about the field name
        //  RETEST THIS for lists
        let comes_from_list_and_last_is_set = !parent.is_empty()
            && (parent.ends_with('*')
                && (parent_flags & FLAG_PARENT_IS_LIST) > 0);
        let comes_from_map_and_last_is_set = !parent.is_empty()
            && (parent.ends_with('*')
                && (parent_flags & FLAG_PARENT_IS_MAP) > 0);

        let src_field_path =
            if comes_from_list_and_last_is_set || comes_from_map_and_last_is_set {
                parent.to_string()
            } else {
                make_path(parent, src_field_name)
            };
        // trace!(target:"deep", "rewrite_schema_field: src_field_name={}, src_field_path={}, filters={:?}, field={:#?}", src_field_name, src_field_path, filters, src_field);
        trace!(target:"deepschema", "rewrite_schema_field: src_field_name={src_field_name}, src_field_path={src_field_path}, filters={filters:?}, parent_flags={parent_flags}");
        trace!(target:"deepschema", "rewrite_schema_field: src_field_type={:?}", &src_field.data_type());
        let field_path_included = path_included(filters, &src_field_path); //filters.contains(&src_field_path);
        if field_path_included {
            trace!(target:"deepschema", "  rewrite_schema_field  return {src_field_path} directly ");
            Some(Arc::clone(src_field))
        } else if data_type_recurs(src_field.data_type())
            && path_prefix_exists(filters, &src_field_path)
        {
            let out = match src_field.data_type() {
                DataType::List(src_inner) => {
                    rewrite_schema_field(
                        make_path(src_field_path.as_str(), "*").as_str(),
                        FLAG_PARENT_IS_LIST,
                        src_inner,
                        filters,
                    )
                        .map(|inner| {
                            trace!(target:"deepschema", "return new list {} = {:#?}", src_field_name.clone(), Arc::clone(&inner));
                            Arc::new(Field::new_list(
                                src_field.name(),
                                inner,
                                src_field.is_nullable(),
                            ))
                        })
                }
                DataType::FixedSizeList(src_inner, src_sz) => rewrite_schema_field(
                    make_path(src_field_path.as_str(), "*").as_str(),
                    FLAG_PARENT_IS_LIST,
                    src_inner,
                    filters,
                )
                    .map(|inner| {
                        Arc::new(Field::new_fixed_size_list(
                            src_field.name(),
                            inner,
                            *src_sz,
                            src_field.is_nullable(),
                        ))
                    }),
                DataType::LargeList(src_inner) => rewrite_schema_field(
                    make_path(&src_field_path, "*").as_str(),
                    FLAG_PARENT_IS_LIST,
                    src_inner,
                    filters,
                )
                    .map(|inner| {
                        Arc::new(Field::new_large_list(
                            src_field.name(),
                            inner,
                            src_field.is_nullable(),
                        ))
                    }),

                DataType::Map(map_entry, map_sorted) => {
                    #[allow(clippy::get_first)]
                    if let DataType::Struct(map_entry_fields) = map_entry.data_type()
                    {
                        let map_key_field = map_entry_fields.get(0).unwrap();
                        let map_value_field = map_entry_fields.get(1).unwrap();
                        rewrite_schema_field(
                            make_path(&src_field_path, "*").as_str(),
                            FLAG_PARENT_IS_MAP,
                            map_value_field,
                            filters,
                        )
                            .map(|inner| {
                                Arc::new(Field::new_map(
                                    src_field_name,
                                    map_entry.name().clone(),
                                    Arc::clone(map_key_field),
                                    inner,
                                    *map_sorted,
                                    src_field.is_nullable(),
                                ))
                            })
                    } else {
                        panic!("Invalid internal field map: expected struct, but got {}", map_entry.data_type());
                    }
                }

                DataType::Struct(src_inner) => {
                    let dst_fields =
                        rewrite_schema_fields(src_field_path.as_str(), parent_flags, src_inner, filters);
                    trace!(target:"deepschema", "for struct: {} {} = {:#?}", src_field_name, src_field_path.clone(), dst_fields);
                    if !dst_fields.is_empty() {
                        Some(Arc::new(Field::new_struct(
                            src_field.name(),
                            dst_fields,
                            src_field.is_nullable(),
                        )))
                    } else {
                        None
                    }
                }
                x => {
                    panic!("Unhandled data type: {x:#?}");
                }
            };
            out
        } else {
            None
        }
    }

    let actual_projection = if projection.is_empty() {
        (0..src.fields().len()).collect()
    } else {
        projection.clone()
    };
    let splatted = splat_columns(src, &actual_projection, projection_deep);

    // trace!(target:"deep", "rewrite_schema source: {:#?}", src);
    trace!(target:"deepschema", "rewrite_schema splatted: {:?} {:?} = {:?}", &actual_projection, &projection_deep, splatted);
    let mut dst_fields: Vec<FieldRef> = vec![];
    for pi in actual_projection.iter() {
        let src_field = src.field(*pi);
        trace!(target:"deepschema", "rewrite_schema at field {}, splatted={:?}", src_field.name(), &splatted);
        let foutopt = rewrite_schema_field(
            "",
            0,
            &Arc::new(src_field.clone()),
            &splatted,
        );
        match foutopt {
            None => {}
            Some(fout) => {
                dst_fields.push(Arc::clone(&fout));
            }
        }
    }

    // let dst_fields = rewrite_fields("".to_string(), src.clone().fields(), &splatted);
    trace!(target:"deepschema", "rewrite_schema dst_fields: {dst_fields:#?}");

    if !dst_fields.is_empty() {
        Arc::new(Schema::new_with_metadata(dst_fields, src.metadata.clone()))
    } else {
        Arc::clone(src)
    }
}

pub fn debug_to_file(name: &str, contents: &str) {
    let mut file = OpenOptions::new()
        .write(true)
        .truncate(true)
        .create(true)
        .open(name)
        .unwrap();
    file.write_all(contents.as_bytes()).unwrap();
}

/// Cast recordbatch to a new target_schema, by casting each column array
pub fn cast_record_batch(
    batch: &RecordBatch,
    target_schema: SchemaRef,
    safe: bool,
    _add_missing: bool,
) -> Result<RecordBatch> {
    let cast_options = CastOptions {
        safe,
        ..Default::default()
    };

    let source: ArrayRef = Arc::new(StructArray::try_new_with_length(
        batch.schema().as_ref().to_owned().fields,
        batch.columns().to_owned(),
        None,
        batch.num_rows(),
    )?);
    let target_type = DataType::Struct(target_schema.fields().clone());
    let casted = cast_column(&source, &target_type, &cast_options)?;
    let casted = as_struct_array(casted.as_ref())?;
    Ok(RecordBatch::try_new_with_options(
        target_schema,
        casted.columns().to_vec(),
        &RecordBatchOptions::new().with_row_count(Some(batch.num_rows())),
    )?)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use crate::deep::rewrite_schema;
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    #[test]
    fn test_schema_rewrite_with_similar_column_names() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("car", DataType::Utf8, true),
            Field::new("cars", DataType::Utf8, true),
            Field::new("amount", DataType::Utf8, true),
            Field::new("amounts", DataType::Utf8, true)
        ]));
        let projection = vec![1];
        let mut projection_deep = HashMap::new();
        projection_deep.insert(1, vec![]);
        let rewritten_schema = rewrite_schema(&schema, &projection, &projection_deep);
        assert_eq!(rewritten_schema.fields().len(), 1);
        assert_eq!(rewritten_schema.field(0).name(), "cars");

        let projection = vec![0];
        let mut projection_deep = HashMap::new();
        projection_deep.insert(0, vec![]);
        let rewritten_schema = rewrite_schema(&schema, &projection, &projection_deep);
        assert_eq!(rewritten_schema.fields().len(), 1);
        assert_eq!(rewritten_schema.field(0).name(), "car");

    }

}
