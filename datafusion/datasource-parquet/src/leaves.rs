// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Schema-aware leaf requirements for opt-in deep projection.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, RecordBatch, StructArray, make_array, new_null_array,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion_common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion_common::{Result, ScalarValue, internal_err};
use datafusion_functions::core::getfield::GetFieldFunc;
use datafusion_functions_nested::extract::ArrayElement;
use datafusion_physical_expr::expressions::{CastExpr, Column, Literal};
use datafusion_physical_expr::projection::ProjectionExprs;
use datafusion_physical_expr::{PhysicalExpr, ScalarFunctionExpr};
use parquet::arrow::ProjectionMask;
use parquet::schema::types::SchemaDescriptor;

use crate::nested_schema_pruning::{clip_for_cast, count_leaves, field_with_type};
use crate::projection_read_plan::ParquetReadPlan;

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Step {
    Field(String),
    Element,
    Value,
}

#[derive(Default)]
struct Access {
    whole: bool,
    children: BTreeMap<Step, Access>,
}

impl Access {
    fn insert(&mut self, path: &[Step]) {
        if let Some((first, rest)) = path.split_first() {
            self.children.entry(first.clone()).or_default().insert(rest);
        } else {
            self.whole = true;
        }
    }

    fn insert_type(&mut self, data_type: &DataType) {
        match data_type {
            DataType::Struct(fields) => {
                for field in fields {
                    self.children
                        .entry(Step::Field(field.name().clone()))
                        .or_default()
                        .insert_type(field.data_type());
                }
            }
            DataType::List(item)
            | DataType::LargeList(item)
            | DataType::FixedSizeList(item, _)
            | DataType::ListView(item)
            | DataType::LargeListView(item) => {
                self.children
                    .entry(Step::Element)
                    .or_default()
                    .insert_type(item.data_type());
            }
            _ => self.whole = true,
        }
    }

    fn prune(&self, data_type: &DataType) -> DataType {
        if self.whole {
            return data_type.clone();
        }
        match data_type {
            DataType::Struct(fields) => {
                let fields = fields
                    .iter()
                    .filter_map(|field| {
                        self.children.get(&Step::Field(field.name().clone())).map(
                            |access| {
                                field_with_type(field, access.prune(field.data_type()))
                            },
                        )
                    })
                    .collect();
                DataType::Struct(fields)
            }
            DataType::List(item)
            | DataType::LargeList(item)
            | DataType::FixedSizeList(item, _)
            | DataType::ListView(item)
            | DataType::LargeListView(item) => {
                let Some(access) = self.children.get(&Step::Element) else {
                    return data_type.clone();
                };
                let item = field_with_type(item, access.prune(item.data_type()));
                match data_type {
                    DataType::List(_) => DataType::List(item),
                    DataType::LargeList(_) => DataType::LargeList(item),
                    DataType::FixedSizeList(_, size) => {
                        DataType::FixedSizeList(item, *size)
                    }
                    DataType::ListView(_) => DataType::ListView(item),
                    _ => DataType::LargeListView(item),
                }
            }
            DataType::Map(entries, sorted) => {
                let DataType::Struct(fields) = entries.data_type() else {
                    return data_type.clone();
                };
                let Some(access) = self.children.get(&Step::Value) else {
                    return data_type.clone();
                };
                let value = &fields[1];
                DataType::Map(
                    field_with_type(
                        entries,
                        DataType::Struct(
                            vec![
                                Arc::clone(&fields[0]),
                                field_with_type(value, access.prune(value.data_type())),
                            ]
                            .into(),
                        ),
                    ),
                    *sorted,
                )
            }
            _ => data_type.clone(),
        }
    }
}

fn field_access(expr: &Arc<dyn PhysicalExpr>) -> Option<(Column, Vec<Step>)> {
    if let Some(column) = expr.downcast_ref::<Column>() {
        return Some((column.clone(), vec![]));
    }
    if let Some(func) =
        ScalarFunctionExpr::try_downcast_func::<GetFieldFunc>(expr.as_ref())
    {
        let (column, mut path) = field_access(func.args().first()?)?;
        for key in &func.args()[1..] {
            let literal = key.downcast_ref::<Literal>()?;
            match literal.value().try_as_str().flatten() {
                Some(name) => path.push(Step::Field(name.to_string())),
                None => path.push(Step::Value),
            }
        }
        return Some((column, path));
    }
    if let Some(func) =
        ScalarFunctionExpr::try_downcast_func::<ArrayElement>(expr.as_ref())
    {
        let (column, mut path) = field_access(func.args().first()?)?;
        path.push(Step::Element);
        return Some((column, path));
    }
    None
}

/// Find the first root column in an expression.
pub fn get_first_column_from_expr(expr: &Arc<dyn PhysicalExpr>) -> Option<Column> {
    let mut column = None;
    expr.apply(|node| {
        if let Some(found) = node.downcast_ref::<Column>() {
            column = Some(found.clone());
            return Ok(TreeNodeRecursion::Stop);
        }
        Ok(TreeNodeRecursion::Continue)
    })
    .expect("Valid expressions must support traversal");
    column
}

/// Whether an expression is a column selector, optionally wrapped in casts.
pub fn expr_is_only_get_field_or_array_or_cast_and_contains_column(
    expr: &Arc<dyn PhysicalExpr>,
) -> bool {
    let stripped = Arc::clone(expr)
        .transform_up(|node| {
            if let Some(cast) = node.downcast_ref::<CastExpr>() {
                Ok(datafusion_common::tree_node::Transformed::yes(Arc::clone(
                    cast.expr(),
                )))
            } else {
                Ok(datafusion_common::tree_node::Transformed::no(node))
            }
        })
        .expect("Valid expressions must support traversal");
    field_access(&stripped.data).is_some()
}

/// Whether an expression contains a field or array-element selector.
pub fn expr_has_get_field_or_array_element(expr: &Arc<dyn PhysicalExpr>) -> bool {
    let mut found = false;
    expr.apply(|node| {
        if ScalarFunctionExpr::try_downcast_func::<GetFieldFunc>(node.as_ref()).is_some()
            || ScalarFunctionExpr::try_downcast_func::<ArrayElement>(node.as_ref())
                .is_some()
        {
            found = true;
            return Ok(TreeNodeRecursion::Stop);
        }
        Ok(TreeNodeRecursion::Continue)
    })
    .expect("Valid expressions must support traversal");
    found
}

/// Split an expression into independently required column selectors.
pub fn extract_expressions_for_deep_projection(
    expr: &Arc<dyn PhysicalExpr>,
) -> Vec<Arc<dyn PhysicalExpr>> {
    column_dependencies(expr)
        .expect("Valid expressions must support dependency traversal")
}

/// Split an expression into independently required column selectors.
pub fn get_expressions_amenable_to_deep_projection(
    expr: &Arc<dyn PhysicalExpr>,
) -> Vec<Arc<dyn PhysicalExpr>> {
    extract_expressions_for_deep_projection(expr)
}

/// Produce an unqualified selector path, omitting cast nodes.
pub fn simplified_parquet_column_path(expr: &Arc<dyn PhysicalExpr>) -> String {
    let stripped = Arc::clone(expr)
        .transform_up(|node| {
            if let Some(cast) = node.downcast_ref::<CastExpr>() {
                Ok(datafusion_common::tree_node::Transformed::yes(Arc::clone(
                    cast.expr(),
                )))
            } else {
                Ok(datafusion_common::tree_node::Transformed::no(node))
            }
        })
        .expect("Valid expressions must support traversal");
    field_access(&stripped.data)
        .map(|(_, path)| {
            path.iter()
                .map(|step| match step {
                    Step::Field(name) => name.as_str(),
                    Step::Element | Step::Value => "*",
                })
                .collect::<Vec<_>>()
                .join(".")
        })
        .unwrap_or_default()
}

/// Resolve implicit list traversal and map lookups in a legacy selector path.
pub fn fix_simplified_column_path(path: &str, field: &Field) -> Result<String> {
    if path.is_empty() {
        return Ok(String::new());
    }
    let steps = path
        .split('.')
        .map(|part| {
            if part == "*" {
                Step::Element
            } else {
                Step::Field(part.to_string())
            }
        })
        .collect::<Vec<_>>();
    let path = normalize_path(field.data_type(), &steps).ok_or_else(|| {
        datafusion_common::internal_datafusion_err!(
            "Cannot resolve nested projection {path} in field {}",
            field.name()
        )
    })?;
    Ok(path
        .iter()
        .map(|step| match step {
            Step::Field(name) => name.as_str(),
            Step::Element | Step::Value => "*",
        })
        .collect::<Vec<_>>()
        .join("."))
}

/// Split arbitrary expressions into field accesses and whole-column dependencies.
/// Index expressions are independent dependencies, including non-literal indices.
pub(crate) fn column_dependencies(
    expr: &Arc<dyn PhysicalExpr>,
) -> Result<Vec<Arc<dyn PhysicalExpr>>> {
    let mut dependencies = Vec::new();
    expr.apply(|node| {
        if field_access(node).is_some() {
            dependencies.extend(selector_argument_dependencies(node)?);
            let hint = Arc::clone(node).transform_up(|access| {
                if ScalarFunctionExpr::try_downcast_func::<ArrayElement>(access.as_ref())
                    .is_some()
                {
                    let mut children =
                        access.children().into_iter().cloned().collect::<Vec<_>>();
                    for index in &mut children[1..] {
                        *index = Arc::new(Literal::new(ScalarValue::Int64(Some(1))));
                    }
                    Ok(datafusion_common::tree_node::Transformed::yes(
                        access.with_new_children(children)?,
                    ))
                } else {
                    Ok(datafusion_common::tree_node::Transformed::no(access))
                }
            })?;
            dependencies.push(hint.data);
            return Ok(TreeNodeRecursion::Jump);
        }
        // Preserve narrowing casts for upstream's schema-driven clipping.
        if let Some(cast) = node.downcast_ref::<CastExpr>()
            && cast.expr().downcast_ref::<Column>().is_some()
        {
            dependencies.push(Arc::clone(node));
            return Ok(TreeNodeRecursion::Jump);
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    Ok(dependencies)
}

fn selector_argument_dependencies(
    expr: &Arc<dyn PhysicalExpr>,
) -> Result<Vec<Arc<dyn PhysicalExpr>>> {
    let mut dependencies = vec![];
    expr.apply(|access| {
        if let Some(func) =
            ScalarFunctionExpr::try_downcast_func::<ArrayElement>(access.as_ref())
                .or_else(|| {
                    ScalarFunctionExpr::try_downcast_func::<GetFieldFunc>(access.as_ref())
                })
        {
            for argument in &func.args()[1..] {
                dependencies.extend(column_dependencies(argument)?);
            }
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    Ok(dependencies)
}

/// Pure selectors can forward narrower demands through computed projections.
/// Their key/index expressions must still run; arbitrary computations and casts
/// retain all dependencies to preserve evaluation errors.
pub(crate) fn computation_dependencies(
    expr: &Arc<dyn PhysicalExpr>,
    schema: &Schema,
) -> Result<Vec<Arc<dyn PhysicalExpr>>> {
    let hint = strip_shape_casts(expr, schema)?;
    if field_access(&hint).is_some() {
        selector_argument_dependencies(&hint)
    } else {
        column_dependencies(&hint)
    }
}

/// Hint expressions are not evaluated. Remove only infallible container casts
/// whose element fields are unchanged, never casts that can change field values.
pub(crate) fn strip_shape_casts(
    expr: &Arc<dyn PhysicalExpr>,
    schema: &Schema,
) -> Result<Arc<dyn PhysicalExpr>> {
    Ok(Arc::clone(expr)
        .transform_up(|node| {
            if let Some(cast) = node.downcast_ref::<CastExpr>()
                && let DataType::FixedSizeList(source, _) =
                    cast.expr().data_type(schema)?
                && let DataType::List(target) | DataType::LargeList(target) =
                    cast.cast_type()
                && source == *target
                && cast.target_nullable() != Some(false)
            {
                Ok(datafusion_common::tree_node::Transformed::yes(Arc::clone(
                    cast.expr(),
                )))
            } else {
                Ok(datafusion_common::tree_node::Transformed::no(node))
            }
        })?
        .data)
}

fn normalize_path(data_type: &DataType, path: &[Step]) -> Option<Vec<Step>> {
    let Some((first, rest)) = path.split_first() else {
        return Some(vec![]);
    };
    match (data_type, first) {
        (DataType::Struct(fields), Step::Field(name)) => {
            let (_, field) = fields.find(name)?;
            let mut result = vec![first.clone()];
            result.extend(normalize_path(field.data_type(), rest)?);
            Some(result)
        }
        (
            DataType::List(item)
            | DataType::LargeList(item)
            | DataType::FixedSizeList(item, _)
            | DataType::ListView(item)
            | DataType::LargeListView(item),
            _,
        ) => {
            let rest = if *first == Step::Element { rest } else { path };
            let mut result = vec![Step::Element];
            result.extend(normalize_path(item.data_type(), rest)?);
            Some(result)
        }
        (DataType::Map(entries, _), Step::Field(_) | Step::Value | Step::Element) => {
            let DataType::Struct(fields) = entries.data_type() else {
                return None;
            };
            let mut result = vec![Step::Value];
            result.extend(normalize_path(fields[1].data_type(), rest)?);
            Some(result)
        }
        _ => None,
    }
}

/// Match root fields by name between two schemas.
pub fn remap_top_level_field_indices(
    left: &SchemaRef,
    right: &SchemaRef,
) -> HashMap<usize, Option<usize>> {
    left.fields()
        .iter()
        .enumerate()
        .map(|(index, field)| {
            (
                index,
                right.fields().find(field.name()).map(|(index, _)| index),
            )
        })
        .collect()
}

/// Expand root indices and nested paths into root-qualified paths.
pub fn splat_columns(
    schema: &SchemaRef,
    projection: &[usize],
    deep: &HashMap<usize, Vec<String>>,
) -> Vec<String> {
    projection
        .iter()
        .flat_map(|&index| {
            let name = schema.field(index).name();
            match deep.get(&index).filter(|paths| !paths.is_empty()) {
                Some(paths) => {
                    paths.iter().map(|path| format!("{name}.{path}")).collect()
                }
                None => vec![name.clone()],
            }
        })
        .collect()
}

/// Resolve legacy nested path specifiers using the Arrow schema, retaining map keys.
#[expect(
    clippy::needless_pass_by_value,
    reason = "Preserve the original public API"
)]
pub fn parquet_leaf_paths(
    schema: SchemaRef,
    parquet_schema: &SchemaDescriptor,
    projection: &[usize],
    deep: &HashMap<usize, Vec<String>>,
) -> Vec<usize> {
    let indices = if projection.is_empty() {
        (0..schema.fields().len()).collect::<Vec<_>>()
    } else {
        projection.to_vec()
    };
    let types = indices
        .into_iter()
        .map(|root| {
            let field = schema.field(root);
            let mut access = Access::default();
            match deep.get(&root).filter(|paths| !paths.is_empty()) {
                Some(paths) => {
                    for path in paths {
                        let path = path
                            .split('.')
                            .map(|part| {
                                if part == "*" {
                                    Step::Element
                                } else {
                                    Step::Field(part.to_string())
                                }
                            })
                            .collect::<Vec<_>>();
                        match normalize_path(field.data_type(), &path) {
                            Some(path) => access.insert(&path),
                            None => access.whole = true,
                        }
                    }
                }
                None => access.whole = true,
            }
            (root, access.prune(field.data_type()))
        })
        .collect();
    let plan = read_plan_from_types(&types, &schema, parquet_schema)
        .expect("Arrow and Parquet schemas must agree for leaf projection");
    (0..parquet_schema.num_columns())
        .filter(|&index| plan.projection_mask.leaf_included(index))
        .collect()
}

/// Whether any root has a partial nested projection.
pub fn has_simplified_parquet_columns(deep: &HashMap<usize, Vec<String>>) -> bool {
    deep.values().any(|paths| !paths.is_empty())
}

/// Report nested read requirements using the original path-specifier interface.
/// The indices are advisory; paths are resolved by root name as in the original port.
#[expect(
    clippy::needless_pass_by_value,
    reason = "Preserve the original public API"
)]
pub fn projection_specifier(
    schema: SchemaRef,
    _projection: &ProjectionExprs,
    hints: Option<&ProjectionExprs>,
    _indices: &[usize],
) -> HashMap<usize, Vec<String>> {
    let mut roots: BTreeMap<usize, Access> = BTreeMap::new();
    for expr in hints.into_iter().flat_map(|hints| hints.expr_iter()) {
        for dependency in column_dependencies(&expr)
            .expect("Valid physical expressions must support dependency traversal")
        {
            let Some((column, path)) = field_access(&dependency) else {
                if let Some(cast) = dependency.downcast_ref::<CastExpr>()
                    && let Some(column) = cast.expr().downcast_ref::<Column>()
                    && let Ok(root) = schema.index_of(column.name())
                {
                    let access = roots.entry(root).or_default();
                    match clip_for_cast(schema.field(root).data_type(), cast.cast_type())
                    {
                        Some((_, clipped)) => access.insert_type(&clipped),
                        None => access.whole = true,
                    }
                }
                continue;
            };
            let Ok(root) = schema.index_of(column.name()) else {
                // Partition and virtual columns are absent from the file schema.
                continue;
            };
            let access = roots.entry(root).or_default();
            match normalize_path(schema.field(root).data_type(), &path) {
                Some(path) => access.insert(&path),
                None => access.whole = true,
            }
        }
    }
    fn paths(access: &Access, prefix: &str, out: &mut Vec<String>) {
        if access.whole {
            out.push(prefix.to_string());
        } else {
            for (step, child) in &access.children {
                let part = match step {
                    Step::Field(name) => name.as_str(),
                    Step::Element | Step::Value => "*",
                };
                let path = if prefix.is_empty() {
                    part.to_string()
                } else {
                    format!("{prefix}.{part}")
                };
                paths(child, &path, out);
            }
        }
    }
    roots
        .into_iter()
        .map(|(root, access)| {
            let mut out = vec![];
            if !access.whole {
                paths(&access, "", &mut out);
            }
            (root, out)
        })
        .collect()
}

pub(crate) fn build_deep_projection_read_plan(
    exprs: impl IntoIterator<Item = Arc<dyn PhysicalExpr>>,
    file_schema: &Schema,
    parquet_schema: &SchemaDescriptor,
) -> Result<ParquetReadPlan> {
    let mut roots: BTreeMap<usize, Access> = BTreeMap::new();
    for expr in exprs {
        for dependency in column_dependencies(&expr)? {
            if let Some((column, path)) = field_access(&dependency) {
                let root = file_schema.index_of(column.name())?;
                let access = roots.entry(root).or_default();
                match normalize_path(file_schema.field(root).data_type(), &path) {
                    Some(path) => access.insert(&path),
                    None => access.whole = true,
                }
            } else if let Some(cast) = dependency.downcast_ref::<CastExpr>()
                && let Some(column) = cast.expr().downcast_ref::<Column>()
            {
                let root = file_schema.index_of(column.name())?;
                let access = roots.entry(root).or_default();
                match clip_for_cast(file_schema.field(root).data_type(), cast.cast_type())
                {
                    Some((_, clipped)) => access.insert_type(&clipped),
                    None => access.whole = true,
                }
            }
        }
    }
    let types = roots
        .into_iter()
        .map(|(root, access)| {
            let field = file_schema.field(root);
            (root, access.prune(field.data_type()))
        })
        .collect();
    read_plan_from_types(&types, file_schema, parquet_schema)
}

fn intersect_types(base: &DataType, hinted: &DataType) -> DataType {
    match (base, hinted) {
        (DataType::Struct(base), DataType::Struct(hinted)) => DataType::Struct(
            base.iter()
                .filter_map(|field| {
                    hinted.find(field.name()).map(|(_, other)| {
                        field_with_type(
                            field,
                            intersect_types(field.data_type(), other.data_type()),
                        )
                    })
                })
                .collect(),
        ),
        (DataType::List(base), DataType::List(hinted)) => DataType::List(
            field_with_type(base, intersect_types(base.data_type(), hinted.data_type())),
        ),
        (DataType::LargeList(base), DataType::LargeList(hinted)) => DataType::LargeList(
            field_with_type(base, intersect_types(base.data_type(), hinted.data_type())),
        ),
        (DataType::FixedSizeList(base, size), DataType::FixedSizeList(hinted, _)) => {
            DataType::FixedSizeList(
                field_with_type(
                    base,
                    intersect_types(base.data_type(), hinted.data_type()),
                ),
                *size,
            )
        }
        (DataType::ListView(base), DataType::ListView(hinted)) => DataType::ListView(
            field_with_type(base, intersect_types(base.data_type(), hinted.data_type())),
        ),
        (DataType::LargeListView(base), DataType::LargeListView(hinted)) => {
            DataType::LargeListView(field_with_type(
                base,
                intersect_types(base.data_type(), hinted.data_type()),
            ))
        }
        (DataType::Map(base, sorted), DataType::Map(hinted, _)) => DataType::Map(
            field_with_type(base, intersect_types(base.data_type(), hinted.data_type())),
            *sorted,
        ),
        _ => base.clone(),
    }
}

fn select_leaves(
    physical: &DataType,
    target: &DataType,
    next: &mut usize,
    leaves: &mut Vec<usize>,
) {
    match (physical, target) {
        (DataType::Struct(fields), DataType::Struct(selected)) => {
            for field in fields {
                if let Some((_, other)) = selected.find(field.name()) {
                    select_leaves(field.data_type(), other.data_type(), next, leaves);
                } else {
                    *next += count_leaves(field.data_type());
                }
            }
        }
        (DataType::List(field), DataType::List(other))
        | (DataType::LargeList(field), DataType::LargeList(other))
        | (DataType::FixedSizeList(field, _), DataType::FixedSizeList(other, _))
        | (DataType::ListView(field), DataType::ListView(other))
        | (DataType::LargeListView(field), DataType::LargeListView(other))
        | (DataType::Map(field, _), DataType::Map(other, _)) => {
            select_leaves(field.data_type(), other.data_type(), next, leaves);
        }
        _ => {
            let count = count_leaves(physical);
            leaves.extend(*next..*next + count);
            *next += count;
        }
    }
}

fn read_plan_from_types(
    types: &BTreeMap<usize, DataType>,
    file_schema: &Schema,
    parquet_schema: &SchemaDescriptor,
) -> Result<ParquetReadPlan> {
    let mut leaves = vec![];
    let mut fields = vec![];
    let mut next = 0;
    for (index, field) in file_schema.fields().iter().enumerate() {
        if let Some(target) = types.get(&index) {
            select_leaves(field.data_type(), target, &mut next, &mut leaves);
            fields.push(field_with_type(field, target.clone()));
        } else {
            next += count_leaves(field.data_type());
        }
    }
    if next != parquet_schema.num_columns() {
        return internal_err!(
            "Deep projection leaf count {next} differs from Parquet schema leaf count {}",
            parquet_schema.num_columns()
        );
    }
    Ok(ParquetReadPlan {
        projection_mask: ProjectionMask::leaves(parquet_schema, leaves),
        projected_schema: Arc::new(Schema::new_with_metadata(
            fields,
            file_schema.metadata().clone(),
        )),
    })
}

/// Hints must not widen an existing projection, for example by adding a filter
/// column that the row filter already reads through its own decoder mask.
pub(crate) fn restrict_to_base_projection(
    base: &ParquetReadPlan,
    hinted: &ParquetReadPlan,
    file_schema: &Schema,
    parquet_schema: &SchemaDescriptor,
) -> Result<ParquetReadPlan> {
    let mut types = BTreeMap::new();
    for field in base.projected_schema.fields() {
        let root = file_schema.index_of(field.name())?;
        let target = match hinted.projected_schema.field_with_name(field.name()) {
            Ok(other) => intersect_types(field.data_type(), other.data_type()),
            Err(_) => field.data_type().clone(),
        };
        types.insert(root, target);
    }
    read_plan_from_types(&types, file_schema, parquet_schema)
}

/// Restore only the schema shape expected by existing operators. The optimizer
/// proves that missing fields are dead; retained arrays and ancestor validity
/// are unchanged. Required dead fields use defaults, not invalid null children.
pub(crate) fn restore_pruned_array(
    array: &ArrayRef,
    target: &DataType,
) -> Result<ArrayRef> {
    if array.data_type() == target {
        return Ok(Arc::clone(array));
    }
    match (array.data_type(), target) {
        (DataType::Struct(_), DataType::Struct(fields)) => {
            let source =
                array
                    .as_any()
                    .downcast_ref::<StructArray>()
                    .ok_or_else(|| {
                        datafusion_common::internal_datafusion_err!(
                            "Expected struct array"
                        )
                    })?;
            let children = fields
                .iter()
                .map(|field| match source.column_by_name(field.name()) {
                    Some(child) => restore_pruned_array(child, field.data_type()),
                    None => dead_field(field, source.len()),
                })
                .collect::<Result<Vec<_>>>()?;
            Ok(Arc::new(StructArray::try_new_with_length(
                fields.clone(),
                children,
                source.nulls().cloned(),
                source.len(),
            )?))
        }
        (DataType::List(_), DataType::List(item))
        | (DataType::LargeList(_), DataType::LargeList(item))
        | (DataType::FixedSizeList(_, _), DataType::FixedSizeList(item, _))
        | (DataType::ListView(_), DataType::ListView(item))
        | (DataType::LargeListView(_), DataType::LargeListView(item))
        | (DataType::Map(_, _), DataType::Map(item, _)) => {
            let data = array.to_data();
            let values = make_array(data.child_data()[0].clone());
            let values = restore_pruned_array(&values, item.data_type())?;
            Ok(make_array(
                data.into_builder()
                    .data_type(target.clone())
                    .child_data(vec![values.to_data()])
                    .build()?,
            ))
        }
        _ => internal_err!(
            "Cannot restore deep projection from {} to {target}",
            array.data_type()
        ),
    }
}

fn dead_field(field: &Field, len: usize) -> Result<ArrayRef> {
    if field.is_nullable() {
        return Ok(new_null_array(field.data_type(), len));
    }
    match field.data_type() {
        DataType::Struct(fields) => {
            let children = fields
                .iter()
                .map(|field| dead_field(field, len))
                .collect::<Result<Vec<_>>>()?;
            Ok(Arc::new(StructArray::try_new_with_length(
                fields.clone(),
                children,
                None,
                len,
            )?))
        }
        DataType::Map(_, _)
        | DataType::List(_)
        | DataType::LargeList(_)
        | DataType::ListView(_)
        | DataType::LargeListView(_) => {
            // Empty containers preserve their exact fields and metadata.
            let data = new_null_array(field.data_type(), len).to_data();
            Ok(make_array(data.into_builder().nulls(None).build()?))
        }
        DataType::FixedSizeList(item, size) => {
            let size = usize::try_from(*size).map_err(|_| {
                datafusion_common::internal_datafusion_err!(
                    "Negative fixed-size list length"
                )
            })?;
            let values_len = len.checked_mul(size).ok_or_else(|| {
                datafusion_common::internal_datafusion_err!(
                    "Fixed-size list default length overflow"
                )
            })?;
            let values = dead_field(item, values_len)?;
            let data = new_null_array(field.data_type(), len).to_data();
            Ok(make_array(
                data.into_builder()
                    .nulls(None)
                    .child_data(vec![values.to_data()])
                    .build()?,
            ))
        }
        _ => ScalarValue::new_default(field.data_type())?.to_array_of_size(len),
    }
}

pub(crate) fn restore_pruned_batch(
    batch: &RecordBatch,
    schema: &Arc<Schema>,
) -> Result<RecordBatch> {
    let arrays = schema
        .fields()
        .iter()
        .map(|field| match batch.column_by_name(field.name()) {
            Some(array) => restore_pruned_array(array, field.data_type()),
            None => dead_field(field, batch.num_rows()),
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(RecordBatch::try_new_with_options(
        Arc::clone(schema),
        arrays,
        &arrow::array::RecordBatchOptions::new().with_row_count(Some(batch.num_rows())),
    )?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, ListArray, StringArray};
    use arrow::buffer::{NullBuffer, OffsetBuffer};
    use arrow::datatypes::Field;

    #[test]
    fn restore_required_dead_fields_and_list_validity() -> Result<()> {
        let fields = vec![Field::new("x", DataType::Int64, false)].into();
        let source = StructArray::new(
            fields,
            vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
            Some(NullBuffer::from(vec![true, false, true])),
        );
        let item = Arc::new(Field::new("item", source.data_type().clone(), true));
        let list: ArrayRef = Arc::new(ListArray::new(
            item,
            OffsetBuffer::from_lengths([2, 1]),
            Arc::new(source),
            Some(NullBuffer::from(vec![true, false])),
        ));
        let target = DataType::List(Arc::new(Field::new(
            "item",
            DataType::Struct(
                vec![
                    Field::new("x", DataType::Int64, false),
                    Field::new("pad", DataType::Utf8, false),
                ]
                .into(),
            ),
            true,
        )));
        let restored = restore_pruned_array(&list, &target)?;
        assert_eq!(restored.data_type(), &target);
        let restored = restored.as_any().downcast_ref::<ListArray>().unwrap();
        let original = list.as_any().downcast_ref::<ListArray>().unwrap();
        assert_eq!(restored.offsets(), original.offsets());
        assert_eq!(restored.nulls(), original.nulls());
        let values = restored
            .values()
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        assert_eq!(
            values.nulls().unwrap(),
            &NullBuffer::from(vec![true, false, true])
        );
        let pad = values
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(pad.null_count(), 0);
        assert_eq!(pad.value(0), "");
        Ok(())
    }

    #[test]
    fn required_dead_maps_preserve_their_type() -> Result<()> {
        let entries = Field::new(
            "entries",
            DataType::Struct(
                vec![
                    Field::new("key", DataType::Utf8, false),
                    Field::new("value", DataType::Int64, false),
                ]
                .into(),
            ),
            false,
        );
        let field = Field::new("m", DataType::Map(Arc::new(entries), true), false);
        let array = dead_field(&field, 3)?;
        assert_eq!(array.data_type(), field.data_type());
        assert_eq!(array.len(), 3);
        assert_eq!(array.null_count(), 0);
        array.to_data().validate_full()?;
        Ok(())
    }

    #[test]
    fn required_dead_list_containers_preserve_fields_and_sizes() -> Result<()> {
        let item = Arc::new(Field::new("element", DataType::Int64, false).with_metadata(
            HashMap::from([("example".to_string(), "metadata".to_string())]),
        ));
        for data_type in [
            DataType::List(Arc::clone(&item)),
            DataType::LargeList(Arc::clone(&item)),
            DataType::FixedSizeList(Arc::clone(&item), 2),
            DataType::ListView(Arc::clone(&item)),
            DataType::LargeListView(item),
        ] {
            let field = Field::new("dead", data_type, false);
            for len in [0, 3] {
                let array = dead_field(&field, len)?;
                assert_eq!(array.data_type(), field.data_type());
                assert_eq!(array.len(), len);
                assert_eq!(array.null_count(), 0);
                array.to_data().validate_full()?;
            }
        }
        Ok(())
    }

    #[test]
    fn legacy_paths_prune_all_list_wrappers_and_keep_map_keys() -> Result<()> {
        use parquet::arrow::ArrowSchemaConverter;

        let value = DataType::Struct(
            vec![
                Field::new("x", DataType::Int64, false),
                Field::new("pad", DataType::Utf8, false),
            ]
            .into(),
        );
        let item = Arc::new(Field::new("element", value.clone(), true));
        let canonical = Schema::new(vec![Field::new(
            "events",
            DataType::List(Arc::clone(&item)),
            true,
        )]);
        let descriptor = ArrowSchemaConverter::new().convert(&canonical)?;
        for data_type in [
            DataType::List(Arc::clone(&item)),
            DataType::LargeList(Arc::clone(&item)),
            DataType::FixedSizeList(Arc::clone(&item), 2),
            DataType::ListView(Arc::clone(&item)),
            DataType::LargeListView(item),
        ] {
            let schema =
                Arc::new(Schema::new(vec![Field::new("events", data_type, true)]));
            assert_eq!(
                parquet_leaf_paths(
                    schema,
                    &descriptor,
                    &[0],
                    &HashMap::from([(0, vec!["*.x".to_string()])])
                ),
                vec![0]
            );
        }
        let entries = Arc::new(Field::new(
            "entries",
            DataType::Struct(
                vec![
                    Field::new("key", DataType::Utf8, false),
                    Field::new("value", value, true),
                ]
                .into(),
            ),
            false,
        ));
        let schema = Arc::new(Schema::new(vec![Field::new(
            "m",
            DataType::Map(entries, false),
            true,
        )]));
        let descriptor = ArrowSchemaConverter::new().convert(&schema)?;
        assert_eq!(
            parquet_leaf_paths(
                schema,
                &descriptor,
                &[0],
                &HashMap::from([(0, vec!["*.x".to_string()])])
            ),
            vec![0, 1]
        );
        Ok(())
    }
}
