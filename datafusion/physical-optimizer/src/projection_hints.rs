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

//! Collect nested leaf requirements without modifying the physical plan.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::datatypes::{DataType, Schema, SchemaRef};
use datafusion_common::tree_node::{
    Transformed, TransformedResult, TreeNode, TreeNodeRecursion,
};
use datafusion_common::{
    JoinSide, JoinType, Result, ScalarValue, internal_datafusion_err,
};
use datafusion_functions::core::getfield::GetFieldFunc;
use datafusion_functions_nested::extract::ArrayElement;
use datafusion_physical_expr::expressions::{CastExpr, Column, Literal};
use datafusion_physical_expr::projection::{ProjectionExpr, ProjectionExprs};
use datafusion_physical_expr::utils::collect_columns;
use datafusion_physical_expr::{PhysicalExpr, PhysicalExprRef, ScalarFunctionExpr};
use datafusion_physical_plan::ExecutionPlan;
use datafusion_physical_plan::aggregates::{AggregateExec, AggregateMode};
use datafusion_physical_plan::filter::FilterExec;
use datafusion_physical_plan::joins::utils::{JoinFilter, build_join_schema};
use datafusion_physical_plan::joins::{
    HashJoinExec, NestedLoopJoinExec, PiecewiseMergeJoinExec, SortMergeJoinExec,
    SymmetricHashJoinExec,
};
use datafusion_physical_plan::projection::ProjectionExec;
use datafusion_physical_plan::union::{InterleaveExec, UnionExec};
use datafusion_physical_plan::windows::{BoundedWindowAggExec, WindowAggExec};

/// Read requirements for one source occurrence.
#[derive(Debug)]
pub struct SourceProjectionHints {
    /// Expressions in the source's underlying schema.
    pub expressions: ProjectionExprs,
    /// Advisory scan-output positions; predicate-only roots can have no position.
    pub indices: Vec<usize>,
}

/// Hints keyed by child-index paths from the root, not displayed plans or pointers.
/// Identical sources on different sides of a self-join remain separate.
pub type ProjectionHints = HashMap<Vec<usize>, SourceProjectionHints>;

/// Supplies reader-specific boundaries and the underlying projection schema.
pub trait ProjectionHintsSourceAdapter {
    /// Stop traversal at this source, even if it has children.
    fn is_leaf_that_accepts_projection(&self, plan: &Arc<dyn ExecutionPlan>) -> bool;

    /// Map output positions to underlying expressions. `None` skips this source.
    fn projection(
        &self,
        plan: &Arc<dyn ExecutionPlan>,
    ) -> Result<Option<ProjectionExprs>>;

    /// Schema against which the source projection and filter are evaluated.
    fn schema(&self, plan: &Arc<dyn ExecutionPlan>) -> SchemaRef {
        plan.schema()
    }

    /// Additional predicate evaluated by the source.
    fn filter(&self, _plan: &Arc<dyn ExecutionPlan>) -> Option<Arc<dyn PhysicalExpr>> {
        None
    }
}

/// Collect requirements without changing the plan or attaching hints to readers.
pub fn collect_projection_hints(
    plan: &Arc<dyn ExecutionPlan>,
    adapter: &dyn ProjectionHintsSourceAdapter,
) -> Result<ProjectionHints> {
    let mut hints = HashMap::new();
    collect_node(
        plan,
        whole_columns(&plan.schema()),
        adapter,
        &mut vec![],
        &mut hints,
    )?;
    Ok(hints)
}

fn whole_columns(schema: &Schema) -> Vec<Arc<dyn PhysicalExpr>> {
    schema
        .fields()
        .iter()
        .enumerate()
        .map(|(index, field)| {
            Arc::new(Column::new(field.name(), index)) as Arc<dyn PhysicalExpr>
        })
        .collect()
}

fn rewrite_columns(
    expr: Arc<dyn PhysicalExpr>,
    mut replacement: impl FnMut(&Column) -> Result<Arc<dyn PhysicalExpr>>,
) -> Result<Arc<dyn PhysicalExpr>> {
    expr.transform_up(|expr| {
        if let Some(column) = expr.downcast_ref::<Column>() {
            Ok(Transformed::yes(replacement(column)?))
        } else {
            Ok(Transformed::no(expr))
        }
    })
    .data()
}

fn rebase(expr: Arc<dyn PhysicalExpr>, schema: &Schema) -> Result<Arc<dyn PhysicalExpr>> {
    rewrite_columns(expr, |column| {
        let field = schema.fields().get(column.index()).ok_or_else(|| {
            internal_datafusion_err!(
                "Invalid deep projection column index {}",
                column.index()
            )
        })?;
        Ok(Arc::new(Column::new(field.name(), column.index())))
    })
}

fn local_expressions(
    plan: &Arc<dyn ExecutionPlan>,
) -> Result<Vec<Arc<dyn PhysicalExpr>>> {
    let mut expressions = vec![];
    plan.apply_expressions(&mut |expr| {
        expressions.extend(column_dependencies(expr)?);
        Ok(TreeNodeRecursion::Continue)
    })?;
    Ok(expressions)
}

struct JoinInputs<'a> {
    join_type: JoinType,
    projection: Option<&'a [usize]>,
    on: &'a [(PhysicalExprRef, PhysicalExprRef)],
    filter: Option<&'a JoinFilter>,
}

fn join_inputs(plan: &Arc<dyn ExecutionPlan>) -> Option<JoinInputs<'_>> {
    if let Some(join) = plan.downcast_ref::<HashJoinExec>() {
        Some(JoinInputs {
            join_type: *join.join_type(),
            projection: join.projection.as_ref().map(|indices| &indices[..]),
            on: join.on(),
            filter: join.filter(),
        })
    } else if let Some(join) = plan.downcast_ref::<NestedLoopJoinExec>() {
        Some(JoinInputs {
            join_type: *join.join_type(),
            projection: join.projection().as_ref().map(|indices| &indices[..]),
            on: &[],
            filter: join.filter(),
        })
    } else if let Some(join) = plan.downcast_ref::<SortMergeJoinExec>() {
        Some(JoinInputs {
            join_type: join.join_type(),
            projection: None,
            on: join.on(),
            filter: join.filter().as_ref(),
        })
    } else if let Some(join) = plan.downcast_ref::<SymmetricHashJoinExec>() {
        Some(JoinInputs {
            join_type: *join.join_type(),
            projection: None,
            on: join.on(),
            filter: join.filter(),
        })
    } else {
        plan.downcast_ref::<PiecewiseMergeJoinExec>()
            .map(|join| JoinInputs {
                join_type: join.join_type(),
                projection: None,
                on: std::slice::from_ref(&join.on),
                filter: None,
            })
    }
}

fn collect_node(
    plan: &Arc<dyn ExecutionPlan>,
    required: Vec<Arc<dyn PhysicalExpr>>,
    adapter: &dyn ProjectionHintsSourceAdapter,
    path: &mut Vec<usize>,
    hints: &mut ProjectionHints,
) -> Result<()> {
    if adapter.is_leaf_that_accepts_projection(plan) {
        let Some(projection) = adapter.projection(plan)? else {
            return Ok(());
        };
        let schema = adapter.schema(plan);
        let mut expressions = required
            .into_iter()
            .map(|expr| {
                rewrite_columns(expr, |column| {
                    projection
                        .as_ref()
                        .get(column.index())
                        .map(|expr| Arc::clone(&expr.expr))
                        .ok_or_else(|| {
                            internal_datafusion_err!(
                                "Deep projection references missing scan output {}",
                                column.index()
                            )
                        })
                })
            })
            .collect::<Result<Vec<_>>>()?;
        // Computed outputs are still evaluated, even if an ancestor discards
        // them. Preserve their dependencies (and potential evaluation errors).
        for expr in projection.iter() {
            expressions.extend(computation_dependencies(&expr.expr, &schema)?);
        }
        expressions.extend(adapter.filter(plan));
        let expressions = expressions
            .iter()
            .map(|expr| column_dependencies(&strip_shape_casts(expr, &schema)?))
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();
        let indices = expressions
            .iter()
            .filter_map(|hint| {
                let columns = collect_columns(hint);
                projection.iter().position(|expr| {
                    collect_columns(&expr.expr)
                        .iter()
                        .any(|column| columns.contains(column))
                })
            })
            .collect();
        hints.insert(
            path.clone(),
            SourceProjectionHints {
                expressions: ProjectionExprs::new(
                    expressions
                        .into_iter()
                        .map(|expr| ProjectionExpr::new(expr, "")),
                ),
                indices,
            },
        );
        return Ok(());
    }

    let children = plan.children();
    if children.is_empty() {
        return Ok(());
    }
    let mut child_requirements = children
        .iter()
        .map(|child| whole_columns(&child.schema()))
        .collect::<Vec<_>>();

    if let Some(projection) = plan.downcast_ref::<ProjectionExec>() {
        let expressions = projection.expr();
        child_requirements[0] = required
            .into_iter()
            .map(|expr| {
                rewrite_columns(expr, |column| {
                    expressions
                        .get(column.index())
                        .map(|expr| Arc::clone(&expr.expr))
                        .ok_or_else(|| {
                            internal_datafusion_err!(
                                "Invalid projection output {}",
                                column.index()
                            )
                        })
                })
            })
            .collect::<Result<Vec<_>>>()?;
        for expr in expressions {
            child_requirements[0]
                .extend(computation_dependencies(&expr.expr, &children[0].schema())?);
        }
    } else if let Some(filter) = plan.downcast_ref::<FilterExec>() {
        let input_schema = children[0].schema();
        child_requirements[0] = required
            .into_iter()
            .map(|expr| {
                rewrite_columns(expr, |column| {
                    let index = match filter.projection() {
                        Some(indices) => {
                            *indices.get(column.index()).ok_or_else(|| {
                                internal_datafusion_err!(
                                    "Invalid filter projection index"
                                )
                            })?
                        }
                        None => column.index(),
                    };
                    let field = input_schema.fields().get(index).ok_or_else(|| {
                        internal_datafusion_err!("Invalid filter input index {index}")
                    })?;
                    Ok(Arc::new(Column::new(field.name(), index)))
                })
            })
            .collect::<Result<Vec<_>>>()?;
        child_requirements[0].extend(column_dependencies(filter.predicate())?);
    } else if plan.downcast_ref::<BoundedWindowAggExec>().is_some()
        || plan.downcast_ref::<WindowAggExec>().is_some()
    {
        let input_schema = children[0].schema();
        child_requirements[0].clear();
        for expr in required {
            for dependency in column_dependencies(&expr)? {
                if collect_columns(&dependency)
                    .iter()
                    .all(|column| column.index() < input_schema.fields().len())
                {
                    child_requirements[0].push(rebase(dependency, &input_schema)?);
                }
            }
        }
        child_requirements[0].extend(local_expressions(plan)?);
    } else if let Some(join) = join_inputs(plan) {
        let (_, mapping) = build_join_schema(
            &children[0].schema(),
            &children[1].schema(),
            &join.join_type,
        );
        child_requirements.iter_mut().for_each(Vec::clear);
        for expr in required {
            for dependency in column_dependencies(&expr)? {
                let columns = collect_columns(&dependency);
                if columns.len() != 1 {
                    return Err(internal_datafusion_err!(
                        "Expected a single deep projection root"
                    ));
                }
                let column = columns.iter().next().unwrap();
                let index = match join.projection {
                    Some(indices) => *indices.get(column.index()).ok_or_else(|| {
                        internal_datafusion_err!("Invalid join projection index")
                    })?,
                    None => column.index(),
                };
                let origin = mapping.get(index).ok_or_else(|| {
                    internal_datafusion_err!("Invalid join output index {index}")
                })?;
                let side = match origin.side {
                    JoinSide::Left => 0,
                    JoinSide::Right => 1,
                    JoinSide::None => continue,
                };
                let schema = children[side].schema();
                child_requirements[side].push(rewrite_columns(dependency, |_| {
                    Ok(Arc::new(Column::new(
                        schema.field(origin.index).name(),
                        origin.index,
                    )))
                })?);
            }
        }
        for (left, right) in join.on {
            child_requirements[0].extend(column_dependencies(left)?);
            child_requirements[1].extend(column_dependencies(right)?);
        }
        if let Some(filter) = join.filter {
            for dependency in column_dependencies(filter.expression())? {
                let columns = collect_columns(&dependency);
                let column = columns.iter().next().ok_or_else(|| {
                    internal_datafusion_err!("Join filter dependency has no column")
                })?;
                let origin =
                    filter.column_indices().get(column.index()).ok_or_else(|| {
                        internal_datafusion_err!("Invalid join filter column index")
                    })?;
                let side = match origin.side {
                    JoinSide::Left => 0,
                    JoinSide::Right => 1,
                    JoinSide::None => continue,
                };
                child_requirements[side].push(rewrite_columns(dependency, |_| {
                    Ok(Arc::new(Column::new(
                        children[side].schema().field(origin.index).name(),
                        origin.index,
                    )))
                })?);
            }
        }
        for expr in plan.dynamic_expressions_produced() {
            child_requirements[1].extend(column_dependencies(&expr)?);
        }
    } else if let Some(aggregate) = plan.downcast_ref::<AggregateExec>() {
        // Final aggregates consume accumulator states, not the original input
        // expressions retained in their aggregate definitions.
        if !matches!(
            aggregate.mode(),
            AggregateMode::Final
                | AggregateMode::FinalPartitioned
                | AggregateMode::PartialReduce
        ) {
            child_requirements[0] = local_expressions(plan)?;
        }
    } else if plan.downcast_ref::<UnionExec>().is_some()
        || plan.downcast_ref::<InterleaveExec>().is_some()
    {
        for (child, requirements) in children.iter().zip(&mut child_requirements) {
            *requirements = required
                .iter()
                .map(|expr| rebase(Arc::clone(expr), &child.schema()))
                .collect::<Result<Vec<_>>>()?;
        }
    } else if children.len() == 1 {
        // Preserve the legacy assumption that unrecognized unary nodes are
        // transparent, so external wrappers do not block nested pruning.
        child_requirements[0] = required
            .into_iter()
            .map(|expr| rebase(expr, &children[0].schema()))
            .collect::<Result<Vec<_>>>()?;
        child_requirements[0].extend(local_expressions(plan)?);
    }

    for (index, (child, required)) in
        children.into_iter().zip(child_requirements).enumerate()
    {
        path.push(index);
        collect_node(child, required, adapter, path, hints)?;
        path.pop();
    }
    Ok(())
}

fn is_selector(expr: &Arc<dyn PhysicalExpr>) -> bool {
    if expr.downcast_ref::<Column>().is_some() {
        return true;
    }
    if let Some(func) =
        ScalarFunctionExpr::try_downcast_func::<GetFieldFunc>(expr.as_ref())
    {
        return func.args().first().is_some_and(is_selector)
            && func.args()[1..]
                .iter()
                .all(|key| key.downcast_ref::<Literal>().is_some());
    }
    if let Some(func) =
        ScalarFunctionExpr::try_downcast_func::<ArrayElement>(expr.as_ref())
    {
        return func.args().first().is_some_and(is_selector);
    }
    false
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
                Ok(Transformed::yes(Arc::clone(cast.expr())))
            } else {
                Ok(Transformed::no(node))
            }
        })
        .expect("Valid expressions must support traversal");
    is_selector(&stripped.data)
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

/// Split arbitrary expressions into field accesses and whole-column dependencies.
/// Index expressions are independent dependencies, including non-literal indices.
pub fn column_dependencies(
    expr: &Arc<dyn PhysicalExpr>,
) -> Result<Vec<Arc<dyn PhysicalExpr>>> {
    let mut dependencies = Vec::new();
    expr.apply(|node| {
        if is_selector(node) {
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
                    Ok(Transformed::yes(access.with_new_children(children)?))
                } else {
                    Ok(Transformed::no(access))
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

/// Preserve computations and selector arguments that must still be evaluated.
fn computation_dependencies(
    expr: &Arc<dyn PhysicalExpr>,
    schema: &Schema,
) -> Result<Vec<Arc<dyn PhysicalExpr>>> {
    let hint = strip_shape_casts(expr, schema)?;
    if is_selector(&hint) {
        selector_argument_dependencies(&hint)
    } else {
        column_dependencies(&hint)
    }
}

/// Remove only infallible container casts whose element fields are unchanged.
fn strip_shape_casts(
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
                Ok(Transformed::yes(Arc::clone(cast.expr())))
            } else {
                Ok(Transformed::no(node))
            }
        })?
        .data)
}
