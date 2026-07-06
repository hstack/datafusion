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

//! Opt-in nested leaf requirements across physical operator boundaries.

use std::sync::Arc;

use arrow::datatypes::Schema;
use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::{
    Transformed, TransformedResult, TreeNode, TreeNodeRecursion,
};
use datafusion_common::{JoinSide, JoinType, Result, internal_datafusion_err};
use datafusion_datasource::file::FileSource;
use datafusion_datasource::file_scan_config::FileScanConfigBuilder;
use datafusion_datasource::source::DataSourceExec;
use datafusion_physical_expr::expressions::Column;
use datafusion_physical_expr::projection::{ProjectionExpr, ProjectionExprs};
use datafusion_physical_expr::utils::collect_columns;
use datafusion_physical_expr::{PhysicalExpr, PhysicalExprRef};
use datafusion_physical_optimizer::PhysicalOptimizerRule;
use datafusion_physical_plan::ExecutionPlan;
use datafusion_physical_plan::aggregates::{AggregateExec, AggregateMode};
#[expect(
    deprecated,
    reason = "Support plans that still contain the legacy coalescer"
)]
use datafusion_physical_plan::coalesce_batches::CoalesceBatchesExec;
use datafusion_physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion_physical_plan::filter::FilterExec;
use datafusion_physical_plan::joins::utils::{JoinFilter, build_join_schema};
use datafusion_physical_plan::joins::{
    HashJoinExec, NestedLoopJoinExec, PiecewiseMergeJoinExec, SortMergeJoinExec,
    SymmetricHashJoinExec,
};
use datafusion_physical_plan::limit::{GlobalLimitExec, LocalLimitExec};
use datafusion_physical_plan::projection::ProjectionExec;
use datafusion_physical_plan::repartition::RepartitionExec;
use datafusion_physical_plan::sorts::sort::SortExec;
use datafusion_physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
use datafusion_physical_plan::union::{InterleaveExec, UnionExec};
use datafusion_physical_plan::windows::{BoundedWindowAggExec, WindowAggExec};

use crate::leaves::{column_dependencies, computation_dependencies, strip_shape_casts};
use crate::source::ParquetSource;

/// Collect nested leaf requirements without moving expressions across operators.
///
/// Register this rule after the default physical optimizer rules. It preserves
/// scan output schemas, fills dead fields after decoding, and reads whole inputs
/// at unknown operator boundaries. Each scan occurrence is analyzed separately,
/// including identical scans on different sides of a self-join.
#[derive(Debug, Default)]
pub struct PushAllProjectionHints {}

impl PhysicalOptimizerRule for PushAllProjectionHints {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let required = whole_columns(&plan.schema());
        optimize_node(plan, required)
    }

    fn name(&self) -> &str {
        "LeafProjectionOptimizerRule"
    }

    fn schema_check(&self) -> bool {
        true
    }
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

#[expect(
    deprecated,
    reason = "Support plans that still contain the legacy coalescer"
)]
fn optimize_node(
    plan: Arc<dyn ExecutionPlan>,
    required: Vec<Arc<dyn PhysicalExpr>>,
) -> Result<Arc<dyn ExecutionPlan>> {
    if let Some(scan) = plan.downcast_ref::<DataSourceExec>()
        && let Some((config, source)) = scan.downcast_to_file_source::<ParquetSource>()
    {
        let projection = source.projection().ok_or_else(|| {
            internal_datafusion_err!("Parquet source is missing its projection")
        })?;
        let mut hints = required
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
            hints.extend(computation_dependencies(
                &expr.expr,
                source.table_schema.table_schema(),
            )?);
        }
        hints.extend(source.filter());
        let hints = hints
            .iter()
            .map(|expr| {
                column_dependencies(&strip_shape_casts(
                    expr,
                    source.table_schema.table_schema(),
                )?)
            })
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();
        // Retain the original advisory scan-output bookkeeping. Predicate-only
        // roots may have no output position; the read requirements stand alone.
        let indices = hints
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
        let mut source = source.clone();
        source.projection_hints = ProjectionExprs::new(
            hints.into_iter().map(|expr| ProjectionExpr::new(expr, "")),
        );
        source.projection_hints_indices = indices;
        let config = FileScanConfigBuilder::from(config.clone())
            .with_source(Arc::new(source))
            .build();
        return Ok(Arc::new(scan.clone().with_data_source(Arc::new(config))));
    }

    let children = plan.children();
    if children.is_empty() {
        return Ok(plan);
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
        child_requirements[0].extend(local_expressions(&plan)?);
    } else if let Some(join) = join_inputs(&plan) {
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
            child_requirements[0] = local_expressions(&plan)?;
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
    } else if plan.downcast_ref::<SortExec>().is_some()
        || plan.downcast_ref::<SortPreservingMergeExec>().is_some()
        || plan.downcast_ref::<RepartitionExec>().is_some()
        || plan.downcast_ref::<CoalesceBatchesExec>().is_some()
        || plan.downcast_ref::<CoalescePartitionsExec>().is_some()
        || plan.downcast_ref::<GlobalLimitExec>().is_some()
        || plan.downcast_ref::<LocalLimitExec>().is_some()
    {
        child_requirements[0] = required
            .into_iter()
            .map(|expr| rebase(expr, &children[0].schema()))
            .collect::<Result<Vec<_>>>()?;
        child_requirements[0].extend(local_expressions(&plan)?);
    }

    let mut requirements = child_requirements.into_iter();
    plan.map_children(|child| {
        let original = Arc::clone(&child);
        optimize_node(
            child,
            requirements.next().ok_or_else(|| {
                internal_datafusion_err!("Missing deep projection child requirements")
            })?,
        )
        .map(|new_child| {
            let changed = !Arc::ptr_eq(&original, &new_child);
            Transformed::new(new_child, changed, TreeNodeRecursion::Continue)
        })
    })
    .data()
}
