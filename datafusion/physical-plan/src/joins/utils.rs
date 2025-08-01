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

//! Join related functionality used both on logical and physical plans

use std::collections::HashSet;
use std::fmt::{self, Debug};
use std::future::Future;
use std::iter::once;
use std::ops::Range;
use std::sync::Arc;
use std::task::{Context, Poll};

use crate::metrics::{self, ExecutionPlanMetricsSet, MetricBuilder};
use crate::{
    ColumnStatistics, ExecutionPlan, ExecutionPlanProperties, Partitioning, Statistics,
};
// compatibility
pub use super::join_filter::JoinFilter;
pub use super::join_hash_map::{JoinHashMap, JoinHashMapType};
pub use crate::joins::{JoinOn, JoinOnRef};

use arrow::array::{
    builder::UInt64Builder, downcast_array, new_null_array, Array, ArrowPrimitiveType,
    BooleanBufferBuilder, NativeAdapter, PrimitiveArray, RecordBatch, RecordBatchOptions,
    UInt32Array, UInt32Builder, UInt64Array,
};
use arrow::compute;
use arrow::datatypes::{
    ArrowNativeType, Field, Schema, SchemaBuilder, UInt32Type, UInt64Type,
};
use arrow_schema::SchemaRef;
use datafusion_common::cast::as_boolean_array;
use datafusion_common::stats::Precision;
use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion_common::{
    plan_err, DataFusionError, JoinSide, JoinType, Result, SharedResult,
};
use datafusion_expr::interval_arithmetic::Interval;
use datafusion_physical_expr::equivalence::{add_offset_to_expr, ProjectionMapping};
use datafusion_physical_expr::expressions::Column;
use datafusion_physical_expr::utils::{collect_columns, merge_vectors};
use datafusion_physical_expr::{
    LexOrdering, PhysicalExpr, PhysicalExprRef, PhysicalSortExpr,
};

use crate::joins::SharedBitmapBuilder;
use crate::projection::ProjectionExec;
use futures::future::{BoxFuture, Shared};
use futures::{ready, FutureExt};
use parking_lot::Mutex;
use crate::common::can_project;

/// Checks whether the schemas "left" and "right" and columns "on" represent a valid join.
/// They are valid whenever their columns' intersection equals the set `on`
pub fn check_join_is_valid(left: &Schema, right: &Schema, on: JoinOnRef) -> Result<()> {
    let left: HashSet<Column> = left
        .fields()
        .iter()
        .enumerate()
        .map(|(idx, f)| Column::new(f.name(), idx))
        .collect();
    let right: HashSet<Column> = right
        .fields()
        .iter()
        .enumerate()
        .map(|(idx, f)| Column::new(f.name(), idx))
        .collect();

    check_join_set_is_valid(&left, &right, on)
}

/// Checks whether the sets left, right and on compose a valid join.
/// They are valid whenever their intersection equals the set `on`
fn check_join_set_is_valid(
    left: &HashSet<Column>,
    right: &HashSet<Column>,
    on: &[(PhysicalExprRef, PhysicalExprRef)],
) -> Result<()> {
    let on_left = &on
        .iter()
        .flat_map(|on| collect_columns(&on.0))
        .collect::<HashSet<_>>();
    let left_missing = on_left.difference(left).collect::<HashSet<_>>();

    let on_right = &on
        .iter()
        .flat_map(|on| collect_columns(&on.1))
        .collect::<HashSet<_>>();
    let right_missing = on_right.difference(right).collect::<HashSet<_>>();

    if !left_missing.is_empty() | !right_missing.is_empty() {
        return plan_err!(
            "The left or right side of the join does not have all columns on \"on\": \nMissing on the left: {left_missing:?}\nMissing on the right: {right_missing:?}"
        );
    };

    Ok(())
}

/// Adjust the right out partitioning to new Column Index
pub fn adjust_right_output_partitioning(
    right_partitioning: &Partitioning,
    left_columns_len: usize,
) -> Partitioning {
    match right_partitioning {
        Partitioning::Hash(exprs, size) => {
            let new_exprs = exprs
                .iter()
                .map(|expr| add_offset_to_expr(Arc::clone(expr), left_columns_len))
                .collect();
            Partitioning::Hash(new_exprs, *size)
        }
        result => result.clone(),
    }
}

/// Replaces the right column (first index in the `on_column` tuple) with
/// the left column (zeroth index in the tuple) inside `right_ordering`.
fn replace_on_columns_of_right_ordering(
    on_columns: &[(PhysicalExprRef, PhysicalExprRef)],
    right_ordering: &mut LexOrdering,
) -> Result<()> {
    for (left_col, right_col) in on_columns {
        right_ordering.transform(|item| {
            let new_expr = Arc::clone(&item.expr)
                .transform(|e| {
                    if e.eq(right_col) {
                        Ok(Transformed::yes(Arc::clone(left_col)))
                    } else {
                        Ok(Transformed::no(e))
                    }
                })
                .data()
                .expect("closure is infallible");
            item.expr = new_expr;
        });
    }
    Ok(())
}

fn offset_ordering(
    ordering: &LexOrdering,
    join_type: &JoinType,
    offset: usize,
) -> LexOrdering {
    match join_type {
        // In the case below, right ordering should be offsetted with the left
        // side length, since we append the right table to the left table.
        JoinType::Inner | JoinType::Left | JoinType::Full | JoinType::Right => ordering
            .iter()
            .map(|sort_expr| PhysicalSortExpr {
                expr: add_offset_to_expr(Arc::clone(&sort_expr.expr), offset),
                options: sort_expr.options,
            })
            .collect(),
        _ => ordering.clone(),
    }
}

/// Calculate the output ordering of a given join operation.
pub fn calculate_join_output_ordering(
    left_ordering: &LexOrdering,
    right_ordering: &LexOrdering,
    join_type: JoinType,
    on_columns: &[(PhysicalExprRef, PhysicalExprRef)],
    left_columns_len: usize,
    maintains_input_order: &[bool],
    probe_side: Option<JoinSide>,
) -> Option<LexOrdering> {
    let output_ordering = match maintains_input_order {
        [true, false] => {
            // Special case, we can prefix ordering of right side with the ordering of left side.
            if join_type == JoinType::Inner && probe_side == Some(JoinSide::Left) {
                replace_on_columns_of_right_ordering(
                    on_columns,
                    &mut right_ordering.clone(),
                )
                .ok()?;
                merge_vectors(
                    left_ordering,
                    offset_ordering(right_ordering, &join_type, left_columns_len)
                        .as_ref(),
                )
            } else {
                left_ordering.clone()
            }
        }
        [false, true] => {
            // Special case, we can prefix ordering of left side with the ordering of right side.
            if join_type == JoinType::Inner && probe_side == Some(JoinSide::Right) {
                replace_on_columns_of_right_ordering(
                    on_columns,
                    &mut right_ordering.clone(),
                )
                .ok()?;
                merge_vectors(
                    offset_ordering(right_ordering, &join_type, left_columns_len)
                        .as_ref(),
                    left_ordering,
                )
            } else {
                offset_ordering(right_ordering, &join_type, left_columns_len)
            }
        }
        // Doesn't maintain ordering, output ordering is None.
        [false, false] => return None,
        [true, true] => unreachable!("Cannot maintain ordering of both sides"),
        _ => unreachable!("Join operators can not have more than two children"),
    };
    (!output_ordering.is_empty()).then_some(output_ordering)
}

/// Information about the index and placement (left or right) of the columns
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnIndex {
    /// Index of the column
    pub index: usize,
    /// Whether the column is at the left or right side
    pub side: JoinSide,
}

/// Returns the output field given the input field. Outer joins may
/// insert nulls even if the input was not null
///
fn output_join_field(old_field: &Field, join_type: &JoinType, is_left: bool) -> Field {
    let force_nullable = match join_type {
        JoinType::Inner => false,
        JoinType::Left => !is_left, // right input is padded with nulls
        JoinType::Right => is_left, // left input is padded with nulls
        JoinType::Full => true,     // both inputs can be padded with nulls
        JoinType::LeftSemi => false, // doesn't introduce nulls
        JoinType::RightSemi => false, // doesn't introduce nulls
        JoinType::LeftAnti => false, // doesn't introduce nulls (or can it??)
        JoinType::RightAnti => false, // doesn't introduce nulls (or can it??)
        JoinType::LeftMark => false,
    };

    if force_nullable {
        old_field.clone().with_nullable(true)
    } else {
        old_field.clone()
    }
}

/// Creates a schema for a join operation.
/// The fields from the left side are first
pub fn build_join_schema(
    left: &Schema,
    right: &Schema,
    join_type: &JoinType,
) -> (Schema, Vec<ColumnIndex>) {
    let left_fields = || {
        left.fields()
            .iter()
            .map(|f| output_join_field(f, join_type, true))
            .enumerate()
            .map(|(index, f)| {
                (
                    f,
                    ColumnIndex {
                        index,
                        side: JoinSide::Left,
                    },
                )
            })
    };

    let right_fields = || {
        right
            .fields()
            .iter()
            .map(|f| output_join_field(f, join_type, false))
            .enumerate()
            .map(|(index, f)| {
                (
                    f,
                    ColumnIndex {
                        index,
                        side: JoinSide::Right,
                    },
                )
            })
    };

    let (fields, column_indices): (SchemaBuilder, Vec<ColumnIndex>) = match join_type {
        JoinType::Inner | JoinType::Left | JoinType::Full | JoinType::Right => {
            // left then right
            left_fields().chain(right_fields()).unzip()
        }
        JoinType::LeftSemi | JoinType::LeftAnti => left_fields().unzip(),
        JoinType::LeftMark => {
            let right_field = once((
                Field::new("mark", arrow::datatypes::DataType::Boolean, false),
                ColumnIndex {
                    index: 0,
                    side: JoinSide::None,
                },
            ));
            left_fields().chain(right_field).unzip()
        }
        JoinType::RightSemi | JoinType::RightAnti => right_fields().unzip(),
    };

    let (schema1, schema2) = match join_type {
        JoinType::Right | JoinType::RightSemi | JoinType::RightAnti => (left, right),
        _ => (right, left),
    };

    let metadata = schema1
        .metadata()
        .clone()
        .into_iter()
        .chain(schema2.metadata().clone())
        .collect();

    (fields.finish().with_metadata(metadata), column_indices)
}

/// This assumes that the projections are relative to the join schema.
/// We need to redo them to point to the actual hash join output schema
pub fn remap_join_projections_join_to_output(
    left: Arc<dyn ExecutionPlan>,
    right: Arc<dyn ExecutionPlan>,
    join_type: &JoinType,
    projection: Option<Vec<usize>>,
) -> Result<Option<Vec<usize>>> {
    match projection {
        Some(ref projection) => {
            let (join_schema, _) = build_join_schema(
                left.schema().as_ref(),
                right.schema().as_ref(),
                join_type
            );

            let join_schema = Arc::new(join_schema);
            can_project(&join_schema, Some(projection.clone()).as_ref())?;

            let projection_exprs = project_index_to_exprs(
                &projection.clone(),
                &join_schema
            );
            let projection_mapping =
                ProjectionMapping::try_new(&projection_exprs, &join_schema)?;

            // projection mapping contains from and to, get the second one
            let dest_physical_exprs = projection_mapping.map.iter().map(|(_, t)| t.clone()).collect::<Vec<_>>();
            let dest_columns = dest_physical_exprs.iter().map(|pe| pe.as_any().downcast_ref::<Column>()).collect::<Vec<_>>();
            let output = dest_physical_exprs.iter().enumerate().map(|(idx, _)| {
                // :Vec<(Arc<dyn PhysicalExpr>, String)>
                // (pe.clone(), dest_column.name().to_owned())
                let dest_column = dest_columns.get(idx).unwrap().unwrap();
                dest_column.index()
            }).collect::<Vec<_>>();
            Ok(Some(output))
        },
        None => Ok(None)
    }
}

pub fn project_index_to_exprs(
    projection_index: &[usize],
    schema: &SchemaRef,
) -> Vec<(Arc<dyn PhysicalExpr>, String)> {
    projection_index
        .iter()
        .map(|index| {
            let field = schema.field(*index);
            (
                Arc::new(Column::new(
                    field.name(),
                    *index,
                )) as Arc<dyn PhysicalExpr>,
                field.name().to_owned(),
            )
        })
        .collect::<Vec<_>>()
}

/// A [`OnceAsync`] runs an `async` closure once, where multiple calls to
/// [`OnceAsync::try_once`] return a [`OnceFut`] that resolves to the result of the
/// same computation.
///
/// This is useful for joins where the results of one child are needed to proceed
/// with multiple output stream
///
///
/// For example, in a hash join, one input is buffered and shared across
/// potentially multiple output partitions. Each output partition must wait for
/// the hash table to be built before proceeding.
///
/// Each output partition waits on the same `OnceAsync` before proceeding.
pub(crate) struct OnceAsync<T> {
    fut: Mutex<Option<SharedResult<OnceFut<T>>>>,
}

impl<T> Default for OnceAsync<T> {
    fn default() -> Self {
        Self {
            fut: Mutex::new(None),
        }
    }
}

impl<T> Debug for OnceAsync<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "OnceAsync")
    }
}

impl<T: 'static> OnceAsync<T> {
    /// If this is the first call to this function on this object, will invoke
    /// `f` to obtain a future and return a [`OnceFut`] referring to this. `f`
    /// may fail, in which case its error is returned.
    ///
    /// If this is not the first call, will return a [`OnceFut`] referring
    /// to the same future as was returned by the first call - or the same
    /// error if the initial call to `f` failed.
    pub(crate) fn try_once<F, Fut>(&self, f: F) -> Result<OnceFut<T>>
    where
        F: FnOnce() -> Result<Fut>,
        Fut: Future<Output = Result<T>> + Send + 'static,
    {
        self.fut
            .lock()
            .get_or_insert_with(|| f().map(OnceFut::new).map_err(Arc::new))
            .clone()
            .map_err(DataFusionError::Shared)
    }
}

/// The shared future type used internally within [`OnceAsync`]
type OnceFutPending<T> = Shared<BoxFuture<'static, SharedResult<Arc<T>>>>;

/// A [`OnceFut`] represents a shared asynchronous computation, that will be evaluated
/// once for all [`Clone`]'s, with [`OnceFut::get`] providing a non-consuming interface
/// to drive the underlying [`Future`] to completion
pub(crate) struct OnceFut<T> {
    state: OnceFutState<T>,
}

impl<T> Clone for OnceFut<T> {
    fn clone(&self) -> Self {
        Self {
            state: self.state.clone(),
        }
    }
}

/// A shared state between statistic aggregators for a join
/// operation.
#[derive(Clone, Debug, Default)]
struct PartialJoinStatistics {
    pub num_rows: usize,
    pub column_statistics: Vec<ColumnStatistics>,
}

/// Estimate the statistics for the given join's output.
pub(crate) fn estimate_join_statistics(
    left_stats: Statistics,
    right_stats: Statistics,
    on: JoinOn,
    join_type: &JoinType,
    schema: &Schema,
) -> Result<Statistics> {
    let join_stats = estimate_join_cardinality(join_type, left_stats, right_stats, &on);
    let (num_rows, column_statistics) = match join_stats {
        Some(stats) => (Precision::Inexact(stats.num_rows), stats.column_statistics),
        None => (Precision::Absent, Statistics::unknown_column(schema)),
    };
    Ok(Statistics {
        num_rows,
        total_byte_size: Precision::Absent,
        column_statistics,
    })
}

// Estimate the cardinality for the given join with input statistics.
fn estimate_join_cardinality(
    join_type: &JoinType,
    left_stats: Statistics,
    right_stats: Statistics,
    on: &JoinOn,
) -> Option<PartialJoinStatistics> {
    let (left_col_stats, right_col_stats) = on
        .iter()
        .map(|(left, right)| {
            match (
                left.as_any().downcast_ref::<Column>(),
                right.as_any().downcast_ref::<Column>(),
            ) {
                (Some(left), Some(right)) => (
                    left_stats.column_statistics[left.index()].clone(),
                    right_stats.column_statistics[right.index()].clone(),
                ),
                _ => (
                    ColumnStatistics::new_unknown(),
                    ColumnStatistics::new_unknown(),
                ),
            }
        })
        .unzip::<_, _, Vec<_>, Vec<_>>();

    match join_type {
        JoinType::Inner | JoinType::Left | JoinType::Right | JoinType::Full => {
            let ij_cardinality = estimate_inner_join_cardinality(
                Statistics {
                    num_rows: left_stats.num_rows,
                    total_byte_size: Precision::Absent,
                    column_statistics: left_col_stats,
                },
                Statistics {
                    num_rows: right_stats.num_rows,
                    total_byte_size: Precision::Absent,
                    column_statistics: right_col_stats,
                },
            )?;

            // The cardinality for inner join can also be used to estimate
            // the cardinality of left/right/full outer joins as long as it
            // it is greater than the minimum cardinality constraints of these
            // joins (so that we don't underestimate the cardinality).
            let cardinality = match join_type {
                JoinType::Inner => ij_cardinality,
                JoinType::Left => ij_cardinality.max(&left_stats.num_rows),
                JoinType::Right => ij_cardinality.max(&right_stats.num_rows),
                JoinType::Full => ij_cardinality
                    .max(&left_stats.num_rows)
                    .add(&ij_cardinality.max(&right_stats.num_rows))
                    .sub(&ij_cardinality),
                _ => unreachable!(),
            };

            Some(PartialJoinStatistics {
                num_rows: *cardinality.get_value()?,
                // We don't do anything specific here, just combine the existing
                // statistics which might yield subpar results (although it is
                // true, esp regarding min/max). For a better estimation, we need
                // filter selectivity analysis first.
                column_statistics: left_stats
                    .column_statistics
                    .into_iter()
                    .chain(right_stats.column_statistics)
                    .collect(),
            })
        }

        // For SemiJoins estimation result is either zero, in cases when inputs
        // are non-overlapping according to statistics, or equal to number of rows
        // for outer input
        JoinType::LeftSemi | JoinType::RightSemi => {
            let (outer_stats, inner_stats) = match join_type {
                JoinType::LeftSemi => (left_stats, right_stats),
                _ => (right_stats, left_stats),
            };
            let cardinality = match estimate_disjoint_inputs(&outer_stats, &inner_stats) {
                Some(estimation) => *estimation.get_value()?,
                None => *outer_stats.num_rows.get_value()?,
            };

            Some(PartialJoinStatistics {
                num_rows: cardinality,
                column_statistics: outer_stats.column_statistics,
            })
        }

        // For AntiJoins estimation always equals to outer statistics, as
        // non-overlapping inputs won't affect estimation
        JoinType::LeftAnti | JoinType::RightAnti => {
            let outer_stats = match join_type {
                JoinType::LeftAnti => left_stats,
                _ => right_stats,
            };

            Some(PartialJoinStatistics {
                num_rows: *outer_stats.num_rows.get_value()?,
                column_statistics: outer_stats.column_statistics,
            })
        }

        JoinType::LeftMark => {
            let num_rows = *left_stats.num_rows.get_value()?;
            let mut column_statistics = left_stats.column_statistics;
            column_statistics.push(ColumnStatistics::new_unknown());
            Some(PartialJoinStatistics {
                num_rows,
                column_statistics,
            })
        }
    }
}

/// Estimate the inner join cardinality by using the basic building blocks of
/// column-level statistics and the total row count. This is a very naive and
/// a very conservative implementation that can quickly give up if there is not
/// enough input statistics.
fn estimate_inner_join_cardinality(
    left_stats: Statistics,
    right_stats: Statistics,
) -> Option<Precision<usize>> {
    // Immediately return if inputs considered as non-overlapping
    if let Some(estimation) = estimate_disjoint_inputs(&left_stats, &right_stats) {
        return Some(estimation);
    };

    // The algorithm here is partly based on the non-histogram selectivity estimation
    // from Spark's Catalyst optimizer.
    let mut join_selectivity = Precision::Absent;
    for (left_stat, right_stat) in left_stats
        .column_statistics
        .iter()
        .zip(right_stats.column_statistics.iter())
    {
        // Break if any of statistics bounds are undefined
        if left_stat.min_value.get_value().is_none()
            || left_stat.max_value.get_value().is_none()
            || right_stat.min_value.get_value().is_none()
            || right_stat.max_value.get_value().is_none()
        {
            return None;
        }

        let left_max_distinct = max_distinct_count(&left_stats.num_rows, left_stat);
        let right_max_distinct = max_distinct_count(&right_stats.num_rows, right_stat);
        let max_distinct = left_max_distinct.max(&right_max_distinct);
        if max_distinct.get_value().is_some() {
            // Seems like there are a few implementations of this algorithm that implement
            // exponential decay for the selectivity (like Hive's Optiq Optimizer). Needs
            // further exploration.
            join_selectivity = max_distinct;
        }
    }

    // With the assumption that the smaller input's domain is generally represented in the bigger
    // input's domain, we can estimate the inner join's cardinality by taking the cartesian product
    // of the two inputs and normalizing it by the selectivity factor.
    let left_num_rows = left_stats.num_rows.get_value()?;
    let right_num_rows = right_stats.num_rows.get_value()?;
    match join_selectivity {
        Precision::Exact(value) if value > 0 => {
            Some(Precision::Exact((left_num_rows * right_num_rows) / value))
        }
        Precision::Inexact(value) if value > 0 => {
            Some(Precision::Inexact((left_num_rows * right_num_rows) / value))
        }
        // Since we don't have any information about the selectivity (which is derived
        // from the number of distinct rows information) we can give up here for now.
        // And let other passes handle this (otherwise we would need to produce an
        // overestimation using just the cartesian product).
        _ => None,
    }
}

/// Estimates if inputs are non-overlapping, using input statistics.
/// If inputs are disjoint, returns zero estimation, otherwise returns None
fn estimate_disjoint_inputs(
    left_stats: &Statistics,
    right_stats: &Statistics,
) -> Option<Precision<usize>> {
    for (left_stat, right_stat) in left_stats
        .column_statistics
        .iter()
        .zip(right_stats.column_statistics.iter())
    {
        // If there is no overlap in any of the join columns, this means the join
        // itself is disjoint and the cardinality is 0. Though we can only assume
        // this when the statistics are exact (since it is a very strong assumption).
        let left_min_val = left_stat.min_value.get_value();
        let right_max_val = right_stat.max_value.get_value();
        if left_min_val.is_some()
            && right_max_val.is_some()
            && left_min_val > right_max_val
        {
            return Some(
                if left_stat.min_value.is_exact().unwrap_or(false)
                    && right_stat.max_value.is_exact().unwrap_or(false)
                {
                    Precision::Exact(0)
                } else {
                    Precision::Inexact(0)
                },
            );
        }

        let left_max_val = left_stat.max_value.get_value();
        let right_min_val = right_stat.min_value.get_value();
        if left_max_val.is_some()
            && right_min_val.is_some()
            && left_max_val < right_min_val
        {
            return Some(
                if left_stat.max_value.is_exact().unwrap_or(false)
                    && right_stat.min_value.is_exact().unwrap_or(false)
                {
                    Precision::Exact(0)
                } else {
                    Precision::Inexact(0)
                },
            );
        }
    }

    None
}

/// Estimate the number of maximum distinct values that can be present in the
/// given column from its statistics. If distinct_count is available, uses it
/// directly. Otherwise, if the column is numeric and has min/max values, it
/// estimates the maximum distinct count from those.
fn max_distinct_count(
    num_rows: &Precision<usize>,
    stats: &ColumnStatistics,
) -> Precision<usize> {
    match &stats.distinct_count {
        &dc @ (Precision::Exact(_) | Precision::Inexact(_)) => dc,
        _ => {
            // The number can never be greater than the number of rows we have
            // minus the nulls (since they don't count as distinct values).
            let result = match num_rows {
                Precision::Absent => Precision::Absent,
                Precision::Inexact(count) => {
                    // To safeguard against inexact number of rows (e.g. 0) being smaller than
                    // an exact null count we need to do a checked subtraction.
                    match count.checked_sub(*stats.null_count.get_value().unwrap_or(&0)) {
                        None => Precision::Inexact(0),
                        Some(non_null_count) => Precision::Inexact(non_null_count),
                    }
                }
                Precision::Exact(count) => {
                    let count = count - stats.null_count.get_value().unwrap_or(&0);
                    if stats.null_count.is_exact().unwrap_or(false) {
                        Precision::Exact(count)
                    } else {
                        Precision::Inexact(count)
                    }
                }
            };
            // Cap the estimate using the number of possible values:
            if let (Some(min), Some(max)) =
                (stats.min_value.get_value(), stats.max_value.get_value())
            {
                if let Some(range_dc) = Interval::try_new(min.clone(), max.clone())
                    .ok()
                    .and_then(|e| e.cardinality())
                {
                    let range_dc = range_dc as usize;
                    // Note that the `unwrap` calls in the below statement are safe.
                    return if matches!(result, Precision::Absent)
                        || &range_dc < result.get_value().unwrap()
                    {
                        if stats.min_value.is_exact().unwrap()
                            && stats.max_value.is_exact().unwrap()
                        {
                            Precision::Exact(range_dc)
                        } else {
                            Precision::Inexact(range_dc)
                        }
                    } else {
                        result
                    };
                }
            }

            result
        }
    }
}

enum OnceFutState<T> {
    Pending(OnceFutPending<T>),
    Ready(SharedResult<Arc<T>>),
}

impl<T> Clone for OnceFutState<T> {
    fn clone(&self) -> Self {
        match self {
            Self::Pending(p) => Self::Pending(p.clone()),
            Self::Ready(r) => Self::Ready(r.clone()),
        }
    }
}

impl<T: 'static> OnceFut<T> {
    /// Create a new [`OnceFut`] from a [`Future`]
    pub(crate) fn new<Fut>(fut: Fut) -> Self
    where
        Fut: Future<Output = Result<T>> + Send + 'static,
    {
        Self {
            state: OnceFutState::Pending(
                fut.map(|res| res.map(Arc::new).map_err(Arc::new))
                    .boxed()
                    .shared(),
            ),
        }
    }

    /// Get the result of the computation if it is ready, without consuming it
    pub(crate) fn get(&mut self, cx: &mut Context<'_>) -> Poll<Result<&T>> {
        if let OnceFutState::Pending(fut) = &mut self.state {
            let r = ready!(fut.poll_unpin(cx));
            self.state = OnceFutState::Ready(r);
        }

        // Cannot use loop as this would trip up the borrow checker
        match &self.state {
            OnceFutState::Pending(_) => unreachable!(),
            OnceFutState::Ready(r) => Poll::Ready(
                r.as_ref()
                    .map(|r| r.as_ref())
                    .map_err(DataFusionError::from),
            ),
        }
    }

    /// Get shared reference to the result of the computation if it is ready, without consuming it
    pub(crate) fn get_shared(&mut self, cx: &mut Context<'_>) -> Poll<Result<Arc<T>>> {
        if let OnceFutState::Pending(fut) = &mut self.state {
            let r = ready!(fut.poll_unpin(cx));
            self.state = OnceFutState::Ready(r);
        }

        match &self.state {
            OnceFutState::Pending(_) => unreachable!(),
            OnceFutState::Ready(r) => {
                Poll::Ready(r.clone().map_err(DataFusionError::Shared))
            }
        }
    }
}

/// Some type `join_type` of join need to maintain the matched indices bit map for the left side, and
/// use the bit map to generate the part of result of the join.
///
/// For example of the `Left` join, in each iteration of right side, can get the matched result, but need
/// to maintain the matched indices bit map to get the unmatched row for the left side.
pub(crate) fn need_produce_result_in_final(join_type: JoinType) -> bool {
    matches!(
        join_type,
        JoinType::Left
            | JoinType::LeftAnti
            | JoinType::LeftSemi
            | JoinType::LeftMark
            | JoinType::Full
    )
}

pub(crate) fn get_final_indices_from_shared_bitmap(
    shared_bitmap: &SharedBitmapBuilder,
    join_type: JoinType,
) -> (UInt64Array, UInt32Array) {
    let bitmap = shared_bitmap.lock();
    get_final_indices_from_bit_map(&bitmap, join_type)
}

/// In the end of join execution, need to use bit map of the matched
/// indices to generate the final left and right indices.
///
/// For example:
///
/// 1. left_bit_map: `[true, false, true, true, false]`
/// 2. join_type: `Left`
///
/// The result is: `([1,4], [null, null])`
pub(crate) fn get_final_indices_from_bit_map(
    left_bit_map: &BooleanBufferBuilder,
    join_type: JoinType,
) -> (UInt64Array, UInt32Array) {
    let left_size = left_bit_map.len();
    if join_type == JoinType::LeftMark {
        let left_indices = (0..left_size as u64).collect::<UInt64Array>();
        let right_indices = (0..left_size)
            .map(|idx| left_bit_map.get_bit(idx).then_some(0))
            .collect::<UInt32Array>();
        return (left_indices, right_indices);
    }
    let left_indices = if join_type == JoinType::LeftSemi {
        (0..left_size)
            .filter_map(|idx| (left_bit_map.get_bit(idx)).then_some(idx as u64))
            .collect::<UInt64Array>()
    } else {
        // just for `Left`, `LeftAnti` and `Full` join
        // `LeftAnti`, `Left` and `Full` will produce the unmatched left row finally
        (0..left_size)
            .filter_map(|idx| (!left_bit_map.get_bit(idx)).then_some(idx as u64))
            .collect::<UInt64Array>()
    };
    // right_indices
    // all the element in the right side is None
    let mut builder = UInt32Builder::with_capacity(left_indices.len());
    builder.append_nulls(left_indices.len());
    let right_indices = builder.finish();
    (left_indices, right_indices)
}

pub(crate) fn apply_join_filter_to_indices(
    build_input_buffer: &RecordBatch,
    probe_batch: &RecordBatch,
    build_indices: UInt64Array,
    probe_indices: UInt32Array,
    filter: &JoinFilter,
    build_side: JoinSide,
) -> Result<(UInt64Array, UInt32Array)> {
    if build_indices.is_empty() && probe_indices.is_empty() {
        return Ok((build_indices, probe_indices));
    };

    let intermediate_batch = build_batch_from_indices(
        filter.schema(),
        build_input_buffer,
        probe_batch,
        &build_indices,
        &probe_indices,
        filter.column_indices(),
        build_side,
    )?;
    let filter_result = filter
        .expression()
        .evaluate(&intermediate_batch)?
        .into_array(intermediate_batch.num_rows())?;
    let mask = as_boolean_array(&filter_result)?;

    let left_filtered = compute::filter(&build_indices, mask)?;
    let right_filtered = compute::filter(&probe_indices, mask)?;
    Ok((
        downcast_array(left_filtered.as_ref()),
        downcast_array(right_filtered.as_ref()),
    ))
}

/// Returns a new [RecordBatch] by combining the `left` and `right` according to `indices`.
/// The resulting batch has [Schema] `schema`.
pub(crate) fn build_batch_from_indices(
    schema: &Schema,
    build_input_buffer: &RecordBatch,
    probe_batch: &RecordBatch,
    build_indices: &UInt64Array,
    probe_indices: &UInt32Array,
    column_indices: &[ColumnIndex],
    build_side: JoinSide,
) -> Result<RecordBatch> {
    if schema.fields().is_empty() {
        let options = RecordBatchOptions::new()
            .with_match_field_names(true)
            .with_row_count(Some(build_indices.len()));

        return Ok(RecordBatch::try_new_with_options(
            Arc::new(schema.clone()),
            vec![],
            &options,
        )?);
    }

    // build the columns of the new [RecordBatch]:
    // 1. pick whether the column is from the left or right
    // 2. based on the pick, `take` items from the different RecordBatches
    let mut columns: Vec<Arc<dyn Array>> = Vec::with_capacity(schema.fields().len());

    for column_index in column_indices {
        let array = if column_index.side == JoinSide::None {
            // LeftMark join, the mark column is a true if the indices is not null, otherwise it will be false
            Arc::new(compute::is_not_null(probe_indices)?)
        } else if column_index.side == build_side {
            let array = build_input_buffer.column(column_index.index);
            if array.is_empty() || build_indices.null_count() == build_indices.len() {
                // Outer join would generate a null index when finding no match at our side.
                // Therefore, it's possible we are empty but need to populate an n-length null array,
                // where n is the length of the index array.
                assert_eq!(build_indices.null_count(), build_indices.len());
                new_null_array(array.data_type(), build_indices.len())
            } else {
                compute::take(array.as_ref(), build_indices, None)?
            }
        } else {
            let array = probe_batch.column(column_index.index);
            if array.is_empty() || probe_indices.null_count() == probe_indices.len() {
                assert_eq!(probe_indices.null_count(), probe_indices.len());
                new_null_array(array.data_type(), probe_indices.len())
            } else {
                compute::take(array.as_ref(), probe_indices, None)?
            }
        };
        columns.push(array);
    }
    Ok(RecordBatch::try_new(Arc::new(schema.clone()), columns)?)
}

/// The input is the matched indices for left and right and
/// adjust the indices according to the join type
pub(crate) fn adjust_indices_by_join_type(
    left_indices: UInt64Array,
    right_indices: UInt32Array,
    adjust_range: Range<usize>,
    join_type: JoinType,
    preserve_order_for_right: bool,
) -> Result<(UInt64Array, UInt32Array)> {
    match join_type {
        JoinType::Inner => {
            // matched
            Ok((left_indices, right_indices))
        }
        JoinType::Left => {
            // matched
            Ok((left_indices, right_indices))
            // unmatched left row will be produced in the end of loop, and it has been set in the left visited bitmap
        }
        JoinType::Right => {
            // combine the matched and unmatched right result together
            append_right_indices(
                left_indices,
                right_indices,
                adjust_range,
                preserve_order_for_right,
            )
        }
        JoinType::Full => {
            append_right_indices(left_indices, right_indices, adjust_range, false)
        }
        JoinType::RightSemi => {
            // need to remove the duplicated record in the right side
            let right_indices = get_semi_indices(adjust_range, &right_indices);
            // the left_indices will not be used later for the `right semi` join
            Ok((left_indices, right_indices))
        }
        JoinType::RightAnti => {
            // need to remove the duplicated record in the right side
            // get the anti index for the right side
            let right_indices = get_anti_indices(adjust_range, &right_indices);
            // the left_indices will not be used later for the `right anti` join
            Ok((left_indices, right_indices))
        }
        JoinType::LeftSemi | JoinType::LeftAnti | JoinType::LeftMark => {
            // matched or unmatched left row will be produced in the end of loop
            // When visit the right batch, we can output the matched left row and don't need to wait the end of loop
            Ok((
                UInt64Array::from_iter_values(vec![]),
                UInt32Array::from_iter_values(vec![]),
            ))
        }
    }
}

/// Appends right indices to left indices based on the specified order mode.
///
/// The function operates in two modes:
/// 1. If `preserve_order_for_right` is true, probe matched and unmatched indices
///    are inserted in order using the `append_probe_indices_in_order()` method.
/// 2. Otherwise, unmatched probe indices are simply appended after matched ones.
///
/// # Parameters
/// - `left_indices`: UInt64Array of left indices.
/// - `right_indices`: UInt32Array of right indices.
/// - `adjust_range`: Range to adjust the right indices.
/// - `preserve_order_for_right`: Boolean flag to determine the mode of operation.
///
/// # Returns
/// A tuple of updated `UInt64Array` and `UInt32Array`.
pub(crate) fn append_right_indices(
    left_indices: UInt64Array,
    right_indices: UInt32Array,
    adjust_range: Range<usize>,
    preserve_order_for_right: bool,
) -> Result<(UInt64Array, UInt32Array)> {
    if preserve_order_for_right {
        Ok(append_probe_indices_in_order(
            left_indices,
            right_indices,
            adjust_range,
        ))
    } else {
        let right_unmatched_indices = get_anti_indices(adjust_range, &right_indices);

        if right_unmatched_indices.is_empty() {
            Ok((left_indices, right_indices))
        } else {
            // `into_builder()` can fail here when there is nothing to be filtered and
            // left_indices or right_indices has the same reference to the cached indices.
            // In that case, we use a slower alternative.

            // the new left indices: left_indices + null array
            let mut new_left_indices_builder =
                left_indices.into_builder().unwrap_or_else(|left_indices| {
                    let mut builder = UInt64Builder::with_capacity(
                        left_indices.len() + right_unmatched_indices.len(),
                    );
                    debug_assert_eq!(
                        left_indices.null_count(),
                        0,
                        "expected left indices to have no nulls"
                    );
                    builder.append_slice(left_indices.values());
                    builder
                });
            new_left_indices_builder.append_nulls(right_unmatched_indices.len());
            let new_left_indices = UInt64Array::from(new_left_indices_builder.finish());

            // the new right indices: right_indices + right_unmatched_indices
            let mut new_right_indices_builder = right_indices
                .into_builder()
                .unwrap_or_else(|right_indices| {
                    let mut builder = UInt32Builder::with_capacity(
                        right_indices.len() + right_unmatched_indices.len(),
                    );
                    debug_assert_eq!(
                        right_indices.null_count(),
                        0,
                        "expected right indices to have no nulls"
                    );
                    builder.append_slice(right_indices.values());
                    builder
                });
            debug_assert_eq!(
                right_unmatched_indices.null_count(),
                0,
                "expected right unmatched indices to have no nulls"
            );
            new_right_indices_builder.append_slice(right_unmatched_indices.values());
            let new_right_indices = UInt32Array::from(new_right_indices_builder.finish());

            Ok((new_left_indices, new_right_indices))
        }
    }
}

/// Returns `range` indices which are not present in `input_indices`
pub(crate) fn get_anti_indices<T: ArrowPrimitiveType>(
    range: Range<usize>,
    input_indices: &PrimitiveArray<T>,
) -> PrimitiveArray<T>
where
    NativeAdapter<T>: From<<T as ArrowPrimitiveType>::Native>,
{
    let mut bitmap = BooleanBufferBuilder::new(range.len());
    bitmap.append_n(range.len(), false);
    input_indices
        .iter()
        .flatten()
        .map(|v| v.as_usize())
        .filter(|v| range.contains(v))
        .for_each(|v| {
            bitmap.set_bit(v - range.start, true);
        });

    let offset = range.start;

    // get the anti index
    (range)
        .filter_map(|idx| {
            (!bitmap.get_bit(idx - offset)).then_some(T::Native::from_usize(idx))
        })
        .collect()
}

/// Returns intersection of `range` and `input_indices` omitting duplicates
pub(crate) fn get_semi_indices<T: ArrowPrimitiveType>(
    range: Range<usize>,
    input_indices: &PrimitiveArray<T>,
) -> PrimitiveArray<T>
where
    NativeAdapter<T>: From<<T as ArrowPrimitiveType>::Native>,
{
    let mut bitmap = BooleanBufferBuilder::new(range.len());
    bitmap.append_n(range.len(), false);
    input_indices
        .iter()
        .flatten()
        .map(|v| v.as_usize())
        .filter(|v| range.contains(v))
        .for_each(|v| {
            bitmap.set_bit(v - range.start, true);
        });

    let offset = range.start;

    // get the semi index
    (range)
        .filter_map(|idx| {
            (bitmap.get_bit(idx - offset)).then_some(T::Native::from_usize(idx))
        })
        .collect()
}

/// Appends probe indices in order by considering the given build indices.
///
/// This function constructs new build and probe indices by iterating through
/// the provided indices, and appends any missing values between previous and
/// current probe index with a corresponding null build index.
///
/// # Parameters
///
/// - `build_indices`: `PrimitiveArray` of `UInt64Type` containing build indices.
/// - `probe_indices`: `PrimitiveArray` of `UInt32Type` containing probe indices.
/// - `range`: The range of indices to consider.
///
/// # Returns
///
/// A tuple of two arrays:
/// - A `PrimitiveArray` of `UInt64Type` with the newly constructed build indices.
/// - A `PrimitiveArray` of `UInt32Type` with the newly constructed probe indices.
fn append_probe_indices_in_order(
    build_indices: PrimitiveArray<UInt64Type>,
    probe_indices: PrimitiveArray<UInt32Type>,
    range: Range<usize>,
) -> (PrimitiveArray<UInt64Type>, PrimitiveArray<UInt32Type>) {
    // Builders for new indices:
    let mut new_build_indices = UInt64Builder::new();
    let mut new_probe_indices = UInt32Builder::new();
    // Set previous index as the start index for the initial loop:
    let mut prev_index = range.start as u32;
    // Zip the two iterators.
    debug_assert!(build_indices.len() == probe_indices.len());
    for (build_index, probe_index) in build_indices
        .values()
        .into_iter()
        .zip(probe_indices.values().into_iter())
    {
        // Append values between previous and current probe index with null build index:
        for value in prev_index..*probe_index {
            new_probe_indices.append_value(value);
            new_build_indices.append_null();
        }
        // Append current indices:
        new_probe_indices.append_value(*probe_index);
        new_build_indices.append_value(*build_index);
        // Set current probe index as previous for the next iteration:
        prev_index = probe_index + 1;
    }
    // Append remaining probe indices after the last valid probe index with null build index.
    for value in prev_index..range.end as u32 {
        new_probe_indices.append_value(value);
        new_build_indices.append_null();
    }
    // Build arrays and return:
    (new_build_indices.finish(), new_probe_indices.finish())
}

/// Metrics for build & probe joins
#[derive(Clone, Debug)]
pub(crate) struct BuildProbeJoinMetrics {
    /// Total time for collecting build-side of join
    pub(crate) build_time: metrics::Time,
    /// Number of batches consumed by build-side
    pub(crate) build_input_batches: metrics::Count,
    /// Number of rows consumed by build-side
    pub(crate) build_input_rows: metrics::Count,
    /// Memory used by build-side in bytes
    pub(crate) build_mem_used: metrics::Gauge,
    /// Total time for joining probe-side batches to the build-side batches
    pub(crate) join_time: metrics::Time,
    /// Number of batches consumed by probe-side of this operator
    pub(crate) input_batches: metrics::Count,
    /// Number of rows consumed by probe-side this operator
    pub(crate) input_rows: metrics::Count,
    /// Number of batches produced by this operator
    pub(crate) output_batches: metrics::Count,
    /// Number of rows produced by this operator
    pub(crate) output_rows: metrics::Count,
}

impl BuildProbeJoinMetrics {
    pub fn new(partition: usize, metrics: &ExecutionPlanMetricsSet) -> Self {
        let join_time = MetricBuilder::new(metrics).subset_time("join_time", partition);

        let build_time = MetricBuilder::new(metrics).subset_time("build_time", partition);

        let build_input_batches =
            MetricBuilder::new(metrics).counter("build_input_batches", partition);

        let build_input_rows =
            MetricBuilder::new(metrics).counter("build_input_rows", partition);

        let build_mem_used =
            MetricBuilder::new(metrics).gauge("build_mem_used", partition);

        let input_batches =
            MetricBuilder::new(metrics).counter("input_batches", partition);

        let input_rows = MetricBuilder::new(metrics).counter("input_rows", partition);

        let output_batches =
            MetricBuilder::new(metrics).counter("output_batches", partition);

        let output_rows = MetricBuilder::new(metrics).output_rows(partition);

        Self {
            build_time,
            build_input_batches,
            build_input_rows,
            build_mem_used,
            join_time,
            input_batches,
            input_rows,
            output_batches,
            output_rows,
        }
    }
}

/// The `handle_state` macro is designed to process the result of a state-changing
/// operation. It operates on a `StatefulStreamResult` by matching its variants and
/// executing corresponding actions. This macro is used to streamline code that deals
/// with state transitions, reducing boilerplate and improving readability.
///
/// # Cases
///
/// - `Ok(StatefulStreamResult::Continue)`: Continues the loop, indicating the
///   stream join operation should proceed to the next step.
/// - `Ok(StatefulStreamResult::Ready(result))`: Returns a `Poll::Ready` with the
///   result, either yielding a value or indicating the stream is awaiting more
///   data.
/// - `Err(e)`: Returns a `Poll::Ready` containing an error, signaling an issue
///   during the stream join operation.
///
/// # Arguments
///
/// * `$match_case`: An expression that evaluates to a `Result<StatefulStreamResult<_>>`.
#[macro_export]
macro_rules! handle_state {
    ($match_case:expr) => {
        match $match_case {
            Ok(StatefulStreamResult::Continue) => continue,
            Ok(StatefulStreamResult::Ready(result)) => {
                Poll::Ready(Ok(result).transpose())
            }
            Err(e) => Poll::Ready(Some(Err(e))),
        }
    };
}

/// Represents the result of a stateful operation.
///
/// This enumeration indicates whether the state produced a result that is
/// ready for use (`Ready`) or if the operation requires continuation (`Continue`).
///
/// Variants:
/// - `Ready(T)`: Indicates that the operation is complete with a result of type `T`.
/// - `Continue`: Indicates that the operation is not yet complete and requires further
///   processing or more data. When this variant is returned, it typically means that the
///   current invocation of the state did not produce a final result, and the operation
///   should be invoked again later with more data and possibly with a different state.
pub enum StatefulStreamResult<T> {
    Ready(T),
    Continue,
}

pub(crate) fn symmetric_join_output_partitioning(
    left: &Arc<dyn ExecutionPlan>,
    right: &Arc<dyn ExecutionPlan>,
    join_type: &JoinType,
) -> Partitioning {
    let left_columns_len = left.schema().fields.len();
    let left_partitioning = left.output_partitioning();
    let right_partitioning = right.output_partitioning();
    match join_type {
        JoinType::Left | JoinType::LeftSemi | JoinType::LeftAnti | JoinType::LeftMark => {
            left_partitioning.clone()
        }
        JoinType::RightSemi | JoinType::RightAnti => right_partitioning.clone(),
        JoinType::Inner | JoinType::Right => {
            adjust_right_output_partitioning(right_partitioning, left_columns_len)
        }
        JoinType::Full => {
            // We could also use left partition count as they are necessarily equal.
            Partitioning::UnknownPartitioning(right_partitioning.partition_count())
        }
    }
}

pub(crate) fn asymmetric_join_output_partitioning(
    left: &Arc<dyn ExecutionPlan>,
    right: &Arc<dyn ExecutionPlan>,
    join_type: &JoinType,
) -> Partitioning {
    match join_type {
        JoinType::Inner | JoinType::Right => adjust_right_output_partitioning(
            right.output_partitioning(),
            left.schema().fields().len(),
        ),
        JoinType::RightSemi | JoinType::RightAnti => right.output_partitioning().clone(),
        JoinType::Left
        | JoinType::LeftSemi
        | JoinType::LeftAnti
        | JoinType::Full
        | JoinType::LeftMark => Partitioning::UnknownPartitioning(
            right.output_partitioning().partition_count(),
        ),
    }
}

/// Trait for incrementally generating Join output.
///
/// This trait is used to limit some join outputs
/// so it does not produce single large batches
pub(crate) trait BatchTransformer: Debug + Clone {
    /// Sets the next `RecordBatch` to be processed.
    fn set_batch(&mut self, batch: RecordBatch);

    /// Retrieves the next `RecordBatch` from the transformer.
    /// Returns `None` if all batches have been produced.
    /// The boolean flag indicates whether the batch is the last one.
    fn next(&mut self) -> Option<(RecordBatch, bool)>;
}

#[derive(Debug, Clone)]
/// A batch transformer that does nothing.
pub(crate) struct NoopBatchTransformer {
    /// RecordBatch to be processed
    batch: Option<RecordBatch>,
}

impl NoopBatchTransformer {
    pub fn new() -> Self {
        Self { batch: None }
    }
}

impl BatchTransformer for NoopBatchTransformer {
    fn set_batch(&mut self, batch: RecordBatch) {
        self.batch = Some(batch);
    }

    fn next(&mut self) -> Option<(RecordBatch, bool)> {
        self.batch.take().map(|batch| (batch, true))
    }
}

#[derive(Debug, Clone)]
/// Splits large batches into smaller batches with a maximum number of rows.
pub(crate) struct BatchSplitter {
    /// RecordBatch to be split
    batch: Option<RecordBatch>,
    /// Maximum number of rows in a split batch
    batch_size: usize,
    /// Current row index
    row_index: usize,
}

impl BatchSplitter {
    /// Creates a new `BatchSplitter` with the specified batch size.
    pub(crate) fn new(batch_size: usize) -> Self {
        Self {
            batch: None,
            batch_size,
            row_index: 0,
        }
    }
}

impl BatchTransformer for BatchSplitter {
    fn set_batch(&mut self, batch: RecordBatch) {
        self.batch = Some(batch);
        self.row_index = 0;
    }

    fn next(&mut self) -> Option<(RecordBatch, bool)> {
        let Some(batch) = &self.batch else {
            return None;
        };

        let remaining_rows = batch.num_rows() - self.row_index;
        let rows_to_slice = remaining_rows.min(self.batch_size);
        let sliced_batch = batch.slice(self.row_index, rows_to_slice);
        self.row_index += rows_to_slice;

        let mut last = false;
        if self.row_index >= batch.num_rows() {
            self.batch = None;
            last = true;
        }

        Some((sliced_batch, last))
    }
}

/// When the order of the join inputs are changed, the output order of columns
/// must remain the same.
///
/// Joins output columns from their left input followed by their right input.
/// Thus if the inputs are reordered, the output columns must be reordered to
/// match the original order.
pub(crate) fn reorder_output_after_swap(
    plan: Arc<dyn ExecutionPlan>,
    left_schema: &Schema,
    right_schema: &Schema,
) -> Result<Arc<dyn ExecutionPlan>> {
    //////////////////////
    let proj = ProjectionExec::try_new(
        swap_reverting_projection(left_schema, right_schema),
        plan,
    )?;
    Ok(Arc::new(proj))
}

/// When the order of the join is changed, the output order of columns must
/// remain the same.
///
/// Returns the expressions that will allow to swap back the values from the
/// original left as the first columns and those on the right next.
pub fn swap_reverting_projection(
    left_schema: &Schema,
    right_schema: &Schema,
) -> Vec<(Arc<dyn PhysicalExpr>, String)> {
    let right_cols = right_schema.fields().iter().enumerate().map(|(i, f)| {
        (
            Arc::new(Column::new(f.name(), i)) as Arc<dyn PhysicalExpr>,
            f.name().to_owned(),
        )
    });
    let right_len = right_cols.len();
    let left_cols = left_schema.fields().iter().enumerate().map(|(i, f)| {
        (
            Arc::new(Column::new(f.name(), right_len + i)) as Arc<dyn PhysicalExpr>,
            f.name().to_owned(),
        )
    });

    left_cols.chain(right_cols).collect()
}

/// This function swaps the given join's projection.
pub(super) fn swap_join_projection(
    left_schema_len: usize,
    right_schema_len: usize,
    projection: Option<&Vec<usize>>,
    join_type: &JoinType,
) -> Option<Vec<usize>> {
    match join_type {
        // For Anti/Semi join types, projection should remain unmodified,
        // since these joins output schema remains the same after swap
        JoinType::LeftAnti
        | JoinType::LeftSemi
        | JoinType::RightAnti
        | JoinType::RightSemi => projection.cloned(),

        _ => projection.map(|p| {
            p.iter()
                .map(|i| {
                    // If the index is less than the left schema length, it is from
                    // the left schema, so we add the right schema length to it.
                    // Otherwise, it is from the right schema, so we subtract the left
                    // schema length from it.
                    if *i < left_schema_len {
                        *i + right_schema_len
                    } else {
                        *i - left_schema_len
                    }
                })
                .collect()
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::pin::Pin;

    use arrow::array::Int32Array;
    use arrow::compute::SortOptions;
    use arrow::datatypes::{DataType, Fields};
    use arrow::error::{ArrowError, Result as ArrowResult};
    use datafusion_common::stats::Precision::{Absent, Exact, Inexact};
    use datafusion_common::{arrow_datafusion_err, arrow_err, ScalarValue};

    use rstest::rstest;

    fn check(
        left: &[Column],
        right: &[Column],
        on: &[(PhysicalExprRef, PhysicalExprRef)],
    ) -> Result<()> {
        let left = left
            .iter()
            .map(|x| x.to_owned())
            .collect::<HashSet<Column>>();
        let right = right
            .iter()
            .map(|x| x.to_owned())
            .collect::<HashSet<Column>>();
        check_join_set_is_valid(&left, &right, on)
    }

    #[test]
    fn check_valid() -> Result<()> {
        let left = vec![Column::new("a", 0), Column::new("b1", 1)];
        let right = vec![Column::new("a", 0), Column::new("b2", 1)];
        let on = &[(
            Arc::new(Column::new("a", 0)) as _,
            Arc::new(Column::new("a", 0)) as _,
        )];

        check(&left, &right, on)?;
        Ok(())
    }

    #[test]
    fn check_not_in_right() {
        let left = vec![Column::new("a", 0), Column::new("b", 1)];
        let right = vec![Column::new("b", 0)];
        let on = &[(
            Arc::new(Column::new("a", 0)) as _,
            Arc::new(Column::new("a", 0)) as _,
        )];

        assert!(check(&left, &right, on).is_err());
    }

    #[tokio::test]
    async fn check_error_nesting() {
        let once_fut = OnceFut::<()>::new(async {
            arrow_err!(ArrowError::CsvError("some error".to_string()))
        });

        struct TestFut(OnceFut<()>);
        impl Future for TestFut {
            type Output = ArrowResult<()>;

            fn poll(
                mut self: Pin<&mut Self>,
                cx: &mut Context<'_>,
            ) -> Poll<Self::Output> {
                match ready!(self.0.get(cx)) {
                    Ok(()) => Poll::Ready(Ok(())),
                    Err(e) => Poll::Ready(Err(e.into())),
                }
            }
        }

        let res = TestFut(once_fut).await;
        let arrow_err_from_fut = res.expect_err("once_fut always return error");

        let wrapped_err = DataFusionError::from(arrow_err_from_fut);
        let root_err = wrapped_err.find_root();

        let _expected =
            arrow_datafusion_err!(ArrowError::CsvError("some error".to_owned()));

        assert!(matches!(root_err, _expected))
    }

    #[test]
    fn check_not_in_left() {
        let left = vec![Column::new("b", 0)];
        let right = vec![Column::new("a", 0)];
        let on = &[(
            Arc::new(Column::new("a", 0)) as _,
            Arc::new(Column::new("a", 0)) as _,
        )];

        assert!(check(&left, &right, on).is_err());
    }

    #[test]
    fn check_collision() {
        // column "a" would appear both in left and right
        let left = vec![Column::new("a", 0), Column::new("c", 1)];
        let right = vec![Column::new("a", 0), Column::new("b", 1)];
        let on = &[(
            Arc::new(Column::new("a", 0)) as _,
            Arc::new(Column::new("b", 1)) as _,
        )];

        assert!(check(&left, &right, on).is_ok());
    }

    #[test]
    fn check_in_right() {
        let left = vec![Column::new("a", 0), Column::new("c", 1)];
        let right = vec![Column::new("b", 0)];
        let on = &[(
            Arc::new(Column::new("a", 0)) as _,
            Arc::new(Column::new("b", 0)) as _,
        )];

        assert!(check(&left, &right, on).is_ok());
    }

    #[test]
    fn test_join_schema() -> Result<()> {
        let a = Schema::new(vec![Field::new("a", DataType::Int32, false)]);
        let a_nulls = Schema::new(vec![Field::new("a", DataType::Int32, true)]);
        let b = Schema::new(vec![Field::new("b", DataType::Int32, false)]);
        let b_nulls = Schema::new(vec![Field::new("b", DataType::Int32, true)]);

        let cases = vec![
            (&a, &b, JoinType::Inner, &a, &b),
            (&a, &b_nulls, JoinType::Inner, &a, &b_nulls),
            (&a_nulls, &b, JoinType::Inner, &a_nulls, &b),
            (&a_nulls, &b_nulls, JoinType::Inner, &a_nulls, &b_nulls),
            // right input of a `LEFT` join can be null, regardless of input nullness
            (&a, &b, JoinType::Left, &a, &b_nulls),
            (&a, &b_nulls, JoinType::Left, &a, &b_nulls),
            (&a_nulls, &b, JoinType::Left, &a_nulls, &b_nulls),
            (&a_nulls, &b_nulls, JoinType::Left, &a_nulls, &b_nulls),
            // left input of a `RIGHT` join can be null, regardless of input nullness
            (&a, &b, JoinType::Right, &a_nulls, &b),
            (&a, &b_nulls, JoinType::Right, &a_nulls, &b_nulls),
            (&a_nulls, &b, JoinType::Right, &a_nulls, &b),
            (&a_nulls, &b_nulls, JoinType::Right, &a_nulls, &b_nulls),
            // Either input of a `FULL` join can be null
            (&a, &b, JoinType::Full, &a_nulls, &b_nulls),
            (&a, &b_nulls, JoinType::Full, &a_nulls, &b_nulls),
            (&a_nulls, &b, JoinType::Full, &a_nulls, &b_nulls),
            (&a_nulls, &b_nulls, JoinType::Full, &a_nulls, &b_nulls),
        ];

        for (left_in, right_in, join_type, left_out, right_out) in cases {
            let (schema, _) = build_join_schema(left_in, right_in, &join_type);

            let expected_fields = left_out
                .fields()
                .iter()
                .cloned()
                .chain(right_out.fields().iter().cloned())
                .collect::<Fields>();

            let expected_schema = Schema::new(expected_fields);
            assert_eq!(
                schema,
                expected_schema,
                "Mismatch with left_in={}:{}, right_in={}:{}, join_type={:?}",
                left_in.fields()[0].name(),
                left_in.fields()[0].is_nullable(),
                right_in.fields()[0].name(),
                right_in.fields()[0].is_nullable(),
                join_type
            );
        }

        Ok(())
    }

    fn create_stats(
        num_rows: Option<usize>,
        column_stats: Vec<ColumnStatistics>,
        is_exact: bool,
    ) -> Statistics {
        Statistics {
            num_rows: if is_exact {
                num_rows.map(Exact)
            } else {
                num_rows.map(Inexact)
            }
            .unwrap_or(Absent),
            column_statistics: column_stats,
            total_byte_size: Absent,
        }
    }

    fn create_column_stats(
        min: Precision<i64>,
        max: Precision<i64>,
        distinct_count: Precision<usize>,
        null_count: Precision<usize>,
    ) -> ColumnStatistics {
        ColumnStatistics {
            distinct_count,
            min_value: min.map(ScalarValue::from),
            max_value: max.map(ScalarValue::from),
            sum_value: Absent,
            null_count,
        }
    }

    type PartialStats = (
        usize,
        Precision<i64>,
        Precision<i64>,
        Precision<usize>,
        Precision<usize>,
    );

    // This is mainly for validating the all edge cases of the estimation, but
    // more advanced (and real world test cases) are below where we need some control
    // over the expected output (since it depends on join type to join type).
    #[test]
    fn test_inner_join_cardinality_single_column() -> Result<()> {
        let cases: Vec<(PartialStats, PartialStats, Option<Precision<usize>>)> = vec![
            // ------------------------------------------------
            // | left(rows, min, max, distinct, null_count),  |
            // | right(rows, min, max, distinct, null_count), |
            // | expected,                                    |
            // ------------------------------------------------

            // Cardinality computation
            // =======================
            //
            // distinct(left) == NaN, distinct(right) == NaN
            (
                (10, Inexact(1), Inexact(10), Absent, Absent),
                (10, Inexact(1), Inexact(10), Absent, Absent),
                Some(Inexact(10)),
            ),
            // range(left) > range(right)
            (
                (10, Inexact(6), Inexact(10), Absent, Absent),
                (10, Inexact(8), Inexact(10), Absent, Absent),
                Some(Inexact(20)),
            ),
            // range(right) > range(left)
            (
                (10, Inexact(8), Inexact(10), Absent, Absent),
                (10, Inexact(6), Inexact(10), Absent, Absent),
                Some(Inexact(20)),
            ),
            // range(left) > len(left), range(right) > len(right)
            (
                (10, Inexact(1), Inexact(15), Absent, Absent),
                (20, Inexact(1), Inexact(40), Absent, Absent),
                Some(Inexact(10)),
            ),
            // When we have distinct count.
            (
                (10, Inexact(1), Inexact(10), Inexact(10), Absent),
                (10, Inexact(1), Inexact(10), Inexact(10), Absent),
                Some(Inexact(10)),
            ),
            // distinct(left) > distinct(right)
            (
                (10, Inexact(1), Inexact(10), Inexact(5), Absent),
                (10, Inexact(1), Inexact(10), Inexact(2), Absent),
                Some(Inexact(20)),
            ),
            // distinct(right) > distinct(left)
            (
                (10, Inexact(1), Inexact(10), Inexact(2), Absent),
                (10, Inexact(1), Inexact(10), Inexact(5), Absent),
                Some(Inexact(20)),
            ),
            // min(left) < 0 (range(left) > range(right))
            (
                (10, Inexact(-5), Inexact(5), Absent, Absent),
                (10, Inexact(1), Inexact(5), Absent, Absent),
                Some(Inexact(10)),
            ),
            // min(right) < 0, max(right) < 0 (range(right) > range(left))
            (
                (10, Inexact(-25), Inexact(-20), Absent, Absent),
                (10, Inexact(-25), Inexact(-15), Absent, Absent),
                Some(Inexact(10)),
            ),
            // range(left) < 0, range(right) >= 0
            // (there isn't a case where both left and right ranges are negative
            //  so one of them is always going to work, this just proves negative
            //  ranges with bigger absolute values are not are not accidentally used).
            (
                (10, Inexact(-10), Inexact(0), Absent, Absent),
                (10, Inexact(0), Inexact(10), Inexact(5), Absent),
                Some(Inexact(10)),
            ),
            // range(left) = 1, range(right) = 1
            (
                (10, Inexact(1), Inexact(1), Absent, Absent),
                (10, Inexact(1), Inexact(1), Absent, Absent),
                Some(Inexact(100)),
            ),
            //
            // Edge cases
            // ==========
            //
            // No column level stats.
            (
                (10, Absent, Absent, Absent, Absent),
                (10, Absent, Absent, Absent, Absent),
                None,
            ),
            // No min or max (or both).
            (
                (10, Absent, Absent, Inexact(3), Absent),
                (10, Absent, Absent, Inexact(3), Absent),
                None,
            ),
            (
                (10, Inexact(2), Absent, Inexact(3), Absent),
                (10, Absent, Inexact(5), Inexact(3), Absent),
                None,
            ),
            (
                (10, Absent, Inexact(3), Inexact(3), Absent),
                (10, Inexact(1), Absent, Inexact(3), Absent),
                None,
            ),
            (
                (10, Absent, Inexact(3), Absent, Absent),
                (10, Inexact(1), Absent, Absent, Absent),
                None,
            ),
            // Non overlapping min/max (when exact=False).
            (
                (10, Absent, Inexact(4), Absent, Absent),
                (10, Inexact(5), Absent, Absent, Absent),
                Some(Inexact(0)),
            ),
            (
                (10, Inexact(0), Inexact(10), Absent, Absent),
                (10, Inexact(11), Inexact(20), Absent, Absent),
                Some(Inexact(0)),
            ),
            (
                (10, Inexact(11), Inexact(20), Absent, Absent),
                (10, Inexact(0), Inexact(10), Absent, Absent),
                Some(Inexact(0)),
            ),
            // distinct(left) = 0, distinct(right) = 0
            (
                (10, Inexact(1), Inexact(10), Inexact(0), Absent),
                (10, Inexact(1), Inexact(10), Inexact(0), Absent),
                None,
            ),
            // Inexact row count < exact null count with absent distinct count
            (
                (0, Inexact(1), Inexact(10), Absent, Exact(5)),
                (10, Inexact(1), Inexact(10), Absent, Absent),
                Some(Inexact(0)),
            ),
        ];

        for (left_info, right_info, expected_cardinality) in cases {
            let left_num_rows = left_info.0;
            let left_col_stats = vec![create_column_stats(
                left_info.1,
                left_info.2,
                left_info.3,
                left_info.4,
            )];

            let right_num_rows = right_info.0;
            let right_col_stats = vec![create_column_stats(
                right_info.1,
                right_info.2,
                right_info.3,
                right_info.4,
            )];

            assert_eq!(
                estimate_inner_join_cardinality(
                    Statistics {
                        num_rows: Inexact(left_num_rows),
                        total_byte_size: Absent,
                        column_statistics: left_col_stats.clone(),
                    },
                    Statistics {
                        num_rows: Inexact(right_num_rows),
                        total_byte_size: Absent,
                        column_statistics: right_col_stats.clone(),
                    },
                ),
                expected_cardinality.clone()
            );

            // We should also be able to use join_cardinality to get the same results
            let join_type = JoinType::Inner;
            let join_on = vec![(
                Arc::new(Column::new("a", 0)) as _,
                Arc::new(Column::new("b", 0)) as _,
            )];
            let partial_join_stats = estimate_join_cardinality(
                &join_type,
                create_stats(Some(left_num_rows), left_col_stats.clone(), false),
                create_stats(Some(right_num_rows), right_col_stats.clone(), false),
                &join_on,
            );

            assert_eq!(
                partial_join_stats.clone().map(|s| Inexact(s.num_rows)),
                expected_cardinality.clone()
            );
            assert_eq!(
                partial_join_stats.map(|s| s.column_statistics),
                expected_cardinality.map(|_| [left_col_stats, right_col_stats].concat())
            );
        }
        Ok(())
    }

    #[test]
    fn test_inner_join_cardinality_multiple_column() -> Result<()> {
        let left_col_stats = vec![
            create_column_stats(Inexact(0), Inexact(100), Inexact(100), Absent),
            create_column_stats(Inexact(100), Inexact(500), Inexact(150), Absent),
        ];

        let right_col_stats = vec![
            create_column_stats(Inexact(0), Inexact(100), Inexact(50), Absent),
            create_column_stats(Inexact(100), Inexact(500), Inexact(200), Absent),
        ];

        // We have statistics about 4 columns, where the highest distinct
        // count is 200, so we are going to pick it.
        assert_eq!(
            estimate_inner_join_cardinality(
                Statistics {
                    num_rows: Inexact(400),
                    total_byte_size: Absent,
                    column_statistics: left_col_stats,
                },
                Statistics {
                    num_rows: Inexact(400),
                    total_byte_size: Absent,
                    column_statistics: right_col_stats,
                },
            ),
            Some(Inexact((400 * 400) / 200))
        );
        Ok(())
    }

    #[test]
    fn test_inner_join_cardinality_decimal_range() -> Result<()> {
        let left_col_stats = vec![ColumnStatistics {
            distinct_count: Absent,
            min_value: Inexact(ScalarValue::Decimal128(Some(32500), 14, 4)),
            max_value: Inexact(ScalarValue::Decimal128(Some(35000), 14, 4)),
            ..Default::default()
        }];

        let right_col_stats = vec![ColumnStatistics {
            distinct_count: Absent,
            min_value: Inexact(ScalarValue::Decimal128(Some(33500), 14, 4)),
            max_value: Inexact(ScalarValue::Decimal128(Some(34000), 14, 4)),
            ..Default::default()
        }];

        assert_eq!(
            estimate_inner_join_cardinality(
                Statistics {
                    num_rows: Inexact(100),
                    total_byte_size: Absent,
                    column_statistics: left_col_stats,
                },
                Statistics {
                    num_rows: Inexact(100),
                    total_byte_size: Absent,
                    column_statistics: right_col_stats,
                },
            ),
            Some(Inexact(100))
        );
        Ok(())
    }

    #[test]
    fn test_join_cardinality() -> Result<()> {
        // Left table (rows=1000)
        //   a: min=0, max=100, distinct=100
        //   b: min=0, max=500, distinct=500
        //   x: min=1000, max=10000, distinct=None
        //
        // Right table (rows=2000)
        //   c: min=0, max=100, distinct=50
        //   d: min=0, max=2000, distinct=2500 (how? some inexact statistics)
        //   y: min=0, max=100, distinct=None
        //
        // Join on a=c, b=d (ignore x/y)
        let cases = vec![
            (JoinType::Inner, 800),
            (JoinType::Left, 1000),
            (JoinType::Right, 2000),
            (JoinType::Full, 2200),
        ];

        let left_col_stats = vec![
            create_column_stats(Inexact(0), Inexact(100), Inexact(100), Absent),
            create_column_stats(Inexact(0), Inexact(500), Inexact(500), Absent),
            create_column_stats(Inexact(1000), Inexact(10000), Absent, Absent),
        ];

        let right_col_stats = vec![
            create_column_stats(Inexact(0), Inexact(100), Inexact(50), Absent),
            create_column_stats(Inexact(0), Inexact(2000), Inexact(2500), Absent),
            create_column_stats(Inexact(0), Inexact(100), Absent, Absent),
        ];

        for (join_type, expected_num_rows) in cases {
            let join_on = vec![
                (
                    Arc::new(Column::new("a", 0)) as _,
                    Arc::new(Column::new("c", 0)) as _,
                ),
                (
                    Arc::new(Column::new("b", 1)) as _,
                    Arc::new(Column::new("d", 1)) as _,
                ),
            ];

            let partial_join_stats = estimate_join_cardinality(
                &join_type,
                create_stats(Some(1000), left_col_stats.clone(), false),
                create_stats(Some(2000), right_col_stats.clone(), false),
                &join_on,
            )
            .unwrap();
            assert_eq!(partial_join_stats.num_rows, expected_num_rows);
            assert_eq!(
                partial_join_stats.column_statistics,
                [left_col_stats.clone(), right_col_stats.clone()].concat()
            );
        }

        Ok(())
    }

    #[test]
    fn test_join_cardinality_when_one_column_is_disjoint() -> Result<()> {
        // Left table (rows=1000)
        //   a: min=0, max=100, distinct=100
        //   b: min=0, max=500, distinct=500
        //   x: min=1000, max=10000, distinct=None
        //
        // Right table (rows=2000)
        //   c: min=0, max=100, distinct=50
        //   d: min=0, max=2000, distinct=2500 (how? some inexact statistics)
        //   y: min=0, max=100, distinct=None
        //
        // Join on a=c, x=y (ignores b/d) where x and y does not intersect

        let left_col_stats = vec![
            create_column_stats(Inexact(0), Inexact(100), Inexact(100), Absent),
            create_column_stats(Inexact(0), Inexact(500), Inexact(500), Absent),
            create_column_stats(Inexact(1000), Inexact(10000), Absent, Absent),
        ];

        let right_col_stats = vec![
            create_column_stats(Inexact(0), Inexact(100), Inexact(50), Absent),
            create_column_stats(Inexact(0), Inexact(2000), Inexact(2500), Absent),
            create_column_stats(Inexact(0), Inexact(100), Absent, Absent),
        ];

        let join_on = vec![
            (
                Arc::new(Column::new("a", 0)) as _,
                Arc::new(Column::new("c", 0)) as _,
            ),
            (
                Arc::new(Column::new("x", 2)) as _,
                Arc::new(Column::new("y", 2)) as _,
            ),
        ];

        let cases = vec![
            // Join type, expected cardinality
            //
            // When an inner join is disjoint, that means it won't
            // produce any rows.
            (JoinType::Inner, 0),
            // But left/right outer joins will produce at least
            // the amount of rows from the left/right side.
            (JoinType::Left, 1000),
            (JoinType::Right, 2000),
            // And a full outer join will produce at least the combination
            // of the rows above (minus the cardinality of the inner join, which
            // is 0).
            (JoinType::Full, 3000),
        ];

        for (join_type, expected_num_rows) in cases {
            let partial_join_stats = estimate_join_cardinality(
                &join_type,
                create_stats(Some(1000), left_col_stats.clone(), true),
                create_stats(Some(2000), right_col_stats.clone(), true),
                &join_on,
            )
            .unwrap();
            assert_eq!(partial_join_stats.num_rows, expected_num_rows);
            assert_eq!(
                partial_join_stats.column_statistics,
                [left_col_stats.clone(), right_col_stats.clone()].concat()
            );
        }

        Ok(())
    }

    #[test]
    fn test_anti_semi_join_cardinality() -> Result<()> {
        let cases: Vec<(JoinType, PartialStats, PartialStats, Option<usize>)> = vec![
            // ------------------------------------------------
            // | join_type ,                                   |
            // | left(rows, min, max, distinct, null_count), |
            // | right(rows, min, max, distinct, null_count), |
            // | expected,                                    |
            // ------------------------------------------------

            // Cardinality computation
            // =======================
            (
                JoinType::LeftSemi,
                (50, Inexact(10), Inexact(20), Absent, Absent),
                (10, Inexact(15), Inexact(25), Absent, Absent),
                Some(50),
            ),
            (
                JoinType::RightSemi,
                (50, Inexact(10), Inexact(20), Absent, Absent),
                (10, Inexact(15), Inexact(25), Absent, Absent),
                Some(10),
            ),
            (
                JoinType::LeftSemi,
                (10, Absent, Absent, Absent, Absent),
                (50, Absent, Absent, Absent, Absent),
                Some(10),
            ),
            (
                JoinType::LeftSemi,
                (50, Inexact(10), Inexact(20), Absent, Absent),
                (10, Inexact(30), Inexact(40), Absent, Absent),
                Some(0),
            ),
            (
                JoinType::LeftSemi,
                (50, Inexact(10), Absent, Absent, Absent),
                (10, Absent, Inexact(5), Absent, Absent),
                Some(0),
            ),
            (
                JoinType::LeftSemi,
                (50, Absent, Inexact(20), Absent, Absent),
                (10, Inexact(30), Absent, Absent, Absent),
                Some(0),
            ),
            (
                JoinType::LeftAnti,
                (50, Inexact(10), Inexact(20), Absent, Absent),
                (10, Inexact(15), Inexact(25), Absent, Absent),
                Some(50),
            ),
            (
                JoinType::RightAnti,
                (50, Inexact(10), Inexact(20), Absent, Absent),
                (10, Inexact(15), Inexact(25), Absent, Absent),
                Some(10),
            ),
            (
                JoinType::LeftAnti,
                (10, Absent, Absent, Absent, Absent),
                (50, Absent, Absent, Absent, Absent),
                Some(10),
            ),
            (
                JoinType::LeftAnti,
                (50, Inexact(10), Inexact(20), Absent, Absent),
                (10, Inexact(30), Inexact(40), Absent, Absent),
                Some(50),
            ),
            (
                JoinType::LeftAnti,
                (50, Inexact(10), Absent, Absent, Absent),
                (10, Absent, Inexact(5), Absent, Absent),
                Some(50),
            ),
            (
                JoinType::LeftAnti,
                (50, Absent, Inexact(20), Absent, Absent),
                (10, Inexact(30), Absent, Absent, Absent),
                Some(50),
            ),
        ];

        let join_on = vec![(
            Arc::new(Column::new("l_col", 0)) as _,
            Arc::new(Column::new("r_col", 0)) as _,
        )];

        for (join_type, outer_info, inner_info, expected) in cases {
            let outer_num_rows = outer_info.0;
            let outer_col_stats = vec![create_column_stats(
                outer_info.1,
                outer_info.2,
                outer_info.3,
                outer_info.4,
            )];

            let inner_num_rows = inner_info.0;
            let inner_col_stats = vec![create_column_stats(
                inner_info.1,
                inner_info.2,
                inner_info.3,
                inner_info.4,
            )];

            let output_cardinality = estimate_join_cardinality(
                &join_type,
                Statistics {
                    num_rows: Inexact(outer_num_rows),
                    total_byte_size: Absent,
                    column_statistics: outer_col_stats,
                },
                Statistics {
                    num_rows: Inexact(inner_num_rows),
                    total_byte_size: Absent,
                    column_statistics: inner_col_stats,
                },
                &join_on,
            )
            .map(|cardinality| cardinality.num_rows);

            assert_eq!(
                output_cardinality, expected,
                "failure for join_type: {join_type}"
            );
        }

        Ok(())
    }

    #[test]
    fn test_semi_join_cardinality_absent_rows() -> Result<()> {
        let dummy_column_stats =
            vec![create_column_stats(Absent, Absent, Absent, Absent)];
        let join_on = vec![(
            Arc::new(Column::new("l_col", 0)) as _,
            Arc::new(Column::new("r_col", 0)) as _,
        )];

        let absent_outer_estimation = estimate_join_cardinality(
            &JoinType::LeftSemi,
            Statistics {
                num_rows: Absent,
                total_byte_size: Absent,
                column_statistics: dummy_column_stats.clone(),
            },
            Statistics {
                num_rows: Exact(10),
                total_byte_size: Absent,
                column_statistics: dummy_column_stats.clone(),
            },
            &join_on,
        );
        assert!(
            absent_outer_estimation.is_none(),
            "Expected \"None\" estimated SemiJoin cardinality for absent outer num_rows"
        );

        let absent_inner_estimation = estimate_join_cardinality(
            &JoinType::LeftSemi,
            Statistics {
                num_rows: Inexact(500),
                total_byte_size: Absent,
                column_statistics: dummy_column_stats.clone(),
            },
            Statistics {
                num_rows: Absent,
                total_byte_size: Absent,
                column_statistics: dummy_column_stats.clone(),
            },
            &join_on,
        ).expect("Expected non-empty PartialJoinStatistics for SemiJoin with absent inner num_rows");

        assert_eq!(absent_inner_estimation.num_rows, 500, "Expected outer.num_rows estimated SemiJoin cardinality for absent inner num_rows");

        let absent_inner_estimation = estimate_join_cardinality(
            &JoinType::LeftSemi,
            Statistics {
                num_rows: Absent,
                total_byte_size: Absent,
                column_statistics: dummy_column_stats.clone(),
            },
            Statistics {
                num_rows: Absent,
                total_byte_size: Absent,
                column_statistics: dummy_column_stats,
            },
            &join_on,
        );
        assert!(absent_inner_estimation.is_none(), "Expected \"None\" estimated SemiJoin cardinality for absent outer and inner num_rows");

        Ok(())
    }

    #[test]
    fn test_calculate_join_output_ordering() -> Result<()> {
        let options = SortOptions::default();
        let left_ordering = LexOrdering::new(vec![
            PhysicalSortExpr {
                expr: Arc::new(Column::new("a", 0)),
                options,
            },
            PhysicalSortExpr {
                expr: Arc::new(Column::new("c", 2)),
                options,
            },
            PhysicalSortExpr {
                expr: Arc::new(Column::new("d", 3)),
                options,
            },
        ]);
        let right_ordering = LexOrdering::new(vec![
            PhysicalSortExpr {
                expr: Arc::new(Column::new("z", 2)),
                options,
            },
            PhysicalSortExpr {
                expr: Arc::new(Column::new("y", 1)),
                options,
            },
        ]);
        let join_type = JoinType::Inner;
        let on_columns = [(
            Arc::new(Column::new("b", 1)) as _,
            Arc::new(Column::new("x", 0)) as _,
        )];
        let left_columns_len = 5;
        let maintains_input_orders = [[true, false], [false, true]];
        let probe_sides = [Some(JoinSide::Left), Some(JoinSide::Right)];

        let expected = [
            Some(LexOrdering::new(vec![
                PhysicalSortExpr {
                    expr: Arc::new(Column::new("a", 0)),
                    options,
                },
                PhysicalSortExpr {
                    expr: Arc::new(Column::new("c", 2)),
                    options,
                },
                PhysicalSortExpr {
                    expr: Arc::new(Column::new("d", 3)),
                    options,
                },
                PhysicalSortExpr {
                    expr: Arc::new(Column::new("z", 7)),
                    options,
                },
                PhysicalSortExpr {
                    expr: Arc::new(Column::new("y", 6)),
                    options,
                },
            ])),
            Some(LexOrdering::new(vec![
                PhysicalSortExpr {
                    expr: Arc::new(Column::new("z", 7)),
                    options,
                },
                PhysicalSortExpr {
                    expr: Arc::new(Column::new("y", 6)),
                    options,
                },
                PhysicalSortExpr {
                    expr: Arc::new(Column::new("a", 0)),
                    options,
                },
                PhysicalSortExpr {
                    expr: Arc::new(Column::new("c", 2)),
                    options,
                },
                PhysicalSortExpr {
                    expr: Arc::new(Column::new("d", 3)),
                    options,
                },
            ])),
        ];

        for (i, (maintains_input_order, probe_side)) in
            maintains_input_orders.iter().zip(probe_sides).enumerate()
        {
            assert_eq!(
                calculate_join_output_ordering(
                    left_ordering.as_ref(),
                    right_ordering.as_ref(),
                    join_type,
                    &on_columns,
                    left_columns_len,
                    maintains_input_order,
                    probe_side,
                ),
                expected[i]
            );
        }

        Ok(())
    }

    fn create_test_batch(num_rows: usize) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let data = Arc::new(Int32Array::from_iter_values(0..num_rows as i32));
        RecordBatch::try_new(schema, vec![data]).unwrap()
    }

    fn assert_split_batches(
        batches: Vec<(RecordBatch, bool)>,
        batch_size: usize,
        num_rows: usize,
    ) {
        let mut row_count = 0;
        for (batch, last) in batches.into_iter() {
            assert_eq!(batch.num_rows(), (num_rows - row_count).min(batch_size));
            let column = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            for i in 0..batch.num_rows() {
                assert_eq!(column.value(i), i as i32 + row_count as i32);
            }
            row_count += batch.num_rows();
            assert_eq!(last, row_count == num_rows);
        }
    }

    #[rstest]
    #[test]
    fn test_batch_splitter(
        #[values(1, 3, 11)] batch_size: usize,
        #[values(1, 6, 50)] num_rows: usize,
    ) {
        let mut splitter = BatchSplitter::new(batch_size);
        splitter.set_batch(create_test_batch(num_rows));

        let mut batches = Vec::with_capacity(num_rows.div_ceil(batch_size));
        while let Some(batch) = splitter.next() {
            batches.push(batch);
        }

        assert!(splitter.next().is_none());
        assert_split_batches(batches, batch_size, num_rows);
    }

    #[tokio::test]
    async fn test_swap_reverting_projection() {
        let left_schema = Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
        ]);

        let right_schema = Schema::new(vec![Field::new("c", DataType::Int32, false)]);

        let proj = swap_reverting_projection(&left_schema, &right_schema);

        assert_eq!(proj.len(), 3);

        let (col, name) = &proj[0];
        assert_eq!(name, "a");
        assert_col_expr(col, "a", 1);

        let (col, name) = &proj[1];
        assert_eq!(name, "b");
        assert_col_expr(col, "b", 2);

        let (col, name) = &proj[2];
        assert_eq!(name, "c");
        assert_col_expr(col, "c", 0);
    }

    fn assert_col_expr(expr: &Arc<dyn PhysicalExpr>, name: &str, index: usize) {
        let col = expr
            .as_any()
            .downcast_ref::<Column>()
            .expect("Projection items should be Column expression");
        assert_eq!(col.name(), name);
        assert_eq!(col.index(), index);
    }

    #[test]
    fn test_join_metadata() -> Result<()> {
        let left_schema = Schema::new(vec![Field::new("a", DataType::Int32, false)])
            .with_metadata(HashMap::from([("key".to_string(), "left".to_string())]));

        let right_schema = Schema::new(vec![Field::new("b", DataType::Int32, false)])
            .with_metadata(HashMap::from([("key".to_string(), "right".to_string())]));

        let (join_schema, _) =
            build_join_schema(&left_schema, &right_schema, &JoinType::Left);
        assert_eq!(
            join_schema.metadata(),
            &HashMap::from([("key".to_string(), "left".to_string())])
        );
        let (join_schema, _) =
            build_join_schema(&left_schema, &right_schema, &JoinType::Right);
        assert_eq!(
            join_schema.metadata(),
            &HashMap::from([("key".to_string(), "right".to_string())])
        );

        Ok(())
    }
}
