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

use std::sync::Arc;

use arrow::util::pretty::pretty_format_batches;
use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::{ParquetReadOptions, SessionConfig, SessionContext};
use datafusion_common::Result;
use datafusion_common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion_datasource_parquet::push_all_projection_hints::PushAllProjectionHints;
use datafusion_physical_plan::{ExecutionPlan, collect};
use tempfile::TempDir;

fn context(deep: bool, pushdown: bool) -> SessionContext {
    context_with_partitions(deep, pushdown, 1)
}

fn context_with_partitions(
    deep: bool,
    pushdown: bool,
    partitions: usize,
) -> SessionContext {
    let config = SessionConfig::new()
        .with_target_partitions(partitions)
        .set_bool("datafusion.execution.parquet.pushdown_filters", pushdown);
    let mut builder = SessionStateBuilder::new()
        .with_config(config)
        .with_default_features();
    if deep {
        builder =
            builder.with_physical_optimizer_rule(Arc::new(PushAllProjectionHints {}));
    }
    SessionContext::new_with_state(builder.build())
}

async fn fixture() -> Result<TempDir> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("data.parquet");
    context(false, false)
        .sql(&format!(
            "COPY (
                SELECT id,
                    CASE WHEN id = 3 THEN NULL ELSE
                        named_struct('x', id, 'y', id + 10, 'pad', repeat('padding', 1000))
                    END AS s,
                    CASE WHEN id = 3 THEN NULL ELSE
                        [named_struct('x', id, 'y', id + 10, 'pad', repeat('padding', 1000)),
                         NULL]
                    END AS events,
                    [named_struct('inner',
                        [named_struct('x', id, 'pad', repeat('padding', 1000))],
                        'pad', repeat('padding', 1000))] AS nested,
                    MAP {{'k': named_struct('x', id, 'pad', repeat('padding', 1000))}} AS m
                FROM (VALUES (1), (2), (3)) AS t(id)
            ) TO '{}' STORED AS PARQUET",
            path.display()
        ))
        .await?
        .collect()
        .await?;
    Ok(dir)
}

fn bytes_scanned(plan: &Arc<dyn ExecutionPlan>) -> Result<usize> {
    let mut bytes = 0;
    plan.apply(|plan| {
        if let Some(metrics) = plan.metrics()
            && let Some(value) = metrics.sum_by_name("bytes_scanned")
        {
            bytes += value.as_usize();
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    Ok(bytes)
}

async fn compare(query: &str, prune: bool, pushdown: bool) -> Result<()> {
    compare_with_schema(query, prune, pushdown, None).await
}

async fn compare_with_schema(
    query: &str,
    prune: bool,
    pushdown: bool,
    schema: Option<&str>,
) -> Result<()> {
    let dir = fixture().await?;
    let path = dir.path().join("data.parquet");
    let mut results = vec![];
    let mut bytes = vec![];
    for (deep, partitions) in [(false, 1), (true, 1), (true, 4)] {
        let ctx = context_with_partitions(deep, pushdown, partitions);
        match schema {
            Some(schema) => {
                ctx.sql(&format!(
                    "CREATE EXTERNAL TABLE t ({schema}) STORED AS PARQUET LOCATION '{}'",
                    path.display()
                ))
                .await?
                .collect()
                .await?;
            }
            None => {
                ctx.register_parquet(
                    "t",
                    path.to_str().unwrap(),
                    ParquetReadOptions::default(),
                )
                .await?;
            }
        }
        let plan = ctx.sql(query).await?.create_physical_plan().await?;
        let schema = plan.schema();
        let batches = collect(Arc::clone(&plan), ctx.task_ctx()).await?;
        bytes.push(bytes_scanned(&plan)?);
        results.push((schema, pretty_format_batches(&batches)?.to_string()));
    }
    for index in 1..results.len() {
        assert_eq!(results[0], results[index], "{query}");
        if prune {
            assert!(bytes[index] < bytes[0], "{query}: {bytes:?}");
        } else {
            assert_eq!(bytes[0], bytes[index], "{query}");
        }
    }
    Ok(())
}

#[tokio::test]
async fn list_field_and_element_projection() -> Result<()> {
    for query in [
        "SELECT events['x'] FROM t ORDER BY id",
        "SELECT events[1]['x'] FROM t ORDER BY id",
        "SELECT events[id]['x'] FROM t ORDER BY id",
        "SELECT events['x'], events['y'] FROM t ORDER BY id",
        "SELECT nested[1]['inner'][1]['x'] FROM t ORDER BY id",
    ] {
        compare(query, true, false).await?;
    }
    Ok(())
}

#[tokio::test]
async fn projection_above_window() -> Result<()> {
    compare(
        "SELECT s['x'], rn FROM (
            SELECT s, row_number() OVER (ORDER BY id) AS rn FROM t
         ) q ORDER BY rn",
        true,
        false,
    )
    .await
}

#[tokio::test]
async fn whole_values_and_maps_are_preserved() -> Result<()> {
    for query in [
        "SELECT s, s['x'] FROM t ORDER BY id",
        "SELECT events, events['x'] FROM t ORDER BY id",
        "SELECT m FROM t ORDER BY id",
        "SELECT DISTINCT s FROM t ORDER BY s['x']",
    ] {
        compare(query, false, false).await?;
    }
    Ok(())
}

#[tokio::test]
async fn map_values_and_missing_keys() -> Result<()> {
    for query in [
        "SELECT m['k']['x'] FROM t ORDER BY id",
        "SELECT m['absent']['x'] FROM t ORDER BY id",
        "SELECT m['k']['x'], m['absent']['x'] FROM t ORDER BY id",
        "SELECT m['k']['x'], rn FROM (
            SELECT m, row_number() OVER (ORDER BY id) AS rn FROM t
         ) q ORDER BY rn",
    ] {
        compare(query, true, false).await?;
    }
    Ok(())
}

#[tokio::test]
async fn selector_evaluation_errors_are_preserved() -> Result<()> {
    let dir = fixture().await?;
    for query in [
        "SELECT events[1 / (id - 1)]['x'] FROM t",
        "SELECT m[CASE WHEN id = 1 THEN 'k' ELSE 'absent' END]['x'] FROM t",
    ] {
        let mut errors = vec![];
        for (deep, partitions) in [(false, 1), (true, 1), (true, 4)] {
            let ctx = context_with_partitions(deep, false, partitions);
            ctx.register_parquet(
                "t",
                dir.path().join("data.parquet").to_str().unwrap(),
                ParquetReadOptions::default(),
            )
            .await?;
            let plan = ctx.sql(query).await?.create_physical_plan().await?;
            errors.push(collect(plan, ctx.task_ctx()).await.unwrap_err().to_string());
        }
        assert_eq!(errors[0], errors[1], "{query}");
        assert_eq!(errors[0], errors[2], "{query}");
    }
    Ok(())
}

#[tokio::test]
async fn grouped_and_final_aggregates() -> Result<()> {
    for query in [
        "SELECT id, sum(s['x'] * s['y']) FROM t GROUP BY id ORDER BY id",
        "SELECT id, sum(id * s['x']) FROM t GROUP BY id ORDER BY id",
        "SELECT sum(id * s['x']) FROM t",
        "SELECT s['x'], count(*) FROM t GROUP BY s['x'] ORDER BY s['x']",
        "SELECT a.id, sum(a.s['x'] * b.s['y']) FROM t a
         JOIN t b ON a.id = b.id GROUP BY a.id ORDER BY a.id",
    ] {
        compare(query, false, false).await?;
    }
    Ok(())
}
#[tokio::test]
async fn window_filter_order_and_partition_dependencies() -> Result<()> {
    for pushdown in [false, true] {
        compare(
            "SELECT s['x'] FROM t WHERE s['y'] > 10 ORDER BY id",
            false,
            pushdown,
        )
        .await?;
        compare(
            "SELECT s['x'], rn FROM (
                SELECT s, row_number() OVER (
                    PARTITION BY s['y'] ORDER BY id DESC
                ) AS rn FROM t WHERE s['y'] > 10
             ) q ORDER BY s['x']",
            true,
            pushdown,
        )
        .await?;
    }

    Ok(())
}

#[tokio::test]
async fn schema_evolution_and_missing_fields() -> Result<()> {
    compare_with_schema(
        "SELECT s['x'], s['missing'], events[1]['x'], events['missing']
         FROM t ORDER BY id",
        false,
        true,
        Some(
            "id BIGINT, s STRUCT<x BIGINT, missing BIGINT>,
             events ARRAY<STRUCT<x BIGINT, missing BIGINT>>",
        ),
    )
    .await
}

#[tokio::test]
async fn identical_self_join_scans_and_union() -> Result<()> {
    compare(
        "SELECT a.s['x'], b.s['y'], a.rn, b.rn FROM
         (SELECT id, s, row_number() OVER (ORDER BY id) AS rn FROM t) a
         JOIN
         (SELECT id, s, row_number() OVER (ORDER BY id) AS rn FROM t) b
         ON a.id = b.id
         ORDER BY a.id",
        true,
        false,
    )
    .await?;
    compare(
        "SELECT events['x'] FROM (
            SELECT events FROM t UNION ALL SELECT events FROM t
         ) q ORDER BY events['x']",
        true,
        false,
    )
    .await
}

#[tokio::test]
async fn non_hash_join_boundaries() -> Result<()> {
    use arrow::compute::SortOptions;
    use datafusion_common::{JoinType, NullEquality};
    use datafusion_physical_expr::ScalarFunctionExpr;
    use datafusion_physical_expr::expressions::{Column, Literal};
    use datafusion_physical_expr::projection::ProjectionExpr;
    use datafusion_physical_optimizer::PhysicalOptimizerRule;
    use datafusion_physical_plan::joins::{
        NestedLoopJoinExecBuilder, PiecewiseMergeJoinExec, SortMergeJoinExec,
        StreamJoinPartitionMode, SymmetricHashJoinExec,
    };
    use datafusion_physical_plan::projection::ProjectionExec;

    let dir = fixture().await?;
    let ctx = context(false, false);
    ctx.register_parquet(
        "t",
        dir.path().join("data.parquet").to_str().unwrap(),
        ParquetReadOptions::default(),
    )
    .await?;
    for kind in ["nested", "sort_merge", "symmetric", "piecewise"] {
        let mut values = vec![];
        let mut reads = vec![];
        for deep in [false, true] {
            let left = ctx
                .sql("SELECT id, s FROM t ORDER BY id")
                .await?
                .create_physical_plan()
                .await?;
            let right = ctx
                .sql("SELECT id, s FROM t ORDER BY id")
                .await?
                .create_physical_plan()
                .await?;
            let key = || {
                Arc::new(Column::new("id", 0))
                    as Arc<dyn datafusion_physical_expr::PhysicalExpr>
            };
            let join: Arc<dyn ExecutionPlan> = match kind {
                "nested" => Arc::new(
                    NestedLoopJoinExecBuilder::new(left, right, JoinType::Inner)
                        .build()?,
                ),
                "sort_merge" => Arc::new(SortMergeJoinExec::try_new(
                    left,
                    right,
                    vec![(key(), key())],
                    None,
                    JoinType::Inner,
                    vec![SortOptions::default()],
                    NullEquality::NullEqualsNothing,
                )?),
                "symmetric" => Arc::new(SymmetricHashJoinExec::try_new(
                    left,
                    right,
                    vec![(key(), key())],
                    None,
                    &JoinType::Inner,
                    NullEquality::NullEqualsNothing,
                    None,
                    None,
                    StreamJoinPartitionMode::SinglePartition,
                )?),
                "piecewise" => Arc::new(PiecewiseMergeJoinExec::try_new(
                    left,
                    right,
                    (key(), key()),
                    datafusion_expr::Operator::Gt,
                    JoinType::Inner,
                    1,
                )?),
                _ => unreachable!(),
            };
            let config = Arc::clone(ctx.state().config_options());
            let field = |index, name: &str| -> Result<ProjectionExpr> {
                let expr = ScalarFunctionExpr::try_new(
                    datafusion_functions::core::get_field(),
                    vec![
                        Arc::new(Column::new("s", index)),
                        Arc::new(Literal::new(datafusion_common::ScalarValue::Utf8(
                            Some(name.to_string()),
                        ))),
                    ],
                    &join.schema(),
                    Arc::clone(&config),
                )?;
                Ok(ProjectionExpr::new(Arc::new(expr), name))
            };
            let plan: Arc<dyn ExecutionPlan> = Arc::new(ProjectionExec::try_new(
                [field(1, "x")?, field(3, "y")?],
                join,
            )?);
            let plan = if deep {
                PushAllProjectionHints {}.optimize(plan, &config)?
            } else {
                plan
            };
            let schema = plan.schema();
            let batches = collect(Arc::clone(&plan), ctx.task_ctx()).await?;
            let formatted = pretty_format_batches(&batches)?.to_string();
            // Symmetric joins may emit matches in a different batch order.
            let mut rows = formatted.lines().map(str::to_owned).collect::<Vec<_>>();
            rows.sort();
            values.push((schema, rows));
            reads.push(bytes_scanned(&plan)?);
        }

        assert_eq!(values[0], values[1], "{kind}");
        assert!(reads[1] < reads[0], "{kind}: {reads:?}");
    }
    Ok(())
}

#[tokio::test]
async fn fixed_size_lists_and_required_dead_containers() -> Result<()> {
    use arrow::array::{
        Array, ArrayRef, FixedSizeListArray, Int64Array, ListArray, RecordBatch,
        StringArray, StructArray,
    };
    use arrow::buffer::OffsetBuffer;
    use arrow::datatypes::{DataType, Field};
    use parquet::arrow::ArrowWriter;

    let dir = tempfile::tempdir()?;
    let path = dir.path().join("required.parquet");
    let values = StructArray::from(vec![
        (
            Arc::new(Field::new("x", DataType::Int64, false)),
            Arc::new(Int64Array::from_iter_values(1..=6)) as ArrayRef,
        ),
        (
            Arc::new(Field::new("pad", DataType::Utf8, false)),
            Arc::new(StringArray::from(vec!["padding".repeat(1024); 6])) as ArrayRef,
        ),
    ]);
    let events = FixedSizeListArray::try_new(
        Arc::new(Field::new("element", values.data_type().clone(), false)),
        2,
        Arc::new(values),
        None,
    )?;
    let list_item = Arc::new(Field::new("element", DataType::Int64, false));
    let list = ListArray::try_new(
        Arc::clone(&list_item),
        OffsetBuffer::from_lengths([1, 0, 1]),
        Arc::new(Int64Array::from(vec![10, 20])),
        None,
    )?;
    let fixed = FixedSizeListArray::try_new(
        list_item,
        2,
        Arc::new(Int64Array::from_iter_values(1..=6)),
        None,
    )?;
    let s = StructArray::from(vec![
        (
            Arc::new(Field::new("x", DataType::Int64, false)),
            Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef,
        ),
        (
            Arc::new(Field::new("list", list.data_type().clone(), false)),
            Arc::new(list) as ArrayRef,
        ),
        (
            Arc::new(Field::new("fixed", fixed.data_type().clone(), false)),
            Arc::new(fixed) as ArrayRef,
        ),
    ]);
    let batch = RecordBatch::try_from_iter([
        ("events", Arc::new(events) as ArrayRef),
        ("s", Arc::new(s) as ArrayRef),
    ])?;
    let mut writer =
        ArrowWriter::try_new(std::fs::File::create(&path)?, batch.schema(), None)?;
    writer.write(&batch)?;
    writer.close()?;
    for query in [
        "SELECT events[1]['x'] FROM t",
        "SELECT s['x'], rn FROM (SELECT s, row_number() OVER (ORDER BY events[1]['x']) AS rn FROM t) q ORDER BY rn",
    ] {
        let mut results = vec![];
        let mut reads = vec![];
        for deep in [false, true] {
            let ctx = context(deep, false);
            ctx.register_parquet(
                "t",
                path.to_str().unwrap(),
                ParquetReadOptions::default(),
            )
            .await?;
            let plan = ctx.sql(query).await?.create_physical_plan().await?;
            let schema = plan.schema();
            let batches = collect(Arc::clone(&plan), ctx.task_ctx()).await?;
            results.push((schema, pretty_format_batches(&batches)?.to_string()));
            reads.push(bytes_scanned(&plan)?);
        }
        assert_eq!(results[0], results[1], "{query}");
        assert!(reads[1] < reads[0], "{query}: {reads:?}");
    }
    Ok(())
}
