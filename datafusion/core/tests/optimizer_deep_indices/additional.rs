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
async fn projection_above_struct_unnest() -> Result<()> {
    compare(
        "WITH parts(x, y, pad) AS (SELECT UNNEST(s) FROM t) SELECT y FROM parts ORDER BY y",
        false,
        false,
    )
    .await
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

mod unary_projection_hints {
    use super::*;
    use arrow::array::AsArray;
    use arrow::datatypes::{Int64Type, SchemaRef};
    use datafusion::catalog::{Session, TableProvider};
    use datafusion::prelude::{DataFrame, col, get_field};
    use datafusion_datasource::file::FileSource;
    use datafusion_datasource::source::DataSourceExec;
    use datafusion_datasource_parquet::leaves::{projection_specifier, splat_columns};
    use datafusion_datasource_parquet::source::ParquetSource;
    use datafusion_expr::{Expr, TableType, expr::Sort};
    use datafusion_physical_optimizer::PhysicalOptimizerRule;
    use datafusion_physical_plan::{coop::CooperativeExec, displayable};

    async fn parquet_table(
        files: usize,
        order: Vec<Sort>,
    ) -> Result<(TempDir, SessionContext)> {
        let dir = fixture().await?;
        if files == 2 {
            std::fs::copy(
                dir.path().join("data.parquet"),
                dir.path().join("copy.parquet"),
            )?;
        }
        let config = SessionConfig::new()
            .with_target_partitions(files)
            // Keep nested sort keys on the operator instead of extracting them into the scan.
            .set_bool(
                "datafusion.optimizer.enable_leaf_expression_pushdown",
                false,
            )
            .set_bool("datafusion.optimizer.prefer_existing_sort", true);
        let ctx = SessionContext::new_with_config(config);
        let mut options = ParquetReadOptions::new();
        if !order.is_empty() {
            options = options.file_sort_order(vec![order]);
        }
        ctx.register_parquet("t", dir.path().to_str().unwrap(), options)
            .await?;
        Ok((dir, ctx))
    }

    // Run the SQL with and without hints. Return the hinted plan for the operator assertion.
    async fn assert_narrow_read(
        df: DataFrame,
        fields: &[&str],
        rows: &[Option<i64>],
    ) -> Result<String> {
        let task = Arc::new(df.task_ctx());
        let baseline = df.create_physical_plan().await?;
        let hinted = PushAllProjectionHints {}.optimize(
            df.create_physical_plan().await?,
            task.session_config().options(),
        )?;
        let physical_plan = displayable(hinted.as_ref()).indent(false).to_string();
        let mut paths = vec![];
        hinted.apply(|p| {
            if let Some((_, source)) = p
                .downcast_ref::<DataSourceExec>()
                .and_then(|s| s.downcast_to_file_source::<ParquetSource>())
            {
                let schema = source.table_schema().file_schema();
                let hints = projection_specifier(
                    Arc::clone(schema),
                    source.projection().unwrap(),
                    Some(&source.projection_hints),
                    &source.projection_hints_indices,
                );
                paths.extend(splat_columns(
                    schema,
                    &hints.keys().copied().collect::<Vec<_>>(),
                    &hints,
                ));
            }
            Ok(TreeNodeRecursion::Continue)
        })?;
        paths.sort();
        assert_eq!(paths, fields, "{physical_plan}");

        for plan in [&baseline, &hinted] {
            let batches = collect(Arc::clone(plan), Arc::clone(&task)).await?;
            let actual: Vec<_> = batches
                .iter()
                .flat_map(|b| b.column(0).as_primitive::<Int64Type>().iter())
                .collect();
            assert_eq!(actual, rows, "{physical_plan}");
        }
        let before = bytes_scanned(&baseline)?;
        let after = bytes_scanned(&hinted)?;
        assert!(
            0 < after && after < before,
            "bytes_scanned: {before} -> {after}\n{physical_plan}"
        );
        Ok(physical_plan)
    }

    #[tokio::test]
    async fn sort_exec() -> Result<()> {
        let (_dir, ctx) = parquet_table(1, vec![]).await?;
        let df = ctx
            .sql("SELECT s['x'] AS x FROM t ORDER BY s['y'] DESC NULLS FIRST")
            .await?;
        let plan =
            assert_narrow_read(df, &["s.x", "s.y"], &[None, Some(2), Some(1)]).await?;
        assert!(
            plan.contains("SortExec: expr=[get_field(s@1, y) DESC]"),
            "{plan}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn sort_preserving_merge_exec() -> Result<()> {
        let (_dir, ctx) =
            parquet_table(2, vec![get_field(col("s"), "y").sort(true, false)]).await?;
        let df = ctx.sql("SELECT s['x'] AS x FROM t ORDER BY s['y']").await?;
        let plan = assert_narrow_read(
            df,
            &["s.x", "s.y"],
            &[Some(1), Some(1), Some(2), Some(2), None, None],
        )
        .await?;
        assert!(
            plan.contains("SortPreservingMergeExec: [get_field(s@1, y) ASC NULLS LAST]"),
            "{plan}"
        );
        assert!(!plan.contains("SortExec:"), "{plan}");
        Ok(())
    }

    #[tokio::test]
    async fn repartition_exec() -> Result<()> {
        let order = vec![
            col("id").sort(true, false),
            get_field(col("s"), "y").sort(true, false),
        ];
        let (_dir, ctx) = parquet_table(2, order).await?;
        let df = ctx
            .sql(
                "SELECT s['x'] AS x FROM t
                 QUALIFY row_number() OVER (PARTITION BY id) <= 2 ORDER BY x",
            )
            .await?;
        let plan = assert_narrow_read(
            df,
            &["id", "s.x", "s.y"],
            &[Some(1), Some(1), Some(2), Some(2), None, None],
        )
        .await?;
        // The window needs id. Only the repartition's merge reads s.y.
        let mut nodes = plan
            .lines()
            .skip_while(|line| !line.contains("RepartitionExec:"));
        let repartition = nodes.next().expect(&plan);
        assert!(repartition.contains("Hash([id@0], 2)"), "{plan}");
        assert!(repartition.contains("preserve_order=true"), "{plan}");
        assert!(
            repartition.contains("get_field(s@1, y) ASC NULLS LAST"),
            "{plan}"
        );
        assert!(
            nodes
                .next()
                .is_some_and(|line| line.trim_start().starts_with("DataSourceExec:")),
            "{plan}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn coalesce_partitions_exec() -> Result<()> {
        let (_dir, ctx) = parquet_table(2, vec![]).await?;
        let df = ctx
            .sql("SELECT s['x'] AS x FROM (SELECT s FROM t LIMIT 6) ORDER BY x")
            .await?;
        let plan = assert_narrow_read(
            df,
            &["s.x"],
            &[Some(1), Some(1), Some(2), Some(2), None, None],
        )
        .await?;
        assert!(plan.contains("CoalescePartitionsExec: fetch=6"), "{plan}");
        Ok(())
    }

    #[tokio::test]
    async fn global_limit_exec() -> Result<()> {
        let (_dir, ctx) = parquet_table(1, vec![]).await?;
        let df = ctx
            .sql("SELECT s['x'] AS x FROM t LIMIT 2 OFFSET 1")
            .await?;
        let plan = assert_narrow_read(df, &["s.x"], &[Some(2), None]).await?;
        assert!(plan.contains("GlobalLimitExec: skip=1, fetch=2"), "{plan}");
        Ok(())
    }

    #[tokio::test]
    async fn local_limit_exec() -> Result<()> {
        let (_dir, ctx) = parquet_table(2, vec![]).await?;
        let df = ctx
            .sql(
                "SELECT s['x'] AS x FROM (VALUES (2)) AS b(k)
                          JOIN t ON t.id < b.k LIMIT 1",
            )
            .await?;
        let plan = assert_narrow_read(df, &["id", "s.x"], &[Some(1)]).await?;
        assert!(plan.contains("LocalLimitExec: fetch=1"), "{plan}");
        Ok(())
    }

    #[tokio::test]
    async fn cooperative_exec() -> Result<()> {
        // Parquet already yields cooperatively. Use a wrapped table as an external provider would.
        #[derive(Debug)]
        struct WrappedTable(Arc<dyn TableProvider>);
        #[async_trait::async_trait]
        impl TableProvider for WrappedTable {
            fn schema(&self) -> SchemaRef {
                self.0.schema()
            }
            fn table_type(&self) -> TableType {
                self.0.table_type()
            }
            async fn scan(
                &self,
                state: &dyn Session,
                projection: Option<&Vec<usize>>,
                filters: &[Expr],
                limit: Option<usize>,
            ) -> Result<Arc<dyn ExecutionPlan>> {
                Ok(Arc::new(CooperativeExec::new(
                    self.0.scan(state, projection, filters, limit).await?,
                )))
            }
        }
        let (_dir, ctx) = parquet_table(1, vec![]).await?;
        ctx.register_table(
            "wrapped",
            Arc::new(WrappedTable(ctx.table_provider("t").await?)),
        )?;
        let df = ctx
            .sql("SELECT s['x'] AS x FROM wrapped LIMIT 2 OFFSET 1")
            .await?;
        let plan = assert_narrow_read(df, &["s.x"], &[Some(2), None]).await?;
        assert!(plan.contains("CooperativeExec\n"), "{plan}");
        Ok(())
    }
}
