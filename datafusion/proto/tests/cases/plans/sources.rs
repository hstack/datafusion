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

//! Scans and data sources: file formats, `FileScanConfig`, listing tables
//! and memory sources.

use super::{
    all_types_context, roundtrip_test, roundtrip_test_and_return,
    roundtrip_test_sql_with_context,
};
use arrow::array::RecordBatch;
use arrow::datatypes::Fields;
use datafusion::arrow::compute::kernels::sort::SortOptions;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::datasource::empty::EmptyTable;
use datafusion::datasource::file_format::json::JsonFormat;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl, PartitionedFile,
};
use datafusion::datasource::object_store::ObjectStoreUrl;
use datafusion::datasource::physical_plan::{
    ArrowSource, CsvSource, FileGroup, FileScanConfig, FileScanConfigBuilder, JsonSource,
    ParquetSource, wrap_partition_type_in_dict, wrap_partition_value_in_dict,
};
use datafusion::datasource::source::DataSourceExec;
use datafusion::execution::TaskContext;
use datafusion::logical_expr::Operator;
use datafusion::physical_expr::LexOrdering;
use datafusion::physical_plan::expressions::{
    BinaryExpr, Column, PhysicalSortExpr, col, lit,
};
use datafusion::physical_plan::filter::FilterExecBuilder;
use datafusion::physical_plan::{
    ExecutionPlan, Partitioning, PhysicalExpr, RangePartitioning, SplitPoint, Statistics,
    displayable,
};
use datafusion::prelude::SessionContext;
use datafusion::scalar::ScalarValue;
use datafusion_common::config::TableParquetOptions;
use datafusion_common::stats::Precision;
use datafusion_common::{DataFusionError, Result, internal_datafusion_err, internal_err};
use datafusion_datasource::{TableSchema, TableSchemaBuilder};
use datafusion_expr::ColumnarValue;
use datafusion_physical_expr_common::physical_expr::proto_decode::PhysicalExprDecodeCtx;
use datafusion_physical_expr_common::physical_expr::proto_encode::PhysicalExprEncodeCtx;
use datafusion_proto::physical_plan::{
    AsExecutionPlan, DefaultPhysicalExtensionCodec, DefaultPhysicalProtoConverter,
    PhysicalExtensionCodec, PhysicalProtoConverterExtension,
};
use datafusion_proto::protobuf::PhysicalPlanNode;
use prost::Message;
use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::sync::Arc;
use std::vec;

#[test]
fn roundtrip_parquet_exec_with_pruning_predicate() -> Result<()> {
    let file_schema =
        Arc::new(Schema::new(vec![Field::new("col", DataType::Utf8, false)]));

    let predicate = Arc::new(BinaryExpr::new(
        Arc::new(Column::new("col", 1)),
        Operator::Eq,
        lit("1"),
    ));

    let mut options = TableParquetOptions::new();
    options.global.pushdown_filters = true;

    let file_source = Arc::new(
        ParquetSource::new(Arc::clone(&file_schema))
            .with_table_parquet_options(options)
            .with_predicate(predicate),
    );

    let scan_config =
        FileScanConfigBuilder::new(ObjectStoreUrl::local_filesystem(), file_source)
            .with_file_groups(vec![FileGroup::new(vec![PartitionedFile::new(
                "/path/to/file.parquet".to_string(),
                1024,
            )])])
            .with_statistics(Statistics {
                num_rows: Precision::Inexact(100),
                total_byte_size: Precision::Inexact(1024),
                column_statistics: Statistics::unknown_column(&Arc::new(Schema::new(
                    vec![Field::new("col", DataType::Utf8, false)],
                ))),
            })
            .build();

    roundtrip_test(DataSourceExec::from_data_source(scan_config))
}

#[test]
fn roundtrip_parquet_exec_attaches_cached_reader_factory_after_roundtrip() -> Result<()> {
    let file_schema =
        Arc::new(Schema::new(vec![Field::new("col", DataType::Utf8, false)]));
    let file_source = Arc::new(ParquetSource::new(Arc::clone(&file_schema)));
    let scan_config =
        FileScanConfigBuilder::new(ObjectStoreUrl::local_filesystem(), file_source)
            .with_file_groups(vec![FileGroup::new(vec![PartitionedFile::new(
                "/path/to/file.parquet".to_string(),
                1024,
            )])])
            .with_statistics(Statistics {
                num_rows: Precision::Inexact(100),
                total_byte_size: Precision::Inexact(1024),
                column_statistics: Statistics::unknown_column(&file_schema),
            })
            .build();
    let exec_plan = DataSourceExec::from_data_source(scan_config);

    let ctx = SessionContext::new();
    let codec = DefaultPhysicalExtensionCodec {};
    let proto_converter = DefaultPhysicalProtoConverter {};
    let roundtripped =
        roundtrip_test_and_return(exec_plan, &ctx, &codec, &proto_converter)?;

    let data_source = roundtripped
        .downcast_ref::<DataSourceExec>()
        .ok_or_else(|| {
            internal_datafusion_err!("Expected DataSourceExec after roundtrip")
        })?;
    let file_scan = data_source
        .data_source()
        .downcast_ref::<FileScanConfig>()
        .ok_or_else(|| {
            internal_datafusion_err!("Expected FileScanConfig after roundtrip")
        })?;
    let parquet_source = file_scan
        .file_source()
        .downcast_ref::<ParquetSource>()
        .ok_or_else(|| {
            internal_datafusion_err!("Expected ParquetSource after roundtrip")
        })?;

    assert!(
        parquet_source.parquet_file_reader_factory().is_some(),
        "Parquet reader factory should be attached after decoding from protobuf"
    );
    Ok(())
}

#[test]
fn roundtrip_parquet_exec_with_unregistered_object_store() -> Result<()> {
    let file_schema =
        Arc::new(Schema::new(vec![Field::new("col", DataType::Utf8, false)]));
    let file_source = Arc::new(ParquetSource::new(Arc::clone(&file_schema)));
    let scan_config = FileScanConfigBuilder::new(
        ObjectStoreUrl::parse("s3://unregistered-bucket")?,
        file_source,
    )
    .with_file_groups(vec![FileGroup::new(vec![PartitionedFile::new(
        "path/to/file.parquet".to_string(),
        1024,
    )])])
    .build();
    let exec_plan = DataSourceExec::from_data_source(scan_config);

    // The store is registered by the caller after decoding.
    let ctx = SessionContext::new();
    let codec = DefaultPhysicalExtensionCodec {};
    let proto_converter = DefaultPhysicalProtoConverter {};
    let roundtripped =
        roundtrip_test_and_return(exec_plan, &ctx, &codec, &proto_converter)?;

    let parquet_source = roundtripped
        .downcast_ref::<DataSourceExec>()
        .and_then(|exec| exec.data_source().downcast_ref::<FileScanConfig>())
        .and_then(|scan| scan.file_source().downcast_ref::<ParquetSource>())
        .ok_or_else(|| {
            internal_datafusion_err!("Expected Parquet scan after roundtrip")
        })?;

    assert!(
        parquet_source.parquet_file_reader_factory().is_none(),
        "Execution should use the default reader factory once the store is registered"
    );
    Ok(())
}

#[test]
fn roundtrip_arrow_scan() -> Result<()> {
    let file_schema =
        Arc::new(Schema::new(vec![Field::new("col", DataType::Utf8, false)]));

    let table_schema = TableSchema::from(&file_schema);
    let file_source = Arc::new(ArrowSource::new_file_source(table_schema));

    let scan_config =
        FileScanConfigBuilder::new(ObjectStoreUrl::local_filesystem(), file_source)
            .with_file_groups(vec![FileGroup::new(vec![PartitionedFile::new(
                "/path/to/file.arrow".to_string(),
                1024,
            )])])
            .with_statistics(Statistics {
                num_rows: Precision::Inexact(100),
                total_byte_size: Precision::Inexact(1024),
                column_statistics: Statistics::unknown_column(&file_schema),
            })
            .build();

    roundtrip_test(DataSourceExec::from_data_source(scan_config))
}

#[test]
fn roundtrip_json_scan() -> Result<()> {
    let file_schema =
        Arc::new(Schema::new(vec![Field::new("col", DataType::Utf8, false)]));
    let file_source = Arc::new(JsonSource::new(TableSchema::from(&file_schema)));
    let scan_config =
        FileScanConfigBuilder::new(ObjectStoreUrl::local_filesystem(), file_source)
            .with_file_groups(vec![FileGroup::new(vec![PartitionedFile::new(
                "/path/to/file.json".to_string(),
                1024,
            )])])
            .build();
    roundtrip_test(DataSourceExec::from_data_source(scan_config))
}

#[cfg(feature = "avro")]
#[test]
fn roundtrip_avro_scan() -> Result<()> {
    use datafusion_datasource_avro::source::AvroSource;

    let file_schema =
        Arc::new(Schema::new(vec![Field::new("col", DataType::Utf8, false)]));
    let file_source = Arc::new(AvroSource::new(TableSchema::from(&file_schema)));
    let scan_config =
        FileScanConfigBuilder::new(ObjectStoreUrl::local_filesystem(), file_source)
            .with_file_groups(vec![FileGroup::new(vec![PartitionedFile::new(
                "/path/to/file.avro".to_string(),
                1024,
            )])])
            .build();
    roundtrip_test(DataSourceExec::from_data_source(scan_config))
}

#[test]
fn roundtrip_csv_scan_preserves_format_options() -> Result<()> {
    use datafusion::common::config::CsvOptions;

    let file_schema =
        Arc::new(Schema::new(vec![Field::new("col", DataType::Utf8, false)]));
    let table_schema = TableSchema::from(&file_schema);
    let file_source =
        Arc::new(CsvSource::new(table_schema).with_csv_options(CsvOptions {
            has_header: Some(false),
            delimiter: b'|',
            quote: b'\'',
            escape: Some(b'\\'),
            comment: Some(b'#'),
            newlines_in_values: Some(true),
            truncated_rows: Some(true),
            ..Default::default()
        }));

    let scan_config =
        FileScanConfigBuilder::new(ObjectStoreUrl::local_filesystem(), file_source)
            .with_file_groups(vec![FileGroup::new(vec![PartitionedFile::new(
                "/path/to/file.csv".to_string(),
                1024,
            )])])
            .build();

    let ctx = SessionContext::new();
    let roundtripped = roundtrip_test_and_return(
        DataSourceExec::from_data_source(scan_config),
        &ctx,
        &DefaultPhysicalExtensionCodec {},
        &DefaultPhysicalProtoConverter {},
    )?;
    let data_source = roundtripped
        .downcast_ref::<DataSourceExec>()
        .ok_or_else(|| internal_datafusion_err!("Expected DataSourceExec"))?;
    let file_scan = data_source
        .data_source()
        .downcast_ref::<FileScanConfig>()
        .ok_or_else(|| internal_datafusion_err!("Expected FileScanConfig"))?;
    let csv_source = file_scan
        .file_source()
        .downcast_ref::<CsvSource>()
        .ok_or_else(|| internal_datafusion_err!("Expected CsvSource"))?;

    assert!(!csv_source.has_header());
    assert_eq!(csv_source.delimiter(), b'|');
    assert_eq!(csv_source.quote(), b'\'');
    assert_eq!(csv_source.escape(), Some(b'\\'));
    assert_eq!(csv_source.comment(), Some(b'#'));
    assert!(csv_source.newlines_in_values());
    assert!(csv_source.truncate_rows());
    Ok(())
}

#[tokio::test]
async fn roundtrip_parquet_exec_with_table_partition_cols() -> Result<()> {
    let mut file_group =
        PartitionedFile::new("/path/to/part=0/file.parquet".to_string(), 1024);
    file_group.partition_values =
        vec![wrap_partition_value_in_dict(ScalarValue::Int64(Some(0)))];
    let schema = Arc::new(Schema::new(vec![Field::new("col", DataType::Utf8, false)]));

    let table_schema = TableSchemaBuilder::from(&schema)
        .with_table_partition_cols(vec![Arc::new(Field::new(
            "part".to_string(),
            wrap_partition_type_in_dict(DataType::Int16),
            false,
        ))])
        .build();

    let file_source = Arc::new(ParquetSource::new(table_schema.clone()));
    let scan_config =
        FileScanConfigBuilder::new(ObjectStoreUrl::local_filesystem(), file_source)
            .with_projection_indices(Some(vec![0, 1]))?
            .with_file_group(FileGroup::new(vec![file_group]))
            .build();

    roundtrip_test(DataSourceExec::from_data_source(scan_config))
}

#[test]
fn roundtrip_parquet_exec_with_custom_predicate_expr() -> Result<()> {
    let file_schema =
        Arc::new(Schema::new(vec![Field::new("col", DataType::Utf8, false)]));

    let custom_predicate_expr = Arc::new(CustomPredicateExpr {
        inner: Arc::new(Column::new("col", 1)),
    });

    let file_source = Arc::new(
        ParquetSource::new(Arc::clone(&file_schema))
            .with_predicate(custom_predicate_expr),
    );

    let scan_config =
        FileScanConfigBuilder::new(ObjectStoreUrl::local_filesystem(), file_source)
            .with_file_groups(vec![FileGroup::new(vec![PartitionedFile::new(
                "/path/to/file.parquet".to_string(),
                1024,
            )])])
            .with_statistics(Statistics {
                num_rows: Precision::Inexact(100),
                total_byte_size: Precision::Inexact(1024),
                column_statistics: Statistics::unknown_column(&Arc::new(Schema::new(
                    vec![Field::new("col", DataType::Utf8, false)],
                ))),
            })
            .build();

    #[derive(Debug, Clone, Eq)]
    struct CustomPredicateExpr {
        inner: Arc<dyn PhysicalExpr>,
    }

    // Manually derive PartialEq and Hash to work around https://github.com/rust-lang/rust/issues/78808
    impl PartialEq for CustomPredicateExpr {
        fn eq(&self, other: &Self) -> bool {
            self.inner.eq(&other.inner)
        }
    }

    impl std::hash::Hash for CustomPredicateExpr {
        fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
            self.inner.hash(state);
        }
    }

    impl Display for CustomPredicateExpr {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            write!(f, "CustomPredicateExpr")
        }
    }

    impl PhysicalExpr for CustomPredicateExpr {
        fn data_type(&self, _input_schema: &Schema) -> Result<DataType> {
            unreachable!()
        }

        fn nullable(&self, _input_schema: &Schema) -> Result<bool> {
            unreachable!()
        }

        fn evaluate(&self, _batch: &RecordBatch) -> Result<ColumnarValue> {
            unreachable!()
        }

        fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
            vec![&self.inner]
        }

        fn with_new_children(
            self: Arc<Self>,
            _children: Vec<Arc<dyn PhysicalExpr>>,
        ) -> Result<Arc<dyn PhysicalExpr>> {
            Ok(self)
        }

        fn fmt_sql(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            Display::fmt(self, f)
        }
    }

    #[derive(Debug)]
    struct CustomPhysicalExtensionCodec;
    impl PhysicalExtensionCodec for CustomPhysicalExtensionCodec {
        fn try_decode(
            &self,
            _buf: &[u8],
            _inputs: &[Arc<dyn ExecutionPlan>],
            _ctx: &TaskContext,
            _proto_converter: &dyn PhysicalProtoConverterExtension,
        ) -> Result<Arc<dyn ExecutionPlan>> {
            unreachable!()
        }

        fn try_encode(
            &self,
            _node: Arc<dyn ExecutionPlan>,
            _buf: &mut Vec<u8>,
            _proto_converter: &dyn PhysicalProtoConverterExtension,
        ) -> Result<()> {
            unreachable!()
        }

        fn try_decode_expr(
            &self,
            buf: &[u8],
            inputs: &[Arc<dyn PhysicalExpr>],
            _ctx: &PhysicalExprDecodeCtx<'_>,
        ) -> Result<Arc<dyn PhysicalExpr>> {
            if buf == "CustomPredicateExpr".as_bytes() {
                Ok(Arc::new(CustomPredicateExpr {
                    inner: inputs[0].clone(),
                }))
            } else {
                internal_err!("Not supported")
            }
        }

        fn try_encode_expr(
            &self,
            node: &Arc<dyn PhysicalExpr>,
            buf: &mut Vec<u8>,
            _ctx: &PhysicalExprEncodeCtx<'_>,
        ) -> Result<()> {
            if node.downcast_ref::<CustomPredicateExpr>().is_some() {
                buf.extend_from_slice("CustomPredicateExpr".as_bytes());
                Ok(())
            } else {
                internal_err!("Not supported")
            }
        }
    }

    let exec_plan = DataSourceExec::from_data_source(scan_config);

    let ctx = SessionContext::new();
    roundtrip_test_and_return(
        exec_plan,
        &ctx,
        &CustomPhysicalExtensionCodec {},
        &DefaultPhysicalProtoConverter {},
    )?;
    Ok(())
}

#[tokio::test]
async fn roundtrip_json_source() -> Result<()> {
    let ctx = SessionContext::new();
    ctx.register_json("t1", "../core/tests/data/1.json", Default::default())
        .await?;
    let plan = ctx.table("t1").await?.create_physical_plan().await?;
    roundtrip_test(plan)
}

#[tokio::test]
async fn roundtrip_coalesce() -> Result<()> {
    let ctx = SessionContext::new();
    ctx.register_table(
        "t",
        Arc::new(EmptyTable::new(Arc::new(Schema::new(Fields::from([
            Arc::new(Field::new("f", DataType::Int64, false)),
        ]))))),
    )?;
    let df = ctx.sql("select coalesce(f) as f from t").await?;
    let plan = df.create_physical_plan().await?;

    let node = PhysicalPlanNode::try_from_physical_plan(
        plan.clone(),
        &DefaultPhysicalExtensionCodec {},
    )?;
    let node = PhysicalPlanNode::decode(node.encode_to_vec().as_slice())
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let restored =
        node.try_into_physical_plan(&ctx.task_ctx(), &DefaultPhysicalExtensionCodec {})?;

    assert_eq!(
        plan.schema(),
        restored.schema(),
        "Schema mismatch for plans:\n>> initial:\n{}>> final: \n{}",
        displayable(plan.as_ref())
            .set_show_schema(true)
            .indent(true),
        displayable(restored.as_ref())
            .set_show_schema(true)
            .indent(true),
    );

    Ok(())
}

#[tokio::test]
async fn roundtrip_generate_series() -> Result<()> {
    let ctx = SessionContext::new();
    ctx.register_table(
        "t",
        Arc::new(EmptyTable::new(Arc::new(Schema::new(Fields::from([
            Arc::new(Field::new("f", DataType::Int64, false)),
        ]))))),
    )?;
    let df = ctx.sql("select * from generate_series(1, 10000)").await?;
    let plan = df.create_physical_plan().await?;

    let node = PhysicalPlanNode::try_from_physical_plan(
        plan.clone(),
        &DefaultPhysicalExtensionCodec {},
    )?;
    let node = PhysicalPlanNode::decode(node.encode_to_vec().as_slice())
        .map_err(|e| DataFusionError::External(Box::new(e)))?;
    let restored =
        node.try_into_physical_plan(&ctx.task_ctx(), &DefaultPhysicalExtensionCodec {})?;

    assert_eq!(
        plan.schema(),
        restored.schema(),
        "Schema mismatch for plans:\n>> initial:\n{}>> final: \n{}",
        displayable(plan.as_ref())
            .set_show_schema(true)
            .indent(true),
        displayable(restored.as_ref())
            .set_show_schema(true)
            .indent(true),
    );

    Ok(())
}

#[tokio::test]
async fn roundtrip_projection_source() -> Result<()> {
    let schema = Arc::new(Schema::new(Fields::from([
        Arc::new(Field::new("a", DataType::Utf8, false)),
        Arc::new(Field::new("b", DataType::Utf8, false)),
        Arc::new(Field::new("c", DataType::Int32, false)),
        Arc::new(Field::new("d", DataType::Int32, false)),
    ])));

    let statistics = Statistics::new_unknown(&schema);

    let file_source = Arc::new(ParquetSource::new(Arc::clone(&schema)));
    let scan_config =
        FileScanConfigBuilder::new(ObjectStoreUrl::local_filesystem(), file_source)
            .with_file_groups(vec![FileGroup::new(vec![PartitionedFile::new(
                "/path/to/file.parquet".to_string(),
                1024,
            )])])
            .with_statistics(statistics)
            .with_projection_indices(Some(vec![0, 1, 2]))?
            .build();

    let filter = Arc::new(
        FilterExecBuilder::new(
            Arc::new(BinaryExpr::new(col("c", &schema)?, Operator::Eq, lit(1))),
            DataSourceExec::from_data_source(scan_config),
        )
        .apply_projection(Some(vec![0, 1]))?
        .build()?,
    );

    roundtrip_test(filter)
}

#[tokio::test]
async fn roundtrip_parquet_select_star() -> Result<()> {
    let ctx = all_types_context().await?;
    let sql = "select * from alltypes_plain";
    roundtrip_test_sql_with_context(sql, &ctx).await
}

#[tokio::test]
async fn roundtrip_parquet_select_projection() -> Result<()> {
    let ctx = all_types_context().await?;
    let sql = "select string_col, timestamp_col from alltypes_plain";
    roundtrip_test_sql_with_context(sql, &ctx).await
}

#[tokio::test]
async fn roundtrip_parquet_select_star_predicate() -> Result<()> {
    let ctx = all_types_context().await?;
    let sql = "select * from alltypes_plain where id > 4";
    roundtrip_test_sql_with_context(sql, &ctx).await
}

#[tokio::test]
async fn roundtrip_parquet_select_projection_predicate() -> Result<()> {
    let ctx = all_types_context().await?;
    let sql = "select string_col, timestamp_col from alltypes_plain where id > 4";
    roundtrip_test_sql_with_context(sql, &ctx).await
}

#[tokio::test]
async fn roundtrip_parquet_deep_projection_hints() -> Result<()> {
    use arrow::util::pretty::pretty_format_batches;
    use datafusion::execution::SessionStateBuilder;
    use datafusion::prelude::{ParquetReadOptions, SessionConfig};
    use datafusion_common::tree_node::{TreeNode, TreeNodeRecursion};
    use datafusion_datasource_parquet::push_all_projection_hints::PushAllProjectionHints;
    use datafusion_physical_plan::collect;
    use datafusion_proto::bytes::{physical_plan_from_bytes, physical_plan_to_bytes};

    fn hints(plan: &Arc<dyn ExecutionPlan>) -> Result<Vec<(Vec<String>, Vec<usize>)>> {
        let mut hints = vec![];
        plan.apply(|plan| {
            if let Some(scan) = plan.downcast_ref::<DataSourceExec>()
                && let Some((_, source)) = scan.downcast_to_file_source::<ParquetSource>()
            {
                hints.push((
                    source
                        .projection_hints
                        .iter()
                        .map(|hint| format!("{:?}", hint.expr))
                        .collect(),
                    source.projection_hints_indices.clone(),
                ));
            }
            Ok(TreeNodeRecursion::Continue)
        })?;
        Ok(hints)
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

    let directory = tempfile::tempdir()?;
    let path = directory.path().join("deep.parquet");
    let config = SessionConfig::new().with_target_partitions(1);
    let baseline = SessionContext::new_with_config(config.clone());
    baseline
        .sql(&format!(
            "COPY (SELECT id, 'prefix' AS unused,
         named_struct('x', id, 'pad', repeat('padding', 1024)) AS s,
         [named_struct('x', id, 'pad', repeat('padding', 1024)), NULL] AS events,
         MAP {{'k': named_struct('x', id, 'pad', repeat('padding', 1024))}} AS m
         FROM (VALUES (1), (2), (3)) t(id)) TO '{}' STORED AS PARQUET",
            path.display()
        ))
        .await?
        .collect()
        .await?;
    let ctx = SessionContext::new_with_state(
        SessionStateBuilder::new()
            .with_config(config)
            .with_default_features()
            .with_physical_optimizer_rule(Arc::new(PushAllProjectionHints {}))
            .build(),
    );
    for context in [&baseline, &ctx] {
        context
            .register_parquet(
                "deep",
                path.to_str().unwrap(),
                ParquetReadOptions::default(),
            )
            .await?;
    }
    for (query, prune) in [
        (
            "SELECT events[1]['x'], m['k']['x'] FROM deep ORDER BY id",
            true,
        ),
        (
            "SELECT s['x'], events[1]['x'], m['k']['x'], rn FROM (
            SELECT s, events, m, row_number() OVER (ORDER BY id) AS rn FROM deep
         ) q ORDER BY rn",
            true,
        ),
        ("SELECT 1 AS value FROM deep", false),
    ] {
        let base_plan = baseline.sql(query).await?.create_physical_plan().await?;
        let expected = collect(Arc::clone(&base_plan), baseline.task_ctx()).await?;
        let plan = ctx.sql(query).await?.create_physical_plan().await?;
        let signature = hints(&plan)?;
        assert!(!signature.is_empty());
        assert!(signature.iter().all(|(hints, _)| hints.is_empty() != prune));
        let encoded = physical_plan_to_bytes(Arc::clone(&plan))?;
        let restored = physical_plan_from_bytes(&encoded, &ctx.task_ctx())?;
        let plans = vec![plan, restored];
        #[cfg(feature = "json")]
        let plans = {
            let mut plans = plans;
            use datafusion_proto::bytes::{
                physical_plan_from_json, physical_plan_to_json,
            };
            let json = physical_plan_to_json(Arc::clone(&plans[0]))?;
            if prune {
                assert!(json.contains("projectionHints"));
            }
            plans.push(physical_plan_from_json(&json, &ctx.task_ctx())?);
            plans
        };
        let mut reads = vec![];
        for plan in plans {
            assert_eq!(hints(&plan)?, signature);
            let batches = collect(Arc::clone(&plan), ctx.task_ctx()).await?;
            assert_eq!(plan.schema(), base_plan.schema());
            assert_eq!(
                pretty_format_batches(&batches)?.to_string(),
                pretty_format_batches(&expected)?.to_string()
            );
            reads.push(bytes_scanned(&plan)?);
        }
        if prune {
            assert!(reads[0] < bytes_scanned(&base_plan)?, "{query}: {reads:?}");
        } else {
            assert_eq!(reads[0], bytes_scanned(&base_plan)?, "{query}");
        }
        assert!(
            reads.iter().all(|&bytes| bytes == reads[0]),
            "{query}: {reads:?}"
        );
    }
    Ok(())
}

#[test]
fn parquet_deep_projection_rejects_malformed_hints_and_accepts_legacy_plans() -> Result<()>
{
    use datafusion_datasource::file::FileSource;
    use datafusion_physical_expr::projection::{ProjectionExpr, ProjectionExprs};

    let schema = Arc::new(Schema::new(vec![
        Field::new("unused", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
    ]));
    let config = FileScanConfigBuilder::new(
        ObjectStoreUrl::local_filesystem(),
        Arc::new(ParquetSource::new(schema)),
    )
    .with_projection_indices(Some(vec![1]))?
    .build();
    let mut source = config
        .file_source()
        .downcast_ref::<ParquetSource>()
        .unwrap()
        .clone();
    source.projection_hints = ProjectionExprs::new([ProjectionExpr::new(
        Arc::new(Column::new("value", 1)),
        "",
    )]);
    source.projection_hints_indices = vec![0];
    let config = FileScanConfigBuilder::from(config)
        .with_source(Arc::new(source))
        .build();
    let node = PhysicalPlanNode::try_from_physical_plan(
        DataSourceExec::from_data_source(config),
        &DefaultPhysicalExtensionCodec {},
    )?;
    let ctx = SessionContext::new();
    for index in [1, u64::MAX] {
        let mut invalid = node.clone();
        let Some(
            datafusion_proto::protobuf::physical_plan_node::PhysicalPlanType::ParquetScan(
                scan,
            ),
        ) = &mut invalid.physical_plan_type
        else {
            panic!("Expected a Parquet scan");
        };
        scan.projection_hints_indices = vec![index];
        let error = invalid
            .try_into_physical_plan(&ctx.task_ctx(), &DefaultPhysicalExtensionCodec {})
            .unwrap_err();
        assert!(
            error.to_string().contains("Deep projection hint index"),
            "{error}"
        );
    }
    let mut missing = node.clone();
    let Some(
        datafusion_proto::protobuf::physical_plan_node::PhysicalPlanType::ParquetScan(
            scan,
        ),
    ) = &mut missing.physical_plan_type
    else {
        panic!("Expected a Parquet scan");
    };
    scan.projection_hints.as_mut().unwrap().projections[0].expr = None;
    let error = missing
        .try_into_physical_plan(&ctx.task_ctx(), &DefaultPhysicalExtensionCodec {})
        .unwrap_err();
    assert!(
        error.to_string().contains("missing its expression"),
        "{error}"
    );
    let mut legacy = node;
    let Some(
        datafusion_proto::protobuf::physical_plan_node::PhysicalPlanType::ParquetScan(
            scan,
        ),
    ) = &mut legacy.physical_plan_type
    else {
        panic!("Expected a Parquet scan");
    };
    scan.projection_hints = None;
    let error = legacy
        .try_into_physical_plan(&ctx.task_ctx(), &DefaultPhysicalExtensionCodec {})
        .unwrap_err();
    assert!(
        error.to_string().contains("require projection hints"),
        "{error}"
    );
    let Some(
        datafusion_proto::protobuf::physical_plan_node::PhysicalPlanType::ParquetScan(
            scan,
        ),
    ) = &mut legacy.physical_plan_type
    else {
        panic!("Expected a Parquet scan");
    };
    scan.projection_hints_indices.clear();
    let restored = legacy
        .try_into_physical_plan(&ctx.task_ctx(), &DefaultPhysicalExtensionCodec {})?;
    let (_, source) = restored
        .downcast_ref::<DataSourceExec>()
        .unwrap()
        .downcast_to_file_source::<ParquetSource>()
        .unwrap();
    assert!(source.projection_hints.as_ref().is_empty());
    assert!(source.projection_hints_indices.is_empty());
    assert_eq!(source.projection().unwrap().as_ref().len(), 1);
    Ok(())
}

#[tokio::test]
async fn roundtrip_empty_projection() -> Result<()> {
    let ctx = all_types_context().await?;
    let sql = "select 1 from alltypes_plain";
    roundtrip_test_sql_with_context(sql, &ctx).await
}

#[tokio::test]
async fn roundtrip_memory_source_empty_projection() -> Result<()> {
    // Memory scan: `Some(vec![])` must not decode back as `None`
    let ctx = SessionContext::new();
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![
            Field::new("a", DataType::Utf8, false),
            Field::new("b", DataType::Int64, false),
        ])),
        vec![
            Arc::new(arrow::array::StringArray::from(vec!["Tom"])),
            Arc::new(arrow::array::Int64Array::from(vec![18i64])),
        ],
    )?;
    ctx.register_batch("tmem", batch)?;
    let sql = "select 1 from tmem";
    roundtrip_test_sql_with_context(sql, &ctx).await
}

#[tokio::test]
async fn roundtrip_memory_source() -> Result<()> {
    let ctx = SessionContext::new();
    let plan = ctx
        .sql("select * from values ('Tom', 18)")
        .await?
        .create_physical_plan()
        .await?;
    roundtrip_test(plan)
}

#[tokio::test]
async fn roundtrip_memory_source_sort_information_and_fetch() -> Result<()> {
    use datafusion::datasource::memory::MemorySourceConfig;
    use datafusion::datasource::source::DataSource as _;

    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Utf8, false),
        Field::new("b", DataType::Int64, false),
    ]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![
            Arc::new(arrow::array::StringArray::from(vec!["Tom", "Bob"])),
            Arc::new(arrow::array::Int64Array::from(vec![18i64, 21i64])),
        ],
    )?;
    let ordering = LexOrdering::new(vec![PhysicalSortExpr::new(
        col("b", &schema)?,
        SortOptions {
            descending: true,
            nulls_first: false,
        },
    )])
    .unwrap();
    let source = MemorySourceConfig::try_new(&[vec![batch]], Arc::clone(&schema), None)?
        .with_limit(Some(1))
        .with_show_sizes(false)
        .try_with_sort_information(vec![ordering])?;
    let exec_plan = DataSourceExec::from_data_source(source.clone());

    let ctx = SessionContext::new();
    let codec = DefaultPhysicalExtensionCodec {};
    let proto_converter = DefaultPhysicalProtoConverter {};
    let decoded = roundtrip_test_and_return(exec_plan, &ctx, &codec, &proto_converter)?;

    // The string representation does not include every field; check the
    // decoded source directly.
    let decoded = decoded
        .downcast_ref::<DataSourceExec>()
        .expect("expected DataSourceExec");
    let decoded_source = decoded
        .data_source()
        .downcast_ref::<MemorySourceConfig>()
        .expect("expected MemorySourceConfig");
    assert_eq!(decoded_source.partitions(), source.partitions());
    assert_eq!(decoded_source.original_schema(), source.original_schema());
    assert_eq!(decoded_source.projection(), source.projection());
    assert_eq!(decoded_source.sort_information(), source.sort_information());
    assert_eq!(decoded_source.fetch(), Some(1));
    assert!(!decoded_source.show_sizes());
    Ok(())
}

#[tokio::test]
async fn roundtrip_listing_table_with_schema_metadata() -> Result<()> {
    let ctx = SessionContext::new();
    let file_format = JsonFormat::default();
    let table_partition_cols = vec![("part".to_owned(), DataType::Int64)];
    let data = "../core/tests/data/partitioned_table_json";
    let listing_table_url = ListingTableUrl::parse(data)?;
    let listing_options = ListingOptions::new(Arc::new(file_format))
        .with_table_partition_cols(table_partition_cols);

    let config = ListingTableConfig::new(listing_table_url)
        .with_listing_options(listing_options)
        .infer_schema(&ctx.state())
        .await?;

    // Decorate metadata onto the inferred ListingTable schema
    let schema_with_meta = config
        .file_schema
        .clone()
        .map(|s| {
            let mut meta: HashMap<String, String> = HashMap::new();
            meta.insert("foo.bar".to_string(), "baz".to_string());
            s.as_ref().clone().with_metadata(meta)
        })
        .expect("Must decorate metadata");

    let config = config.with_schema(Arc::new(schema_with_meta));
    ctx.register_table("hive_style", Arc::new(ListingTable::try_new(config)?))?;

    let plan = ctx
        .sql("select * from hive_style limit 1")
        .await?
        .create_physical_plan()
        .await?;

    roundtrip_test(plan)
}

fn roundtrip_file_scan_config(scan_config: FileScanConfig) -> Result<FileScanConfig> {
    let exec_plan: Arc<dyn ExecutionPlan> = DataSourceExec::from_data_source(scan_config);
    let ctx = SessionContext::new();
    let codec = DefaultPhysicalExtensionCodec {};
    let proto_converter = DefaultPhysicalProtoConverter {};
    let result_plan =
        roundtrip_test_and_return(exec_plan, &ctx, &codec, &proto_converter)?;

    let data_source_exec = result_plan
        .downcast_ref::<DataSourceExec>()
        .expect("Expected DataSourceExec");
    let file_scan_config = data_source_exec
        .data_source()
        .downcast_ref::<FileScanConfig>()
        .expect("Expected FileScanConfig");
    Ok(file_scan_config.clone())
}

#[test]
fn roundtrip_parquet_exec_output_partitioning() -> Result<()> {
    let file_schema =
        Arc::new(Schema::new(vec![Field::new("col", DataType::Utf8, false)]));
    let file_source = Arc::new(ParquetSource::new(Arc::clone(&file_schema)));
    let output_partitioning =
        Partitioning::Hash(vec![Arc::new(Column::new("col", 0))], 1);
    let scan_config =
        FileScanConfigBuilder::new(ObjectStoreUrl::local_filesystem(), file_source)
            .with_file_groups(vec![FileGroup::new(vec![PartitionedFile::new(
                "/path/to/file.parquet".to_string(),
                1024,
            )])])
            .with_output_partitioning(Some(output_partitioning.clone()))
            .build();

    assert_eq!(
        roundtrip_file_scan_config(scan_config)?.output_partitioning,
        Some(output_partitioning)
    );

    Ok(())
}

#[test]
fn roundtrip_parquet_exec_range_output_partitioning() -> Result<()> {
    let file_schema =
        Arc::new(Schema::new(vec![Field::new("col", DataType::Int32, false)]));
    let file_source = Arc::new(ParquetSource::new(Arc::clone(&file_schema)));
    let output_partitioning = Partitioning::Range(RangePartitioning::new(
        LexOrdering::new(vec![PhysicalSortExpr::new_default(Arc::new(Column::new(
            "col", 0,
        )))])
        .unwrap(),
        vec![SplitPoint::new(vec![ScalarValue::Int32(Some(10))])],
    ));
    let scan_config =
        FileScanConfigBuilder::new(ObjectStoreUrl::local_filesystem(), file_source)
            .with_file_groups(vec![
                FileGroup::new(vec![PartitionedFile::new(
                    "/path/to/file-1.parquet".to_string(),
                    1024,
                )]),
                FileGroup::new(vec![PartitionedFile::new(
                    "/path/to/file-2.parquet".to_string(),
                    1024,
                )]),
            ])
            .with_output_partitioning(Some(output_partitioning.clone()))
            .build();

    assert_eq!(
        roundtrip_file_scan_config(scan_config)?.output_partitioning,
        Some(output_partitioning)
    );

    Ok(())
}
