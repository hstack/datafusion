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

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{ArrayRef, Int32Array, RecordBatch, StructArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use bytes::Bytes;
use datafusion_common::ScalarValue;
use datafusion_common::config::ConfigOptions;
use datafusion_datasource_parquet::leaves::{
    fix_simplified_column_path, parquet_leaf_paths, projection_specifier, splat_columns,
};
use datafusion_physical_expr::expressions::{Column, Literal};
use datafusion_physical_expr::projection::{ProjectionExpr, ProjectionExprs};
use datafusion_physical_expr::{PhysicalExpr, ScalarFunctionExpr};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::{ArrowWriter, ProjectionMask};

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new(
            "s",
            DataType::Struct(
                ["keep", "other", "detail.name", "keep.extra"]
                    .map(|name| Field::new(name, DataType::Int32, true))
                    .to_vec()
                    .into(),
            ),
            true,
        ),
        Field::new(
            "t",
            DataType::Struct(
                ["keep", "other"]
                    .map(|name| Field::new(name, DataType::Int32, true))
                    .to_vec()
                    .into(),
            ),
            true,
        ),
    ]))
}

fn get_field(schema: &Schema, index: usize, child: &str) -> Arc<dyn PhysicalExpr> {
    Arc::new(
        ScalarFunctionExpr::try_new(
            datafusion_functions::core::get_field(),
            vec![
                Arc::new(Column::new(schema.field(index).name(), index)),
                Arc::new(Literal::new(ScalarValue::Utf8(Some(child.to_owned())))),
            ],
            schema,
            Arc::new(ConfigOptions::default()),
        )
        .unwrap(),
    )
}

fn specifier(
    schema: &SchemaRef,
    fields: &[(usize, &str)],
) -> HashMap<usize, Vec<String>> {
    let indices = (0..schema.fields().len()).collect::<Vec<_>>();
    let projection = ProjectionExprs::from_indices(&indices, schema);
    let hints = ProjectionExprs::new(fields.iter().map(|&(index, child)| {
        ProjectionExpr::new(get_field(schema, index, child), child)
    }));
    projection_specifier(Arc::clone(schema), &projection, Some(&hints), &indices)
}

#[test]
fn literal_child_names_are_not_legacy_nested_paths() {
    let schema = schema();
    for child in ["detail.name", "keep.extra"] {
        // These are literal child names, not nested accesses. The physical
        // expressions are valid even though simplified paths split on dots.
        let expr = get_field(&schema, 0, child);
        assert_eq!(expr.data_type(&schema).unwrap(), DataType::Int32);
        assert!(fix_simplified_column_path(child, schema.field(0)).is_err());
    }
    assert_eq!(
        fix_simplified_column_path("keep", schema.field(0)).unwrap(),
        "keep"
    );
}

#[test]
fn literal_child_names_are_preserved_in_either_hint_order() {
    let schema = schema();
    for child in ["detail.name", "keep.extra"] {
        for hints in [[(0, "keep"), (0, child)], [(0, child), (0, "keep")]] {
            let deep = specifier(&schema, &hints);
            let expected = match child {
                "detail.name" => vec!["detail.name".to_owned(), "keep".to_owned()],
                _ => vec!["keep".to_owned(), "keep.extra".to_owned()],
            };
            assert_eq!(deep, HashMap::from([(0, expected)]), "hints: {hints:?}");
        }
    }
}

#[test]
fn literal_child_names_do_not_change_other_column_requirements() {
    let schema = schema();
    let deep = specifier(&schema, &[(0, "keep"), (1, "keep"), (0, "detail.name")]);
    assert_eq!(deep.get(&1), Some(&vec!["keep".to_owned()]));
    assert_eq!(
        deep.get(&0),
        Some(&vec!["detail.name".to_owned(), "keep".to_owned()])
    );
}

#[test]
fn all_valid_hints_remain_narrowed_and_deduplicated() {
    let schema = schema();
    let deep = specifier(
        &schema,
        &[(0, "other"), (1, "keep"), (0, "keep"), (0, "other")],
    );
    assert_eq!(
        deep,
        HashMap::from([
            (0, vec!["keep".to_owned(), "other".to_owned()]),
            (1, vec!["keep".to_owned()]),
        ])
    );
    assert_eq!(
        splat_columns(&schema, &[0, 1], &deep),
        vec!["s.keep", "s.other", "t.keep"]
    );
}

#[test]
fn multiple_literal_dotted_children_are_preserved() {
    let schema = schema();
    let deep = specifier(&schema, &[(0, "detail.name"), (0, "keep.extra")]);
    assert_eq!(
        deep,
        HashMap::from([(0, vec!["detail.name".to_owned(), "keep.extra".to_owned()])])
    );
}

#[test]
fn legacy_path_fallback_preserves_parquet_schema_and_values() {
    let s = StructArray::from(vec![
        (
            Arc::new(Field::new("keep", DataType::Int32, true)),
            Arc::new(Int32Array::from(vec![Some(1), None, Some(3)])) as ArrayRef,
        ),
        (
            Arc::new(Field::new("detail.name", DataType::Int32, true)),
            Arc::new(Int32Array::from(vec![Some(10), Some(20), None])) as ArrayRef,
        ),
    ]);
    let batch = RecordBatch::try_from_iter([("s", Arc::new(s) as ArrayRef)]).unwrap();
    let schema = batch.schema();
    let mut encoded = Vec::new();
    let mut writer =
        ArrowWriter::try_new(&mut encoded, Arc::clone(&schema), None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    let bytes = Bytes::from(encoded);

    let expected = ParquetRecordBatchReaderBuilder::try_new(bytes.clone())
        .unwrap()
        .build()
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(expected, vec![batch]);

    let deep = specifier(&schema, &[(0, "keep"), (0, "detail.name")]);
    let builder = ParquetRecordBatchReaderBuilder::try_new(bytes).unwrap();
    let leaves =
        parquet_leaf_paths(Arc::clone(&schema), builder.parquet_schema(), &[0], &deep);
    let mask = ProjectionMask::leaves(builder.parquet_schema(), leaves);
    let actual = builder
        .with_projection(mask)
        .build()
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();

    assert_eq!(actual.len(), expected.len());
    assert_eq!(actual[0].schema(), expected[0].schema());
    assert_eq!(actual, expected);
}
