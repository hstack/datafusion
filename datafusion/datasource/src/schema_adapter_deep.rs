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
//! TODO: module doc

use crate::schema_adapter::{SchemaAdapter, SchemaMapper};
use arrow::array::RecordBatch;
use arrow::datatypes::{Fields, Schema, SchemaRef};
use datafusion_common::deep::{
    can_rewrite_field, try_rewrite_record_batch, try_rewrite_record_batch_with_mappings,
};
use datafusion_common::{plan_err, ColumnStatistics};
use log::{error, trace};
use std::sync::Arc;

#[derive(Clone, Debug)]
pub struct NestedSchemaAdapter {
    /// The schema for the table, projected to include only the fields being output (projected) by the
    /// associated ParquetExec
    pub projected_table_schema: SchemaRef,
    /// The entire table schema for the table we're using this to adapt.
    ///
    /// This is used to evaluate any filters pushed down into the scan
    /// which may refer to columns that are not referred to anywhere
    /// else in the plan.
    pub table_schema: SchemaRef,
}

impl NestedSchemaAdapter {
    fn map_schema_nested(
        &self,
        fields: &Fields,
    ) -> datafusion_common::Result<(Arc<NestedSchemaMapping>, Vec<usize>)> {
        let mut projection = Vec::with_capacity(fields.len());
        let mut field_mappings = vec![None; self.table_schema.fields().len()];

        // start from the destination fields
        for (table_idx, table_field) in self.table_schema.fields.iter().enumerate() {
            // if the file exists in the source, check if we can rewrite it to the destination,
            // and add it to the projections
            if let Some((file_idx, file_field)) = fields.find(table_field.name()) {
                if can_rewrite_field(table_field, file_field, true) {
                    field_mappings[table_idx] = Some(projection.len());
                    projection.push(file_idx);
                } else {
                    error!(
                        "Deep adapter: cannot cast file schema field {} of type {:?} to table schema field of type {:?}",
                        file_field.name(),
                        file_field.data_type(),
                        table_field.data_type()
                    );
                    return plan_err!(
                        "Deep adapter: cannot cast file schema field {} of type {:?} to table schema field of type {:?}",
                        file_field.name(),
                        file_field.data_type(),
                        table_field.data_type()
                    );
                }
            }
        }
        Ok((
            Arc::new(NestedSchemaMapping {
                projected_table_schema: self.projected_table_schema.clone(),
                field_mappings,
                table_schema: self.table_schema.clone(),
            }),
            projection,
        ))
    }
}

impl SchemaAdapter for NestedSchemaAdapter {
    fn map_column_index(&self, index: usize, file_schema: &Schema) -> Option<usize> {
        let field = self.projected_table_schema.field(index);
        Some(file_schema.fields.find(field.name())?.0)
    }

    fn map_schema(
        &self,
        file_schema: &Schema,
    ) -> datafusion_common::Result<(Arc<dyn SchemaMapper>, Vec<usize>)> {
        // self.map_schema_nested(file_schema.fields())
        //     .map(|(s, v)| (s as Arc<dyn SchemaMapper>, v))
        // trace!(target: "deep", "map_schema:           file_schema: {:#?}", file_schema);
        // trace!(target: "deep", "map_schema:           table_schema: {:#?}", self.table_schema);
        // trace!(target: "deep", "map_schema: projected_table_schema: {:#?}", self.projected_table_schema);

        let mut projection = Vec::with_capacity(file_schema.fields().len());
        let mut field_mappings = vec![None; self.projected_table_schema.fields().len()];

        for (file_idx, file_field) in file_schema.fields.iter().enumerate() {
            if let Some((table_idx, table_field)) =
                self.projected_table_schema.fields().find(file_field.name())
            {
                match can_rewrite_field(table_field, file_field, true) {
                    true => {
                        field_mappings[table_idx] = Some(projection.len());
                        projection.push(file_idx);
                    }
                    false => {
                        error!(
                            "Deep adapter: cannot cast file schema field {} of type {:#?} to table schema field of type {:#?}",
                            file_field.name(),
                            file_field.data_type(),
                            table_field.data_type()
                        );
                        return plan_err!(
                            "Deep adapter: Cannot cast file schema field {} of type {:?} to table schema field of type {:?}",
                            file_field.name(),
                            file_field.data_type(),
                            table_field.data_type()
                        );
                    }
                }
            }
        }

        Ok((
            Arc::new(NestedSchemaMapping {
                projected_table_schema: self.projected_table_schema.clone(),
                field_mappings,
                table_schema: self.table_schema.clone(),
            }),
            projection,
        ))
    }
}

/// TODO: struct doc
#[derive(Debug)]
pub struct NestedSchemaMapping {
    /// The schema of the table. This is the expected schema after conversion and it should match
    /// the schema of the query result.
    projected_table_schema: SchemaRef,
    /// Mapping from field index in `projected_table_schema` to index in projected file_schema.
    /// They are Options instead of just plain `usize`s because the table could have fields that
    /// don't exist in the file.
    field_mappings: Vec<Option<usize>>,
    /// The entire table schema, as opposed to the projected_table_schema (which only contains the
    /// columns that we are projecting out of this query). This contains all fields in the table,
    /// regardless of if they will be projected out or not.
    table_schema: SchemaRef,
}

impl SchemaMapper for NestedSchemaMapping {
    /// Adapts a `RecordBatch` to match the `projected_table_schema` using the stored mapping and
    /// conversions. The produced RecordBatch has a schema that contains only the projected
    /// columns, so if one needs a RecordBatch with a schema that references columns which are not
    /// in the projected, it would be better to use `map_partial_batch`
    fn map_batch(&self, batch: RecordBatch) -> datafusion_common::Result<RecordBatch> {
        let record_batch = try_rewrite_record_batch_with_mappings(
            batch.schema(),
            batch,
            self.projected_table_schema.clone(),
            // FIXME: @HStack ADR: will this break delta tests ?
            // There are some cases
            self.field_mappings.clone(),
        )?;
        Ok(record_batch)
    }

    fn map_column_statistics(
        &self,
        file_col_statistics: &[ColumnStatistics],
    ) -> datafusion_common::Result<Vec<ColumnStatistics>> {
        let mut table_col_statistics = vec![];

        // Map the statistics for each field in the file schema to the corresponding field in the
        // table schema, if a field is not present in the file schema, we need to fill it with `ColumnStatistics::new_unknown`
        for (_, file_col_idx) in self
            .projected_table_schema
            .fields()
            .iter()
            .zip(&self.field_mappings)
        {
            if let Some(file_col_idx) = file_col_idx {
                table_col_statistics.push(
                    file_col_statistics
                        .get(*file_col_idx)
                        .cloned()
                        .unwrap_or_default(),
                );
            } else {
                table_col_statistics.push(ColumnStatistics::new_unknown());
            }
        }

        Ok(table_col_statistics)
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::{
        ArrayBuilder, BooleanArray, BooleanBuilder, GenericStringBuilder, Int32Builder,
        ListBuilder, RecordBatch, StringArray, StringBuilder, StructArray, StructBuilder,
        UInt32Array, UInt32Builder,
    };
    use arrow::datatypes::{DataType, Field, Fields, Schema, TimeUnit};
    use datafusion::dataframe::DataFrame;
    use datafusion::execution::context::SessionContext;
    use datafusion_catalog::MemTable;
    use datafusion_common::deep::{rewrite_schema, try_rewrite_record_batch};
    use datafusion_optimizer::optimize_projections::OptimizeProjections;
    use datafusion_optimizer::optimizer::{Optimizer, OptimizerContext};
    use datafusion_physical_plan::get_plan_string;
    use log::info;
    use parquet::arrow::parquet_to_arrow_schema;
    use parquet::schema::parser::parse_message_type;
    use parquet::schema::types::SchemaDescriptor;
    use std::collections::HashMap;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_rewrite_schema() -> datafusion_common::Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("i1", DataType::Int32, true),
            Field::new(
                "l1",
                DataType::List(Arc::new(Field::new(
                    "s1",
                    DataType::Struct(Fields::from(vec![
                        Field::new("s1s1", DataType::Utf8, true),
                        Field::new("s1i2", DataType::Int32, true),
                        Field::new(
                            "s1m1",
                            DataType::Map(
                                Arc::new(Field::new(
                                    "entries",
                                    DataType::Struct(Fields::from(vec![
                                        Field::new("key", DataType::Utf8, false),
                                        Field::new("value", DataType::Utf8, false),
                                    ])),
                                    true,
                                )),
                                false,
                            ),
                            true,
                        ),
                        Field::new(
                            "s1l1",
                            DataType::List(Arc::new(Field::new(
                                "s1l1i1",
                                DataType::Date32,
                                true,
                            ))),
                            true,
                        ),
                        // extra field
                        Field::new("s1ts1", DataType::Time32(TimeUnit::Second), true),
                    ])),
                    true,
                ))),
                true,
            ),
        ]));
        let _ = rewrite_schema(
            schema,
            &vec![1],
            &HashMap::from([
                (0, vec![]),
                (1, vec!["*.s1s1".to_string(), "*.s1l1".to_string()]),
            ]),
        );
        // info!("out: {:#?}", out);
        Ok(())
    }

    #[tokio::test]
    async fn test_rewrite() -> datafusion_common::Result<()> {
        let _ = env_logger::try_init();

        let _message_type = "
        message schema {
            REQUIRED INT32 int1;
            OPTIONAL INT32 int2;
            REQUIRED BYTE_ARRAY str1 (UTF8);
            OPTIONAL GROUP stringlist1 (LIST) {
                repeated group list {
                    optional BYTE_ARRAY element (UTF8);
                }
            }
            OPTIONAL group map1 (MAP) {
                REPEATED group map {
                  REQUIRED binary str (UTF8);
                  REQUIRED int32 num;
                }
            }
            OPTIONAL GROUP array_of_arrays (LIST) {
                REPEATED GROUP list {
                    REQUIRED GROUP element (LIST) {
                        REPEATED GROUP list {
                            REQUIRED INT32 element;
                        }
                    }
                }
            }
            REQUIRED GROUP array_of_struct (LIST) {
                REPEATED GROUP struct {
                    REQUIRED BOOLEAN bools;
                    REQUIRED INT32 uint32 (INTEGER(32,false));
                    REQUIRED GROUP   int32 (LIST) {
                        REPEATED GROUP list {
                            OPTIONAL INT32 element;
                        }
                    }
                }
            }
        }
        ";
        let message_type = r#"
            message schema {
                REQUIRED GROUP struct {
                    REQUIRED BINARY name (UTF8);
                    REQUIRED BOOLEAN bools;
                    REQUIRED INT32 uint32 (INTEGER(32,false));
                    REQUIRED GROUP tags (LIST) {
                        REPEATED GROUP tags {
                            OPTIONAL BINARY tag (UTF8);
                        }
                    }
                }
            }
        "#;
        let parquet_schema = parse_message_type(message_type)
            .map(|t| Arc::new(SchemaDescriptor::new(Arc::new(t))))
            .unwrap();

        let arrow_schema =
            Arc::new(parquet_to_arrow_schema(parquet_schema.as_ref(), None).unwrap());
        // println!("schema: {:#?}", arrow_schema);
        let (_idx, ffield) = arrow_schema.fields().find("struct").unwrap();
        let struct_field = ffield.clone();
        let struct_fields = match struct_field.data_type() {
            DataType::Struct(fields) => Some(fields),
            _ => None,
        }
        .unwrap();

        let elem_builder: GenericStringBuilder<i32> = GenericStringBuilder::new();
        let mut expected_builder = ListBuilder::new(elem_builder).with_field(Field::new(
            "tag",
            DataType::Utf8,
            true,
        ));
        expected_builder.values().append_value("foo");
        expected_builder.values().append_value("bar");
        expected_builder.append(true);
        expected_builder.values().append_value("bar");
        expected_builder.values().append_value("foo");
        expected_builder.append(true);
        let expected = expected_builder.finish();
        let struct_column = StructArray::new(
            struct_fields.clone(),
            vec![
                Arc::new(StringArray::from(vec!["name1", "name2"])),
                Arc::new(BooleanArray::from(vec![true, false])),
                Arc::new(UInt32Array::from(vec![1, 2])),
                Arc::new(expected),
            ],
            None,
        );
        let record_batch =
            RecordBatch::try_new(arrow_schema.clone(), vec![Arc::new(struct_column)])
                .unwrap();
        // println!("rb: {:#?}", record_batch);

        let message_type = r#"
            message schema {
                REQUIRED GROUP struct {
                    REQUIRED GROUP tags (LIST) {
                        REPEATED GROUP tags {
                            OPTIONAL BINARY tag (UTF8);
                        }
                    }
                }
            }
        "#;
        let parquet_schema_2 = parse_message_type(message_type)
            .map(|t| Arc::new(SchemaDescriptor::new(Arc::new(t))))
            .unwrap();
        let arrow_schema_2 =
            Arc::new(parquet_to_arrow_schema(parquet_schema_2.as_ref(), None).unwrap());
        let new_rb = try_rewrite_record_batch(
            arrow_schema.clone(),
            record_batch,
            arrow_schema_2.clone(),
            true,
            false,
        )
        .unwrap();

        Ok(())
    }

    pub fn logical_plan_str(dataframe: &DataFrame) -> String {
        let cl = dataframe.clone();
        let op = cl.into_optimized_plan().unwrap();
        format!("{}", op.display_indent())
    }

    pub async fn physical_plan_str(dataframe: &DataFrame) -> String {
        let cl = dataframe.clone();
        let pp = cl.create_physical_plan().await.unwrap();
        get_plan_string(&pp).join("\n")
    }

    #[tokio::test]
    async fn test_deep_schema() -> datafusion_common::Result<()> {
        let _ = env_logger::try_init();
        let message_type = r#"
            message schema {
                REQUIRED INT32 id;
                REQUIRED GROUP struct1 {
                    REQUIRED BINARY name (UTF8);
                    REQUIRED BOOLEAN bools;
                    REQUIRED INT32 uint32 (INTEGER(32,false));
                    REQUIRED GROUP tags (LIST) {
                        REPEATED GROUP tags {
                            OPTIONAL BINARY tag (UTF8);
                        }
                    }
                }
                OPTIONAL GROUP list_struct (LIST) {
                    REPEATED GROUP item {
                        REQUIRED BOOLEAN bools;
                        REQUIRED INT32 uint32 (INTEGER(32,false));
                        REQUIRED GROUP int32 (LIST) {
                            REPEATED GROUP list {
                                OPTIONAL INT32 element;
                            }
                        }
                    }
                }
                OPTIONAL GROUP struct_list {
                    REQUIRED BOOLEAN bools;
                    REQUIRED INT32 uint32 (INTEGER(32,false));
                    REQUIRED GROUP products (LIST) {
                        REPEATED GROUP product {
                            OPTIONAL INT32 qty;
                            OPTIONAL binary name(utf8);
                        }
                    }
                }
            }
        "#;
        let parquet_schema = parse_message_type(message_type)
            .map(|t| Arc::new(SchemaDescriptor::new(Arc::new(t))))
            .unwrap();
        {}
        // return Ok(());

        let complete_schema =
            Arc::new(parquet_to_arrow_schema(parquet_schema.as_ref(), None).unwrap());
        // info!("schema: {:#?}", complete_schema.clone());
        // {
        //     let kk = generate_leaf_paths(
        //         complete_schema,
        //         parquet_schema.as_ref(),
        //         &vec![1, 2],
        //         &HashMap::from([
        //             (1 as usize, vec!["name".to_string(), "tags".to_string()])
        //         ])
        //     );
        //     info!("kk: {:#?}", kk);
        // }
        // return Ok(());

        let ctx = SessionContext::new();

        let schema_fields = complete_schema.fields().clone();
        let mut row_builder = StructBuilder::from_fields(schema_fields, 1);

        // field 0
        let f0_builder = row_builder.field_builder::<Int32Builder>(0).unwrap();
        f0_builder.append_value(1);
        let f0_arr = f0_builder.finish();

        // field 1
        let f1_builder = row_builder.field_builder::<StructBuilder>(1).unwrap();

        // tbl.struct.name
        {
            let f1_name_builder = f1_builder.field_builder::<StringBuilder>(0).unwrap();
            f1_name_builder.append_value("n1");
        }
        // tbl.struct.bools
        {
            let f1_bools_builder = f1_builder.field_builder::<BooleanBuilder>(1).unwrap();
            f1_bools_builder.append_value(true);
        }
        // tbl.struct.uint32
        let f1_uint32_builder = f1_builder.field_builder::<UInt32Builder>(2).unwrap();
        f1_uint32_builder.append_value(1);
        // tbl.struct.tags
        let f1_tags_list_builder = f1_builder
            .field_builder::<ListBuilder<Box<dyn ArrayBuilder>>>(3)
            .unwrap();
        let f1_tags_item_builder = f1_tags_list_builder
            .values()
            .as_any_mut()
            .downcast_mut::<StringBuilder>()
            .unwrap();
        f1_tags_item_builder.append_value("t1");
        f1_tags_item_builder.append_value("t2");
        f1_tags_list_builder.append(true);

        f1_builder.append(true);

        let f1_arr = f1_builder.finish();
        // field 2
        // make_array(
        //     named_struct(
        //         'bools', false,
        //         'uint32', 5,
        //         'int32', make_array(10, 20)
        //     )
        // ),
        let f2_builder = row_builder
            .field_builder::<ListBuilder<Box<dyn ArrayBuilder>>>(2)
            .unwrap();
        let f2_item_builder = f2_builder
            .values()
            .as_any_mut()
            .downcast_mut::<StructBuilder>()
            .unwrap();

        //tbl.list_struct[].bools
        let f2_item_bools_builder =
            f2_item_builder.field_builder::<BooleanBuilder>(0).unwrap();
        f2_item_bools_builder.append_value(true);
        // tbl.list_struct[].uint32
        let f2_item_uint32_builder =
            f2_item_builder.field_builder::<UInt32Builder>(1).unwrap();
        f2_item_uint32_builder.append_value(5);
        // tbl.list_struct[].uint32
        let f2_item_int32_list_builder = f2_item_builder
            .field_builder::<ListBuilder<Box<dyn ArrayBuilder>>>(2)
            .unwrap();
        let f2_item_int32_item_builder = f2_item_int32_list_builder
            .values()
            .as_any_mut()
            .downcast_mut::<Int32Builder>()
            .unwrap();
        f2_item_int32_item_builder.append_values(&[10, 20], &[true, true]);
        f2_item_int32_list_builder.append(true);

        f2_item_builder.append(true);

        f2_builder.append(true);

        let f2_arr = f2_builder.finish();

        // field 3
        // named_struct(
        //     'bools', true,
        //     'uint32', 5,
        //     'products', make_array(
        //         named_struct(
        //             'qty', 1,
        //             'name', 'product1'
        //         ),
        //         named_struct(
        //             'qty', 2,
        //             'name', 'product2'
        //         )
        //     )
        // )
        let f3_builder = row_builder.field_builder::<StructBuilder>(3).unwrap();
        // tbl.named_struct.bools
        let f3_bools_builder = f3_builder.field_builder::<BooleanBuilder>(0).unwrap();
        f3_bools_builder.append_value(true);
        // tbl.named_struct.uint32
        let f3_uint32_builder = f3_builder.field_builder::<UInt32Builder>(1).unwrap();
        f3_uint32_builder.append_value(5);
        // tbl.named_struct.uint32
        let f3_products_builder = f3_builder
            .field_builder::<ListBuilder<Box<dyn ArrayBuilder>>>(2)
            .unwrap();
        {
            let f3_field_products_item_builder = f3_products_builder
                .values()
                .as_any_mut()
                .downcast_mut::<StructBuilder>()
                .unwrap();
            let qty_builder = f3_field_products_item_builder
                .field_builder::<Int32Builder>(0)
                .unwrap();
            qty_builder.append_value(1);
            let name_builder = f3_field_products_item_builder
                .field_builder::<StringBuilder>(1)
                .unwrap();
            name_builder.append_value("product1");

            f3_field_products_item_builder.append(true);

            let f3_field_products_item_builder = f3_products_builder
                .values()
                .as_any_mut()
                .downcast_mut::<StructBuilder>()
                .unwrap();
            let qty_builder = f3_field_products_item_builder
                .field_builder::<Int32Builder>(0)
                .unwrap();
            qty_builder.append_value(1);
            let name_builder = f3_field_products_item_builder
                .field_builder::<StringBuilder>(1)
                .unwrap();
            name_builder.append_value("product1");
            f3_field_products_item_builder.append(true);
        }
        f3_products_builder.append(true);
        f3_builder.append(true);

        let f3_arr = f3_builder.finish();

        let row = StructArray::new(
            complete_schema.fields.clone(),
            vec![
                // 1
                Arc::new(f0_arr),
                Arc::new(f1_arr),
                Arc::new(f2_arr),
                Arc::new(f3_arr),
            ],
            None,
        );
        let initial_table = Arc::new(MemTable::try_new(
            complete_schema.clone(),
            vec![vec![RecordBatch::from(row)]],
        )?);

        ctx.register_table("tbl", initial_table.clone()).unwrap();
        info!("AAAAAAAAAAAAAAAAAAAAAAAAAAA");
        let df = ctx
            .sql(
                r#"
            select
                struct1['tags'] as tags,
                list_struct[0]['int32'] as f2
            from
                tbl;
        "#,
            )
            .await
            .unwrap();

        let df_plan = df.clone().logical_plan().clone();
        info!("df_plan: {:?}", df_plan);

        let optimizer = Optimizer::with_rules(vec![Arc::new(OptimizeProjections::new())]);
        let optimized_plan =
            optimizer.optimize(df_plan, &OptimizerContext::new(), |_, _| {})?;
        info!("df_plan: {:?}", optimized_plan);

        info!("logical = {}", logical_plan_str(&df));
        info!("physical = {}", physical_plan_str(&df).await);
        df.show().await?;
        Ok(())
    }
}
