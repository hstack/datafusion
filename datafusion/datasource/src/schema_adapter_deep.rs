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
    can_rewrite_field, try_rewrite_record_batch_with_mappings,
};
use datafusion_common::{plan_err, ColumnStatistics};
use log::error;
use std::sync::Arc;

#[derive(Clone, Debug)]
#[allow(dead_code)]
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

#[allow(dead_code)]
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
                projected_table_schema: Arc::clone(&self.projected_table_schema),
                field_mappings,
                table_schema: Arc::clone(&self.table_schema),
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
                projected_table_schema: Arc::clone(&self.projected_table_schema),
                field_mappings,
                table_schema: Arc::clone(&self.table_schema),
            }),
            projection,
        ))
    }
}

/// TODO: struct doc
#[derive(Debug)]
#[allow(dead_code)]
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
            &batch.schema(),
            &batch,
            &self.projected_table_schema,
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
