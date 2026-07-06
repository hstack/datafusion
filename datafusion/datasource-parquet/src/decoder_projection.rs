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

//! Decoder-projection construction for the parquet scan.
//!
//! [`DecoderProjection`] owns the two halves of "project a decoded parquet
//! batch onto the scan's output schema":
//!
//! * the [`ProjectionMask`] installed on the parquet decoder (and on any
//!   rebuild performed via `into_builder` at a row-group boundary), and
//! * the per-batch transform ([`DecoderProjection::map`]) that applies the
//!   projector and, when needed, rebuilds the batch with the user's
//!   `output_schema` to recover metadata / nullability the file schema does
//!   not carry.
//!
//! The opener constructs one [`DecoderProjection`] per file via
//! [`DecoderProjection::try_new`] and hands it to the push-decoder stream,
//! which calls [`map`](DecoderProjection::map) on every decoded batch.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{RecordBatch, RecordBatchOptions};
use arrow::datatypes::SchemaRef;
use itertools::Itertools;
use log::{debug, trace};

use datafusion_common::Result;
use datafusion_common::deep::{cast_record_batch, has_deep_projection, rewrite_schema};
use datafusion_physical_expr::projection::{ProjectionExprs, Projector};
use datafusion_physical_expr::utils::reassign_expr_columns;
use datafusion_physical_expr_adapter::replace_columns_with_literals;

use parquet::arrow::ProjectionMask;
use parquet::schema::types::SchemaDescriptor;

use crate::leaves::{parquet_leaf_paths, projection_specifier, remap_top_level_field_indices};
use crate::opener::{VirtualColumnsState, append_fields};
use crate::projection_read_plan::{ParquetReadPlan, build_projection_read_plan};

/// Per-file decoder projection: the [`ProjectionMask`] installed on the
/// parquet decoder, plus the per-batch transform that maps the decoder's
/// output onto the scan's `output_schema`.
///
/// Built once per file by the opener via [`Self::try_new`]; the
/// push-decoder stream installs [`Self::projection_mask`] on the decoder
/// (and on any rebuild performed via `into_builder` at a row-group
/// boundary) and calls [`Self::map`] on every decoded batch.
pub(crate) struct DecoderProjection {
    projection_mask: ProjectionMask,
    projector: Projector,
    /// Schema the decoder is expected to yield batches in (file columns plus
    /// any virtual columns). When the deep-projection path narrows the
    /// [`ProjectionMask`] to specific leaves, the decoder's raw output can
    /// differ from this schema (e.g. missing struct sub-fields); [`map`]
    /// casts the batch up to `stream_schema` before projecting whenever they
    /// differ.
    stream_schema: SchemaRef,
    output_schema: SchemaRef,
    /// `true` when the projector's output schema differs from `output_schema`
    /// in metadata / nullability and [`map`](Self::map) must rebuild the batch
    /// with `output_schema`.
    replace_schema: bool,
}

impl DecoderProjection {
    /// Build the decoder projection for a file.
    ///
    /// `projection` references columns in `physical_file_schema` (i.e. already
    /// adapted by the per-file expr adapter); `parquet_schema` is the
    /// corresponding parquet [`SchemaDescriptor`]. `output_schema` is what
    /// consumers of the scan stream expect.
    ///
    /// `virtual_state`, when present, describes virtual columns the reader
    /// will append to each decoded batch (e.g. parquet `row_number`). Virtual
    /// columns are stripped from the projection fed into
    /// `build_projection_read_plan` (which only understands file columns) and
    /// appended to the stream schema so the projector can resolve them.
    ///
    /// `logical_file_schema`, `projection_hints`, and `projection_hints_indices`
    /// (`@HStack` deep projections) let the caller narrow the read plan to
    /// specific struct/list leaves instead of whole root columns, when the
    /// scan carries deep-projection hints. When no hints resolve to a deep
    /// projection, this falls back to the standard
    /// [`build_projection_read_plan`] path.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn try_new(
        projection: &ProjectionExprs,
        physical_file_schema: &SchemaRef,
        parquet_schema: &SchemaDescriptor,
        output_schema: &SchemaRef,
        virtual_state: Option<&VirtualColumnsState>,
        logical_file_schema: &SchemaRef,
        projection_hints: Option<&ProjectionExprs>,
        projection_hints_indices: &[usize],
    ) -> Result<Self> {
        // Virtual columns are produced by the reader separately from the
        // projection mask, so strip them from the expressions we feed into
        // `build_projection_read_plan`. We substitute each virtual column
        // reference with a null literal; that leaves the remaining Column
        // refs (into `physical_file_schema`) intact for
        // `ProjectionMask::roots`, which only understands file columns.
        let projection_for_read_plan = match virtual_state {
            None => projection.clone(),
            Some(state) => projection.clone().try_map_exprs(|expr| {
                replace_columns_with_literals(expr, state.null_replacements())
            })?,
        };
        let read_plan = build_read_plan_with_deep_projection(
            &projection_for_read_plan,
            physical_file_schema,
            parquet_schema,
            logical_file_schema,
            projection_hints,
            projection_hints_indices,
        );

        // The reader produces projected file columns followed by any virtual
        // columns (`ArrowReaderOptions::with_virtual_columns` appends them to
        // each decoded batch).
        let stream_schema = match virtual_state {
            Some(state) => {
                append_fields(&read_plan.projected_schema, state.virtual_columns())
            }
            None => Arc::clone(&read_plan.projected_schema),
        };

        // Rebase the projection onto the decoder's stream schema (column
        // indices change because the decoder yields only the masked columns).
        let rebased_projection = projection
            .clone()
            .try_map_exprs(|expr| reassign_expr_columns(expr, &stream_schema))?;
        let projector = rebased_projection.make_projector(&stream_schema)?;

        // Compare against the projector's *output* schema rather than the
        // stream schema, so future widening of the mask (e.g. for post-scan
        // filter columns) does not flip this flag.
        let replace_schema = projector.output_schema() != output_schema;

        Ok(Self {
            projection_mask: read_plan.projection_mask,
            projector,
            stream_schema,
            output_schema: Arc::clone(output_schema),
            replace_schema,
        })
    }

    /// The projection mask to install on every parquet decoder in the scan.
    pub(crate) fn projection_mask(&self) -> &ProjectionMask {
        &self.projection_mask
    }

    /// Map a decoded batch onto the scan's output schema.
    ///
    /// When the decoder's raw batch schema differs from `stream_schema` (the
    /// `@HStack` deep-projection path can narrow the [`ProjectionMask`] to
    /// specific leaves, yielding struct columns with fewer sub-fields than
    /// `stream_schema` expects), the batch is first cast up to
    /// `stream_schema`, filling missing struct fields with nulls.
    ///
    /// Applies the [`Projector`] and, when the projector's output schema
    /// differs from `output_schema` in metadata or nullability, rebuilds the
    /// batch with `output_schema` (some writers emit OPTIONAL fields even when
    /// the data has no nulls; some logical schemas carry field-level metadata
    /// the file schema does not).
    pub(crate) fn map(&self, batch: &RecordBatch) -> Result<RecordBatch> {
        let owned_batch;
        let batch = if !self.stream_schema.fields().is_empty()
            && batch.schema_ref().as_ref() != self.stream_schema.as_ref()
        {
            owned_batch =
                cast_record_batch(batch, Arc::clone(&self.stream_schema), false, true)?;
            &owned_batch
        } else {
            batch
        };
        let projected = self.projector.project_batch(batch)?;
        if !self.replace_schema {
            return Ok(projected);
        }
        let (_stream_schema, arrays, num_rows) = projected.into_parts();
        let options = RecordBatchOptions::new().with_row_count(Some(num_rows));
        Ok(RecordBatch::try_new_with_options(
            Arc::clone(&self.output_schema),
            arrays,
            &options,
        )?)
    }
}

/// Builds a [`ParquetReadPlan`] for `projection`, using `@HStack` deep
/// projection hints when they narrow the read below whole root columns and
/// falling back to [`build_projection_read_plan`] otherwise.
///
/// `logical_file_schema`, `projection_hints`, and `projection_hints_indices`
/// describe struct/list leaf projections the planner discovered on top of
/// `projection`; when [`has_deep_projection`] reports that at least one
/// projected root can be narrowed to specific leaves, this reads only those
/// leaves (via [`parquet_leaf_paths`]) instead of every leaf under the root.
fn build_read_plan_with_deep_projection(
    projection: &ProjectionExprs,
    physical_file_schema: &SchemaRef,
    parquet_schema: &SchemaDescriptor,
    logical_file_schema: &SchemaRef,
    projection_hints: Option<&ProjectionExprs>,
    projection_hints_indices: &[usize],
) -> ParquetReadPlan {
    let simplified_parquet_columns_in_logical_file_schema: HashMap<usize, Vec<String>> =
        projection_specifier(
            Arc::clone(logical_file_schema),
            projection,
            projection_hints,
            projection_hints_indices,
        );
    trace!(
        target: "deep",
        "DecoderProjection::try_new simplified_parquet_columns_in_logical_file_schema: {:?}",
        &simplified_parquet_columns_in_logical_file_schema
    );

    // Remap the top-level indices from the logical file schema to the
    // physical file schema (they can differ, e.g. after type coercion).
    let simplified_parquet_columns_in_physical_file_schema = {
        let indices_map =
            remap_top_level_field_indices(logical_file_schema, physical_file_schema);
        simplified_parquet_columns_in_logical_file_schema
            .iter()
            .filter_map(|(li, v)| {
                // SAFETY - we ALWAYS fill the left fields
                indices_map.get(li).unwrap().as_ref().map(|pi| (*pi, v.clone()))
            })
            .collect::<HashMap<_, _>>()
    };
    trace!(
        target: "deep",
        "DecoderProjection::try_new simplified_parquet_columns_in_physical_file_schema: {:?}",
        &simplified_parquet_columns_in_physical_file_schema
    );

    if !has_deep_projection(&simplified_parquet_columns_in_physical_file_schema) {
        return build_projection_read_plan(
            projection.expr_iter(),
            physical_file_schema,
            parquet_schema,
        );
    }

    let indices = projection.column_indices();
    // We need the physical file schema, but only for the top-level
    // projections (each kept in full - the ProjectionMask below narrows the
    // parquet leaves actually decoded).
    let top_level_projection_physical_file_schema = rewrite_schema(
        physical_file_schema,
        &indices,
        &indices
            .iter()
            .map(|idx| (*idx, vec![]))
            .collect::<HashMap<usize, Vec<String>>>(),
    );
    trace!(
        target: "deep",
        "DecoderProjection::try_new top_level_projection_physical_file_schema: {:?}",
        &top_level_projection_physical_file_schema
    );
    if let Some(hints) = projection_hints {
        trace!(
            target: "deep",
            "DecoderProjection::try_new projection_hints: {}",
            hints.iter().map(|pe| pe.expr.to_string()).join(", ")
        );
    }
    trace!(
        target: "deep",
        "DecoderProjection::try_new deep projections: {:?}",
        simplified_parquet_columns_in_physical_file_schema
    );

    let leaves = parquet_leaf_paths(
        Arc::clone(physical_file_schema),
        parquet_schema,
        &indices,
        &simplified_parquet_columns_in_physical_file_schema,
    );
    debug!(
        target: "deep",
        "DecoderProjection::try_new, using deep projection parquet leaves: {:?}",
        leaves
    );
    let mask = ProjectionMask::leaves(parquet_schema, leaves);
    ParquetReadPlan {
        projection_mask: mask,
        projected_schema: top_level_projection_physical_file_schema,
    }
}

