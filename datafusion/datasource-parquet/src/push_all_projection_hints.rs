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

use arrow::datatypes::SchemaRef;
use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion_common::{Result, internal_datafusion_err};
use datafusion_datasource::file::FileSource;
use datafusion_datasource::file_scan_config::FileScanConfigBuilder;
use datafusion_datasource::source::DataSourceExec;
use datafusion_physical_expr::PhysicalExpr;
use datafusion_physical_expr::projection::ProjectionExprs;
use datafusion_physical_optimizer::PhysicalOptimizerRule;
use datafusion_physical_optimizer::projection_hints::{
    ProjectionHints, ProjectionHintsSourceAdapter, collect_projection_hints,
};
use datafusion_physical_plan::ExecutionPlan;

use crate::source::ParquetSource;

/// Supplies Parquet source boundaries, projections, and predicates.
#[derive(Debug)]
pub struct ParquetProjectionHintsAdapter;

impl ProjectionHintsSourceAdapter for ParquetProjectionHintsAdapter {
    fn is_leaf_that_accepts_projection(&self, plan: &Arc<dyn ExecutionPlan>) -> bool {
        plan.downcast_ref::<DataSourceExec>()
            .and_then(|scan| scan.downcast_to_file_source::<ParquetSource>())
            .is_some()
    }

    fn projection(
        &self,
        plan: &Arc<dyn ExecutionPlan>,
    ) -> Result<Option<ProjectionExprs>> {
        let Some((_, source)) = plan
            .downcast_ref::<DataSourceExec>()
            .and_then(|scan| scan.downcast_to_file_source::<ParquetSource>())
        else {
            return Ok(None);
        };
        source.projection().cloned().map(Some).ok_or_else(|| {
            internal_datafusion_err!("Parquet source is missing its projection")
        })
    }

    fn schema(&self, plan: &Arc<dyn ExecutionPlan>) -> SchemaRef {
        Arc::clone(
            plan.downcast_ref::<DataSourceExec>()
                .expect("Parquet source boundary")
                .downcast_to_file_source::<ParquetSource>()
                .expect("Parquet source boundary")
                .1
                .table_schema
                .table_schema(),
        )
    }

    fn filter(&self, plan: &Arc<dyn ExecutionPlan>) -> Option<Arc<dyn PhysicalExpr>> {
        plan.downcast_ref::<DataSourceExec>()?
            .downcast_to_file_source::<ParquetSource>()?
            .1
            .filter()
    }
}

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
        let hints = collect_projection_hints(&plan, &ParquetProjectionHintsAdapter)?;
        apply_projection_hints(plan, &mut vec![], &hints)
    }

    fn name(&self) -> &str {
        "LeafProjectionOptimizerRule"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

fn apply_projection_hints(
    plan: Arc<dyn ExecutionPlan>,
    path: &mut Vec<usize>,
    hints: &ProjectionHints,
) -> Result<Arc<dyn ExecutionPlan>> {
    if let Some(hints) = hints.get(path) {
        let (scan, config, source) = plan
            .downcast_ref::<DataSourceExec>()
            .and_then(|scan| {
                scan.downcast_to_file_source::<ParquetSource>()
                    .map(|(config, source)| (scan, config, source))
            })
            .ok_or_else(|| {
                internal_datafusion_err!("Projection hints require a Parquet source")
            })?;
        let mut source = source.clone();
        source.projection_hints = hints.expressions.clone();
        source.projection_hints_indices.clone_from(&hints.indices);
        let config = FileScanConfigBuilder::from(config.clone())
            .with_source(Arc::new(source))
            .build();
        return Ok(Arc::new(scan.clone().with_data_source(Arc::new(config))));
    }
    let mut index = 0;
    plan.map_children(|child| {
        path.push(index);
        index += 1;
        let original = Arc::clone(&child);
        let new_child = apply_projection_hints(child, path, hints)?;
        path.pop();
        let changed = !Arc::ptr_eq(&original, &new_child);
        Ok(Transformed::new(
            new_child,
            changed,
            datafusion_common::tree_node::TreeNodeRecursion::Continue,
        ))
    })
    .data()
}
