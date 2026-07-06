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

use arrow::datatypes::{DataType, Field, Schema};
use datafusion_physical_expr::expressions::Column;
use datafusion_physical_expr::projection::{ProjectionExpr, ProjectionExprs};
use datafusion_physical_optimizer::projection_hints::{
    ProjectionHintsSourceAdapter, collect_projection_hints,
    find_sources_for_column_expr, get_first_column_from_expr, get_key_from_plan,
};
use datafusion_physical_plan::ExecutionPlan;
use datafusion_physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion_physical_plan::empty::EmptyExec;
use datafusion_physical_plan::projection::ProjectionExec;

struct EmptySourceAdapter {
    stop_at_projection: bool,
}

impl EmptySourceAdapter {
    fn supports_source(&self, plan: &Arc<dyn ExecutionPlan>) -> bool {
        plan.downcast_ref::<EmptyExec>().is_some()
    }
}

impl ProjectionHintsSourceAdapter for EmptySourceAdapter {
    fn is_leaf_that_accepts_projection(&self, plan: &Arc<dyn ExecutionPlan>) -> bool {
        plan.downcast_ref::<EmptyExec>().is_some()
            || (self.stop_at_projection
                && plan.downcast_ref::<ProjectionExec>().is_some())
    }

    fn projection(&self, plan: &Arc<dyn ExecutionPlan>) -> Option<ProjectionExprs> {
        if !self.supports_source(plan) {
            return None;
        }
        assert!(self.supports_source(plan));
        // The reader exposes output@0 from stored@2 in its underlying schema.
        Some(ProjectionExprs::new([ProjectionExpr::new(
            Arc::new(Column::new("stored", 2)),
            "output".to_string(),
        )]))
    }
}

fn source() -> Arc<dyn ExecutionPlan> {
    Arc::new(EmptyExec::new(Arc::new(Schema::new(vec![Field::new(
        "output",
        DataType::Int32,
        true,
    )]))))
}

fn projection(source: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
    Arc::new(
        ProjectionExec::try_new(
            vec![ProjectionExpr::new(
                Arc::new(Column::new("output", 0)),
                "alias".to_string(),
            )],
            Arc::new(CoalescePartitionsExec::new(source)),
        )
        .unwrap(),
    )
}

#[test]
fn collects_a_non_parquet_source() {
    let source = source();
    let hints = collect_projection_hints(
        &source,
        &EmptySourceAdapter {
            stop_at_projection: false,
        },
    );
    assert_eq!(hints.len(), 1);
    let hints = &hints[&get_key_from_plan(&source)];
    assert_eq!(hints.len(), 1);
    assert_eq!(hints[0].1, 0);
    assert_eq!(
        get_first_column_from_expr(&hints[0].0),
        Some(Column::new("stored", 2))
    );
}

#[test]
fn traces_through_projection_to_an_external_source_mapping() {
    let source = source();
    let plan = projection(Arc::clone(&source));
    let adapter = EmptySourceAdapter {
        stop_at_projection: false,
    };
    let hints = collect_projection_hints(&plan, &adapter);
    assert_eq!(hints.len(), 1);
    let hints = &hints[&get_key_from_plan(&source)];
    assert_eq!(hints.len(), 1);
    assert_eq!(hints[0].1, 0);
    assert_eq!(
        get_first_column_from_expr(&hints[0].0),
        Some(Column::new("stored", 2))
    );

    let expr = plan.downcast_ref::<ProjectionExec>().unwrap().expr()[0]
        .expr
        .clone();
    let sources = find_sources_for_column_expr(&expr, &plan, &adapter);
    assert_eq!(sources.len(), 1);
    assert!(Arc::ptr_eq(&sources[0].plan, &source));
    assert_eq!(sources[0].col_index, 0);
    assert_eq!(
        get_first_column_from_expr(&sources[0].expr),
        Some(Column::new("stored", 2))
    );
}

#[test]
fn unsupported_source_boundary_does_not_descend() {
    let plan = projection(source());
    let hints = collect_projection_hints(
        &plan,
        &EmptySourceAdapter {
            stop_at_projection: true,
        },
    );
    assert!(hints.is_empty());
}
