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
use datafusion_common::tree_node::TreeNodeRecursion;
use datafusion_common::{Result, ScalarValue};
use datafusion_execution::TaskContext;
use datafusion_physical_expr::PhysicalExpr;
use datafusion_physical_expr::ScalarFunctionExpr;
use datafusion_physical_expr::expressions::{Column, Literal};
use datafusion_physical_expr::projection::{ProjectionExpr, ProjectionExprs};
use datafusion_physical_optimizer::projection_hints::{
    ProjectionHintsSourceAdapter, collect_projection_hints, get_first_column_from_expr,
};
use datafusion_physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion_physical_plan::coop::CooperativeExec;
use datafusion_physical_plan::empty::EmptyExec;
use datafusion_physical_plan::projection::ProjectionExec;
use datafusion_physical_plan::union::UnionExec;
use datafusion_physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties,
    SendableRecordBatchStream,
};

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

    fn projection(
        &self,
        plan: &Arc<dyn ExecutionPlan>,
    ) -> Result<Option<ProjectionExprs>> {
        if !self.supports_source(plan) {
            return Ok(None);
        }
        // The reader exposes output@0 from stored@2 in its underlying schema.
        Ok(Some(ProjectionExprs::new([ProjectionExpr::new(
            Arc::new(Column::new("stored", 2)),
            "output".to_string(),
        )])))
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
    )
    .unwrap();
    assert_eq!(hints.len(), 1);
    let hints = &hints[&vec![]];
    assert_eq!(hints.expressions.as_ref().len(), 1);
    assert_eq!(hints.indices, vec![0]);
    assert_eq!(
        get_first_column_from_expr(&hints.expressions.as_ref()[0].expr),
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
    let hints = collect_projection_hints(&plan, &adapter).unwrap();
    assert_eq!(hints.len(), 1);
    let hints = &hints[&vec![0, 0]];
    assert_eq!(hints.expressions.as_ref().len(), 1);
    assert_eq!(hints.indices, vec![0]);
    assert_eq!(
        get_first_column_from_expr(&hints.expressions.as_ref()[0].expr),
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
    )
    .unwrap();
    assert!(hints.is_empty());
}

#[test]
fn identical_source_occurrences_have_separate_hints() {
    let source = source();
    let plan = UnionExec::try_new(vec![Arc::clone(&source), source]).unwrap();
    let hints = collect_projection_hints(
        &plan,
        &EmptySourceAdapter {
            stop_at_projection: false,
        },
    )
    .unwrap();
    assert_eq!(hints.len(), 2);
    for path in [vec![0], vec![1]] {
        let hints = &hints[&path];
        assert_eq!(hints.indices, vec![0]);
        assert_eq!(
            get_first_column_from_expr(&hints.expressions.as_ref()[0].expr),
            Some(Column::new("stored", 2))
        );
    }
}

struct IdentitySourceAdapter;

impl ProjectionHintsSourceAdapter for IdentitySourceAdapter {
    fn is_leaf_that_accepts_projection(&self, plan: &Arc<dyn ExecutionPlan>) -> bool {
        plan.downcast_ref::<EmptyExec>().is_some()
    }

    fn projection(
        &self,
        plan: &Arc<dyn ExecutionPlan>,
    ) -> Result<Option<ProjectionExprs>> {
        Ok(Some(ProjectionExprs::from_indices(
            &(0..plan.schema().fields().len()).collect::<Vec<_>>(),
            &plan.schema(),
        )))
    }
}

fn nested_source() -> Arc<dyn ExecutionPlan> {
    Arc::new(EmptyExec::new(Arc::new(Schema::new(vec![Field::new(
        "profile",
        DataType::Struct(
            vec![
                Field::new("keep", DataType::Int32, true),
                Field::new("payload", DataType::Utf8, true),
            ]
            .into(),
        ),
        true,
    )]))))
}

fn nested_projection(input: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
    let selector = ScalarFunctionExpr::try_new(
        datafusion_functions::core::get_field(),
        vec![
            Arc::new(Column::new(input.schema().field(0).name(), 0)),
            Arc::new(Literal::new(ScalarValue::Utf8(Some("keep".into())))),
        ],
        &input.schema(),
        Arc::new(Default::default()),
    )
    .unwrap();
    Arc::new(
        ProjectionExec::try_new(
            vec![ProjectionExpr::new(Arc::new(selector), "keep")],
            input,
        )
        .unwrap(),
    )
}

#[test]
fn preserves_nested_requirements_through_cooperative_exec() {
    let plan = nested_projection(Arc::new(CooperativeExec::new(nested_source())));
    let hints = collect_projection_hints(&plan, &IdentitySourceAdapter).unwrap();
    let hints = &hints[&vec![0, 0]];
    assert_eq!(hints.expressions.as_ref().len(), 1);
    assert_eq!(
        hints.expressions.as_ref()[0].expr.to_string(),
        "get_field(profile@0, keep)"
    );
    assert_eq!(hints.indices, vec![0]);
}

#[derive(Debug)]
struct ExternalWrapperExec(Vec<Arc<dyn ExecutionPlan>>);

impl DisplayAs for ExternalWrapperExec {
    fn fmt_as(
        &self,
        _t: DisplayFormatType,
        f: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result {
        write!(f, "ExternalWrapperExec")
    }
}

impl ExecutionPlan for ExternalWrapperExec {
    fn name(&self) -> &str {
        "ExternalWrapperExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        self.0[0].properties()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        self.0.iter().collect()
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self(children)))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        self.0[0].execute(partition, context)
    }
}

#[test]
fn preserves_nested_requirements_through_unknown_unary_wrapper() {
    for alias in [false, true] {
        let input: Arc<dyn ExecutionPlan> =
            Arc::new(ExternalWrapperExec(vec![Arc::new(CooperativeExec::new(
                nested_source(),
            ))]));
        let input = if alias {
            Arc::new(
                ProjectionExec::try_new(
                    vec![ProjectionExpr::new(
                        Arc::new(Column::new("profile", 0)),
                        "aliased_profile",
                    )],
                    input,
                )
                .unwrap(),
            ) as Arc<dyn ExecutionPlan>
        } else {
            input
        };
        let plan = nested_projection(input);
        let hints = collect_projection_hints(&plan, &IdentitySourceAdapter).unwrap();
        let path = if alias {
            vec![0, 0, 0, 0]
        } else {
            vec![0, 0, 0]
        };
        assert_eq!(hints.len(), 1);
        let hints = &hints[&path];
        assert_eq!(hints.expressions.as_ref().len(), 1);
        assert_eq!(
            hints.expressions.as_ref()[0].expr.to_string(),
            "get_field(profile@0, keep)"
        );
        assert_eq!(hints.indices, vec![0]);
    }
}

#[test]
fn unknown_multi_child_operator_retains_whole_column_requirements() {
    let plan = nested_projection(Arc::new(ExternalWrapperExec(vec![
        nested_source(),
        nested_source(),
    ])));
    let hints = collect_projection_hints(&plan, &IdentitySourceAdapter).unwrap();
    assert_eq!(hints.len(), 2);
    for path in [vec![0, 0], vec![0, 1]] {
        let hints = &hints[&path];
        assert_eq!(hints.expressions.as_ref().len(), 1);
        assert_eq!(hints.expressions.as_ref()[0].expr.to_string(), "profile@0");
    }
}
