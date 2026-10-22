use datafusion_common::Result;
use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_datasource::file::FileSource;
use datafusion_datasource::file_scan_config::FileScanConfigBuilder;
use datafusion_datasource::source::DataSourceExec;
use crate::source::ParquetSource;
use datafusion_physical_expr::projection::{
    ProjectionExpr, ProjectionExprs,
};
use datafusion_physical_optimizer::PhysicalOptimizerRule;
use datafusion_physical_optimizer::projection_hints::{self, ProjectionHintsSourceAdapter};
use datafusion_physical_plan::ExecutionPlan;
use std::sync::Arc;

pub use datafusion_physical_optimizer::projection_hints::{
    ExpressionSource, col_index_and_plan_for_join_like_plan, get_key_from_plan,
    merge_column_expressions, replace_column_in_expr,
};

/// Supplies the existing Parquet source boundaries and projection mapping.
#[derive(Debug)]
pub struct ParquetProjectionHintsAdapter;

impl ProjectionHintsSourceAdapter for ParquetProjectionHintsAdapter {
    fn is_leaf_that_accepts_projection(&self, plan: &Arc<dyn ExecutionPlan>) -> bool {
        plan.downcast_ref::<DataSourceExec>().is_some()
    }

    fn projection(&self, plan: &Arc<dyn ExecutionPlan>) -> Option<ProjectionExprs> {
        plan.downcast_ref::<DataSourceExec>()?
            .downcast_to_file_source::<ParquetSource>()?
            .1
            .projection()
            .cloned()
    }
}

#[derive(Debug)]
pub struct PushAllProjectionHints {}

impl PushAllProjectionHints {}

/// This optimizer rule
///     - searches recursively in the execution plans and tries to resolve expressions referenced
///       in the plan down to DataSourceExec
///         - some type of plans have projections specified . For those plans, we try to go down through
///           the projections, so we always solve down to Data sources
///         - for array_element / get_field expressions we collect and change the expressions
///           so that we reach the minimal projection needed to solve them
///     - the result - is saved to the DataSourceExec
impl PhysicalOptimizerRule for PushAllProjectionHints {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let accum = projection_hints::collect_projection_hints(&plan, &ParquetProjectionHintsAdapter);

        // recreate the plan with extra projections
        if accum.len() == 0 {
            return Ok(plan);
        }

        let new_plan = plan.transform_down(|p| {
            if let Some(ds_exec) = p.downcast_ref::<DataSourceExec>() {
                if let Some((file_scan_conf, parquet_source)) =
                    ds_exec.downcast_to_file_source::<ParquetSource>()
                {
                    let key = get_key_from_plan(&p);
                    if let Some(exprs) = accum.get(&key) {
                        let (exprs, indices): (Vec<_>, Vec<_>) =
                            exprs.into_iter().map(|(e, i)| (e.clone(), i)).unzip();

                        let pexprs = exprs
                            .iter()
                            .map(|e| ProjectionExpr::new(e.clone(), "".to_string()));

                        let mut new_parquet_source = parquet_source.clone();
                        new_parquet_source.projection_hints =
                            ProjectionExprs::new(pexprs);
                        new_parquet_source.projection_hints_indices = indices;

                        let new_file_scan_config =
                            FileScanConfigBuilder::from(file_scan_conf.clone())
                                .with_source(Arc::new(new_parquet_source))
                                .build();

                        let execution_plan = Arc::new(
                            ds_exec
                                .clone()
                                .with_data_source(Arc::new(new_file_scan_config)),
                        );
                        return Ok(Transformed::yes(execution_plan));
                    }
                }
            }
            Ok(Transformed::no(p))
        })?;

        Ok(new_plan.data)
    }

    fn name(&self) -> &str {
        "LeafProjectionOptimizerRule"
    }

    fn schema_check(&self) -> bool {
        true
    }
}
