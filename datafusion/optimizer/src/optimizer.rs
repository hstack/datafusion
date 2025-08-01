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

//! [`Optimizer`] and [`OptimizerRule`]

use std::fmt::Debug;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use datafusion_expr::registry::FunctionRegistry;
use datafusion_expr::{assert_expected_schema, InvariantLevel};
use log::{debug, warn};

use datafusion_common::alias::AliasGenerator;
use datafusion_common::config::ConfigOptions;
use datafusion_common::instant::Instant;
use datafusion_common::tree_node::{Transformed, TreeNodeRewriter};
use datafusion_common::{internal_err, DFSchema, DataFusionError, HashSet, Result};
use datafusion_expr::logical_plan::LogicalPlan;

use crate::common_subexpr_eliminate::CommonSubexprEliminate;
use crate::decorrelate_lateral_join::DecorrelateLateralJoin;
use crate::decorrelate_predicate_subquery::DecorrelatePredicateSubquery;
use crate::eliminate_cross_join::EliminateCrossJoin;
use crate::eliminate_duplicated_expr::EliminateDuplicatedExpr;
use crate::eliminate_filter::EliminateFilter;
use crate::eliminate_group_by_constant::EliminateGroupByConstant;
use crate::eliminate_join::EliminateJoin;
use crate::eliminate_limit::EliminateLimit;
use crate::eliminate_nested_union::EliminateNestedUnion;
use crate::eliminate_one_union::EliminateOneUnion;
use crate::eliminate_outer_join::EliminateOuterJoin;
use crate::extract_equijoin_predicate::ExtractEquijoinPredicate;
use crate::filter_null_join_keys::FilterNullJoinKeys;
use crate::optimize_projections::OptimizeProjections;
use crate::optimize_projections_deep::OptimizeProjectionsDeep;
use crate::plan_signature::LogicalPlanSignature;
use crate::propagate_empty_relation::PropagateEmptyRelation;
use crate::push_down_filter::PushDownFilter;
use crate::push_down_limit::PushDownLimit;
use crate::replace_distinct_aggregate::ReplaceDistinctWithAggregate;
use crate::scalar_subquery_to_join::ScalarSubqueryToJoin;
use crate::simplify_expressions::SimplifyExpressions;
use crate::single_distinct_to_groupby::SingleDistinctToGroupBy;
use crate::utils::log_plan;

/// `OptimizerRule`s transforms one [`LogicalPlan`] into another which
/// computes the same results, but in a potentially more efficient
/// way. If there are no suitable transformations for the input plan,
/// the optimizer should simply return it unmodified.
///
/// To change the semantics of a `LogicalPlan`, see [`AnalyzerRule`]
///
/// Use [`SessionState::add_optimizer_rule`] to register additional
/// `OptimizerRule`s.
///
/// [`AnalyzerRule`]: crate::analyzer::AnalyzerRule
/// [`SessionState::add_optimizer_rule`]: https://docs.rs/datafusion/latest/datafusion/execution/session_state/struct.SessionState.html#method.add_optimizer_rule
pub trait OptimizerRule: Debug {
    /// A human readable name for this optimizer rule
    fn name(&self) -> &str;

    /// How should the rule be applied by the optimizer? See comments on
    /// [`ApplyOrder`] for details.
    ///
    /// If returns `None`, the default, the rule must handle recursion itself
    fn apply_order(&self) -> Option<ApplyOrder> {
        None
    }

    /// Does this rule support rewriting owned plans (rather than by reference)?
    #[deprecated(since = "47.0.0", note = "This method is no longer used")]
    fn supports_rewrite(&self) -> bool {
        true
    }

    /// Try to rewrite `plan` to an optimized form, returning `Transformed::yes`
    /// if the plan was rewritten and `Transformed::no` if it was not.
    fn rewrite(
        &self,
        _plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>, DataFusionError> {
        internal_err!("rewrite is not implemented for {}", self.name())
    }
}

/// Options to control the DataFusion Optimizer.
pub trait OptimizerConfig {
    /// Return the time at which the query execution started. This
    /// time is used as the value for now()
    fn query_execution_start_time(&self) -> DateTime<Utc>;

    /// Return alias generator used to generate unique aliases for subqueries
    fn alias_generator(&self) -> &Arc<AliasGenerator>;

    fn options(&self) -> &ConfigOptions;

    fn function_registry(&self) -> Option<&dyn FunctionRegistry> {
        None
    }
}

/// A standalone [`OptimizerConfig`] that can be used independently
/// of DataFusion's config management
#[derive(Debug)]
pub struct OptimizerContext {
    /// Query execution start time that can be used to rewrite
    /// expressions such as `now()` to use a literal value instead
    query_execution_start_time: DateTime<Utc>,

    /// Alias generator used to generate unique aliases for subqueries
    alias_generator: Arc<AliasGenerator>,

    options: ConfigOptions,
}

impl OptimizerContext {
    /// Create optimizer config
    pub fn new() -> Self {
        let mut options = ConfigOptions::default();
        options.optimizer.filter_null_join_keys = true;

        Self {
            query_execution_start_time: Utc::now(),
            alias_generator: Arc::new(AliasGenerator::new()),
            options,
        }
    }

    /// Specify whether to enable the filter_null_keys rule
    pub fn filter_null_keys(mut self, filter_null_keys: bool) -> Self {
        self.options.optimizer.filter_null_join_keys = filter_null_keys;
        self
    }

    /// Specify whether the optimizer should skip rules that produce
    /// errors, or fail the query
    pub fn with_query_execution_start_time(
        mut self,
        query_execution_tart_time: DateTime<Utc>,
    ) -> Self {
        self.query_execution_start_time = query_execution_tart_time;
        self
    }

    /// Specify whether the optimizer should skip rules that produce
    /// errors, or fail the query
    pub fn with_skip_failing_rules(mut self, b: bool) -> Self {
        self.options.optimizer.skip_failed_rules = b;
        self
    }

    /// Specify how many times to attempt to optimize the plan
    pub fn with_max_passes(mut self, v: u8) -> Self {
        self.options.optimizer.max_passes = v as usize;
        self
    }
}

impl Default for OptimizerContext {
    /// Create optimizer config
    fn default() -> Self {
        Self::new()
    }
}

impl OptimizerConfig for OptimizerContext {
    fn query_execution_start_time(&self) -> DateTime<Utc> {
        self.query_execution_start_time
    }

    fn alias_generator(&self) -> &Arc<AliasGenerator> {
        &self.alias_generator
    }

    fn options(&self) -> &ConfigOptions {
        &self.options
    }
}

/// A rule-based optimizer.
#[derive(Clone, Debug)]
pub struct Optimizer {
    /// All optimizer rules to apply
    pub rules: Vec<Arc<dyn OptimizerRule + Send + Sync>>,
}

/// Specifies how recursion for an `OptimizerRule` should be handled.
///
/// * `Some(apply_order)`: The Optimizer will recursively apply the rule to the plan.
/// * `None`: the rule must handle any required recursion itself.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ApplyOrder {
    /// Apply the rule to the node before its inputs
    TopDown,
    /// Apply the rule to the node after its inputs
    BottomUp,
}

impl Default for Optimizer {
    fn default() -> Self {
        Self::new()
    }
}

impl Optimizer {
    /// Create a new optimizer using the recommended list of rules
    pub fn new() -> Self {
        let rules: Vec<Arc<dyn OptimizerRule + Sync + Send>> = vec![
            Arc::new(EliminateNestedUnion::new()),
            Arc::new(SimplifyExpressions::new()),
            Arc::new(ReplaceDistinctWithAggregate::new()),
            Arc::new(EliminateJoin::new()),
            Arc::new(DecorrelatePredicateSubquery::new()),
            Arc::new(ScalarSubqueryToJoin::new()),
            Arc::new(DecorrelateLateralJoin::new()),
            Arc::new(ExtractEquijoinPredicate::new()),
            Arc::new(EliminateDuplicatedExpr::new()),
            Arc::new(EliminateFilter::new()),
            Arc::new(EliminateCrossJoin::new()),
            Arc::new(EliminateLimit::new()),
            Arc::new(PropagateEmptyRelation::new()),
            // Must be after PropagateEmptyRelation
            Arc::new(EliminateOneUnion::new()),
            Arc::new(FilterNullJoinKeys::default()),
            Arc::new(EliminateOuterJoin::new()),
            // Filters can't be pushed down past Limits, we should do PushDownFilter after PushDownLimit
            Arc::new(PushDownLimit::new()),
            Arc::new(PushDownFilter::new()),
            Arc::new(SingleDistinctToGroupBy::new()),
            // The previous optimizations added expressions and projections,
            // that might benefit from the following rules
            Arc::new(EliminateGroupByConstant::new()),
            Arc::new(CommonSubexprEliminate::new()),
            Arc::new(OptimizeProjections::new()),
        ];

        Self::with_rules(rules)
    }

    /// Create a new optimizer with the given rules
    pub fn with_rules(rules: Vec<Arc<dyn OptimizerRule + Send + Sync>>) -> Self {
        Self { rules }
    }
}

/// Recursively rewrites LogicalPlans
struct Rewriter<'a> {
    apply_order: ApplyOrder,
    rule: &'a dyn OptimizerRule,
    config: &'a dyn OptimizerConfig,
}

impl<'a> Rewriter<'a> {
    fn new(
        apply_order: ApplyOrder,
        rule: &'a dyn OptimizerRule,
        config: &'a dyn OptimizerConfig,
    ) -> Self {
        Self {
            apply_order,
            rule,
            config,
        }
    }
}

impl TreeNodeRewriter for Rewriter<'_> {
    type Node = LogicalPlan;

    fn f_down(&mut self, node: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
        if self.apply_order == ApplyOrder::TopDown {
            {
                self.rule.rewrite(node, self.config)
            }
        } else {
            Ok(Transformed::no(node))
        }
    }

    fn f_up(&mut self, node: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
        if self.apply_order == ApplyOrder::BottomUp {
            {
                self.rule.rewrite(node, self.config)
            }
        } else {
            Ok(Transformed::no(node))
        }
    }
}

impl Optimizer {
    /// Optimizes the logical plan by applying optimizer rules, and
    /// invoking observer function after each call
    pub fn optimize<F>(
        &self,
        plan: LogicalPlan,
        config: &dyn OptimizerConfig,
        mut observer: F,
    ) -> Result<LogicalPlan>
    where
        F: FnMut(&LogicalPlan, &dyn OptimizerRule),
    {
        // verify LP is valid, before the first LP optimizer pass.
        plan.check_invariants(InvariantLevel::Executable)
            .map_err(|e| e.context("Invalid input plan before LP Optimizers"))?;

        let start_time = Instant::now();
        let options = config.options();
        let mut new_plan = plan;

        let mut previous_plans = HashSet::with_capacity(16);
        previous_plans.insert(LogicalPlanSignature::new(&new_plan));

        let starting_schema = Arc::clone(new_plan.schema());

        let mut i = 0;
        while i < options.optimizer.max_passes {
            log_plan(&format!("Optimizer input (pass {i})"), &new_plan);

            for rule in &self.rules {
                // If skipping failed rules, copy plan before attempting to rewrite
                // as rewriting is destructive
                let prev_plan = options
                    .optimizer
                    .skip_failed_rules
                    .then(|| new_plan.clone());

                let starting_schema = Arc::clone(new_plan.schema());

                let result = match rule.apply_order() {
                    // optimizer handles recursion
                    Some(apply_order) => new_plan.rewrite_with_subqueries(
                        &mut Rewriter::new(apply_order, rule.as_ref(), config),
                    ),
                    // rule handles recursion itself
                    None => {
                        rule.rewrite(new_plan, config)
                    },
                }
                .and_then(|tnr| {
                    // run checks optimizer invariant checks, per optimizer rule applied
                    assert_valid_optimization(&tnr.data, &starting_schema)
                        .map_err(|e| e.context(format!("Check optimizer-specific invariants after optimizer rule: {}", rule.name())))?;

                    // run LP invariant checks only in debug mode for performance reasons
                    #[cfg(debug_assertions)]
                    tnr.data.check_invariants(InvariantLevel::Executable)
                        .map_err(|e| e.context(format!("Invalid (non-executable) plan after Optimizer rule: {}", rule.name())))?;

                    Ok(tnr)
                });

                // Handle results
                match (result, prev_plan) {
                    // OptimizerRule was successful
                    (
                        Ok(Transformed {
                            data, transformed, ..
                        }),
                        _,
                    ) => {
                        new_plan = data;
                        observer(&new_plan, rule.as_ref());
                        if transformed {
                            log_plan(rule.name(), &new_plan);
                        } else {
                            debug!(
                                "Plan unchanged by optimizer rule '{}' (pass {})",
                                rule.name(),
                                i
                            );
                        }
                    }
                    // OptimizerRule was unsuccessful, but skipped failed rules is on
                    // so use the previous plan
                    (Err(e), Some(orig_plan)) => {
                        // Note to future readers: if you see this warning it signals a
                        // bug in the DataFusion optimizer. Please consider filing a ticket
                        // https://github.com/apache/datafusion
                        warn!(
                            "Skipping optimizer rule '{}' due to unexpected error: {}",
                            rule.name(),
                            e
                        );
                        new_plan = orig_plan;
                    }
                    // OptimizerRule was unsuccessful, but skipped failed rules is off, return error
                    (Err(e), None) => {
                        return Err(e.context(format!(
                            "Optimizer rule '{}' failed",
                            rule.name()
                        )));
                    }
                }
            }
            log_plan(&format!("Optimized plan (pass {i})"), &new_plan);

            // HashSet::insert returns, whether the value was newly inserted.
            let plan_is_fresh =
                previous_plans.insert(LogicalPlanSignature::new(&new_plan));
            if !plan_is_fresh {
                // plan did not change, so no need to continue trying to optimize
                debug!("optimizer pass {i} did not make changes");
                break;
            }
            i += 1;
        }

        // Heuristic to ignore test contexts
        if self.rules.len() > 10 {
            // This breaks with optimize projections, we need to execute this last
            let optimize_deep_projections_rule = Arc::new(OptimizeProjectionsDeep::new());
            let last_plan = optimize_deep_projections_rule.rewrite(new_plan, config)?;
            new_plan = last_plan.data;
        }

        // verify that the optimizer passes only mutated what was permitted.
        assert_valid_optimization(&new_plan, &starting_schema).map_err(|e| {
            e.context("Check optimizer-specific invariants after all passes")
        })?;

        // verify LP is valid, after the last optimizer pass.
        new_plan
            .check_invariants(InvariantLevel::Executable)
            .map_err(|e| {
                e.context("Invalid (non-executable) plan after LP Optimizers")
            })?;

        log_plan("Final optimized plan", &new_plan);
        debug!("Optimizer took {} ms", start_time.elapsed().as_millis());
        Ok(new_plan)
    }
}

/// These are invariants which should hold true before and after [`LogicalPlan`] optimization.
///
/// This differs from [`LogicalPlan::check_invariants`], which addresses if a singular
/// LogicalPlan is valid. Instead this address if the optimization was valid based upon permitted changes.
fn assert_valid_optimization(
    plan: &LogicalPlan,
    prev_schema: &Arc<DFSchema>,
) -> Result<()> {
    // verify invariant: optimizer passes should not change the schema if the schema can't be cast from the previous schema.
    // Refer to <https://datafusion.apache.org/contributor-guide/specification/invariants.html#logical-schema-is-invariant-under-logical-optimization>
    assert_expected_schema(prev_schema, plan)?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use datafusion_common::tree_node::Transformed;
    use datafusion_common::{
        assert_contains, plan_err, DFSchema, DFSchemaRef, DataFusionError, Result,
    };
    use datafusion_expr::logical_plan::EmptyRelation;
    use datafusion_expr::{col, lit, LogicalPlan, LogicalPlanBuilder, Projection};

    use crate::optimizer::Optimizer;
    use crate::test::test_table_scan;
    use crate::{OptimizerConfig, OptimizerContext, OptimizerRule};

    use super::ApplyOrder;

    #[test]
    fn skip_failing_rule() {
        let opt = Optimizer::with_rules(vec![Arc::new(BadRule {})]);
        let config = OptimizerContext::new().with_skip_failing_rules(true);
        let plan = LogicalPlan::EmptyRelation(EmptyRelation {
            produce_one_row: false,
            schema: Arc::new(DFSchema::empty()),
        });
        opt.optimize(plan, &config, &observe).unwrap();
    }

    #[test]
    fn no_skip_failing_rule() {
        let opt = Optimizer::with_rules(vec![Arc::new(BadRule {})]);
        let config = OptimizerContext::new().with_skip_failing_rules(false);
        let plan = LogicalPlan::EmptyRelation(EmptyRelation {
            produce_one_row: false,
            schema: Arc::new(DFSchema::empty()),
        });
        let err = opt.optimize(plan, &config, &observe).unwrap_err();
        assert_eq!(
            "Optimizer rule 'bad rule' failed\ncaused by\n\
            Error during planning: rule failed",
            err.strip_backtrace()
        );
    }

    #[test]
    fn generate_different_schema() {
        let opt = Optimizer::with_rules(vec![Arc::new(GetTableScanRule {})]);
        let config = OptimizerContext::new().with_skip_failing_rules(false);
        let plan = LogicalPlan::EmptyRelation(EmptyRelation {
            produce_one_row: false,
            schema: Arc::new(DFSchema::empty()),
        });
        let err = opt.optimize(plan, &config, &observe).unwrap_err();

        // Simplify assert to check the error message contains the expected message
        assert_contains!(
            err.strip_backtrace(),
            "Failed due to a difference in schemas: original schema: DFSchema"
        );
    }

    #[test]
    fn skip_generate_different_schema() {
        let opt = Optimizer::with_rules(vec![Arc::new(GetTableScanRule {})]);
        let config = OptimizerContext::new().with_skip_failing_rules(true);
        let plan = LogicalPlan::EmptyRelation(EmptyRelation {
            produce_one_row: false,
            schema: Arc::new(DFSchema::empty()),
        });
        opt.optimize(plan, &config, &observe).unwrap();
    }

    #[test]
    fn generate_same_schema_different_metadata() -> Result<()> {
        // if the plan creates more metadata than previously (because
        // some wrapping functions are removed, etc) do not error
        let opt = Optimizer::with_rules(vec![Arc::new(GetTableScanRule {})]);
        let config = OptimizerContext::new().with_skip_failing_rules(false);

        let input = Arc::new(test_table_scan()?);
        let input_schema = Arc::clone(input.schema());

        let plan = LogicalPlan::Projection(Projection::try_new_with_schema(
            vec![col("a"), col("b"), col("c")],
            input,
            add_metadata_to_fields(input_schema.as_ref()),
        )?);

        // optimizing should be ok, but the schema will have changed  (no metadata)
        assert_ne!(plan.schema().as_ref(), input_schema.as_ref());
        let optimized_plan = opt.optimize(plan, &config, &observe)?;
        // metadata was removed
        assert_eq!(optimized_plan.schema().as_ref(), input_schema.as_ref());
        Ok(())
    }

    #[test]
    fn optimizer_detects_plan_equal_to_the_initial() -> Result<()> {
        // Run a goofy optimizer, which rotates projection columns
        // [1, 2, 3] -> [2, 3, 1] -> [3, 1, 2] -> [1, 2, 3]

        let opt = Optimizer::with_rules(vec![Arc::new(RotateProjectionRule::new(false))]);
        let config = OptimizerContext::new().with_max_passes(16);

        let initial_plan = LogicalPlanBuilder::empty(false)
            .project([lit(1), lit(2), lit(3)])?
            .project([lit(100)])? // to not trigger changed schema error
            .build()?;

        let mut plans: Vec<LogicalPlan> = Vec::new();
        let final_plan =
            opt.optimize(initial_plan.clone(), &config, |p, _| plans.push(p.clone()))?;

        // initial_plan is not observed, so we have 3 plans
        assert_eq!(3, plans.len());

        // we got again the initial_plan with [1, 2, 3]
        assert_eq!(initial_plan, final_plan);

        Ok(())
    }

    #[test]
    fn optimizer_detects_plan_equal_to_a_non_initial() -> Result<()> {
        // Run a goofy optimizer, which reverses and rotates projection columns
        // [1, 2, 3] -> [3, 2, 1] -> [2, 1, 3] -> [1, 3, 2] -> [3, 2, 1]

        let opt = Optimizer::with_rules(vec![Arc::new(RotateProjectionRule::new(true))]);
        let config = OptimizerContext::new().with_max_passes(16);

        let initial_plan = LogicalPlanBuilder::empty(false)
            .project([lit(1), lit(2), lit(3)])?
            .project([lit(100)])? // to not trigger changed schema error
            .build()?;

        let mut plans: Vec<LogicalPlan> = Vec::new();
        let final_plan =
            opt.optimize(initial_plan, &config, |p, _| plans.push(p.clone()))?;

        // initial_plan is not observed, so we have 4 plans
        assert_eq!(4, plans.len());

        // we got again the plan with [3, 2, 1]
        assert_eq!(plans[0], final_plan);

        Ok(())
    }

    fn add_metadata_to_fields(schema: &DFSchema) -> DFSchemaRef {
        let new_fields = schema
            .iter()
            .enumerate()
            .map(|(i, (qualifier, field))| {
                let metadata =
                    [("key".into(), format!("value {i}"))].into_iter().collect();

                let new_arrow_field = field.as_ref().clone().with_metadata(metadata);
                (qualifier.cloned(), Arc::new(new_arrow_field))
            })
            .collect::<Vec<_>>();

        let new_metadata = schema.metadata().clone();
        Arc::new(DFSchema::new_with_metadata(new_fields, new_metadata).unwrap())
    }

    fn observe(_plan: &LogicalPlan, _rule: &dyn OptimizerRule) {}

    #[derive(Default, Debug)]
    struct BadRule {}

    impl OptimizerRule for BadRule {
        fn name(&self) -> &str {
            "bad rule"
        }

        fn supports_rewrite(&self) -> bool {
            true
        }

        fn rewrite(
            &self,
            _plan: LogicalPlan,
            _config: &dyn OptimizerConfig,
        ) -> Result<Transformed<LogicalPlan>, DataFusionError> {
            plan_err!("rule failed")
        }
    }

    /// Replaces whatever plan with a single table scan
    #[derive(Default, Debug)]
    struct GetTableScanRule {}

    impl OptimizerRule for GetTableScanRule {
        fn name(&self) -> &str {
            "get table_scan rule"
        }

        fn supports_rewrite(&self) -> bool {
            true
        }

        fn rewrite(
            &self,
            _plan: LogicalPlan,
            _config: &dyn OptimizerConfig,
        ) -> Result<Transformed<LogicalPlan>> {
            let table_scan = test_table_scan()?;
            Ok(Transformed::yes(
                LogicalPlanBuilder::from(table_scan).build()?,
            ))
        }
    }

    /// A goofy rule doing rotation of columns in all projections.
    ///
    /// Useful to test cycle detection.
    #[derive(Default, Debug)]
    struct RotateProjectionRule {
        // reverse exprs instead of rotating on the first pass
        reverse_on_first_pass: Mutex<bool>,
    }

    impl RotateProjectionRule {
        fn new(reverse_on_first_pass: bool) -> Self {
            Self {
                reverse_on_first_pass: Mutex::new(reverse_on_first_pass),
            }
        }
    }

    impl OptimizerRule for RotateProjectionRule {
        fn name(&self) -> &str {
            "rotate_projection"
        }

        fn apply_order(&self) -> Option<ApplyOrder> {
            Some(ApplyOrder::TopDown)
        }

        fn supports_rewrite(&self) -> bool {
            true
        }

        fn rewrite(
            &self,
            plan: LogicalPlan,
            _config: &dyn OptimizerConfig,
        ) -> Result<Transformed<LogicalPlan>> {
            let projection = match plan {
                LogicalPlan::Projection(p) if p.expr.len() >= 2 => p,
                _ => return Ok(Transformed::no(plan)),
            };

            let mut exprs = projection.expr.clone();

            let mut reverse = self.reverse_on_first_pass.lock().unwrap();
            if *reverse {
                exprs.reverse();
                *reverse = false;
            } else {
                exprs.rotate_left(1);
            }

            Ok(Transformed::yes(LogicalPlan::Projection(
                Projection::try_new(exprs, Arc::clone(&projection.input))?,
            )))
        }
    }
}
