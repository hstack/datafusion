use datafusion_common::Result;
use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion_common::{JoinSide, JoinType};
use datafusion_datasource::file::FileSource;
use datafusion_datasource::file_scan_config::FileScanConfigBuilder;
use datafusion_datasource::source::DataSourceExec;
use crate::leaves::{
    expr_has_get_field_or_array_element, expr_is_only_get_field_or_array_or_cast_and_contains_column,
    get_expressions_amenable_to_deep_projection, get_first_column_from_expr,
};
use crate::source::ParquetSource;
use datafusion_physical_expr::expressions::Column;
use datafusion_physical_expr::projection::{
    ProjectionExpr, ProjectionExprs, ProjectionRef,
};
use datafusion_physical_expr_common::physical_expr::PhysicalExpr;
use datafusion_physical_optimizer::PhysicalOptimizerRule;
use datafusion_physical_plan::aggregates::AggregateExec;
use datafusion_physical_plan::filter::FilterExec;
use datafusion_physical_plan::joins::utils::build_join_schema;
use datafusion_physical_plan::joins::{
    HashJoinExec, NestedLoopJoinExec, PiecewiseMergeJoinExec, SortMergeJoinExec,
    SymmetricHashJoinExec,
};
use datafusion_physical_plan::projection::ProjectionExec;
use datafusion_physical_plan::sorts::sort::SortExec;
use datafusion_physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
use datafusion_physical_plan::union::UnionExec;
use datafusion_physical_plan::windows::{BoundedWindowAggExec, WindowAggExec};
use datafusion_physical_plan::{ExecutionPlan, displayable};
use log::{error, trace, warn};
use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

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
        trace!(
            target: "deep",
            "PushAllProjectionHints input: {}",
            displayable(plan.as_ref()).indent(true)
        );
        // find DataSourceExec
        let mut data_sources: HashMap<String, &Arc<dyn ExecutionPlan>> = HashMap::new();
        let _ = plan.apply(|p| {
            if let Some(ds_exec) = p.as_any().downcast_ref::<DataSourceExec>() {
                if let Some((_file_scan_conf, _parquet_source)) =
                    ds_exec.downcast_to_file_source::<ParquetSource>()
                {
                    // fix this, but how ?
                    let plan_key = displayable(p.as_ref()).indent(true).to_string();
                    data_sources.insert(plan_key, p);
                }
                return Ok(TreeNodeRecursion::Jump);
            }
            Ok(TreeNodeRecursion::Continue)
        });

        // find extra projections
        let mut accum: HashMap<String, Vec<(Arc<dyn PhysicalExpr>, usize)>> =
            HashMap::new();

        let mut visited_expressions: HashMap<String, Vec<usize>> = HashMap::new();
        let _ = plan.apply(|physical_plan| {
            if let Some(_ds_exec) =
                physical_plan.as_any().downcast_ref::<DataSourceExec>()
            {
                let key = get_key_from_plan(physical_plan);
                // if we do not have expression for this plan, accumulate the expressions in this plan
                let already_used_col_indices_for_this_datasource_exec = accum.get(key.as_str())
                    .into_iter()
                    .flatten()
                    .map(|(_expr, index)| {
                        *index
                    })
                    .collect::<Vec<_>>();
                let exprs = extract_expressions_containing_column_from_plan(&physical_plan);
                for (expr_index, expr) in exprs.iter().enumerate() {
                    trace!(target:"deep",
                        "PushAllProjectionHints::optimize accum expr for DataSourceExec index={}: {}",
                        expr_index,
                        &expr.to_string()
                    );
                    if let Some(ve) = visited_expressions.get(get_key_from_plan(&physical_plan).as_str())
                        && ve.contains(&expr_index) {
                        trace!(target:"deep",
                            "  PushAllProjectionHints::optimize expr at index {} is used, skip",
                            expr_index,
                        );
                        continue
                    }
                    if !already_used_col_indices_for_this_datasource_exec.contains(&expr_index) {
                        accum
                            .entry(key.clone())
                            .or_default()
                            .push((expr.clone(), expr_index));
                    }
                }
                Ok(TreeNodeRecursion::Jump)
            } else {
                let exprs = extract_expressions_containing_column_from_plan(&physical_plan);
                for (expr_index, expr) in exprs.iter().enumerate() {
                    trace!(target:"deep",
                        "PushAllProjectionHints::optimize accum expr for plan type={} index={}: {}",
                        &physical_plan.name(),
                        expr_index,
                        &expr.to_string()
                    );
                    // if (&physical_plan).name() == "FilterExec"
                    //     && let Some(ve) = visited_expressions.get(get_key_from_plan(&physical_plan).as_str())
                    //     && ve.contains(&expr_index) {
                    //     continue
                    // }
                    if let Some(ve) = visited_expressions.get(get_key_from_plan(&physical_plan).as_str())
                        && ve.contains(&expr_index) {
                        trace!(target:"deep",
                            "  PushAllProjectionHints::optimize expr at index {} is used, skip",
                            expr_index,
                        );
                        continue
                    }
                    let expression_sources = find_sources_for_column_expr(&expr, &physical_plan);
                    visited_expressions = combine_visited_expressions(
                        visited_expressions.clone(),
                        expression_sources
                            .iter()
                            .map(|es| es.visited_exprs.clone())
                            .collect::<Vec<HashMap<_, _>>>()
                    );
                    for ExpressionSource{plan:source, expr: source_expr, col_index: source_col_index, .. } in expression_sources
                    {
                        trace!(target:"deep", "  PushAllProjectionHints::optimize FOUND modified EXPR: {}", &source_expr.to_string());
                        accum
                            .entry(get_key_from_plan(&source))
                            .or_default()
                            .push((source_expr, source_col_index));
                    }
                }
                Ok(TreeNodeRecursion::Continue)
            }
        });

        // recreate the plan with extra projections
        if accum.len() == 0 {
            return Ok(plan);
        }

        let new_plan = plan.transform_down(|p| {
            if let Some(ds_exec) = p.as_any().downcast_ref::<DataSourceExec>() {
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

/// For an expression, we might get a list of expressions in the data source
/// like we have Column(a) + Column(b) - this will get resolved down to the DataSource, but there
/// we will recurse two times, and we need to mark both columns as being used.
fn combine_visited_expressions(
    initial: HashMap<String, Vec<usize>>,
    maps: Vec<HashMap<String, usize>>,
) -> HashMap<String, Vec<usize>> {
    let mut acc: HashMap<String, BTreeSet<usize>> = initial
        .into_iter()
        .map(|(k, vs)| (k, vs.into_iter().collect()))
        .collect();
    for m in maps {
        for (k, v) in m {
            acc.entry(k).or_default().insert(v);
        }
    }
    acc.into_iter()
        .map(|(k, set)| (k, set.into_iter().collect()))
        .collect()
}

/// builds a key for an execution plan
/// we need this so we can keep, for each plan, the list of "visited" expressions.
///
/// there is a *small* possibility that this will break if we have two plans inside a
pub fn get_key_from_plan(plan: &Arc<dyn ExecutionPlan>) -> String {
    // fix this, but how ?
    let plan_key = displayable(plan.as_ref()).indent(false).to_string();
    return plan_key;
}

pub fn extract_expressions_containing_column_from_plan(
    input: &Arc<dyn ExecutionPlan>,
) -> Vec<Arc<dyn PhysicalExpr>> {
    let mut out = vec![];
    get_expressions_to_check(input)
        .iter()
        .for_each(|e| out.extend(get_expressions_amenable_to_deep_projection(e)));
    out
}

/// extract lists of expression from a physical plan
/// TODO: add here for missing plan types.
pub fn get_expressions_to_check(
    plan: &Arc<dyn ExecutionPlan>,
) -> Vec<Arc<dyn PhysicalExpr>> {
    if let Some(ds_exec) = plan.as_any().downcast_ref::<DataSourceExec>() {
        if let Some((_file_scan_conf, parquet_source)) =
            ds_exec.downcast_to_file_source::<ParquetSource>()
        {
            return parquet_source
                .projection()
                // SAFETY - parquet source always has a projection
                .unwrap()
                .iter()
                .map(|pe| pe.expr.clone())
                .collect::<Vec<_>>();
        } else {
            return vec![];
        }
    }

    if let Some(plan) = plan.as_any().downcast_ref::<ProjectionExec>() {
        return plan
            .expr()
            .iter()
            .map(|pe| pe.expr.clone())
            .collect::<Vec<_>>();
    }
    if let Some(plan) = plan.as_any().downcast_ref::<FilterExec>() {
        let mut out = vec![];
        if let Some(filter_projection) = plan.projection() {
            let input = plan.children()[0];
            let filter_projection_columns = filter_projection
                .iter()
                .map(|index| {
                    let name =
                        input.schema().fields().get(*index).unwrap().name().clone();
                    let col = Column::new(name.as_str(), *index);
                    Arc::new(col) as Arc<dyn PhysicalExpr>
                })
                .collect::<Vec<Arc<dyn PhysicalExpr>>>();
            out.extend(filter_projection_columns);
        }
        out.push(plan.predicate().clone());
        return out;
    }
    if let Some(plan) = plan.as_any().downcast_ref::<SortExec>() {
        return plan
            .expr()
            .iter()
            .map(|le| le.expr.clone())
            .collect::<Vec<_>>();
    }
    if let Some(plan) = plan.as_any().downcast_ref::<AggregateExec>() {
        let mut out: Vec<Arc<dyn PhysicalExpr>> = vec![];
        plan.group_expr()
            .expr()
            .iter()
            .for_each(|ge| out.push(ge.0.clone()));
        plan.aggr_expr().iter().for_each(|ae| {
            ae.expressions().iter().for_each(|e| out.push(e.clone()));
        });
        plan.filter_expr().iter().for_each(|fe| {
            if let Some(fe) = fe {
                out.push(fe.clone())
            }
        });
        return out;
    }
    if let Some(plan) = plan.as_any().downcast_ref::<BoundedWindowAggExec>() {
        let mut out: Vec<Arc<dyn PhysicalExpr>> = vec![];
        plan.window_expr()
            .iter()
            .for_each(|we| we.expressions().iter().for_each(|e| out.push(e.clone())));
        return out;
    }
    if let Some(plan) = plan.as_any().downcast_ref::<WindowAggExec>() {
        let mut out: Vec<Arc<dyn PhysicalExpr>> = vec![];
        plan.window_expr()
            .iter()
            .for_each(|we| we.expressions().iter().for_each(|e| out.push(e.clone())));
        return out;
    }
    if let Some(plan) = plan.as_any().downcast_ref::<HashJoinExec>() {
        let mut out: Vec<Arc<dyn PhysicalExpr>> = vec![];
        plan.on.iter().for_each(|oe| {
            out.push(oe.0.clone());
            out.push(oe.1.clone());
        });
        return out;
    }
    // if let Some(plan) = plan.as_any().downcast_ref::<CrossJoinExec>() {}
    // if let Some(plan) = plan.as_any().downcast_ref::<NestedLoopJoinExec>() {}
    if let Some(plan) = plan.as_any().downcast_ref::<PiecewiseMergeJoinExec>() {
        return vec![plan.on.0.clone(), plan.on.1.clone()];
    }
    if let Some(plan) = plan.as_any().downcast_ref::<SortMergeJoinExec>() {
        let mut out: Vec<Arc<dyn PhysicalExpr>> = vec![];
        plan.on.iter().for_each(|oe| {
            out.push(oe.0.clone());
            out.push(oe.1.clone());
        });
        return out;
    }
    if let Some(plan) = plan.as_any().downcast_ref::<SortPreservingMergeExec>() {
        let mut out: Vec<Arc<dyn PhysicalExpr>> = vec![];
        plan.expr().iter().for_each(|se| out.push(se.expr.clone()));
        return out;
    }
    if let Some(plan) = plan.as_any().downcast_ref::<SymmetricHashJoinExec>() {
        let mut out: Vec<Arc<dyn PhysicalExpr>> = vec![];
        plan.on().iter().for_each(|oe| {
            out.push(oe.0.clone());
            out.push(oe.1.clone());
        });
        return out;
    }
    vec![]
}

// if we call this, we have to make sure that expr contains a column
pub fn replace_column_in_expr(
    expr: &Arc<dyn PhysicalExpr>,
    new_col: &Arc<dyn PhysicalExpr>,
) -> Arc<dyn PhysicalExpr> {
    let mut executed = false;
    let new_expr = expr
        .clone()
        .transform_down(|e| {
            if let Some(_col) = e.as_any().downcast_ref::<Column>()
                && !executed
            {
                executed = true;
                return Ok(Transformed::yes(new_col.clone()));
            }
            Ok(Transformed::no(e))
        })
        .unwrap();
    new_expr.data
}

pub fn merge_column_expressions(
    left: &Arc<dyn PhysicalExpr>,
    right: &Arc<dyn PhysicalExpr>,
) -> Arc<dyn PhysicalExpr> {
    let _left_column = get_first_column_from_expr(left).unwrap();
    let right_column = get_first_column_from_expr(right).unwrap();
    let left_has_deep_projection = expr_has_get_field_or_array_element(left);
    let right_has_deep_projection = expr_has_get_field_or_array_element(right);
    if !left_has_deep_projection {
        // left is a column, so we return right.
        right.clone()
    } else {
        // left HAS deep projection
        if !right_has_deep_projection {
            replace_column_in_expr(
                &left,
                &(Arc::new(right_column.clone()) as Arc<dyn PhysicalExpr>),
            )
        } else {
            // both left AND right have deep projection !!!!!!!
            trace!(
                target: "deep",
                "dpp::merge_column_expressions: complicated merge !!!!!! left={}, right={}",
                &left.to_string(), &right.to_string()
            );
            let out = replace_column_in_expr(&left, &right);
            // let out = right.clone();
            trace!(target: "deep", "  dpp::merge_column_expressions forced merged: {}", &out.to_string());
            out
        }
    }
}

/// temporary struct that is kept until we resolve all expressions down to their data sources
#[derive(Debug, Clone)]
pub struct ExpressionSource {
    pub plan: Arc<dyn ExecutionPlan>,
    pub expr: Arc<dyn PhysicalExpr>,
    pub col_index: usize,
    pub visited_exprs: HashMap<String, usize>,
}

// expands an complex expression to a list of get_field / array_element / column expressions
pub fn find_sources_for_column_expr(
    expr: &Arc<dyn PhysicalExpr>,
    plan: &Arc<dyn ExecutionPlan>,
) -> Vec<ExpressionSource> {
    let mut out: Vec<ExpressionSource> = vec![];
    let expanded_exprs = get_expressions_amenable_to_deep_projection(expr);
    // info!("find_sources_for_column_expr EXPRESSIONS LEN: {}", expanded_exprs.len());

    for expr in expanded_exprs {
        // info!("find_sources_for_column_expr START: {}", expr.to_string());
        out.extend(find_sources_for_single_column_expr(&expr, plan));
    }
    out
}

pub fn find_sources_for_single_column_expr(
    expr: &Arc<dyn PhysicalExpr>,
    plan: &Arc<dyn ExecutionPlan>,
) -> Vec<ExpressionSource> {
    trace!(target: "deep", "dpp::find_sources_for_single_column_expr expr={}", &expr.to_string());
    let mut expr = expr.clone();
    let mut expr_is_from_current_plan = true;
    if let Some(col) = get_first_column_from_expr(&expr) {
        let mut col_index = col.index();
        // let mut col_expr = expr.clone();
        let mut visited_exprs: HashMap<String, usize> = HashMap::new();

        trace!(target:"deep", "dpp::find_sources_for_single_column_expr for expr={} index={}", expr.to_string(), col_index);
        let mut current = plan;

        if let Some(plan) = current.as_any().downcast_ref::<SortMergeJoinExec>() {
            let mut found = false;
            for (l, r) in plan.on().iter() {
                if &expr == l {
                    current = plan.left();
                    found = true;
                }
                if &expr == r {
                    current = plan.right();
                    found = true;
                }
            }
            if !found {
                warn!("find_sources_for_single_column_expr Could not find expr {:?} in plan HashJoinExec", expr);
                return vec![];
            }
        }

        'outer: loop {
            trace!(target:"deep",
                "  dpp::find_sources_for_single_column_expr start at plan {}, col_index={}, expr={}",
                current.name(),
                col_index,
                expr.to_string()
            );
            if !expr_is_from_current_plan {
                trace!(target:"deep",
                    "    dpp::find_sources_for_single_column_expr SAVE used column in plan={}, column={}, expr={}",
                    current.name(),
                    col_index,
                    expr.to_string()
                );
                visited_exprs.insert(get_key_from_plan(current), col_index);
            } else {
                trace!(target:"deep",
                    "    dpp::find_sources_for_single_column_expr DON'T SAVE used column in plan={}, column={}, expr={}",
                    current.name(),
                    col_index,
                    expr.to_string()
                );
            }

            if let Some(plan) = current.as_any().downcast_ref::<DataSourceExec>() {
                // info!("    at DataSourceExec, col_index = {}", col_index);
                if let Some((_file_scan_conf, parquet_source)) =
                    plan.downcast_to_file_source::<ParquetSource>()
                {
                    // SAFETY = parquet source always has a projection
                    let projection = parquet_source
                        .projection()
                        .expect("ParquetSource projection");
                    let projection_vec = projection.iter().collect::<Vec<_>>();
                    if let Some(pexpr) = projection_vec.get(col_index) {
                        let actual_expr = pexpr.expr.clone();
                        let new_scalar_func_expr =
                            merge_column_expressions(&expr, &actual_expr);
                        return vec![ExpressionSource {
                            plan: current.clone(),
                            expr: new_scalar_func_expr,
                            col_index,
                            visited_exprs,
                        }];
                    } else {
                        return vec![];
                    }
                }

                return vec![];
            }
            // col_index refers to the index of the column in current plan's input
            if current.children().len() == 0 {
                error!("unreachable code in find_source_for_column_expr: 0 children for plan {}", current.name());
                return vec![];
            }
            // some plans have projection, like projection, filter, hash join, nested loop join
            if let Some(proj) = current.as_any().downcast_ref::<ProjectionExec>() {
                if Arc::ptr_eq(current, plan) {
                    // the expression starts here
                    current = current.children()[0];
                    expr_is_from_current_plan = false;
                } else {
                    if let Some(expr_in_projection) =
                        proj.projection_expr().as_ref().get(col_index)
                    {
                        trace!(
                            target: "deep",
                            "    dpp::find_sources_for_single_column_expr EXPR IN PROJECTION IS: {}",
                            expr_in_projection.to_string()
                        );

                        if expr_is_only_get_field_or_array_or_cast_and_contains_column(
                            &expr_in_projection.expr,
                        ) {
                            let column_in_projection =
                                get_first_column_from_expr(&expr_in_projection.expr).unwrap();
                            expr =
                                merge_column_expressions(&expr, &expr_in_projection.expr);
                            col_index = column_in_projection.index();
                            current = proj.children()[0];
                            expr_is_from_current_plan = false;
                            continue 'outer;
                        } else {
                            trace!(
                                target: "deep",
                                "      dpp::find_sources_for_single_column_expr can't handle this expression: {}",
                                expr.to_string()
                            );
                            // we DON't do anything, don't know how to handle other things in projection
                            let out = find_sources_for_column_expr(&expr_in_projection.expr, current);
                            trace!(
                                target: "deep",
                                "      dpp::find_sources_for_single_column_expr EXPANDED: {}",
                                out.iter().map(|es| es.expr.to_string()).collect::<Vec<_>>().join(", ")
                            );
                            return out;
                        }
                    } else {
                        return vec![];
                    }
                }
            }

            if let Some(filter_child) = current.as_any().downcast_ref::<FilterExec>() {
                // filter has a projection, but we apply this ONLY if the expression comes from above this plan
                // if the expression is in the actual FilterExec plan, we just go down
                let new_col_index = if Arc::ptr_eq(plan, current) {
                    Some(&col_index)
                } else {
                    if let Some(projection) = &filter_child.projection() {
                        projection.get(col_index)
                    } else {
                        Some(&col_index)
                    }
                };
                if let Some(new_col_index) = new_col_index {
                    col_index = *new_col_index;
                    current = filter_child.input();
                    expr_is_from_current_plan = false;
                    continue 'outer;
                } else {
                    return vec![];
                }
            }
            if let Some(join) = current.as_any().downcast_ref::<HashJoinExec>() {
                if Arc::ptr_eq(current, plan) {
                    // if the expression originates in this plan
                    let mut found = false;
                    for (l, r) in join.on().iter() {
                        if &expr == l {
                            current = join.left();
                            expr_is_from_current_plan = false;
                            found = true;
                        }
                        if &expr == r {
                            current = join.right();
                            expr_is_from_current_plan = false;
                            found = true;
                        }
                    }
                    if !found {
                        warn!("Could not find expr {:?} in plan HashJoinExec", expr);
                        return vec![];
                    }
                    continue 'outer;
                } else {
                    if let Some((tmp_col_index, tmp_plan)) =
                        col_index_and_plan_for_join_like_plan(
                            join.left(),
                            join.right(),
                            &join.join_type,
                            join.projection.clone(),
                            col_index,
                        )
                    {
                        col_index = tmp_col_index;
                        current = tmp_plan;
                        expr_is_from_current_plan = false;
                        continue 'outer;
                    } else {
                        return vec![];
                    }
                }
            }
            if current.children().len() == 1
                && let Some(join) = current.children()[0]
                .as_any()
                .downcast_ref::<NestedLoopJoinExec>()
            {
                if let Some((tmp_col_index, tmp_plan)) =
                    col_index_and_plan_for_join_like_plan(
                        join.left(),
                        join.right(),
                        join.join_type(),
                        join.projection().clone(),
                        col_index,
                    )
                {
                    col_index = tmp_col_index;
                    current = tmp_plan;
                    expr_is_from_current_plan = false;
                    continue 'outer;
                } else {
                    return vec![];
                }
            }
            if current.children().len() == 1
                && let Some(join) = current.children()[0]
                .as_any()
                .downcast_ref::<SymmetricHashJoinExec>()
            {
                if Arc::ptr_eq(plan, current) {
                    let mut found = false;
                    for (l, r) in join.on().iter() {
                        if &expr == l {
                            current = join.left();
                            expr_is_from_current_plan = false;
                            found = true;
                        }
                        if &expr == r {
                            current = join.right();
                            expr_is_from_current_plan = false;
                            found = true;
                        }
                    }
                    if !found {
                        warn!("Could not find expr {:?} in plan HashJoinExec", expr);
                        return vec![];
                    }
                    continue 'outer;
                } else {
                    if let Some((tmp_col_index, tmp_plan)) =
                        col_index_and_plan_for_join_like_plan(
                            join.left(),
                            join.right(),
                            join.join_type(),
                            None,
                            col_index,
                        )
                    {
                        col_index = tmp_col_index;
                        current = tmp_plan;
                        expr_is_from_current_plan = false;
                        continue 'outer;
                    } else {
                        return vec![];
                    }
                }
            }
            if current.children().len() == 1
                && let Some(join) = current.children()[0]
                .as_any()
                .downcast_ref::<SortMergeJoinExec>()
            {
                if Arc::ptr_eq(current, plan) {
                    let mut found = false;
                    for (l, r) in join.on().iter() {
                        if &expr == l {
                            current = join.left();
                            expr_is_from_current_plan = false;
                            found = true;
                        }
                        if &expr == r {
                            current = join.right();
                            expr_is_from_current_plan = false;
                            found = true;
                        }
                    }
                    if !found {
                        warn!("Could not find expr {:?} in plan HashJoinExec", expr);
                        return vec![];
                    }
                    continue 'outer;
                } else {
                    if let Some((tmp_col_index, tmp_plan)) =
                        col_index_and_plan_for_join_like_plan(
                            join.left(),
                            join.right(),
                            &join.join_type(),
                            None,
                            col_index,
                        )
                    {
                        col_index = tmp_col_index;
                        current = tmp_plan;
                        expr_is_from_current_plan = false;
                        continue 'outer;
                    } else {
                        return vec![];
                    }
                }
            }
            if current.children().len() == 1
                && let Some(union) =
                current.children()[0].as_any().downcast_ref::<UnionExec>()
            {
                let mut out = vec![];
                for union_child in union.children().iter() {
                    out.extend(find_sources_for_column_expr(&expr, union_child))
                }
                return out;
            }

            if current.children().len() == 1 {
                current = current.children()[0];
                expr_is_from_current_plan = false;
                continue 'outer;
            }

            warn!(
                "Should NOT reach here, number of children: {} for plan name {}",
                current.children().len(), current.name()
            );
            break 'outer;
        }
    }
    vec![]
}

pub fn col_index_and_plan_for_join_like_plan<'a>(
    left: &'a Arc<dyn ExecutionPlan>,
    right: &'a Arc<dyn ExecutionPlan>,
    join_type: &JoinType,
    projection_ref: Option<ProjectionRef>,
    col_index: usize,
) -> Option<(usize, &'a Arc<dyn ExecutionPlan>)> {
    let (_join_schema, column_indices) =
        build_join_schema(&left.schema(), &right.schema(), &join_type);
    let col_index_to_search_in_indices = if let Some(projection_ref) = projection_ref {
        projection_ref.get(col_index).cloned()
    } else {
        Some(col_index)
    };
    if col_index_to_search_in_indices.is_none() {
        return None;
    }
    let col_index_to_search_in_indices = col_index_to_search_in_indices.unwrap();
    let column_index = column_indices.get(col_index_to_search_in_indices);

    if column_index.is_none() {
        return None;
    }
    let column_index = column_index.unwrap();

    match column_index.side {
        JoinSide::Left => Some((column_index.index, left)),
        JoinSide::Right => Some((column_index.index, right)),
        JoinSide::None => None,
    }
}
