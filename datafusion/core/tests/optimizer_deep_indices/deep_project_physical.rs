use arrow_schema::{DataType, Field, FieldRef};
use datafusion_common::Result;
use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion_common::{JoinSide, JoinType, ScalarValue, internal_err};
use datafusion_datasource::file::FileSource;
use datafusion_datasource::file_scan_config::FileScanConfigBuilder;
use datafusion_datasource::source::DataSourceExec;
use datafusion_datasource_parquet::source::ParquetSource;
use datafusion_datasource_parquet::{
    expr_is_get_field_or_array_or_cast_or_column, extract_expressions_containing_column,
    find_column_in_expr, fix_simplified_column_path, simplified_parquet_column_path,
};
use datafusion_physical_expr::ScalarFunctionExpr;
use datafusion_physical_expr::expressions::{CastExpr, Column, Literal};
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
use itertools::Itertools;
use log::{error, info, warn};
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;

#[derive(Debug)]
pub struct PushAllProjectionHints {}

impl PushAllProjectionHints {}

impl PhysicalOptimizerRule for PushAllProjectionHints {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        info!(
            "PushAllProjectionHints input: {}",
            displayable(plan.as_ref()).indent(true)
        );
        // find datasourceexecs
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
        let _ = plan.apply(|physical_plan| {
            if let Some(_ds_exec) =
                physical_plan.as_any().downcast_ref::<DataSourceExec>()
            {
                return Ok(TreeNodeRecursion::Jump);
            }
            let exprs = extract_expressions_containing_column_from_plan(&physical_plan);
            for expr in exprs {
                // info!(
                //     "accum expr for plan type={}: {:?}",
                //     &physical_plan.name(),
                //     &expr
                // );
                for (source, source_expr, source_col_index) in
                    find_source_for_column_expr(&expr, &physical_plan)
                {
                    // info!("  FOUND modified EXPR: {:?}", &source_expr);
                    if let Some(key) = get_key_from_plan(&source) {
                        accum
                            .entry(key)
                            .or_default()
                            .push((source_expr, source_col_index));
                    }
                }
            }
            Ok(TreeNodeRecursion::Continue)
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
                    let key = get_key_from_plan(&p).expect("Should have key");
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

pub fn get_key_from_plan(plan: &Arc<dyn ExecutionPlan>) -> Option<String> {
    if let Some(ds_exec) = plan.as_any().downcast_ref::<DataSourceExec>() {
        if let Some((_file_scan_conf, _parquet_source)) =
            ds_exec.downcast_to_file_source::<ParquetSource>()
        {
            // fix this, but how ?
            let plan_key = displayable(plan.as_ref()).indent(true).to_string();
            return Some(plan_key);
        }
    }
    None
}

pub fn extract_expressions_containing_column_from_plan(
    input: &Arc<dyn ExecutionPlan>,
) -> Vec<Arc<dyn PhysicalExpr>> {
    let mut out = vec![];
    get_expressions_to_check(input)
        .iter()
        .for_each(|e| out.extend(extract_expressions_containing_column(e)));
    out
}

/// extract lists of expression from a physical plan
/// TODO: add here for missing plan types.
pub fn get_expressions_to_check(
    plan: &Arc<dyn ExecutionPlan>,
) -> Vec<Arc<dyn PhysicalExpr>> {
    if let Some(plan) = plan.as_any().downcast_ref::<ProjectionExec>() {
        return plan
            .expr()
            .iter()
            .map(|pe| pe.expr.clone())
            .collect::<Vec<_>>();
    }
    if let Some(plan) = plan.as_any().downcast_ref::<FilterExec>() {
        return vec![plan.predicate().clone()];
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
    let saved_col = find_column_in_expr(expr).unwrap();
    // info!("REPLACING COL: {:?}", &saved_col);
    let new_expr = expr
        .clone()
        .transform_down(|e| {
            // info!("AT EXPR: {:?}", e);
            if let Some(col) = e.as_any().downcast_ref::<Column>()
                && (col.name() == saved_col.name() && col.index() == saved_col.index())
            {
                return Ok(Transformed::yes(new_col.clone()));
            }
            Ok(Transformed::no(e))
        })
        .unwrap();
    // info!("RETURN !!!!!!!!!!: {:?}", new_expr.data);
    new_expr.data
}

pub fn find_source_for_column_expr(
    expr: &Arc<dyn PhysicalExpr>,
    plan: &Arc<dyn ExecutionPlan>,
) -> Vec<(Arc<dyn ExecutionPlan>, Arc<dyn PhysicalExpr>, usize)> {
    if let Some(col) = find_column_in_expr(expr) {
        let mut col_index = col.index();

        // info!("FIND SOURCE for index={}", col_index);
        let mut current = plan;

        if let Some(plan) = current.as_any().downcast_ref::<SortMergeJoinExec>() {
            let mut found = false;
            for (l, r) in plan.on().iter() {
                if expr == l {
                    current = plan.left();
                    found = true;
                }
                if expr == r {
                    current = plan.right();
                    found = true;
                }
            }
            if !found {
                warn!("Could not find expr {:?} in plan HashJoinExec", expr);
                return vec![];
            }
        }

        'outer: loop {
            // info!(
            //     "  start at plan {}, col_index={}",
            //     current.name(),
            //     col_index
            // );

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
                            replace_column_in_expr(expr, &actual_expr);
                        return vec![(current.clone(), new_scalar_func_expr, col_index)];
                    } else {
                        return vec![];
                    }
                }

                return vec![];
            }
            // col_index refers to the index of the column in current plan's input
            if current.children().len() == 0 {
                error!("unreachable code in find_source_for_column_expr: 0 children");
                return vec![];
            }
            // some plans have projection, like projection, filter, hash join, nested loop join
            if let Some(proj) = current.as_any().downcast_ref::<ProjectionExec>() {
                if Arc::ptr_eq(current, plan) {
                    // the expression starts here
                    current = current.children()[0];
                } else {
                    if let Some(expr_in_projection) =
                        proj.projection_expr().as_ref().get(col_index)
                    {
                        if expr_is_get_field_or_array_or_cast_or_column(
                            &expr_in_projection.expr,
                        ) {
                            let column_in_projection =
                                find_column_in_expr(&expr_in_projection.expr).unwrap();
                            col_index = column_in_projection.index();
                            current = proj.children()[0];
                            continue 'outer;
                        } else {
                            // we DON't do anything, don't know how to handle other things in projection
                            return vec![];
                        }
                    } else {
                        return vec![];
                    }
                }
            }

            if let Some(filter_child) = current.as_any().downcast_ref::<FilterExec>() {
                // filter has a projection
                let new_col_index = if let Some(projection) = &filter_child.projection() {
                    projection.get(col_index)
                } else {
                    Some(&col_index)
                };
                if let Some(new_col_index) = new_col_index {
                    col_index = *new_col_index;
                    current = filter_child.input();
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
                        if expr == l {
                            current = join.left();
                            found = true;
                        }
                        if expr == r {
                            current = join.right();
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
                        if expr == l {
                            current = join.left();
                            found = true;
                        }
                        if expr == r {
                            current = join.right();
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
                        if expr == l {
                            current = join.left();
                            found = true;
                        }
                        if expr == r {
                            current = join.right();
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
                    out.extend(find_source_for_column_expr(expr, union_child))
                }
                return out;
            }

            if current.children().len() == 1 {
                current = current.children()[0];
                continue 'outer;
            }

            panic!(
                "Should NOT reach here, number of children: {}",
                current.children().len()
            )
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

pub fn projection_specifier(
    plan: &Arc<dyn ExecutionPlan>,
) -> Option<HashMap<usize, Vec<String>>> {
    if let Some(data_source_exec) = plan.as_any().downcast_ref::<DataSourceExec>() {
        if let Some((_file_scan_conf, parquet_source)) =
            data_source_exec.downcast_to_file_source::<ParquetSource>()
        {
            let source_schema = parquet_source.table_schema().file_schema().clone();
            let projection_exprs = parquet_source
                .projection()
                .expect("parquet_source projection");
            let projection_hints = &parquet_source.projection_hints;

            let col_indices_referenced_in_hints =
                parquet_source.projection_hints_indices.clone();

            // info!(
            //     "col indices referenced {:?}",
            //     &col_indices_referenced_in_hints
            // );
            let mut deep_column_map: HashMap<usize, HashSet<String>> = HashMap::new();

            for expr in projection_exprs
                .iter()
                .map(|pe| extract_expressions_containing_column(&pe.expr))
                .flatten()
            {
                let col_arg = find_column_in_expr(&expr).unwrap();
                let col_index = source_schema
                    .index_of(col_arg.name())
                    .expect("Col in table");
                if !col_indices_referenced_in_hints.contains(&col_index) {
                    // let marker = format!("xx: {} {}", col_index, expr_to_deep_projection(&pexpr));
                    let marker = simplified_parquet_column_path(&expr);
                    deep_column_map.entry(col_index).or_default().insert(marker);
                }
            }

            for pexpr in projection_hints.iter() {
                let expr = pexpr.clone().expr;
                // info!("projection hint: {:?}", expr);
                let col_arg = find_column_in_expr(&expr).unwrap();
                let col_index = source_schema
                    .index_of(col_arg.name())
                    .expect("Col in table");
                let marker = simplified_parquet_column_path(&expr);
                // info!("adding marker {} for expr = {:?}", marker, expr);
                deep_column_map.entry(col_index).or_default().insert(marker);
            }

            let final_map: HashMap<usize, Vec<String>> = deep_column_map
                .iter()
                .map(|(k, v)| {
                    let k = k.clone();
                    let newv = v
                        .into_iter()
                        // clone
                        .map(|s| s.clone())
                        // remove empty specifiers
                        .filter(|v| v != "")
                        // fix fake field names which are actually map names
                        .map(|v| {
                            // info!("fix_deep for {}", v.as_str());
                            fix_simplified_column_path(v.as_str(), source_schema.field(k))
                                .unwrap()
                            // let pieces = v.split(".").collect::<Vec<_>>();
                        })
                        // sort and then keep the minimal prefixes for each string
                        .sorted()
                        .dedup_by(|longer, shorter| longer.starts_with(shorter.as_str()))
                        .collect::<Vec<String>>();
                    (k, newv)
                })
                .into_iter()
                .collect();
            return Some(final_map);
        }
    }
    None
}
