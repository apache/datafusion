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

//! Experimental query fusion rewrites.
//!
//! This proof of concept implements the UNION ALL branch-tag rewrite from
//! "Computation reuse via fusion in Amazon Athena" for a deliberately narrow
//! shape: UNION ALL branches that differ only by safe filters over the same
//! source and compatible wrappers.

use std::sync::Arc;

use crate::{OptimizerConfig, OptimizerRule};
use datafusion_common::tree_node::{
    Transformed, TreeNode, TreeNodeRecursion, TreeNodeRewriter,
};
use datafusion_common::{Column, Result};
use datafusion_expr::WindowFunctionDefinition;
use datafusion_expr::expr::WindowFunction;
use datafusion_expr::logical_plan::builder::LogicalPlanBuilder;
use datafusion_expr::{
    Aggregate, Expr, Filter, Join, JoinType, LogicalPlan, Projection, SubqueryAlias,
    Union, col, lit,
};
use log::debug;

const BRANCH_TAG_COLUMN: &str = "__datafusion_query_fusion_branch";

#[derive(Default, Debug)]
pub struct QueryFusion;

impl QueryFusion {
    #[expect(missing_docs)]
    pub fn new() -> Self {
        Self
    }
}

impl OptimizerRule for QueryFusion {
    fn name(&self) -> &str {
        "query_fusion"
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        if !config.options().optimizer.enable_query_fusion {
            return Ok(Transformed::no(plan));
        }

        if !plan.exists(|p| {
            Ok(matches!(
                p,
                LogicalPlan::Union(_) | LogicalPlan::Join(_) | LogicalPlan::Projection(_)
            ))
        })? {
            return Ok(Transformed::no(plan));
        }

        plan.rewrite_with_subqueries(&mut QueryFusionRewriter)
    }
}

struct QueryFusionRewriter;

impl TreeNodeRewriter for QueryFusionRewriter {
    type Node = LogicalPlan;

    fn f_up(&mut self, plan: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
        match &plan {
            LogicalPlan::Union(_) => match try_rewrite_union_all(plan.clone())? {
                Some(rewritten) => Ok(Transformed::yes(rewritten)),
                None => Ok(Transformed::no(plan)),
            },
            LogicalPlan::Join(_) => {
                if let Some(rewritten) =
                    try_rewrite_group_by_join_to_window(plan.clone())?
                {
                    Ok(Transformed::yes(rewritten))
                } else {
                    match try_rewrite_scalar_aggregate_join(plan.clone())? {
                        Some(rewritten) => Ok(Transformed::yes(rewritten)),
                        None => Ok(Transformed::no(plan)),
                    }
                }
            }
            LogicalPlan::Projection(_) => {
                match try_rewrite_scalar_aggregate_subqueries(plan.clone())? {
                    Some(rewritten) => Ok(Transformed::yes(rewritten)),
                    None => Ok(Transformed::no(plan)),
                }
            }
            _ => Ok(Transformed::no(plan)),
        }
    }
}

fn try_rewrite_union_all(plan: LogicalPlan) -> Result<Option<LogicalPlan>> {
    let LogicalPlan::Union(Union { inputs, schema }) = plan else {
        return Ok(None);
    };

    if inputs.len() < 2 {
        debug!("query_fusion skipped: UNION has fewer than two inputs");
        return Ok(None);
    }

    let mut branches = Vec::with_capacity(inputs.len());
    for input in inputs {
        let Some(branch) = extract_branch(Arc::unwrap_or_clone(input))? else {
            return Ok(None);
        };
        branches.push(branch);
    }

    let Some(first) = branches.first() else {
        return Ok(None);
    };
    if !branches
        .iter()
        .all(|branch| branch.source == first.source && branch.wrappers == first.wrappers)
    {
        debug!("query_fusion skipped: UNION branches do not share one source");
        return Ok(None);
    }

    let fused_source = first.source.clone();
    let tag_values = branches
        .iter()
        .enumerate()
        .map(|(idx, _)| vec![lit(idx as i32)])
        .collect::<Vec<_>>();
    let tags = LogicalPlanBuilder::values(tag_values)?
        .project(vec![col("column1").alias(BRANCH_TAG_COLUMN)])?
        .build()?;

    let tagged = LogicalPlanBuilder::from(fused_source)
        .cross_join(tags)?
        .build()?;

    let tag_col = col(BRANCH_TAG_COLUMN);
    let filter = branches
        .iter()
        .enumerate()
        .map(|(idx, branch)| {
            tag_col
                .clone()
                .eq(lit(idx as i32))
                .and(branch.predicate.clone())
        })
        .reduce(|left, right| left.or(right))
        .expect("at least two UNION inputs");

    let filtered = LogicalPlanBuilder::from(tagged).filter(filter)?.build()?;
    let projected = wrap_branch(filtered, &first.wrappers)?;
    align_plan_to_schema(projected, schema).map(Some)
}

struct UnionBranch {
    source: LogicalPlan,
    predicate: Expr,
    wrappers: Vec<Wrapper>,
}

fn extract_branch(plan: LogicalPlan) -> Result<Option<UnionBranch>> {
    let (wrappers, plan) = peel_wrappers(plan);

    if !wrapper_projections_are_safe(&wrappers) {
        debug!("query_fusion skipped: wrapper projection is unsafe");
        return Ok(None);
    }

    match plan {
        LogicalPlan::Filter(Filter {
            predicate, input, ..
        }) => {
            if !is_mergeable_predicate(&predicate) {
                debug!("query_fusion skipped: branch predicate is unsafe");
                return Ok(None);
            }
            Ok(Some(UnionBranch {
                source: strip_passthrough_nodes(Arc::unwrap_or_clone(input)),
                predicate,
                wrappers,
            }))
        }
        LogicalPlan::Limit(_) => {
            debug!("query_fusion skipped: branch contains LIMIT");
            Ok(None)
        }
        LogicalPlan::Sort(_) => {
            debug!("query_fusion skipped: branch contains SORT");
            Ok(None)
        }
        other => Ok(Some(UnionBranch {
            source: strip_passthrough_nodes(other),
            predicate: lit(true),
            wrappers,
        })),
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Wrapper {
    Projection {
        expr: Vec<Expr>,
        schema: datafusion_common::DFSchemaRef,
    },
    SubqueryAlias {
        alias: datafusion_common::TableReference,
        schema: datafusion_common::DFSchemaRef,
    },
}

fn peel_wrappers(mut plan: LogicalPlan) -> (Vec<Wrapper>, LogicalPlan) {
    let mut wrappers = vec![];
    loop {
        match plan {
            LogicalPlan::Projection(Projection {
                expr,
                input,
                schema,
                ..
            }) => {
                wrappers.push(Wrapper::Projection { expr, schema });
                plan = Arc::unwrap_or_clone(input);
            }
            LogicalPlan::SubqueryAlias(SubqueryAlias {
                input,
                alias,
                schema,
                ..
            }) => {
                wrappers.push(Wrapper::SubqueryAlias { alias, schema });
                plan = Arc::unwrap_or_clone(input);
            }
            other => return (wrappers, other),
        }
    }
}

fn wrap_branch(mut plan: LogicalPlan, wrappers: &[Wrapper]) -> Result<LogicalPlan> {
    for wrapper in wrappers.iter().rev() {
        plan = match wrapper {
            Wrapper::Projection { expr, schema } => {
                LogicalPlan::Projection(Projection::try_new_with_schema(
                    expr.clone(),
                    Arc::new(plan),
                    Arc::clone(schema),
                )?)
            }
            Wrapper::SubqueryAlias { alias, .. } => LogicalPlan::SubqueryAlias(
                SubqueryAlias::try_new(Arc::new(plan), alias.clone())?,
            ),
        };
    }
    Ok(plan)
}

fn strip_passthrough_nodes(mut plan: LogicalPlan) -> LogicalPlan {
    loop {
        plan = match plan {
            LogicalPlan::Projection(Projection { input, .. }) => {
                Arc::unwrap_or_clone(input)
            }
            LogicalPlan::SubqueryAlias(SubqueryAlias { input, .. }) => {
                Arc::unwrap_or_clone(input)
            }
            other => return other,
        };
    }
}

fn try_rewrite_scalar_aggregate_subqueries(
    plan: LogicalPlan,
) -> Result<Option<LogicalPlan>> {
    let LogicalPlan::Projection(Projection {
        expr,
        input,
        schema,
        ..
    }) = plan
    else {
        return Ok(None);
    };

    let candidates = collect_scalar_aggregate_subqueries(&expr)?;
    if candidates.len() < 2 {
        return Ok(None);
    }

    let Some(first) = candidates.first() else {
        return Ok(None);
    };
    if !candidates
        .iter()
        .all(|candidate| candidate.branch.source == first.branch.source)
    {
        debug!(
            "query_fusion skipped: scalar aggregate subqueries do not share one source"
        );
        return Ok(None);
    }

    let source = add_source_filter(
        first.branch.source.clone(),
        candidates
            .iter()
            .map(|candidate| candidate.branch.predicate.clone()),
    )?;

    let mut merged_exprs = Vec::with_capacity(candidates.len());
    let mut replacements = Vec::with_capacity(candidates.len());
    for (idx, candidate) in candidates.iter().enumerate() {
        let Some(aggr_expr) = candidate.branch.aggr_expr.first() else {
            return Ok(None);
        };
        let alias = format!("__datafusion_query_fusion_scalar_agg_{idx}");
        merged_exprs.push(
            add_aggregate_filter(
                strip_outer_alias(aggr_expr.clone()),
                &candidate.branch.predicate,
            )?
            .alias(&alias),
        );
        replacements.push((
            candidate.scalar_subquery.clone(),
            Expr::Column(Column::new_unqualified(alias)),
        ));
    }

    let merged = LogicalPlan::Aggregate(Aggregate::try_new(
        Arc::new(source),
        Vec::<Expr>::new(),
        merged_exprs,
    )?);
    let joined_input = LogicalPlanBuilder::from(Arc::unwrap_or_clone(input))
        .cross_join(merged)?
        .build()?;

    let rewritten_expr = expr
        .into_iter()
        .map(|expr| replace_scalar_subqueries(expr, &replacements))
        .collect::<Result<Vec<_>>>()?;

    Ok(Some(LogicalPlan::Projection(
        Projection::try_new_with_schema(rewritten_expr, Arc::new(joined_input), schema)?,
    )))
}

struct ScalarAggregateSubquery {
    scalar_subquery: Expr,
    branch: ScalarAggregateBranch,
}

fn collect_scalar_aggregate_subqueries(
    exprs: &[Expr],
) -> Result<Vec<ScalarAggregateSubquery>> {
    let mut candidates = vec![];
    for expr in exprs {
        expr.apply(|expr| {
            let Expr::ScalarSubquery(subquery) = expr else {
                return Ok(TreeNodeRecursion::Continue);
            };
            if !subquery.outer_ref_columns.is_empty() {
                return Ok(TreeNodeRecursion::Jump);
            }
            if let Some(branch) = extract_scalar_aggregate((*subquery.subquery).clone())?
                && branch.aggr_expr.len() == 1
            {
                candidates.push(ScalarAggregateSubquery {
                    scalar_subquery: expr.clone(),
                    branch,
                });
            }
            Ok(TreeNodeRecursion::Jump)
        })?;
    }
    Ok(candidates)
}

fn replace_scalar_subqueries(expr: Expr, replacements: &[(Expr, Expr)]) -> Result<Expr> {
    Ok(expr
        .transform(|expr| {
            if let Some((_, replacement)) = replacements
                .iter()
                .find(|(scalar_subquery, _)| scalar_subquery == &expr)
            {
                Ok(Transformed::yes(replacement.clone()))
            } else {
                Ok(Transformed::no(expr))
            }
        })?
        .data)
}

fn try_rewrite_group_by_join_to_window(plan: LogicalPlan) -> Result<Option<LogicalPlan>> {
    let LogicalPlan::Join(Join {
        left,
        right,
        on,
        filter,
        join_type,
        schema,
        ..
    }) = plan
    else {
        return Ok(None);
    };

    if join_type != JoinType::Inner || on.is_empty() || filter.is_some() {
        return Ok(None);
    }

    let LogicalPlan::Aggregate(Aggregate {
        input: aggregate_input,
        group_expr,
        aggr_expr,
        ..
    }) = right.as_ref()
    else {
        return Ok(None);
    };

    if group_expr.is_empty() || aggr_expr.is_empty() || group_expr.len() != on.len() {
        return Ok(None);
    }

    if strip_passthrough_nodes(left.as_ref().clone())
        != strip_passthrough_nodes(aggregate_input.as_ref().clone())
    {
        return Ok(None);
    }

    if !aggr_expr.iter().all(is_mergeable_window_aggregate_expr) {
        debug!("query_fusion skipped: grouped aggregate is unsafe for window");
        return Ok(None);
    }

    let left_schema = Arc::clone(left.schema());
    let rewritten_group_expr = group_expr
        .iter()
        .cloned()
        .map(|expr| rewrite_columns_to_schema(expr, &left_schema))
        .collect::<Result<Vec<_>>>()?;

    for (left_key, right_key) in &on {
        let rewritten_right_key =
            rewrite_columns_to_schema(right_key.clone(), &left_schema)?;
        if left_key != &rewritten_right_key
            || !rewritten_group_expr.iter().any(|expr| expr == left_key)
        {
            return Ok(None);
        }
    }

    let non_null_filter = on
        .iter()
        .map(|(left_key, _)| left_key.clone().is_not_null())
        .reduce(|left, right| left.and(right));

    let mut input = Arc::unwrap_or_clone(left);
    if let Some(non_null_filter) = non_null_filter {
        input = LogicalPlanBuilder::from(input)
            .filter(non_null_filter)?
            .build()?;
    }

    let left_field_count = input.schema().fields().len();
    let mut window_exprs = Vec::with_capacity(aggr_expr.len());
    let mut projection_exprs = input
        .schema()
        .iter()
        .enumerate()
        .map(|(i, _)| Expr::Column(Column::from(input.schema().qualified_field(i))))
        .collect::<Vec<_>>();
    projection_exprs.extend(rewritten_group_expr.iter().cloned());

    for (idx, aggr_expr) in aggr_expr.iter().enumerate() {
        let alias = format!("__datafusion_query_fusion_window_agg_{idx}");
        let aggr_expr = rewrite_columns_to_schema(aggr_expr.clone(), &left_schema)?;
        window_exprs.push(aggregate_expr_to_window_expr(
            aggr_expr,
            &rewritten_group_expr,
            &alias,
        )?);
        projection_exprs.push(Expr::Column(Column::new_unqualified(alias)));
    }

    if projection_exprs.len() != schema.fields().len()
        || left_field_count + group_expr.len() + aggr_expr.len() != schema.fields().len()
    {
        return Ok(None);
    }

    let windowed = LogicalPlanBuilder::from(input)
        .window(window_exprs)?
        .build()?;

    Ok(Some(LogicalPlan::Projection(
        Projection::try_new_with_schema(projection_exprs, Arc::new(windowed), schema)?,
    )))
}

fn try_rewrite_scalar_aggregate_join(plan: LogicalPlan) -> Result<Option<LogicalPlan>> {
    let LogicalPlan::Join(Join {
        left,
        right,
        on,
        filter,
        join_type,
        schema,
        ..
    }) = plan
    else {
        return Ok(None);
    };

    if join_type != JoinType::Inner || !on.is_empty() || filter.is_some() {
        return Ok(None);
    }

    let Some(left_aggregate) = extract_scalar_aggregate(Arc::unwrap_or_clone(left))?
    else {
        return Ok(None);
    };
    let Some(right_aggregate) = extract_scalar_aggregate(Arc::unwrap_or_clone(right))?
    else {
        return Ok(None);
    };

    if left_aggregate.source != right_aggregate.source {
        debug!("query_fusion skipped: scalar aggregates do not share one source");
        return Ok(None);
    }

    let source = add_source_filter(
        left_aggregate.source,
        [
            left_aggregate.predicate.clone(),
            right_aggregate.predicate.clone(),
        ],
    )?;

    let mut merged_exprs = Vec::with_capacity(
        left_aggregate.aggr_expr.len() + right_aggregate.aggr_expr.len(),
    );
    let mut projection_exprs = Vec::with_capacity(merged_exprs.capacity());

    for (idx, (aggr_expr, predicate)) in left_aggregate
        .aggr_expr
        .into_iter()
        .map(|expr| (expr, left_aggregate.predicate.clone()))
        .chain(
            right_aggregate
                .aggr_expr
                .into_iter()
                .map(|expr| (expr, right_aggregate.predicate.clone())),
        )
        .enumerate()
    {
        let alias = format!("__datafusion_query_fusion_scalar_agg_{idx}");
        merged_exprs.push(
            add_aggregate_filter(strip_outer_alias(aggr_expr), &predicate)?.alias(&alias),
        );
        projection_exprs.push(Expr::Column(Column::new_unqualified(alias)));
    }

    let merged = LogicalPlan::Aggregate(Aggregate::try_new(
        Arc::new(source),
        Vec::<Expr>::new(),
        merged_exprs,
    )?);

    Ok(Some(LogicalPlan::Projection(
        Projection::try_new_with_schema(projection_exprs, Arc::new(merged), schema)?,
    )))
}

struct ScalarAggregateBranch {
    source: LogicalPlan,
    predicate: Expr,
    aggr_expr: Vec<Expr>,
}

fn extract_scalar_aggregate(
    mut plan: LogicalPlan,
) -> Result<Option<ScalarAggregateBranch>> {
    if let LogicalPlan::Projection(Projection { expr, input, .. }) = plan {
        if expr.len() != 1 || !wrapper_projection_expr_is_safe(&expr[0]) {
            return Ok(None);
        }
        plan = Arc::unwrap_or_clone(input);
    }

    let LogicalPlan::Aggregate(Aggregate {
        input,
        group_expr,
        aggr_expr,
        ..
    }) = plan
    else {
        return Ok(None);
    };

    if !group_expr.is_empty() || aggr_expr.is_empty() {
        return Ok(None);
    }

    if !aggr_expr.iter().all(is_mergeable_aggregate_expr) {
        debug!("query_fusion skipped: scalar aggregate expression is unsafe");
        return Ok(None);
    }

    let input = Arc::unwrap_or_clone(input);
    let (source, predicate) = match input {
        LogicalPlan::Filter(Filter {
            predicate, input, ..
        }) => {
            if !is_mergeable_predicate(&predicate) {
                debug!("query_fusion skipped: scalar aggregate filter is unsafe");
                return Ok(None);
            }
            (Arc::unwrap_or_clone(input), predicate)
        }
        other => (other, lit(true)),
    };

    Ok(Some(ScalarAggregateBranch {
        source,
        predicate,
        aggr_expr,
    }))
}

fn add_source_filter(
    source: LogicalPlan,
    predicates: impl IntoIterator<Item = Expr>,
) -> Result<LogicalPlan> {
    let predicate = predicates
        .into_iter()
        .filter(|expr| expr != &lit(true))
        .reduce(|left, right| left.or(right));

    match predicate {
        Some(predicate) => LogicalPlanBuilder::from(source).filter(predicate)?.build(),
        None => Ok(source),
    }
}

fn add_aggregate_filter(aggr_expr: Expr, predicate: &Expr) -> Result<Expr> {
    if predicate == &lit(true) {
        return Ok(aggr_expr);
    }

    Ok(aggr_expr
        .transform(|expr| {
            let Expr::AggregateFunction(mut aggregate) = expr else {
                return Ok(Transformed::no(expr));
            };

            let filter = aggregate
                .params
                .filter
                .map(|existing| existing.and(predicate.clone()))
                .unwrap_or_else(|| predicate.clone());
            aggregate.params.filter = Some(Box::new(filter));
            Ok(Transformed::yes(Expr::AggregateFunction(aggregate)))
        })?
        .data)
}

fn strip_outer_alias(expr: Expr) -> Expr {
    match expr {
        Expr::Alias(alias) => *alias.expr,
        other => other,
    }
}

fn aggregate_expr_to_window_expr(
    expr: Expr,
    partition_by: &[Expr],
    alias: &str,
) -> Result<Expr> {
    let Expr::AggregateFunction(aggregate) = strip_outer_alias(expr) else {
        return datafusion_common::plan_err!(
            "expected aggregate function in query fusion window rewrite"
        );
    };

    let mut window = WindowFunction::new(
        WindowFunctionDefinition::AggregateUDF(aggregate.func),
        aggregate.params.args,
    );
    window.params.partition_by = partition_by.to_vec();
    window.params.filter = aggregate.params.filter;
    window.params.null_treatment = aggregate.params.null_treatment;
    Ok(Expr::WindowFunction(Box::new(window)).alias(alias))
}

fn rewrite_columns_to_schema(
    expr: Expr,
    schema: &datafusion_common::DFSchema,
) -> Result<Expr> {
    Ok(expr
        .transform(|expr| {
            let Expr::Column(column) = expr else {
                return Ok(Transformed::no(expr));
            };
            let matches = schema.qualified_fields_with_unqualified_name(&column.name);
            if matches.len() == 1 {
                Ok(Transformed::yes(Expr::Column(Column::from(matches[0]))))
            } else {
                Ok(Transformed::no(Expr::Column(column)))
            }
        })?
        .data)
}

fn is_mergeable_aggregate_expr(expr: &Expr) -> bool {
    !expr.is_volatile()
        && !expr_contains_subquery(expr)
        && expr
            .exists(|expr| {
                let Expr::AggregateFunction(aggregate) = expr else {
                    return Ok(false);
                };
                Ok(aggregate.params.distinct || !aggregate.params.order_by.is_empty())
            })
            .map(|has_unsupported| !has_unsupported)
            .unwrap_or(false)
}

fn is_mergeable_window_aggregate_expr(expr: &Expr) -> bool {
    !expr.is_volatile()
        && !expr_contains_subquery(expr)
        && matches!(
            strip_outer_alias(expr.clone()),
            Expr::AggregateFunction(aggregate)
                if !aggregate.params.distinct && aggregate.params.order_by.is_empty()
        )
}

fn align_plan_to_schema(
    plan: LogicalPlan,
    schema: datafusion_common::DFSchemaRef,
) -> Result<LogicalPlan> {
    if plan.schema() == &schema {
        return Ok(plan);
    }

    let expr = plan
        .schema()
        .iter()
        .take(schema.fields().len())
        .enumerate()
        .map(|(i, _)| Expr::Column(Column::from(plan.schema().qualified_field(i))))
        .collect::<Vec<_>>();

    Ok(LogicalPlan::Projection(Projection::try_new_with_schema(
        expr,
        Arc::new(plan),
        schema,
    )?))
}

fn is_mergeable_predicate(expr: &Expr) -> bool {
    !expr.is_volatile() && !expr_contains_subquery(expr)
}

fn wrapper_projections_are_safe(wrappers: &[Wrapper]) -> bool {
    wrappers.iter().all(|w| match w {
        Wrapper::Projection { expr, .. } => {
            expr.iter().all(wrapper_projection_expr_is_safe)
        }
        Wrapper::SubqueryAlias { .. } => true,
    })
}

fn wrapper_projection_expr_is_safe(expr: &Expr) -> bool {
    !expr.is_volatile() && !expr_contains_subquery(expr)
}

fn expr_contains_subquery(expr: &Expr) -> bool {
    expr.exists(|e| match e {
        Expr::ScalarSubquery(_) | Expr::Exists(_) | Expr::InSubquery(_) => Ok(true),
        _ => Ok(false),
    })
    .expect("boolean expression walk is infallible")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::OptimizerContext;
    use crate::assert_optimized_plan_eq_snapshot;
    use crate::test::test_table_scan_with_name;
    use arrow::datatypes::DataType;
    use datafusion_expr::ExprFunctionExt;
    use datafusion_expr::test::function_stub::{avg, count, sum};
    use datafusion_expr::{
        ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
        Volatility, scalar_subquery,
    };

    macro_rules! assert_optimized_plan_equal {
        (
            $plan:expr,
            @ $expected:literal $(,)?
        ) => {{
            let mut options = datafusion_common::config::ConfigOptions::default();
            options.optimizer.enable_query_fusion = true;
            let optimizer_ctx = OptimizerContext::new_with_config_options(Arc::new(options))
                .with_max_passes(1);
            let rules: Vec<Arc<dyn crate::OptimizerRule + Send + Sync>> =
                vec![Arc::new(QueryFusion::new())];
            assert_optimized_plan_eq_snapshot!(
                optimizer_ctx,
                rules,
                $plan,
                @ $expected,
            )
        }};
    }

    #[derive(Debug, PartialEq, Eq, Hash)]
    struct VolatileTestUdf;

    impl ScalarUDFImpl for VolatileTestUdf {
        fn name(&self) -> &str {
            "volatile_test"
        }

        fn signature(&self) -> &Signature {
            static SIGNATURE: std::sync::LazyLock<Signature> =
                std::sync::LazyLock::new(|| Signature::nullary(Volatility::Volatile));
            &SIGNATURE
        }

        fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
            Ok(DataType::Float64)
        }

        fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue> {
            panic!("VolatileTestUdf is not intended for execution")
        }
    }

    fn volatile_expr() -> Expr {
        ScalarUDF::new_from_impl(VolatileTestUdf).call(vec![])
    }

    #[test]
    fn rewrite_union_all_same_source_filters() -> Result<()> {
        let left = LogicalPlanBuilder::from(test_table_scan_with_name("t")?)
            .filter(col("a").eq(lit(1)))?
            .build()?;
        let right = LogicalPlanBuilder::from(test_table_scan_with_name("t")?)
            .filter(col("a").eq(lit(2)))?
            .build()?;

        let plan = LogicalPlanBuilder::from(left).union(right)?.build()?;

        assert_optimized_plan_equal!(plan, @r"
        Projection: t.a, t.b, t.c
          Filter: __datafusion_query_fusion_branch = Int32(0) AND t.a = Int32(1) OR __datafusion_query_fusion_branch = Int32(1) AND t.a = Int32(2)
            Cross Join:
              TableScan: t
              Projection: column1 AS __datafusion_query_fusion_branch
                Values: (Int32(0)), (Int32(1))
        ")?;
        Ok(())
    }

    #[test]
    fn keep_union_all_different_sources() -> Result<()> {
        let left = LogicalPlanBuilder::from(test_table_scan_with_name("t1")?)
            .filter(col("a").eq(lit(1)))?
            .build()?;
        let right = LogicalPlanBuilder::from(test_table_scan_with_name("t2")?)
            .filter(col("a").eq(lit(2)))?
            .build()?;

        let plan = LogicalPlanBuilder::from(left).union(right)?.build()?;

        assert_optimized_plan_equal!(plan, @r"
        Union
          Filter: t1.a = Int32(1)
            TableScan: t1
          Filter: t2.a = Int32(2)
            TableScan: t2
        ")?;
        Ok(())
    }

    #[test]
    fn rewrite_union_all_preserves_overlap_duplicates() -> Result<()> {
        let left = LogicalPlanBuilder::from(test_table_scan_with_name("t")?)
            .filter(col("a").gt(lit(1)))?
            .build()?;
        let right = LogicalPlanBuilder::from(test_table_scan_with_name("t")?)
            .filter(col("a").lt(lit(10)))?
            .build()?;

        let plan = LogicalPlanBuilder::from(left).union(right)?.build()?;

        assert_optimized_plan_equal!(plan, @r"
        Projection: t.a, t.b, t.c
          Filter: __datafusion_query_fusion_branch = Int32(0) AND t.a > Int32(1) OR __datafusion_query_fusion_branch = Int32(1) AND t.a < Int32(10)
            Cross Join:
              TableScan: t
              Projection: column1 AS __datafusion_query_fusion_branch
                Values: (Int32(0)), (Int32(1))
        ")?;
        Ok(())
    }

    #[test]
    fn rewrite_union_all_with_matching_projection() -> Result<()> {
        let left = LogicalPlanBuilder::from(test_table_scan_with_name("emp")?)
            .filter(col("a").eq(lit(1)))?
            .project(vec![col("a").alias("mgr"), col("b").alias("comm")])?
            .build()?;
        let right = LogicalPlanBuilder::from(test_table_scan_with_name("emp")?)
            .filter(col("a").eq(lit(2)))?
            .project(vec![col("a").alias("mgr"), col("b").alias("comm")])?
            .build()?;

        let plan = LogicalPlanBuilder::from(left).union(right)?.build()?;

        assert_optimized_plan_equal!(plan, @r"
        Projection: emp.a AS mgr, emp.b AS comm
          Filter: __datafusion_query_fusion_branch = Int32(0) AND emp.a = Int32(1) OR __datafusion_query_fusion_branch = Int32(1) AND emp.a = Int32(2)
            Cross Join:
              TableScan: emp
              Projection: column1 AS __datafusion_query_fusion_branch
                Values: (Int32(0)), (Int32(1))
        ")?;
        Ok(())
    }

    #[test]
    fn keep_union_all_with_volatile_projection() -> Result<()> {
        let left = LogicalPlanBuilder::from(test_table_scan_with_name("t")?)
            .filter(col("a").eq(lit(1)))?
            .project(vec![volatile_expr().alias("v"), col("a")])?
            .build()?;
        let right = LogicalPlanBuilder::from(test_table_scan_with_name("t")?)
            .filter(col("a").eq(lit(2)))?
            .project(vec![volatile_expr().alias("v"), col("a")])?
            .build()?;

        let plan = LogicalPlanBuilder::from(left).union(right)?.build()?;

        assert_optimized_plan_equal!(plan, @r"
        Union
          Projection: volatile_test() AS v, t.a
            Filter: t.a = Int32(1)
              TableScan: t
          Projection: volatile_test() AS v, t.a
            Filter: t.a = Int32(2)
              TableScan: t
        ")?;
        Ok(())
    }

    #[test]
    fn rewrite_scalar_aggregate_cross_join_same_source() -> Result<()> {
        let left = LogicalPlanBuilder::from(test_table_scan_with_name("t")?)
            .filter(col("a").gt(lit(1)))?
            .aggregate(Vec::<Expr>::new(), vec![sum(col("b")).alias("sum_b")])?
            .build()?;
        let right = LogicalPlanBuilder::from(test_table_scan_with_name("t")?)
            .filter(col("a").lt(lit(10)))?
            .aggregate(Vec::<Expr>::new(), vec![avg(col("c")).alias("avg_c")])?
            .build()?;

        let plan = LogicalPlanBuilder::from(left).cross_join(right)?.build()?;

        assert_optimized_plan_equal!(plan, @r"
        Projection: __datafusion_query_fusion_scalar_agg_0, __datafusion_query_fusion_scalar_agg_1
          Aggregate: groupBy=[[]], aggr=[[sum(t.b) FILTER (WHERE t.a > Int32(1)) AS __datafusion_query_fusion_scalar_agg_0, avg(t.c) FILTER (WHERE t.a < Int32(10)) AS __datafusion_query_fusion_scalar_agg_1]]
            Filter: t.a > Int32(1) OR t.a < Int32(10)
              TableScan: t
        ")?;
        Ok(())
    }

    #[test]
    fn rewrite_scalar_aggregate_cross_join_preserves_existing_filters() -> Result<()> {
        let left_agg = sum(col("b"))
            .filter(col("c").gt(lit(5)))
            .build()?
            .alias("sum_b");
        let right_agg = count(col("a")).alias("count_a");

        let left = LogicalPlanBuilder::from(test_table_scan_with_name("t")?)
            .filter(col("a").gt(lit(1)))?
            .aggregate(Vec::<Expr>::new(), vec![left_agg])?
            .build()?;
        let right = LogicalPlanBuilder::from(test_table_scan_with_name("t")?)
            .filter(col("a").lt(lit(10)))?
            .aggregate(Vec::<Expr>::new(), vec![right_agg])?
            .build()?;

        let plan = LogicalPlanBuilder::from(left).cross_join(right)?.build()?;

        assert_optimized_plan_equal!(plan, @r"
        Projection: __datafusion_query_fusion_scalar_agg_0, __datafusion_query_fusion_scalar_agg_1
          Aggregate: groupBy=[[]], aggr=[[sum(t.b) FILTER (WHERE t.c > Int32(5) AND t.a > Int32(1)) AS __datafusion_query_fusion_scalar_agg_0, COUNT(t.a) FILTER (WHERE t.a < Int32(10)) AS __datafusion_query_fusion_scalar_agg_1]]
            Filter: t.a > Int32(1) OR t.a < Int32(10)
              TableScan: t
        ")?;
        Ok(())
    }

    #[test]
    fn keep_scalar_aggregate_cross_join_different_sources() -> Result<()> {
        let left = LogicalPlanBuilder::from(test_table_scan_with_name("t1")?)
            .aggregate(Vec::<Expr>::new(), vec![sum(col("b")).alias("sum_b")])?
            .build()?;
        let right = LogicalPlanBuilder::from(test_table_scan_with_name("t2")?)
            .aggregate(Vec::<Expr>::new(), vec![avg(col("c")).alias("avg_c")])?
            .build()?;

        let plan = LogicalPlanBuilder::from(left).cross_join(right)?.build()?;

        assert_optimized_plan_equal!(plan, @r"
        Cross Join:
          Aggregate: groupBy=[[]], aggr=[[sum(t1.b) AS sum_b]]
            TableScan: t1
          Aggregate: groupBy=[[]], aggr=[[avg(t2.c) AS avg_c]]
            TableScan: t2
        ")?;
        Ok(())
    }

    #[test]
    fn rewrite_projection_scalar_aggregate_subqueries() -> Result<()> {
        let count_subquery = LogicalPlanBuilder::from(test_table_scan_with_name("t")?)
            .filter(col("a").gt(lit(1)))?
            .aggregate(Vec::<Expr>::new(), vec![count(col("a"))])?
            .build()?;
        let avg_subquery = LogicalPlanBuilder::from(test_table_scan_with_name("t")?)
            .filter(col("a").lt(lit(10)))?
            .aggregate(Vec::<Expr>::new(), vec![avg(col("c"))])?
            .build()?;

        let plan = LogicalPlanBuilder::from(test_table_scan_with_name("r")?)
            .project(vec![
                scalar_subquery(Arc::new(count_subquery)).alias("count_a"),
                scalar_subquery(Arc::new(avg_subquery)).alias("avg_c"),
            ])?
            .build()?;

        assert_optimized_plan_equal!(plan, @r"
        Projection: __datafusion_query_fusion_scalar_agg_0 AS count_a, __datafusion_query_fusion_scalar_agg_1 AS avg_c
          Cross Join:
            TableScan: r
            Aggregate: groupBy=[[]], aggr=[[COUNT(t.a) FILTER (WHERE t.a > Int32(1)) AS __datafusion_query_fusion_scalar_agg_0, avg(t.c) FILTER (WHERE t.a < Int32(10)) AS __datafusion_query_fusion_scalar_agg_1]]
              Filter: t.a > Int32(1) OR t.a < Int32(10)
                TableScan: t
        ")?;
        Ok(())
    }

    #[test]
    fn rewrite_group_by_join_to_window() -> Result<()> {
        let left = LogicalPlanBuilder::from(test_table_scan_with_name("t")?)
            .alias("l")?
            .build()?;
        let aggregate = LogicalPlanBuilder::from(test_table_scan_with_name("t")?)
            .alias("r")?
            .aggregate(vec![col("r.a")], vec![sum(col("r.b")).alias("sum_b")])?
            .build()?;

        let plan = LogicalPlanBuilder::from(left)
            .join(aggregate, JoinType::Inner, (vec!["l.a"], vec!["r.a"]), None)?
            .build()?;

        assert_optimized_plan_equal!(plan, @r"
        Projection: l.a, l.b, l.c, l.a, __datafusion_query_fusion_window_agg_0
          WindowAggr: windowExpr=[[sum(l.b) PARTITION BY [l.a] ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING AS __datafusion_query_fusion_window_agg_0]]
            Filter: l.a IS NOT NULL
              SubqueryAlias: l
                TableScan: t
        ")?;
        Ok(())
    }
}
