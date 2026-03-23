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

//! Rewrites a single [`Aggregate`] that contains multiple `COUNT(DISTINCT ...)`
//! into a join of smaller aggregates so each distinct is computed with one
//! accumulator set, reducing peak memory for high-cardinality distincts.

use std::sync::Arc;

use crate::optimizer::ApplyOrder;
use crate::{OptimizerConfig, OptimizerRule};

use datafusion_common::{
    Column, JoinConstraint, NullEquality, Result, internal_err, tree_node::Transformed,
};
use datafusion_expr::builder::project;
use datafusion_expr::expr::{AggregateFunction, AggregateFunctionParams, ScalarFunction};
use datafusion_expr::logical_plan::{
    Aggregate, Join, JoinType, LogicalPlan, SubqueryAlias,
};
use datafusion_expr::{Expr, col, lit, logical_plan::LogicalPlanBuilder};

const MAX_DISTINCT_REWRITE_BRANCHES: usize = 8;

/// Optimizer rule: multiple `COUNT(DISTINCT ...)` → join of per-distinct sub-aggregates.
#[derive(Default, Debug)]
pub struct MultiDistinctCountRewrite {}

impl MultiDistinctCountRewrite {
    /// Create a new rule instance.
    pub fn new() -> Self {
        Self {}
    }

    fn is_simple_count_distinct(
        e: &Expr,
    ) -> Option<(Arc<datafusion_expr::AggregateUDF>, Expr)> {
        if let Expr::AggregateFunction(AggregateFunction { func, params }) = e {
            let AggregateFunctionParams {
                distinct,
                args,
                filter,
                order_by,
                ..
            } = &params;
            if func.name().eq_ignore_ascii_case("count")
                && *distinct
                && args.len() == 1
                && filter.is_none()
                && order_by.is_empty()
            {
                let arg = args.first().cloned()?;
                if Self::is_safe_distinct_arg(&arg) {
                    return Some((Arc::clone(func), arg));
                }
            }
        }
        None
    }

    fn is_safe_distinct_arg(e: &Expr) -> bool {
        if e.is_volatile() {
            return false;
        }
        match e {
            Expr::Column(_) => true,
            Expr::ScalarFunction(ScalarFunction { func, args }) => {
                matches!(func.name().to_ascii_lowercase().as_str(), "lower" | "upper")
                    && args.len() == 1
                    && matches!(args.first(), Some(Expr::Column(_)))
            }
            Expr::Cast(cast) => matches!(cast.expr.as_ref(), Expr::Column(_)),
            _ => false,
        }
    }

    fn is_simple_group_expr(e: &Expr) -> bool {
        matches!(e, Expr::Column(_))
    }

    fn contains_grouping_set(group_expr: &[Expr]) -> bool {
        group_expr
            .first()
            .is_some_and(|e| matches!(e, Expr::GroupingSet(_)))
    }
}

impl OptimizerRule for MultiDistinctCountRewrite {
    fn name(&self) -> &str {
        "multi_distinct_count_rewrite"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::BottomUp)
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        if !config
            .options()
            .optimizer
            .enable_multi_distinct_count_rewrite
        {
            return Ok(Transformed::no(plan));
        }

        let LogicalPlan::Aggregate(Aggregate {
            input,
            aggr_expr,
            schema,
            group_expr,
            ..
        }) = plan
        else {
            return Ok(Transformed::no(plan));
        };

        if Self::contains_grouping_set(&group_expr) {
            return Ok(Transformed::no(LogicalPlan::Aggregate(Aggregate::try_new(
                input, group_expr, aggr_expr,
            )?)));
        }

        if !group_expr.iter().all(Self::is_simple_group_expr) {
            return Ok(Transformed::no(LogicalPlan::Aggregate(Aggregate::try_new(
                input, group_expr, aggr_expr,
            )?)));
        }

        let group_size = group_expr.len();
        let mut distinct_list: Vec<(Expr, usize, Arc<datafusion_expr::AggregateUDF>)> =
            vec![];
        let mut other_list: Vec<(Expr, usize)> = vec![];

        for (i, e) in aggr_expr.iter().enumerate() {
            if let Some((func, arg)) = Self::is_simple_count_distinct(e) {
                distinct_list.push((arg, group_size + i, func));
            } else {
                other_list.push((e.clone(), group_size + i));
            }
        }

        if distinct_list.len() < 2 {
            return Ok(Transformed::no(LogicalPlan::Aggregate(Aggregate::try_new(
                input, group_expr, aggr_expr,
            )?)));
        }

        if distinct_list.len() > MAX_DISTINCT_REWRITE_BRANCHES {
            return Ok(Transformed::no(LogicalPlan::Aggregate(Aggregate::try_new(
                input, group_expr, aggr_expr,
            )?)));
        }

        {
            use std::collections::HashSet;
            let mut seen: HashSet<&Expr> = HashSet::new();
            for (arg, _, _) in distinct_list.iter() {
                if !seen.insert(arg) {
                    return Ok(Transformed::no(LogicalPlan::Aggregate(
                        Aggregate::try_new(input, group_expr, aggr_expr)?,
                    )));
                }
            }
        }

        let count_udaf = Arc::clone(&distinct_list[0].2);

        let count_star = Expr::AggregateFunction(AggregateFunction::new_udf(
            Arc::clone(&count_udaf),
            vec![lit(1i64)],
            false,
            None,
            vec![],
            None,
        ))
        .alias("_cnt");

        let base_aggr_exprs: Vec<Expr> = other_list
            .iter()
            .map(|(e, schema_idx)| {
                let (q, f) = schema.qualified_field(*schema_idx);
                e.clone().alias_qualified(q.cloned(), f.name())
            })
            .collect();

        // `Aggregate` must have at least one of grouping exprs or aggregate exprs.
        // Global multi-`COUNT(DISTINCT)` (no GROUP BY, no other aggs) has neither — skip a base node.
        let base_plan_opt: Option<Arc<LogicalPlan>> =
            if group_expr.is_empty() && other_list.is_empty() {
                None
            } else {
                let base_plan = LogicalPlan::Aggregate(Aggregate::try_new(
                    Arc::clone(&input),
                    group_expr.clone(),
                    base_aggr_exprs,
                )?);
                let base_alias = config.alias_generator().next("mdc_base");
                Some(Arc::new(LogicalPlan::SubqueryAlias(
                    SubqueryAlias::try_new(Arc::new(base_plan), &base_alias)?,
                )))
            };

        let mut current = base_plan_opt;

        for (distinct_arg, schema_aggr_idx, _) in distinct_list.iter() {
            // COUNT(DISTINCT x) ignores NULLs; filter before grouping by x.
            let filtered_input = LogicalPlanBuilder::from(input.as_ref().clone())
                .filter(distinct_arg.clone().is_not_null())?
                .build()?;

            let inner_group: Vec<Expr> = group_expr
                .iter()
                .cloned()
                .chain(std::iter::once(distinct_arg.clone()))
                .collect();
            let inner_agg = LogicalPlan::Aggregate(Aggregate::try_new(
                Arc::new(filtered_input),
                inner_group,
                vec![count_star.clone()],
            )?);

            let (_, field) = schema.qualified_field(*schema_aggr_idx);
            let outer_name = field.name().clone();
            let outer_aggr = Expr::AggregateFunction(AggregateFunction::new_udf(
                Arc::clone(&count_udaf),
                vec![col("_cnt")],
                false,
                None,
                vec![],
                None,
            ))
            .alias(outer_name.clone());

            let branch_plan = LogicalPlan::Aggregate(Aggregate::try_new(
                Arc::new(inner_agg),
                group_expr.clone(),
                vec![outer_aggr],
            )?);

            let alias_name = config.alias_generator().next("mdc_d");
            let branch_aliased = LogicalPlan::SubqueryAlias(SubqueryAlias::try_new(
                Arc::new(branch_plan),
                &alias_name,
            )?);

            current = match current {
                None => Some(Arc::new(branch_aliased)),
                Some(prev) => {
                    let left_schema = prev.schema();
                    let right_schema = branch_aliased.schema();
                    let join_keys: Vec<(Expr, Expr)> = (0..group_size)
                        .map(|i| {
                            let (lq, lf) = left_schema.qualified_field(i);
                            let (rq, rf) = right_schema.qualified_field(i);
                            (
                                Expr::Column(Column::new(lq.cloned(), lf.name())),
                                Expr::Column(Column::new(rq.cloned(), rf.name())),
                            )
                        })
                        .collect();

                    let join = Join::try_new(
                        prev,
                        Arc::new(branch_aliased),
                        join_keys,
                        None,
                        JoinType::Inner,
                        JoinConstraint::On,
                        NullEquality::NullEqualsNothing,
                        false,
                    )?;
                    Some(Arc::new(LogicalPlan::Join(join)))
                }
            };
        }

        let current =
            current.expect("distinct_list non-empty implies at least one branch");
        let join_schema = current.schema();

        let base_field_count = group_size + other_list.len();

        let mut proj_exprs: Vec<Expr> = vec![];
        for i in 0..group_size {
            let (q, f) = schema.qualified_field(i);
            let orig_name = f.name();
            let (join_q, join_f) = join_schema.qualified_field(i);
            let c = Expr::Column(Column::new(join_q.cloned(), join_f.name()));
            proj_exprs.push(c.alias_qualified(q.cloned(), orig_name));
        }
        // Preserve original aggregate column order (distinct and non-distinct may be interleaved).
        for aggr_i in 0..aggr_expr.len() {
            let schema_idx = group_size + aggr_i;
            let (q, f) = schema.qualified_field(schema_idx);
            let orig_name = f.name();

            if let Some((dist_idx, (_, _, _))) = distinct_list
                .iter()
                .enumerate()
                .find(|(_, (_, idx, _))| *idx == schema_idx)
            {
                let branch_start_idx = base_field_count + dist_idx * (group_size + 1);
                let branch_aggr_idx = branch_start_idx + group_size;
                let (join_q, join_f) = join_schema.qualified_field(branch_aggr_idx);
                let c = Expr::Column(Column::new(join_q.cloned(), join_f.name()));
                proj_exprs.push(c.alias_qualified(q.cloned(), orig_name));
            } else if let Some((other_idx, _)) = other_list
                .iter()
                .enumerate()
                .find(|(_, (_, idx))| *idx == schema_idx)
            {
                let join_idx = group_size + other_idx;
                let (join_q, join_f) = join_schema.qualified_field(join_idx);
                let c = Expr::Column(Column::new(join_q.cloned(), join_f.name()));
                proj_exprs.push(c.alias_qualified(q.cloned(), orig_name));
            } else {
                return internal_err!(
                    "aggregate index {aggr_i} (schema index {schema_idx}) is neither distinct nor other"
                );
            }
        }

        let out = project((*current).clone(), proj_exprs)?;
        Ok(Transformed::yes(out))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Optimizer;
    use crate::OptimizerContext;
    use crate::OptimizerRule;
    use crate::test::*;
    use arrow::datatypes::DataType;
    use datafusion_expr::GroupingSet;
    use datafusion_expr::LogicalPlan;
    use datafusion_expr::expr_fn::cast;
    use datafusion_expr::logical_plan::Aggregate;
    use datafusion_expr::logical_plan::builder::LogicalPlanBuilder;
    use datafusion_expr::{Expr, col};
    use datafusion_functions_aggregate::expr_fn::{count, count_distinct};

    fn optimize_with_rule_config(
        plan: LogicalPlan,
        rule: Arc<dyn OptimizerRule + Send + Sync>,
        enable_multi_distinct_count_rewrite: bool,
    ) -> Result<LogicalPlan> {
        Optimizer::with_rules(vec![rule]).optimize(
            plan,
            &OptimizerContext::new().with_enable_multi_distinct_count_rewrite(
                enable_multi_distinct_count_rewrite,
            ),
            |_, _| {},
        )
    }

    fn optimize_with_rule(
        plan: LogicalPlan,
        rule: Arc<dyn OptimizerRule + Send + Sync>,
    ) -> Result<LogicalPlan> {
        optimize_with_rule_config(plan, rule, true)
    }

    #[test]
    fn rewrites_two_count_distinct() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(
                vec![col("a")],
                vec![count_distinct(col("b")), count_distinct(col("c"))],
            )?
            .build()?;

        let optimized =
            optimize_with_rule(plan, Arc::new(MultiDistinctCountRewrite::new()))?;
        let s = optimized.display_indent_schema().to_string();
        assert!(s.contains("Inner Join"), "expected join rewrite, got:\n{s}");
        assert!(
            s.contains("Filter: test.b IS NOT NULL"),
            "expected null filter on b, got:\n{s}"
        );
        assert!(
            s.contains("Filter: test.c IS NOT NULL"),
            "expected null filter on c, got:\n{s}"
        );
        assert!(
            s.contains("SubqueryAlias: mdc_base"),
            "expected base alias, got:\n{s}"
        );
        assert!(
            s.matches("SubqueryAlias: mdc_d").count() >= 2,
            "expected distinct branches, got:\n{s}"
        );
        Ok(())
    }

    #[test]
    fn rewrites_global_three_count_distinct() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(
                Vec::<Expr>::new(),
                vec![
                    count_distinct(col("a")),
                    count_distinct(col("b")),
                    count_distinct(col("c")),
                ],
            )?
            .build()?;

        let optimized =
            optimize_with_rule(plan, Arc::new(MultiDistinctCountRewrite::new()))?;
        let s = optimized.display_indent_schema().to_string();
        assert!(
            s.contains("Cross Join") || s.contains("Inner Join"),
            "expected join rewrite for global multi-distinct, got:\n{s}"
        );
        assert!(
            !s.contains("mdc_base"),
            "global-only rewrite should not use mdc_base, got:\n{s}"
        );
        Ok(())
    }

    /// Grouped query with multiple `COUNT(DISTINCT …)` **and** non-distinct aggregates (typical BI).
    /// Non-distinct aggs live in `mdc_base`; each distinct column gets a branch + join on keys.
    #[test]
    fn rewrites_two_count_distinct_with_non_distinct_count() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(
                vec![col("a")],
                vec![
                    count_distinct(col("b")),
                    count_distinct(col("c")),
                    count(col("a")),
                ],
            )?
            .build()?;

        let optimized =
            optimize_with_rule(plan, Arc::new(MultiDistinctCountRewrite::new()))?;
        let s = optimized.display_indent_schema().to_string();
        assert!(s.contains("Inner Join"), "expected join rewrite, got:\n{s}");
        assert!(
            s.contains("SubqueryAlias: mdc_base"),
            "expected base aggregate for non-distinct aggs, got:\n{s}"
        );
        Ok(())
    }

    #[test]
    fn does_not_rewrite_two_count_distinct_same_column() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(
                vec![col("a")],
                vec![
                    count_distinct(col("b")).alias("cd1"),
                    count_distinct(col("b")).alias("cd2"),
                ],
            )?
            .build()?;
        let before = plan.display_indent_schema().to_string();
        let optimized =
            optimize_with_rule(plan, Arc::new(MultiDistinctCountRewrite::new()))?;
        let after = optimized.display_indent_schema().to_string();
        assert_eq!(before, after);
        Ok(())
    }

    #[test]
    fn does_not_rewrite_single_count_distinct() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(vec![col("a")], vec![count_distinct(col("b"))])?
            .build()?;
        let before = plan.display_indent_schema().to_string();
        let optimized =
            optimize_with_rule(plan, Arc::new(MultiDistinctCountRewrite::new()))?;
        let after = optimized.display_indent_schema().to_string();
        assert_eq!(before, after);
        Ok(())
    }

    #[test]
    fn rewrites_three_count_distinct_grouped() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(
                vec![col("a")],
                vec![
                    count_distinct(col("b")),
                    count_distinct(col("c")),
                    count_distinct(col("a")),
                ],
            )?
            .build()?;

        let optimized =
            optimize_with_rule(plan, Arc::new(MultiDistinctCountRewrite::new()))?;
        let s = optimized.display_indent_schema().to_string();
        assert!(
            s.matches("Inner Join").count() >= 2,
            "expected two joins for three branches, got:\n{s}"
        );
        assert!(
            s.contains("SubqueryAlias: mdc_base"),
            "expected base aggregate, got:\n{s}"
        );
        Ok(())
    }

    #[test]
    fn rewrites_interleaved_non_distinct_between_distincts() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(
                vec![col("a")],
                vec![
                    count_distinct(col("b")),
                    count(col("a")),
                    count_distinct(col("c")),
                ],
            )?
            .build()?;

        let optimized =
            optimize_with_rule(plan, Arc::new(MultiDistinctCountRewrite::new()))?;
        let s = optimized.display_indent_schema().to_string();
        assert!(s.contains("Inner Join"), "expected join rewrite, got:\n{s}");
        assert!(
            s.contains("SubqueryAlias: mdc_base"),
            "expected base for middle count(a), got:\n{s}"
        );
        Ok(())
    }

    #[test]
    fn rewrites_count_distinct_on_cast_exprs() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(
                vec![col("a")],
                vec![
                    count_distinct(cast(col("b"), DataType::Int64)),
                    count_distinct(cast(col("c"), DataType::Int64)),
                ],
            )?
            .build()?;

        let optimized =
            optimize_with_rule(plan, Arc::new(MultiDistinctCountRewrite::new()))?;
        let s = optimized.display_indent_schema().to_string();
        assert!(s.contains("Inner Join"), "expected join rewrite, got:\n{s}");
        assert!(
            s.contains("Filter: CAST(test.b AS Int64) IS NOT NULL"),
            "expected null filter on cast(b), got:\n{s}"
        );
        assert!(
            s.contains("Filter: CAST(test.c AS Int64) IS NOT NULL"),
            "expected null filter on cast(c), got:\n{s}"
        );
        Ok(())
    }

    #[test]
    fn does_not_rewrite_grouping_sets_multi_distinct() -> Result<()> {
        let table_scan = test_table_scan()?;
        let group_expr = vec![Expr::GroupingSet(GroupingSet::GroupingSets(vec![vec![
            col("a"),
        ]]))];
        let aggr_expr = vec![count_distinct(col("b")), count_distinct(col("c"))];
        let plan = LogicalPlan::Aggregate(Aggregate::try_new(
            Arc::new(table_scan),
            group_expr,
            aggr_expr,
        )?);
        let before = plan.display_indent_schema().to_string();
        let optimized =
            optimize_with_rule(plan, Arc::new(MultiDistinctCountRewrite::new()))?;
        let after = optimized.display_indent_schema().to_string();
        assert_eq!(before, after);
        Ok(())
    }

    #[test]
    fn skips_rewrite_when_config_disabled() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(
                vec![col("a")],
                vec![count_distinct(col("b")), count_distinct(col("c"))],
            )?
            .build()?;
        let before = plan.display_indent_schema().to_string();
        let optimized = optimize_with_rule_config(
            plan,
            Arc::new(MultiDistinctCountRewrite::new()),
            false,
        )?;
        assert_eq!(before, optimized.display_indent_schema().to_string());
        Ok(())
    }

    #[test]
    fn does_not_rewrite_mixed_agg() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(
                vec![col("a")],
                vec![count_distinct(col("b")), count(col("c"))],
            )?
            .build()?;
        let before = plan.display_indent_schema().to_string();
        let optimized =
            optimize_with_rule(plan, Arc::new(MultiDistinctCountRewrite::new()))?;
        let after = optimized.display_indent_schema().to_string();
        assert_eq!(before, after);
        Ok(())
    }
}
