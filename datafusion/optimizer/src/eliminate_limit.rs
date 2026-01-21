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

//! [`EliminateLimit`] eliminates `LIMIT` when possible
use crate::optimizer::ApplyOrder;
use crate::{OptimizerConfig, OptimizerRule};
use datafusion_common::Result;
use datafusion_common::tree_node::Transformed;
use datafusion_expr::logical_plan::{EmptyRelation, FetchType, LogicalPlan, SkipType};
use std::sync::Arc;

/// Optimizer rule to replace `LIMIT 0` or `LIMIT` whose ancestor LIMIT's skip is
/// greater than or equal to current's fetch
///
/// It can cooperate with `propagate_empty_relation` and `limit_push_down`. on a
/// plan with an empty relation.
///
/// This rule also removes OFFSET 0 from the [LogicalPlan]
#[derive(Default, Debug)]
pub struct EliminateLimit;

impl EliminateLimit {
    #[expect(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
}

impl OptimizerRule for EliminateLimit {
    fn name(&self) -> &str {
        "eliminate_limit"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::BottomUp)
    }

    fn supports_rewrite(&self) -> bool {
        true
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>, datafusion_common::DataFusionError> {
        match plan {
            LogicalPlan::Limit(limit) => {
                // Only supports rewriting for literal fetch
                let FetchType::Literal(fetch) = limit.get_fetch_type()? else {
                    return Ok(Transformed::no(LogicalPlan::Limit(limit)));
                };

                if let Some(v) = fetch {
                    if v == 0 {
                        return Ok(Transformed::yes(LogicalPlan::EmptyRelation(
                            EmptyRelation {
                                produce_one_row: false,
                                schema: Arc::clone(limit.input.schema()),
                            },
                        )));
                    }
                } else if matches!(limit.get_skip_type()?, SkipType::Literal(0)) {
                    // If fetch is `None` and skip is 0, then Limit takes no effect and
                    // we can remove it. Its input also can be Limit, so we should apply again.
                    #[expect(clippy::used_underscore_binding)]
                    return self.rewrite(Arc::unwrap_or_clone(limit.input), _config);
                }
                Ok(Transformed::no(LogicalPlan::Limit(limit)))
            }
            _ => Ok(Transformed::no(plan)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::OptimizerContext;
    use crate::test::*;
    use datafusion_common::Column;
    use datafusion_expr::{
        col,
        logical_plan::{JoinType, builder::LogicalPlanBuilder},
    };
    use std::sync::Arc;

    use crate::assert_optimized_plan_eq_snapshot;
    use crate::push_down_limit::PushDownLimit;
    use datafusion_expr::test::function_stub::sum;

    macro_rules! assert_optimized_plan_equal {
        (
            $plan:expr,
            @ $expected:literal $(,)?
        ) => {{
            let rules: Vec<Arc<dyn crate::OptimizerRule + Send + Sync>> = vec![Arc::new(EliminateLimit::new())];
            let optimizer_ctx = OptimizerContext::new();
            assert_optimized_plan_eq_snapshot!(
                optimizer_ctx,
                rules,
                $plan,
                @ $expected,
            )
        }};
    }

    macro_rules! assert_optimized_plan_eq_with_pushdown {
        (
            $plan:expr,
            @ $expected:literal $(,)?
        ) => {{
            let optimizer_ctx = OptimizerContext::new().with_max_passes(1);
            let rules: Vec<Arc<dyn crate::OptimizerRule + Send + Sync>> = vec![
                Arc::new(PushDownLimit::new()),
                Arc::new(EliminateLimit::new())
                ];
            assert_optimized_plan_eq_snapshot!(
                optimizer_ctx,
                rules,
                $plan,
                @ $expected,
            )
        }};
    }

    #[test]
    fn limit_0_root() -> Result<()> {
        let table_scan = test_table_scan().unwrap();
        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(vec![col("a")], vec![sum(col("b"))])?
            .limit(0, Some(0))?
            .build()?;
        // No aggregate / scan / limit
        assert_optimized_plan_equal!(
            plan,
            @ "EmptyRelation: rows=0"
        )
    }

    #[test]
    fn limit_0_nested() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan1 = LogicalPlanBuilder::from(table_scan.clone())
            .aggregate(vec![col("a")], vec![sum(col("b"))])?
            .build()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(vec![col("a")], vec![sum(col("b"))])?
            .limit(0, Some(0))?
            .union(plan1)?
            .build()?;

        // Left side is removed
        assert_optimized_plan_equal!(
            plan,
            @ r"
        Union
          EmptyRelation: rows=0
          Aggregate: groupBy=[[test.a]], aggr=[[sum(test.b)]]
            TableScan: test
        "
        )
    }

    #[test]
    fn limit_fetch_with_ancestor_limit_skip() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(vec![col("a")], vec![sum(col("b"))])?
            .limit(0, Some(2))?
            .limit(2, None)?
            .build()?;

        // No aggregate / scan / limit
        assert_optimized_plan_eq_with_pushdown!(
            plan,
            @ "EmptyRelation: rows=0"
        )
    }

    #[test]
    fn multi_limit_offset_sort_eliminate() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(vec![col("a")], vec![sum(col("b"))])?
            .limit(0, Some(2))?
            .sort_by(vec![col("a")])?
            .limit(2, Some(1))?
            .build()?;

        // After remove global-state, we don't record the parent <skip, fetch>
        // So, bottom don't know parent info, so can't eliminate.
        assert_optimized_plan_eq_with_pushdown!(
            plan,
            @ r"
        Limit: skip=2, fetch=1
          Sort: test.a ASC NULLS LAST, fetch=3
            Limit: skip=0, fetch=2
              Aggregate: groupBy=[[test.a]], aggr=[[sum(test.b)]]
                TableScan: test
        "
        )
    }

    #[test]
    fn limit_fetch_with_ancestor_limit_fetch() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(vec![col("a")], vec![sum(col("b"))])?
            .limit(0, Some(2))?
            .sort_by(vec![col("a")])?
            .limit(0, Some(1))?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @ r"
        Limit: skip=0, fetch=1
          Sort: test.a ASC NULLS LAST
            Limit: skip=0, fetch=2
              Aggregate: groupBy=[[test.a]], aggr=[[sum(test.b)]]
                TableScan: test
        "
        )
    }

    #[test]
    fn limit_with_ancestor_limit() -> Result<()> {
        let table_scan = test_table_scan().unwrap();
        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(vec![col("a")], vec![sum(col("b"))])?
            .limit(2, Some(1))?
            .sort_by(vec![col("a")])?
            .limit(3, Some(1))?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @ r"
        Limit: skip=3, fetch=1
          Sort: test.a ASC NULLS LAST
            Limit: skip=2, fetch=1
              Aggregate: groupBy=[[test.a]], aggr=[[sum(test.b)]]
                TableScan: test
        "
        )
    }

    #[test]
    fn limit_join_with_ancestor_limit() -> Result<()> {
        let table_scan = test_table_scan()?;
        let table_scan_inner = test_table_scan_with_name("test1")?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .limit(2, Some(1))?
            .join_using(
                table_scan_inner,
                JoinType::Inner,
                vec![Column::from_name("a".to_string())],
            )?
            .limit(3, Some(1))?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @ r"
        Limit: skip=3, fetch=1
          Inner Join: Using test.a = test1.a
            Limit: skip=2, fetch=1
              TableScan: test
            TableScan: test1
        "
        )
    }

    #[test]
    fn remove_zero_offset() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(vec![col("a")], vec![sum(col("b"))])?
            .limit(0, None)?
            .build()?;

        assert_optimized_plan_equal!(
            plan,
            @ r"
        Aggregate: groupBy=[[test.a]], aggr=[[sum(test.b)]]
          TableScan: test
        "
        )
    }
}
