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

//! [`PushDownLimit`] pushes `LIMIT` earlier in the query plan

use std::cmp::min;
use std::sync::Arc;

use crate::optimizer::ApplyOrder;
use crate::{OptimizerConfig, OptimizerRule};

use datafusion_common::tree_node::Transformed;
use datafusion_common::utils::combine_limit;
use datafusion_common::Result;
use datafusion_expr::logical_plan::{Join, JoinType, Limit, LogicalPlan};

/// Optimization rule that tries to push down `LIMIT`.
///
//. It will push down through projection, limits (taking the smaller limit)
#[derive(Default, Debug)]
pub struct PushDownLimit {}

impl PushDownLimit {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
}

/// Push down Limit.
impl OptimizerRule for PushDownLimit {
    fn supports_rewrite(&self) -> bool {
        true
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        _config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>> {
        let LogicalPlan::Limit(mut limit) = plan else {
            return Ok(Transformed::no(plan));
        };

        let Limit { skip, fetch, input } = limit;

        // Merge the Parent Limit and the Child Limit.
        if let LogicalPlan::Limit(child) = input.as_ref() {
            let (skip, fetch) =
                combine_limit(limit.skip, limit.fetch, child.skip, child.fetch);

            let plan = LogicalPlan::Limit(Limit {
                skip,
                fetch,
                input: Arc::clone(&child.input),
            });

            // recursively reapply the rule on the new plan
            return self.rewrite(plan, _config);
        }

        // no fetch to push, so return the original plan
        let Some(fetch) = fetch else {
            return Ok(Transformed::no(LogicalPlan::Limit(Limit {
                skip,
                fetch,
                input,
            })));
        };

        match Arc::unwrap_or_clone(input) {
            LogicalPlan::TableScan(mut scan) => {
                let rows_needed = if fetch != 0 { fetch + skip } else { 0 };
                let new_fetch = scan
                    .fetch
                    .map(|x| min(x, rows_needed))
                    .or(Some(rows_needed));
                if new_fetch == scan.fetch {
                    original_limit(skip, fetch, LogicalPlan::TableScan(scan))
                } else {
                    // push limit into the table scan itself
                    scan.fetch = scan
                        .fetch
                        .map(|x| min(x, rows_needed))
                        .or(Some(rows_needed));
                    transformed_limit(skip, fetch, LogicalPlan::TableScan(scan))
                }
            }
            LogicalPlan::Union(mut union) => {
                // push limits to each input of the union
                union.inputs = union
                    .inputs
                    .into_iter()
                    .map(|input| make_arc_limit(0, fetch + skip, input))
                    .collect();
                transformed_limit(skip, fetch, LogicalPlan::Union(union))
            }

            LogicalPlan::CrossJoin(mut cross_join) => {
                // push limit to both inputs
                cross_join.left = make_arc_limit(0, fetch + skip, cross_join.left);
                cross_join.right = make_arc_limit(0, fetch + skip, cross_join.right);
                transformed_limit(skip, fetch, LogicalPlan::CrossJoin(cross_join))
            }

            LogicalPlan::Join(join) => Ok(push_down_join(join, fetch + skip)
                .update_data(|join| {
                    make_limit(skip, fetch, Arc::new(LogicalPlan::Join(join)))
                })),

            LogicalPlan::Sort(mut sort) => {
                let new_fetch = {
                    let sort_fetch = skip + fetch;
                    Some(sort.fetch.map(|f| f.min(sort_fetch)).unwrap_or(sort_fetch))
                };
                if new_fetch == sort.fetch {
                    if skip > 0 {
                        original_limit(skip, fetch, LogicalPlan::Sort(sort))
                    } else {
                        Ok(Transformed::yes(LogicalPlan::Sort(sort)))
                    }
                } else {
                    sort.fetch = new_fetch;
                    limit.input = Arc::new(LogicalPlan::Sort(sort));
                    Ok(Transformed::yes(LogicalPlan::Limit(limit)))
                }
            }
            LogicalPlan::Projection(mut proj) => {
                // commute
                limit.input = Arc::clone(&proj.input);
                let new_limit = LogicalPlan::Limit(limit);
                proj.input = Arc::new(new_limit);
                Ok(Transformed::yes(LogicalPlan::Projection(proj)))
            }
            LogicalPlan::SubqueryAlias(mut subquery_alias) => {
                // commute
                limit.input = Arc::clone(&subquery_alias.input);
                let new_limit = LogicalPlan::Limit(limit);
                subquery_alias.input = Arc::new(new_limit);
                Ok(Transformed::yes(LogicalPlan::SubqueryAlias(subquery_alias)))
            }
            LogicalPlan::Extension(extension_plan)
                if extension_plan.node.supports_limit_pushdown() =>
            {
                let new_children = extension_plan
                    .node
                    .inputs()
                    .into_iter()
                    .map(|child| {
                        LogicalPlan::Limit(Limit {
                            skip: 0,
                            fetch: Some(fetch + skip),
                            input: Arc::new(child.clone()),
                        })
                    })
                    .collect::<Vec<_>>();

                // Create a new extension node with updated inputs
                let child_plan = LogicalPlan::Extension(extension_plan);
                let new_extension =
                    child_plan.with_new_exprs(child_plan.expressions(), new_children)?;

                transformed_limit(skip, fetch, new_extension)
            }
            input => original_limit(skip, fetch, input),
        }
    }

    fn name(&self) -> &str {
        "push_down_limit"
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        Some(ApplyOrder::TopDown)
    }
}

/// Wrap the input plan with a limit node
///
/// Original:
/// ```text
/// input
/// ```
///
/// Return
/// ```text
/// Limit: skip=skip, fetch=fetch
///  input
/// ```
fn make_limit(skip: usize, fetch: usize, input: Arc<LogicalPlan>) -> LogicalPlan {
    LogicalPlan::Limit(Limit {
        skip,
        fetch: Some(fetch),
        input,
    })
}

/// Wrap the input plan with a limit node
fn make_arc_limit(
    skip: usize,
    fetch: usize,
    input: Arc<LogicalPlan>,
) -> Arc<LogicalPlan> {
    Arc::new(make_limit(skip, fetch, input))
}

/// Returns the original limit (non transformed)
fn original_limit(
    skip: usize,
    fetch: usize,
    input: LogicalPlan,
) -> Result<Transformed<LogicalPlan>> {
    Ok(Transformed::no(LogicalPlan::Limit(Limit {
        skip,
        fetch: Some(fetch),
        input: Arc::new(input),
    })))
}

/// Returns the a transformed limit
fn transformed_limit(
    skip: usize,
    fetch: usize,
    input: LogicalPlan,
) -> Result<Transformed<LogicalPlan>> {
    Ok(Transformed::yes(LogicalPlan::Limit(Limit {
        skip,
        fetch: Some(fetch),
        input: Arc::new(input),
    })))
}

/// Adds a limit to the inputs of a join, if possible
fn push_down_join(mut join: Join, limit: usize) -> Transformed<Join> {
    use JoinType::*;

    fn is_no_join_condition(join: &Join) -> bool {
        join.on.is_empty() && join.filter.is_none()
    }

    let (left_limit, right_limit) = if is_no_join_condition(&join) {
        match join.join_type {
            Left | Right | Full => (Some(limit), Some(limit)),
            LeftAnti | LeftSemi => (Some(limit), None),
            RightAnti | RightSemi => (None, Some(limit)),
            Inner => (None, None),
        }
    } else {
        match join.join_type {
            Left => (Some(limit), None),
            Right => (None, Some(limit)),
            _ => (None, None),
        }
    };

    if left_limit.is_none() && right_limit.is_none() {
        return Transformed::no(join);
    }
    if let Some(limit) = left_limit {
        join.left = make_arc_limit(0, limit, join.left);
    }
    if let Some(limit) = right_limit {
        join.right = make_arc_limit(0, limit, join.right);
    }
    Transformed::yes(join)
}

#[cfg(test)]
mod test {
    use std::cmp::Ordering;
    use std::fmt::{Debug, Formatter};
    use std::vec;

    use super::*;
    use crate::test::*;

    use datafusion_common::DFSchemaRef;
    use datafusion_expr::{
        col, exists, logical_plan::builder::LogicalPlanBuilder, Expr, Extension,
        UserDefinedLogicalNodeCore,
    };
    use datafusion_functions_aggregate::expr_fn::max;

    fn assert_optimized_plan_equal(plan: LogicalPlan, expected: &str) -> Result<()> {
        assert_optimized_plan_eq(Arc::new(PushDownLimit::new()), plan, expected)
    }

    #[derive(Debug, PartialEq, Eq, Hash)]
    pub struct NoopPlan {
        input: Vec<LogicalPlan>,
        schema: DFSchemaRef,
    }

    // Manual implementation needed because of `schema` field. Comparison excludes this field.
    impl PartialOrd for NoopPlan {
        fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
            self.input.partial_cmp(&other.input)
        }
    }

    impl UserDefinedLogicalNodeCore for NoopPlan {
        fn name(&self) -> &str {
            "NoopPlan"
        }

        fn inputs(&self) -> Vec<&LogicalPlan> {
            self.input.iter().collect()
        }

        fn schema(&self) -> &DFSchemaRef {
            &self.schema
        }

        fn expressions(&self) -> Vec<Expr> {
            self.input
                .iter()
                .flat_map(|child| child.expressions())
                .collect()
        }

        fn fmt_for_explain(&self, f: &mut Formatter) -> std::fmt::Result {
            write!(f, "NoopPlan")
        }

        fn with_exprs_and_inputs(
            &self,
            _exprs: Vec<Expr>,
            inputs: Vec<LogicalPlan>,
        ) -> Result<Self> {
            Ok(Self {
                input: inputs,
                schema: Arc::clone(&self.schema),
            })
        }

        fn supports_limit_pushdown(&self) -> bool {
            true // Allow limit push-down
        }
    }

    #[derive(Debug, PartialEq, Eq, Hash)]
    struct NoLimitNoopPlan {
        input: Vec<LogicalPlan>,
        schema: DFSchemaRef,
    }

    // Manual implementation needed because of `schema` field. Comparison excludes this field.
    impl PartialOrd for NoLimitNoopPlan {
        fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
            self.input.partial_cmp(&other.input)
        }
    }

    impl UserDefinedLogicalNodeCore for NoLimitNoopPlan {
        fn name(&self) -> &str {
            "NoLimitNoopPlan"
        }

        fn inputs(&self) -> Vec<&LogicalPlan> {
            self.input.iter().collect()
        }

        fn schema(&self) -> &DFSchemaRef {
            &self.schema
        }

        fn expressions(&self) -> Vec<Expr> {
            self.input
                .iter()
                .flat_map(|child| child.expressions())
                .collect()
        }

        fn fmt_for_explain(&self, f: &mut Formatter) -> std::fmt::Result {
            write!(f, "NoLimitNoopPlan")
        }

        fn with_exprs_and_inputs(
            &self,
            _exprs: Vec<Expr>,
            inputs: Vec<LogicalPlan>,
        ) -> Result<Self> {
            Ok(Self {
                input: inputs,
                schema: Arc::clone(&self.schema),
            })
        }

        fn supports_limit_pushdown(&self) -> bool {
            false // Disallow limit push-down by default
        }
    }
    #[test]
    fn limit_pushdown_basic() -> Result<()> {
        let table_scan = test_table_scan()?;
        let noop_plan = LogicalPlan::Extension(Extension {
            node: Arc::new(NoopPlan {
                input: vec![table_scan.clone()],
                schema: Arc::clone(table_scan.schema()),
            }),
        });

        let plan = LogicalPlanBuilder::from(noop_plan)
            .limit(0, Some(1000))?
            .build()?;

        let expected = "Limit: skip=0, fetch=1000\
        \n  NoopPlan\
        \n    Limit: skip=0, fetch=1000\
        \n      TableScan: test, fetch=1000";

        assert_optimized_plan_equal(plan, expected)
    }

    #[test]
    fn limit_pushdown_with_skip() -> Result<()> {
        let table_scan = test_table_scan()?;
        let noop_plan = LogicalPlan::Extension(Extension {
            node: Arc::new(NoopPlan {
                input: vec![table_scan.clone()],
                schema: Arc::clone(table_scan.schema()),
            }),
        });

        let plan = LogicalPlanBuilder::from(noop_plan)
            .limit(10, Some(1000))?
            .build()?;

        let expected = "Limit: skip=10, fetch=1000\
        \n  NoopPlan\
        \n    Limit: skip=0, fetch=1010\
        \n      TableScan: test, fetch=1010";

        assert_optimized_plan_equal(plan, expected)
    }

    #[test]
    fn limit_pushdown_multiple_limits() -> Result<()> {
        let table_scan = test_table_scan()?;
        let noop_plan = LogicalPlan::Extension(Extension {
            node: Arc::new(NoopPlan {
                input: vec![table_scan.clone()],
                schema: Arc::clone(table_scan.schema()),
            }),
        });

        let plan = LogicalPlanBuilder::from(noop_plan)
            .limit(10, Some(1000))?
            .limit(20, Some(500))?
            .build()?;

        let expected = "Limit: skip=30, fetch=500\
        \n  NoopPlan\
        \n    Limit: skip=0, fetch=530\
        \n      TableScan: test, fetch=530";

        assert_optimized_plan_equal(plan, expected)
    }

    #[test]
    fn limit_pushdown_multiple_inputs() -> Result<()> {
        let table_scan = test_table_scan()?;
        let noop_plan = LogicalPlan::Extension(Extension {
            node: Arc::new(NoopPlan {
                input: vec![table_scan.clone(), table_scan.clone()],
                schema: Arc::clone(table_scan.schema()),
            }),
        });

        let plan = LogicalPlanBuilder::from(noop_plan)
            .limit(0, Some(1000))?
            .build()?;

        let expected = "Limit: skip=0, fetch=1000\
        \n  NoopPlan\
        \n    Limit: skip=0, fetch=1000\
        \n      TableScan: test, fetch=1000\
        \n    Limit: skip=0, fetch=1000\
        \n      TableScan: test, fetch=1000";

        assert_optimized_plan_equal(plan, expected)
    }

    #[test]
    fn limit_pushdown_disallowed_noop_plan() -> Result<()> {
        let table_scan = test_table_scan()?;
        let no_limit_noop_plan = LogicalPlan::Extension(Extension {
            node: Arc::new(NoLimitNoopPlan {
                input: vec![table_scan.clone()],
                schema: Arc::clone(table_scan.schema()),
            }),
        });

        let plan = LogicalPlanBuilder::from(no_limit_noop_plan)
            .limit(0, Some(1000))?
            .build()?;

        let expected = "Limit: skip=0, fetch=1000\
        \n  NoLimitNoopPlan\
        \n    TableScan: test";

        assert_optimized_plan_equal(plan, expected)
    }

    #[test]
    fn limit_pushdown_projection_table_provider() -> Result<()> {
        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("a")])?
            .limit(0, Some(1000))?
            .build()?;

        // Should push the limit down to table provider
        // When it has a select
        let expected = "Projection: test.a\
        \n  Limit: skip=0, fetch=1000\
        \n    TableScan: test, fetch=1000";

        assert_optimized_plan_equal(plan, expected)
    }

    #[test]
    fn limit_push_down_take_smaller_limit() -> Result<()> {
        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .limit(0, Some(1000))?
            .limit(0, Some(10))?
            .build()?;

        // Should push down the smallest limit
        // Towards table scan
        // This rule doesn't replace multiple limits
        let expected = "Limit: skip=0, fetch=10\
        \n  TableScan: test, fetch=10";

        assert_optimized_plan_equal(plan, expected)
    }

    #[test]
    fn limit_doesnt_push_down_aggregation() -> Result<()> {
        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(vec![col("a")], vec![max(col("b"))])?
            .limit(0, Some(1000))?
            .build()?;

        // Limit should *not* push down aggregate node
        let expected = "Limit: skip=0, fetch=1000\
        \n  Aggregate: groupBy=[[test.a]], aggr=[[max(test.b)]]\
        \n    TableScan: test";

        assert_optimized_plan_equal(plan, expected)
    }

    #[test]
    fn limit_should_push_down_union() -> Result<()> {
        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(table_scan.clone())
            .union(LogicalPlanBuilder::from(table_scan).build()?)?
            .limit(0, Some(1000))?
            .build()?;

        // Limit should push down through union
        let expected = "Limit: skip=0, fetch=1000\
        \n  Union\
        \n    Limit: skip=0, fetch=1000\
        \n      TableScan: test, fetch=1000\
        \n    Limit: skip=0, fetch=1000\
        \n      TableScan: test, fetch=1000";

        assert_optimized_plan_equal(plan, expected)
    }

    #[test]
    fn limit_push_down_sort() -> Result<()> {
        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .sort_by(vec![col("a")])?
            .limit(0, Some(10))?
            .build()?;

        // Should push down limit to sort
        let expected = "Limit: skip=0, fetch=10\
        \n  Sort: test.a ASC NULLS LAST, fetch=10\
        \n    TableScan: test";

        assert_optimized_plan_equal(plan, expected)
    }

    #[test]
    fn limit_push_down_sort_skip() -> Result<()> {
        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .sort_by(vec![col("a")])?
            .limit(5, Some(10))?
            .build()?;

        // Should push down limit to sort
        let expected = "Limit: skip=5, fetch=10\
        \n  Sort: test.a ASC NULLS LAST, fetch=15\
        \n    TableScan: test";

        assert_optimized_plan_equal(plan, expected)
    }

    #[test]
    fn multi_stage_limit_recursive_to_deeper_limit() -> Result<()> {
        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .limit(0, Some(1000))?
            .aggregate(vec![col("a")], vec![max(col("b"))])?
            .limit(0, Some(10))?
            .build()?;

        // Limit should use deeper LIMIT 1000, but Limit 10 shouldn't push down aggregation
        let expected = "Limit: skip=0, fetch=10\
        \n  Aggregate: groupBy=[[test.a]], aggr=[[max(test.b)]]\
        \n    Limit: skip=0, fetch=1000\
        \n      TableScan: test, fetch=1000";

        assert_optimized_plan_equal(plan, expected)
    }

    #[test]
    fn limit_pushdown_should_not_pushdown_limit_with_offset_only() -> Result<()> {
        let table_scan = test_table_scan()?;
        let plan = LogicalPlanBuilder::from(table_scan)
            .limit(10, None)?
            .build()?;

        // Should not push any limit down to table provider
        // When it has a select
        let expected = "Limit: skip=10, fetch=None\
        \n  TableScan: test";

        assert_optimized_plan_equal(plan, expected)
    }

    #[test]
    fn limit_pushdown_with_offset_projection_table_provider() -> Result<()> {
        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("a")])?
            .limit(10, Some(1000))?
            .build()?;

        // Should push the limit down to table provider
        // When it has a select
        let expected = "Projection: test.a\
        \n  Limit: skip=10, fetch=1000\
        \n    TableScan: test, fetch=1010";

        assert_optimized_plan_equal(plan, expected)
    }

    #[test]
    fn limit_pushdown_with_offset_after_limit() -> Result<()> {
        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("a")])?
            .limit(0, Some(1000))?
            .limit(10, None)?
            .build()?;

        let expected = "Projection: test.a\
        \n  Limit: skip=10, fetch=990\
        \n    TableScan: test, fetch=1000";

        assert_optimized_plan_equal(plan, expected)
    }

    #[test]
    fn limit_pushdown_with_limit_after_offset() -> Result<()> {
        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .project(vec![col("a")])?
            .limit(10, None)?
            .limit(0, Some(1000))?
            .build()?;

        let expected = "Projection: test.a\
        \n  Limit: skip=10, fetch=1000\
        \n    TableScan: test, fetch=1010";

        assert_optimized_plan_equal(plan, expected)
    }

    #[test]
    fn limit_push_down_with_offset_take_smaller_limit() -> Result<()> {
        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .limit(10, None)?
            .limit(0, Some(1000))?
            .limit(0, Some(10))?
            .build()?;

        let expected = "Limit: skip=10, fetch=10\
        \n  TableScan: test, fetch=20";

        assert_optimized_plan_equal(plan, expected)
    }

    #[test]
    fn limit_doesnt_push_down_with_offset_aggregation() -> Result<()> {
        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(table_scan)
            .aggregate(vec![col("a")], vec![max(col("b"))])?
            .limit(10, Some(1000))?
            .build()?;

        // Limit should *not* push down aggregate node
        let expected = "Limit: skip=10, fetch=1000\
        \n  Aggregate: groupBy=[[test.a]], aggr=[[max(test.b)]]\
        \n    TableScan: test";

        assert_optimized_plan_equal(plan, expected)
    }

    #[test]
    fn limit_should_push_down_with_offset_union() -> Result<()> {
        let table_scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(table_scan.clone())
            .union(LogicalPlanBuilder::from(table_scan).build()?)?
            .limit(10, Some(1000))?
            .build()?;

        // Limit should push down through union
        let expected = "Limit: skip=10, fetch=1000\
        \n  Union\
        \n    Limit: skip=0, fetch=1010\
        \n      TableScan: test, fetch=1010\
        \n    Limit: skip=0, fetch=1010\
        \n      TableScan: test, fetch=1010";

        assert_optimized_plan_equal(plan, expected)
    }

    #[test]
    fn limit_offset_should_not_push_down_with_offset_join() -> Result<()> {
        let table_scan_1 = test_table_scan()?;
        let table_scan_2 = test_table_scan_with_name("test2")?;

        let plan = LogicalPlanBuilder::from(table_scan_1)
            .join(
                LogicalPlanBuilder::from(table_scan_2).build()?,
                JoinType::Inner,
                (vec!["a"], vec!["a"]),
                None,
            )?
            .limit(10, Some(1000))?
            .build()?;

        // Limit pushdown Not supported in Join
        let expected = "Limit: skip=10, fetch=1000\
        \n  Inner Join: test.a = test2.a\
        \n    TableScan: test\
        \n    TableScan: test2";

        assert_optimized_plan_equal(plan, expected)
    }

    #[test]
    fn offset_limit_should_not_push_down_with_offset_join() -> Result<()> {
        let table_scan_1 = test_table_scan()?;
        let table_scan_2 = test_table_scan_with_name("test2")?;

        let plan = LogicalPlanBuilder::from(table_scan_1)
            .join(
                LogicalPlanBuilder::from(table_scan_2).build()?,
                JoinType::Inner,
                (vec!["a"], vec!["a"]),
                None,
            )?
            .limit(10, Some(1000))?
            .build()?;

        // Limit pushdown Not supported in Join
        let expected = "Limit: skip=10, fetch=1000\
        \n  Inner Join: test.a = test2.a\
        \n    TableScan: test\
        \n    TableScan: test2";

        assert_optimized_plan_equal(plan, expected)
    }

    #[test]
    fn limit_offset_should_not_push_down_with_offset_sub_query() -> Result<()> {
        let table_scan_1 = test_table_scan_with_name("test1")?;
        let table_scan_2 = test_table_scan_with_name("test2")?;

        let subquery = LogicalPlanBuilder::from(table_scan_1)
            .project(vec![col("a")])?
            .filter(col("a").eq(col("test1.a")))?
            .build()?;

        let outer_query = LogicalPlanBuilder::from(table_scan_2)
            .project(vec![col("a")])?
            .filter(exists(Arc::new(subquery)))?
            .limit(10, Some(100))?
            .build()?;

        // Limit pushdown Not supported in sub_query
        let expected = "Limit: skip=10, fetch=100\
        \n  Filter: EXISTS (<subquery>)\
        \n    Subquery:\
        \n      Filter: test1.a = test1.a\
        \n        Projection: test1.a\
        \n          TableScan: test1\
        \n    Projection: test2.a\
        \n      TableScan: test2";

        assert_optimized_plan_equal(outer_query, expected)
    }

    #[test]
    fn offset_limit_should_not_push_down_with_offset_sub_query() -> Result<()> {
        let table_scan_1 = test_table_scan_with_name("test1")?;
        let table_scan_2 = test_table_scan_with_name("test2")?;

        let subquery = LogicalPlanBuilder::from(table_scan_1)
            .project(vec![col("a")])?
            .filter(col("a").eq(col("test1.a")))?
            .build()?;

        let outer_query = LogicalPlanBuilder::from(table_scan_2)
            .project(vec![col("a")])?
            .filter(exists(Arc::new(subquery)))?
            .limit(10, Some(100))?
            .build()?;

        // Limit pushdown Not supported in sub_query
        let expected = "Limit: skip=10, fetch=100\
        \n  Filter: EXISTS (<subquery>)\
        \n    Subquery:\
        \n      Filter: test1.a = test1.a\
        \n        Projection: test1.a\
        \n          TableScan: test1\
        \n    Projection: test2.a\
        \n      TableScan: test2";

        assert_optimized_plan_equal(outer_query, expected)
    }

    #[test]
    fn limit_should_push_down_join_without_condition() -> Result<()> {
        let table_scan_1 = test_table_scan()?;
        let table_scan_2 = test_table_scan_with_name("test2")?;
        let left_keys: Vec<&str> = Vec::new();
        let right_keys: Vec<&str> = Vec::new();
        let plan = LogicalPlanBuilder::from(table_scan_1.clone())
            .join(
                LogicalPlanBuilder::from(table_scan_2.clone()).build()?,
                JoinType::Left,
                (left_keys.clone(), right_keys.clone()),
                None,
            )?
            .limit(0, Some(1000))?
            .build()?;

        let expected = "Limit: skip=0, fetch=1000\
        \n  Left Join: \
        \n    Limit: skip=0, fetch=1000\
        \n      TableScan: test, fetch=1000\
        \n    Limit: skip=0, fetch=1000\
        \n      TableScan: test2, fetch=1000";

        assert_optimized_plan_equal(plan, expected)?;

        let plan = LogicalPlanBuilder::from(table_scan_1.clone())
            .join(
                LogicalPlanBuilder::from(table_scan_2.clone()).build()?,
                JoinType::Right,
                (left_keys.clone(), right_keys.clone()),
                None,
            )?
            .limit(0, Some(1000))?
            .build()?;

        let expected = "Limit: skip=0, fetch=1000\
        \n  Right Join: \
        \n    Limit: skip=0, fetch=1000\
        \n      TableScan: test, fetch=1000\
        \n    Limit: skip=0, fetch=1000\
        \n      TableScan: test2, fetch=1000";

        assert_optimized_plan_equal(plan, expected)?;

        let plan = LogicalPlanBuilder::from(table_scan_1.clone())
            .join(
                LogicalPlanBuilder::from(table_scan_2.clone()).build()?,
                JoinType::Full,
                (left_keys.clone(), right_keys.clone()),
                None,
            )?
            .limit(0, Some(1000))?
            .build()?;

        let expected = "Limit: skip=0, fetch=1000\
        \n  Full Join: \
        \n    Limit: skip=0, fetch=1000\
        \n      TableScan: test, fetch=1000\
        \n    Limit: skip=0, fetch=1000\
        \n      TableScan: test2, fetch=1000";

        assert_optimized_plan_equal(plan, expected)?;

        let plan = LogicalPlanBuilder::from(table_scan_1.clone())
            .join(
                LogicalPlanBuilder::from(table_scan_2.clone()).build()?,
                JoinType::LeftSemi,
                (left_keys.clone(), right_keys.clone()),
                None,
            )?
            .limit(0, Some(1000))?
            .build()?;

        let expected = "Limit: skip=0, fetch=1000\
        \n  LeftSemi Join: \
        \n    Limit: skip=0, fetch=1000\
        \n      TableScan: test, fetch=1000\
        \n    TableScan: test2";

        assert_optimized_plan_equal(plan, expected)?;

        let plan = LogicalPlanBuilder::from(table_scan_1.clone())
            .join(
                LogicalPlanBuilder::from(table_scan_2.clone()).build()?,
                JoinType::LeftAnti,
                (left_keys.clone(), right_keys.clone()),
                None,
            )?
            .limit(0, Some(1000))?
            .build()?;

        let expected = "Limit: skip=0, fetch=1000\
        \n  LeftAnti Join: \
        \n    Limit: skip=0, fetch=1000\
        \n      TableScan: test, fetch=1000\
        \n    TableScan: test2";

        assert_optimized_plan_equal(plan, expected)?;

        let plan = LogicalPlanBuilder::from(table_scan_1.clone())
            .join(
                LogicalPlanBuilder::from(table_scan_2.clone()).build()?,
                JoinType::RightSemi,
                (left_keys.clone(), right_keys.clone()),
                None,
            )?
            .limit(0, Some(1000))?
            .build()?;

        let expected = "Limit: skip=0, fetch=1000\
        \n  RightSemi Join: \
        \n    TableScan: test\
        \n    Limit: skip=0, fetch=1000\
        \n      TableScan: test2, fetch=1000";

        assert_optimized_plan_equal(plan, expected)?;

        let plan = LogicalPlanBuilder::from(table_scan_1)
            .join(
                LogicalPlanBuilder::from(table_scan_2).build()?,
                JoinType::RightAnti,
                (left_keys, right_keys),
                None,
            )?
            .limit(0, Some(1000))?
            .build()?;

        let expected = "Limit: skip=0, fetch=1000\
        \n  RightAnti Join: \
        \n    TableScan: test\
        \n    Limit: skip=0, fetch=1000\
        \n      TableScan: test2, fetch=1000";

        assert_optimized_plan_equal(plan, expected)
    }

    #[test]
    fn limit_should_push_down_left_outer_join() -> Result<()> {
        let table_scan_1 = test_table_scan()?;
        let table_scan_2 = test_table_scan_with_name("test2")?;

        let plan = LogicalPlanBuilder::from(table_scan_1)
            .join(
                LogicalPlanBuilder::from(table_scan_2).build()?,
                JoinType::Left,
                (vec!["a"], vec!["a"]),
                None,
            )?
            .limit(0, Some(1000))?
            .build()?;

        // Limit pushdown Not supported in Join
        let expected = "Limit: skip=0, fetch=1000\
        \n  Left Join: test.a = test2.a\
        \n    Limit: skip=0, fetch=1000\
        \n      TableScan: test, fetch=1000\
        \n    TableScan: test2";

        assert_optimized_plan_equal(plan, expected)
    }

    #[test]
    fn limit_should_push_down_left_outer_join_with_offset() -> Result<()> {
        let table_scan_1 = test_table_scan()?;
        let table_scan_2 = test_table_scan_with_name("test2")?;

        let plan = LogicalPlanBuilder::from(table_scan_1)
            .join(
                LogicalPlanBuilder::from(table_scan_2).build()?,
                JoinType::Left,
                (vec!["a"], vec!["a"]),
                None,
            )?
            .limit(10, Some(1000))?
            .build()?;

        // Limit pushdown Not supported in Join
        let expected = "Limit: skip=10, fetch=1000\
        \n  Left Join: test.a = test2.a\
        \n    Limit: skip=0, fetch=1010\
        \n      TableScan: test, fetch=1010\
        \n    TableScan: test2";

        assert_optimized_plan_equal(plan, expected)
    }

    #[test]
    fn limit_should_push_down_right_outer_join() -> Result<()> {
        let table_scan_1 = test_table_scan()?;
        let table_scan_2 = test_table_scan_with_name("test2")?;

        let plan = LogicalPlanBuilder::from(table_scan_1)
            .join(
                LogicalPlanBuilder::from(table_scan_2).build()?,
                JoinType::Right,
                (vec!["a"], vec!["a"]),
                None,
            )?
            .limit(0, Some(1000))?
            .build()?;

        // Limit pushdown Not supported in Join
        let expected = "Limit: skip=0, fetch=1000\
        \n  Right Join: test.a = test2.a\
        \n    TableScan: test\
        \n    Limit: skip=0, fetch=1000\
        \n      TableScan: test2, fetch=1000";

        assert_optimized_plan_equal(plan, expected)
    }

    #[test]
    fn limit_should_push_down_right_outer_join_with_offset() -> Result<()> {
        let table_scan_1 = test_table_scan()?;
        let table_scan_2 = test_table_scan_with_name("test2")?;

        let plan = LogicalPlanBuilder::from(table_scan_1)
            .join(
                LogicalPlanBuilder::from(table_scan_2).build()?,
                JoinType::Right,
                (vec!["a"], vec!["a"]),
                None,
            )?
            .limit(10, Some(1000))?
            .build()?;

        // Limit pushdown with offset supported in right outer join
        let expected = "Limit: skip=10, fetch=1000\
        \n  Right Join: test.a = test2.a\
        \n    TableScan: test\
        \n    Limit: skip=0, fetch=1010\
        \n      TableScan: test2, fetch=1010";

        assert_optimized_plan_equal(plan, expected)
    }

    #[test]
    fn limit_push_down_cross_join() -> Result<()> {
        let table_scan_1 = test_table_scan()?;
        let table_scan_2 = test_table_scan_with_name("test2")?;

        let plan = LogicalPlanBuilder::from(table_scan_1)
            .cross_join(LogicalPlanBuilder::from(table_scan_2).build()?)?
            .limit(0, Some(1000))?
            .build()?;

        let expected = "Limit: skip=0, fetch=1000\
        \n  CrossJoin:\
        \n    Limit: skip=0, fetch=1000\
        \n      TableScan: test, fetch=1000\
        \n    Limit: skip=0, fetch=1000\
        \n      TableScan: test2, fetch=1000";

        assert_optimized_plan_equal(plan, expected)
    }

    #[test]
    fn skip_limit_push_down_cross_join() -> Result<()> {
        let table_scan_1 = test_table_scan()?;
        let table_scan_2 = test_table_scan_with_name("test2")?;

        let plan = LogicalPlanBuilder::from(table_scan_1)
            .cross_join(LogicalPlanBuilder::from(table_scan_2).build()?)?
            .limit(1000, Some(1000))?
            .build()?;

        let expected = "Limit: skip=1000, fetch=1000\
        \n  CrossJoin:\
        \n    Limit: skip=0, fetch=2000\
        \n      TableScan: test, fetch=2000\
        \n    Limit: skip=0, fetch=2000\
        \n      TableScan: test2, fetch=2000";

        assert_optimized_plan_equal(plan, expected)
    }

    #[test]
    fn merge_limit_result_empty() -> Result<()> {
        let scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(scan)
            .limit(0, Some(1000))?
            .limit(1000, None)?
            .build()?;

        let expected = "Limit: skip=1000, fetch=0\
        \n  TableScan: test, fetch=0";

        assert_optimized_plan_equal(plan, expected)
    }

    #[test]
    fn skip_great_than_fetch() -> Result<()> {
        let scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(scan)
            .limit(0, Some(1))?
            .limit(1000, None)?
            .build()?;

        let expected = "Limit: skip=1000, fetch=0\
        \n  TableScan: test, fetch=0";

        assert_optimized_plan_equal(plan, expected)
    }

    #[test]
    fn push_down_subquery_alias() -> Result<()> {
        let scan = test_table_scan()?;

        let plan = LogicalPlanBuilder::from(scan)
            .alias("a")?
            .limit(0, Some(1))?
            .limit(1000, None)?
            .build()?;

        let expected = "SubqueryAlias: a\
        \n  Limit: skip=1000, fetch=0\
        \n    TableScan: test, fetch=0";

        assert_optimized_plan_equal(plan, expected)
    }
}
