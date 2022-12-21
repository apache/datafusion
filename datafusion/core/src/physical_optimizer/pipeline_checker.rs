// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! PipelineChecker rule is ensures that a given plan can accommodate its
//! infinite sources, if there are any.
//!
use crate::error::Result;
use crate::physical_optimizer::join_selection::swap_hash_join;
use crate::physical_optimizer::PhysicalOptimizerRule;
use crate::physical_plan::joins::{HashJoinExec, PartitionMode};
use crate::physical_plan::rewrite::TreeNodeRewritable;
use crate::physical_plan::{with_new_children_if_necessary, ExecutionPlan};
use crate::prelude::SessionConfig;
use datafusion_common::DataFusionError;
use datafusion_expr::logical_plan::JoinType;
use std::sync::Arc;

/// The PipelineChecker rule ensures that the given plan can accommodate
/// its infinite sources, if there are any. It will reject any plan with
/// pipeline-breaking operators with an diagnostic error message.
#[derive(Default)]
pub struct PipelineChecker {}

impl PipelineChecker {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
}
type PipelineCheckerSubrule =
    dyn Fn(&PipelineStatePropagator) -> Option<Result<PipelineStatePropagator>>;
impl PhysicalOptimizerRule for PipelineChecker {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &SessionConfig,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let pipeline = PipelineStatePropagator::new(plan);
        let physical_optimizer_subrules: Vec<Box<PipelineCheckerSubrule>> =
            vec![Box::new(hash_join_swap_subrule)];
        let state = pipeline.transform_up(&|p| {
            apply_subrules_and_check_finiteness_requirements(
                p,
                &physical_optimizer_subrules,
            )
        })?;
        Ok(state.plan)
    }

    fn name(&self) -> &str {
        "PipelineChecker"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

/// It will swap build/probe sides of a hash join depending on whether its inputs may
/// produce an infinite stream of records. The rule ensures that the left (build) side
/// of the join always operates on an input stream that will produce a finite set of.
/// recordsç If the left side can not be chosen to be "finite", the order stays the
/// same as the original query.
/// ```text
/// For example, this rule makes the following transformation:
///
///
///
///           +--------------+              +--------------+
///           |              |  unbounded   |              |
///    Left   | Infinite     |    true      | Hash         |\true
///           | Data source  |--------------| Repartition  | \   +--------------+       +--------------+
///           |              |              |              |  \  |              |       |              |
///           +--------------+              +--------------+   - |  Hash Join   |-------| Projection   |
///                                                            - |              |       |              |
///           +--------------+              +--------------+  /  +--------------+       +--------------+
///           |              |  unbounded   |              | /
///    Right  | Finite       |    false     | Hash         |/false
///           | Data Source  |--------------| Repartition  |
///           |              |              |              |
///           +--------------+              +--------------+
///
///
///
///           +--------------+              +--------------+
///           |              |  unbounded   |              |
///    Left   | Finite       |    false     | Hash         |\false
///           | Data source  |--------------| Repartition  | \   +--------------+       +--------------+
///           |              |              |              |  \  |              | true  |              | true
///           +--------------+              +--------------+   - |  Hash Join   |-------| Projection   |-----
///                                                            - |              |       |              |
///           +--------------+              +--------------+  /  +--------------+       +--------------+
///           |              |  unbounded   |              | /
///    Right  | Infinite     |    true      | Hash         |/true
///           | Data Source  |--------------| Repartition  |
///           |              |              |              |
///           +--------------+              +--------------+
///
/// ```
fn hash_join_swap_subrule(
    input: &PipelineStatePropagator,
) -> Option<Result<PipelineStatePropagator>> {
    let plan = input.plan.clone();
    let children = &input.children_unbounded;
    if let Some(hash_join) = plan.as_any().downcast_ref::<HashJoinExec>() {
        let (left_unbounded, right_unbounded) = (children[0], children[1]);
        let new_plan = match (left_unbounded, right_unbounded) {
            (true, false) => {
                if matches!(
                    *hash_join.join_type(),
                    JoinType::Inner
                        | JoinType::Left
                        | JoinType::LeftSemi
                        | JoinType::LeftAnti
                ) {
                    swap(hash_join)
                } else {
                    Ok(plan)
                }
            }
            _ => Ok(plan),
        };
        match new_plan {
            Ok(plan) => Some(Ok(PipelineStatePropagator {
                plan,
                unbounded: left_unbounded || right_unbounded,
                children_unbounded: vec![left_unbounded, right_unbounded],
            })),
            Err(e) => Some(Err(e)),
        }
    } else {
        None
    }
}

/// This function swaps sides of a hash join to make it runnable even if one of its
/// inputs are infinite. Note that this is not always possible; i.e. [JoinType::Full],
/// [JoinType::Left], [JoinType::LeftAnti] and [JoinType::LeftSemi] can not run with
/// an unbounded left side, even if we swap. Therefore, we do not consider them here.
fn swap(hash_join: &HashJoinExec) -> Result<Arc<dyn ExecutionPlan>> {
    let partition_mode = hash_join.partition_mode();
    let join_type = hash_join.join_type();
    match (*partition_mode, *join_type) {
        (
            _,
            JoinType::Right | JoinType::RightSemi | JoinType::RightAnti | JoinType::Full,
        ) => Err(DataFusionError::Internal(format!(
            "{} join cannot be swapped.",
            join_type
        ))),
        (PartitionMode::Partitioned, _) => {
            swap_hash_join(hash_join, PartitionMode::Partitioned)
        }
        (PartitionMode::CollectLeft, _) => {
            swap_hash_join(hash_join, PartitionMode::CollectLeft)
        }
        (PartitionMode::Auto, _) => Err(DataFusionError::Internal(
            "Auto is not acceptable here.".to_string(),
        )),
    }
}

/// [PipelineStatePropagator] propagates the [ExecutionPlan] pipelining information.
#[derive(Clone, Debug)]
pub struct PipelineStatePropagator {
    pub(crate) plan: Arc<dyn ExecutionPlan>,
    pub(crate) unbounded: bool,
    pub(crate) children_unbounded: Vec<bool>,
}

impl PipelineStatePropagator {
    /// Constructs a new, default pipelining state.
    pub fn new(plan: Arc<dyn ExecutionPlan>) -> Self {
        let length = plan.children().len();
        PipelineStatePropagator {
            plan,
            unbounded: false,
            children_unbounded: vec![false; length],
        }
    }
    /// It generates children of the execution plan with state.
    pub fn children(&self) -> Vec<PipelineStatePropagator> {
        self.plan
            .children()
            .into_iter()
            .map(|child| {
                let length = child.children().len();
                PipelineStatePropagator {
                    plan: child,
                    unbounded: false,
                    children_unbounded: vec![false; length],
                }
            })
            .collect()
    }
}

impl TreeNodeRewritable for PipelineStatePropagator {
    fn map_children<F>(self, transform: F) -> Result<Self>
    where
        F: FnMut(Self) -> Result<Self>,
    {
        let children = self.children();
        if !children.is_empty() {
            let new_children = children
                .into_iter()
                .map(transform)
                .collect::<Result<Vec<_>>>()?;
            let children_unbounded = new_children
                .iter()
                .map(|c| c.unbounded)
                .collect::<Vec<bool>>();
            let children_plans = new_children
                .into_iter()
                .map(|child| child.plan)
                .collect::<Vec<_>>();
            Ok(PipelineStatePropagator {
                plan: with_new_children_if_necessary(self.plan, children_plans)?,
                unbounded: self.unbounded,
                children_unbounded,
            })
        } else {
            Ok(self)
        }
    }
}

fn apply_subrules_and_check_finiteness_requirements(
    mut input: PipelineStatePropagator,
    physical_optimizer_subrules: &Vec<Box<PipelineCheckerSubrule>>,
) -> Result<Option<PipelineStatePropagator>> {
    for sub_rule in physical_optimizer_subrules {
        match sub_rule(&input) {
            Some(Ok(value)) => {
                input = value;
            }
            Some(Err(e)) => return Err(e),
            _ => {}
        }
    }

    let plan = input.plan;
    let children = &input.children_unbounded;
    match plan.unbounded_output(children) {
        Ok(value) => Ok(Some(PipelineStatePropagator {
            plan,
            unbounded: value,
            children_unbounded: input.children_unbounded,
        })),
        Err(e) => Err(e),
    }
}
#[cfg(test)]
mod sql_tests {
    use super::*;
    use crate::physical_optimizer::test_utils::{
        BinaryTestCase, QueryCase, SourceType, UnaryTestCase,
    };

    #[tokio::test]
    async fn test_hash_left_join_swap() -> Result<()> {
        let test1 = BinaryTestCase {
            source_types: (SourceType::Unbounded, SourceType::Bounded),
            expect_fail: false,
        };
        let test2 = BinaryTestCase {
            source_types: (SourceType::Unbounded, SourceType::Unbounded),
            expect_fail: true,
        };
        let test3 = BinaryTestCase {
            source_types: (SourceType::Bounded, SourceType::Unbounded),
            expect_fail: true,
        };
        let test4 = BinaryTestCase {
            source_types: (SourceType::Bounded, SourceType::Bounded),
            expect_fail: false,
        };
        let case = QueryCase {
            sql: "SELECT t2.c1 FROM left as t1 LEFT JOIN right as t2 ON t1.c1 = t2.c1"
                .to_string(),
            cases: vec![
                Arc::new(test1),
                Arc::new(test2),
                Arc::new(test3),
                Arc::new(test4),
            ],
            error_operator: "Join Error".to_string(),
        };

        case.run().await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_hash_right_join_swap() -> Result<()> {
        let test1 = BinaryTestCase {
            source_types: (SourceType::Unbounded, SourceType::Bounded),
            expect_fail: true,
        };
        let test2 = BinaryTestCase {
            source_types: (SourceType::Unbounded, SourceType::Unbounded),
            expect_fail: true,
        };
        let test3 = BinaryTestCase {
            source_types: (SourceType::Bounded, SourceType::Unbounded),
            expect_fail: false,
        };
        let test4 = BinaryTestCase {
            source_types: (SourceType::Bounded, SourceType::Bounded),
            expect_fail: false,
        };
        let case = QueryCase {
            sql: "SELECT t2.c1 FROM left as t1 RIGHT JOIN right as t2 ON t1.c1 = t2.c1"
                .to_string(),
            cases: vec![
                Arc::new(test1),
                Arc::new(test2),
                Arc::new(test3),
                Arc::new(test4),
            ],
            error_operator: "Join Error".to_string(),
        };

        case.run().await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_hash_inner_join_swap() -> Result<()> {
        let test1 = BinaryTestCase {
            source_types: (SourceType::Unbounded, SourceType::Bounded),
            expect_fail: false,
        };
        let test2 = BinaryTestCase {
            source_types: (SourceType::Unbounded, SourceType::Unbounded),
            expect_fail: true,
        };
        let test3 = BinaryTestCase {
            source_types: (SourceType::Bounded, SourceType::Unbounded),
            expect_fail: false,
        };
        let test4 = BinaryTestCase {
            source_types: (SourceType::Bounded, SourceType::Bounded),
            expect_fail: false,
        };
        let case = QueryCase {
            sql: "SELECT t2.c1 FROM left as t1 JOIN right as t2 ON t1.c1 = t2.c1"
                .to_string(),
            cases: vec![
                Arc::new(test1),
                Arc::new(test2),
                Arc::new(test3),
                Arc::new(test4),
            ],
            error_operator: "Join Error".to_string(),
        };

        case.run().await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_hash_full_outer_join_swap() -> Result<()> {
        let test1 = BinaryTestCase {
            source_types: (SourceType::Unbounded, SourceType::Bounded),
            expect_fail: true,
        };
        let test2 = BinaryTestCase {
            source_types: (SourceType::Unbounded, SourceType::Unbounded),
            expect_fail: true,
        };
        let test3 = BinaryTestCase {
            source_types: (SourceType::Bounded, SourceType::Unbounded),
            expect_fail: true,
        };
        let test4 = BinaryTestCase {
            source_types: (SourceType::Bounded, SourceType::Bounded),
            expect_fail: false,
        };
        let case = QueryCase {
            sql: "SELECT t2.c1 FROM left as t1 FULL JOIN right as t2 ON t1.c1 = t2.c1"
                .to_string(),
            cases: vec![
                Arc::new(test1),
                Arc::new(test2),
                Arc::new(test3),
                Arc::new(test4),
            ],
            error_operator: "Join Error".to_string(),
        };

        case.run().await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_aggregate() -> Result<()> {
        let test1 = UnaryTestCase {
            source_type: SourceType::Bounded,
            expect_fail: false,
        };
        let test2 = UnaryTestCase {
            source_type: SourceType::Unbounded,
            expect_fail: true,
        };
        let case = QueryCase {
            sql: "SELECT c1, MIN(c4) FROM test GROUP BY c1".to_string(),
            cases: vec![Arc::new(test1), Arc::new(test2)],
            error_operator: "Aggregate Error".to_string(),
        };

        case.run().await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_window_agg_hash_partition() -> Result<()> {
        let test1 = UnaryTestCase {
            source_type: SourceType::Bounded,
            expect_fail: false,
        };
        let test2 = UnaryTestCase {
            source_type: SourceType::Unbounded,
            expect_fail: true,
        };
        let case = QueryCase {
            sql: "SELECT
                    c9,
                    SUM(c9) OVER(PARTITION BY c1 ORDER BY c9 ASC ROWS BETWEEN 1 PRECEDING AND 5 FOLLOWING) as sum1
                  FROM test
                  LIMIT 5".to_string(),
            cases: vec![Arc::new(test1), Arc::new(test2)],
            error_operator: "Sort Error".to_string()
        };

        case.run().await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_window_agg_single_partition() -> Result<()> {
        let test1 = UnaryTestCase {
            source_type: SourceType::Bounded,
            expect_fail: false,
        };
        let test2 = UnaryTestCase {
            source_type: SourceType::Unbounded,
            expect_fail: true,
        };
        let case = QueryCase {
            sql: "SELECT
                        c9,
                        SUM(c9) OVER(ORDER BY c9 ASC ROWS BETWEEN 1 PRECEDING AND 5 FOLLOWING) as sum1
                  FROM test".to_string(),
            cases: vec![Arc::new(test1), Arc::new(test2)],
            error_operator: "Sort Error".to_string()
        };
        case.run().await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_hash_cross_join() -> Result<()> {
        let test1 = BinaryTestCase {
            source_types: (SourceType::Unbounded, SourceType::Bounded),
            expect_fail: true,
        };
        let test2 = BinaryTestCase {
            source_types: (SourceType::Unbounded, SourceType::Unbounded),
            expect_fail: true,
        };
        let test3 = BinaryTestCase {
            source_types: (SourceType::Bounded, SourceType::Unbounded),
            expect_fail: true,
        };
        let test4 = BinaryTestCase {
            source_types: (SourceType::Bounded, SourceType::Bounded),
            expect_fail: false,
        };
        let case = QueryCase {
            sql: "SELECT t2.c1 FROM left as t1 CROSS JOIN right as t2".to_string(),
            cases: vec![
                Arc::new(test1),
                Arc::new(test2),
                Arc::new(test3),
                Arc::new(test4),
            ],
            error_operator: "Cross Join Error".to_string(),
        };

        case.run().await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_analyzer() -> Result<()> {
        let test1 = UnaryTestCase {
            source_type: SourceType::Bounded,
            expect_fail: false,
        };
        let test2 = UnaryTestCase {
            source_type: SourceType::Unbounded,
            expect_fail: true,
        };
        let case = QueryCase {
            sql: "EXPLAIN ANALYZE SELECT * FROM test".to_string(),
            cases: vec![Arc::new(test1), Arc::new(test2)],
            error_operator: "Analyze Error".to_string(),
        };

        case.run().await?;
        Ok(())
    }
}

#[cfg(test)]
mod hash_join_tests {
    use super::*;
    use crate::physical_optimizer::join_selection::swap_join_type;
    use crate::physical_optimizer::test_utils::SourceType;
    use crate::physical_plan::expressions::Column;
    use crate::physical_plan::projection::ProjectionExec;
    use crate::{physical_plan::joins::PartitionMode, test::exec::UnboundedExec};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    struct TestCase {
        case: String,
        initial_sources_unbounded: (SourceType, SourceType),
        initial_join_type: JoinType,
        initial_mode: PartitionMode,
        expected_sources_unbounded: (SourceType, SourceType),
        expected_join_type: JoinType,
        expected_mode: PartitionMode,
        expecting_swap: bool,
    }

    #[tokio::test]
    async fn test_join_with_swap_full() -> Result<()> {
        // NOTE: Currently, some initial conditions are not viable after join order selection.
        //       For example, full join always comes in partitioned mode. See the warning in
        //       function "swap". If this changes in the future, we should update these tests.
        let cases = vec![
            TestCase {
                case: "Bounded - Unbounded 1".to_string(),
                initial_sources_unbounded: (SourceType::Bounded, SourceType::Unbounded),
                initial_join_type: JoinType::Full,
                initial_mode: PartitionMode::Partitioned,
                expected_sources_unbounded: (SourceType::Bounded, SourceType::Unbounded),
                expected_join_type: JoinType::Full,
                expected_mode: PartitionMode::Partitioned,
                expecting_swap: false,
            },
            TestCase {
                case: "Unbounded - Bounded 2".to_string(),
                initial_sources_unbounded: (SourceType::Unbounded, SourceType::Bounded),
                initial_join_type: JoinType::Full,
                initial_mode: PartitionMode::Partitioned,
                expected_sources_unbounded: (SourceType::Unbounded, SourceType::Bounded),
                expected_join_type: JoinType::Full,
                expected_mode: PartitionMode::Partitioned,
                expecting_swap: false,
            },
            TestCase {
                case: "Bounded - Bounded 3".to_string(),
                initial_sources_unbounded: (SourceType::Bounded, SourceType::Bounded),
                initial_join_type: JoinType::Full,
                initial_mode: PartitionMode::Partitioned,
                expected_sources_unbounded: (SourceType::Bounded, SourceType::Bounded),
                expected_join_type: JoinType::Full,
                expected_mode: PartitionMode::Partitioned,
                expecting_swap: false,
            },
            TestCase {
                case: "Unbounded - Unbounded 4".to_string(),
                initial_sources_unbounded: (SourceType::Unbounded, SourceType::Unbounded),
                initial_join_type: JoinType::Full,
                initial_mode: PartitionMode::Partitioned,
                expected_sources_unbounded: (
                    SourceType::Unbounded,
                    SourceType::Unbounded,
                ),
                expected_join_type: JoinType::Full,
                expected_mode: PartitionMode::Partitioned,
                expecting_swap: false,
            },
        ];
        for case in cases.into_iter() {
            test_join_with_maybe_swap_unbounded_case(case).await?
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_cases_without_collect_left_check() -> Result<()> {
        let mut cases = vec![];
        let join_types = vec![JoinType::LeftSemi, JoinType::Inner];
        for join_type in join_types {
            cases.push(TestCase {
                case: "Unbounded - Bounded / CollectLeft".to_string(),
                initial_sources_unbounded: (SourceType::Unbounded, SourceType::Bounded),
                initial_join_type: join_type,
                initial_mode: PartitionMode::CollectLeft,
                expected_sources_unbounded: (SourceType::Bounded, SourceType::Unbounded),
                expected_join_type: swap_join_type(join_type),
                expected_mode: PartitionMode::CollectLeft,
                expecting_swap: true,
            });
            cases.push(TestCase {
                case: "Bounded - Unbounded / CollectLeft".to_string(),
                initial_sources_unbounded: (SourceType::Bounded, SourceType::Unbounded),
                initial_join_type: join_type,
                initial_mode: PartitionMode::CollectLeft,
                expected_sources_unbounded: (SourceType::Bounded, SourceType::Unbounded),
                expected_join_type: join_type,
                expected_mode: PartitionMode::CollectLeft,
                expecting_swap: false,
            });
            cases.push(TestCase {
                case: "Unbounded - Unbounded / CollectLeft".to_string(),
                initial_sources_unbounded: (SourceType::Unbounded, SourceType::Unbounded),
                initial_join_type: join_type,
                initial_mode: PartitionMode::CollectLeft,
                expected_sources_unbounded: (
                    SourceType::Unbounded,
                    SourceType::Unbounded,
                ),
                expected_join_type: join_type,
                expected_mode: PartitionMode::CollectLeft,
                expecting_swap: false,
            });
            cases.push(TestCase {
                case: "Bounded - Bounded / CollectLeft".to_string(),
                initial_sources_unbounded: (SourceType::Bounded, SourceType::Bounded),
                initial_join_type: join_type,
                initial_mode: PartitionMode::CollectLeft,
                expected_sources_unbounded: (SourceType::Bounded, SourceType::Bounded),
                expected_join_type: join_type,
                expected_mode: PartitionMode::CollectLeft,
                expecting_swap: false,
            });
            cases.push(TestCase {
                case: "Unbounded - Bounded / Partitioned".to_string(),
                initial_sources_unbounded: (SourceType::Unbounded, SourceType::Bounded),
                initial_join_type: join_type,
                initial_mode: PartitionMode::Partitioned,
                expected_sources_unbounded: (SourceType::Bounded, SourceType::Unbounded),
                expected_join_type: swap_join_type(join_type),
                expected_mode: PartitionMode::Partitioned,
                expecting_swap: true,
            });
            cases.push(TestCase {
                case: "Bounded - Unbounded / Partitioned".to_string(),
                initial_sources_unbounded: (SourceType::Bounded, SourceType::Unbounded),
                initial_join_type: join_type,
                initial_mode: PartitionMode::Partitioned,
                expected_sources_unbounded: (SourceType::Bounded, SourceType::Unbounded),
                expected_join_type: join_type,
                expected_mode: PartitionMode::Partitioned,
                expecting_swap: false,
            });
            cases.push(TestCase {
                case: "Bounded - Bounded / Partitioned".to_string(),
                initial_sources_unbounded: (SourceType::Bounded, SourceType::Bounded),
                initial_join_type: join_type,
                initial_mode: PartitionMode::Partitioned,
                expected_sources_unbounded: (SourceType::Bounded, SourceType::Bounded),
                expected_join_type: join_type,
                expected_mode: PartitionMode::Partitioned,
                expecting_swap: false,
            });
            cases.push(TestCase {
                case: "Unbounded - Unbounded / Partitioned".to_string(),
                initial_sources_unbounded: (SourceType::Unbounded, SourceType::Unbounded),
                initial_join_type: join_type,
                initial_mode: PartitionMode::Partitioned,
                expected_sources_unbounded: (
                    SourceType::Unbounded,
                    SourceType::Unbounded,
                ),
                expected_join_type: join_type,
                expected_mode: PartitionMode::Partitioned,
                expecting_swap: false,
            });
        }

        for case in cases.into_iter() {
            test_join_with_maybe_swap_unbounded_case(case).await?
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_not_support_collect_left() -> Result<()> {
        let mut cases = vec![];
        // After [JoinSelection] optimization, these join types cannot run in CollectLeft mode except
        // [JoinType::LeftSemi]
        let the_ones_not_support_collect_left = vec![JoinType::Left, JoinType::LeftAnti];
        for join_type in the_ones_not_support_collect_left {
            cases.push(TestCase {
                case: "Unbounded - Bounded".to_string(),
                initial_sources_unbounded: (SourceType::Unbounded, SourceType::Bounded),
                initial_join_type: join_type,
                initial_mode: PartitionMode::Partitioned,
                expected_sources_unbounded: (SourceType::Bounded, SourceType::Unbounded),
                expected_join_type: swap_join_type(join_type),
                expected_mode: PartitionMode::Partitioned,
                expecting_swap: true,
            });
            cases.push(TestCase {
                case: "Bounded - Unbounded".to_string(),
                initial_sources_unbounded: (SourceType::Bounded, SourceType::Unbounded),
                initial_join_type: join_type,
                initial_mode: PartitionMode::Partitioned,
                expected_sources_unbounded: (SourceType::Bounded, SourceType::Unbounded),
                expected_join_type: join_type,
                expected_mode: PartitionMode::Partitioned,
                expecting_swap: false,
            });
            cases.push(TestCase {
                case: "Bounded - Bounded".to_string(),
                initial_sources_unbounded: (SourceType::Bounded, SourceType::Bounded),
                initial_join_type: join_type,
                initial_mode: PartitionMode::Partitioned,
                expected_sources_unbounded: (SourceType::Bounded, SourceType::Bounded),
                expected_join_type: join_type,
                expected_mode: PartitionMode::Partitioned,
                expecting_swap: false,
            });
            cases.push(TestCase {
                case: "Unbounded - Unbounded".to_string(),
                initial_sources_unbounded: (SourceType::Unbounded, SourceType::Unbounded),
                initial_join_type: join_type,
                initial_mode: PartitionMode::Partitioned,
                expected_sources_unbounded: (
                    SourceType::Unbounded,
                    SourceType::Unbounded,
                ),
                expected_join_type: join_type,
                expected_mode: PartitionMode::Partitioned,
                expecting_swap: false,
            });
        }

        for case in cases.into_iter() {
            test_join_with_maybe_swap_unbounded_case(case).await?
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_not_supporting_swaps_possible_collect_left() -> Result<()> {
        let mut cases = vec![];
        let the_ones_not_support_collect_left =
            vec![JoinType::Right, JoinType::RightAnti, JoinType::RightSemi];
        for join_type in the_ones_not_support_collect_left {
            // We expect that (SourceType::Unbounded, SourceType::Bounded) will change, regardless of the
            // statistics.
            cases.push(TestCase {
                case: "Unbounded - Bounded / CollectLeft".to_string(),
                initial_sources_unbounded: (SourceType::Unbounded, SourceType::Bounded),
                initial_join_type: join_type,
                initial_mode: PartitionMode::CollectLeft,
                expected_sources_unbounded: (SourceType::Unbounded, SourceType::Bounded),
                expected_join_type: join_type,
                expected_mode: PartitionMode::CollectLeft,
                expecting_swap: false,
            });
            // We expect that (SourceType::Bounded, SourceType::Unbounded) will stay same, regardless of the
            // statistics.
            cases.push(TestCase {
                case: "Bounded - Unbounded / CollectLeft".to_string(),
                initial_sources_unbounded: (SourceType::Bounded, SourceType::Unbounded),
                initial_join_type: join_type,
                initial_mode: PartitionMode::CollectLeft,
                expected_sources_unbounded: (SourceType::Bounded, SourceType::Unbounded),
                expected_join_type: join_type,
                expected_mode: PartitionMode::CollectLeft,
                expecting_swap: false,
            });
            cases.push(TestCase {
                case: "Unbounded - Unbounded / CollectLeft".to_string(),
                initial_sources_unbounded: (SourceType::Unbounded, SourceType::Unbounded),
                initial_join_type: join_type,
                initial_mode: PartitionMode::CollectLeft,
                expected_sources_unbounded: (
                    SourceType::Unbounded,
                    SourceType::Unbounded,
                ),
                expected_join_type: join_type,
                expected_mode: PartitionMode::CollectLeft,
                expecting_swap: false,
            });
            //
            cases.push(TestCase {
                case: "Bounded - Bounded / CollectLeft".to_string(),
                initial_sources_unbounded: (SourceType::Bounded, SourceType::Bounded),
                initial_join_type: join_type,
                initial_mode: PartitionMode::CollectLeft,
                expected_sources_unbounded: (SourceType::Bounded, SourceType::Bounded),
                expected_join_type: join_type,
                expected_mode: PartitionMode::CollectLeft,
                expecting_swap: false,
            });
            // If cases are partitioned, only unbounded & bounded check will affect the order.
            cases.push(TestCase {
                case: "Unbounded - Bounded / Partitioned".to_string(),
                initial_sources_unbounded: (SourceType::Unbounded, SourceType::Bounded),
                initial_join_type: join_type,
                initial_mode: PartitionMode::Partitioned,
                expected_sources_unbounded: (SourceType::Unbounded, SourceType::Bounded),
                expected_join_type: join_type,
                expected_mode: PartitionMode::Partitioned,
                expecting_swap: false,
            });
            cases.push(TestCase {
                case: "Bounded - Unbounded / Partitioned".to_string(),
                initial_sources_unbounded: (SourceType::Bounded, SourceType::Unbounded),
                initial_join_type: join_type,
                initial_mode: PartitionMode::Partitioned,
                expected_sources_unbounded: (SourceType::Bounded, SourceType::Unbounded),
                expected_join_type: join_type,
                expected_mode: PartitionMode::Partitioned,
                expecting_swap: false,
            });
            cases.push(TestCase {
                case: "Bounded - Bounded / Partitioned".to_string(),
                initial_sources_unbounded: (SourceType::Bounded, SourceType::Bounded),
                initial_join_type: join_type,
                initial_mode: PartitionMode::Partitioned,
                expected_sources_unbounded: (SourceType::Bounded, SourceType::Bounded),
                expected_join_type: join_type,
                expected_mode: PartitionMode::Partitioned,
                expecting_swap: false,
            });
            cases.push(TestCase {
                case: "Unbounded - Unbounded / Partitioned".to_string(),
                initial_sources_unbounded: (SourceType::Unbounded, SourceType::Unbounded),
                initial_join_type: join_type,
                initial_mode: PartitionMode::Partitioned,
                expected_sources_unbounded: (
                    SourceType::Unbounded,
                    SourceType::Unbounded,
                ),
                expected_join_type: join_type,
                expected_mode: PartitionMode::Partitioned,
                expecting_swap: false,
            });
        }

        for case in cases.into_iter() {
            test_join_with_maybe_swap_unbounded_case(case).await?
        }
        Ok(())
    }
    #[allow(clippy::vtable_address_comparisons)]
    async fn test_join_with_maybe_swap_unbounded_case(t: TestCase) -> Result<()> {
        let left_unbounded = t.initial_sources_unbounded.0 == SourceType::Unbounded;
        let right_unbounded = t.initial_sources_unbounded.1 == SourceType::Unbounded;
        let left_exec = Arc::new(UnboundedExec::new(
            left_unbounded,
            Schema::new(vec![Field::new("a", DataType::Int32, false)]),
        )) as Arc<dyn ExecutionPlan>;
        let right_exec = Arc::new(UnboundedExec::new(
            right_unbounded,
            Schema::new(vec![Field::new("b", DataType::Int32, false)]),
        )) as Arc<dyn ExecutionPlan>;

        let join = HashJoinExec::try_new(
            Arc::clone(&left_exec),
            Arc::clone(&right_exec),
            vec![(
                Column::new_with_schema("a", &left_exec.schema())?,
                Column::new_with_schema("b", &right_exec.schema())?,
            )],
            None,
            &t.initial_join_type,
            t.initial_mode,
            &false,
        )?;

        let initial_hash_join_state = PipelineStatePropagator {
            plan: Arc::new(join),
            unbounded: false,
            children_unbounded: vec![left_unbounded, right_unbounded],
        };
        let optimized_hash_join =
            hash_join_swap_subrule(&initial_hash_join_state).unwrap()?;
        let optimized_join_plan = optimized_hash_join.plan;

        // If swap did happen
        let projection_added = optimized_join_plan.as_any().is::<ProjectionExec>();
        let plan = if projection_added {
            let proj = optimized_join_plan
                .as_any()
                .downcast_ref::<ProjectionExec>()
                .expect(
                    "A proj is required to swap columns back to their original order",
                );
            proj.input().clone()
        } else {
            optimized_join_plan
        };

        if let Some(HashJoinExec {
            left,
            right,
            join_type,
            mode,
            ..
        }) = plan.as_any().downcast_ref::<HashJoinExec>()
        {
            let left_changed = Arc::ptr_eq(left, &right_exec);
            let right_changed = Arc::ptr_eq(right, &left_exec);
            // If this is not equal, we have a bigger problem.
            assert_eq!(left_changed, right_changed);
            assert_eq!(
                (
                    t.case.as_str(),
                    if left.unbounded_output(&[])? {
                        SourceType::Unbounded
                    } else {
                        SourceType::Bounded
                    },
                    if right.unbounded_output(&[])? {
                        SourceType::Unbounded
                    } else {
                        SourceType::Bounded
                    },
                    join_type,
                    mode,
                    left_changed && right_changed
                ),
                (
                    t.case.as_str(),
                    t.expected_sources_unbounded.0,
                    t.expected_sources_unbounded.1,
                    &t.expected_join_type,
                    &t.expected_mode,
                    t.expecting_swap
                )
            );
        };
        Ok(())
    }
    #[cfg(not(target_os = "windows"))]
    mod unix_test {
        use crate::prelude::SessionConfig;
        use crate::{
            prelude::{CsvReadOptions, SessionContext},
            test_util::{aggr_test_schema, arrow_test_data},
        };
        use arrow::datatypes::{DataType, Field, Schema};
        use datafusion_common::{DataFusionError, Result};
        use futures::StreamExt;
        use itertools::enumerate;
        use nix::sys::stat;
        use nix::unistd;
        use rstest::*;
        use std::fs::{File, OpenOptions};
        use std::io::Write;
        use std::path::Path;
        use std::path::PathBuf;
        use std::sync::mpsc;
        use std::sync::mpsc::{Receiver, Sender};
        use std::sync::{Arc, Mutex};
        use std::thread;
        use std::time::{Duration, Instant};
        use tempfile::TempDir;
        // !  For the sake of the test, do not alter the numbers. !
        // Session batch size
        const TEST_BATCH_SIZE: usize = 20;
        // Number of lines written to FIFO
        const TEST_DATA_SIZE: usize = 20_000;
        // Number of lines what can be joined. Each joinable key produced 20 lines with
        // aggregate_test_100 dataset. We will use these joinable keys for understanding
        // incremental execution.
        const TEST_JOIN_RATIO: f64 = 0.01;

        fn create_fifo_file(tmp_dir: &TempDir, file_name: &str) -> Result<PathBuf> {
            let file_path = tmp_dir.path().join(file_name);
            // Simulate an infinite environment via a FIFO file
            if let Err(e) = unistd::mkfifo(&file_path, stat::Mode::S_IRWXU) {
                Err(DataFusionError::Execution(e.to_string()))
            } else {
                Ok(file_path)
            }
        }

        fn write_to_fifo(
            mut file: &File,
            line: &str,
            ref_time: Instant,
            broken_pipe_timeout: Duration,
        ) -> Result<usize> {
            // We need to handle broken pipe error until the reader is ready. This
            // is why we use a timeout to limit the wait duration for the reader.
            // If the error is different than broken pipe, we fail immediately.
            file.write(line.as_bytes()).or_else(|e| {
                if e.raw_os_error().unwrap() == 32 {
                    let interval = Instant::now().duration_since(ref_time);
                    if interval < broken_pipe_timeout {
                        thread::sleep(Duration::from_millis(100));
                        return Ok(0);
                    }
                }
                Err(DataFusionError::Execution(e.to_string()))
            })
        }

        async fn create_ctx(
            fifo_path: &Path,
            with_unbounded_execution: bool,
        ) -> Result<SessionContext> {
            let config = SessionConfig::new()
                .with_batch_size(TEST_BATCH_SIZE)
                .set_u64(
                    "datafusion.execution.coalesce_target_batch_size",
                    TEST_BATCH_SIZE as u64,
                );
            let ctx = SessionContext::with_config(config);
            // Register left table
            let left_schema = Arc::new(Schema::new(vec![
                Field::new("a1", DataType::Utf8, false),
                Field::new("a2", DataType::UInt32, false),
            ]));
            ctx.register_csv(
                "left",
                fifo_path.as_os_str().to_str().unwrap(),
                CsvReadOptions::new()
                    .schema(left_schema.as_ref())
                    .has_header(false)
                    .mark_infinite(with_unbounded_execution),
            )
            .await?;
            // Register right table
            let schema = aggr_test_schema();
            let test_data = arrow_test_data();
            ctx.register_csv(
                "right",
                &format!("{}/csv/aggregate_test_100.csv", test_data),
                CsvReadOptions::new().schema(schema.as_ref()),
            )
            .await?;
            Ok(ctx)
        }

        #[derive(Debug, PartialEq)]
        enum Operation {
            Read,
            Write,
        }

        /// Checks if there is a [Operation::Read] between [Operation::Write]s.
        /// This indicates we did not wait for the file to finish before processing it.
        fn interleave(result: &[Operation]) -> bool {
            let first_read = result.iter().position(|op| op == &Operation::Read);
            let last_write = result.iter().rev().position(|op| op == &Operation::Write);
            match (first_read, last_write) {
                (Some(first_read), Some(last_write)) => {
                    result.len() - 1 - last_write > first_read
                }
                (_, _) => false,
            }
        }

        // This test provides a relatively realistic end-to-end scenario where
        // we ensure that we swap join sides correctly to accommodate a FIFO source.
        #[rstest]
        #[timeout(std::time::Duration::from_secs(30))]
        #[tokio::test(flavor = "multi_thread", worker_threads = 5)]
        async fn unbounded_file_with_swapped_join(
            #[values(true, false)] unbounded_file: bool,
        ) -> Result<()> {
            // To make unbounded deterministic
            let waiting = Arc::new(Mutex::new(unbounded_file));
            let waiting_thread = waiting.clone();
            // Create a channel
            let (tx, rx): (Sender<Operation>, Receiver<Operation>) = mpsc::channel();
            // Create a new temporary FIFO file
            let tmp_dir = TempDir::new()?;
            let fifo_path = create_fifo_file(&tmp_dir, "fisrt_fifo.csv")?;
            // Prevent move
            let fifo_path_thread = fifo_path.clone();
            // Timeout for a long period of BrokenPipe error
            let broken_pipe_timeout = Duration::from_secs(5);
            // The sender endpoint can be copied
            let thread_tx = tx.clone();
            // Spawn a new thread to write to the FIFO file
            let fifo_writer = thread::spawn(move || {
                let first_file = OpenOptions::new()
                    .write(true)
                    .open(fifo_path_thread)
                    .unwrap();
                // Reference time to use when deciding to fail the test
                let execution_start = Instant::now();
                // Execution can calculated at least one RecordBatch after the number of
                // "joinable_lines_length" lines are read.
                let joinable_lines_length =
                    (TEST_DATA_SIZE as f64 * TEST_JOIN_RATIO).round() as usize;
                // The row including "a" is joinable with aggregate_test_100.c1
                let joinable_iterator =
                    (0..joinable_lines_length).map(|_| "a".to_string());
                let second_joinable_iterator =
                    (0..joinable_lines_length).map(|_| "a".to_string());
                // The row including "zzz" is not joinable with aggregate_test_100.c1
                let non_joinable_iterator = (0..(TEST_DATA_SIZE - joinable_lines_length))
                    .map(|_| "zzz".to_string());
                let string_array = joinable_iterator
                    .chain(non_joinable_iterator)
                    .chain(second_joinable_iterator);
                for (cnt, string_col) in enumerate(string_array) {
                    // Wait a reading sign for unbounded execution
                    // For unbounded execution:
                    //  After joinable_lines_length FIFO reading, we MUST get a Operation::Read.
                    // For bounded execution:
                    //  Never goes into while loop since waiting_thread initiated as false.
                    while *waiting_thread.lock().unwrap() && joinable_lines_length < cnt {
                        thread::sleep(Duration::from_millis(200));
                    }
                    // Each thread queues a message in the channel
                    if cnt % TEST_BATCH_SIZE == 0 {
                        thread_tx.send(Operation::Write).unwrap();
                    }
                    let line = format!("{},{}\n", string_col, cnt).to_owned();
                    write_to_fifo(
                        &first_file,
                        &line,
                        execution_start,
                        broken_pipe_timeout,
                    )
                    .unwrap();
                }
            });
            // Collects operations from both writer and executor.
            let result_collector = thread::spawn(move || {
                let mut results = vec![];
                while let Ok(res) = rx.recv() {
                    results.push(res);
                }
                results
            });
            // Create an execution case with bounded or unbounded flag.
            let ctx = create_ctx(&fifo_path, unbounded_file).await?;
            // Execute the query
            let df = ctx.sql("SELECT t1.a2, t2.c1, t2.c4, t2.c5 FROM left as t1 JOIN right as t2 ON t1.a1 = t2.c1").await?;
            let mut stream = df.execute_stream().await?;
            while (stream.next().await).is_some() {
                *waiting.lock().unwrap() = false;
                tx.send(Operation::Read).unwrap();
            }
            fifo_writer.join().unwrap();
            drop(tx);
            let result = result_collector.join().unwrap();
            assert_eq!(interleave(&result), unbounded_file);
            Ok(())
        }
    }
}
