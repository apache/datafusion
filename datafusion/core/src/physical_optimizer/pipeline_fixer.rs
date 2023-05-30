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

//! The [PipelineFixer] rule tries to modify a given plan so that it can
//! accommodate its infinite sources, if there are any. In other words,
//! it tries to obtain a runnable query (with the given infinite sources)
//! from an non-runnable query by transforming pipeline-breaking operations
//! to pipeline-friendly ones. If this can not be done, the rule emits a
//! diagnostic error message.
//!
use crate::config::ConfigOptions;
use crate::error::Result;
use crate::physical_optimizer::join_selection::swap_hash_join;
use crate::physical_optimizer::pipeline_checker::PipelineStatePropagator;
use crate::physical_optimizer::PhysicalOptimizerRule;
use crate::physical_plan::joins::{HashJoinExec, PartitionMode, SymmetricHashJoinExec};
use crate::physical_plan::ExecutionPlan;
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_common::DataFusionError;
use datafusion_expr::logical_plan::JoinType;

use std::sync::Arc;

/// The [`PipelineFixer`] rule tries to modify a given plan so that it can
/// accommodate its infinite sources, if there are any. If this is not
/// possible, the rule emits a diagnostic error message.
#[derive(Default)]
pub struct PipelineFixer {}

impl PipelineFixer {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
}
/// [`PipelineFixer`] subrules are functions of this type. Such functions take a
/// single [`PipelineStatePropagator`] argument, which stores state variables
/// indicating the unboundedness status of the current [`ExecutionPlan`] as
/// the `PipelineFixer` rule traverses the entire plan tree.
type PipelineFixerSubrule =
    dyn Fn(PipelineStatePropagator) -> Option<Result<PipelineStatePropagator>>;

impl PhysicalOptimizerRule for PipelineFixer {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let pipeline = PipelineStatePropagator::new(plan);
        let subrules: Vec<Box<PipelineFixerSubrule>> = vec![
            Box::new(hash_join_convert_symmetric_subrule),
            Box::new(hash_join_swap_subrule),
        ];
        let state = pipeline.transform_up(&|p| apply_subrules(p, &subrules))?;
        Ok(state.plan)
    }

    fn name(&self) -> &str {
        "PipelineFixer"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

/// This subrule checks if one can replace a hash join with a symmetric hash
/// join so that the pipeline does not break due to the join operation in
/// question. If possible, it makes this replacement; otherwise, it has no
/// effect.
fn hash_join_convert_symmetric_subrule(
    mut input: PipelineStatePropagator,
) -> Option<Result<PipelineStatePropagator>> {
    if let Some(hash_join) = input.plan.as_any().downcast_ref::<HashJoinExec>() {
        let ub_flags = &input.children_unbounded;
        let (left_unbounded, right_unbounded) = (ub_flags[0], ub_flags[1]);
        input.unbounded = left_unbounded || right_unbounded;
        let result = if left_unbounded && right_unbounded {
            SymmetricHashJoinExec::try_new(
                hash_join.left().clone(),
                hash_join.right().clone(),
                hash_join
                    .on()
                    .iter()
                    .map(|(l, r)| (l.clone(), r.clone()))
                    .collect(),
                hash_join.filter().cloned(),
                hash_join.join_type(),
                hash_join.null_equals_null(),
            )
            .map(|exec| {
                input.plan = Arc::new(exec) as _;
                input
            })
        } else {
            Ok(input)
        };
        Some(result)
    } else {
        None
    }
}

/// This subrule will swap build/probe sides of a hash join depending on whether its inputs
/// may produce an infinite stream of records. The rule ensures that the left (build) side
/// of the hash join always operates on an input stream that will produce a finite set of.
/// records If the left side can not be chosen to be "finite", the order stays the
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
    mut input: PipelineStatePropagator,
) -> Option<Result<PipelineStatePropagator>> {
    if let Some(hash_join) = input.plan.as_any().downcast_ref::<HashJoinExec>() {
        let ub_flags = &input.children_unbounded;
        let (left_unbounded, right_unbounded) = (ub_flags[0], ub_flags[1]);
        input.unbounded = left_unbounded || right_unbounded;
        let result = if left_unbounded
            && !right_unbounded
            && matches!(
                *hash_join.join_type(),
                JoinType::Inner
                    | JoinType::Left
                    | JoinType::LeftSemi
                    | JoinType::LeftAnti
            ) {
            swap(hash_join).map(|plan| {
                input.plan = plan;
                input
            })
        } else {
            Ok(input)
        };
        Some(result)
    } else {
        None
    }
}

/// This function swaps sides of a hash join to make it runnable even if one of its
/// inputs are infinite. Note that this is not always possible; i.e. [JoinType::Full],
/// [JoinType::Right], [JoinType::RightAnti] and [JoinType::RightSemi] can not run with
/// an unbounded left side, even if we swap. Therefore, we do not consider them here.
fn swap(hash_join: &HashJoinExec) -> Result<Arc<dyn ExecutionPlan>> {
    let partition_mode = hash_join.partition_mode();
    let join_type = hash_join.join_type();
    match (*partition_mode, *join_type) {
        (
            _,
            JoinType::Right | JoinType::RightSemi | JoinType::RightAnti | JoinType::Full,
        ) => Err(DataFusionError::Internal(format!(
            "{join_type} join cannot be swapped for unbounded input."
        ))),
        (PartitionMode::Partitioned, _) => {
            swap_hash_join(hash_join, PartitionMode::Partitioned)
        }
        (PartitionMode::CollectLeft, _) => {
            swap_hash_join(hash_join, PartitionMode::CollectLeft)
        }
        (PartitionMode::Auto, _) => Err(DataFusionError::Internal(
            "Auto is not acceptable for unbounded input here.".to_string(),
        )),
    }
}

fn apply_subrules(
    mut input: PipelineStatePropagator,
    subrules: &Vec<Box<PipelineFixerSubrule>>,
) -> Result<Transformed<PipelineStatePropagator>> {
    for subrule in subrules {
        if let Some(value) = subrule(input.clone()).transpose()? {
            input = value;
        }
    }
    let is_unbounded = input
        .plan
        .unbounded_output(&input.children_unbounded)
        // Treat the case where an operator can not run on unbounded data as
        // if it can and it outputs unbounded data. Do not raise an error yet.
        // Such operators may be fixed, adjusted or replaced later on during
        // optimization passes -- sorts may be removed, windows may be adjusted
        // etc. If this doesn't happen, the final `PipelineChecker` rule will
        // catch this and raise an error anyway.
        .unwrap_or(true);
    input.unbounded = is_unbounded;
    Ok(Transformed::Yes(input))
}

#[cfg(test)]
mod util_tests {
    use datafusion_expr::Operator;
    use datafusion_physical_expr::expressions::{BinaryExpr, Column, NegativeExpr};
    use datafusion_physical_expr::intervals::check_support;
    use datafusion_physical_expr::PhysicalExpr;
    use std::sync::Arc;

    #[test]
    fn check_expr_supported() {
        let supported_expr = Arc::new(BinaryExpr::new(
            Arc::new(Column::new("a", 0)),
            Operator::Plus,
            Arc::new(Column::new("a", 0)),
        )) as Arc<dyn PhysicalExpr>;
        assert!(check_support(&supported_expr));
        let supported_expr_2 = Arc::new(Column::new("a", 0)) as Arc<dyn PhysicalExpr>;
        assert!(check_support(&supported_expr_2));
        let unsupported_expr = Arc::new(BinaryExpr::new(
            Arc::new(Column::new("a", 0)),
            Operator::Or,
            Arc::new(Column::new("a", 0)),
        )) as Arc<dyn PhysicalExpr>;
        assert!(!check_support(&unsupported_expr));
        let unsupported_expr_2 = Arc::new(BinaryExpr::new(
            Arc::new(Column::new("a", 0)),
            Operator::Or,
            Arc::new(NegativeExpr::new(Arc::new(Column::new("a", 0)))),
        )) as Arc<dyn PhysicalExpr>;
        assert!(!check_support(&unsupported_expr_2));
    }
}

#[cfg(test)]
mod hash_join_tests {
    use super::*;
    use crate::physical_optimizer::join_selection::swap_join_type;
    use crate::physical_optimizer::test_utils::SourceType;
    use crate::physical_plan::expressions::Column;
    use crate::physical_plan::joins::PartitionMode;
    use crate::physical_plan::projection::ProjectionExec;
    use crate::test_util::UnboundedExec;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion_common::utils::DataPtr;
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

    async fn test_join_with_maybe_swap_unbounded_case(t: TestCase) -> Result<()> {
        let left_unbounded = t.initial_sources_unbounded.0 == SourceType::Unbounded;
        let right_unbounded = t.initial_sources_unbounded.1 == SourceType::Unbounded;
        let left_exec = Arc::new(UnboundedExec::new(
            (!left_unbounded).then_some(1),
            RecordBatch::new_empty(Arc::new(Schema::new(vec![Field::new(
                "a",
                DataType::Int32,
                false,
            )]))),
            2,
        )) as Arc<dyn ExecutionPlan>;
        let right_exec = Arc::new(UnboundedExec::new(
            (!right_unbounded).then_some(1),
            RecordBatch::new_empty(Arc::new(Schema::new(vec![Field::new(
                "b",
                DataType::Int32,
                false,
            )]))),
            2,
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
            false,
        )?;

        let initial_hash_join_state = PipelineStatePropagator {
            plan: Arc::new(join),
            unbounded: false,
            children_unbounded: vec![left_unbounded, right_unbounded],
        };
        let optimized_hash_join =
            hash_join_swap_subrule(initial_hash_join_state).unwrap()?;
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
            let left_changed = Arc::data_ptr_eq(left, &right_exec);
            let right_changed = Arc::data_ptr_eq(right, &left_exec);
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
}
