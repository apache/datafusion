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

//! PipelineChecker rule is ensures that a given plan can accommodate its
//! infinite sources, if there are any.
//!
use crate::error::Result;
use crate::physical_optimizer::PhysicalOptimizerRule;
use crate::physical_plan::aggregates::AggregateExec;
use crate::physical_plan::analyze::AnalyzeExec;
use crate::physical_plan::joins::{CrossJoinExec, HashJoinExec};
use crate::physical_plan::rewrite::TreeNodeRewritable;
use crate::physical_plan::sorts::sort::SortExec;
use crate::physical_plan::windows::WindowAggExec;
use crate::physical_plan::ExecutionPlan;
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

impl PhysicalOptimizerRule for PipelineChecker {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &SessionConfig,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        plan.transform_up(&{ |plan| Ok(Some(check_finiteness_requirements(plan)?)) })
    }

    fn name(&self) -> &str {
        "PipelineChecker"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

fn check_finiteness_requirements(
    plan: Arc<dyn ExecutionPlan>,
) -> Result<Arc<dyn ExecutionPlan>> {
    let plan_any = plan.as_any();
    if let Some(HashJoinExec {
        left,
        right,
        join_type,
        ..
    }) = plan_any.downcast_ref::<HashJoinExec>()
    {
        let left = left.unbounded_output();
        let right = right.unbounded_output();
        let breaking = left
            || (right
                && matches!(
                    join_type,
                    JoinType::Right | JoinType::Full | JoinType::RightAnti
                ));
        if breaking {
            Err(DataFusionError::Plan(format!(
                "Join Error: The join with cannot be executed. {}",
                if left && right {
                    "Currently, we do not support unbounded inputs on both sides."
                } else {
                    "Please consider a different type of join or sources."
                }
            )))
        } else {
            Ok(plan)
        }
    } else if let Some(SortExec { input, .. }) = plan_any.downcast_ref::<SortExec>() {
        if input.unbounded_output() {
            Err(DataFusionError::Plan(
                "Sort Error: Can not sort unbounded inputs.".to_string(),
            ))
        } else {
            Ok(plan)
        }
    } else if let Some(WindowAggExec { input, .. }) =
        plan_any.downcast_ref::<WindowAggExec>()
    {
        if input.unbounded_output() {
            Err(DataFusionError::Plan(
                "Window Error: Windowing is not currently support for unbounded inputs."
                    .to_string(),
            ))
        } else {
            Ok(plan)
        }
    } else if let Some(AggregateExec { input, .. }) =
        plan_any.downcast_ref::<AggregateExec>()
    {
        if input.unbounded_output() {
            Err(DataFusionError::Plan(
                "Aggregate Error: `GROUP BY` clause (including the more general GROUPING SET) is not supported for unbounded inputs.".to_string(),
            ))
        } else {
            Ok(plan)
        }
    } else if let Some(AnalyzeExec { input, .. }) = plan_any.downcast_ref::<AnalyzeExec>()
    {
        if input.unbounded_output() {
            Err(DataFusionError::Plan(
                "Analyze Error: Analysis is not supported for unbounded inputs"
                    .to_string(),
            ))
        } else {
            Ok(plan)
        }
    } else if let Some(CrossJoinExec { left, right, .. }) =
        plan_any.downcast_ref::<CrossJoinExec>()
    {
        if left.unbounded_output() || right.unbounded_output() {
            Err(DataFusionError::Plan(
                "Cross Join Error: Cross join is not supported for the unbounded inputs."
                    .to_string(),
            ))
        } else {
            Ok(plan)
        }
    } else {
        Ok(plan)
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    use crate::physical_optimizer::test_utils::{
        BinaryTestCase, QueryCase, SourceType, UnaryTestCase,
    };

    #[tokio::test]
    async fn test_hash_left_join_swap() -> Result<()> {
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
