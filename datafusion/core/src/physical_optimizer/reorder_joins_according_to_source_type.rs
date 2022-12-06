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

//! Select the proper PartitionMode and build side based on whether the input is unbounded or not.
use std::sync::Arc;

use datafusion_common::DataFusionError;

use crate::execution::context::SessionConfig;
use crate::logical_expr::JoinType;
use crate::physical_plan::joins::{HashJoinExec, PartitionMode};
use crate::physical_plan::ExecutionPlan;

use super::optimizer::PhysicalOptimizerRule;
use crate::error::Result;
use crate::physical_optimizer::join_selection::{supports_swap, swap_hash_join};
use crate::physical_plan::rewrite::TreeNodeRewritable;

/// ReorderJoinsSourceType rule will reorder the build and probe phase of the hash joins
/// based on the source type.
/// The rule enforces the order such that the left (build) side of the join is the bounded.
/// If the left side can not enforced to be bounded, the order stays the same as the original query.
/// '''
///
//  	 Apply side changes according to the side's data source
//
//
//
//           +--------------+              +--------------+
//           |              |  unbounded   |              |
//    Left   | Infinite     |    true      | Hash         |\true
//           | Data source  |--------------| Repartition  | \   +--------------+       +--------------+
//           |              |              |              |  \  |              |       |              |
//           +--------------+              +--------------+   - |  Hash Join   |-------| Projection   |
//                                                            - |              |       |              |
//           +--------------+              +--------------+  /  +--------------+       +--------------+
//           |              |  unbounded   |              | /
//    Right  | Finite       |    false     | Hash         |/false
//           | Data Source  |--------------| Repartition  |
//           |              |              |              |
//           +--------------+              +--------------+
//
//
//
//           +--------------+              +--------------+
//           |              |  unbounded   |              |
//    Left   | Finite       |    false     | Hash         |\false
//           | Data source  |--------------| Repartition  | \   +--------------+       +--------------+
//           |              |              |              |  \  |              | true  |              | true
//           +--------------+              +--------------+   - |  Hash Join   |-------| Projection   |-----
//                                                            - |              |       |              |
//           +--------------+              +--------------+  /  +--------------+       +--------------+
//           |              |  unbounded   |              | /
//    Right  | Infinite     |    true      | Hash         |/true
//           | Data Source  |--------------| Repartition  |
//           |              |              |              |
//           +--------------+              +--------------+
//
/// '''
#[derive(Default)]
pub struct ReorderJoinsSourceType {}

impl ReorderJoinsSourceType {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self {}
    }
}
/// A hash join can support a continuous stream from right side. This function provides alignment
/// information about sides.
fn must_swap_join_order_to_adjust_unbounded_side(
    left: &dyn ExecutionPlan,
    right: &dyn ExecutionPlan,
) -> bool {
    match (left.unbounded_output(), right.unbounded_output()) {
        (true, false) => true,
        (_, _) => false,
    }
}
/// For [JoinType::Full], it is always unable to run [PartitionMode::CollectLeft] mode and will
/// return None.
/// For [JoinType::Left] and [JoinType::LeftAnti], can not run [PartitionMode::CollectLeft] mode,
/// should swap join type to [JoinType::Right] and [JoinType::RightAnti].
/// Even if we cannot prevent rejecting in next PipelineChecker rule, we apply change anyway,
/// i.e for [JoinType::Full]. Normally, it is currently impossible to execute a [JoinType::Full]
/// query with unbounded side.
fn swap(hash_join: &HashJoinExec) -> Result<Arc<dyn ExecutionPlan>> {
    let partition_mode = hash_join.partition_mode();
    let join_type = hash_join.join_type();
    match (*partition_mode, *join_type) {
        (PartitionMode::Partitioned, _) => {
            swap_hash_join(hash_join, PartitionMode::Partitioned)
        }
        (
            PartitionMode::CollectLeft,
            JoinType::LeftSemi | JoinType::RightSemi | JoinType::Inner,
        ) => swap_hash_join(hash_join, PartitionMode::CollectLeft),
        (PartitionMode::CollectLeft, JoinType::Left | JoinType::LeftAnti) => {
            swap_hash_join(hash_join, PartitionMode::CollectLeft)
        }
        (
            PartitionMode::CollectLeft,
            JoinType::Right | JoinType::RightAnti | JoinType::Full,
        ) => swap_hash_join(hash_join, PartitionMode::Partitioned),
        (PartitionMode::Auto, _) => Err(DataFusionError::Internal(
            "Auto is not acceptable here.".to_string(),
        )),
    }
}

impl PhysicalOptimizerRule for ReorderJoinsSourceType {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _session_config: &SessionConfig,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        plan.transform_up(&|plan| {
            if let Some(hash_join) = plan.as_any().downcast_ref::<HashJoinExec>() {
                let left = hash_join.left();
                let right = hash_join.right();
                match must_swap_join_order_to_adjust_unbounded_side(&**left, &**right) {
                    true => {
                        if supports_swap(*hash_join.join_type()) {
                            Ok(Some(swap(hash_join)?))
                        } else {
                            Ok(None)
                        }
                    }
                    false => Ok(Some(Arc::new(HashJoinExec::try_new(
                        Arc::clone(left),
                        Arc::clone(right),
                        hash_join.on().to_vec(),
                        hash_join.filter().cloned(),
                        hash_join.join_type(),
                        *hash_join.partition_mode(),
                        hash_join.null_equals_null(),
                    )?))),
                }
            } else {
                Ok(None)
            }
        })
    }

    fn name(&self) -> &str {
        "ReorderJoinsSourceType"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::physical_optimizer::join_selection::swap_join_type;
    use crate::physical_optimizer::test_utils::SourceType;
    use crate::physical_plan::expressions::Column;
    use crate::physical_plan::projection::ProjectionExec;
    use crate::{
        physical_plan::joins::PartitionMode, prelude::SessionConfig,
        test::exec::UnboundableExec,
    };
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
    async fn test_join_with_swap_full() {
        let mut cases = vec![];
        // !!! We cannot have some initial conditions after join order selection. Full join always
        // come with partitioned mode.
        cases.push(TestCase {
            case: "Case4".to_string(),
            initial_sources_unbounded: (SourceType::Bounded, SourceType::Unbounded),
            initial_join_type: JoinType::Full,
            initial_mode: PartitionMode::Partitioned,
            expected_sources_unbounded: (SourceType::Bounded, SourceType::Unbounded),
            expected_join_type: JoinType::Full,
            expected_mode: PartitionMode::Partitioned,
            expecting_swap: false,
        });
        cases.push(TestCase {
            case: "Case3".to_string(),
            initial_sources_unbounded: (SourceType::Unbounded, SourceType::Bounded),
            initial_join_type: JoinType::Full,
            initial_mode: PartitionMode::Partitioned,
            expected_sources_unbounded: (SourceType::Bounded, SourceType::Unbounded),
            expected_join_type: JoinType::Full,
            expected_mode: PartitionMode::Partitioned,
            expecting_swap: true,
        });

        cases.push(TestCase {
            case: "Case5".to_string(),
            initial_sources_unbounded: (SourceType::Bounded, SourceType::Bounded),
            initial_join_type: JoinType::Full,
            initial_mode: PartitionMode::Partitioned,
            expected_sources_unbounded: (SourceType::Bounded, SourceType::Bounded),
            expected_join_type: JoinType::Full,
            expected_mode: PartitionMode::Partitioned,
            expecting_swap: false,
        });
        cases.push(TestCase {
            case: "Case6".to_string(),
            initial_sources_unbounded: (SourceType::Unbounded, SourceType::Unbounded),
            initial_join_type: JoinType::Full,
            initial_mode: PartitionMode::Partitioned,
            expected_sources_unbounded: (SourceType::Unbounded, SourceType::Unbounded),
            expected_join_type: JoinType::Full,
            expected_mode: PartitionMode::Partitioned,
            expecting_swap: false,
        });
        for case in cases.into_iter() {
            test_join_with_maybe_swap_unbounded_case(case).await
        }
    }

    #[tokio::test]
    async fn test_cases_without_collect_left_check() {
        let mut cases = vec![];
        let join_types = vec![JoinType::LeftSemi, JoinType::RightSemi, JoinType::Inner];
        for join_type in join_types {
            cases.push(TestCase {
                case: "Case 1".to_string(),
                initial_sources_unbounded: (SourceType::Unbounded, SourceType::Bounded),
                initial_join_type: join_type,
                initial_mode: PartitionMode::CollectLeft,
                expected_sources_unbounded: (SourceType::Bounded, SourceType::Unbounded),
                expected_join_type: swap_join_type(join_type),
                expected_mode: PartitionMode::CollectLeft,
                expecting_swap: true,
            });
            cases.push(TestCase {
                case: "Case 2".to_string(),
                initial_sources_unbounded: (SourceType::Bounded, SourceType::Unbounded),
                initial_join_type: join_type,
                initial_mode: PartitionMode::CollectLeft,
                expected_sources_unbounded: (SourceType::Bounded, SourceType::Unbounded),
                expected_join_type: join_type,
                expected_mode: PartitionMode::CollectLeft,
                expecting_swap: false,
            });
            cases.push(TestCase {
                case: "Case 3".to_string(),
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
                case: "Case 4".to_string(),
                initial_sources_unbounded: (SourceType::Bounded, SourceType::Bounded),
                initial_join_type: join_type,
                initial_mode: PartitionMode::CollectLeft,
                expected_sources_unbounded: (SourceType::Bounded, SourceType::Bounded),
                expected_join_type: join_type,
                expected_mode: PartitionMode::CollectLeft,
                expecting_swap: false,
            });
            cases.push(TestCase {
                case: "Case 5".to_string(),
                initial_sources_unbounded: (SourceType::Unbounded, SourceType::Bounded),
                initial_join_type: join_type,
                initial_mode: PartitionMode::Partitioned,
                expected_sources_unbounded: (SourceType::Bounded, SourceType::Unbounded),
                expected_join_type: swap_join_type(join_type),
                expected_mode: PartitionMode::Partitioned,
                expecting_swap: true,
            });
            cases.push(TestCase {
                case: "Case 6".to_string(),
                initial_sources_unbounded: (SourceType::Bounded, SourceType::Unbounded),
                initial_join_type: join_type,
                initial_mode: PartitionMode::Partitioned,
                expected_sources_unbounded: (SourceType::Bounded, SourceType::Unbounded),
                expected_join_type: join_type,
                expected_mode: PartitionMode::Partitioned,
                expecting_swap: false,
            });
            cases.push(TestCase {
                case: "Case 7".to_string(),
                initial_sources_unbounded: (SourceType::Bounded, SourceType::Bounded),
                initial_join_type: join_type,
                initial_mode: PartitionMode::Partitioned,
                expected_sources_unbounded: (SourceType::Bounded, SourceType::Bounded),
                expected_join_type: join_type,
                expected_mode: PartitionMode::Partitioned,
                expecting_swap: false,
            });
            cases.push(TestCase {
                case: "Case 8".to_string(),
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
            test_join_with_maybe_swap_unbounded_case(case).await
        }
    }

    #[tokio::test]
    async fn test_not_support_collect_left() {
        let mut cases = vec![];
        // After [JoinSelection] optimization, these join types cannot run in CollectLeft mode.
        let the_ones_not_support_collect_left = vec![JoinType::Left, JoinType::LeftAnti];
        for join_type in the_ones_not_support_collect_left {
            cases.push(TestCase {
                case: "Case: 5".to_string(),
                initial_sources_unbounded: (SourceType::Unbounded, SourceType::Bounded),
                initial_join_type: join_type,
                initial_mode: PartitionMode::Partitioned,
                expected_sources_unbounded: (SourceType::Bounded, SourceType::Unbounded),
                expected_join_type: swap_join_type(join_type),
                expected_mode: PartitionMode::Partitioned,
                expecting_swap: true,
            });
            cases.push(TestCase {
                case: "Case: 6".to_string(),
                initial_sources_unbounded: (SourceType::Bounded, SourceType::Unbounded),
                initial_join_type: join_type,
                initial_mode: PartitionMode::Partitioned,
                expected_sources_unbounded: (SourceType::Bounded, SourceType::Unbounded),
                expected_join_type: join_type,
                expected_mode: PartitionMode::Partitioned,
                expecting_swap: false,
            });
            cases.push(TestCase {
                case: "Case: 7".to_string(),
                initial_sources_unbounded: (SourceType::Bounded, SourceType::Bounded),
                initial_join_type: join_type,
                initial_mode: PartitionMode::Partitioned,
                expected_sources_unbounded: (SourceType::Bounded, SourceType::Bounded),
                expected_join_type: join_type,
                expected_mode: PartitionMode::Partitioned,
                expecting_swap: false,
            });
            cases.push(TestCase {
                case: "Case: 8".to_string(),
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
            test_join_with_maybe_swap_unbounded_case(case).await
        }
    }

    #[tokio::test]
    async fn test_must_collect_left_swap_makes_partitioned() {
        let mut cases = vec![];
        let the_ones_not_support_collect_left =
            vec![JoinType::Right, JoinType::RightAnti];
        for join_type in the_ones_not_support_collect_left {
            // We expect that (SourceType::Unbounded, SourceType::Bounded) will change, regardless of the
            // statistics.
            cases.push(TestCase {
                case: "Case: 1".to_string(),
                initial_sources_unbounded: (SourceType::Unbounded, SourceType::Bounded),
                initial_join_type: join_type,
                initial_mode: PartitionMode::CollectLeft,
                expected_sources_unbounded: (SourceType::Bounded, SourceType::Unbounded),
                expected_join_type: swap_join_type(join_type),
                expected_mode: PartitionMode::Partitioned,
                expecting_swap: true,
            });
            // We expect that (SourceType::Bounded, SourceType::Unbounded) will stay same, regardless of the
            // statistics.
            cases.push(TestCase {
                case: "Case: 2".to_string(),
                initial_sources_unbounded: (SourceType::Bounded, SourceType::Unbounded),
                initial_join_type: join_type,
                initial_mode: PartitionMode::CollectLeft,
                expected_sources_unbounded: (SourceType::Bounded, SourceType::Unbounded),
                expected_join_type: join_type,
                expected_mode: PartitionMode::CollectLeft,
                expecting_swap: false,
            });
            cases.push(TestCase {
                case: "Case: 3".to_string(),
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
                case: "Case: 4".to_string(),
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
                case: "Case: 5".to_string(),
                initial_sources_unbounded: (SourceType::Unbounded, SourceType::Bounded),
                initial_join_type: join_type,
                initial_mode: PartitionMode::Partitioned,
                expected_sources_unbounded: (SourceType::Bounded, SourceType::Unbounded),
                expected_join_type: swap_join_type(join_type),
                expected_mode: PartitionMode::Partitioned,
                expecting_swap: true,
            });
            cases.push(TestCase {
                case: "Case: 6".to_string(),
                initial_sources_unbounded: (SourceType::Bounded, SourceType::Unbounded),
                initial_join_type: join_type,
                initial_mode: PartitionMode::Partitioned,
                expected_sources_unbounded: (SourceType::Bounded, SourceType::Unbounded),
                expected_join_type: join_type,
                expected_mode: PartitionMode::Partitioned,
                expecting_swap: false,
            });
            cases.push(TestCase {
                case: "Case: 7".to_string(),
                initial_sources_unbounded: (SourceType::Bounded, SourceType::Bounded),
                initial_join_type: join_type,
                initial_mode: PartitionMode::Partitioned,
                expected_sources_unbounded: (SourceType::Bounded, SourceType::Bounded),
                expected_join_type: join_type,
                expected_mode: PartitionMode::Partitioned,
                expecting_swap: false,
            });
            cases.push(TestCase {
                case: "Case: 8".to_string(),
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
            test_join_with_maybe_swap_unbounded_case(case).await
        }
    }
    #[allow(clippy::vtable_address_comparisons)]
    async fn test_join_with_maybe_swap_unbounded_case(t: TestCase) {
        let left_exec = Arc::new(UnboundableExec::new(
            t.initial_sources_unbounded.0 == SourceType::Unbounded,
            Schema::new(vec![Field::new("a", DataType::Int32, false)]),
        )) as Arc<dyn ExecutionPlan>;
        let right_exec = Arc::new(UnboundableExec::new(
            t.initial_sources_unbounded.1 == SourceType::Unbounded,
            Schema::new(vec![Field::new("b", DataType::Int32, false)]),
        )) as Arc<dyn ExecutionPlan>;

        let join = HashJoinExec::try_new(
            Arc::clone(&left_exec),
            Arc::clone(&right_exec),
            vec![(
                Column::new_with_schema("a", &left_exec.schema()).unwrap(),
                Column::new_with_schema("b", &right_exec.schema()).unwrap(),
            )],
            None,
            &t.initial_join_type,
            t.initial_mode,
            &false,
        )
        .unwrap();

        let optimized_join = ReorderJoinsSourceType::new()
            .optimize(Arc::new(join), &SessionConfig::new())
            .unwrap();

        // If swap did not happened
        let projection_added = optimized_join.as_any().is::<ProjectionExec>();
        let plan = if projection_added {
            let proj = optimized_join
                .as_any()
                .downcast_ref::<ProjectionExec>()
                .expect(
                    "A proj is required to swap columns back to their original order",
                );
            proj.input().clone()
        } else {
            optimized_join
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
                    if left.unbounded_output() {
                        SourceType::Unbounded
                    } else {
                        SourceType::Bounded
                    },
                    if right.unbounded_output() {
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
        }
    }
    #[cfg(not(target_os = "windows"))]
    mod unix_test {
        use crate::prelude::SessionConfig;
        use crate::{
            prelude::{CsvReadOptions, SessionContext},
            test_util::{aggr_test_schema, arrow_test_data},
        };
        use arrow::datatypes::{DataType, Field, Schema};
        use datafusion_common::Result;
        use futures::StreamExt;
        use nix::sys::stat;
        use nix::unistd;
        use rand::seq::SliceRandom;
        use rand::thread_rng;
        use std::fs::OpenOptions;
        use std::io::Write;
        use std::sync::Arc;
        use std::thread;
        use std::time::{Duration, Instant};
        use tempfile::TempDir;

        #[tokio::test]
        async fn unbounded_file_with_swapped_join() -> Result<()> {
            // Create a new temporary FIFO file
            let tmp_dir = TempDir::new().unwrap();
            let file_path = tmp_dir.path().join("my_fifo.csv");
            let writer_path = file_path.clone();
            let path_string = file_path.clone().into_os_string().into_string().unwrap();
            // Timeout for a long period of BrokenPipe error
            let timeout = Duration::from_secs(10);
            // Simulate an infinite environment
            let record_count = 20_000;
            // Create FIFO file
            unistd::mkfifo(&file_path, stat::Mode::S_IRWXU).unwrap();
            // Spawn a new thread to write to the FIFO file
            let writer = thread::spawn(move || {
                let mut file = OpenOptions::new()
                    .write(true)
                    .open(writer_path)
                    .expect("Failed to open FIFO file");
                // Create a vector of characters
                let idx = vec!["b", "z", "t", "g", "x"];
                // Initialize the counter
                let mut cnt = 1;
                // Get a reference to the thread-local random number generator
                let mut rng = thread_rng();
                // Sync timeout
                let now = Instant::now();
                loop {
                    // Choose a random element from the vector
                    let chosen_idx = *idx
                        .choose(&mut rng)
                        .expect("Failed to select a random number");
                    // Line
                    match file
                        .write(format!("{},{}\n", chosen_idx, cnt).to_owned().as_bytes())
                    {
                        Ok(_) => {}
                        Err(e) => {
                            // Broken Pipe error
                            if e.raw_os_error().unwrap() == 32 {
                                if Instant::now().duration_since(now) > timeout {
                                    panic!("Cannot read the FIFO file.")
                                }
                                thread::sleep(Duration::from_millis(100));
                            } else {
                                panic!("{}", e.to_string())
                            }
                        }
                    }
                    cnt += 1;
                    // Finalize the writing
                    if cnt > record_count {
                        break;
                    }
                }
            });

            let config = SessionConfig::new();
            let ctx = SessionContext::with_config(config);
            // Register left table
            let left_schema = Arc::new(Schema::new(vec![
                Field::new("a1", DataType::Utf8, false),
                Field::new("a2", DataType::UInt32, false),
            ]));
            ctx.register_csv(
                "left",
                path_string.as_str(),
                CsvReadOptions::new()
                    .schema(left_schema.as_ref())
                    .has_header(false)
                    .mark_infinite(true),
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
            // execute the query
            let df = ctx.sql("SELECT t1.a2, t2.c1, t2.c4, t2.c5 FROM left as t1 JOIN right as t2 ON t1.a1 = t2.c1").await?;
            let mut stream = df.execute_stream().await?;
            while let Some(_result) = stream.next().await {
                {}
            }
            writer
                .join()
                .expect("Normally, it will not join since it is infinite.");
            Ok(())
        }
    }
}
