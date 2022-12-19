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
use log::warn;
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

/// ReorderUnboundedJoins rule will reorder the build and probe sides of a hash
/// join depending on whether its inputs may produce an infinite stream of records.
/// The rule ensure that the left (build) side of the join always operates on an
/// input stream that will produce a finite set of records.
/// If the left side can not be chosen to be "finite", the order stays the same as
/// the original query.
/// ```text
///
//  	 For example, this rule makes the following transformation:
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
/// ```
#[derive(Default)]
pub struct ReorderUnboundedJoins {}

impl ReorderUnboundedJoins {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        Self {}
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
        (
            PartitionMode::CollectLeft,
            JoinType::Left | JoinType::LeftAnti | JoinType::Full,
        ) => {
            warn!("Warning: Left, LeftAnti and Full joins were not supported in CollectLeft mode, but it seems like they are now. Double check the code and remove this warning.");
            swap_hash_join(hash_join, PartitionMode::Partitioned)
        }
        (PartitionMode::CollectLeft, JoinType::Right | JoinType::RightAnti) => {
            swap_hash_join(hash_join, PartitionMode::Partitioned)
        }
        (PartitionMode::Auto, _) => Err(DataFusionError::Internal(
            "Auto is not acceptable here.".to_string(),
        )),
    }
}

impl PhysicalOptimizerRule for ReorderUnboundedJoins {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &SessionConfig,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        plan.transform_up(&|plan| {
            Ok(
                if let Some(hash_join) = plan.as_any().downcast_ref::<HashJoinExec>() {
                    let left = hash_join.left();
                    let right = hash_join.right();
                    if left.unbounded_output() && !right.unbounded_output() {
                        if supports_swap(*hash_join.join_type()) {
                            Some(swap(hash_join)?)
                        } else {
                            None
                        }
                    } else {
                        Some(Arc::new(HashJoinExec::try_new(
                            Arc::clone(left),
                            Arc::clone(right),
                            hash_join.on().to_vec(),
                            hash_join.filter().cloned(),
                            hash_join.join_type(),
                            *hash_join.partition_mode(),
                            hash_join.null_equals_null(),
                        )?))
                    }
                } else {
                    None
                },
            )
        })
    }

    fn name(&self) -> &str {
        "ReorderUnboundedJoins"
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
        test::exec::UnboundedExec,
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
        // NOTE: Currently, some initial conditions are not viable after join order selection.
        //       For example, full join always comes in partitioned mode. See the warning in
        //       function "swap". If this changes in the future, we should update these tests.
        cases.push(TestCase {
            case: "Bounded - Unbounded".to_string(),
            initial_sources_unbounded: (SourceType::Bounded, SourceType::Unbounded),
            initial_join_type: JoinType::Full,
            initial_mode: PartitionMode::Partitioned,
            expected_sources_unbounded: (SourceType::Bounded, SourceType::Unbounded),
            expected_join_type: JoinType::Full,
            expected_mode: PartitionMode::Partitioned,
            expecting_swap: false,
        });
        cases.push(TestCase {
            case: "Unbounded - Bounded".to_string(),
            initial_sources_unbounded: (SourceType::Unbounded, SourceType::Bounded),
            initial_join_type: JoinType::Full,
            initial_mode: PartitionMode::Partitioned,
            expected_sources_unbounded: (SourceType::Bounded, SourceType::Unbounded),
            expected_join_type: JoinType::Full,
            expected_mode: PartitionMode::Partitioned,
            expecting_swap: true,
        });

        cases.push(TestCase {
            case: "Bounded - Bounded".to_string(),
            initial_sources_unbounded: (SourceType::Bounded, SourceType::Bounded),
            initial_join_type: JoinType::Full,
            initial_mode: PartitionMode::Partitioned,
            expected_sources_unbounded: (SourceType::Bounded, SourceType::Bounded),
            expected_join_type: JoinType::Full,
            expected_mode: PartitionMode::Partitioned,
            expecting_swap: false,
        });
        cases.push(TestCase {
            case: "Unbounded - Unbounded".to_string(),
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
                case: "Unbounded - Bounded / CollectLeft".to_string(),
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
            test_join_with_maybe_swap_unbounded_case(case).await
        }
    }
    #[allow(clippy::vtable_address_comparisons)]
    async fn test_join_with_maybe_swap_unbounded_case(t: TestCase) {
        let left_exec = Arc::new(UnboundedExec::new(
            t.initial_sources_unbounded.0 == SourceType::Unbounded,
            Schema::new(vec![Field::new("a", DataType::Int32, false)]),
        )) as Arc<dyn ExecutionPlan>;
        let right_exec = Arc::new(UnboundedExec::new(
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

        let optimized_join = ReorderUnboundedJoins::new()
            .optimize(Arc::new(join), &SessionConfig::new())
            .unwrap();

        // If swap did not happen
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
        use datafusion_common::{DataFusionError, Result};
        use futures::StreamExt;
        use nix::sys::stat;
        use nix::unistd;
        use rand::seq::SliceRandom;
        use rand::thread_rng;
        use rstest::*;
        use std::fs::{File, OpenOptions};
        use std::io::Write;
        use std::path::PathBuf;
        use std::sync::mpsc;
        use std::sync::mpsc::{Receiver, Sender};
        use std::sync::{Arc, Mutex};
        use std::thread;
        use std::time::{Duration, Instant};
        use tempfile::TempDir;
        // Session batch size
        const TEST_BATCH_SIZE: u64 = 20;
        // Number of lines written to FIFO
        const TEST_DATA_SIZE: u64 = 20_000;

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
            fifo_path: &PathBuf,
            with_unbounded_execution: bool,
        ) -> Result<SessionContext> {
            let config = SessionConfig::new()
                .with_batch_size(TEST_BATCH_SIZE as usize)
                .set_u64(
                    "datafusion.execution.coalesce_target_batch_size",
                    TEST_BATCH_SIZE,
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
                // Create a vector of characters
                let chars = vec!["b", "z", "t", "g", "x"];
                // Get a reference to the thread-local random number generator
                let mut rng = thread_rng();
                // Reference time to use when deciding to fail the test
                let execution_start = Instant::now();
                for cnt in 1..TEST_DATA_SIZE {
                    // Each thread queues a message in the channel
                    if cnt % TEST_BATCH_SIZE == 0 {
                        thread_tx.send(Operation::Write).unwrap();
                    }
                    // Choose a random element from the vector
                    let chosen_idx = *chars.choose(&mut rng).unwrap();
                    let line = format!("{},{}\n", chosen_idx, cnt).to_owned();
                    write_to_fifo(
                        &first_file,
                        &line,
                        execution_start,
                        broken_pipe_timeout,
                    )
                    .unwrap();
                }
                while *waiting_thread.lock().unwrap() {
                    thread::sleep(Duration::from_millis(200));
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
