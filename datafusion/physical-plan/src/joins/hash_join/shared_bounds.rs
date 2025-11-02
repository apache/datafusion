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

//! Utilities for shared build-side information. Used in dynamic filter pushdown in Hash Joins.
// TODO: include the link to the Dynamic Filter blog post.

use std::fmt;
use std::sync::Arc;

use crate::joins::utils::JoinHashMapType;
use crate::joins::PartitionMode;
use crate::ExecutionPlan;
use crate::ExecutionPlanProperties;

use ahash::RandomState;
use arrow::array::ArrayRef;
use arrow::datatypes::Schema;
use datafusion_common::{Result, ScalarValue};
use datafusion_physical_expr::expressions::DynamicFilterPhysicalExpr;
use datafusion_physical_expr::PhysicalExprRef;

use parking_lot::Mutex;
use tokio::sync::Barrier;

/// Hash join seed - must match the one used when building hash tables
#[allow(dead_code)] // Will be used in future PR for filter pushdown
const HASH_JOIN_SEED: RandomState =
    RandomState::with_seeds('J' as u64, 'O' as u64, 'I' as u64, 'N' as u64);

/// Represents the minimum and maximum values for a specific column.
/// Used in dynamic filter pushdown to establish value boundaries.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ColumnBounds {
    /// The minimum value observed for this column
    min: ScalarValue,
    /// The maximum value observed for this column
    max: ScalarValue,
}

impl ColumnBounds {
    pub(crate) fn new(min: ScalarValue, max: ScalarValue) -> Self {
        Self { min, max }
    }
}

/// Represents the bounds for all join key columns from a single partition.
/// This contains the min/max values computed from one partition's build-side data.
#[derive(Debug, Clone)]
#[allow(dead_code)] // Will be used in future PR for filter pushdown
pub(crate) struct PartitionBounds {
    /// Min/max bounds for each join key column in this partition.
    /// Index corresponds to the join key expression index.
    column_bounds: Vec<ColumnBounds>,
}

#[allow(dead_code)] // Will be used in future PR for filter pushdown
impl PartitionBounds {
    pub(crate) fn new(column_bounds: Vec<ColumnBounds>) -> Self {
        Self { column_bounds }
    }

    pub(crate) fn get_column_bounds(&self, index: usize) -> Option<&ColumnBounds> {
        self.column_bounds.get(index)
    }
}

/// Creates a membership predicate for filter pushdown.
///
/// STUB: This is a placeholder for future filter pushdown functionality.
/// In the full implementation, this would create InList or HashTableLookup expressions.
#[allow(dead_code)] // Will be used in future PR for filter pushdown
fn create_membership_predicate(
    _on_right: &[PhysicalExprRef],
    pushdown: PushdownStrategy,
    _random_state: &RandomState,
) -> Result<Option<()>> {
    match pushdown {
        PushdownStrategy::Empty => Ok(None),
        _ => {
            // TODO: Implement in future PR when in_list_from_array is available
            Ok(None)
        }
    }
}

/// Creates a bounds predicate from partition bounds.
///
/// STUB: This is a placeholder for future filter pushdown functionality.
/// In the full implementation, this would create range predicates (col >= min AND col <= max).
#[allow(dead_code)] // Will be used in future PR for filter pushdown
fn create_bounds_predicate(
    _on_right: &[PhysicalExprRef],
    _bounds: &PartitionBounds,
) -> Option<()> {
    // TODO: Implement in future PR when expression building is available
    None
}

/// Coordinates build-side information collection across multiple partitions
///
/// This structure collects information from the build side (hash tables and/or bounds) and
/// ensures that dynamic filters are built with complete information from all relevant
/// partitions before being applied to probe-side scans. Incomplete filters would
/// incorrectly eliminate valid join results.
///
/// ## Synchronization Strategy
///
/// 1. Each partition computes information from its build-side data (hash maps and/or bounds)
/// 2. Information is stored in the shared state
/// 3. A barrier tracks how many partitions have reported
/// 4. When the last partition reports, information is merged and the filter is updated exactly once
///
/// ## Hash Map vs Bounds
///
/// - **Hash Maps (Partitioned mode)**: Collects Arc references to hash tables from each partition.
///   Creates a `PartitionedHashLookupPhysicalExpr` that routes rows to the correct partition's hash table.
/// - **Bounds (CollectLeft mode)**: Collects min/max bounds and creates range predicates.
///
/// ## Partition Counting
///
/// The `total_partitions` count represents how many times `collect_build_side` will be called:
/// - **CollectLeft**: Number of output partitions (each accesses shared build data)
/// - **Partitioned**: Number of input partitions (each builds independently)
///
/// ## Thread Safety
///
/// All fields use a single mutex to ensure correct coordination between concurrent
/// partition executions.
#[allow(dead_code)] // Fields will be used in future PR for filter pushdown
pub(crate) struct SharedBuildAccumulator {
    /// Build-side data protected by a single mutex to avoid ordering concerns
    inner: Mutex<AccumulatedBuildData>,
    barrier: Barrier,
    /// Dynamic filter for pushdown to probe side
    dynamic_filter: Arc<DynamicFilterPhysicalExpr>,
    /// Right side join expressions needed for creating filter expressions
    on_right: Vec<PhysicalExprRef>,
    /// Random state for partitioning (RepartitionExec's hash function with 0,0,0,0 seeds)
    /// Used for PartitionedHashLookupPhysicalExpr
    repartition_random_state: RandomState,
    /// Schema of the probe (right) side for evaluating filter expressions
    probe_schema: Arc<Schema>,
}

/// Strategy for filter pushdown (decided at collection time)
#[derive(Clone)]
#[allow(dead_code)] // Will be used in future PR for filter pushdown
pub(crate) enum PushdownStrategy {
    /// Use InList for small build sides (< 128MB)
    InList(ArrayRef),
    /// Use hash table lookup for large build sides
    HashTable(Arc<dyn JoinHashMapType>),
    /// There was no data in this partition, do not build a dynamic filter for it
    Empty,
}

/// Build-side data reported by a single partition
#[allow(dead_code)] // Will be used in future PR for filter pushdown
pub(crate) enum PartitionBuildData {
    Partitioned {
        partition_id: usize,
        pushdown: PushdownStrategy,
        bounds: PartitionBounds,
    },
    CollectLeft {
        pushdown: PushdownStrategy,
        bounds: PartitionBounds,
    },
}

/// Per-partition accumulated data (Partitioned mode)
#[derive(Clone)]
#[allow(dead_code)] // Used in report_build_data, which is currently stubbed
struct PartitionData {
    bounds: PartitionBounds,
    pushdown: PushdownStrategy,
}

/// Build-side data organized by partition mode
#[allow(dead_code)] // Fields used in report_build_data, which is currently stubbed
enum AccumulatedBuildData {
    Partitioned {
        partitions: Vec<Option<PartitionData>>,
    },
    CollectLeft {
        data: Option<PartitionData>,
    },
}

impl SharedBuildAccumulator {
    /// Creates a new SharedBuildAccumulator configured for the given partition mode
    ///
    /// This method calculates how many times `collect_build_side` will be called based on the
    /// partition mode's execution pattern. This count is critical for determining when we have
    /// complete information from all partitions to build the dynamic filter.
    ///
    /// ## Partition Mode Execution Patterns
    ///
    /// - **CollectLeft**: Build side is collected ONCE from partition 0 and shared via `OnceFut`
    ///   across all output partitions. Each output partition calls `collect_build_side` to access the shared build data.
    ///   Although this results in multiple invocations, the  `report_partition_bounds` function contains deduplication logic to handle them safely.
    ///   Expected calls = number of output partitions.
    ///
    ///
    /// - **Partitioned**: Each partition independently builds its own hash table by calling
    ///   `collect_build_side` once. Expected calls = number of build partitions.
    ///
    /// - **Auto**: Placeholder mode resolved during optimization. Uses 1 as safe default since
    ///   the actual mode will be determined and a new accumulator created before execution.
    ///
    /// ## Why This Matters
    ///
    /// We cannot build a partial filter from some partitions - it would incorrectly eliminate
    /// valid join results. We must wait until we have complete information from ALL
    /// relevant partitions before updating the dynamic filter.
    pub(crate) fn new_from_partition_mode(
        partition_mode: PartitionMode,
        left_child: &dyn ExecutionPlan,
        right_child: &dyn ExecutionPlan,
        dynamic_filter: Arc<DynamicFilterPhysicalExpr>,
        on_right: Vec<PhysicalExprRef>,
        repartition_random_state: RandomState,
    ) -> Self {
        // Troubleshooting: If partition counts are incorrect, verify this logic matches
        // the actual execution pattern in collect_build_side()
        let expected_calls = match partition_mode {
            // Each output partition accesses shared build data
            PartitionMode::CollectLeft => {
                right_child.output_partitioning().partition_count()
            }
            // Each partition builds its own data
            PartitionMode::Partitioned => {
                left_child.output_partitioning().partition_count()
            }
            // Default value, will be resolved during optimization (does not exist once `execute()` is called; will be replaced by one of the other two)
            PartitionMode::Auto => unreachable!("PartitionMode::Auto should not be present at execution time. This is a bug in DataFusion, please report it!"),
        };

        let mode_data = match partition_mode {
            PartitionMode::Partitioned => AccumulatedBuildData::Partitioned {
                partitions: vec![None; left_child.output_partitioning().partition_count()],
            },
            PartitionMode::CollectLeft => AccumulatedBuildData::CollectLeft {
                data: None,
            },
            PartitionMode::Auto => unreachable!("PartitionMode::Auto should not be present at execution time. This is a bug in DataFusion, please report it!"),
        };

        Self {
            inner: Mutex::new(mode_data),
            barrier: Barrier::new(expected_calls),
            dynamic_filter,
            on_right,
            repartition_random_state,
            probe_schema: right_child.schema(),
        }
    }

    /// Report build-side data from a partition
    ///
    /// STUB: This method is stubbed out for now as it depends on expression building
    /// functionality that will be added in a future PR.
    ///
    /// # Arguments
    /// * `data` - Build data including hash map, pushdown strategy, and bounds
    ///
    /// # Returns
    /// * `Result<()>` - Ok if successful, Err if filter update failed or mode mismatch
    #[allow(dead_code)] // Will be used in future PR for filter pushdown
    pub(crate) async fn report_build_data(&self, data: PartitionBuildData) -> Result<()> {
        // Store data in the accumulator
        {
            let mut guard = self.inner.lock();

            match (data, &mut *guard) {
                // Partitioned mode
                (
                    PartitionBuildData::Partitioned {
                        partition_id,
                        pushdown,
                        bounds,
                    },
                    AccumulatedBuildData::Partitioned { partitions },
                ) => {
                    partitions[partition_id] = Some(PartitionData { pushdown, bounds });
                }
                // CollectLeft mode (store once, deduplicate across partitions)
                (
                    PartitionBuildData::CollectLeft { pushdown, bounds },
                    AccumulatedBuildData::CollectLeft { data },
                ) => {
                    // Deduplicate - all partitions report the same data in CollectLeft
                    if data.is_none() {
                        *data = Some(PartitionData { pushdown, bounds });
                    }
                }
                // Mismatched modes - should never happen
                _ => {
                    return datafusion_common::internal_err!(
                        "Build data mode mismatch in report_build_data"
                    );
                }
            }
        }

        // Wait for all partitions to report
        self.barrier.wait().await;
        // TODO: Build and update dynamic filter in future PR when expression building is available
        // For now, we just collect the data without building filters (pure refactoring)

        Ok(())
    }

    /// Report partition bounds from a partition (compatibility wrapper)
    ///
    /// This method provides backwards compatibility for the old API.
    /// It wraps report_build_data with stub data.
    pub(crate) async fn report_partition_bounds(
        &self,
        _partition_id: usize,
        _bounds: Option<Vec<ColumnBounds>>,
    ) -> Result<()> {
        // TODO: This is a compatibility stub. In a future PR, this will be removed
        // and callers will use report_build_data directly.
        self.barrier.wait().await;
        Ok(())
    }
}

impl fmt::Debug for SharedBuildAccumulator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SharedBuildAccumulator")
    }
}
