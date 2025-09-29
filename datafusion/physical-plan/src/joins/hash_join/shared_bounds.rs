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

//! Utilities for shared bounds. Used in dynamic filter pushdown in Hash Joins.
// TODO: include the link to the Dynamic Filter blog post.

use std::fmt;
use std::sync::Arc;

use crate::joins::PartitionMode;
use crate::ExecutionPlan;
use crate::ExecutionPlanProperties;

use datafusion_common::{Result, ScalarValue};
use datafusion_expr::Operator;
use datafusion_physical_expr::expressions::{lit, BinaryExpr, DynamicFilterPhysicalExpr};
use datafusion_physical_expr::{PhysicalExpr, PhysicalExprRef};

use itertools::Itertools;
use parking_lot::Mutex;
use tokio::sync::Barrier;

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
pub(crate) struct PartitionBounds {
    /// Partition identifier for debugging and determinism (not strictly necessary)
    partition: usize,
    /// Min/max bounds for each join key column in this partition.
    /// Index corresponds to the join key expression index.
    column_bounds: Vec<ColumnBounds>,
}

impl PartitionBounds {
    pub(crate) fn new(partition: usize, column_bounds: Vec<ColumnBounds>) -> Self {
        Self {
            partition,
            column_bounds,
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.column_bounds.len()
    }

    pub(crate) fn get_column_bounds(&self, index: usize) -> Option<&ColumnBounds> {
        self.column_bounds.get(index)
    }
}

/// Coordinates dynamic filter bounds collection across multiple partitions
///
/// This structure ensures that dynamic filters are built with complete information from all
/// relevant partitions before being applied to probe-side scans. Incomplete filters would
/// incorrectly eliminate valid join results.
///
/// ## Synchronization Strategy
///
/// 1. Each partition computes bounds from its build-side data
/// 2. Bounds are stored in the shared vector
/// 3. A barrier tracks how many partitions have reported their bounds
/// 4. When the last partition reports, bounds are merged and the filter is updated exactly once
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
pub(crate) struct SharedBoundsAccumulator {
    /// Shared state protected by a single mutex to avoid ordering concerns
    inner: Mutex<SharedBoundsState>,
    barrier: Barrier,
    /// Dynamic filter for pushdown to probe side
    dynamic_filter: Arc<DynamicFilterPhysicalExpr>,
    /// Right side join expressions needed for creating filter bounds
    on_right: Vec<PhysicalExprRef>,
}

/// State protected by SharedBoundsAccumulator's mutex
struct SharedBoundsState {
    /// Bounds from completed partitions.
    /// Each element represents the column bounds computed by one partition.
    bounds: Vec<PartitionBounds>,
}

impl SharedBoundsAccumulator {
    /// Creates a new SharedBoundsAccumulator configured for the given partition mode
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
    ///   the actual mode will be determined and a new bounds_accumulator created before execution.
    ///
    /// ## Why This Matters
    ///
    /// We cannot build a partial filter from some partitions - it would incorrectly eliminate
    /// valid join results. We must wait until we have complete bounds information from ALL
    /// relevant partitions before updating the dynamic filter.
    pub(crate) fn new_from_partition_mode(
        partition_mode: PartitionMode,
        left_child: &dyn ExecutionPlan,
        right_child: &dyn ExecutionPlan,
        dynamic_filter: Arc<DynamicFilterPhysicalExpr>,
        on_right: Vec<PhysicalExprRef>,
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
        Self {
            inner: Mutex::new(SharedBoundsState {
                bounds: Vec::with_capacity(expected_calls),
            }),
            barrier: Barrier::new(expected_calls),
            dynamic_filter,
            on_right,
        }
    }

    /// Create a filter expression from individual partition bounds using OR logic.
    ///
    /// This creates a filter where each partition's bounds form a conjunction (AND)
    /// of column range predicates, and all partitions are combined with OR.
    ///
    /// For example, with 2 partitions and 2 columns:
    /// ((col0 >= p0_min0 AND col0 <= p0_max0 AND col1 >= p0_min1 AND col1 <= p0_max1)
    ///  OR
    ///  (col0 >= p1_min0 AND col0 <= p1_max0 AND col1 >= p1_min1 AND col1 <= p1_max1))
    pub(crate) fn create_filter_from_partition_bounds(
        &self,
        bounds: &[PartitionBounds],
    ) -> Result<Arc<dyn PhysicalExpr>> {
        if bounds.is_empty() {
            return Ok(lit(true));
        }

        // Create a predicate for each partition
        let mut partition_predicates = Vec::with_capacity(bounds.len());

        for partition_bounds in bounds.iter().sorted_by_key(|b| b.partition) {
            // Create range predicates for each join key in this partition
            let mut column_predicates = Vec::with_capacity(partition_bounds.len());

            for (col_idx, right_expr) in self.on_right.iter().enumerate() {
                if let Some(column_bounds) = partition_bounds.get_column_bounds(col_idx) {
                    // Create predicate: col >= min AND col <= max
                    let min_expr = Arc::new(BinaryExpr::new(
                        Arc::clone(right_expr),
                        Operator::GtEq,
                        lit(column_bounds.min.clone()),
                    )) as Arc<dyn PhysicalExpr>;
                    let max_expr = Arc::new(BinaryExpr::new(
                        Arc::clone(right_expr),
                        Operator::LtEq,
                        lit(column_bounds.max.clone()),
                    )) as Arc<dyn PhysicalExpr>;
                    let range_expr =
                        Arc::new(BinaryExpr::new(min_expr, Operator::And, max_expr))
                            as Arc<dyn PhysicalExpr>;
                    column_predicates.push(range_expr);
                }
            }

            // Combine all column predicates for this partition with AND
            if !column_predicates.is_empty() {
                let partition_predicate = column_predicates
                    .into_iter()
                    .reduce(|acc, pred| {
                        Arc::new(BinaryExpr::new(acc, Operator::And, pred))
                            as Arc<dyn PhysicalExpr>
                    })
                    .unwrap();
                partition_predicates.push(partition_predicate);
            }
        }

        // Combine all partition predicates with OR
        let combined_predicate = partition_predicates
            .into_iter()
            .reduce(|acc, pred| {
                Arc::new(BinaryExpr::new(acc, Operator::Or, pred))
                    as Arc<dyn PhysicalExpr>
            })
            .unwrap_or_else(|| lit(true));

        Ok(combined_predicate)
    }

    /// Report bounds from a completed partition and update dynamic filter if all partitions are done
    ///
    /// This method coordinates the dynamic filter updates across all partitions. It stores the
    /// bounds from the current partition, increments the completion counter, and when all
    /// partitions have reported, creates an OR'd filter from individual partition bounds.
    ///
    /// This method is async and uses a [`tokio::sync::Barrier`] to wait for all partitions
    /// to report their bounds. Once that occurs, the method will resolve for all callers and the
    /// dynamic filter will be updated exactly once.
    ///
    /// # Note
    ///
    /// As barriers are reusable, it is likely an error to call this method more times than the
    /// total number of partitions - as it can lead to pending futures that never resolve. We rely
    /// on correct usage from the caller rather than imposing additional checks here. If this is a concern,
    /// consider making the resulting future shared so the ready result can be reused.
    ///
    /// # Arguments
    /// * `left_side_partition_id` - The identifier for the **left-side** partition reporting its bounds
    /// * `partition_bounds` - The bounds computed by this partition (if any)
    ///
    /// # Returns
    /// * `Result<()>` - Ok if successful, Err if filter update failed
    pub(crate) async fn report_partition_bounds(
        &self,
        left_side_partition_id: usize,
        partition_bounds: Option<Vec<ColumnBounds>>,
    ) -> Result<()> {
        // Store bounds in the accumulator - this runs once per partition
        if let Some(bounds) = partition_bounds {
            let mut guard = self.inner.lock();

            let should_push = if let Some(last_bound) = guard.bounds.last() {
                // In `PartitionMode::CollectLeft`, all streams on the left side share the same partition id (0).
                // Since this function can be called multiple times for that same partition, we must deduplicate
                // by checking against the last recorded bound.
                last_bound.partition != left_side_partition_id
            } else {
                true
            };

            if should_push {
                guard
                    .bounds
                    .push(PartitionBounds::new(left_side_partition_id, bounds));
            }
        }

        if self.barrier.wait().await.is_leader() {
            // All partitions have reported, so we can update the filter
            let inner = self.inner.lock();
            if !inner.bounds.is_empty() {
                let filter_expr =
                    self.create_filter_from_partition_bounds(&inner.bounds)?;
                self.dynamic_filter.update(filter_expr)?;
            }
        }

        Ok(())
    }
}

impl fmt::Debug for SharedBoundsAccumulator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SharedBoundsAccumulator")
    }
}
