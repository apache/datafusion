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

use crate::joins::hash_join::partitioned_hash_eval::HashExpr;
use crate::joins::PartitionMode;
use crate::ExecutionPlan;
use crate::ExecutionPlanProperties;

use ahash::RandomState;
use datafusion_common::{Result, ScalarValue};
use datafusion_expr::Operator;
use datafusion_physical_expr::expressions::{
    lit, BinaryExpr, CaseExpr, DynamicFilterPhysicalExpr,
};
use datafusion_physical_expr::{PhysicalExpr, PhysicalExprRef};

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
    /// Min/max bounds for each join key column in this partition.
    /// Index corresponds to the join key expression index.
    column_bounds: Vec<ColumnBounds>,
}

impl PartitionBounds {
    pub(crate) fn new(column_bounds: Vec<ColumnBounds>) -> Self {
        Self { column_bounds }
    }

    pub(crate) fn get_column_bounds(&self, index: usize) -> Option<&ColumnBounds> {
        self.column_bounds.get(index)
    }
}

/// Creates a bounds predicate from partition bounds.
///
/// Returns a bound predicate (col >= min AND col <= max) for all key columns in the ON expression that have computed bounds from the build phase.
///
/// Returns `None` if no column bounds are available.
fn create_bounds_predicate(
    on_right: &[PhysicalExprRef],
    bounds: &PartitionBounds,
) -> Option<Arc<dyn PhysicalExpr>> {
    let mut column_predicates = Vec::new();

    for (col_idx, right_expr) in on_right.iter().enumerate() {
        if let Some(column_bounds) = bounds.get_column_bounds(col_idx) {
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
            let range_expr = Arc::new(BinaryExpr::new(min_expr, Operator::And, max_expr))
                as Arc<dyn PhysicalExpr>;
            column_predicates.push(range_expr);
        }
    }

    if column_predicates.is_empty() {
        None
    } else {
        Some(
            column_predicates
                .into_iter()
                .reduce(|acc, pred| {
                    Arc::new(BinaryExpr::new(acc, Operator::And, pred))
                        as Arc<dyn PhysicalExpr>
                })
                .unwrap(),
        )
    }
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
}

#[derive(Clone)]
pub(crate) enum PartitionBuildDataReport {
    Partitioned {
        partition_id: usize,
        /// Bounds computed from this partition's build side.
        /// If the partition is empty (no rows) this will be None.
        bounds: Option<PartitionBounds>,
    },
    CollectLeft {
        /// Bounds computed from the collected build side.
        /// If the build side is empty (no rows) this will be None.
        bounds: Option<PartitionBounds>,
    },
}

#[derive(Clone)]
struct PartitionedBuildData {
    partition_id: usize,
    bounds: PartitionBounds,
}

#[derive(Clone)]
struct CollectLeftBuildData {
    bounds: PartitionBounds,
}

/// Build-side data organized by partition mode
enum AccumulatedBuildData {
    Partitioned {
        partitions: Vec<Option<PartitionedBuildData>>,
    },
    CollectLeft {
        data: Option<CollectLeftBuildData>,
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
        }
    }

    /// Report build-side data from a partition
    ///
    /// This unified method handles both CollectLeft and Partitioned modes. When all partitions
    /// have reported (barrier wait), the leader builds the appropriate filter expression:
    /// - CollectLeft: Simple conjunction of bounds and membership check
    /// - Partitioned: CASE expression routing to per-partition filters
    ///
    /// # Arguments
    /// * `data` - Build data including hash map, pushdown strategy, and bounds
    ///
    /// # Returns
    /// * `Result<()>` - Ok if successful, Err if filter update failed or mode mismatch
    pub(crate) async fn report_build_data(
        &self,
        data: PartitionBuildDataReport,
    ) -> Result<()> {
        // Store data in the accumulator
        {
            let mut guard = self.inner.lock();

            match (data, &mut *guard) {
                // Partitioned mode
                (
                    PartitionBuildDataReport::Partitioned {
                        partition_id,
                        bounds,
                    },
                    AccumulatedBuildData::Partitioned { partitions },
                ) => {
                    if let Some(bounds) = bounds {
                        partitions[partition_id] = Some(PartitionedBuildData {
                            partition_id,
                            bounds,
                        });
                    }
                }
                // CollectLeft mode (store once, deduplicate across partitions)
                (
                    PartitionBuildDataReport::CollectLeft { bounds },
                    AccumulatedBuildData::CollectLeft { data },
                ) => {
                    match (bounds, data) {
                        (None, _) | (_, Some(_)) => {
                            // No bounds reported or already reported; do nothing
                        }
                        (Some(new_bounds), data) => {
                            // First report, store the bounds
                            *data = Some(CollectLeftBuildData { bounds: new_bounds });
                        }
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
        if self.barrier.wait().await.is_leader() {
            // All partitions have reported, so we can create and update the filter
            let inner = self.inner.lock();

            match &*inner {
                // CollectLeft: Simple conjunction of bounds and membership check
                AccumulatedBuildData::CollectLeft { data } => {
                    if let Some(partition_data) = data {
                        // Create bounds check expression (if bounds available)
                        let Some(filter_expr) = create_bounds_predicate(
                            &self.on_right,
                            &partition_data.bounds,
                        ) else {
                            // No bounds available, nothing to update
                            return Ok(());
                        };

                        self.dynamic_filter.update(filter_expr)?;
                    }
                }
                // Partitioned: CASE expression routing to per-partition filters
                AccumulatedBuildData::Partitioned { partitions } => {
                    // Collect all partition data, skipping empty partitions
                    let partition_data: Vec<_> =
                        partitions.iter().filter_map(|p| p.as_ref()).collect();

                    if partition_data.is_empty() {
                        // All partitions are empty: no rows can match, skip the probe side entirely
                        self.dynamic_filter.update(lit(false))?;
                        return Ok(());
                    }

                    // Build a CASE expression that combines range checks AND membership checks
                    // CASE (hash_repartition(join_keys) % num_partitions)
                    //   WHEN 0 THEN (col >= min_0 AND col <= max_0 AND ...)
                    //   WHEN 1 THEN (col >= min_1 AND col <= max_1 AND ...)
                    //   ...
                    //   ELSE false
                    // END

                    let num_partitions = partitions.len();

                    // Create base expression: hash_repartition(join_keys) % num_partitions
                    let routing_hash_expr = Arc::new(HashExpr::new(
                        self.on_right.clone(),
                        self.repartition_random_state.clone(),
                        "hash_repartition".to_string(),
                    ))
                        as Arc<dyn PhysicalExpr>;

                    let modulo_expr = Arc::new(BinaryExpr::new(
                        routing_hash_expr,
                        Operator::Modulo,
                        lit(ScalarValue::UInt64(Some(num_partitions as u64))),
                    )) as Arc<dyn PhysicalExpr>;

                    // Create WHEN branches for each partition
                    let when_then_branches: Vec<(
                        Arc<dyn PhysicalExpr>,
                        Arc<dyn PhysicalExpr>,
                    )> = partition_data
                        .into_iter()
                        .map(|pdata| -> Result<_> {
                            // WHEN partition_id
                            let when_expr =
                                lit(ScalarValue::UInt64(Some(pdata.partition_id as u64)));

                            // Create bounds check expression for this partition (if bounds available)
                            let bounds_expr =
                                create_bounds_predicate(&self.on_right, &pdata.bounds)
                                    .unwrap_or_else(|| lit(true)); // No bounds means all rows pass

                            Ok((when_expr, bounds_expr))
                        })
                        .collect::<Result<Vec<_>>>()?;

                    let case_expr = Arc::new(CaseExpr::try_new(
                        Some(modulo_expr),
                        when_then_branches,
                        Some(lit(false)), // ELSE false
                    )?) as Arc<dyn PhysicalExpr>;

                    self.dynamic_filter.update(case_expr)?;
                }
            }
            self.dynamic_filter.mark_complete();
        }

        Ok(())
    }
}

impl fmt::Debug for SharedBuildAccumulator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SharedBuildAccumulator")
    }
}
