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

use arrow::datatypes::{DataType, Field};
use datafusion_common::config::ConfigOptions;
use datafusion_common::{Result, ScalarValue};
use datafusion_expr::Operator;
use datafusion_expr::ScalarUDF;
use datafusion_functions::hash::Hash;
use datafusion_physical_expr::expressions::{lit, BinaryExpr, DynamicFilterPhysicalExpr};
use datafusion_physical_expr::{PhysicalExpr, PhysicalExprRef, ScalarFunctionExpr};

use ahash::RandomState;
use itertools::Itertools;
use parking_lot::Mutex;
use std::collections::HashSet;

/// RandomState used by RepartitionExec for consistent hash partitioning
/// This must match the seeds used in RepartitionExec to ensure our hash-based
/// filter expressions compute the same partition assignments as the actual partitioning
const REPARTITION_RANDOM_STATE: RandomState = RandomState::with_seeds(0, 0, 0, 0);

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

/// Coordinates dynamic filter bounds collection across multiple partitions with progressive filtering
///
/// This structure applies dynamic filters progressively as each partition completes, rather than
/// waiting for all partitions. This reduces probe-side scan overhead and improves performance.
///
/// ## Progressive Filtering Strategy
///
/// 1. Each partition computes bounds from its build-side data and immediately injects a filter
/// 2. Filters use hash-based expressions to avoid false negatives:
///    `(hash(cols) % num_partitions != partition_id OR col >= min AND col <= max)`
/// 3. As partitions complete, their specific bounds are added to the combined filter
/// 4. When all partitions complete, hash checks are removed for optimization
///
/// ## Filter Expression Evolution
///
/// **Progressive Phase**: `(hash(cols) % n != 0 OR bounds_0) AND (hash(cols) % n != 1 OR bounds_1) AND ...`
/// **Final Phase**: `bounds_0 OR bounds_1 OR bounds_2 OR ...`
///
/// ## Thread Safety
///
/// All fields use a single mutex to ensure correct coordination between concurrent
/// partition executions.
pub(crate) struct SharedBoundsAccumulator {
    /// Shared state protected by a single mutex to avoid ordering concerns
    inner: Mutex<SharedBoundsState>,
    /// Total number of partitions expected to report
    total_partitions: usize,
    /// Dynamic filter for pushdown to probe side
    dynamic_filter: Arc<DynamicFilterPhysicalExpr>,
    /// Right side join expressions needed for creating filter bounds
    on_right: Vec<PhysicalExprRef>,
    /// Cached ConfigOptions to avoid repeated allocations
    config_options: Arc<ConfigOptions>,
}

/// State protected by SharedBoundsAccumulator's mutex
struct SharedBoundsState {
    /// Bounds from completed partitions.
    /// Each element represents the column bounds computed by one partition.
    bounds: Vec<PartitionBounds>,
    /// Set of partition IDs that have reported their bounds
    completed_partitions: HashSet<usize>,
    /// Whether we've optimized the filter to remove hash checks
    filter_optimized: bool,
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
        let total_partitions = match partition_mode {
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
                bounds: Vec::with_capacity(total_partitions),
                completed_partitions: HashSet::new(),
                filter_optimized: false,
            }),
            total_partitions,
            dynamic_filter,
            on_right,
            config_options: Arc::new(ConfigOptions::default()),
        }
    }

    /// Create hash expression for the join keys: hash(col1, col2, ...)
    fn create_hash_expression(&self) -> Result<Arc<dyn PhysicalExpr>> {
        // Use the same random state as RepartitionExec for consistent partitioning
        // This ensures hash(row) % num_partitions produces the same partition assignment
        // as the original repartitioning operation
        let hash_udf = Arc::new(ScalarUDF::from(Hash::new_with_random_state(
            REPARTITION_RANDOM_STATE,
        )));

        // Create the hash expression using ScalarFunctionExpr
        Ok(Arc::new(ScalarFunctionExpr::new(
            "hash",
            hash_udf,
            self.on_right.clone(),
            Field::new("hash_result", DataType::UInt64, false).into(),
            Arc::clone(&self.config_options),
        )))
    }

    /// Create a bounds predicate for a single partition: (col >= min AND col <= max) for all columns
    fn create_partition_bounds_predicate(
        &self,
        partition_bounds: &PartitionBounds,
    ) -> Result<Arc<dyn PhysicalExpr>> {
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
        if column_predicates.is_empty() {
            Ok(lit(true))
        } else {
            let predicate = column_predicates
                .into_iter()
                .reduce(|acc, pred| {
                    Arc::new(BinaryExpr::new(acc, Operator::And, pred))
                        as Arc<dyn PhysicalExpr>
                })
                .unwrap();
            Ok(predicate)
        }
    }

    /// Create progressive filter: (hash(...) % n != partition_id OR bounds_predicate)
    fn create_progressive_partition_filter(
        &self,
        partition_bounds: &PartitionBounds,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        let hash_expr = self.create_hash_expression()?;

        // hash(...) % total_partitions
        let modulo_expr = Arc::new(BinaryExpr::new(
            hash_expr,
            Operator::Modulo,
            lit(ScalarValue::UInt64(Some(self.total_partitions as u64))),
        )) as Arc<dyn PhysicalExpr>;

        // hash(...) % total_partitions != partition_id
        let hash_check = Arc::new(BinaryExpr::new(
            modulo_expr,
            Operator::NotEq,
            lit(ScalarValue::UInt64(Some(partition_bounds.partition as u64))),
        )) as Arc<dyn PhysicalExpr>;

        // Create bounds predicate for this partition
        let bounds_predicate =
            self.create_partition_bounds_predicate(partition_bounds)?;

        // Combine: (hash_check OR bounds_predicate)
        Ok(
            Arc::new(BinaryExpr::new(hash_check, Operator::Or, bounds_predicate))
                as Arc<dyn PhysicalExpr>,
        )
    }

    /// Create final optimized filter from all partition bounds using OR logic
    pub(crate) fn create_optimized_filter_from_partition_bounds(
        &self,
        bounds: &[PartitionBounds],
    ) -> Result<Arc<dyn PhysicalExpr>> {
        if bounds.is_empty() {
            return Ok(lit(true));
        }

        // Create a predicate for each partition
        let mut partition_predicates = Vec::with_capacity(bounds.len());

        for partition_bounds in bounds.iter().sorted_by_key(|b| b.partition) {
            let bounds_predicate =
                self.create_partition_bounds_predicate(partition_bounds)?;
            partition_predicates.push(bounds_predicate);
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

    /// Create progressive filter from completed partitions using AND logic
    pub(crate) fn create_progressive_filter_from_partition_bounds(
        &self,
        bounds: &[PartitionBounds],
    ) -> Result<Arc<dyn PhysicalExpr>> {
        if bounds.is_empty() {
            return Ok(lit(true));
        }

        // Create a progressive filter for each completed partition
        let mut partition_filters = Vec::with_capacity(bounds.len());

        for partition_bounds in bounds.iter().sorted_by_key(|b| b.partition) {
            let progressive_filter =
                self.create_progressive_partition_filter(partition_bounds)?;
            partition_filters.push(progressive_filter);
        }

        // Combine all partition filters with AND
        let combined_filter = partition_filters
            .into_iter()
            .reduce(|acc, filter| {
                Arc::new(BinaryExpr::new(acc, Operator::And, filter))
                    as Arc<dyn PhysicalExpr>
            })
            .unwrap_or_else(|| lit(true));

        Ok(combined_filter)
    }

    /// Report bounds from a completed partition and immediately update the dynamic filter
    ///
    /// This method applies progressive filtering by immediately injecting a filter for each
    /// completed partition. The filter uses hash-based expressions to ensure correctness:
    /// `(hash(cols) % num_partitions != partition_id OR col >= min AND col <= max)`
    ///
    /// When all partitions have completed, the filter is optimized to remove hash checks.
    ///
    /// # Arguments
    /// * `left_side_partition_id` - The identifier for the **left-side** partition reporting its bounds
    /// * `partition_bounds` - The bounds computed by this partition (if any)
    ///
    /// # Returns
    /// * `Result<()>` - Ok if successful, Err if filter update failed
    pub(crate) fn report_partition_bounds(
        &self,
        left_side_partition_id: usize,
        partition_bounds: Option<Vec<ColumnBounds>>,
    ) -> Result<()> {
        let mut inner = self.inner.lock();

        // Skip processing if this partition has already reported its bounds to prevent duplicate updates
        if inner.completed_partitions.contains(&left_side_partition_id) {
            return Ok(());
        }

        // Store bounds and mark partition as completed
        if let Some(bounds) = partition_bounds {
            let should_push = if let Some(last_bound) = inner.bounds.last() {
                // In `PartitionMode::CollectLeft`, all streams on the left side share the same partition id (0).
                // Since this function can be called multiple times for that same partition, we must deduplicate
                // by checking against the last recorded bound.
                last_bound.partition != left_side_partition_id
            } else {
                true
            };

            if should_push {
                inner
                    .bounds
                    .push(PartitionBounds::new(left_side_partition_id, bounds));
            }
        }
        inner.completed_partitions.insert(left_side_partition_id);

        let all_partitions_complete =
            inner.completed_partitions.len() == self.total_partitions;

        // Create the appropriate filter based on completion status
        let filter_expr = if all_partitions_complete && !inner.filter_optimized {
            // All partitions complete - use optimized filter without hash checks
            inner.filter_optimized = true;
            self.create_optimized_filter_from_partition_bounds(&inner.bounds)?
        } else {
            // Progressive phase - use hash-based filter
            self.create_progressive_filter_from_partition_bounds(&inner.bounds)?
        };

        // Release lock before updating filter to avoid holding it during the update
        drop(inner);

        // Update the dynamic filter
        self.dynamic_filter.update(filter_expr)?;

        Ok(())
    }
}

impl fmt::Debug for SharedBoundsAccumulator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SharedBoundsAccumulator")
    }
}
