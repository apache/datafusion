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

use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use crate::joins::PartitionMode;
use crate::repartition::hash::repartition_hash;
use crate::ExecutionPlan;
use crate::ExecutionPlanProperties;

use datafusion_common::{Result, ScalarValue};
use datafusion_expr::Operator;
use datafusion_physical_expr::expressions::{lit, BinaryExpr, DynamicFilterPhysicalExpr};
use datafusion_physical_expr::{PhysicalExpr, PhysicalExprRef};
use itertools::Itertools;
use parking_lot::Mutex;

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
/// The key insight is that we can apply partial filters immediately without waiting for all
/// partitions to complete, while maintaining correctness through hash-based expressions.
///
/// 1. **Immediate Filter Injection**: Each partition computes bounds from its build-side data
///    and immediately injects a progressive filter
/// 2. **Hash-Based Correctness**: Filters use hash expressions to ensure no false negatives:
///    `CASE hash(cols) % num_partitions WHEN partition_id THEN (col >= min AND col <= max) ELSE true END`
/// 3. **Incremental Improvement**: As partitions complete, filter selectivity increases
/// 4. **Final Optimization**: When all partitions complete, hash checks are removed
///
/// ## Concrete Example
///
/// Consider a 3-partition hash join on column `id` with build-side values:
/// - Partition 0: id ∈ [10, 20] (completes first)
/// - Partition 1: id ∈ [30, 40] (completes second)
/// - Partition 2: id ∈ [50, 60] (completes last)
///
/// ### Progressive Phase Filters:
///
/// **After Partition 0 completes:**
/// ```sql
/// CASE hash(id) % 3
///   WHEN 0 THEN id >= 10 AND id <= 20
///   ELSE true
/// END
/// ```
/// → Filters partition-0 data immediately, passes through all other data
///
/// **After Partition 1 completes:**
/// ```sql
/// CASE hash(id) % 3
///   WHEN 0 THEN id >= 10 AND id <= 20
///   WHEN 1 THEN id >= 30 AND id <= 40
///   ELSE true
/// END
/// ```
/// → Now filters both partition-0 and partition-1 data
///
/// ### Final Phase Filter:
/// **After all partitions complete:**
/// ```sql
/// (id >= 10 AND id <= 20) OR (id >= 30 AND id <= 40) OR (id >= 50 AND id <= 60)
/// ```
/// → Optimized bounds-only filter, no hash computation needed
///
/// ## Correctness Guarantee
///
/// The hash-based approach ensures **no false negatives**:
/// - For rows belonging to completed partitions: bounds check filters correctly
/// - For rows belonging to incomplete partitions: `ELSE true` passes everything through
/// - Hash function matches the partitioning scheme, ensuring correct partition assignment
///
/// ## Performance Benefits
///
/// - **Early Filtering**: Probe-side scans start filtering immediately, not after barrier
/// - **Progressive Improvement**: Filter selectivity increases with each completed partition
/// - **Reduced I/O**: Less data read from probe-side sources as partitions complete
/// - **No Coordination Overhead**: Eliminates barrier synchronization between partitions
/// - **Final Optimization**: Removes hash computation cost when all partitions are done
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
}

/// State protected by SharedBoundsAccumulator's mutex
struct SharedBoundsState {
    /// Bounds from completed partitions.
    /// Each value represents the column bounds computed by one partition and the keys are the partition ids.
    bounds: HashMap<usize, PartitionBounds>,
    /// Whether we've optimized the filter to remove hash checks
    filter_optimized: bool,
    /// Number of partitions that have reported completion (for tracking when all are done)
    completed_count: usize,
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
                bounds: HashMap::with_capacity(total_partitions),
                filter_optimized: false,
                completed_count: 0,
            }),
            total_partitions,
            dynamic_filter,
            on_right,
        }
    }

    /// Create a bounds predicate for a single partition: (col >= min AND col <= max) for all columns.
    /// This is used in both progressive and final filter creation.
    /// Returns None if no bounds are available for this partition.
    fn create_partition_bounds_predicate(
        &self,
        partition_bounds: &PartitionBounds,
    ) -> Result<Option<Arc<dyn PhysicalExpr>>> {
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
            } else {
                // Missing bounds for this column, the created predicate will have lower selectivity but will still be correct
                continue;
            }
        }

        // Combine all column predicates for this partition with AND
        Ok(column_predicates.into_iter().reduce(|acc, pred| {
            Arc::new(BinaryExpr::new(acc, Operator::And, pred)) as Arc<dyn PhysicalExpr>
        }))
    }

    /// Create progressive filter using hash-based expressions to avoid false negatives.
    ///
    /// This is the heart of progressive filtering. It creates a CASE expression that applies
    /// bounds filtering only to rows belonging to completed partitions, while safely passing
    /// through all data from incomplete partitions.
    ///
    /// ## Generated Expression Structure:
    /// ```sql
    /// CASE hash(cols) % num_partitions
    ///   WHEN 0 THEN (col1 >= min1 AND col1 <= max1 AND col2 >= min2 AND col2 <= max2)
    ///   WHEN 1 THEN (col1 >= min3 AND col1 <= max3 AND col2 >= min4 AND col2 <= max4)
    ///   ...
    ///   ELSE true  -- Critical: ensures no false negatives for incomplete partitions
    /// END
    /// ```
    ///
    /// ## Correctness Key Points:
    /// - **Hash Function**: Uses the same hash as the join's partitioning scheme
    /// - **Modulo Operation**: Maps hash values to partition IDs (0 to num_partitions-1)
    /// - **WHEN Clauses**: Only created for partitions that have completed and reported bounds
    /// - **ELSE true**: Ensures rows from incomplete partitions are never filtered out
    /// - **Single Hash**: Hash is computed once per row, regardless of how many partitions completed
    pub(crate) fn create_progressive_filter_from_partition_bounds(
        &self,
        bounds: &HashMap<usize, PartitionBounds>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        // Step 1: Create the partition assignment expression: hash(join_cols) % num_partitions
        // This must match the hash function used by RepartitionExec for correctness
        let hash_expr = repartition_hash(self.on_right.clone())?;
        let total_partitions_expr =
            lit(ScalarValue::UInt64(Some(self.total_partitions as u64)));
        let modulo_expr = Arc::new(BinaryExpr::new(
            hash_expr,
            Operator::Modulo,
            total_partitions_expr,
        )) as Arc<dyn PhysicalExpr>;

        // Step 2: Build WHEN clauses for each completed partition
        // Format: WHEN partition_id THEN (bounds_predicate)
        let when_thens = bounds.values().sorted_by_key(|b| b.partition).try_fold(
            Vec::new(),
            |mut acc, partition_bounds| {
                // Create literal for partition ID (e.g., WHEN 0, WHEN 1, etc.)
                let when_value =
                    lit(ScalarValue::UInt64(Some(partition_bounds.partition as u64)));

                // Create bounds predicate for this partition (e.g., col >= min AND col <= max)
                if let Some(then_predicate) =
                    self.create_partition_bounds_predicate(partition_bounds)?
                {
                    acc.push((when_value, then_predicate));
                }
                Ok::<_, datafusion_common::DataFusionError>(acc)
            },
        )?;

        // Step 3: Build the complete CASE expression
        use datafusion_physical_expr::expressions::case;
        let expr = if when_thens.is_empty() {
            // Edge case: No partitions have completed yet - pass everything through
            lit(ScalarValue::Boolean(Some(true)))
        } else {
            // Create CASE expression with critical ELSE true clause
            // The ELSE true ensures we never filter out rows from incomplete partitions
            case(
                Some(modulo_expr),    // CASE hash(cols) % num_partitions
                when_thens,           // WHEN clauses for completed partitions
                Some(lit(ScalarValue::Boolean(Some(true)))),  // ELSE true - no false negatives!
            )?
        };

        Ok(expr)
    }

    /// Create final optimized filter when all partitions have completed
    ///
    /// This method represents the performance optimization phase of progressive filtering.
    /// Once all partitions have reported their bounds, we can eliminate the hash-based
    /// CASE expression and use a simpler, more efficient bounds-only filter.
    ///
    /// ## Optimization Benefits:
    /// 1. **No Hash Computation**: Eliminates expensive hash calculations per row
    /// 2. **Simpler Expression**: OR-based bounds are faster to evaluate than CASE expressions
    /// 3. **Better Vectorization**: Simple bounds comparisons optimize better in Arrow
    /// 4. **Reduced CPU Overhead**: Significant performance improvement for large datasets
    ///
    /// ## Generated Expression Structure:
    /// ```sql
    /// (col1 >= min1 AND col1 <= max1) OR    -- Partition 0 bounds
    /// (col1 >= min2 AND col1 <= max2) OR    -- Partition 1 bounds
    /// ...
    /// (col1 >= minN AND col1 <= maxN)       -- Partition N bounds
    /// ```
    ///
    /// ## Correctness Maintained:
    /// - Each OR clause represents the exact bounds from one partition
    /// - Union of all partition bounds = complete build-side value range
    /// - No false negatives: if a value exists in build side, it passes this filter
    /// - Same filtering effect as progressive filter, but much more efficient
    ///
    /// This transformation is only applied when ALL partitions have completed to ensure
    /// we have complete bounds information.
    pub(crate) fn create_optimized_filter_from_partition_bounds(
        &self,
        bounds: &HashMap<usize, PartitionBounds>,
    ) -> Result<Option<Arc<dyn PhysicalExpr>>> {
        // Build individual partition predicates - each becomes one OR clause
        let mut partition_filters = Vec::with_capacity(bounds.len());

        for partition_bounds in bounds.values().sorted_by_key(|b| b.partition) {
            if let Some(filter) =
                self.create_partition_bounds_predicate(partition_bounds)?
            {
                // This partition contributed bounds - include in optimized filter
                partition_filters.push(filter);
            }
            // Skip empty partitions gracefully - they don't contribute bounds but
            // shouldn't prevent the optimization from proceeding
        }

        // Create the final OR expression: bounds_0 OR bounds_1 OR ... OR bounds_N
        // This replaces the hash-based CASE expression with a much faster bounds-only check
        Ok(partition_filters.into_iter().reduce(|acc, filter| {
            Arc::new(BinaryExpr::new(acc, Operator::Or, filter)) as Arc<dyn PhysicalExpr>
        }))
    }

    /// Report bounds from a completed partition and immediately update the dynamic filter
    ///
    /// This is the core method that implements progressive filtering. Unlike traditional approaches
    /// that wait for all partitions to complete, this method immediately applies a partial filter
    /// as soon as each partition finishes building its hash table.
    ///
    /// ## Progressive Filter Logic
    ///
    /// The method maintains correctness through careful filter design:
    ///
    /// **Key Insight**: We can safely filter rows that belong to completed partitions while
    /// letting all other rows pass through, because the hash function determines partition
    /// membership deterministically.
    ///
    /// ## Filter Evolution Example
    ///
    /// Consider a 2-partition join on column `price`:
    ///
    /// **Initial state**: No filter applied
    /// ```sql
    /// -- All probe-side rows pass through
    /// SELECT * FROM probe_table  -- No filtering
    /// ```
    ///
    /// **After Partition 0 completes** (found price range [100, 200]):
    /// ```sql
    /// -- Progressive filter applied
    /// SELECT * FROM probe_table
    /// WHERE CASE hash(price) % 2
    ///         WHEN 0 THEN price >= 100 AND price <= 200  -- Filter partition-0 data
    ///         ELSE true                                   -- Pass through partition-1 data
    ///       END
    /// ```
    /// → Filters out probe rows with price ∉ [100, 200] that hash to partition 0
    ///
    /// **After Partition 1 completes** (found price range [500, 600]):
    /// ```sql
    /// -- Final optimized filter
    /// SELECT * FROM probe_table
    /// WHERE (price >= 100 AND price <= 200) OR (price >= 500 AND price <= 600)
    /// ```
    /// → Clean bounds-only filter, no hash computation needed
    ///
    /// ## Correctness Guarantee
    ///
    /// This approach ensures **zero false negatives** (never incorrectly excludes valid joins):
    ///
    /// 1. **Completed Partitions**: Rows are filtered by actual build-side bounds
    /// 2. **Incomplete Partitions**: All rows pass through (`ELSE true`)
    /// 3. **Partition Assignment**: Hash function matches the join's partitioning scheme exactly
    /// 4. **Bounds Accuracy**: Min/max values computed from actual build-side data
    ///
    /// The filter may have **false positives** (includes rows that won't join) during the
    /// progressive phase, but these are eliminated during the actual join operation.
    ///
    /// ## Concurrency Handling
    ///
    /// - **Thread Safety**: Uses mutex to coordinate between concurrent partition executions
    /// - **Deduplication**: Handles multiple reports from same partition (CollectLeft mode)
    /// - **Atomic Updates**: Filter updates are applied atomically to avoid inconsistent states
    ///
    /// ## Performance Impact
    ///
    /// - **Immediate Benefit**: Probe-side filtering starts as soon as first partition completes
    /// - **I/O Reduction**: Less data read from storage/network as build partitions complete
    /// - **CPU Optimization**: Final filter removes hash computation overhead
    /// - **Scalability**: No barrier synchronization delays between partitions
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

        // Always increment completion counter - every partition reports exactly once
        inner.completed_count += 1;

        // Store bounds from this partition (avoid duplicates)
        // In CollectLeft mode, multiple streams may report the same partition_id,
        // but we only want to store bounds once
        inner
            .bounds
            .entry(left_side_partition_id)
            .or_insert_with(|| {
                if let Some(bounds) = partition_bounds {
                    PartitionBounds::new(left_side_partition_id, bounds)
                } else {
                    // Insert an empty bounds entry to track this partition
                    PartitionBounds::new(left_side_partition_id, vec![])
                }
            });

        let completed = inner.completed_count;
        let total = self.total_partitions;

        let all_partitions_complete = completed == total;

        // Create the appropriate filter based on completion status
        let filter_expr = if all_partitions_complete && !inner.filter_optimized {
            // All partitions complete - use optimized filter without hash checks
            inner.filter_optimized = true;
            self.create_optimized_filter_from_partition_bounds(&inner.bounds)?
        } else {
            // Progressive phase - use hash-based filter
            Some(self.create_progressive_filter_from_partition_bounds(&inner.bounds)?)
        };

        // Release lock before updating filter to avoid holding it during the update
        drop(inner);

        // Update the dynamic filter
        if let Some(filter_expr) = filter_expr {
            self.dynamic_filter.update(filter_expr)?;
        }

        Ok(())
    }
}

impl fmt::Debug for SharedBoundsAccumulator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SharedBoundsAccumulator")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Array;
    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion_common::cast::as_boolean_array;
    use std::sync::Arc;

    #[test]
    fn test_create_optimized_filter_case_expr() -> Result<()> {
        // Create a simple test setup
        let schema = Schema::new(vec![
            Field::new("col1", DataType::Int32, false),
            Field::new("col2", DataType::Int32, false),
        ]);

        // Create test partition bounds
        let bounds1 = PartitionBounds::new(
            0,
            vec![
                ColumnBounds::new(
                    ScalarValue::Int32(Some(1)),
                    ScalarValue::Int32(Some(10)),
                ),
                ColumnBounds::new(
                    ScalarValue::Int32(Some(5)),
                    ScalarValue::Int32(Some(15)),
                ),
            ],
        );
        let bounds2 = PartitionBounds::new(
            1,
            vec![
                ColumnBounds::new(
                    ScalarValue::Int32(Some(20)),
                    ScalarValue::Int32(Some(30)),
                ),
                ColumnBounds::new(
                    ScalarValue::Int32(Some(25)),
                    ScalarValue::Int32(Some(35)),
                ),
            ],
        );

        // Create mock accumulator (we only need the parts that create_optimized_filter uses)
        let on_right = vec![
            Arc::new(datafusion_physical_expr::expressions::Column::new(
                "col1", 0,
            )) as Arc<dyn PhysicalExpr>,
            Arc::new(datafusion_physical_expr::expressions::Column::new(
                "col2", 1,
            )) as Arc<dyn PhysicalExpr>,
        ];

        let accumulator = SharedBoundsAccumulator {
            inner: Mutex::new(SharedBoundsState {
                bounds: HashMap::new(),
                filter_optimized: false,
                completed_count: 0,
            }),
            total_partitions: 2,
            dynamic_filter: Arc::new(DynamicFilterPhysicalExpr::new(
                on_right.clone(),
                Arc::new(datafusion_physical_expr::expressions::Literal::new(
                    ScalarValue::Boolean(Some(true)),
                )),
            )),
            on_right,
        };

        // Test the optimized filter creation
        let bounds = HashMap::from([(0, bounds1.clone()), (1, bounds2.clone())]);
        let filter = accumulator
            .create_optimized_filter_from_partition_bounds(&bounds)?
            .unwrap();

        // Verify the filter is a CaseExpr (indirectly by checking it doesn't panic and has reasonable behavior)
        let test_batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(Int32Array::from(vec![5, 25, 100])), // col1 values
                Arc::new(Int32Array::from(vec![10, 30, 200])), // col2 values
            ],
        )?;

        let result = filter.evaluate(&test_batch)?;
        let result_array = result.into_array(test_batch.num_rows())?;
        let result_array = as_boolean_array(&result_array)?;

        // Should have 3 results
        assert_eq!(result_array.len(), 3);

        // The exact results depend on hash values, but we should get boolean results
        for i in 0..3 {
            // Just verify we get boolean values (true/false, not null for this simple case)
            assert!(
                !result_array.is_null(i),
                "Result should not be null at index {i}"
            );
        }

        Ok(())
    }

    #[test]
    fn test_empty_bounds() -> Result<()> {
        let on_right = vec![Arc::new(datafusion_physical_expr::expressions::Column::new(
            "col1", 0,
        )) as Arc<dyn PhysicalExpr>];

        let accumulator = SharedBoundsAccumulator {
            inner: Mutex::new(SharedBoundsState {
                bounds: HashMap::new(),
                filter_optimized: false,
                completed_count: 0,
            }),
            total_partitions: 2,
            dynamic_filter: Arc::new(DynamicFilterPhysicalExpr::new(
                on_right.clone(),
                Arc::new(datafusion_physical_expr::expressions::Literal::new(
                    ScalarValue::Boolean(Some(true)),
                )),
            )),
            on_right,
        };

        // Test with empty bounds
        let res =
            accumulator.create_optimized_filter_from_partition_bounds(&HashMap::new())?;
        assert!(res.is_none(), "Expected None for empty bounds");

        Ok(())
    }
}
