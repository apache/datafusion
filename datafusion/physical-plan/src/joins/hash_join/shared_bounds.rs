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

use crate::ExecutionPlan;
use crate::ExecutionPlanProperties;
use crate::joins::Map;
use crate::joins::PartitionMode;
use crate::joins::hash_join::exec::HASH_JOIN_SEED;
use crate::joins::hash_join::inlist_builder::build_struct_fields;
use crate::joins::hash_join::partitioned_hash_eval::{
    HashExpr, HashTableLookupExpr, SeededRandomState,
};
use arrow::array::ArrayRef;
use arrow::datatypes::{DataType, Field, Schema};
use datafusion_common::config::ConfigOptions;
use datafusion_common::{Result, ScalarValue};
use datafusion_expr::Operator;
use datafusion_functions::core::r#struct as struct_func;
use datafusion_physical_expr::expressions::{
    BinaryExpr, CaseExpr, DynamicFilterPhysicalExpr, InListExpr, lit,
};
use datafusion_physical_expr::{PhysicalExpr, PhysicalExprRef, ScalarFunctionExpr};

use parking_lot::Mutex;
use tokio::sync::Barrier;

/// Represents the minimum and maximum values for a specific column.
/// Used in dynamic filter pushdown to establish value boundaries.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ColumnBounds {
    /// The minimum value observed for this column
    pub(crate) min: ScalarValue,
    /// The maximum value observed for this column
    pub(crate) max: ScalarValue,
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

/// Creates a membership predicate for filter pushdown.
///
/// If `inlist_values` is provided (for small build sides), creates an InList expression.
/// Otherwise, creates a HashTableLookup expression (for large build sides).
///
/// Supports both single-column and multi-column joins using struct expressions.
fn create_membership_predicate(
    on_right: &[PhysicalExprRef],
    pushdown: PushdownStrategy,
    random_state: &SeededRandomState,
    schema: &Schema,
) -> Result<Option<Arc<dyn PhysicalExpr>>> {
    match pushdown {
        // Use InList expression for small build sides
        PushdownStrategy::InList(in_list_array) => {
            // Build the expression to compare against
            let expr = if on_right.len() == 1 {
                // Single column: col IN (val1, val2, ...)
                Arc::clone(&on_right[0])
            } else {
                let fields = build_struct_fields(
                    on_right
                        .iter()
                        .map(|r| r.data_type(schema))
                        .collect::<Result<Vec<_>>>()?
                        .as_ref(),
                )?;

                // The return field name and the function field name don't really matter here.
                let return_field =
                    Arc::new(Field::new("struct", DataType::Struct(fields), true));

                Arc::new(ScalarFunctionExpr::new(
                    "struct",
                    struct_func(),
                    on_right.to_vec(),
                    return_field,
                    Arc::new(ConfigOptions::default()),
                )) as Arc<dyn PhysicalExpr>
            };

            // Use in_list_from_array() helper to create InList with static_filter optimization (hash-based lookup)
            Ok(Some(Arc::new(InListExpr::try_new_from_array(
                expr,
                in_list_array,
                false,
            )?)))
        }
        // Use hash table lookup for large build sides
        PushdownStrategy::Map(hash_map) => Ok(Some(Arc::new(HashTableLookupExpr::new(
            on_right.to_vec(),
            random_state.clone(),
            hash_map,
            "hash_lookup".to_string(),
        )) as Arc<dyn PhysicalExpr>)),
        // Empty partition - should not create a filter for this
        PushdownStrategy::Empty => Ok(None),
    }
}

/// Creates a bounds predicate from partition bounds.
///
/// Returns `None` if no column bounds are available.
/// Returns a combined predicate (col >= min AND col <= max) for all columns with bounds.
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

/// Build a filter expression for a single partition's data.
///
/// Returns a combined (bounds AND membership) expression, or `None` if neither bounds nor
/// membership data is available.
fn build_partition_filter_expr(
    on_right: &[PhysicalExprRef],
    partition: &PartitionData,
    probe_schema: &Schema,
) -> Result<Option<Arc<dyn PhysicalExpr>>> {
    let membership_expr = create_membership_predicate(
        on_right,
        partition.pushdown.clone(),
        &HASH_JOIN_SEED,
        probe_schema,
    )?;
    let bounds_expr = create_bounds_predicate(on_right, &partition.bounds);

    Ok(match (membership_expr, bounds_expr) {
        (Some(membership), Some(bounds)) => {
            Some(Arc::new(BinaryExpr::new(bounds, Operator::And, membership))
                as Arc<dyn PhysicalExpr>)
        }
        (Some(membership), None) => Some(membership),
        (None, Some(bounds)) => Some(bounds),
        (None, None) => None,
    })
}

/// Combine multiple filter expressions into a single OR expression.
/// Returns `None` if the input is empty.
fn combine_or_filters(
    filters: Vec<Arc<dyn PhysicalExpr>>,
) -> Option<Arc<dyn PhysicalExpr>> {
    filters.into_iter().reduce(|acc, filter| {
        Arc::new(BinaryExpr::new(acc, Operator::Or, filter)) as Arc<dyn PhysicalExpr>
    })
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
    repartition_random_state: SeededRandomState,
    /// Schema of the probe (right) side for evaluating filter expressions
    probe_schema: Arc<Schema>,
    /// When true, Partitioned mode uses Global OR instead of CASE hash routing. This is needed
    /// when the data is not truly hash-distributed (i.e. using file-group partitioning from
    /// `preserve_file_partitions`). In this case, using `CASE hash(key) % N` routing would apply
    /// the wrong partition's filter to data. Global OR combines all partition's filters which is
    /// safe for any partitioning scheme and still allows for effective pruning.
    use_global_or: bool,
}

/// Strategy for filter pushdown (decided at collection time)
#[derive(Clone)]
pub(crate) enum PushdownStrategy {
    /// Use InList for small build sides (< 128MB)
    InList(ArrayRef),
    /// Use map lookup for large build sides
    Map(Arc<Map>),
    /// There was no data in this partition, do not build a dynamic filter for it
    Empty,
}

/// Build-side data reported by a single partition
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
struct PartitionData {
    bounds: PartitionBounds,
    pushdown: PushdownStrategy,
}

/// Build-side data organized by partition mode
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
        repartition_random_state: SeededRandomState,
        use_global_or: bool,
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
            PartitionMode::Auto => unreachable!(
                "PartitionMode::Auto should not be present at execution time. This is a bug in DataFusion, please report it!"
            ),
        };

        let mode_data = match partition_mode {
            PartitionMode::Partitioned => AccumulatedBuildData::Partitioned {
                partitions: vec![
                    None;
                    left_child.output_partitioning().partition_count()
                ],
            },
            PartitionMode::CollectLeft => {
                AccumulatedBuildData::CollectLeft { data: None }
            }
            PartitionMode::Auto => unreachable!(
                "PartitionMode::Auto should not be present at execution time. This is a bug in DataFusion, please report it!"
            ),
        };

        Self {
            inner: Mutex::new(mode_data),
            barrier: Barrier::new(expected_calls),
            dynamic_filter,
            on_right,
            repartition_random_state,
            probe_schema: right_child.schema(),
            use_global_or,
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
        if self.barrier.wait().await.is_leader() {
            // All partitions have reported, so we can create and update the filter
            let inner = self.inner.lock();

            match &*inner {
                // CollectLeft: Simple conjunction of bounds and membership check
                AccumulatedBuildData::CollectLeft { data } => {
                    if let Some(partition_data) = data {
                        // Create membership predicate (InList for small build sides, hash lookup otherwise)
                        let membership_expr = create_membership_predicate(
                            &self.on_right,
                            partition_data.pushdown.clone(),
                            &HASH_JOIN_SEED,
                            self.probe_schema.as_ref(),
                        )?;

                        // Create bounds check expression (if bounds available)
                        let bounds_expr = create_bounds_predicate(
                            &self.on_right,
                            &partition_data.bounds,
                        );

                        // Combine membership and bounds expressions for multi-layer optimization:
                        // - Bounds (min/max): Enable statistics-based pruning (Parquet row group/file skipping)
                        // - Membership (InList/hash lookup): Enables:
                        //   * Precise filtering (exact value matching)
                        //   * Bloom filter utilization (if present in Parquet files)
                        //   * Better pruning for data types where min/max isn't effective (e.g., UUIDs)
                        // Together, they provide complementary benefits and maximize data skipping.
                        // Only update the filter if we have something to push down
                        if let Some(filter_expr) = match (membership_expr, bounds_expr) {
                            (Some(membership), Some(bounds)) => {
                                // Both available: combine with AND
                                Some(Arc::new(BinaryExpr::new(
                                    bounds,
                                    Operator::And,
                                    membership,
                                ))
                                    as Arc<dyn PhysicalExpr>)
                            }
                            (Some(membership), None) => {
                                // Membership available but no bounds
                                // This is reachable when we have data but bounds aren't available
                                // (e.g., unsupported data types or no columns with bounds)
                                Some(membership)
                            }
                            (None, Some(bounds)) => {
                                // Bounds available but no membership.
                                // This should be unreachable in practice: we can always push down a reference
                                // to the hash table.
                                // But it seems safer to handle it defensively.
                                Some(bounds)
                            }
                            (None, None) => {
                                // No filter available (e.g., empty build side)
                                // Don't update the filter, but continue to mark complete
                                None
                            }
                        } {
                            self.dynamic_filter.update(filter_expr)?;
                        }
                    }
                }
                // Partitioned: build filter from all partitions
                AccumulatedBuildData::Partitioned { partitions } => {
                    // Collect all partition data (should all be Some at this point)
                    let partition_data: Vec<_> =
                        partitions.iter().filter_map(|p| p.as_ref()).collect();

                    if !partition_data.is_empty() {
                        if self.use_global_or {
                            // Global OR: combine all partition filters with OR
                            //
                            // Used when data is not truly hash-distributed (file-group
                            // partitioning from `preserve_file_partitions`). In this case,
                            // the CASE hash(key) % N routing would apply the wrong
                            // partition's filter. Global OR is safe for any partitioning
                            // scheme: PruningPredicate decomposes OR branches independently
                            // for row-group pruning, achieving equivalent selectivity.
                            //
                            // WHERE (
                            //  (col >= min_0 AND col <= max_0) OR
                            //  (col >= min_1 AND col <= max_1) OR
                            //   ...
                            // )
                            let partition_filters: Vec<Arc<dyn PhysicalExpr>> =
                                partitions
                                    .iter()
                                    .filter_map(|partition_opt| {
                                        partition_opt.as_ref().and_then(|partition| {
                                            match &partition.pushdown {
                                                PushdownStrategy::Empty => None,
                                                _ => Some(partition),
                                            }
                                        })
                                    })
                                    .map(|partition| {
                                        build_partition_filter_expr(
                                            &self.on_right,
                                            partition,
                                            self.probe_schema.as_ref(),
                                        )
                                    })
                                    .collect::<Result<Vec<_>>>()?
                                    .into_iter()
                                    .flatten()
                                    .collect();

                            if let Some(filter_expr) =
                                combine_or_filters(partition_filters)
                            {
                                self.dynamic_filter.update(filter_expr)?;
                            }
                        } else {
                            // CASE hash routing: route each row to the correct partition's filter
                            // CASE (hash_repartition(join_keys) % num_partitions)
                            //   WHEN 0 THEN (col >= min_0 AND col <= max_0 AND ...) AND membership_check_0
                            //   WHEN 1 THEN (col >= min_1 AND col <= max_1 AND ...) AND membership_check_1
                            //   ...
                            //   ELSE false
                            // END
                            let num_partitions = partition_data.len();

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
                            ))
                                as Arc<dyn PhysicalExpr>;

                            // Create WHEN branches for each partition
                            let when_then_branches: Vec<(
                                Arc<dyn PhysicalExpr>,
                                Arc<dyn PhysicalExpr>,
                            )> = partitions
                                .iter()
                                .enumerate()
                                .filter_map(|(partition_id, partition_opt)| {
                                    partition_opt.as_ref().and_then(|partition| {
                                        match &partition.pushdown {
                                            PushdownStrategy::Empty => None,
                                            _ => Some((partition_id, partition)),
                                        }
                                    })
                                })
                                .map(|(partition_id, partition)| -> Result<_> {
                                    let when_expr = lit(ScalarValue::UInt64(Some(
                                        partition_id as u64,
                                    )));

                                    let then_expr = build_partition_filter_expr(
                                        &self.on_right,
                                        partition,
                                        self.probe_schema.as_ref(),
                                    )?
                                    .unwrap_or_else(|| lit(true));

                                    Ok((when_expr, then_expr))
                                })
                                .collect::<Result<Vec<_>>>()?;

                            // Optimize for single partition: skip CASE expression entirely
                            let filter_expr = if when_then_branches.is_empty() {
                                // All partitions are empty: no rows can match
                                lit(false)
                            } else if when_then_branches.len() == 1 {
                                // Single partition: just use the condition directly
                                // since hash % 1 == 0 always, the WHEN 0 branch will always match
                                Arc::clone(&when_then_branches[0].1)
                            } else {
                                // Multiple partitions: create CASE expression
                                Arc::new(CaseExpr::try_new(
                                    Some(modulo_expr),
                                    when_then_branches,
                                    Some(lit(false)), // ELSE false
                                )?)
                                    as Arc<dyn PhysicalExpr>
                            };

                            self.dynamic_filter.update(filter_expr)?;
                        }
                    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int32Array;
    use arrow::datatypes::Field;
    use datafusion_physical_expr::expressions::Column;

    /// Creates a Column expression
    fn test_col(name: &str, index: usize) -> PhysicalExprRef {
        Arc::new(Column::new(name, index))
    }

    #[test]
    fn test_global_or_combines_multiple_partition_bounds() {
        // 3 partitions with different ranges:
        //   Partition 0: a in [1, 100]
        //   Partition 1: a in [200, 300]
        //   Partition 2: a in [400, 500]
        // Global OR should produce:
        //   (a >= 1 AND a <= 100) OR (a >= 200 AND a <= 300) OR (a >= 400 AND a <= 500)
        let col_a = test_col("a", 0);
        let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);

        let partitions: Vec<PartitionData> = vec![
            PartitionData {
                bounds: PartitionBounds::new(vec![ColumnBounds::new(
                    ScalarValue::Int32(Some(1)),
                    ScalarValue::Int32(Some(100)),
                )]),
                pushdown: PushdownStrategy::Empty,
            },
            PartitionData {
                bounds: PartitionBounds::new(vec![ColumnBounds::new(
                    ScalarValue::Int32(Some(200)),
                    ScalarValue::Int32(Some(300)),
                )]),
                pushdown: PushdownStrategy::Empty,
            },
            PartitionData {
                bounds: PartitionBounds::new(vec![ColumnBounds::new(
                    ScalarValue::Int32(Some(400)),
                    ScalarValue::Int32(Some(500)),
                )]),
                pushdown: PushdownStrategy::Empty,
            },
        ];

        // Build per-partition filters (same logic as Global OR path)
        let partition_filters: Vec<Arc<dyn PhysicalExpr>> = partitions
            .iter()
            .filter_map(|p| {
                build_partition_filter_expr(std::slice::from_ref(&col_a), p, &schema)
                    .ok()
                    .flatten()
            })
            .collect();

        assert_eq!(
            partition_filters.len(),
            3,
            "should have 3 partition filters"
        );

        let combined = combine_or_filters(partition_filters).unwrap();
        let display = format!("{combined}");
        assert_eq!(
            display,
            "a@0 >= 1 AND a@0 <= 100 OR a@0 >= 200 AND a@0 <= 300 OR a@0 >= 400 AND a@0 <= 500"
        );
    }

    #[test]
    fn test_global_or_skips_empty_partitions() {
        // 3 partitions with different ranges:
        //   Partition 0: a in [1, 100]
        //   Partition 1: empty
        //   Partition 2: a in [400, 500]
        // Should produce: (a >= 1 AND a <= 100) OR (a >= 400 AND a <= 500)
        let col_a = test_col("a", 0);
        let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);

        let partitions: Vec<Option<PartitionData>> = vec![
            Some(PartitionData {
                bounds: PartitionBounds::new(vec![ColumnBounds::new(
                    ScalarValue::Int32(Some(1)),
                    ScalarValue::Int32(Some(100)),
                )]),
                pushdown: PushdownStrategy::Empty,
            }),
            None, // empty partition
            Some(PartitionData {
                bounds: PartitionBounds::new(vec![ColumnBounds::new(
                    ScalarValue::Int32(Some(400)),
                    ScalarValue::Int32(Some(500)),
                )]),
                pushdown: PushdownStrategy::Empty,
            }),
        ];

        // Build per-partition filters (same logic as Global OR path)
        let partition_filters: Vec<Arc<dyn PhysicalExpr>> = partitions
            .iter()
            .filter_map(|opt| opt.as_ref())
            .filter_map(|p| {
                build_partition_filter_expr(std::slice::from_ref(&col_a), p, &schema)
                    .ok()
                    .flatten()
            })
            .collect();

        assert_eq!(
            partition_filters.len(),
            2,
            "should skip the empty partition"
        );

        let combined = combine_or_filters(partition_filters).unwrap();
        let display = format!("{combined}");
        assert_eq!(
            display,
            "a@0 >= 1 AND a@0 <= 100 OR a@0 >= 400 AND a@0 <= 500"
        );
    }

    #[test]
    fn test_global_or_single_partition() {
        // Single partition should produce just the bounds
        let col_a = test_col("a", 0);
        let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);

        let partition = PartitionData {
            bounds: PartitionBounds::new(vec![ColumnBounds::new(
                ScalarValue::Int32(Some(10)),
                ScalarValue::Int32(Some(50)),
            )]),
            pushdown: PushdownStrategy::Empty,
        };

        let filter = build_partition_filter_expr(&[col_a], &partition, &schema)
            .unwrap()
            .unwrap();
        let combined = combine_or_filters(vec![filter]).unwrap();
        let display = format!("{combined}");
        // No OR, just the single partition's filter
        assert_eq!(display, "a@0 >= 10 AND a@0 <= 50");
    }

    #[test]
    fn test_global_or_all_empty_partitions() {
        // All partitions empty → no filter
        let col_a = test_col("a", 0);
        let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);

        let partitions: Vec<Option<PartitionData>> = vec![None, None, None];

        let partition_filters: Vec<Arc<dyn PhysicalExpr>> = partitions
            .iter()
            .filter_map(|opt| opt.as_ref())
            .filter_map(|p| {
                build_partition_filter_expr(std::slice::from_ref(&col_a), p, &schema)
                    .ok()
                    .flatten()
            })
            .collect();

        assert!(partition_filters.is_empty());
        let combined = combine_or_filters(partition_filters);
        assert!(
            combined.is_none(),
            "all-empty partitions should produce no filter"
        );
    }

    #[test]
    fn test_global_or_row_level_evaluation() {
        // 2 partitions with different ranges:
        //   Partition 0: key in [1, 100]
        //   Partition 1: key in [200, 300]
        //
        // A batch with keys [50, 250, 150] should produce:
        //   row 0 (key=50):  (50>=1 AND 50<=100)=true  OR (50>=200 AND 50<=300)=false -> true
        //   row 1 (key=250): (250>=1 AND 250<=100)=false OR (250>=200 AND 250<=300)=true -> true
        //   row 2 (key=150): (150>=1 AND 150<=100)=false OR (150>=200 AND 150<=300)=false -> false
        use arrow::record_batch::RecordBatch;

        let col_key = test_col("key", 0);
        let schema = Schema::new(vec![Field::new("key", DataType::Int32, false)]);

        let partitions = [
            PartitionData {
                bounds: PartitionBounds::new(vec![ColumnBounds::new(
                    ScalarValue::Int32(Some(1)),
                    ScalarValue::Int32(Some(100)),
                )]),
                pushdown: PushdownStrategy::Empty,
            },
            PartitionData {
                bounds: PartitionBounds::new(vec![ColumnBounds::new(
                    ScalarValue::Int32(Some(200)),
                    ScalarValue::Int32(Some(300)),
                )]),
                pushdown: PushdownStrategy::Empty,
            },
        ];

        let filters: Vec<Arc<dyn PhysicalExpr>> = partitions
            .iter()
            .filter_map(|p| {
                build_partition_filter_expr(std::slice::from_ref(&col_key), p, &schema)
                    .ok()
                    .flatten()
            })
            .collect();

        let or_filter = combine_or_filters(filters).unwrap();

        // Evaluate against a batch
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(Int32Array::from(vec![50, 250, 150]))],
        )
        .unwrap();

        let result = or_filter
            .evaluate(&batch)
            .unwrap()
            .into_array(batch.num_rows())
            .unwrap();
        let bools = result
            .as_any()
            .downcast_ref::<arrow::array::BooleanArray>()
            .unwrap();

        assert!(bools.value(0), "key=50 should match partition 0's range");
        assert!(bools.value(1), "key=250 should match partition 1's range");
        assert!(
            !bools.value(2),
            "key=150 should not match any partition's range"
        );
    }
}
