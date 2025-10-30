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

use crate::joins::hash_join::partitioned_hash_eval::{HashExpr, HashTableLookupExpr};
use crate::joins::utils::JoinHashMapType;
use crate::joins::PartitionMode;
use crate::ExecutionPlan;
use crate::ExecutionPlanProperties;

use ahash::RandomState;
use arrow::datatypes::{DataType, Field};
use datafusion_common::config::ConfigOptions;
use datafusion_common::{Result, ScalarValue};
use datafusion_expr::Operator;
use datafusion_functions::core::r#struct as struct_func;
use datafusion_physical_expr::expressions::{
    lit, BinaryExpr, CaseExpr, DynamicFilterPhysicalExpr, InListExpr,
};
use datafusion_physical_expr::{PhysicalExpr, PhysicalExprRef, ScalarFunctionExpr};

use parking_lot::Mutex;
use tokio::sync::Barrier;

/// Hash join seed - must match the one used when building hash tables
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
    hash_map: &Arc<dyn JoinHashMapType>,
    inlist_values: Option<&Vec<ScalarValue>>,
    random_state: &RandomState,
) -> Result<Arc<dyn PhysicalExpr>> {
    // Use InList expression for small build sides
    if let Some(values) = inlist_values {
        // Convert ScalarValues to literal expressions
        let list_exprs: Vec<Arc<dyn PhysicalExpr>> = values
            .iter()
            .map(|v| lit(v.clone()) as Arc<dyn PhysicalExpr>)
            .collect();

        // Build the expression to compare against
        let expr = if on_right.len() == 1 {
            // Single column: col IN (val1, val2, ...)
            Arc::clone(&on_right[0])
        } else {
            // Multi-column: struct(col1, col2, ...) IN (struct_val1, struct_val2, ...)
            // Create struct expression from columns
            let struct_udf = struct_func();

            // Build return field for the struct
            let fields: Vec<Field> = on_right
                .iter()
                .enumerate()
                .map(|(i, _expr)| {
                    // Use generic field types - will be inferred during evaluation
                    Field::new(format!("c{}", i), DataType::Null, true)
                })
                .collect();

            let return_field =
                Arc::new(Field::new("struct", DataType::Struct(fields.into()), true));

            Arc::new(ScalarFunctionExpr::new(
                "struct",
                struct_udf,
                on_right.to_vec(),
                return_field,
                Arc::new(ConfigOptions::default()),
            )) as Arc<dyn PhysicalExpr>
        };

        return Ok(Arc::new(InListExpr::new(
            expr, list_exprs, false, // not negated
            None,  // no static filter optimization
        )));
    }

    // Use hash table lookup for large build sides
    let lookup_hash_expr = Arc::new(HashExpr::new(
        on_right.to_vec(),
        random_state.clone(),
        "hash_join".to_string(),
    )) as Arc<dyn PhysicalExpr>;

    Ok(Arc::new(HashTableLookupExpr::new(
        lookup_hash_expr,
        Arc::clone(hash_map),
        "hash_lookup".to_string(),
    )) as Arc<dyn PhysicalExpr>)
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
    /// Shared state protected by a single mutex to avoid ordering concerns
    inner: Mutex<SharedBuildState>,
    barrier: Barrier,
    /// Dynamic filter for pushdown to probe side
    dynamic_filter: Arc<DynamicFilterPhysicalExpr>,
    /// Right side join expressions needed for creating filter expressions
    on_right: Vec<PhysicalExprRef>,
    /// Random state for partitioning (RepartitionExec's hash function with 0,0,0,0 seeds)
    /// Used for PartitionedHashLookupPhysicalExpr
    repartition_random_state: RandomState,
}

/// Strategy for filter pushdown (decided at collection time)
#[derive(Debug, Clone)]
pub(crate) enum PushdownStrategy {
    /// Use InList for small build sides (< 128MB)
    InList(Vec<ScalarValue>),
    /// Use hash table lookup for large build sides
    UseHashTable,
}

/// Build-side data reported by a single partition
pub(crate) enum PartitionBuildData {
    Partitioned {
        partition_id: usize,
        hash_map: Arc<dyn JoinHashMapType>,
        pushdown: PushdownStrategy,
    },
    CollectLeft {
        hash_map: Arc<dyn JoinHashMapType>,
        pushdown: PushdownStrategy,
    },
}

/// Per-partition accumulated data (Partitioned mode)
#[derive(Clone)]
struct PartitionData {
    hash_map: Arc<dyn JoinHashMapType>,
    pushdown: PushdownStrategy,
}

/// Build-side data organized by partition mode
enum AccumulatedBuildData {
    Partitioned {
        partitions: Vec<Option<PartitionData>>,
    },
    CollectLeft {
        hash_map: Option<Arc<dyn JoinHashMapType>>,
        pushdown: Option<PushdownStrategy>,
    },
}

/// State protected by SharedBuildAccumulator's mutex
struct SharedBuildState {
    /// Bounds from completed partitions
    /// Each element represents the column bounds computed by one partition.
    bounds: Vec<PartitionBounds>,
    /// Build-side data organized by partition mode
    mode_data: AccumulatedBuildData,
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

        let num_partitions = match partition_mode {
            PartitionMode::Partitioned => {
                left_child.output_partitioning().partition_count()
            }
            _ => 0, // Not used for CollectLeft
        };

        let mode_data = match partition_mode {
            PartitionMode::Partitioned => AccumulatedBuildData::Partitioned {
                partitions: vec![None; num_partitions],
            },
            PartitionMode::CollectLeft => AccumulatedBuildData::CollectLeft {
                hash_map: None,
                pushdown: None,
            },
            PartitionMode::Auto => unreachable!("PartitionMode::Auto should not be present at execution time. This is a bug in DataFusion, please report it!"),
        };

        Self {
            inner: Mutex::new(SharedBuildState {
                bounds: Vec::with_capacity(expected_calls),
                mode_data,
            }),
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
    /// * `data` - Build data including hash map and pushdown strategy
    /// * `bounds` - Min/max bounds for this partition
    ///
    /// # Returns
    /// * `Result<()>` - Ok if successful, Err if filter update failed or mode mismatch
    pub(crate) async fn report_build_data(
        &self,
        data: PartitionBuildData,
        bounds: Option<Vec<ColumnBounds>>,
    ) -> Result<()> {
        // Store data in the accumulator
        {
            let mut guard = self.inner.lock();

            match (data, &mut guard.mode_data) {
                // Partitioned mode
                (
                    PartitionBuildData::Partitioned {
                        partition_id,
                        hash_map,
                        pushdown,
                    },
                    AccumulatedBuildData::Partitioned { partitions },
                ) => {
                    partitions[partition_id] = Some(PartitionData { hash_map, pushdown });

                    if let Some(b) = bounds {
                        guard.bounds.push(PartitionBounds::new(partition_id, b));
                    }
                }
                // CollectLeft mode (store once, deduplicate across partitions)
                (
                    PartitionBuildData::CollectLeft { hash_map, pushdown },
                    AccumulatedBuildData::CollectLeft {
                        hash_map: ref mut stored_map,
                        pushdown: ref mut stored_pushdown,
                    },
                ) => {
                    if stored_map.is_none() {
                        *stored_map = Some(hash_map);
                        *stored_pushdown = Some(pushdown);
                    }

                    if let Some(b) = bounds {
                        // Use partition 0 for the single hash table
                        let should_push = if let Some(last_bound) = guard.bounds.last() {
                            // Deduplicate - all partitions report the same data in CollectLeft
                            last_bound.partition != 0
                        } else {
                            true
                        };

                        if should_push {
                            guard.bounds.push(PartitionBounds::new(0, b));
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

            match &inner.mode_data {
                // CollectLeft: Simple conjunction of bounds and membership check
                AccumulatedBuildData::CollectLeft { hash_map, pushdown } => {
                    if let Some(ref hash_map) = hash_map {
                        // Create membership predicate (InList for small build sides, hash lookup otherwise)
                        let inlist_values = match pushdown.as_ref().unwrap() {
                            PushdownStrategy::InList(values) => Some(values),
                            PushdownStrategy::UseHashTable => None,
                        };

                        let membership_expr = create_membership_predicate(
                            &self.on_right,
                            hash_map,
                            inlist_values,
                            &HASH_JOIN_SEED,
                        )?;

                        // Create bounds check expression (if bounds available)
                        let mut filter_expr = membership_expr;

                        if let Some(partition_bounds) = inner.bounds.first() {
                            let mut column_predicates = Vec::new();

                            for (col_idx, right_expr) in self.on_right.iter().enumerate()
                            {
                                if let Some(column_bounds) =
                                    partition_bounds.get_column_bounds(col_idx)
                                {
                                    // Create predicate: col >= min AND col <= max
                                    let min_expr = Arc::new(BinaryExpr::new(
                                        Arc::clone(right_expr),
                                        Operator::GtEq,
                                        lit(column_bounds.min.clone()),
                                    ))
                                        as Arc<dyn PhysicalExpr>;
                                    let max_expr = Arc::new(BinaryExpr::new(
                                        Arc::clone(right_expr),
                                        Operator::LtEq,
                                        lit(column_bounds.max.clone()),
                                    ))
                                        as Arc<dyn PhysicalExpr>;
                                    let range_expr = Arc::new(BinaryExpr::new(
                                        min_expr,
                                        Operator::And,
                                        max_expr,
                                    ))
                                        as Arc<dyn PhysicalExpr>;
                                    column_predicates.push(range_expr);
                                }
                            }

                            // Combine all column range predicates with AND
                            if !column_predicates.is_empty() {
                                let bounds_expr = column_predicates
                                    .into_iter()
                                    .reduce(|acc, pred| {
                                        Arc::new(BinaryExpr::new(
                                            acc,
                                            Operator::And,
                                            pred,
                                        ))
                                            as Arc<dyn PhysicalExpr>
                                    })
                                    .unwrap();

                                // Combine bounds_expr AND membership_expr
                                filter_expr = Arc::new(BinaryExpr::new(
                                    bounds_expr,
                                    Operator::And,
                                    filter_expr,
                                ))
                                    as Arc<dyn PhysicalExpr>;
                            }
                        }

                        self.dynamic_filter.update(filter_expr)?;
                    }
                }
                // Partitioned: CASE expression routing to per-partition filters
                AccumulatedBuildData::Partitioned { partitions } => {
                    // Collect all partition data (should all be Some at this point)
                    let partition_data: Vec<_> =
                        partitions.iter().filter_map(|p| p.as_ref()).collect();

                    if !partition_data.is_empty() {
                        // Build a CASE expression that combines range checks AND membership checks
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
                                partition_opt
                                    .as_ref()
                                    .map(|partition| (partition_id, partition))
                            })
                            .map(|(partition_id, partition)| -> Result<_> {
                                // WHEN partition_id
                                let when_expr =
                                    lit(ScalarValue::UInt64(Some(partition_id as u64)));

                                // THEN: Combine bounds check AND membership predicate

                                // 1. Create membership predicate (InList for small build sides, hash lookup otherwise)
                                let inlist_values = match &partition.pushdown {
                                    PushdownStrategy::InList(values) => Some(values),
                                    PushdownStrategy::UseHashTable => None,
                                };

                                let membership_expr = create_membership_predicate(
                                    &self.on_right,
                                    &partition.hash_map,
                                    inlist_values,
                                    &HASH_JOIN_SEED,
                                )?;

                                // 2. Create bounds check expression for this partition (if bounds available)
                                let mut then_expr = membership_expr;

                                if let Some(partition_bounds) = inner
                                    .bounds
                                    .iter()
                                    .find(|pb| pb.partition == partition_id)
                                {
                                    let mut column_predicates = Vec::new();

                                    for (col_idx, right_expr) in
                                        self.on_right.iter().enumerate()
                                    {
                                        if let Some(column_bounds) =
                                            partition_bounds.get_column_bounds(col_idx)
                                        {
                                            // Create predicate: col >= min AND col <= max
                                            let min_expr = Arc::new(BinaryExpr::new(
                                                Arc::clone(right_expr),
                                                Operator::GtEq,
                                                lit(column_bounds.min.clone()),
                                            ))
                                                as Arc<dyn PhysicalExpr>;
                                            let max_expr = Arc::new(BinaryExpr::new(
                                                Arc::clone(right_expr),
                                                Operator::LtEq,
                                                lit(column_bounds.max.clone()),
                                            ))
                                                as Arc<dyn PhysicalExpr>;
                                            let range_expr = Arc::new(BinaryExpr::new(
                                                min_expr,
                                                Operator::And,
                                                max_expr,
                                            ))
                                                as Arc<dyn PhysicalExpr>;
                                            column_predicates.push(range_expr);
                                        }
                                    }

                                    // Combine all column range predicates with AND
                                    if !column_predicates.is_empty() {
                                        let bounds_expr = column_predicates
                                            .into_iter()
                                            .reduce(|acc, pred| {
                                                Arc::new(BinaryExpr::new(
                                                    acc,
                                                    Operator::And,
                                                    pred,
                                                ))
                                                    as Arc<dyn PhysicalExpr>
                                            })
                                            .unwrap();

                                        // Combine bounds_expr AND membership_expr
                                        then_expr = Arc::new(BinaryExpr::new(
                                            bounds_expr,
                                            Operator::And,
                                            then_expr,
                                        ))
                                            as Arc<dyn PhysicalExpr>;
                                    }
                                }

                                Ok((when_expr, then_expr))
                            })
                            .collect::<Result<Vec<_>>>()?;

                        // Create CASE expression
                        let filter_expr = Arc::new(CaseExpr::try_new(
                            Some(modulo_expr),
                            when_then_branches,
                            Some(lit(false)), // ELSE false
                        )?)
                            as Arc<dyn PhysicalExpr>;

                        self.dynamic_filter.update(filter_expr)?;
                    }
                }
            }
        }

        Ok(())
    }
}

impl fmt::Debug for SharedBuildAccumulator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SharedBuildAccumulator")
    }
}
