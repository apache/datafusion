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
use crate::joins::PartitionMode;
use crate::joins::hash_join::exec::{DynamicFilterRoutingMode, HASH_JOIN_SEED};
use crate::joins::hash_join::inlist_builder::build_struct_fields;
use crate::joins::hash_join::partitioned_hash_eval::{
    HashExpr, HashTableLookupExpr, SeededRandomState,
};
use crate::joins::utils::JoinHashMapType;
use arrow::array::ArrayRef;
use arrow::datatypes::{DataType, Field, Schema};
use datafusion_common::config::ConfigOptions;
use datafusion_common::{Result, ScalarValue};
use datafusion_expr::Operator;
use datafusion_functions::core::r#struct as struct_func;
use datafusion_physical_expr::expressions::{
    BinaryExpr, CaseExpr, DynamicFilterPhysicalExpr, DynamicFilterUpdate, InListExpr, lit,
};
use datafusion_physical_expr::{PhysicalExpr, PhysicalExprRef, ScalarFunctionExpr};

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

/// Creates a membership predicate for filter pushdown.
///
/// If `inlist_values` is provided (for small build sides), creates an InList expression.
/// Otherwise, creates a HashTableLookup expression (for large build sides).
///
/// Supports both single-column and multi-column joins using struct expressions.
fn create_membership_predicate(
    on_right: &[PhysicalExprRef],
    pushdown: &PushdownStrategy,
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
                Arc::clone(in_list_array),
                false,
            )?)))
        }
        // Use hash table lookup for large build sides
        PushdownStrategy::HashTable(hash_map) => {
            let lookup_hash_expr = Arc::new(HashExpr::new(
                on_right.to_vec(),
                random_state.clone(),
                "hash_join".to_string(),
            )) as Arc<dyn PhysicalExpr>;

            Ok(Some(Arc::new(HashTableLookupExpr::new(
                lookup_hash_expr,
                Arc::clone(hash_map),
                "hash_lookup".to_string(),
            )) as Arc<dyn PhysicalExpr>))
        }
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
    /// How to route partitioned dynamic filters.
    routing: DynamicFilterRoutingMode,
    /// Dynamic filter for pushdown to probe side
    dynamic_filter: Arc<DynamicFilterPhysicalExpr>,
    /// Right side join expressions needed for creating filter expressions
    on_right: Vec<PhysicalExprRef>,
    /// Random state for partitioning (RepartitionExec's hash function with 0,0,0,0 seeds)
    /// Used for PartitionedHashLookupPhysicalExpr
    repartition_random_state: SeededRandomState,
    /// Schema of the probe (right) side for evaluating filter expressions
    probe_schema: Arc<Schema>,
}

/// Strategy for filter pushdown (decided at collection time)
#[derive(Clone)]
pub(crate) enum PushdownStrategy {
    /// Use InList for small build sides (< 128MB)
    InList(ArrayRef),
    /// Use hash table lookup for large build sides
    HashTable(Arc<dyn JoinHashMapType>),
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
    fn combine_bounds_and_membership(
        membership_expr: Option<Arc<dyn PhysicalExpr>>,
        bounds_expr: Option<Arc<dyn PhysicalExpr>>,
    ) -> Option<Arc<dyn PhysicalExpr>> {
        match (membership_expr, bounds_expr) {
            (Some(membership), Some(bounds)) => {
                Some(Arc::new(BinaryExpr::new(bounds, Operator::And, membership))
                    as Arc<dyn PhysicalExpr>)
            }
            (Some(membership), None) => Some(membership),
            (None, Some(bounds)) => Some(bounds),
            (None, None) => None,
        }
    }

    fn build_partition_filter_expr(
        &self,
        pushdown: &PushdownStrategy,
        bounds: &PartitionBounds,
    ) -> Result<Option<Arc<dyn PhysicalExpr>>> {
        let membership_expr = create_membership_predicate(
            &self.on_right,
            pushdown,
            &HASH_JOIN_SEED,
            self.probe_schema.as_ref(),
        )?;

        let bounds_expr = create_bounds_predicate(&self.on_right, bounds);

        Ok(Self::combine_bounds_and_membership(
            membership_expr,
            bounds_expr,
        ))
    }

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
        routing: DynamicFilterRoutingMode,
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
            routing,
            dynamic_filter,
            on_right,
            repartition_random_state,
            probe_schema: right_child.schema(),
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
                        // Combine membership and bounds expressions for multi-layer optimization:
                        // - Bounds (min/max): Enable statistics-based pruning (Parquet row group/file skipping)
                        // - Membership (InList/hash lookup): Enables:
                        //   * Precise filtering (exact value matching)
                        //   * Bloom filter utilization (if present in Parquet files)
                        //   * Better pruning for data types where min/max isn't effective (e.g., UUIDs)
                        // Together, they provide complementary benefits and maximize data skipping.
                        if let Some(filter_expr) = self.build_partition_filter_expr(
                            &partition_data.pushdown,
                            &partition_data.bounds,
                        )? {
                            self.dynamic_filter
                                .update(DynamicFilterUpdate::Global(filter_expr))?;
                        }
                    }
                }
                // Partitioned: CASE expression routing to per-partition filters
                AccumulatedBuildData::Partitioned { partitions } => {
                    // Collect all partition data (should all be Some at this point)
                    let partition_data: Vec<_> =
                        partitions.iter().filter_map(|p| p.as_ref()).collect();

                    if !partition_data.is_empty() {
                        match self.routing {
                            DynamicFilterRoutingMode::CaseHash => {
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
                                        partition_opt.as_ref().and_then(|partition| {
                                            // Skip empty partitions - they would always return false anyway
                                            match &partition.pushdown {
                                                PushdownStrategy::Empty => None,
                                                _ => Some((partition_id, partition)),
                                            }
                                        })
                                    })
                                    .filter_map(|(partition_id, partition)| {
                                        match self.build_partition_filter_expr(
                                            &partition.pushdown,
                                            &partition.bounds,
                                        ) {
                                            Ok(Some(filter_expr)) => Some(Ok((
                                                lit(ScalarValue::UInt64(Some(
                                                    partition_id as u64,
                                                ))),
                                                filter_expr,
                                            ))),
                                            Ok(None) => None,
                                            Err(e) => Some(Err(e)),
                                        }
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

                                self.dynamic_filter
                                    .update(DynamicFilterUpdate::Global(filter_expr))?;
                            }
                            DynamicFilterRoutingMode::PartitionIndex => {
                                let mut partition_filters: Vec<
                                    Option<Arc<dyn PhysicalExpr>>,
                                > = vec![None; partitions.len()];

                                for (partition_id, partition) in
                                    partitions.iter().enumerate().filter_map(|(i, p)| {
                                        p.as_ref().map(|partition| (i, partition))
                                    })
                                {
                                    if matches!(
                                        partition.pushdown,
                                        PushdownStrategy::Empty
                                    ) {
                                        continue;
                                    }

                                    let filter_expr = self
                                        .build_partition_filter_expr(
                                            &partition.pushdown,
                                            &partition.bounds,
                                        )?
                                        // Defensive fallback: if no partition-local
                                        // predicate can be built, keep this partition
                                        // fail-open.
                                        .unwrap_or_else(|| lit(true));
                                    partition_filters[partition_id] = Some(filter_expr);
                                }

                                self.dynamic_filter.update(
                                    DynamicFilterUpdate::Partitioned(partition_filters),
                                )?;
                            }
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
    use crate::test::TestMemoryExec;
    use arrow::array::{ArrayRef, Int32Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion_physical_expr::expressions::{
        Column, DynamicFilterRuntimeContext, Literal,
    };
    use datafusion_physical_expr_common::physical_expr::bind_runtime_physical_expr;

    fn make_exec(schema: Arc<Schema>, partitions: usize) -> Arc<dyn ExecutionPlan> {
        let batch = RecordBatch::new_empty(Arc::clone(&schema));
        let mut partitioned_batches = Vec::with_capacity(partitions);
        for _ in 0..partitions {
            partitioned_batches.push(vec![batch.clone()]);
        }
        TestMemoryExec::try_new_exec(&partitioned_batches, schema, None).unwrap()
    }

    fn make_bounds(min: i32, max: i32) -> PartitionBounds {
        PartitionBounds::new(vec![ColumnBounds::new(
            ScalarValue::Int32(Some(min)),
            ScalarValue::Int32(Some(max)),
        )])
    }

    fn make_in_list(values: &[i32]) -> PushdownStrategy {
        let array: ArrayRef = Arc::new(Int32Array::from(values.to_vec()));
        PushdownStrategy::InList(array)
    }

    fn contains_hash_expr(expr: &Arc<dyn PhysicalExpr>) -> bool {
        if expr.as_any().downcast_ref::<HashExpr>().is_some() {
            return true;
        }
        expr.children()
            .iter()
            .any(|child| contains_hash_expr(child))
    }

    fn contains_case_expr(expr: &Arc<dyn PhysicalExpr>) -> bool {
        if expr.as_any().downcast_ref::<CaseExpr>().is_some() {
            return true;
        }
        expr.children()
            .iter()
            .any(|child| contains_case_expr(child))
    }

    fn is_literal_true(expr: &Arc<dyn PhysicalExpr>) -> bool {
        if let Some(literal) = expr.as_any().downcast_ref::<Literal>() {
            matches!(literal.value(), ScalarValue::Boolean(Some(true)))
        } else {
            false
        }
    }

    #[tokio::test]
    async fn partitioned_dynamic_filter_uses_hash_routing_when_enabled() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new("b", DataType::Int32, true)]));
        let left = make_exec(Arc::clone(&schema), 2);
        let right = make_exec(Arc::clone(&schema), 2);
        let on_right: Vec<PhysicalExprRef> =
            vec![Arc::new(Column::new_with_schema("b", &schema)?)];
        let dynamic_filter =
            Arc::new(DynamicFilterPhysicalExpr::new(on_right.clone(), lit(true)));
        let accumulator = SharedBuildAccumulator::new_from_partition_mode(
            PartitionMode::Partitioned,
            left.as_ref(),
            right.as_ref(),
            Arc::clone(&dynamic_filter),
            on_right,
            SeededRandomState::with_seeds(0, 0, 0, 0),
            DynamicFilterRoutingMode::CaseHash,
        );

        tokio::try_join!(
            accumulator.report_build_data(PartitionBuildData::Partitioned {
                partition_id: 0,
                pushdown: make_in_list(&[1, 2]),
                bounds: make_bounds(1, 2),
            }),
            accumulator.report_build_data(PartitionBuildData::Partitioned {
                partition_id: 1,
                pushdown: make_in_list(&[10, 11]),
                bounds: make_bounds(10, 11),
            })
        )?;

        let expr = dynamic_filter.current()?;
        assert!(
            contains_hash_expr(&expr),
            "expected hash routing expression"
        );
        Ok(())
    }

    #[tokio::test]
    async fn collect_left_dynamic_filter_never_uses_hash_routing() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new("b", DataType::Int32, true)]));
        let left = make_exec(Arc::clone(&schema), 1);
        let right = make_exec(Arc::clone(&schema), 2);
        let on_right: Vec<PhysicalExprRef> =
            vec![Arc::new(Column::new_with_schema("b", &schema)?)];
        let dynamic_filter =
            Arc::new(DynamicFilterPhysicalExpr::new(on_right.clone(), lit(true)));
        let accumulator = SharedBuildAccumulator::new_from_partition_mode(
            PartitionMode::CollectLeft,
            left.as_ref(),
            right.as_ref(),
            Arc::clone(&dynamic_filter),
            on_right,
            SeededRandomState::with_seeds(0, 0, 0, 0),
            DynamicFilterRoutingMode::CaseHash,
        );

        tokio::try_join!(
            accumulator.report_build_data(PartitionBuildData::CollectLeft {
                pushdown: make_in_list(&[1, 2]),
                bounds: make_bounds(1, 2),
            }),
            accumulator.report_build_data(PartitionBuildData::CollectLeft {
                pushdown: make_in_list(&[1, 2]),
                bounds: make_bounds(1, 2),
            })
        )?;

        let expr = dynamic_filter.current()?;
        assert!(
            !contains_hash_expr(&expr),
            "collect-left should not introduce hash routing"
        );
        Ok(())
    }

    #[tokio::test]
    async fn partitioned_dynamic_filter_or_path_ignores_empty_partitions() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new("b", DataType::Int32, true)]));
        let left = make_exec(Arc::clone(&schema), 2);
        let right = make_exec(Arc::clone(&schema), 2);
        let on_right: Vec<PhysicalExprRef> =
            vec![Arc::new(Column::new_with_schema("b", &schema)?)];
        let dynamic_filter =
            Arc::new(DynamicFilterPhysicalExpr::new(on_right.clone(), lit(true)));
        let accumulator = SharedBuildAccumulator::new_from_partition_mode(
            PartitionMode::Partitioned,
            left.as_ref(),
            right.as_ref(),
            Arc::clone(&dynamic_filter),
            on_right,
            SeededRandomState::with_seeds(0, 0, 0, 0),
            DynamicFilterRoutingMode::PartitionIndex,
        );

        tokio::try_join!(
            accumulator.report_build_data(PartitionBuildData::Partitioned {
                partition_id: 0,
                pushdown: PushdownStrategy::Empty,
                bounds: make_bounds(0, 0),
            }),
            accumulator.report_build_data(PartitionBuildData::Partitioned {
                partition_id: 1,
                pushdown: make_in_list(&[10, 11]),
                bounds: make_bounds(10, 11),
            })
        )?;

        let runtime_ctx = DynamicFilterRuntimeContext::for_partition(1);
        let bound = bind_runtime_physical_expr(
            Arc::clone(&dynamic_filter) as Arc<dyn PhysicalExpr>,
            &runtime_ctx,
        )?;
        let per_partition = bound.snapshot()?.expect("expected snapshot");
        assert!(
            !contains_hash_expr(&per_partition),
            "partition-index routing should not introduce hash routing"
        );
        assert!(
            !contains_case_expr(&per_partition),
            "partition-index routing should not introduce CASE routing"
        );
        assert!(
            !is_literal_true(&per_partition),
            "expected a concrete per-partition filter"
        );
        Ok(())
    }

    #[tokio::test]
    async fn partitioned_dynamic_filter_single_non_empty_skips_case_expr() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new("b", DataType::Int32, true)]));
        let left = make_exec(Arc::clone(&schema), 2);
        let right = make_exec(Arc::clone(&schema), 2);
        let on_right: Vec<PhysicalExprRef> =
            vec![Arc::new(Column::new_with_schema("b", &schema)?)];
        let dynamic_filter =
            Arc::new(DynamicFilterPhysicalExpr::new(on_right.clone(), lit(true)));
        let accumulator = SharedBuildAccumulator::new_from_partition_mode(
            PartitionMode::Partitioned,
            left.as_ref(),
            right.as_ref(),
            Arc::clone(&dynamic_filter),
            on_right,
            SeededRandomState::with_seeds(0, 0, 0, 0),
            DynamicFilterRoutingMode::PartitionIndex,
        );

        tokio::try_join!(
            accumulator.report_build_data(PartitionBuildData::Partitioned {
                partition_id: 0,
                pushdown: make_in_list(&[1, 2]),
                bounds: make_bounds(1, 2),
            }),
            accumulator.report_build_data(PartitionBuildData::Partitioned {
                partition_id: 1,
                pushdown: PushdownStrategy::Empty,
                bounds: make_bounds(0, 0),
            })
        )?;

        let runtime_ctx = DynamicFilterRuntimeContext::for_partition(0);
        let bound = bind_runtime_physical_expr(
            Arc::clone(&dynamic_filter) as Arc<dyn PhysicalExpr>,
            &runtime_ctx,
        )?;
        let expr = bound.snapshot()?.expect("expected snapshot");
        assert!(
            !contains_hash_expr(&expr),
            "single non-empty partition should not need hash routing"
        );
        assert!(
            !contains_case_expr(&expr),
            "single non-empty partition should not need CASE routing"
        );
        Ok(())
    }
}
