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
use crate::Partitioning;
use crate::joins::Map;
use crate::joins::PartitionMode;
use crate::joins::hash_join::bloom_filter::{self, BloomFilterExpr, BloomFilterOptions};
use crate::joins::hash_join::exec::{HASH_JOIN_SEED, NullAwareMode};
use crate::joins::hash_join::inlist_builder::build_struct_fields;
use crate::joins::hash_join::partitioned_hash_eval::{
    HashExpr, HashTableLookupExpr, SeededRandomState,
};
use crate::joins::key_range_bitmap::{KeyRangeBitmap, KeyRangePart};
use crate::repartition::RangeExpr;
use arrow::array::{Array, ArrayRef};
use arrow::datatypes::{DataType, Field, Schema};
use datafusion_common::config::ConfigOptions;
use datafusion_common::{
    DataFusionError, NullEquality, Result, ScalarValue, SharedResult,
    assert_or_internal_err,
};
use datafusion_execution::memory_pool::MemoryPool;
use datafusion_expr::Operator;
use datafusion_functions::core::r#struct as struct_func;
use datafusion_physical_expr::expressions::{
    BinaryExpr, CaseExpr, DynamicFilterPhysicalExpr, InListExpr, IsNullExpr, lit,
};
use datafusion_physical_expr::{
    PhysicalExpr, PhysicalExprRef, RangePartitioning, ScalarFunctionExpr,
};

use parking_lot::Mutex;
use tokio::sync::Notify;

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

    /// Bounds that hold the keys of every partition: the smallest minimum and the
    /// largest maximum of each column.
    ///
    /// `None` when a partition has no bounds or a column's values cannot be
    /// compared. Then a bounds predicate is not possible.
    fn merge<'a>(
        mut partitions: impl Iterator<Item = &'a PartitionBounds>,
    ) -> Option<Self> {
        let mut merged = partitions.next()?.clone();
        if merged.column_bounds.is_empty() {
            return None;
        }
        for bounds in partitions {
            if bounds.column_bounds.len() != merged.column_bounds.len() {
                return None;
            }
            for (acc, b) in merged.column_bounds.iter_mut().zip(&bounds.column_bounds) {
                // A NULL bound comes from a partition with no non-NULL key in this
                // column. It must not widen (or, as a NULL comparison, disable) the
                // bounds of the keys of other partitions.
                if acc.min.is_null()
                    || (!b.min.is_null() && b.min.partial_cmp(&acc.min)?.is_lt())
                {
                    acc.min = b.min.clone();
                }
                if acc.max.is_null()
                    || (!b.max.is_null() && b.max.partial_cmp(&acc.max)?.is_gt())
                {
                    acc.max = b.max.clone();
                }
            }
        }
        Some(merged)
    }
}

/// The build side key columns of one partition, for the global dynamic filter.
#[derive(Clone, Default)]
pub(crate) struct BuildKeys {
    /// One array per join key
    pub(crate) keys: Vec<ArrayRef>,
    /// Number of distinct key tuples
    pub(crate) num_distinct: usize,
}

/// Options for the global dynamic filter of a hash join, from the session
/// configuration.
#[derive(Debug, Clone)]
pub(crate) struct GlobalFilterOptions {
    /// Use an InList when all partitions together have at most this many
    /// distinct keys (`hash_join_dynamic_filter_inlist_max_distinct_values`).
    pub(crate) inlist_max_distinct_values: usize,
    pub(crate) bloom_filter: BloomFilterOptions,
    /// Pool that the bloom filter memory is charged to. `None` (tests only)
    /// does not track the memory.
    pub(crate) memory_pool: Option<Arc<dyn MemoryPool>>,
}

impl GlobalFilterOptions {
    pub(crate) fn new(config: &ConfigOptions) -> Self {
        let optimizer = &config.optimizer;
        Self {
            inlist_max_distinct_values: optimizer
                .hash_join_dynamic_filter_inlist_max_distinct_values,
            bloom_filter: BloomFilterOptions {
                fpp: optimizer.hash_join_bloom_filter_fpp,
                max_bytes: optimizer.hash_join_bloom_filter_max_bytes,
                max_fpp: optimizer.hash_join_bloom_filter_max_fpp,
            },
            memory_pool: None,
        }
    }

    /// Charge the bloom filter memory to `pool`.
    pub(crate) fn with_memory_pool(mut self, pool: Arc<dyn MemoryPool>) -> Self {
        self.memory_pool = Some(pool);
        self
    }
}

impl Default for GlobalFilterOptions {
    fn default() -> Self {
        Self::new(&ConfigOptions::default())
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

            // Use InListExpr::try_new_from_array() to build an InList with static_filter optimization (hash-based lookup)
            Ok(Some(Arc::new(InListExpr::try_new_from_array(
                expr,
                in_list_array,
                false,
                schema,
            )?)))
        }
        // Use hash table lookup for large build sides
        PushdownStrategy::Map(hash_map, pruning_bitmap) => {
            Ok(Some(Arc::new(HashTableLookupExpr::new(
                on_right.to_vec(),
                random_state.clone(),
                hash_map,
                "hash_lookup".to_string(),
                pruning_bitmap,
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
    let mut column_predicates = Vec::with_capacity(on_right.len());

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

/// Combines a membership predicate and a bounds predicate with logical AND.
///
/// Returns `None` when neither is available; callers decide the fallback (e.g.
/// skip updating the filter vs. emit a `lit(true)` branch inside a CASE).
fn combine_membership_and_bounds(
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
/// 2. Information is stored in the shared state, which tracks how many partitions have reported
/// 3. When the last partition reports, one waiter is elected as the finalizer; it merges the
///    collected information, updates the dynamic filter exactly once, and publishes the
///    terminal result by transitioning [`CompletionState`] to `Ready`
/// 4. A [`tokio::sync::Notify`] wakes any other partitions parked in `wait_for_completion`,
///    which then observe the `Ready` state under the mutex and return immediately
///
/// ## Hash Map vs Bounds
///
/// - **Global filter (default)**: merges the bounds and keys of all partitions
///   into one unrouted filter: global min/max bounds AND an InList (few keys) or
///   a bloom filter of all build keys. See `try_build_global_filter`.
/// - **Routed filter (fallback)**: when a partition was canceled or the key type
///   has no bloom filter support, a `CASE` on the repartition hash (or range)
///   routes each probe row to its partition's bounds and InList / hash table.
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
    inner: Mutex<AccumulatorState>,
    /// Wakes every partition that is parked in [`Self::wait_for_completion`]
    /// once [`AccumulatorState::completion`] transitions to
    /// [`CompletionState::Ready`]. Notifications are fired once per
    /// accumulator lifetime (the elected finalizer publishes the terminal
    /// result, then broadcasts), so late subscribers simply re-check the
    /// state under the mutex and return immediately.
    completion_notify: Notify,
    /// Dynamic filter for pushdown to probe side
    dynamic_filter: Arc<DynamicFilterPhysicalExpr>,
    /// Right side join expressions needed for creating filter expressions
    on_right: Vec<PhysicalExprRef>,
    /// Random state for partitioning (RepartitionExec's hash function with 0,0,0,0 seeds)
    /// Used for PartitionedHashLookupPhysicalExpr
    repartition_random_state: SeededRandomState,
    /// Schema of the probe (right) side for evaluating filter expressions
    probe_schema: Arc<Schema>,
    /// Probe-side Range routing metadata for partitioned dynamic filters.
    probe_range_partitioning: Option<RangePartitioning>,
    /// Null equality of the join. Under `NullEqualsNull` a probe-side NULL can match a
    /// build-side NULL, so the pushed filter must keep NULL rows here too.
    null_equality: NullEquality,
    /// Null-aware anti join (`NOT IN`). A probe-side NULL must reach the join so its
    /// three-valued logic can collapse the result, so the pushed filter keeps NULL rows.
    null_aware: Option<NullAwareMode>,
    /// Options for the global (unrouted) filter
    global_filter_options: GlobalFilterOptions,
}

/// Strategy for filter pushdown (decided at collection time)
#[derive(Clone)]
pub(crate) enum PushdownStrategy {
    /// Use InList for small build sides (< 128MB)
    InList(ArrayRef),
    /// Use map lookup for large build sides. If `Some` the second field
    /// represents the build side's keys using a bucket bitmap.
    Map(Arc<Map>, Option<Arc<KeyRangeBitmap>>),
    /// There was no data in this partition, do not build a dynamic filter for it
    Empty,
}

/// Build-side data reported by a single partition
pub(crate) enum PartitionBuildData {
    Partitioned {
        partition_id: usize,
        pushdown: PushdownStrategy,
        bounds: PartitionBounds,
        keys_have_null: bool,
        build_keys: BuildKeys,
    },
    CollectLeft {
        pushdown: PushdownStrategy,
        bounds: PartitionBounds,
        keys_have_null: bool,
        build_keys: BuildKeys,
    },
}

/// Per-partition accumulated data (Partitioned mode)
#[derive(Clone)]
struct PartitionData {
    bounds: PartitionBounds,
    pushdown: PushdownStrategy,
    /// Whether any build key of this partition is NULL. Decides whether the pushed
    /// filter must keep probe-side NULL rows for a null-equal join to match them.
    keys_have_null: bool,
    /// The build side keys, for the global filter
    build_keys: BuildKeys,
}

/// Build-side data organized by partition mode
enum AccumulatedBuildData {
    Partitioned {
        partitions: Vec<PartitionStatus>,
        completed_partitions: usize,
    },
    CollectLeft {
        data: PartitionStatus,
        reported_count: usize,
        expected_reports: usize,
    },
}

enum CompletionState {
    Pending,
    Finalizing,
    Ready(SharedResult<()>),
}

struct AccumulatorState {
    data: AccumulatedBuildData,
    completion: CompletionState,
}

#[derive(Clone)]
enum PartitionStatus {
    Pending,
    Reported(PartitionData),
    CanceledUnknown,
}

#[derive(Clone)]
enum FinalizeInput {
    Partitioned(Vec<PartitionStatus>),
    CollectLeft(PartitionStatus),
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
    #[expect(clippy::too_many_arguments)]
    pub(crate) fn new_from_partition_mode(
        partition_mode: PartitionMode,
        left_child: &dyn ExecutionPlan,
        right_child: &dyn ExecutionPlan,
        dynamic_filter: Arc<DynamicFilterPhysicalExpr>,
        on_right: Vec<PhysicalExprRef>,
        repartition_random_state: SeededRandomState,
        null_equality: NullEquality,
        null_aware: Option<NullAwareMode>,
        global_filter_options: GlobalFilterOptions,
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
                    PartitionStatus::Pending;
                    left_child.output_partitioning().partition_count()
                ],
                completed_partitions: 0,
            },
            PartitionMode::CollectLeft => AccumulatedBuildData::CollectLeft {
                data: PartitionStatus::Pending,
                reported_count: 0,
                expected_reports: expected_calls,
            },
            PartitionMode::Auto => unreachable!(
                "PartitionMode::Auto should not be present at execution time. This is a bug in DataFusion, please report it!"
            ),
        };

        let probe_range_partitioning =
            match (partition_mode, right_child.output_partitioning()) {
                (PartitionMode::Partitioned, Partitioning::Range(range)) => {
                    Some(range.clone())
                }
                _ => None,
            };

        Self {
            inner: Mutex::new(AccumulatorState {
                data: mode_data,
                completion: CompletionState::Pending,
            }),
            completion_notify: Notify::new(),
            dynamic_filter,
            on_right,
            repartition_random_state,
            probe_schema: right_child.schema(),
            probe_range_partitioning,
            null_equality,
            null_aware,
            global_filter_options,
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
        let finalize_input = {
            let mut guard = self.inner.lock();
            self.store_build_data(&mut guard, data)?;
            self.take_finalize_input_if_ready(&mut guard)
        };

        if let Some(finalize_input) = finalize_input {
            self.finish(finalize_input);
        }

        self.wait_for_completion().await
    }

    pub(crate) fn report_canceled_partition(&self, partition_id: usize) {
        let finalize_input = {
            let mut guard = self.inner.lock();
            self.store_canceled_partition(&mut guard, partition_id);
            self.take_finalize_input_if_ready(&mut guard)
        };

        if let Some(finalize_input) = finalize_input {
            self.finish(finalize_input);
        }
    }

    fn store_build_data(
        &self,
        guard: &mut AccumulatorState,
        data: PartitionBuildData,
    ) -> Result<()> {
        match (data, &mut guard.data) {
            (
                PartitionBuildData::Partitioned {
                    partition_id,
                    pushdown,
                    bounds,
                    keys_have_null,
                    build_keys,
                },
                AccumulatedBuildData::Partitioned {
                    partitions,
                    completed_partitions,
                },
            ) => {
                if matches!(partitions[partition_id], PartitionStatus::Pending) {
                    *completed_partitions += 1;
                }
                partitions[partition_id] = PartitionStatus::Reported(PartitionData {
                    bounds,
                    pushdown,
                    keys_have_null,
                    build_keys,
                });
            }
            (
                PartitionBuildData::CollectLeft {
                    pushdown,
                    bounds,
                    keys_have_null,
                    build_keys,
                },
                AccumulatedBuildData::CollectLeft {
                    data,
                    reported_count,
                    ..
                },
            ) => {
                if matches!(data, PartitionStatus::Pending) {
                    *data = PartitionStatus::Reported(PartitionData {
                        bounds,
                        pushdown,
                        keys_have_null,
                        build_keys,
                    });
                }
                *reported_count += 1;
            }
            _ => {
                return datafusion_common::internal_err!(
                    "Build data mode mismatch in report_build_data"
                );
            }
        }
        Ok(())
    }

    fn store_canceled_partition(
        &self,
        guard: &mut AccumulatorState,
        partition_id: usize,
    ) {
        if let AccumulatedBuildData::Partitioned {
            partitions,
            completed_partitions,
        } = &mut guard.data
            && matches!(partitions[partition_id], PartitionStatus::Pending)
        {
            partitions[partition_id] = PartitionStatus::CanceledUnknown;
            *completed_partitions += 1;
        }
    }

    fn take_finalize_input_if_ready(
        &self,
        guard: &mut AccumulatorState,
    ) -> Option<FinalizeInput> {
        if !matches!(guard.completion, CompletionState::Pending) {
            return None;
        }

        let finalize_input = match &guard.data {
            AccumulatedBuildData::Partitioned {
                partitions,
                completed_partitions,
            } if *completed_partitions == partitions.len() => {
                Some(FinalizeInput::Partitioned(partitions.clone()))
            }
            AccumulatedBuildData::CollectLeft {
                data,
                reported_count,
                expected_reports,
            } if *reported_count == *expected_reports => {
                Some(FinalizeInput::CollectLeft(data.clone()))
            }
            _ => None,
        }?;

        guard.completion = CompletionState::Finalizing;
        Some(finalize_input)
    }

    fn finish(&self, finalize_input: FinalizeInput) {
        let result = self.build_filter(finalize_input).map_err(Arc::new);
        self.dynamic_filter.mark_complete();

        let mut guard = self.inner.lock();
        guard.completion = CompletionState::Ready(result);
        drop(guard);
        self.completion_notify.notify_waiters();
    }

    async fn wait_for_completion(&self) -> Result<()> {
        loop {
            let notified = {
                let guard = self.inner.lock();
                match &guard.completion {
                    CompletionState::Ready(Ok(())) => return Ok(()),
                    CompletionState::Ready(Err(err)) => {
                        return Err(DataFusionError::Shared(Arc::clone(err)));
                    }
                    CompletionState::Pending | CompletionState::Finalizing => {
                        self.completion_notify.notified()
                    }
                }
            };
            notified.await;
        }
    }

    fn build_filter(&self, finalize_input: FinalizeInput) -> Result<()> {
        match finalize_input {
            FinalizeInput::CollectLeft(partition) => {
                self.build_collect_left_filter(partition)
            }
            FinalizeInput::Partitioned(partitions) => {
                self.build_partitioned_filter(partitions)
            }
        }
    }

    /// Builds the single global filter used by a collect-left join.
    fn build_collect_left_filter(&self, partition: PartitionStatus) -> Result<()> {
        // An empty build side keeps the filter unchanged, as before.
        if let PartitionStatus::Reported(data) = &partition
            && !matches!(data.pushdown, PushdownStrategy::Empty)
            && let Some(filter_expr) = self.try_build_global_filter(&[data])?
        {
            return self.dynamic_filter.update(filter_expr);
        }
        match partition {
            PartitionStatus::Reported(PartitionData {
                bounds,
                pushdown,
                keys_have_null,
                build_keys: _,
            }) => {
                let membership_expr = create_membership_predicate(
                    &self.on_right,
                    pushdown,
                    &HASH_JOIN_SEED,
                    self.probe_schema.as_ref(),
                )?;
                let bounds_expr = create_bounds_predicate(&self.on_right, &bounds);

                if let Some(filter_expr) =
                    combine_membership_and_bounds(membership_expr, bounds_expr)
                {
                    self.dynamic_filter.update(
                        self.preserve_probe_nulls(filter_expr, keys_have_null)?,
                    )?;
                }
                Ok(())
            }
            PartitionStatus::Pending => datafusion_common::internal_err!(
                "attempted to finalize collect-left dynamic filter without reported build data"
            ),
            PartitionStatus::CanceledUnknown => datafusion_common::internal_err!(
                "collect-left dynamic filter cannot finalize with canceled build data"
            ),
        }
    }

    /// Builds one routed probe-side filter from finalized partitioned build data.
    /// Empty partitions reject their routed rows, while canceled partitions stay
    /// permissive because their build contents are unknown.
    fn build_partitioned_filter(&self, partitions: Vec<PartitionStatus>) -> Result<()> {
        // The global filter needs the keys of every partition. A canceled
        // partition's keys are unknown, so then the routed filter is used: it keeps
        // every row that routes to the canceled partition.
        let reported = partitions
            .iter()
            .map(|p| match p {
                PartitionStatus::Reported(data) => Some(data),
                _ => None,
            })
            .collect::<Option<Vec<_>>>();
        if let Some(reported) = reported
            && let Some(filter_expr) = self.try_build_global_filter(&reported)?
        {
            return self.dynamic_filter.update(filter_expr);
        }

        let mut partition_filters = Vec::with_capacity(partitions.len());
        let mut real_partition_ids = Vec::new();
        let mut empty_partition_ids = Vec::new();
        let mut has_canceled_unknown = false;
        let mut keys_have_null = false;

        for (partition_id, partition) in partitions.into_iter().enumerate() {
            match partition {
                PartitionStatus::Reported(PartitionData {
                    pushdown: PushdownStrategy::Empty,
                    ..
                }) => {
                    empty_partition_ids.push(partition_id);
                    partition_filters.push(lit(false));
                }
                PartitionStatus::Reported(PartitionData {
                    bounds,
                    pushdown,
                    keys_have_null: partition_keys_have_null,
                    build_keys: _,
                }) => {
                    real_partition_ids.push(partition_id);
                    keys_have_null |= partition_keys_have_null;
                    let membership_expr = create_membership_predicate(
                        &self.on_right,
                        pushdown,
                        &HASH_JOIN_SEED,
                        self.probe_schema.as_ref(),
                    )?;
                    let bounds_expr = create_bounds_predicate(&self.on_right, &bounds);
                    let then_expr =
                        combine_membership_and_bounds(membership_expr, bounds_expr)
                            .unwrap_or_else(|| lit(true));
                    partition_filters.push(then_expr);
                }
                PartitionStatus::CanceledUnknown => {
                    has_canceled_unknown = true;
                    partition_filters.push(lit(true));
                    // A canceled partition's build content is unknown, so it
                    // may hold a NULL key.
                    keys_have_null = true;
                }
                PartitionStatus::Pending => {
                    return datafusion_common::internal_err!(
                        "attempted to finalize dynamic filter with pending partition"
                    );
                }
            }
        }

        let all_partitions_canceled = has_canceled_unknown
            && real_partition_ids.is_empty()
            && empty_partition_ids.is_empty();
        let all_partitions_empty = !has_canceled_unknown && real_partition_ids.is_empty();
        let one_non_empty_partition =
            !has_canceled_unknown && real_partition_ids.len() == 1;

        let filter_expr = if all_partitions_canceled {
            // No build data is known, so filtering any probe row could discard a match.
            lit(true)
        } else if all_partitions_empty {
            // No build row exists, so no probe row can match.
            lit(false)
        } else if one_non_empty_partition {
            // Only one build partition contains rows, so its filter covers every
            // possible probe match without routing.
            Arc::clone(&partition_filters[real_partition_ids[0]])
        } else {
            // Builds the shared sparse `CASE` for partition filter routing.
            // Without cancellation, omitted branches are known empty and safely fall
            // through to `ELSE false`. With cancellation, omitted canceled partitions
            // have unknown contents and must fall through to `ELSE true`, so known-empty
            // partitions are emitted explicitly as false branches.
            let mut branches = if has_canceled_unknown {
                empty_partition_ids
                    .iter()
                    .map(|&partition_id| (lit(partition_id as u64), lit(false)))
                    .collect::<Vec<_>>()
            } else {
                vec![]
            };
            branches.extend(real_partition_ids.iter().map(|&partition_id| {
                (
                    lit(partition_id as u64),
                    Arc::clone(&partition_filters[partition_id]),
                )
            }));

            let routing_expr = if let Some(range_partitioning) =
                &self.probe_range_partitioning
            {
                // Routes probe rows with the partition id selected by [`RangeExpr`].
                // CASE range_partition(keys)
                //   WHEN empty_partition_id THEN false  -- only when cancellation exists
                //   WHEN real_partition_id THEN F(real_partition_id)
                //   ...
                //   ELSE has_canceled_unknown
                // END
                assert_or_internal_err!(
                    partition_filters.len() == range_partitioning.partition_count(),
                    "Dynamic filter partition count {} does not match Range partition count {}",
                    partition_filters.len(),
                    range_partitioning.partition_count()
                );
                Arc::new(RangeExpr::try_new_with_schema(
                    self.on_right.clone(),
                    range_partitioning,
                    &self.probe_schema,
                )?) as Arc<dyn PhysicalExpr>
            } else {
                // Routes probe rows with the same `hash(keys) % partition_count` expression used
                // by Hash repartitioning.
                // CASE hash(keys) % partition_count
                //   WHEN empty_partition_id THEN false  -- only when cancellation exists
                //   WHEN real_partition_id THEN F(real_partition_id)
                //   ...
                //   ELSE has_canceled_unknown
                // END
                let routing_hash_expr = Arc::new(HashExpr::new(
                    self.on_right.clone(),
                    self.repartition_random_state.clone(),
                    "hash_repartition".to_string(),
                )) as Arc<dyn PhysicalExpr>;
                Arc::new(BinaryExpr::new(
                    routing_hash_expr,
                    Operator::Modulo,
                    lit(partition_filters.len() as u64),
                )) as Arc<dyn PhysicalExpr>
            };

            Arc::new(CaseExpr::try_new(
                Some(routing_expr),
                branches,
                Some(lit(has_canceled_unknown)),
            )?)
        };

        self.dynamic_filter
            .update(self.preserve_probe_nulls(filter_expr, keys_have_null)?)
    }

    /// Builds one filter for all probe rows from the build data of every partition,
    /// with no routing to partitions:
    ///
    /// ```text
    /// [key IS NULL OR] key >= min AND key <= max AND membership(key)
    /// ```
    ///
    /// `min` and `max` are the global bounds. `membership` is an InList of the
    /// keys of all partitions when they have at most
    /// `hash_join_dynamic_filter_inlist_max_distinct_values` distinct keys together,
    /// and a [`BloomFilterExpr`] otherwise. The filter is true for every build
    /// key, whatever the partitioning of the build and probe sides, so it is
    /// correct for hash, range and collect-left joins.
    ///
    /// Returns `None` when no global membership test is possible (a key type that
    /// the bloom filter does not support). Then the caller builds the
    /// per-partition filter.
    fn try_build_global_filter(
        &self,
        partitions: &[&PartitionData],
    ) -> Result<Option<Arc<dyn PhysicalExpr>>> {
        let non_empty: Vec<&PartitionData> = partitions
            .iter()
            .copied()
            .filter(|p| !matches!(p.pushdown, PushdownStrategy::Empty))
            .collect();
        if non_empty.is_empty() {
            // No build row exists, so no probe row can match.
            return Ok(Some(lit(false)));
        }
        let keys_have_null = non_empty.iter().any(|p| p.keys_have_null);
        let bounds = PartitionBounds::merge(non_empty.iter().map(|p| &p.bounds));
        let num_distinct: usize =
            non_empty.iter().map(|p| p.build_keys.num_distinct).sum();

        let in_lists = non_empty
            .iter()
            .map(|p| match &p.pushdown {
                PushdownStrategy::InList(values) => Some(Arc::clone(values)),
                _ => None,
            })
            .collect::<Option<Vec<_>>>();
        let membership_expr = match in_lists {
            // The partitions hold disjoint keys, so the sum of their distinct
            // counts is the number of distinct keys of the build side.
            Some(in_lists)
                if num_distinct
                    <= self.global_filter_options.inlist_max_distinct_values =>
            {
                let arrays: Vec<&dyn Array> =
                    in_lists.iter().map(|a| a.as_ref()).collect();
                let values = arrow::compute::concat(&arrays)?;
                create_membership_predicate(
                    &self.on_right,
                    PushdownStrategy::InList(values),
                    &HASH_JOIN_SEED,
                    self.probe_schema.as_ref(),
                )?
            }
            _ => {
                let supported = self.on_right.iter().try_fold(true, |acc, expr| {
                    Ok::<_, DataFusionError>(
                        acc && bloom_filter::supports_data_type(
                            &expr.data_type(&self.probe_schema)?,
                        ),
                    )
                })?;
                if !supported {
                    return Ok(None);
                }
                let pruning_bitmap =
                    self.merge_pruning_bitmaps(&non_empty, bounds.as_ref());
                Some(Arc::new(BloomFilterExpr::new(
                    self.on_right.clone(),
                    non_empty
                        .iter()
                        .map(|p| p.build_keys.keys.clone())
                        .collect(),
                    num_distinct,
                    self.global_filter_options.bloom_filter,
                    self.global_filter_options.memory_pool.clone(),
                    pruning_bitmap,
                )) as Arc<dyn PhysicalExpr>)
            }
        };
        let bounds_expr = bounds
            .as_ref()
            .and_then(|b| create_bounds_predicate(&self.on_right, b));
        let filter_expr = combine_membership_and_bounds(membership_expr, bounds_expr)
            .unwrap_or_else(|| lit(true));
        Ok(Some(
            self.preserve_probe_nulls(filter_expr, keys_have_null)?,
        ))
    }

    /// One [`KeyRangeBitmap`] for the keys of all partitions, so that scans can
    /// prune containers (for example parquet row groups and pages) with the
    /// global filter. Only for a single integer key.
    fn merge_pruning_bitmaps(
        &self,
        partitions: &[&PartitionData],
        bounds: Option<&PartitionBounds>,
    ) -> Option<Arc<KeyRangeBitmap>> {
        if self.on_right.len() != 1 {
            return None;
        }
        let column_bounds = bounds?.get_column_bounds(0)?;
        let parts = partitions
            .iter()
            .map(|p| match &p.pushdown {
                PushdownStrategy::Map(_, Some(bitmap)) => {
                    Some(KeyRangePart::Bitmap(bitmap.as_ref()))
                }
                _ => p
                    .build_keys
                    .keys
                    .first()
                    .map(|keys| KeyRangePart::Keys(keys.as_ref())),
            })
            .collect::<Option<Vec<_>>>()?;
        KeyRangeBitmap::try_union(&parts, &column_bounds.min, &column_bounds.max)
            .map(Arc::new)
    }

    /// Keeps probe rows with a NULL key when the join semantics need them.
    ///
    /// The build-side predicate drops probe rows whose key is NULL. A null-aware join
    /// needs a NULL value key to reach the join for `NOT IN` three-valued logic, and a
    /// null-equal join needs NULL keys to match build-side NULLs.
    ///
    /// For a null-aware join, only the `NOT IN` value keys `on_right[..V]` need the
    /// escape, while `on_right[V..]` are correlation scope keys: a NULL in any element
    /// of a multi-column value key can make the tuple comparison UNKNOWN. For a
    /// null-equal join, every nullable key needs the escape.
    fn preserve_probe_nulls(
        &self,
        filter_expr: Arc<dyn PhysicalExpr>,
        build_keys_have_null: bool,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        // A null-aware anti join needs every probe NULL no matter what the build holds: one
        // probe NULL makes `NOT IN` unknown for every build row. A null-equal join needs probe
        // NULLs only to match an actual build-side NULL, so a NULL-free build keeps the filter
        // at full selectivity.
        let needs_probe_nulls = self.null_aware.is_some()
            || (self.null_equality == NullEquality::NullEqualsNull
                && build_keys_have_null);
        if !needs_probe_nulls {
            return Ok(filter_expr);
        }
        let keys = match self.null_aware {
            Some(mode) => {
                let value_keys = mode.value_keys();
                assert_or_internal_err!(
                    (1..=self.on_right.len()).contains(&value_keys),
                    "null-aware join must have between 1 and {} value keys, got {value_keys}",
                    self.on_right.len()
                );
                &self.on_right[..value_keys]
            }
            None => self.on_right.as_slice(),
        };

        // Only a key that can actually be NULL needs the disjunct; a NOT NULL key
        // never widens the filter.
        let mut any_key_is_null: Option<Arc<dyn PhysicalExpr>> = None;
        for key in keys {
            // `nullable` fails only when a key is out of sync with the probe schema. That is
            // a construction bug, so surface it instead of widening around it.
            if !key.nullable(&self.probe_schema)? {
                continue;
            }
            let is_null =
                Arc::new(IsNullExpr::new(Arc::clone(key))) as Arc<dyn PhysicalExpr>;
            any_key_is_null = Some(match any_key_is_null {
                Some(acc) => Arc::new(BinaryExpr::new(acc, Operator::Or, is_null)) as _,
                None => is_null,
            });
        }
        // Cheap null check first short-circuits before the costlier dynamic filter.
        Ok(match any_key_is_null {
            Some(any_key_is_null) => {
                Arc::new(BinaryExpr::new(any_key_is_null, Operator::Or, filter_expr))
            }
            None => filter_expr,
        })
    }
}

impl fmt::Debug for SharedBuildAccumulator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SharedBuildAccumulator")
    }
}

#[cfg(test)]
pub(super) fn make_partitioned_accumulator_for_test(
    num_partitions: usize,
) -> SharedBuildAccumulator {
    let probe_schema = Arc::new(Schema::new(vec![Field::new(
        "probe_key",
        DataType::Int32,
        false,
    )]));
    let dynamic_filter = Arc::new(DynamicFilterPhysicalExpr::new(vec![], lit(true)));
    SharedBuildAccumulator {
        inner: Mutex::new(AccumulatorState {
            data: AccumulatedBuildData::Partitioned {
                partitions: vec![PartitionStatus::Pending; num_partitions],
                completed_partitions: 0,
            },
            completion: CompletionState::Pending,
        }),
        completion_notify: Notify::new(),
        dynamic_filter,
        on_right: vec![],
        repartition_random_state: SeededRandomState::with_seed(1),
        probe_schema,
        probe_range_partitioning: None,
        null_equality: NullEquality::NullEqualsNothing,
        null_aware: None,
        global_filter_options: GlobalFilterOptions::default(),
    }
}

#[cfg(test)]
pub(super) fn completed_partitions_for_test(acc: &SharedBuildAccumulator) -> usize {
    let guard = acc.inner.lock();
    let AccumulatedBuildData::Partitioned {
        completed_partitions,
        ..
    } = &guard.data
    else {
        panic!("expected partitioned accumulator");
    };
    *completed_partitions
}

#[cfg(test)]
mod tests {
    use super::*;

    use arrow::array::{ArrayRef, BooleanArray, Float64Array, Int32Array};
    use arrow::compute::SortOptions;
    use arrow::record_batch::RecordBatch;
    use datafusion_common::SplitPoint;
    use datafusion_physical_expr::{
        PhysicalSortExpr,
        expressions::{Column, Literal},
    };

    fn test_on_right() -> Vec<PhysicalExprRef> {
        vec![Arc::new(Column::new("probe_key", 0))]
    }

    fn test_probe_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![Field::new(
            "probe_key",
            DataType::Int32,
            false,
        )]))
    }

    fn test_dynamic_filter(
        on_right: &[PhysicalExprRef],
    ) -> Arc<DynamicFilterPhysicalExpr> {
        Arc::new(DynamicFilterPhysicalExpr::new(on_right.to_vec(), lit(true)))
    }

    fn make_accumulator_for_test(
        data: AccumulatedBuildData,
        on_right: Vec<PhysicalExprRef>,
    ) -> SharedBuildAccumulator {
        let dynamic_filter = test_dynamic_filter(&on_right);
        SharedBuildAccumulator {
            inner: Mutex::new(AccumulatorState {
                data,
                completion: CompletionState::Pending,
            }),
            completion_notify: Notify::new(),
            dynamic_filter,
            on_right,
            repartition_random_state: SeededRandomState::with_seed(1),
            probe_schema: test_probe_schema(),
            probe_range_partitioning: None,
            null_equality: NullEquality::NullEqualsNothing,
            null_aware: None,
            global_filter_options: GlobalFilterOptions::default(),
        }
    }

    fn make_collect_left_accumulator_for_test() -> SharedBuildAccumulator {
        make_accumulator_for_test(
            AccumulatedBuildData::CollectLeft {
                data: PartitionStatus::Pending,
                reported_count: 0,
                expected_reports: 1,
            },
            test_on_right(),
        )
    }

    fn make_partitioned_expr_accumulator_for_test(
        num_partitions: usize,
    ) -> SharedBuildAccumulator {
        make_accumulator_for_test(
            AccumulatedBuildData::Partitioned {
                partitions: vec![PartitionStatus::Pending; num_partitions],
                completed_partitions: 0,
            },
            test_on_right(),
        )
    }

    fn in_list(values: &[i32]) -> PushdownStrategy {
        PushdownStrategy::InList(Arc::new(Int32Array::from(values.to_vec())) as ArrayRef)
    }

    fn bounds(min: i32, max: i32) -> PartitionBounds {
        PartitionBounds::new(vec![ColumnBounds::new(
            ScalarValue::Int32(Some(min)),
            ScalarValue::Int32(Some(max)),
        )])
    }

    fn no_bounds() -> PartitionBounds {
        PartitionBounds::new(vec![])
    }

    fn reported(pushdown: PushdownStrategy, bounds: PartitionBounds) -> PartitionStatus {
        PartitionStatus::Reported(PartitionData {
            pushdown,
            bounds,
            keys_have_null: false,
            build_keys: BuildKeys::default(),
        })
    }

    fn current_expr(acc: &SharedBuildAccumulator) -> PhysicalExprRef {
        acc.dynamic_filter
            .current()
            .expect("dynamic filter current expression should be available")
    }

    fn in_list_expr(expr: &PhysicalExprRef) -> &InListExpr {
        expr.downcast_ref::<InListExpr>()
            .expect("expected InListExpr dynamic filter")
    }

    fn assert_in_list_column_values(
        expr: &PhysicalExprRef,
        expected_column_name: &str,
        expected_column_index: usize,
        expected_values: &[i32],
    ) {
        let in_list = in_list_expr(expr);
        let column = in_list
            .expr()
            .downcast_ref::<Column>()
            .expect("expected InListExpr child column");
        assert_eq!(column.name(), expected_column_name);
        assert_eq!(column.index(), expected_column_index);

        let actual_values = in_list
            .list()
            .iter()
            .map(|expr| {
                let literal = expr
                    .downcast_ref::<Literal>()
                    .expect("expected InListExpr literal value");
                match literal.value() {
                    ScalarValue::Int32(Some(value)) => *value,
                    value => panic!("expected Int32 in-list value, got {value:?}"),
                }
            })
            .collect::<Vec<_>>();
        assert_eq!(actual_values, expected_values);
    }

    fn binary_expr(expr: &PhysicalExprRef) -> &BinaryExpr {
        expr.downcast_ref::<BinaryExpr>()
            .expect("expected BinaryExpr dynamic filter")
    }

    fn case_expr(expr: &PhysicalExprRef) -> &CaseExpr {
        expr.downcast_ref::<CaseExpr>()
            .expect("expected CaseExpr dynamic filter")
    }

    fn assert_literal_bool(expr: &PhysicalExprRef, expected: bool) {
        let literal = expr
            .downcast_ref::<Literal>()
            .expect("expected literal bool dynamic filter");
        assert_eq!(literal.value(), &ScalarValue::Boolean(Some(expected)));
    }

    fn assert_top_binary_op(expr: &PhysicalExprRef, expected: Operator) {
        assert_eq!(binary_expr(expr).op(), &expected);
    }

    fn partitioned_state(acc: &SharedBuildAccumulator) -> (Vec<PartitionStatus>, usize) {
        let guard = acc.inner.lock();
        let AccumulatedBuildData::Partitioned {
            partitions,
            completed_partitions,
        } = &guard.data
        else {
            panic!("expected partitioned accumulator");
        };
        (partitions.clone(), *completed_partitions)
    }

    /// A partition that reports a hash table (more keys than the InList limit).
    fn reported_map(keys: Vec<i32>) -> PartitionStatus {
        let (min, max) = (*keys.iter().min().unwrap(), *keys.iter().max().unwrap());
        let num_distinct = keys.len();
        PartitionStatus::Reported(PartitionData {
            pushdown: PushdownStrategy::Map(
                Arc::new(Map::HashMap(Box::new(
                    crate::joins::join_hash_map::JoinHashMapU32::with_capacity(0),
                ))),
                None,
            ),
            bounds: bounds(min, max),
            keys_have_null: false,
            build_keys: BuildKeys {
                keys: vec![Arc::new(Int32Array::from(keys))],
                num_distinct,
            },
        })
    }

    fn eval_bool(expr: &PhysicalExprRef, keys: Vec<i32>) -> Vec<bool> {
        let batch = RecordBatch::try_new(
            test_probe_schema(),
            vec![Arc::new(Int32Array::from(keys))],
        )
        .unwrap();
        let result = expr
            .evaluate(&batch)
            .unwrap()
            .into_array(batch.num_rows())
            .unwrap();
        let result = result.as_any().downcast_ref::<BooleanArray>().unwrap();
        result.iter().map(|v| v.unwrap_or(false)).collect()
    }

    /// Hash partitions with many keys get one unrouted filter:
    /// `key >= global_min AND key <= global_max AND bloom_filter`.
    #[test]
    fn partitioned_global_bloom_filter() {
        let acc = make_partitioned_expr_accumulator_for_test(3);
        acc.build_filter(FinalizeInput::Partitioned(vec![
            reported_map((0..1000).map(|k| k * 3).collect()),
            reported(PushdownStrategy::Empty, no_bounds()),
            reported_map((0..1000).map(|k| k * 3 + 10_000).collect()),
        ]))
        .unwrap();

        let expr = current_expr(&acc);
        let text = format!("{expr}");
        assert_eq!(
            text, "probe_key@0 >= 0 AND probe_key@0 <= 12997 AND bloom_filter",
            "{text}"
        );
        assert!(!text.contains("CASE"));

        let probe: Vec<i32> = (-10..13_010).collect();
        let result = eval_bool(&expr, probe.clone());
        let mut false_positives = 0;
        for (k, pass) in probe.iter().zip(result) {
            let is_key = (*k >= 0 && *k < 3000 && k % 3 == 0)
                || (*k >= 10_000 && *k < 13_000 && (k - 10_000) % 3 == 0);
            assert!(pass || !is_key, "build key {k} dropped");
            false_positives += (pass && !is_key) as usize;
        }
        // Bounds remove everything outside [0, 12997]; the bloom filter removes
        // most other keys
        assert!(false_positives < 300, "{false_positives} false positives");
    }

    /// Partitions that each fit an InList but together exceed the InList limit get
    /// a bloom filter of all their keys.
    #[test]
    fn partitioned_inlists_over_global_limit_use_bloom_filter() {
        let mut acc = make_partitioned_expr_accumulator_for_test(2);
        acc.global_filter_options.inlist_max_distinct_values = 3;
        let in_list_partition = |keys: Vec<i32>| {
            PartitionStatus::Reported(PartitionData {
                pushdown: in_list(&keys),
                bounds: bounds(*keys.iter().min().unwrap(), *keys.iter().max().unwrap()),
                keys_have_null: false,
                build_keys: BuildKeys {
                    num_distinct: keys.len(),
                    keys: vec![Arc::new(Int32Array::from(keys))],
                },
            })
        };
        acc.build_filter(FinalizeInput::Partitioned(vec![
            in_list_partition(vec![1, 3]),
            in_list_partition(vec![5, 7]),
        ]))
        .unwrap();
        let expr = current_expr(&acc);
        assert!(format!("{expr}").ends_with("AND bloom_filter"), "{expr}");
        let result = eval_bool(&expr, (0..10).collect());
        for k in [1, 3, 5, 7] {
            assert!(result[k], "build key {k} dropped");
        }
        assert!(!result[0] && !result[8] && !result[9]);

        // At the limit: one InList with the keys of both partitions
        let mut acc = make_partitioned_expr_accumulator_for_test(2);
        acc.global_filter_options.inlist_max_distinct_values = 4;
        acc.build_filter(FinalizeInput::Partitioned(vec![
            in_list_partition(vec![1, 3]),
            in_list_partition(vec![5, 7]),
        ]))
        .unwrap();
        let expr = current_expr(&acc);
        let and = binary_expr(&expr);
        assert_in_list_column_values(and.right(), "probe_key", 0, &[1, 3, 5, 7]);
    }

    /// A partition whose only key is NULL has NULL bounds. The merged bounds
    /// ignore them, so the filter keeps the non-NULL keys of the other partitions.
    #[test]
    fn merged_bounds_ignore_null_bounds() {
        let null_bounds = PartitionBounds::new(vec![ColumnBounds::new(
            ScalarValue::Int32(None),
            ScalarValue::Int32(None),
        )]);
        let merged = PartitionBounds::merge(
            [&null_bounds, &bounds(11, 11), &null_bounds].into_iter(),
        )
        .unwrap();
        assert_eq!(
            merged.get_column_bounds(0).unwrap(),
            &ColumnBounds::new(
                ScalarValue::Int32(Some(11)),
                ScalarValue::Int32(Some(11))
            )
        );
        let merged =
            PartitionBounds::merge([&bounds(5, 6), &null_bounds].into_iter()).unwrap();
        assert_eq!(
            merged.get_column_bounds(0).unwrap(),
            &ColumnBounds::new(ScalarValue::Int32(Some(5)), ScalarValue::Int32(Some(6)))
        );
    }

    /// Hash and range partitioning both use the same global filter: it holds every
    /// build key, so it does not depend on the routing.
    #[test]
    fn range_partitioned_uses_global_filter() -> Result<()> {
        let mut acc = make_partitioned_expr_accumulator_for_test(2);
        acc.probe_range_partitioning = Some(RangePartitioning::try_new(
            [PhysicalSortExpr::new(
                Arc::clone(&acc.on_right[0]),
                Default::default(),
            )]
            .into(),
            vec![SplitPoint::new(vec![ScalarValue::Int32(Some(1000))])],
        )?);
        acc.build_filter(FinalizeInput::Partitioned(vec![
            reported_map((0..500).map(|k| k * 2).collect()),
            reported_map((0..500).map(|k| 1000 + k * 2).collect()),
        ]))?;
        let expr = current_expr(&acc);
        assert!(!format!("{expr}").contains("CASE"), "{expr}");
        let result = eval_bool(&expr, (0..2000).collect());
        for k in (0..2000).step_by(2) {
            assert!(result[k], "build key {k} dropped");
        }
        Ok(())
    }

    /// A canceled partition's keys are unknown, so the global filter is not
    /// possible: the routed filter keeps every row of the canceled route.
    #[test]
    fn partitioned_canceled_partition_falls_back_to_routed_filter() {
        let acc = make_partitioned_expr_accumulator_for_test(2);
        acc.build_filter(FinalizeInput::Partitioned(vec![
            reported_map((0..1000).collect()),
            PartitionStatus::CanceledUnknown,
        ]))
        .unwrap();
        let expr = current_expr(&acc);
        let case = case_expr(&expr);
        assert_literal_bool(
            case.else_expr().expect("expected permissive fallback"),
            true,
        );
        assert!(!format!("{expr}").contains("bloom_filter"));
    }

    /// Key types that the bloom filter does not support (floats) keep the routed
    /// per-partition filter.
    #[test]
    fn unsupported_key_type_falls_back_to_routed_filter() {
        let mut acc = make_partitioned_expr_accumulator_for_test(2);
        acc.probe_schema = Arc::new(Schema::new(vec![Field::new(
            "probe_key",
            DataType::Float64,
            false,
        )]));
        let float_partition = |keys: Vec<f64>| {
            PartitionStatus::Reported(PartitionData {
                pushdown: PushdownStrategy::Map(
                    Arc::new(Map::HashMap(Box::new(
                        crate::joins::join_hash_map::JoinHashMapU32::with_capacity(0),
                    ))),
                    None,
                ),
                bounds: no_bounds(),
                keys_have_null: false,
                build_keys: BuildKeys {
                    num_distinct: keys.len(),
                    keys: vec![Arc::new(Float64Array::from(keys))],
                },
            })
        };
        acc.build_filter(FinalizeInput::Partitioned(vec![
            float_partition((0..1000).map(f64::from).collect()),
            float_partition((1000..2000).map(f64::from).collect()),
        ]))
        .unwrap();
        let expr = current_expr(&acc);
        case_expr(&expr);
    }

    /// With a memory pool too small for the bloom filter, the global filter
    /// keeps every build key: it falls back to the bounds (and the pruning
    /// bitmap), and the join results stay correct.
    #[test]
    fn small_memory_pool_falls_back_to_bounds() {
        use datafusion_execution::memory_pool::GreedyMemoryPool;
        let mut acc = make_partitioned_expr_accumulator_for_test(2);
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1024));
        acc.global_filter_options.memory_pool = Some(Arc::clone(&pool));
        acc.build_filter(FinalizeInput::Partitioned(vec![
            reported_map((0..1000).map(|k| k * 4).collect()),
            reported_map((0..1000).map(|k| k * 4 + 1).collect()),
        ]))
        .unwrap();
        let expr = current_expr(&acc);
        let result = eval_bool(&expr, (-10..4010).collect());
        for (k, pass) in (-10..4010).zip(result) {
            // Bounds only: every key in [0, 3997] passes, nothing outside
            assert_eq!(pass, (0..=3997).contains(&k), "key {k}");
        }
        assert_eq!(pool.reserved(), 0);
    }

    /// The bloom filter carries one key-range bitmap for all partitions, so scans
    /// can prune containers with it.
    #[test]
    fn global_bloom_filter_has_merged_pruning_bitmap() {
        let acc = make_partitioned_expr_accumulator_for_test(2);
        acc.build_filter(FinalizeInput::Partitioned(vec![
            reported_map((0..100).collect()),
            reported_map((1_000_000..1_000_100).collect()),
        ]))
        .unwrap();
        let expr = current_expr(&acc);
        let bloom = binary_expr(&expr)
            .right()
            .downcast_ref::<BloomFilterExpr>()
            .expect("expected a bloom filter");
        let bitmap = bloom.pruning_bitmap().expect("expected a pruning bitmap");
        let may = |lo: i32, hi: i32| bitmap.may_contain_range(lo as u64, hi as u64);
        assert!(may(50, 60));
        assert!(may(1_000_050, 1_000_060));
        assert!(!may(400_000, 600_000));
    }

    #[test]
    fn collect_left_updates_with_membership_only() {
        let acc = make_collect_left_accumulator_for_test();

        acc.build_filter(FinalizeInput::CollectLeft(reported(
            in_list(&[1, 2, 3]),
            no_bounds(),
        )))
        .unwrap();

        let expr = current_expr(&acc);
        assert_in_list_column_values(&expr, "probe_key", 0, &[1, 2, 3]);
    }

    #[test]
    fn collect_left_updates_with_bounds_only() {
        let acc = make_collect_left_accumulator_for_test();

        acc.build_filter(FinalizeInput::CollectLeft(reported(
            PushdownStrategy::Empty,
            bounds(10, 20),
        )))
        .unwrap();

        let expr = current_expr(&acc);
        assert_top_binary_op(&expr, Operator::And);
    }

    #[test]
    fn collect_left_empty_build_data_does_not_update_filter() {
        let acc = make_collect_left_accumulator_for_test();
        let initial_generation = acc.dynamic_filter.snapshot_generation();

        acc.build_filter(FinalizeInput::CollectLeft(reported(
            PushdownStrategy::Empty,
            no_bounds(),
        )))
        .unwrap();

        assert_eq!(
            acc.dynamic_filter.snapshot_generation(),
            initial_generation,
            "empty CollectLeft input must not update with a no-op filter"
        );
        let expr = current_expr(&acc);
        assert_literal_bool(&expr, true);
    }

    #[test]
    fn partitioned_one_real_partition_with_rest_empty_skips_case() {
        let acc = make_partitioned_expr_accumulator_for_test(3);

        acc.build_filter(FinalizeInput::Partitioned(vec![
            reported(PushdownStrategy::Empty, no_bounds()),
            reported(in_list(&[2]), no_bounds()),
            reported(PushdownStrategy::Empty, no_bounds()),
        ]))
        .unwrap();

        let expr = current_expr(&acc);
        in_list_expr(&expr);
        assert!(expr.downcast_ref::<CaseExpr>().is_none());
    }

    #[test]
    fn partitioned_canceled_unknown_partitions_keep_unknown_routes_permissive() {
        let acc = make_partitioned_expr_accumulator_for_test(2);

        acc.build_filter(FinalizeInput::Partitioned(vec![
            PartitionStatus::CanceledUnknown,
            reported(PushdownStrategy::Empty, no_bounds()),
        ]))
        .unwrap();

        let expr = current_expr(&acc);
        let case = case_expr(&expr);
        assert_eq!(case.when_then_expr().len(), 1);
        assert_literal_bool(&case.when_then_expr()[0].1, false);
        assert_literal_bool(
            case.else_expr().expect("expected permissive fallback"),
            true,
        );
    }

    #[test]
    fn partitioned_range_dynamic_filter_routes_with_range_expr() -> Result<()> {
        let mut acc = make_partitioned_expr_accumulator_for_test(4);
        acc.probe_range_partitioning = Some(RangePartitioning::try_new(
            [PhysicalSortExpr::new(
                Arc::clone(&acc.on_right[0]),
                Default::default(),
            )]
            .into(),
            vec![
                SplitPoint::new(vec![ScalarValue::Int32(Some(10))]),
                SplitPoint::new(vec![ScalarValue::Int32(Some(20))]),
                SplitPoint::new(vec![ScalarValue::Int32(Some(30))]),
            ],
        )?);

        acc.build_filter(FinalizeInput::Partitioned(vec![
            reported(PushdownStrategy::Empty, no_bounds()),
            PartitionStatus::CanceledUnknown,
            reported(in_list(&[20, 29]), no_bounds()),
            reported(in_list(&[30]), no_bounds()),
        ]))?;

        let expr = current_expr(&acc);
        let case = case_expr(&expr);
        assert!(
            case.expr()
                .and_then(|expr| expr.downcast_ref::<RangeExpr>())
                .is_some(),
            "Range routing must use RangeExpr"
        );
        assert_eq!(case.when_then_expr().len(), 3);
        assert_literal_bool(&case.when_then_expr()[0].1, false);
        assert_literal_bool(
            case.else_expr().expect("expected permissive fallback"),
            true,
        );

        let batch = RecordBatch::try_new(
            test_probe_schema(),
            vec![Arc::new(Int32Array::from(vec![
                9, 10, 19, 20, 21, 29, 30, 31,
            ]))],
        )?;
        let result = expr.evaluate(&batch)?.into_array(batch.num_rows())?;
        let result = result
            .as_any()
            .downcast_ref::<BooleanArray>()
            .expect("dynamic filter should evaluate to BooleanArray");
        assert_eq!(
            result,
            &BooleanArray::from(vec![false, true, true, true, false, true, true, false,])
        );

        Ok(())
    }

    #[test]
    fn partitioned_range_dynamic_filter_routes_compound_nullable_keys() -> Result<()> {
        let probe_schema = Arc::new(Schema::new(vec![
            Field::new("probe_key", DataType::Int32, true),
            Field::new("probe_tie", DataType::Int32, true),
        ]));
        let on_right: Vec<PhysicalExprRef> = vec![
            Arc::new(Column::new("probe_key", 0)),
            Arc::new(Column::new("probe_tie", 1)),
        ];
        let mut acc = make_accumulator_for_test(
            AccumulatedBuildData::Partitioned {
                partitions: vec![PartitionStatus::Pending; 4],
                completed_partitions: 0,
            },
            on_right,
        );
        acc.probe_schema = Arc::clone(&probe_schema);
        acc.probe_range_partitioning = Some(RangePartitioning::try_new(
            [
                PhysicalSortExpr::new(
                    Arc::clone(&acc.on_right[0]),
                    SortOptions::new(false, true),
                ),
                PhysicalSortExpr::new(
                    Arc::clone(&acc.on_right[1]),
                    SortOptions::new(false, false),
                ),
            ]
            .into(),
            vec![
                SplitPoint::new(vec![
                    ScalarValue::Int32(None),
                    ScalarValue::Int32(Some(10)),
                ]),
                SplitPoint::new(vec![ScalarValue::Int32(None), ScalarValue::Int32(None)]),
                SplitPoint::new(vec![
                    ScalarValue::Int32(Some(10)),
                    ScalarValue::Int32(None),
                ]),
            ],
        )?);

        acc.build_filter(FinalizeInput::Partitioned(vec![
            reported(PushdownStrategy::Empty, no_bounds()),
            PartitionStatus::CanceledUnknown,
            reported(PushdownStrategy::Empty, no_bounds()),
            PartitionStatus::CanceledUnknown,
        ]))?;

        let expr = current_expr(&acc);
        let case = case_expr(&expr);
        assert!(case.expr().is_some());
        assert_eq!(case.when_then_expr().len(), 2);
        assert_literal_bool(
            case.else_expr().expect("expected permissive fallback"),
            true,
        );

        let batch = RecordBatch::try_new(
            probe_schema,
            vec![
                Arc::new(Int32Array::from(vec![
                    None,
                    None,
                    None,
                    None,
                    Some(9),
                    Some(10),
                    Some(10),
                    Some(11),
                ])),
                Arc::new(Int32Array::from(vec![
                    Some(9),
                    Some(10),
                    Some(11),
                    None,
                    None,
                    Some(9),
                    None,
                    None,
                ])),
            ],
        )?;
        let result = expr.evaluate(&batch)?.into_array(batch.num_rows())?;
        let result = result
            .as_any()
            .downcast_ref::<BooleanArray>()
            .expect("dynamic filter should evaluate to BooleanArray");
        assert_eq!(
            result,
            &BooleanArray::from(
                vec![false, true, true, false, false, false, true, true,]
            )
        );

        Ok(())
    }

    #[test]
    fn partitioned_range_dynamic_filter_preserves_signed_zero_routing() -> Result<()> {
        let probe_schema = Arc::new(Schema::new(vec![Field::new(
            "probe_key",
            DataType::Float64,
            false,
        )]));
        let on_right: Vec<PhysicalExprRef> = vec![Arc::new(Column::new("probe_key", 0))];
        let mut acc = make_accumulator_for_test(
            AccumulatedBuildData::Partitioned {
                partitions: vec![PartitionStatus::Pending; 2],
                completed_partitions: 0,
            },
            on_right,
        );
        acc.probe_schema = Arc::clone(&probe_schema);
        acc.probe_range_partitioning = Some(RangePartitioning::try_new(
            [PhysicalSortExpr::new(
                Arc::clone(&acc.on_right[0]),
                SortOptions::default(),
            )]
            .into(),
            vec![SplitPoint::new(vec![ScalarValue::Float64(Some(0.0))])],
        )?);

        acc.build_filter(FinalizeInput::Partitioned(vec![
            PartitionStatus::CanceledUnknown,
            reported(PushdownStrategy::Empty, no_bounds()),
        ]))?;

        let expr = current_expr(&acc);
        let batch = RecordBatch::try_new(
            probe_schema,
            vec![Arc::new(Float64Array::from(vec![-0.0, 0.0]))],
        )?;
        let result = expr.evaluate(&batch)?.into_array(batch.num_rows())?;
        let result = result
            .as_any()
            .downcast_ref::<BooleanArray>()
            .expect("dynamic filter should evaluate to BooleanArray");
        assert_eq!(result, &BooleanArray::from(vec![true, false]));

        Ok(())
    }

    // Regression guard for the build-report lifecycle fix: on `Drop`, a stream
    // in `BuildReportState::ReportScheduled` still calls `report_canceled_partition`
    // because it cannot tell whether the coordinator has already observed the
    // report (first poll of the `OnceFut` runs `store_build_data` synchronously
    // before the future's first `.await`, but the stream doesn't learn that
    // until `get_shared` returns `Ok`). Correctness therefore relies on
    // `store_canceled_partition` being a no-op when the partition is already
    // `Reported`. This test pins that invariant.
    #[test]
    fn report_canceled_partition_is_noop_after_report() {
        let acc = make_partitioned_accumulator_for_test(2);

        {
            let mut guard = acc.inner.lock();
            acc.store_build_data(
                &mut guard,
                PartitionBuildData::Partitioned {
                    partition_id: 0,
                    pushdown: PushdownStrategy::Empty,
                    bounds: PartitionBounds::new(vec![]),
                    keys_have_null: false,
                    build_keys: BuildKeys::default(),
                },
            )
            .unwrap();
        }
        let (partitions, completed) = partitioned_state(&acc);
        assert!(matches!(partitions[0], PartitionStatus::Reported(_)));
        assert_eq!(completed, 1);

        acc.report_canceled_partition(0);
        let (partitions, completed) = partitioned_state(&acc);
        assert!(
            matches!(partitions[0], PartitionStatus::Reported(_)),
            "late cancel must not overwrite a prior Reported status"
        );
        assert_eq!(completed, 1, "late cancel must not double-count completion");
    }

    // Drop from the `NotReported` (or first-poll-never-ran) state must
    // transition `Pending` -> `CanceledUnknown` and bump `completed_partitions`,
    // which is what unblocks sibling partitions waiting on the coordinator.
    #[test]
    fn report_canceled_partition_marks_pending_partition_canceled() {
        let acc = make_partitioned_accumulator_for_test(2);

        acc.report_canceled_partition(0);
        let (partitions, completed) = partitioned_state(&acc);
        assert!(matches!(partitions[0], PartitionStatus::CanceledUnknown));
        assert_eq!(completed, 1);

        // Idempotent: a second cancel (e.g. a stray double-drop) must not
        // double-count completion.
        acc.report_canceled_partition(0);
        let (partitions, completed) = partitioned_state(&acc);
        assert!(matches!(partitions[0], PartitionStatus::CanceledUnknown));
        assert_eq!(completed, 1);
    }

    fn null_semantics_accumulator(
        probe_schema: Arc<Schema>,
        on_right: Vec<PhysicalExprRef>,
        null_equality: NullEquality,
        null_aware: bool,
    ) -> SharedBuildAccumulator {
        SharedBuildAccumulator {
            inner: Mutex::new(AccumulatorState {
                data: AccumulatedBuildData::Partitioned {
                    partitions: vec![PartitionStatus::Pending; 1],
                    completed_partitions: 0,
                },
                completion: CompletionState::Pending,
            }),
            completion_notify: Notify::new(),
            dynamic_filter: Arc::new(DynamicFilterPhysicalExpr::new(vec![], lit(true))),
            on_right,
            repartition_random_state: SeededRandomState::with_seed(1),
            probe_schema,
            probe_range_partitioning: None,
            null_equality,
            null_aware: null_aware.then_some(NullAwareMode::LeftAnti {
                correlated: false,
                value_keys: 1,
            }),
            global_filter_options: GlobalFilterOptions::default(),
        }
    }

    fn null_equal_accumulator(
        probe_schema: Arc<Schema>,
        on_right: Vec<PhysicalExprRef>,
    ) -> SharedBuildAccumulator {
        null_semantics_accumulator(
            probe_schema,
            on_right,
            NullEquality::NullEqualsNull,
            false,
        )
    }

    fn reported_with_null_keys(
        pushdown: PushdownStrategy,
        bounds: PartitionBounds,
    ) -> PartitionStatus {
        PartitionStatus::Reported(PartitionData {
            pushdown,
            bounds,
            keys_have_null: true,
            build_keys: BuildKeys::default(),
        })
    }

    /// A partitioned accumulator whose probe key is nullable, so that a build-side
    /// NULL can actually widen the filter.
    fn null_equal_partitioned_accumulator(
        num_partitions: usize,
    ) -> SharedBuildAccumulator {
        let mut acc = make_accumulator_for_test(
            AccumulatedBuildData::Partitioned {
                partitions: vec![PartitionStatus::Pending; num_partitions],
                completed_partitions: 0,
            },
            test_on_right(),
        );
        acc.probe_schema = Arc::new(Schema::new(vec![Field::new(
            "probe_key",
            DataType::Int32,
            true,
        )]));
        acc.null_equality = NullEquality::NullEqualsNull;
        acc
    }

    /// The IS NULL disjunct must wrap the global filter as a whole when any
    /// partition reports a NULL key.
    #[test]
    fn partitioned_null_keys_in_one_partition_widen_whole_global_filter() {
        let acc = null_equal_partitioned_accumulator(2);

        acc.build_filter(FinalizeInput::Partitioned(vec![
            reported(in_list(&[1]), no_bounds()),
            reported_with_null_keys(in_list(&[2]), no_bounds()),
        ]))
        .unwrap();

        let expr = current_expr(&acc);
        assert_top_binary_op(&expr, Operator::Or);
        let widened = binary_expr(&expr);
        assert!(
            widened.left().downcast_ref::<IsNullExpr>().is_some(),
            "expected the IS NULL disjunct first, got: {expr}"
        );
        assert_in_list_column_values(widened.right(), "probe_key", 0, &[1, 2]);
        assert_eq!(format!("{expr}").matches("IS NULL").count(), 1);
    }

    /// A canceled partition's build content is unknown, so it may hold a NULL key:
    /// the aggregated flag must be permissive even when no partition reported one.
    #[test]
    fn partitioned_canceled_partition_widens_routed_filter() {
        let acc = null_equal_partitioned_accumulator(2);

        acc.build_filter(FinalizeInput::Partitioned(vec![
            PartitionStatus::CanceledUnknown,
            reported(in_list(&[2]), no_bounds()),
        ]))
        .unwrap();

        let expr = current_expr(&acc);
        assert_top_binary_op(&expr, Operator::Or);
        assert!(
            binary_expr(&expr)
                .left()
                .downcast_ref::<IsNullExpr>()
                .is_some()
        );
    }

    #[test]
    fn preserve_probe_nulls_only_widens_nullable_keys() {
        let probe_schema = Arc::new(Schema::new(vec![
            Field::new("k_nullable", DataType::Int32, true),
            Field::new("k_not_null", DataType::Int32, false),
        ]));
        let on_right: Vec<PhysicalExprRef> = vec![
            Arc::new(Column::new("k_nullable", 0)),
            Arc::new(Column::new("k_not_null", 1)),
        ];
        let acc = null_equal_accumulator(probe_schema, on_right);

        // Only the nullable key earns an IS NULL disjunct; the NOT NULL key is left out.
        let widened = acc.preserve_probe_nulls(lit(true), true).unwrap();
        assert_eq!(format!("{widened}").matches("IS NULL").count(), 1);
    }

    #[test]
    fn preserve_probe_nulls_leaves_all_not_null_keys_untouched() {
        let probe_schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
        ]));
        let on_right: Vec<PhysicalExprRef> =
            vec![Arc::new(Column::new("a", 0)), Arc::new(Column::new("b", 1))];
        let acc = null_equal_accumulator(probe_schema, on_right);

        // Every key is NOT NULL, so there is nothing to OR in and the filter is returned as-is.
        let filter = lit(true);
        let result = acc.preserve_probe_nulls(Arc::clone(&filter), true).unwrap();
        assert_eq!(format!("{result}"), format!("{filter}"));
    }

    #[test]
    fn preserve_probe_nulls_rejects_out_of_sync_key() {
        let probe_schema =
            Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, true)]));
        // The key's column index points past the probe schema: a construction bug that
        // must surface as an error, not get widened around.
        let on_right: Vec<PhysicalExprRef> = vec![Arc::new(Column::new("b", 1))];
        let acc = null_equal_accumulator(probe_schema, on_right);

        assert!(acc.preserve_probe_nulls(lit(true), true).is_err());
    }

    #[test]
    fn preserve_probe_nulls_skips_wrap_when_build_has_no_nulls() {
        let probe_schema =
            Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, true)]));
        let on_right: Vec<PhysicalExprRef> = vec![Arc::new(Column::new("a", 0))];
        let acc = null_equal_accumulator(probe_schema, on_right);

        // A NULL-free build has nothing for a probe NULL to null-match, so the
        // filter keeps its full selectivity.
        let filter = lit(true);
        let result = acc
            .preserve_probe_nulls(Arc::clone(&filter), false)
            .unwrap();
        assert_eq!(format!("{result}"), format!("{filter}"));
    }

    #[test]
    fn preserve_probe_nulls_wraps_null_aware_regardless_of_build() {
        let probe_schema =
            Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, true)]));
        let on_right: Vec<PhysicalExprRef> = vec![Arc::new(Column::new("a", 0))];
        let acc = null_semantics_accumulator(
            probe_schema,
            on_right,
            NullEquality::NullEqualsNothing,
            true,
        );

        // One probe NULL collapses `NOT IN` for every build row, so the wrap must not
        // depend on the build content.
        let widened = acc.preserve_probe_nulls(lit(true), false).unwrap();
        assert_eq!(format!("{widened}").matches("IS NULL").count(), 1);
    }

    // A null-aware join with several probe keys has its `NOT IN` value keys
    // first (`on_right[..V]`: one for a scalar `NOT IN`, one per tuple element
    // for a multi-column one) and correlation scope keys after them. A NULL in
    // any value key can make `NOT IN` UNKNOWN, so the NULL escape must wrap
    // every value key and no scope key.
    #[test]
    fn null_aware_multi_key_filter_escapes_value_keys_only() {
        for value_keys in [1, 2] {
            let mut on_right: Vec<PhysicalExprRef> = (0..value_keys)
                .map(|i| Arc::new(Column::new(&format!("value_key_{i}"), i)) as _)
                .collect();
            on_right.push(Arc::new(Column::new("scope_key", value_keys)));
            let mut fields: Vec<Field> = (0..value_keys)
                .map(|i| Field::new(format!("value_key_{i}"), DataType::Int32, true))
                .collect();
            // Keep the scope key nullable so this test proves it is not widened.
            fields.push(Field::new("scope_key", DataType::Int32, true));

            let dynamic_filter = test_dynamic_filter(&on_right);
            let acc = SharedBuildAccumulator {
                inner: Mutex::new(AccumulatorState {
                    data: AccumulatedBuildData::CollectLeft {
                        data: PartitionStatus::Pending,
                        reported_count: 0,
                        expected_reports: 1,
                    },
                    completion: CompletionState::Pending,
                }),
                completion_notify: Notify::new(),
                dynamic_filter,
                on_right,
                repartition_random_state: SeededRandomState::with_seed(1),
                probe_schema: Arc::new(Schema::new(fields)),
                probe_range_partitioning: None,
                null_equality: NullEquality::NullEqualsNothing,
                null_aware: Some(NullAwareMode::LeftMark {
                    correlated: true,
                    value_keys,
                }),
                global_filter_options: GlobalFilterOptions::default(),
            };

            let bounds = PartitionBounds::new(
                (0..=value_keys as i32)
                    .map(|i| {
                        ColumnBounds::new(
                            ScalarValue::Int32(Some(10 * i)),
                            ScalarValue::Int32(Some(10 * i + 5)),
                        )
                    })
                    .collect(),
            );
            acc.build_filter(FinalizeInput::CollectLeft(reported(
                PushdownStrategy::Empty,
                bounds,
            )))
            .unwrap();

            let expr = current_expr(&acc);
            let or = binary_expr(&expr);
            assert_eq!(or.op(), &Operator::Or);
            let mut escaped = vec![];
            collect_is_null_columns(or.left(), &mut escaped);
            assert_eq!(
                escaped,
                (0..value_keys).collect::<Vec<_>>(),
                "escape must target every NOT IN value key and nothing else"
            );
        }
    }

    /// Column indices under the `IS NULL` disjuncts of an `OR` chain.
    fn collect_is_null_columns(expr: &Arc<dyn PhysicalExpr>, out: &mut Vec<usize>) {
        if let Some(or) = expr.downcast_ref::<BinaryExpr>() {
            assert_eq!(or.op(), &Operator::Or);
            collect_is_null_columns(or.left(), out);
            collect_is_null_columns(or.right(), out);
        } else {
            let column = expr
                .downcast_ref::<IsNullExpr>()
                .expect("expected IS NULL escape")
                .arg()
                .downcast_ref::<Column>()
                .expect("expected column under IS NULL");
            out.push(column.index());
        }
    }
}
