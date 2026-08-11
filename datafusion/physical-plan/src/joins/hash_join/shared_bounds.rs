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
use crate::joins::hash_join::bounds_union::{
    create_merged_bounds_predicate, merge_partition_bounds,
};
use crate::joins::hash_join::exec::HASH_JOIN_SEED;
use crate::joins::hash_join::inlist_builder::build_struct_fields;
use crate::joins::hash_join::partitioned_hash_eval::{
    HashExpr, HashTableLookupExpr, SeededRandomState,
};
use arrow::array::ArrayRef;
use arrow::compute::concat;
use arrow::datatypes::{DataType, Field, Schema};
use datafusion_common::config::ConfigOptions;
use datafusion_common::{
    DataFusionError, NullEquality, Result, ScalarValue, SharedResult,
};
use datafusion_expr::Operator;
use datafusion_functions::core::r#struct as struct_func;
use datafusion_physical_expr::expressions::{
    BinaryExpr, CaseExpr, DynamicFilterPhysicalExpr, InListExpr, IsNullExpr, lit,
};
use datafusion_physical_expr::{PhysicalExpr, PhysicalExprRef, ScalarFunctionExpr};

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
    inner: Mutex<AccumulatorState>,
    /// Wakes every partition that is parked in [`Self::wait_for_completion`]
    /// once [`AccumulatorState::completion`] transitions to
    /// [`CompletionState::Ready`]. Notifications are fired once per
    /// accumulator lifetime (the elected finalizer publishes the terminal
    /// result, then broadcasts), so late subscribers simply re-check the
    /// state under the mutex and return immediately.
    completion_notify: Notify,
    /// Dynamic filter carrying the membership half of the pushed predicate
    /// (and, when [`Self::bounds_dynamic_filter`] is absent, the whole thing).
    dynamic_filter: Arc<DynamicFilterPhysicalExpr>,
    /// Second dynamic filter carrying the build-side value ranges, `AND`'d with
    /// [`Self::dynamic_filter`] on the probe side.
    ///
    /// Two wrappers rather than one `DynamicFilter[bounds AND membership]` is
    /// deliberate: `split_conjunction` splits only on a top-level `AND` and
    /// does not look inside a `DynamicFilterPhysicalExpr`, so a single wrapper
    /// reaches the Parquet reader as one opaque conjunct. Split in two, the
    /// reader builds two `ArrowPredicate`s and applies them in sequence against
    /// an accumulating `RowSelection` — the cheap vectorized range check runs
    /// first and the expensive membership lookup only sees the survivors — and
    /// the range half additionally becomes visible to row-group pruning.
    ///
    /// `None` for `CollectLeft` joins, which have no routing to hoist bounds
    /// out of, and whenever the second filter did not survive pushdown.
    bounds_dynamic_filter: Option<Arc<DynamicFilterPhysicalExpr>>,
    /// Right side join expressions needed for creating filter expressions
    on_right: Vec<PhysicalExprRef>,
    /// Random state for partitioning (RepartitionExec's hash function with 0,0,0,0 seeds)
    /// Used for PartitionedHashLookupPhysicalExpr
    repartition_random_state: SeededRandomState,
    /// Schema of the probe (right) side for evaluating filter expressions
    probe_schema: Arc<Schema>,
    /// Null equality of the join. Under `NullEqualsNull` a probe-side NULL can match a
    /// build-side NULL, so the pushed filter must keep NULL rows here too.
    null_equality: NullEquality,
    /// Null-aware anti join (`NOT IN`). A probe-side NULL must reach the join so its
    /// three-valued logic can collapse the result, so the pushed filter keeps NULL rows.
    null_aware: bool,
}

/// Ceiling on the combined size of the per-partition `InList` arrays that
/// [`SharedBuildAccumulator::union_inlist_membership`] will concatenate.
///
/// Each partition's list is independently capped by
/// `hash_join_inlist_pushdown_max_size`, so without a combined cap the union
/// grows with the partition count. Past this size, keeping the routed `CASE` —
/// where each probe row only ever probes one list — is the cheaper shape.
const MAX_UNIONED_INLIST_BYTES: usize = 1024 * 1024;

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
        keys_have_null: bool,
    },
    CollectLeft {
        pushdown: PushdownStrategy,
        bounds: PartitionBounds,
        keys_have_null: bool,
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
        bounds_dynamic_filter: Option<Arc<DynamicFilterPhysicalExpr>>,
        on_right: Vec<PhysicalExprRef>,
        repartition_random_state: SeededRandomState,
        null_equality: NullEquality,
        null_aware: bool,
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

        Self {
            inner: Mutex::new(AccumulatorState {
                data: mode_data,
                completion: CompletionState::Pending,
            }),
            completion_notify: Notify::new(),
            dynamic_filter,
            // Only a partitioned join routes through a `CASE`, so only it has
            // bounds to hoist out into a second conjunct.
            bounds_dynamic_filter: matches!(partition_mode, PartitionMode::Partitioned)
                .then_some(bounds_dynamic_filter)
                .flatten(),
            on_right,
            repartition_random_state,
            probe_schema: right_child.schema(),
            null_equality,
            null_aware,
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
                    pushdown,
                    bounds,
                    keys_have_null,
                });
            }
            (
                PartitionBuildData::CollectLeft {
                    pushdown,
                    bounds,
                    keys_have_null,
                },
                AccumulatedBuildData::CollectLeft {
                    data,
                    reported_count,
                    ..
                },
            ) => {
                if matches!(data, PartitionStatus::Pending) {
                    *data = PartitionStatus::Reported(PartitionData {
                        pushdown,
                        bounds,
                        keys_have_null,
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
        if let Some(bounds_filter) = &self.bounds_dynamic_filter {
            bounds_filter.mark_complete();
        }

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
            FinalizeInput::CollectLeft(partition) => match partition {
                PartitionStatus::Reported(partition_data) => {
                    let membership_expr = create_membership_predicate(
                        &self.on_right,
                        partition_data.pushdown.clone(),
                        &HASH_JOIN_SEED,
                        self.probe_schema.as_ref(),
                    )?;
                    let bounds_expr =
                        create_bounds_predicate(&self.on_right, &partition_data.bounds);

                    if let Some(filter_expr) =
                        combine_membership_and_bounds(membership_expr, bounds_expr)
                    {
                        self.dynamic_filter.update(self.preserve_probe_nulls(
                            filter_expr,
                            partition_data.keys_have_null,
                        )?)?;
                    }
                }
                PartitionStatus::Pending => {
                    return datafusion_common::internal_err!(
                        "attempted to finalize collect-left dynamic filter without reported build data"
                    );
                }
                PartitionStatus::CanceledUnknown => {
                    return datafusion_common::internal_err!(
                        "collect-left dynamic filter cannot finalize with canceled build data"
                    );
                }
            },
            FinalizeInput::Partitioned(partitions) => {
                let mut real_partitions: Vec<(usize, &PartitionData)> = Vec::new();
                let mut empty_partition_ids = Vec::new();
                let mut has_canceled_unknown = false;
                let mut keys_have_null = false;

                for (partition_id, partition) in partitions.iter().enumerate() {
                    match partition {
                        PartitionStatus::Reported(partition)
                            if matches!(partition.pushdown, PushdownStrategy::Empty) =>
                        {
                            empty_partition_ids.push(partition_id);
                        }
                        PartitionStatus::Reported(partition) => {
                            keys_have_null |= partition.keys_have_null;
                            real_partitions.push((partition_id, partition));
                        }
                        PartitionStatus::CanceledUnknown => {
                            has_canceled_unknown = true;
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

                // A canceled partition can hold any value, so its route must stay
                // permissive and no set-theoretic union over the *reported* partitions
                // describes the build side. Keep the bounds inside the `CASE` in that
                // case, exactly as before.
                let split_bounds =
                    self.bounds_dynamic_filter.is_some() && !has_canceled_unknown;

                let bounds_expr = if split_bounds {
                    let merged = merge_partition_bounds(
                        self.on_right.len(),
                        &real_partitions
                            .iter()
                            .map(|(_, partition)| &partition.bounds)
                            .collect::<Vec<_>>(),
                    );
                    log::debug!(
                        "hash join merged build-side bounds over {} partitions, \
                         degenerate {}, estimated relaxation {:?}",
                        real_partitions.len(),
                        merged.is_degenerate(),
                        merged.relaxation()
                    );
                    // A degenerate merge constrains nothing, so emitting it
                    // would cost an evaluation per probe batch and prune
                    // nothing. Leave the conjunct at `true` instead.
                    if merged.is_degenerate() {
                        None
                    } else {
                        create_merged_bounds_predicate(&self.on_right, &merged)
                    }
                } else {
                    None
                };

                let membership_expr = self.build_partitioned_membership(
                    partitions.len(),
                    &real_partitions,
                    &empty_partition_ids,
                    has_canceled_unknown,
                    // When the bounds cannot be hoisted into their own conjunct they
                    // must stay in the routed branches, or they are lost entirely.
                    !split_bounds,
                )?;

                match (&self.bounds_dynamic_filter, bounds_expr) {
                    (Some(bounds_filter), Some(bounds_expr)) => {
                        // Both halves keep the NULL widening: the probe side sees
                        // `(nulls OR bounds) AND (nulls OR membership)`, which is
                        // `nulls OR (bounds AND membership)`.
                        bounds_filter.update(
                            self.preserve_probe_nulls(bounds_expr, keys_have_null)?,
                        )?;
                    }
                    // Nothing usable to hoist; leave the conjunct as the no-op it
                    // was initialized with so it folds away.
                    (Some(bounds_filter), None) => bounds_filter.update(lit(true))?,
                    (None, _) => {}
                }

                self.dynamic_filter.update(
                    self.preserve_probe_nulls(membership_expr, keys_have_null)?,
                )?;
            }
        }

        Ok(())
    }

    /// Builds the routing half of a partitioned join's pushed predicate.
    ///
    /// Normally this is a `CASE hash(keys) % n WHEN i THEN <partition i's check>`,
    /// but when every non-empty partition pushes an `InList` the routing is
    /// redundant and the whole thing collapses to a single `InList` over the
    /// union — see [`Self::union_inlist_membership`].
    fn build_partitioned_membership(
        &self,
        num_partitions: usize,
        real_partitions: &[(usize, &PartitionData)],
        empty_partition_ids: &[usize],
        has_canceled_unknown: bool,
        include_bounds: bool,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        if !has_canceled_unknown
            && !include_bounds
            && let Some(union) = self.union_inlist_membership(real_partitions)?
        {
            return Ok(union);
        }

        let mut real_branches = Vec::with_capacity(real_partitions.len());
        for (partition_id, partition) in real_partitions {
            let membership_expr = create_membership_predicate(
                &self.on_right,
                partition.pushdown.clone(),
                &HASH_JOIN_SEED,
                self.probe_schema.as_ref(),
            )?;
            let bounds_expr = include_bounds
                .then(|| create_bounds_predicate(&self.on_right, &partition.bounds))
                .flatten();
            let then_expr = combine_membership_and_bounds(membership_expr, bounds_expr)
                .unwrap_or_else(|| lit(true));
            real_branches.push((
                lit(ScalarValue::UInt64(Some(*partition_id as u64))),
                then_expr,
            ));
        }

        let routing_hash_expr = Arc::new(HashExpr::new(
            self.on_right.clone(),
            self.repartition_random_state.clone(),
            "hash_repartition".to_string(),
        )) as Arc<dyn PhysicalExpr>;
        let modulo_expr = Arc::new(BinaryExpr::new(
            routing_hash_expr,
            Operator::Modulo,
            lit(ScalarValue::UInt64(Some(num_partitions as u64))),
        )) as Arc<dyn PhysicalExpr>;

        if has_canceled_unknown {
            let mut when_then_branches = empty_partition_ids
                .iter()
                .map(|partition_id| {
                    (
                        lit(ScalarValue::UInt64(Some(*partition_id as u64))),
                        lit(false),
                    )
                })
                .collect::<Vec<_>>();
            when_then_branches.extend(real_branches);

            if when_then_branches.is_empty() {
                Ok(lit(true))
            } else {
                Ok(Arc::new(CaseExpr::try_new(
                    Some(modulo_expr),
                    when_then_branches,
                    Some(lit(true)),
                )?) as Arc<dyn PhysicalExpr>)
            }
        } else if real_branches.is_empty() {
            Ok(lit(false))
        } else if real_branches.len() == 1
            && empty_partition_ids.len() + 1 == num_partitions
        {
            Ok(Arc::clone(&real_branches[0].1))
        } else {
            Ok(Arc::new(CaseExpr::try_new(
                Some(modulo_expr),
                real_branches,
                Some(lit(false)),
            )?) as Arc<dyn PhysicalExpr>)
        }
    }

    /// Collapses an all-`InList` partitioned membership check into one `InList`
    /// over the union of the per-partition lists.
    ///
    /// This is **exact**, not a relaxation: routing is a deterministic function
    /// of the key columns, so every build row holding key `K` lands in the same
    /// partition a probe row holding `K` routes to. Testing `K` against the
    /// union therefore accepts and rejects precisely what the `CASE` does, and
    /// the result no longer needs the routing hash at all — nor is it opaque to
    /// pruning the way a `CASE` is.
    ///
    /// Returns `None` when the collapse does not apply (some partition pushes a
    /// hash map instead of a list, the lists disagree on type, or the union
    /// would be too large to be worth materializing).
    fn union_inlist_membership(
        &self,
        real_partitions: &[(usize, &PartitionData)],
    ) -> Result<Option<Arc<dyn PhysicalExpr>>> {
        if real_partitions.is_empty() {
            return Ok(None);
        }

        let mut arrays = Vec::with_capacity(real_partitions.len());
        let mut total_bytes = 0;
        for (_, partition) in real_partitions {
            let PushdownStrategy::InList(values) = &partition.pushdown else {
                return Ok(None);
            };
            total_bytes += values.get_array_memory_size();
            if total_bytes > MAX_UNIONED_INLIST_BYTES {
                return Ok(None);
            }
            if arrays
                .first()
                .is_some_and(|first: &&ArrayRef| first.data_type() != values.data_type())
            {
                return Ok(None);
            }
            arrays.push(values);
        }

        let union: ArrayRef = if arrays.len() == 1 {
            Arc::clone(arrays[0])
        } else {
            concat(&arrays.iter().map(|a| a.as_ref()).collect::<Vec<_>>())?
        };

        create_membership_predicate(
            &self.on_right,
            PushdownStrategy::InList(union),
            &HASH_JOIN_SEED,
            self.probe_schema.as_ref(),
        )
    }

    /// Keeps probe rows with a NULL key when the join semantics need them.
    ///
    /// The build-side predicate drops probe rows whose key is NULL. A null-aware anti join
    /// (`NOT IN`) needs that NULL to reach the join so three-valued logic can collapse the
    /// result, and a null-equal join needs it to match a build-side NULL. OR-ing `key IS NULL`
    /// keeps those rows while preserving the filter's selectivity for the rest; the join refines
    /// whatever the widened filter lets through.
    fn preserve_probe_nulls(
        &self,
        filter_expr: Arc<dyn PhysicalExpr>,
        build_keys_have_null: bool,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        // A null-aware anti join needs every probe NULL no matter what the build holds: one
        // probe NULL makes `NOT IN` unknown for every build row. A null-equal join needs probe
        // NULLs only to match an actual build-side NULL, so a NULL-free build keeps the filter
        // at full selectivity.
        let needs_probe_nulls = self.null_aware
            || (self.null_equality == NullEquality::NullEqualsNull
                && build_keys_have_null);
        if !needs_probe_nulls {
            return Ok(filter_expr);
        }
        // Only a key that can actually be NULL needs the disjunct; a NOT NULL key never widens.
        // Null-aware joins are single-key; null-equal joins can be multi-key, so OR every nullable
        // key. If every key is NOT NULL the filter is left untouched, at full selectivity.
        let mut any_key_is_null: Option<Arc<dyn PhysicalExpr>> = None;
        for key in &self.on_right {
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
        bounds_dynamic_filter: None,
        on_right: vec![],
        repartition_random_state: SeededRandomState::with_seed(1),
        probe_schema,
        null_equality: NullEquality::NullEqualsNothing,
        null_aware: false,
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

    use crate::joins::join_hash_map::JoinHashMapU32;

    use arrow::array::{ArrayRef, Int32Array};
    use datafusion_physical_expr::expressions::{Column, Literal};

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
        make_accumulator_with_bounds_filter_for_test(data, on_right, false)
    }

    fn make_accumulator_with_bounds_filter_for_test(
        data: AccumulatedBuildData,
        on_right: Vec<PhysicalExprRef>,
        split_bounds: bool,
    ) -> SharedBuildAccumulator {
        let dynamic_filter = test_dynamic_filter(&on_right);
        let bounds_dynamic_filter = split_bounds.then(|| test_dynamic_filter(&on_right));
        SharedBuildAccumulator {
            inner: Mutex::new(AccumulatorState {
                data,
                completion: CompletionState::Pending,
            }),
            completion_notify: Notify::new(),
            dynamic_filter,
            bounds_dynamic_filter,
            on_right,
            repartition_random_state: SeededRandomState::with_seed(1),
            probe_schema: test_probe_schema(),
            null_equality: NullEquality::NullEqualsNothing,
            null_aware: false,
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

    /// A partitioned accumulator wired the way filter pushdown wires it in
    /// production: a separate dynamic filter for the bounds conjunct.
    fn make_split_partitioned_accumulator_for_test(
        num_partitions: usize,
    ) -> SharedBuildAccumulator {
        make_accumulator_with_bounds_filter_for_test(
            AccumulatedBuildData::Partitioned {
                partitions: vec![PartitionStatus::Pending; num_partitions],
                completed_partitions: 0,
            },
            test_on_right(),
            true,
        )
    }

    fn current_bounds_expr(acc: &SharedBuildAccumulator) -> PhysicalExprRef {
        acc.bounds_dynamic_filter
            .as_ref()
            .expect("expected a bounds dynamic filter")
            .current()
            .expect("bounds dynamic filter current expression should be available")
    }

    fn in_list(values: &[i32]) -> PushdownStrategy {
        PushdownStrategy::InList(Arc::new(Int32Array::from(values.to_vec())) as ArrayRef)
    }

    /// A build side too large for an `InList`, which is what forces the routed
    /// `CASE` to survive.
    fn map_pushdown() -> PushdownStrategy {
        PushdownStrategy::Map(Arc::new(Map::HashMap(Box::new(
            JoinHashMapU32::with_capacity(1),
        ))))
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
    fn partitioned_split_hoists_merged_bounds_out_of_the_case() {
        let acc = make_split_partitioned_accumulator_for_test(2);

        // Two large (map-backed) partitions whose ranges overlap: the union is
        // a single range and the routing CASE keeps only the membership checks.
        acc.build_filter(FinalizeInput::Partitioned(vec![
            reported(map_pushdown(), bounds(0, 10)),
            reported(map_pushdown(), bounds(5, 20)),
        ]))
        .unwrap();

        let bounds_expr = current_bounds_expr(&acc);
        assert_eq!(
            format!("{bounds_expr}"),
            "probe_key@0 >= 0 AND probe_key@0 <= 20"
        );

        // The membership half is a routing CASE with no bounds left in it.
        let membership_expr = current_expr(&acc);
        let case = case_expr(&membership_expr);
        assert_eq!(case.when_then_expr().len(), 2);
        for (_, then_expr) in case.when_then_expr() {
            assert!(
                then_expr.downcast_ref::<HashTableLookupExpr>().is_some(),
                "expected a bare membership check, got {then_expr}"
            );
        }
    }

    #[test]
    fn partitioned_split_ors_disjoint_partition_ranges() {
        let acc = make_split_partitioned_accumulator_for_test(2);

        acc.build_filter(FinalizeInput::Partitioned(vec![
            reported(map_pushdown(), bounds(0, 10)),
            reported(map_pushdown(), bounds(100, 110)),
        ]))
        .unwrap();

        // Disjoint ranges are kept apart instead of being widened to the hull:
        // `[0, 110]` would admit everything in between for nothing.
        let bounds_expr = current_bounds_expr(&acc);
        assert_eq!(
            format!("{bounds_expr}"),
            "probe_key@0 >= 0 AND probe_key@0 <= 10 OR probe_key@0 >= 100 AND probe_key@0 <= 110"
        );
    }

    #[test]
    fn partitioned_all_inlist_collapses_to_a_single_union_inlist() {
        let acc = make_split_partitioned_accumulator_for_test(3);

        acc.build_filter(FinalizeInput::Partitioned(vec![
            reported(in_list(&[1, 4]), bounds(1, 4)),
            reported(in_list(&[2, 5]), bounds(2, 5)),
            reported(PushdownStrategy::Empty, no_bounds()),
        ]))
        .unwrap();

        // Routing is a function of the key, so a probe key only ever matches
        // the list of the partition it routes to: the union is exact and the
        // `CASE` (and its routing hash) disappears entirely.
        let membership_expr = current_expr(&acc);
        assert!(membership_expr.downcast_ref::<CaseExpr>().is_none());
        assert_in_list_column_values(&membership_expr, "probe_key", 0, &[1, 4, 2, 5]);
    }

    #[test]
    fn partitioned_mixed_strategies_keep_the_routing_case() {
        let acc = make_split_partitioned_accumulator_for_test(2);

        acc.build_filter(FinalizeInput::Partitioned(vec![
            reported(in_list(&[1, 2]), bounds(1, 2)),
            reported(map_pushdown(), bounds(3, 4)),
        ]))
        .unwrap();

        // One partition still needs a hash-table lookup, so routing is required.
        let membership_expr = current_expr(&acc);
        assert_eq!(case_expr(&membership_expr).when_then_expr().len(), 2);
    }

    #[test]
    fn partitioned_split_leaves_bounds_in_the_case_when_a_partition_is_canceled() {
        let acc = make_split_partitioned_accumulator_for_test(2);

        acc.build_filter(FinalizeInput::Partitioned(vec![
            PartitionStatus::CanceledUnknown,
            reported(map_pushdown(), bounds(0, 10)),
        ]))
        .unwrap();

        // A canceled partition can hold any value, so no union over the
        // reported partitions describes the build side: the hoisted conjunct
        // must stay a no-op and the bounds stay inside the routed branch.
        assert_literal_bool(&current_bounds_expr(&acc), true);
        let membership_expr = current_expr(&acc);
        let case = case_expr(&membership_expr);
        assert_eq!(case.when_then_expr().len(), 1);
        assert_top_binary_op(&case.when_then_expr()[0].1, Operator::And);
    }

    #[test]
    fn partitioned_split_without_usable_bounds_emits_a_no_op_conjunct() {
        let acc = make_split_partitioned_accumulator_for_test(2);

        acc.build_filter(FinalizeInput::Partitioned(vec![
            reported(map_pushdown(), bounds(0, 10)),
            // No bounds reported here, so any value could route to partition 1
            // and the column cannot be constrained at all.
            reported(map_pushdown(), no_bounds()),
        ]))
        .unwrap();

        assert_literal_bool(&current_bounds_expr(&acc), true);
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
            bounds_dynamic_filter: None,
            on_right,
            repartition_random_state: SeededRandomState::with_seed(1),
            probe_schema,
            null_equality,
            null_aware,
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
}
