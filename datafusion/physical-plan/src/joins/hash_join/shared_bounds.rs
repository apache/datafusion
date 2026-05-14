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
    HashTableLookupExpr, MultiMapLookupExpr, SeededRandomState,
};
use arrow::array::{Array, ArrayRef};
use arrow::compute::concat;
use arrow::datatypes::{DataType, Field, Schema};
use datafusion_common::config::ConfigOptions;
use datafusion_common::{DataFusionError, Result, ScalarValue, SharedResult};
use datafusion_expr::Operator;
use datafusion_functions::core::r#struct as struct_func;
use datafusion_physical_expr::expressions::{
    BinaryExpr, DynamicFilterPhysicalExpr, InListExpr, lit,
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

/// Build a `IN (SET)` predicate over `on_right` from a deduplicated build-side
/// array. Single-column joins compare scalars directly; multi-column joins
/// wrap the right-hand columns in a `struct(...)` to match the build side's
/// shape.
fn create_inlist_predicate(
    on_right: &[PhysicalExprRef],
    in_list_array: ArrayRef,
    schema: &Schema,
) -> Result<Arc<dyn PhysicalExpr>> {
    let expr = if on_right.len() == 1 {
        Arc::clone(&on_right[0])
    } else {
        let fields = build_struct_fields(
            on_right
                .iter()
                .map(|r| r.data_type(schema))
                .collect::<Result<Vec<_>>>()?
                .as_ref(),
        )?;
        let return_field = Arc::new(Field::new("struct", DataType::Struct(fields), true));
        Arc::new(ScalarFunctionExpr::new(
            "struct",
            struct_func(),
            on_right.to_vec(),
            return_field,
            Arc::new(ConfigOptions::default()),
        )) as Arc<dyn PhysicalExpr>
    };

    Ok(Arc::new(InListExpr::try_new_from_array(
        expr,
        in_list_array,
        false,
        schema,
    )?))
}

/// Build a single-map `hash_lookup` predicate over `on_right` against `map`.
/// Used by the CollectLeft path; the Partitioned path emits
/// [`MultiMapLookupExpr`] over multiple maps instead.
fn create_hash_lookup_predicate(
    on_right: &[PhysicalExprRef],
    map: Arc<Map>,
    random_state: &SeededRandomState,
) -> Arc<dyn PhysicalExpr> {
    Arc::new(HashTableLookupExpr::new(
        on_right.to_vec(),
        random_state.clone(),
        map,
        "hash_lookup".to_string(),
    ))
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

/// Compute the global (envelope) min/max bounds across a set of partition bounds.
///
/// For each column index, returns the smallest min seen and the largest max seen.
/// Columns where any partition is missing bounds, or where bounds are not totally
/// ordered (e.g. mixed-type comparisons), are dropped from the global envelope.
fn compute_global_bounds(per_partition: &[&PartitionBounds]) -> Option<PartitionBounds> {
    let mut iter = per_partition.iter();
    let first = iter.next()?;
    let mut acc: Vec<Option<ColumnBounds>> = first
        .column_bounds
        .iter()
        .map(|cb| Some(cb.clone()))
        .collect();

    for partition in iter {
        if partition.column_bounds.len() != acc.len() {
            return None;
        }
        for (slot, cb) in acc.iter_mut().zip(partition.column_bounds.iter()) {
            let Some(existing) = slot.as_mut() else {
                continue;
            };
            match cb.min.partial_cmp(&existing.min) {
                Some(std::cmp::Ordering::Less) => existing.min = cb.min.clone(),
                Some(_) => {}
                None => {
                    *slot = None;
                    continue;
                }
            }
            match cb.max.partial_cmp(&existing.max) {
                Some(std::cmp::Ordering::Greater) => existing.max = cb.max.clone(),
                Some(_) => {}
                None => *slot = None,
            }
        }
    }

    let merged: Vec<ColumnBounds> = acc.into_iter().flatten().collect();
    if merged.is_empty() {
        None
    } else {
        Some(PartitionBounds::new(merged))
    }
}

/// Concatenate per-partition InList arrays into a single array. Returns `None`
/// if data types disagree across partitions or if `arrow::compute::concat`
/// fails (e.g. unsupported types).
fn merge_inlist_arrays(arrays: &[ArrayRef]) -> Option<ArrayRef> {
    let first = arrays.first()?;
    if arrays.iter().any(|a| a.data_type() != first.data_type()) {
        return None;
    }
    let array_refs: Vec<&dyn Array> = arrays.iter().map(|a| a.as_ref()).collect();
    concat(&array_refs).ok()
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
    /// Dynamic filter for pushdown to probe side
    dynamic_filter: Arc<DynamicFilterPhysicalExpr>,
    /// Right side join expressions needed for creating filter expressions
    on_right: Vec<PhysicalExprRef>,
    /// Schema of the probe (right) side for evaluating filter expressions
    probe_schema: Arc<Schema>,
    /// Maximum distinct entries the cross-partition merged InList may
    /// contain before we fall back to `multi_hash_lookup`. Sourced from
    /// `optimizer.hash_join_inlist_pushdown_max_distinct_values` so the
    /// same threshold caps both per-partition and cross-partition InList
    /// pushdown.
    inlist_max_distinct_values: usize,
}

/// Build-side data needed to construct a dynamic filter for one partition.
///
/// `map` is always set for non-empty partitions (the join's hash table is
/// built unconditionally). `inlist` is additionally set when the build side
/// fit under both the per-partition InList caps
/// (`hash_join_inlist_pushdown_max_size` /
/// `hash_join_inlist_pushdown_max_distinct_values`) — that's our signal that
/// the partition's keys are small enough to participate in parquet stats /
/// bloom-filter pruning at the scan when collapsed across partitions.
#[derive(Clone, Default)]
pub(crate) struct PushdownStrategy {
    /// Hash table for the partition's build side. `None` if the partition was
    /// empty.
    pub(crate) map: Option<Arc<Map>>,
    /// Concatenable array of build-side join keys (single column or struct
    /// of columns). `Some` when the build side was small enough for the
    /// InList pushdown; otherwise `None` and the filter falls back to
    /// `hash_lookup` / `multi_hash_lookup`.
    pub(crate) inlist: Option<ArrayRef>,
}

impl PushdownStrategy {
    pub(crate) fn empty() -> Self {
        Self::default()
    }

    pub(crate) fn from_map(map: Arc<Map>) -> Self {
        Self {
            map: Some(map),
            inlist: None,
        }
    }

    pub(crate) fn from_map_and_inlist(map: Arc<Map>, inlist: ArrayRef) -> Self {
        Self {
            map: Some(map),
            inlist: Some(inlist),
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.map.is_none()
    }
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
    pub(crate) fn new_from_partition_mode(
        partition_mode: PartitionMode,
        left_child: &dyn ExecutionPlan,
        right_child: &dyn ExecutionPlan,
        dynamic_filter: Arc<DynamicFilterPhysicalExpr>,
        on_right: Vec<PhysicalExprRef>,
        inlist_max_distinct_values: usize,
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
            on_right,
            probe_schema: right_child.schema(),
            inlist_max_distinct_values,
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
                },
                AccumulatedBuildData::Partitioned {
                    partitions,
                    completed_partitions,
                },
            ) => {
                if matches!(partitions[partition_id], PartitionStatus::Pending) {
                    *completed_partitions += 1;
                }
                partitions[partition_id] =
                    PartitionStatus::Reported(PartitionData { pushdown, bounds });
            }
            (
                PartitionBuildData::CollectLeft { pushdown, bounds },
                AccumulatedBuildData::CollectLeft {
                    data,
                    reported_count,
                    ..
                },
            ) => {
                if matches!(data, PartitionStatus::Pending) {
                    *data = PartitionStatus::Reported(PartitionData { pushdown, bounds });
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
            FinalizeInput::CollectLeft(partition) => match partition {
                PartitionStatus::Reported(partition_data) => {
                    let membership_expr =
                        self.collect_left_membership(&partition_data.pushdown)?;
                    let bounds_expr =
                        create_bounds_predicate(&self.on_right, &partition_data.bounds);
                    if let Some(filter_expr) =
                        combine_membership_and_bounds(membership_expr, bounds_expr)
                    {
                        self.dynamic_filter.update(filter_expr)?;
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
                let mut real_partitions: Vec<&PartitionData> = Vec::new();
                let mut has_canceled_unknown = false;
                for partition in partitions.iter() {
                    match partition {
                        PartitionStatus::Reported(partition)
                            if partition.pushdown.is_empty() =>
                        {
                            // Empty partitions contribute neither a map nor
                            // an InList; nothing to add to the filter.
                        }
                        PartitionStatus::Reported(partition) => {
                            real_partitions.push(partition);
                        }
                        PartitionStatus::CanceledUnknown => {
                            has_canceled_unknown = true;
                        }
                        PartitionStatus::Pending => {
                            return datafusion_common::internal_err!(
                                "attempted to finalize dynamic filter with pending partition"
                            );
                        }
                    }
                }

                let filter_expr = self
                    .build_partitioned_filter(&real_partitions, has_canceled_unknown)?;
                self.dynamic_filter.update(filter_expr)?;
            }
        }

        Ok(())
    }

    /// CollectLeft has a single shared build side, so we always have one
    /// `Map`. We prefer the InList expression when it's available (the build
    /// side fit under the InList caps) because it's directly representable in
    /// parquet stats / bloom-filter pruning at the scan; otherwise fall back
    /// to a single `hash_lookup` against the map.
    fn collect_left_membership(
        &self,
        pushdown: &PushdownStrategy,
    ) -> Result<Option<Arc<dyn PhysicalExpr>>> {
        if let Some(arr) = &pushdown.inlist {
            return Ok(Some(create_inlist_predicate(
                &self.on_right,
                Arc::clone(arr),
                self.probe_schema.as_ref(),
            )?));
        }
        Ok(pushdown.map.as_ref().map(|map| {
            create_hash_lookup_predicate(&self.on_right, Arc::clone(map), &HASH_JOIN_SEED)
        }))
    }

    /// Build the dynamic filter for `PartitionMode::Partitioned`. Emits
    /// one of:
    ///
    /// ```text
    /// global_minmax AND merged_in_list
    /// global_minmax AND multi_hash_lookup
    /// ```
    ///
    /// independent of how the build side was repartitioned. The two
    /// membership shapes are alternatives, not combined — see below.
    ///
    /// * `global_minmax` — envelope of every partition's per-column min/max.
    ///   Cheap short-circuit and the only piece visible to scan-level
    ///   `pruning_predicate` extraction.
    /// * `merged_in_list` — concatenated, deduplicated build keys when every
    ///   reported partition contributed an `InList` array and the
    ///   cross-partition union fits under
    ///   `optimizer.hash_join_inlist_pushdown_max_distinct_values`. A small
    ///   `IN (SET)` participates in parquet stats / bloom-filter pruning,
    ///   which `multi_hash_lookup` does not. When present it fully replaces
    ///   the lookup.
    /// * `multi_hash_lookup` — hashes the join keys once and ORs
    ///   `contain_hashes()` across every partition's hash table.
    ///
    /// `has_canceled_unknown` partitions short-circuit to `lit(true)`: we
    /// don't have their maps, so we cannot include them in the lookup, and
    /// the query is being torn down anyway.
    fn build_partitioned_filter(
        &self,
        real_partitions: &[&PartitionData],
        has_canceled_unknown: bool,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        if has_canceled_unknown {
            return Ok(lit(true));
        }
        if real_partitions.is_empty() {
            return Ok(lit(false));
        }

        let bounds_refs: Vec<&PartitionBounds> =
            real_partitions.iter().map(|p| &p.bounds).collect();
        let global_bounds_expr = compute_global_bounds(&bounds_refs)
            .as_ref()
            .and_then(|b| create_bounds_predicate(&self.on_right, b));

        // The merged InList covers the union of every partition's
        // build-side keys, so when it fires it stands alone — there is no
        // need to also AND a `multi_hash_lookup` (which would just probe
        // the same data via a different structure).
        let membership_expr =
            if let Some(merged) = self.try_build_merged_inlist(real_partitions)? {
                Some(merged)
            } else {
                let maps: Vec<Arc<Map>> = real_partitions
                    .iter()
                    .filter_map(|p| p.pushdown.map.clone())
                    .collect();
                if maps.is_empty() {
                    // Defensive: every reported (non-empty) partition is
                    // supposed to carry a Map. If none do, we have no
                    // membership predicate to AND with the bounds, so the
                    // final filter is `global_minmax` alone — or
                    // `lit(true)` if global bounds were also unavailable.
                    None
                } else {
                    Some(Arc::new(MultiMapLookupExpr::new(
                        self.on_right.clone(),
                        HASH_JOIN_SEED.clone(),
                        maps,
                        "multi_hash_lookup".to_string(),
                    )) as Arc<dyn PhysicalExpr>)
                }
            };

        Ok(
            combine_membership_and_bounds(membership_expr, global_bounds_expr)
                .unwrap_or_else(|| lit(true)),
        )
    }

    /// If every reported partition contributed an InList array, concatenate
    /// them, deduplicate by scalar value, and gate on the
    /// `inlist_max_distinct_values` cap. Returns the merged
    /// `(struct(...))? IN (SET) ([…])` predicate built over the
    /// deduplicated keys when the cap is satisfied; `None` otherwise.
    ///
    /// Per-partition arrays carry duplicates — each partition ships its raw
    /// build-side join keys, dedup happens here. The dedup walk early-aborts
    /// the moment we cross the cap, so the cost stays bounded by
    /// `O(rows-until-cap+1-distinct-found)` rather than total input size.
    fn try_build_merged_inlist(
        &self,
        real_partitions: &[&PartitionData],
    ) -> Result<Option<Arc<dyn PhysicalExpr>>> {
        let cap = self.inlist_max_distinct_values;
        let mut arrays: Vec<ArrayRef> = Vec::with_capacity(real_partitions.len());
        for p in real_partitions {
            let Some(arr) = &p.pushdown.inlist else {
                return Ok(None);
            };
            arrays.push(Arc::clone(arr));
        }
        let Some(merged) = merge_inlist_arrays(&arrays) else {
            return Ok(None);
        };
        // Walk the merged array once, recording the first index of each
        // distinct ScalarValue. If we cross the cap we abort early without
        // materialising a longer index list. `arrow::compute::take` then
        // produces the deduplicated array (in first-seen order, matching the
        // shape `InListExpr::try_new_from_array` expects).
        let mut seen = std::collections::HashSet::with_capacity(cap.saturating_add(1));
        let mut indices: Vec<u32> = Vec::with_capacity(cap.min(merged.len()));
        for i in 0..merged.len() {
            let scalar = ScalarValue::try_from_array(merged.as_ref(), i)?;
            if seen.insert(scalar) {
                if indices.len() >= cap {
                    // One more distinct value would exceed the cap.
                    return Ok(None);
                }
                indices.push(i as u32);
            }
        }
        let idx_array = arrow::array::UInt32Array::from(indices);
        let deduped = arrow::compute::take(merged.as_ref(), &idx_array, None)?;
        Ok(Some(create_inlist_predicate(
            &self.on_right,
            deduped,
            self.probe_schema.as_ref(),
        )?))
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

    fn make_partitioned_accumulator(num_partitions: usize) -> SharedBuildAccumulator {
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
            probe_schema,
            inlist_max_distinct_values: 20,
        }
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
        let acc = make_partitioned_accumulator(2);

        {
            let mut guard = acc.inner.lock();
            acc.store_build_data(
                &mut guard,
                PartitionBuildData::Partitioned {
                    partition_id: 0,
                    pushdown: PushdownStrategy::empty(),
                    bounds: PartitionBounds::new(vec![]),
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
        let acc = make_partitioned_accumulator(2);

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

    #[test]
    fn compute_global_bounds_takes_envelope() {
        let p1 = PartitionBounds::new(vec![ColumnBounds::new(
            ScalarValue::Int32(Some(5)),
            ScalarValue::Int32(Some(10)),
        )]);
        let p2 = PartitionBounds::new(vec![ColumnBounds::new(
            ScalarValue::Int32(Some(3)),
            ScalarValue::Int32(Some(7)),
        )]);
        let p3 = PartitionBounds::new(vec![ColumnBounds::new(
            ScalarValue::Int32(Some(8)),
            ScalarValue::Int32(Some(20)),
        )]);
        let global = compute_global_bounds(&[&p1, &p2, &p3]).unwrap();
        let cb = global.get_column_bounds(0).unwrap();
        assert_eq!(cb.min, ScalarValue::Int32(Some(3)));
        assert_eq!(cb.max, ScalarValue::Int32(Some(20)));
    }

    /// When one partition's bounds for a column cannot be ordered against
    /// another's (different ScalarValue variants → `partial_cmp` returns
    /// `None`), that column is dropped from the global envelope while
    /// orderable columns continue to widen normally.
    #[test]
    fn compute_global_bounds_drops_incomparable_column() {
        let p1 = PartitionBounds::new(vec![
            ColumnBounds::new(ScalarValue::Int32(Some(5)), ScalarValue::Int32(Some(10))),
            ColumnBounds::new(ScalarValue::Int32(Some(1)), ScalarValue::Int32(Some(2))),
        ]);
        let p2 = PartitionBounds::new(vec![
            ColumnBounds::new(ScalarValue::Int32(Some(3)), ScalarValue::Int32(Some(20))),
            // Column 1 has a different ScalarValue variant from p1's
            // column 1; partial_cmp returns None and the column must
            // drop out of the global envelope.
            ColumnBounds::new(
                ScalarValue::Utf8(Some("a".to_string())),
                ScalarValue::Utf8(Some("z".to_string())),
            ),
        ]);
        let global = compute_global_bounds(&[&p1, &p2]).unwrap();
        // Only the orderable column survives.
        assert_eq!(global.column_bounds.len(), 1);
        let cb = global.get_column_bounds(0).unwrap();
        assert_eq!(cb.min, ScalarValue::Int32(Some(3)));
        assert_eq!(cb.max, ScalarValue::Int32(Some(20)));
    }

    /// If the only column is incomparable across partitions, the global
    /// envelope has no surviving column and the function returns `None`
    /// — there are no bounds to filter on, and the caller must fall
    /// back to a membership-only filter.
    #[test]
    fn compute_global_bounds_none_when_all_columns_incomparable() {
        let p1 = PartitionBounds::new(vec![ColumnBounds::new(
            ScalarValue::Int32(Some(5)),
            ScalarValue::Int32(Some(10)),
        )]);
        let p2 = PartitionBounds::new(vec![ColumnBounds::new(
            ScalarValue::Utf8(Some("a".to_string())),
            ScalarValue::Utf8(Some("z".to_string())),
        )]);
        assert!(compute_global_bounds(&[&p1, &p2]).is_none());
    }
}
