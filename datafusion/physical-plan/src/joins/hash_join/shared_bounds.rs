// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Utilities for shared build-side information. Used in dynamic filter pushdown in Hash Joins.
// TODO: include the link to the Dynamic Filter blog post.

use std::collections::HashSet;
use std::fmt;
use std::sync::Arc;

use crate::ExecutionPlan;
use crate::ExecutionPlanProperties;
use crate::joins::PartitionMode;
use crate::joins::hash_join::bloom_filter::{BloomFilterExpr, BloomFilterOptions};
use crate::joins::hash_join::inlist_builder::build_struct_fields;
use crate::joins::hash_join::not_in::NotIn;
use crate::joins::key_range_bitmap::{KeyRangeBitmap, KeyRangePart};
use arrow::array::{Array, ArrayRef, UInt32Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::row::{RowConverter, SortField};
use datafusion_common::config::ConfigOptions;
use datafusion_common::{
    DataFusionError, NullEquality, Result, ScalarValue, SharedResult,
    assert_or_internal_err,
};
use datafusion_execution::memory_pool::{MemoryPool, UnboundedMemoryPool};
use datafusion_expr::Operator;
use datafusion_functions::core::r#struct as struct_func;
use datafusion_physical_expr::expressions::{
    BinaryExpr, DynamicFilterPhysicalExpr, InListExpr, IsNullExpr, lit,
};
use datafusion_physical_expr::{PhysicalExpr, PhysicalExprRef, ScalarFunctionExpr};
use parquet::bloom_filter::Sbbf;

use parking_lot::Mutex;
use tokio::sync::Notify;

/// The distinct values of `values`, in the order of their first occurrence.
///
/// The InList of a partition holds every build row, so the concatenated lists
/// of all partitions can repeat few keys many times.
fn distinct_values(values: &ArrayRef) -> Result<ArrayRef> {
    let converter = RowConverter::new(vec![SortField::new(values.data_type().clone())])?;
    let rows = converter.convert_columns(std::slice::from_ref(values))?;
    let mut seen = HashSet::with_capacity(rows.num_rows());
    let indices: UInt32Array = (0..rows.num_rows() as u32)
        .filter(|&i| seen.insert(rows.row(i as usize)))
        .collect();
    if indices.len() == values.len() {
        return Ok(Arc::clone(values));
    }
    Ok(arrow::compute::take(values.as_ref(), &indices, None)?)
}

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
    /// Bloom filter of the keys. `None` when this partition has no filter,
    /// then the global filter has no bloom filter.
    pub(crate) bloom_filter: Option<Arc<Sbbf>>,
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

/// Creates `key IN (values)` for a single key, or
/// `struct(keys) IN (values)` for several keys.
fn create_in_list_predicate(
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

        // The return field name and the function field name don't really matter here.
        let return_field = Arc::new(Field::new("struct", DataType::Struct(fields), true));

        Arc::new(ScalarFunctionExpr::new(
            "struct",
            struct_func(),
            on_right.to_vec(),
            return_field,
            Arc::new(ConfigOptions::default()),
        )) as Arc<dyn PhysicalExpr>
    };

    // `try_new_from_array` builds the InList with its hash-based static filter.
    Ok(Arc::new(InListExpr::try_new_from_array(
        expr,
        in_list_array,
        false,
        schema,
    )?))
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
/// Returns `None` when neither is available.
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
/// This structure collects information from the build side (keys and bounds) and
/// ensures that dynamic filters are built with complete information from all relevant
/// partitions before being applied to probe-side scans. Incomplete filters would
/// incorrectly eliminate valid join results.
///
/// ## Synchronization Strategy
///
/// 1. Each partition computes information from its build-side data (keys and bounds)
/// 2. Information is stored in the shared state, which tracks how many partitions have reported
/// 3. When the last partition reports, one waiter is elected as the finalizer; it merges the
///    collected information, updates the dynamic filter exactly once, and publishes the
///    terminal result by transitioning [`CompletionState`] to `Ready`
/// 4. A [`tokio::sync::Notify`] wakes any other partitions parked in `wait_for_completion`,
///    which then observe the `Ready` state under the mutex and return immediately
///
/// ## Filter
///
/// The filter merges the bounds and keys of all partitions into one global
/// filter: global min/max bounds AND an InList (few keys) or a bloom filter of
/// all build keys. See `build_global_filter`. When a partition is canceled,
/// the filter is `true`.
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
    /// Null equality of the join. Under `NullEqualsNull` a probe-side NULL can match a
    /// build-side NULL, so the pushed filter must keep NULL rows here too.
    null_equality: NullEquality,
    /// Null-aware anti join (`NOT IN`). A probe-side NULL must reach the join so its
    /// three-valued logic can collapse the result, so the pushed filter keeps NULL rows.
    not_in: Option<NotIn>,
    /// Options for the global filter
    global_filter_options: GlobalFilterOptions,
}

/// Strategy for filter pushdown (decided at collection time)
#[derive(Clone)]
pub(crate) enum PushdownStrategy {
    /// Use InList for small build sides (< 128MB)
    InList(ArrayRef),
    /// The build side has too many keys for an InList. The global filter
    /// uses the bloom filter of [`BuildKeys`]. `Some` holds a bucket bitmap of
    /// the keys, for pruning.
    Large(Option<Arc<KeyRangeBitmap>>),
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
        null_equality: NullEquality,
        not_in: Option<NotIn>,
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

        Self {
            inner: Mutex::new(AccumulatorState {
                data: mode_data,
                completion: CompletionState::Pending,
            }),
            completion_notify: Notify::new(),
            dynamic_filter,
            on_right,
            probe_schema: right_child.schema(),
            null_equality,
            not_in,
            global_filter_options,
        }
    }

    /// Report build-side data from a partition
    ///
    /// This unified method handles both CollectLeft and Partitioned modes. When all partitions
    /// have reported (barrier wait), the leader builds the appropriate filter expression:
    /// the global filter of all partitions, or `true` when a partition is canceled.
    ///
    /// # Arguments
    /// * `data` - Build data including keys, pushdown strategy, and bounds
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
                self.build_partitioned_filter(&partitions)
            }
        }
    }

    /// Builds the single global filter used by a collect-left join.
    fn build_collect_left_filter(&self, partition: PartitionStatus) -> Result<()> {
        match partition {
            // An empty build side keeps the filter unchanged.
            PartitionStatus::Reported(PartitionData {
                pushdown: PushdownStrategy::Empty,
                ..
            }) => Ok(()),
            PartitionStatus::Reported(data) => self
                .dynamic_filter
                .update(self.build_global_filter(&[&data])?),
            PartitionStatus::Pending => datafusion_common::internal_err!(
                "attempted to finalize collect-left dynamic filter without reported build data"
            ),
            PartitionStatus::CanceledUnknown => datafusion_common::internal_err!(
                "collect-left dynamic filter cannot finalize with canceled build data"
            ),
        }
    }

    /// Builds the global filter from finalized partitioned build data.
    ///
    /// A canceled partition's keys are unknown, so then the filter is `true`.
    /// `true` is correct for any consumer that reads the filter, including the
    /// rows of the canceled partition.
    fn build_partitioned_filter(&self, partitions: &[PartitionStatus]) -> Result<()> {
        let mut reported = Vec::with_capacity(partitions.len());
        for partition in partitions {
            match partition {
                PartitionStatus::Reported(data) => reported.push(data),
                PartitionStatus::CanceledUnknown => {
                    return self.dynamic_filter.update(lit(true));
                }
                PartitionStatus::Pending => {
                    return datafusion_common::internal_err!(
                        "attempted to finalize dynamic filter with pending partition"
                    );
                }
            }
        }
        self.dynamic_filter
            .update(self.build_global_filter(&reported)?)
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
    fn build_global_filter(
        &self,
        partitions: &[&PartitionData],
    ) -> Result<Arc<dyn PhysicalExpr>> {
        let non_empty: Vec<&PartitionData> = partitions
            .iter()
            .copied()
            .filter(|p| !matches!(p.pushdown, PushdownStrategy::Empty))
            .collect();
        if non_empty.is_empty() {
            // No build row exists, so no probe row can match.
            return Ok(lit(false));
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
                let values = distinct_values(&arrow::compute::concat(&arrays)?)?;
                Some(create_in_list_predicate(
                    &self.on_right,
                    values,
                    self.probe_schema.as_ref(),
                )?)
            }
            _ => {
                let pruning_bitmap =
                    self.merge_pruning_bitmaps(&non_empty, bounds.as_ref());
                // Every partition must have its filter. A partition has none
                // when the build side is too big for a bloom filter, or its
                // memory reservation failed.
                let filters = non_empty
                    .iter()
                    .map(|p| p.build_keys.bloom_filter.as_deref())
                    .collect::<Option<Vec<_>>>();
                let pool = self
                    .global_filter_options
                    .memory_pool
                    .clone()
                    .unwrap_or_else(|| Arc::new(UnboundedMemoryPool::default()));
                let bloom = BloomFilterExpr::try_new(
                    self.on_right.clone(),
                    filters.as_deref(),
                    self.global_filter_options.bloom_filter,
                    &pool,
                    pruning_bitmap,
                )?;
                // Without a bloom filter, keep the expression only for its
                // pruning bitmap
                (bloom.has_filter() || bloom.pruning_bitmap().is_some())
                    .then(|| Arc::new(bloom) as Arc<dyn PhysicalExpr>)
            }
        };
        let bounds_expr = bounds
            .as_ref()
            .and_then(|b| create_bounds_predicate(&self.on_right, b));
        let filter_expr = combine_membership_and_bounds(membership_expr, bounds_expr)
            .unwrap_or_else(|| lit(true));
        self.preserve_probe_nulls(filter_expr, keys_have_null)
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
                PushdownStrategy::Large(Some(bitmap)) => {
                    Some(KeyRangePart::Bitmap(bitmap.as_ref()))
                }
                _ => p
                    .build_keys
                    .keys
                    .first()
                    .map(|keys| KeyRangePart::Keys(keys.as_ref())),
            })
            .collect::<Option<Vec<_>>>()?;
        KeyRangeBitmap::union(&column_bounds.min, &column_bounds.max, &parts)
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
        let needs_probe_nulls = self.not_in.is_some()
            || (self.null_equality == NullEquality::NullEqualsNull
                && build_keys_have_null);
        if !needs_probe_nulls {
            return Ok(filter_expr);
        }
        let keys = match self.not_in {
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
        probe_schema,
        null_equality: NullEquality::NullEqualsNothing,
        not_in: None,
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
    use crate::joins::hash_join::not_in::{NotInOutput, NotInScope};

    /// Adds the bloom filter of `build_keys.keys`, sized for 4 times its
    /// keys (the keys of a few partitions).
    fn test_build_keys(mut build_keys: BuildKeys) -> BuildKeys {
        let reservation = datafusion_execution::memory_pool::MemoryConsumer::new("test")
            .register(&(Arc::new(UnboundedMemoryPool::default()) as Arc<dyn MemoryPool>));
        build_keys.bloom_filter =
            crate::joins::hash_join::bloom_filter::partition_filter(
                &build_keys.keys,
                build_keys.num_distinct * 4,
                GlobalFilterOptions::default().bloom_filter,
                &reservation,
            )
            .unwrap()
            .map(Arc::new);
        build_keys
    }

    use arrow::array::{ArrayRef, BooleanArray, Float64Array, Int32Array};
    use arrow::record_batch::RecordBatch;
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
        let dynamic_filter = test_dynamic_filter(&on_right);
        SharedBuildAccumulator {
            inner: Mutex::new(AccumulatorState {
                data,
                completion: CompletionState::Pending,
            }),
            completion_notify: Notify::new(),
            dynamic_filter,
            on_right,
            probe_schema: test_probe_schema(),
            null_equality: NullEquality::NullEqualsNothing,
            not_in: None,
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

    /// A partition with more keys than the InList limit.
    fn reported_map(keys: Vec<i32>) -> PartitionStatus {
        let (min, max) = (*keys.iter().min().unwrap(), *keys.iter().max().unwrap());
        let num_distinct = keys.len();
        PartitionStatus::Reported(PartitionData {
            pushdown: PushdownStrategy::Large(None),
            bounds: bounds(min, max),
            keys_have_null: false,
            build_keys: test_build_keys(BuildKeys {
                bloom_filter: None,
                keys: vec![Arc::new(Int32Array::from(keys))],
                num_distinct,
            }),
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
                build_keys: test_build_keys(BuildKeys {
                    bloom_filter: None,
                    num_distinct: keys.len(),
                    keys: vec![Arc::new(Int32Array::from(keys))],
                }),
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

    /// A partition InList holds every build row; the global InList holds each
    /// key once.
    #[test]
    fn global_inlist_has_distinct_keys() {
        let acc = make_partitioned_expr_accumulator_for_test(2);
        let partition = |rows: Vec<i32>, num_distinct: usize| {
            PartitionStatus::Reported(PartitionData {
                pushdown: in_list(&rows),
                bounds: bounds(*rows.iter().min().unwrap(), *rows.iter().max().unwrap()),
                keys_have_null: false,
                build_keys: test_build_keys(BuildKeys {
                    bloom_filter: None,
                    num_distinct,
                    keys: vec![Arc::new(Int32Array::from(rows))],
                }),
            })
        };
        acc.build_filter(FinalizeInput::Partitioned(vec![
            partition(vec![1, 1, 3, 1, 3], 2),
            partition(vec![5, 5, 5], 1),
        ]))
        .unwrap();
        let expr = current_expr(&acc);
        let and = binary_expr(&expr);
        assert_in_list_column_values(and.right(), "probe_key", 0, &[1, 3, 5]);
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

    /// A canceled partition's keys are unknown, so the filter keeps every row.
    #[test]
    fn partitioned_canceled_partition_sets_filter_to_true() {
        let acc = make_partitioned_expr_accumulator_for_test(3);
        acc.build_filter(FinalizeInput::Partitioned(vec![
            reported_map((0..1000).collect()),
            PartitionStatus::CanceledUnknown,
            reported(PushdownStrategy::Empty, no_bounds()),
        ]))
        .unwrap();
        assert_eq!(current_expr(&acc).to_string(), "true");
    }

    /// Only canceled partitions: the filter is `true`, not the `false` of an
    /// empty build side.
    #[test]
    fn partitioned_all_canceled_partitions_set_filter_to_true() {
        let acc = make_partitioned_expr_accumulator_for_test(2);
        acc.build_filter(FinalizeInput::Partitioned(vec![
            PartitionStatus::CanceledUnknown,
            PartitionStatus::CanceledUnknown,
        ]))
        .unwrap();
        assert_eq!(current_expr(&acc).to_string(), "true");
    }

    /// All partitions empty: no probe row can match.
    #[test]
    fn partitioned_all_empty_partitions_set_filter_to_false() {
        let acc = make_partitioned_expr_accumulator_for_test(2);
        acc.build_filter(FinalizeInput::Partitioned(vec![
            reported(PushdownStrategy::Empty, no_bounds()),
            reported(PushdownStrategy::Empty, no_bounds()),
        ]))
        .unwrap();
        assert_eq!(current_expr(&acc).to_string(), "false");
    }

    /// Float keys get the global bloom filter too.
    #[test]
    fn float_keys_use_global_bloom_filter() {
        let mut acc = make_partitioned_expr_accumulator_for_test(2);
        acc.probe_schema = Arc::new(Schema::new(vec![Field::new(
            "probe_key",
            DataType::Float64,
            false,
        )]));
        let float_partition = |keys: Vec<f64>| {
            PartitionStatus::Reported(PartitionData {
                pushdown: PushdownStrategy::Large(None),
                bounds: no_bounds(),
                keys_have_null: false,
                build_keys: test_build_keys(BuildKeys {
                    bloom_filter: None,
                    num_distinct: keys.len(),
                    keys: vec![Arc::new(Float64Array::from(keys))],
                }),
            })
        };
        acc.build_filter(FinalizeInput::Partitioned(vec![
            float_partition((0..1000).map(f64::from).collect()),
            float_partition((1000..2000).map(f64::from).collect()),
        ]))
        .unwrap();
        let expr = current_expr(&acc);
        assert_eq!(format!("{expr}"), "bloom_filter");
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

    /// A partition without hashes (too big alone, or its memory reservation
    /// failed) means no bloom filter: the filter keeps only the bounds and the
    /// pruning bitmap.
    #[test]
    fn partition_without_hashes_skips_bloom_filter() {
        let acc = make_partitioned_expr_accumulator_for_test(2);
        let mut no_hashes = reported_map((1_000_000..1_000_100).collect());
        if let PartitionStatus::Reported(data) = &mut no_hashes {
            data.build_keys.bloom_filter = None;
        }
        acc.build_filter(FinalizeInput::Partitioned(vec![
            reported_map((0..100).collect()),
            no_hashes,
        ]))
        .unwrap();
        let expr = current_expr(&acc);
        assert_eq!(
            expr.to_string(),
            "probe_key@0 >= 0 AND probe_key@0 <= 1000099 AND key_range_pruning"
        );
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
    fn partitioned_one_real_partition_with_rest_empty_uses_its_in_list() {
        let acc = make_partitioned_expr_accumulator_for_test(3);

        acc.build_filter(FinalizeInput::Partitioned(vec![
            reported(PushdownStrategy::Empty, no_bounds()),
            reported(in_list(&[2]), no_bounds()),
            reported(PushdownStrategy::Empty, no_bounds()),
        ]))
        .unwrap();

        let expr = current_expr(&acc);
        assert_in_list_column_values(&expr, "probe_key", 0, &[2]);
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

    /// A partition that waits for the filter completes when another partition
    /// is canceled, and the filter is `true`.
    #[tokio::test]
    async fn canceled_partition_completes_waiting_partition() {
        let acc = make_partitioned_expr_accumulator_for_test(2);
        let report = acc.report_build_data(PartitionBuildData::Partitioned {
            partition_id: 0,
            pushdown: in_list(&[1, 2]),
            bounds: bounds(1, 2),
            keys_have_null: false,
            build_keys: BuildKeys::default(),
        });
        let mut report = std::pin::pin!(report);
        let mut cx = std::task::Context::from_waker(futures::task::noop_waker_ref());
        assert!(report.as_mut().poll(&mut cx).is_pending());

        acc.report_canceled_partition(1);

        tokio::time::timeout(std::time::Duration::from_secs(5), report)
            .await
            .expect("the waiting partition must complete")
            .unwrap();
        assert_eq!(current_expr(&acc).to_string(), "true");
        assert!(
            futures::poll!(std::pin::pin!(acc.dynamic_filter.wait_complete())).is_ready()
        );
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
            probe_schema,
            null_equality,
            not_in: null_aware.then_some(NotIn::BuildRows {
                scope: NotInScope::Uncorrelated,
                output: NotInOutput::Rows,
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

    /// A canceled partition gives `true` without an IS NULL disjunct, also for a
    /// null-equal join.
    #[test]
    fn partitioned_canceled_partition_null_equal_filter_is_true() {
        let acc = null_equal_partitioned_accumulator(2);

        acc.build_filter(FinalizeInput::Partitioned(vec![
            PartitionStatus::CanceledUnknown,
            reported_with_null_keys(in_list(&[2]), no_bounds()),
        ]))
        .unwrap();

        assert_eq!(current_expr(&acc).to_string(), "true");
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
                probe_schema: Arc::new(Schema::new(fields)),
                null_equality: NullEquality::NullEqualsNothing,
                not_in: Some(NotIn::BuildRows {
                    scope: NotInScope::Correlated { value_keys },
                    output: NotInOutput::Mark,
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
                PushdownStrategy::Large(None),
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
