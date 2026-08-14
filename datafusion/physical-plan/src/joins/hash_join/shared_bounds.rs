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
use crate::joins::PartitionMode;
use crate::joins::hash_join::exec::HASH_JOIN_SEED;
use crate::joins::hash_join::inlist_builder::build_struct_fields;
use crate::joins::hash_join::partitioned_hash_eval::{
    HashExpr, HashTableLookupExpr, SeededRandomState,
};
use crate::joins::{ArrayMap, Map};
use crate::metrics::Count;
use crate::repartition::RangeExpr;
use arrow::array::ArrayRef;
use arrow::datatypes::{DataType, Field, Schema};
use datafusion_common::config::ConfigOptions;
use datafusion_common::{
    DataFusionError, NullEquality, Result, ScalarValue, SharedResult,
    assert_or_internal_err,
};
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
}

/// A build-side representation that can produce a membership predicate.
#[derive(Clone, Copy)]
enum MembershipSource<'a> {
    InList(&'a InListMembership),
    Map(&'a Arc<Map>),
}

impl MembershipSource<'_> {
    fn distinct_key_count_lower_bound(self) -> Option<usize> {
        match self {
            Self::InList(membership) => membership.distinct_key_count_lower_bound,
            Self::Map(map) => Some(map.num_of_distinct_key()),
        }
    }
}

/// Creates a membership predicate using the representation selected while
/// collecting the build side.
///
/// Supports both single-column and multi-column joins using struct expressions.
fn create_membership_predicate(
    on_right: &[PhysicalExprRef],
    source: MembershipSource<'_>,
    random_state: &SeededRandomState,
    schema: &Schema,
) -> Result<Arc<dyn PhysicalExpr>> {
    match source {
        MembershipSource::InList(membership) => {
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
            Ok(Arc::new(InListExpr::try_new_from_array(
                expr,
                Arc::clone(&membership.values),
                false,
                schema,
            )?))
        }
        MembershipSource::Map(hash_map) => Ok(Arc::new(HashTableLookupExpr::new(
            on_right.to_vec(),
            random_state.clone(),
            Arc::clone(hash_map),
            "hash_lookup".to_string(),
        )) as Arc<dyn PhysicalExpr>),
    }
}

fn create_column_bounds_predicate(
    right_expr: &PhysicalExprRef,
    column_bounds: &ColumnBounds,
) -> Arc<dyn PhysicalExpr> {
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
    Arc::new(BinaryExpr::new(min_expr, Operator::And, max_expr)) as Arc<dyn PhysicalExpr>
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
            column_predicates
                .push(create_column_bounds_predicate(right_expr, column_bounds));
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

fn inclusive_integer_span(column_bounds: &ColumnBounds) -> Option<u128> {
    if column_bounds.min.data_type() != column_bounds.max.data_type()
        || column_bounds.min > column_bounds.max
    {
        return None;
    }
    let (min, max) = ArrayMap::key_to_u64(&column_bounds.min)
        .zip(ArrayMap::key_to_u64(&column_bounds.max))?;

    // ArrayMap uses two's-complement values and wrapping subtraction, which
    // also gives the correct span for signed ranges that cross zero.
    Some(u128::from(ArrayMap::calculate_range(min, max)) + 1)
}

/// Returns the bounds predicate when the membership set is provably identical
/// to its single-column integer bounds.
///
/// `distinct_key_count_lower_bound` is exact for [`ArrayMap`] and is the number of
/// distinct hashes for the regular hash map. The latter is a lower bound on
/// distinct keys. Because the keys are also bounded by the inclusive integer
/// span, equality with the span proves that every value is present even when
/// hash collisions are possible.
fn complete_integer_domain_bounds_predicate(
    source: MembershipSource<'_>,
    bounds: &PartitionBounds,
    on_right: &[PhysicalExprRef],
) -> Option<Arc<dyn PhysicalExpr>> {
    let [right_expr] = on_right else {
        return None;
    };
    let [column_bounds] = bounds.column_bounds.as_slice() else {
        return None;
    };
    let distinct_key_count_lower_bound = source.distinct_key_count_lower_bound()?;
    let inclusive_span = inclusive_integer_span(column_bounds)?;
    if let MembershipSource::InList(membership) = source
        && membership.values.data_type() != &column_bounds.min.data_type()
    {
        return None;
    }

    if inclusive_span != distinct_key_count_lower_bound as u128 {
        return None;
    }

    Some(create_column_bounds_predicate(right_expr, column_bounds))
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
    null_aware: bool,
    /// Membership predicates omitted after proving their integer keys cover
    /// every value in the corresponding min/max bounds.
    membership_predicates_elided: Count,
}

/// Runtime dependencies used to construct a HashJoin dynamic filter.
pub(super) struct DynamicFilterBuildContext {
    filter: Arc<DynamicFilterPhysicalExpr>,
    probe_exprs: Vec<PhysicalExprRef>,
    repartition_random_state: SeededRandomState,
    null_equality: NullEquality,
    null_aware: bool,
    membership_predicates_elided: Count,
}

impl DynamicFilterBuildContext {
    pub(super) fn new(
        filter: Arc<DynamicFilterPhysicalExpr>,
        probe_exprs: Vec<PhysicalExprRef>,
        repartition_random_state: SeededRandomState,
        null_equality: NullEquality,
        null_aware: bool,
        membership_predicates_elided: Count,
    ) -> Self {
        Self {
            filter,
            probe_exprs,
            repartition_random_state,
            null_equality,
            null_aware,
            membership_predicates_elided,
        }
    }
}

/// Data required to build an `InListExpr`, together with cardinality evidence
/// derived from the same build-side key set.
#[derive(Clone)]
pub(crate) struct InListMembership {
    values: ArrayRef,
    /// A lower bound on the number of distinct matchable keys. This is exact
    /// for an ArrayMap and may be conservative for a hash map due to collisions.
    distinct_key_count_lower_bound: Option<usize>,
}

impl InListMembership {
    /// `map` must index the same build-side rows represented by `values`.
    pub(super) fn new(values: ArrayRef, map: &Map) -> Self {
        let distinct_key_count_lower_bound = map.num_of_distinct_key();
        Self {
            // A count larger than the source array cannot describe the same
            // build set, so discard the evidence and retain membership.
            distinct_key_count_lower_bound: (distinct_key_count_lower_bound
                <= values.len())
            .then_some(distinct_key_count_lower_bound),
            values,
        }
    }
}

/// Strategy for filter pushdown (decided at collection time)
#[derive(Clone)]
pub(crate) enum PushdownStrategy {
    /// Use InList when the configured size and cardinality limits allow it.
    InList(InListMembership),
    /// Reuse the build map when an InList is not selected.
    Map(Arc<Map>),
    /// There was no data in this partition, do not build a dynamic filter for it
    Empty,
}

impl PushdownStrategy {
    fn membership_source(&self) -> Option<MembershipSource<'_>> {
        match self {
            Self::InList(membership) => Some(MembershipSource::InList(membership)),
            Self::Map(map) => Some(MembershipSource::Map(map)),
            Self::Empty => None,
        }
    }
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
    pub(super) fn new_from_partition_mode(
        partition_mode: PartitionMode,
        left_child: &dyn ExecutionPlan,
        right_child: &dyn ExecutionPlan,
        context: DynamicFilterBuildContext,
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

        let DynamicFilterBuildContext {
            filter,
            probe_exprs,
            repartition_random_state,
            null_equality,
            null_aware,
            membership_predicates_elided,
        } = context;

        Self {
            inner: Mutex::new(AccumulatorState {
                data: mode_data,
                completion: CompletionState::Pending,
            }),
            completion_notify: Notify::new(),
            dynamic_filter: filter,
            on_right: probe_exprs,
            repartition_random_state,
            probe_schema: right_child.schema(),
            probe_range_partitioning,
            null_equality,
            null_aware,
            membership_predicates_elided,
        }
    }

    /// Builds the complete dynamic-filter predicate for one build partition.
    /// The elision count is returned separately so it can be committed only
    /// after the complete dynamic filter has been installed successfully.
    fn create_partition_filter(
        &self,
        partition: &PartitionData,
    ) -> Result<(Option<Arc<dyn PhysicalExpr>>, usize)> {
        let Some(source) = partition.pushdown.membership_source() else {
            return Ok((
                create_bounds_predicate(&self.on_right, &partition.bounds),
                0,
            ));
        };

        let (filter_expr, membership_predicates_elided) =
            self.create_non_empty_partition_filter(partition, source)?;
        Ok((Some(filter_expr), membership_predicates_elided))
    }

    /// Builds the predicate for a partition that has membership data, together
    /// with the number of membership predicates omitted from that expression.
    fn create_non_empty_partition_filter(
        &self,
        partition: &PartitionData,
        source: MembershipSource<'_>,
    ) -> Result<(Arc<dyn PhysicalExpr>, usize)> {
        // Membership cardinality can include NULL as a distinct key under
        // `NullEqualsNull`. Since min/max bounds describe only non-NULL values,
        // counting that NULL could falsely prove a gapped integer interval is
        // complete (for example {1, 3, NULL} over [1, 3]).
        let cardinality_includes_null = self.null_equality
            == NullEquality::NullEqualsNull
            && partition.keys_have_null;
        if !cardinality_includes_null
            && let Some(bounds_expr) = complete_integer_domain_bounds_predicate(
                source,
                &partition.bounds,
                &self.on_right,
            )
        {
            return Ok((bounds_expr, 1));
        }

        let membership_expr = create_membership_predicate(
            &self.on_right,
            source,
            &HASH_JOIN_SEED,
            self.probe_schema.as_ref(),
        )?;

        Ok((
            if let Some(bounds_expr) =
                create_bounds_predicate(&self.on_right, &partition.bounds)
            {
                Arc::new(BinaryExpr::new(bounds_expr, Operator::And, membership_expr))
                    as Arc<dyn PhysicalExpr>
            } else {
                membership_expr
            },
            0,
        ))
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
        let membership_predicates_elided = match finalize_input {
            FinalizeInput::CollectLeft(partition) => match partition {
                PartitionStatus::Reported(partition_data) => {
                    let (filter_expr, membership_predicates_elided) =
                        self.create_partition_filter(&partition_data)?;
                    if let Some(filter_expr) = filter_expr {
                        self.dynamic_filter.update(self.preserve_probe_nulls(
                            filter_expr,
                            partition_data.keys_have_null,
                        )?)?;
                    }
                    membership_predicates_elided
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
                let num_partitions = partitions.len();
                let mut partition_filters = Vec::with_capacity(num_partitions);
                let mut real_partition_ids = Vec::new();
                let mut empty_partition_ids = Vec::new();
                let mut has_canceled_unknown = false;
                let mut membership_predicates_elided = 0;
                let mut keys_have_null = false;

                for (partition_id, partition) in partitions.iter().enumerate() {
                    match partition {
                        PartitionStatus::Reported(partition)
                            if matches!(partition.pushdown, PushdownStrategy::Empty) =>
                        {
                            empty_partition_ids.push(partition_id);
                            partition_filters.push(lit(false));
                        }
                        PartitionStatus::Reported(partition) => {
                            real_partition_ids.push(partition_id);
                            keys_have_null |= partition.keys_have_null;
                            let Some(source) = partition.pushdown.membership_source()
                            else {
                                return datafusion_common::internal_err!(
                                    "non-empty pushdown must have membership data"
                                );
                            };
                            let (then_expr, partition_membership_predicates_elided) =
                                self.create_non_empty_partition_filter(
                                    partition, source,
                                )?;
                            membership_predicates_elided +=
                                partition_membership_predicates_elided;
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

                let filter_expr = if has_canceled_unknown
                    && real_partition_ids.is_empty()
                    && empty_partition_ids.is_empty()
                {
                    lit(true)
                } else if !has_canceled_unknown && real_partition_ids.is_empty() {
                    lit(false)
                } else if !has_canceled_unknown
                    && real_partition_ids.len() == 1
                    && empty_partition_ids.len() + 1 == num_partitions
                {
                    Arc::clone(&partition_filters[real_partition_ids[0]])
                } else if let Some(range_partitioning) = &self.probe_range_partitioning {
                    // Range partitioning
                    assert_or_internal_err!(
                        partition_filters.len() == range_partitioning.partition_count(),
                        "Dynamic filter partition count {} does not match Range partition count {}",
                        partition_filters.len(),
                        range_partitioning.partition_count()
                    );
                    let routing_range_expr = Arc::new(RangeExpr::try_new(
                        self.on_right.clone(),
                        range_partitioning,
                    )?)
                        as Arc<dyn PhysicalExpr>;
                    let else_expr = partition_filters
                        .pop()
                        .expect("Range partitioning always has at least one partition");

                    // CASE range_partition(key)
                    //   WHEN 0 THEN F0
                    //   WHEN 1 THEN F1
                    //   ...
                    //   ELSE Fn
                    // END
                    let when_then_expr = partition_filters
                        .into_iter()
                        .enumerate()
                        .map(|(partition_id, then_expr)| {
                            (
                                lit(ScalarValue::UInt64(Some(partition_id as u64))),
                                then_expr,
                            )
                        })
                        .collect();

                    Arc::new(CaseExpr::try_new(
                        Some(routing_range_expr),
                        when_then_expr,
                        Some(else_expr),
                    )?) as Arc<dyn PhysicalExpr>
                } else {
                    // Hash partitioning
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

                    let mut when_then_branches = if has_canceled_unknown {
                        empty_partition_ids
                            .into_iter()
                            .map(|partition_id| {
                                (
                                    lit(ScalarValue::UInt64(Some(partition_id as u64))),
                                    lit(false),
                                )
                            })
                            .collect::<Vec<_>>()
                    } else {
                        vec![]
                    };
                    when_then_branches.extend(real_partition_ids.into_iter().map(
                        |partition_id| {
                            (
                                lit(ScalarValue::UInt64(Some(partition_id as u64))),
                                Arc::clone(&partition_filters[partition_id]),
                            )
                        },
                    ));

                    Arc::new(CaseExpr::try_new(
                        Some(modulo_expr),
                        when_then_branches,
                        Some(lit(has_canceled_unknown)),
                    )?) as Arc<dyn PhysicalExpr>
                };

                self.dynamic_filter
                    .update(self.preserve_probe_nulls(filter_expr, keys_have_null)?)?;
                membership_predicates_elided
            }
        };

        self.membership_predicates_elided
            .add(membership_predicates_elided);
        Ok(())
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
        on_right: vec![],
        repartition_random_state: SeededRandomState::with_seed(1),
        probe_schema,
        probe_range_partitioning: None,
        null_equality: NullEquality::NullEqualsNothing,
        null_aware: false,
        membership_predicates_elided: Count::new(),
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
    use crate::joins::utils::JoinHashMapType;
    use arrow::array::{ArrayRef, BooleanArray, Float64Array, Int32Array, UInt64Array};
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

    fn invalid_test_on_right() -> Vec<PhysicalExprRef> {
        vec![Arc::new(Column::new("missing_probe_key", 1))]
    }

    fn membership_matches_integer_bounds(
        pushdown: &PushdownStrategy,
        bounds: &PartitionBounds,
        join_key_count: usize,
    ) -> bool {
        let on_right = (0..join_key_count)
            .map(|index| Arc::new(Column::new("probe_key", index)) as PhysicalExprRef)
            .collect::<Vec<_>>();
        pushdown
            .membership_source()
            .and_then(|source| {
                complete_integer_domain_bounds_predicate(source, bounds, &on_right)
            })
            .is_some()
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
            null_aware: false,
            membership_predicates_elided: Count::new(),
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
        let array = Arc::new(Int32Array::from(values.to_vec())) as ArrayRef;
        let map = array_map_from_i32(values);
        PushdownStrategy::InList(InListMembership::new(array, map.as_ref()))
    }

    fn array_map_from_i32(values: &[i32]) -> Arc<Map> {
        let array = Arc::new(Int32Array::from(values.to_vec())) as ArrayRef;
        let min = values.iter().min().copied().unwrap() as u64;
        let max = values.iter().max().copied().unwrap() as u64;
        Arc::new(Map::ArrayMap(ArrayMap::try_new(&array, min, max).unwrap()))
    }

    fn array_map(values: &[i32]) -> PushdownStrategy {
        PushdownStrategy::Map(array_map_from_i32(values))
    }

    fn regular_hash_map_with_hashes(hashes: &[u64]) -> PushdownStrategy {
        let mut map = JoinHashMapU32::with_capacity(hashes.len());
        map.update_from_iter(Box::new(hashes.iter().enumerate()), 0);
        PushdownStrategy::Map(Arc::new(Map::HashMap(Box::new(map))))
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
    fn collect_left_contiguous_integer_membership_uses_bounds_only() {
        let acc = make_collect_left_accumulator_for_test();

        acc.build_filter(FinalizeInput::CollectLeft(reported(
            in_list(&[1, 2, 3]),
            bounds(1, 3),
        )))
        .unwrap();

        let expr = current_expr(&acc);
        let bounds = binary_expr(&expr);
        assert_eq!(bounds.op(), &Operator::And);
        assert!(
            bounds.right().downcast_ref::<InListExpr>().is_none(),
            "a complete integer interval must not retain a redundant InList"
        );
        assert_eq!(acc.membership_predicates_elided.value(), 1);
    }

    #[test]
    fn collect_left_duplicate_contiguous_membership_uses_bounds_only() {
        let acc = make_collect_left_accumulator_for_test();

        acc.build_filter(FinalizeInput::CollectLeft(reported(
            in_list(&[1, 2, 2, 3]),
            bounds(1, 3),
        )))
        .unwrap();

        let expr = current_expr(&acc);
        assert!(
            binary_expr(&expr)
                .right()
                .downcast_ref::<InListExpr>()
                .is_none()
        );
    }

    #[test]
    fn collect_left_gapped_integer_membership_is_retained() {
        let acc = make_collect_left_accumulator_for_test();

        acc.build_filter(FinalizeInput::CollectLeft(reported(
            in_list(&[1, 3]),
            bounds(1, 3),
        )))
        .unwrap();

        let expr = current_expr(&acc);
        assert!(
            binary_expr(&expr)
                .right()
                .downcast_ref::<InListExpr>()
                .is_some()
        );
        assert_eq!(acc.membership_predicates_elided.value(), 0);
    }

    #[test]
    fn collect_left_contiguous_array_map_uses_bounds_only() {
        let acc = make_collect_left_accumulator_for_test();

        acc.build_filter(FinalizeInput::CollectLeft(reported(
            array_map(&[1, 2, 3]),
            bounds(1, 3),
        )))
        .unwrap();

        let expr = current_expr(&acc);
        assert!(
            binary_expr(&expr)
                .right()
                .downcast_ref::<HashTableLookupExpr>()
                .is_none()
        );
    }

    #[test]
    fn collect_left_gapped_array_map_membership_is_retained() {
        let acc = make_collect_left_accumulator_for_test();

        acc.build_filter(FinalizeInput::CollectLeft(reported(
            array_map(&[1, 3]),
            bounds(1, 3),
        )))
        .unwrap();

        let expr = current_expr(&acc);
        assert!(
            binary_expr(&expr)
                .right()
                .downcast_ref::<HashTableLookupExpr>()
                .is_some()
        );
    }

    #[test]
    fn collect_left_contiguous_regular_hash_map_uses_bounds_only() {
        let acc = make_collect_left_accumulator_for_test();

        acc.build_filter(FinalizeInput::CollectLeft(reported(
            regular_hash_map_with_hashes(&[10, 11, 12]),
            bounds(1, 3),
        )))
        .unwrap();

        let expr = current_expr(&acc);
        assert!(
            binary_expr(&expr)
                .right()
                .downcast_ref::<HashTableLookupExpr>()
                .is_none()
        );
        assert_eq!(acc.membership_predicates_elided.value(), 1);
    }

    #[test]
    fn null_equal_cardinality_does_not_prove_non_null_integer_domain() {
        let mut acc = make_collect_left_accumulator_for_test();
        acc.null_equality = NullEquality::NullEqualsNull;
        let partition = PartitionData {
            // Model the three distinct matchable keys {1, 3, NULL}. The NULL
            // contributes to the hash-map cardinality but not to [min, max].
            pushdown: regular_hash_map_with_hashes(&[10, 11, 12]),
            bounds: bounds(1, 3),
            keys_have_null: true,
        };
        let source = partition
            .pushdown
            .membership_source()
            .expect("regular hash map must provide membership data");

        let (expr, membership_predicates_elided) = acc
            .create_non_empty_partition_filter(&partition, source)
            .unwrap();

        assert!(
            binary_expr(&expr)
                .right()
                .downcast_ref::<HashTableLookupExpr>()
                .is_some(),
            "NULL cardinality must not make the gapped non-NULL domain look complete"
        );
        assert_eq!(membership_predicates_elided, 0);
    }

    #[test]
    fn collect_left_gapped_regular_hash_map_membership_is_retained() {
        let acc = make_collect_left_accumulator_for_test();

        acc.build_filter(FinalizeInput::CollectLeft(reported(
            regular_hash_map_with_hashes(&[10, 12]),
            bounds(1, 3),
        )))
        .unwrap();

        let expr = current_expr(&acc);
        assert!(
            binary_expr(&expr)
                .right()
                .downcast_ref::<HashTableLookupExpr>()
                .is_some()
        );
        assert_eq!(acc.membership_predicates_elided.value(), 0);
    }

    #[test]
    fn collect_left_regular_hash_map_collision_is_conservative() {
        let acc = make_collect_left_accumulator_for_test();

        // Three contiguous keys that produce only two distinct hashes do not
        // satisfy the lower-bound proof, even though the key domain is complete.
        acc.build_filter(FinalizeInput::CollectLeft(reported(
            regular_hash_map_with_hashes(&[10, 10, 12]),
            bounds(1, 3),
        )))
        .unwrap();

        let expr = current_expr(&acc);
        assert!(
            binary_expr(&expr)
                .right()
                .downcast_ref::<HashTableLookupExpr>()
                .is_some()
        );
        assert_eq!(acc.membership_predicates_elided.value(), 0);
    }

    #[test]
    fn membership_range_proof_accepts_signed_and_unsigned_integers() {
        let membership = in_list(&[-2, -1, 0, 1, 2]);
        assert!(membership_matches_integer_bounds(
            &membership,
            &bounds(-2, 2),
            1,
        ));

        let values = Arc::new(UInt64Array::from(vec![10, 11, 12])) as ArrayRef;
        let map = Arc::new(Map::ArrayMap(ArrayMap::try_new(&values, 10, 12).unwrap()));
        let membership =
            PushdownStrategy::InList(InListMembership::new(values, map.as_ref()));
        assert!(membership_matches_integer_bounds(
            &membership,
            &PartitionBounds::new(vec![ColumnBounds::new(
                ScalarValue::UInt64(Some(10)),
                ScalarValue::UInt64(Some(12)),
            )]),
            1,
        ));
    }

    #[test]
    fn membership_range_proof_rejects_unsupported_inputs() {
        let membership = in_list(&[-2, -1, 0, 1, 2]);
        assert!(!membership_matches_integer_bounds(
            &PushdownStrategy::Empty,
            &bounds(-2, 2),
            1,
        ));
        assert!(!membership_matches_integer_bounds(
            &membership,
            &no_bounds(),
            1,
        ));
        assert!(!membership_matches_integer_bounds(
            &membership,
            &bounds(-2, 2),
            0,
        ));
        assert!(!membership_matches_integer_bounds(
            &membership,
            &bounds(-2, 2),
            2,
        ));
        assert!(!membership_matches_integer_bounds(
            &membership,
            &PartitionBounds::new(vec![
                ColumnBounds::new(
                    ScalarValue::Int32(Some(-2)),
                    ScalarValue::Int32(Some(2)),
                ),
                ColumnBounds::new(
                    ScalarValue::Int32(Some(0)),
                    ScalarValue::Int32(Some(0)),
                ),
            ]),
            1,
        ));
        assert!(!membership_matches_integer_bounds(
            &membership,
            &PartitionBounds::new(vec![ColumnBounds::new(
                ScalarValue::Utf8(Some("a".to_string())),
                ScalarValue::Utf8(Some("e".to_string())),
            )]),
            1,
        ));
        assert!(!membership_matches_integer_bounds(
            &membership,
            &PartitionBounds::new(vec![ColumnBounds::new(
                ScalarValue::Int32(Some(-2)),
                ScalarValue::Int64(Some(2)),
            )]),
            1,
        ));
        assert!(!membership_matches_integer_bounds(
            &membership,
            &PartitionBounds::new(vec![ColumnBounds::new(
                ScalarValue::Int32(None),
                ScalarValue::Int32(Some(2)),
            )]),
            1,
        ));
        assert!(!membership_matches_integer_bounds(
            &membership,
            &PartitionBounds::new(vec![ColumnBounds::new(
                ScalarValue::Int32(Some(-2)),
                ScalarValue::Int32(None),
            )]),
            1,
        ));
        assert!(!membership_matches_integer_bounds(
            &membership,
            &PartitionBounds::new(vec![ColumnBounds::new(
                ScalarValue::Int64(Some(-2)),
                ScalarValue::Int64(Some(2)),
            )]),
            1,
        ));
        assert!(!membership_matches_integer_bounds(
            &membership,
            &bounds(2, -2),
            1,
        ));

        let values = Arc::new(Int32Array::from(vec![1, 3])) as ArrayRef;
        let unrelated_map = array_map_from_i32(&[1, 2, 3]);
        let inconsistent = PushdownStrategy::InList(InListMembership::new(
            values,
            unrelated_map.as_ref(),
        ));
        assert!(!membership_matches_integer_bounds(
            &inconsistent,
            &bounds(1, 3),
            1,
        ));

        let full_i64_domain = ColumnBounds::new(
            ScalarValue::Int64(Some(i64::MIN)),
            ScalarValue::Int64(Some(i64::MAX)),
        );
        assert_eq!(inclusive_integer_span(&full_i64_domain), Some(1_u128 << 64));
        assert_ne!(
            inclusive_integer_span(&full_i64_domain),
            Some(usize::MAX as u128)
        );
    }

    #[test]
    fn membership_expression_errors_propagate_for_both_modes() {
        let collect_left = make_accumulator_for_test(
            AccumulatedBuildData::CollectLeft {
                data: PartitionStatus::Pending,
                reported_count: 0,
                expected_reports: 1,
            },
            invalid_test_on_right(),
        );
        assert!(
            collect_left
                .build_filter(FinalizeInput::CollectLeft(reported(
                    in_list(&[1, 3]),
                    no_bounds(),
                )))
                .is_err(),
            "CollectLeft must propagate membership-expression construction errors"
        );

        let partitioned = make_accumulator_for_test(
            AccumulatedBuildData::Partitioned {
                partitions: vec![PartitionStatus::Pending; 2],
                completed_partitions: 0,
            },
            invalid_test_on_right(),
        );
        assert!(
            partitioned
                .build_filter(FinalizeInput::Partitioned(vec![
                    reported(in_list(&[1, 2, 3]), bounds(1, 3)),
                    reported(in_list(&[1, 3]), no_bounds()),
                ]))
                .is_err(),
            "Partitioned must propagate membership-expression construction errors"
        );
        assert_eq!(
            partitioned.membership_predicates_elided.value(),
            0,
            "failed finalization must not report an uninstalled elision"
        );
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
    fn partitioned_elides_only_contiguous_membership_predicates() {
        let acc = make_partitioned_expr_accumulator_for_test(2);

        acc.build_filter(FinalizeInput::Partitioned(vec![
            reported(in_list(&[1, 2, 3]), bounds(1, 3)),
            reported(in_list(&[10, 12]), bounds(10, 12)),
        ]))
        .unwrap();

        let expr = current_expr(&acc);
        let branches = case_expr(&expr).when_then_expr();
        assert_eq!(branches.len(), 2);

        let contiguous = binary_expr(&branches[0].1);
        assert_eq!(contiguous.op(), &Operator::And);
        assert!(contiguous.right().downcast_ref::<InListExpr>().is_none());

        let gapped = binary_expr(&branches[1].1);
        assert_eq!(gapped.op(), &Operator::And);
        assert!(gapped.right().downcast_ref::<InListExpr>().is_some());
        assert_eq!(acc.membership_predicates_elided.value(), 1);
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
        assert_eq!(case.when_then_expr().len(), 3);

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
            null_aware,
            membership_predicates_elided: Count::new(),
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
