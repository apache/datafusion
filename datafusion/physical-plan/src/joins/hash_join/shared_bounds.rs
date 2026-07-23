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
use crate::ordering::build_lexicographic_filter;
use arrow::array::ArrayRef;
use arrow::datatypes::{DataType, Field, Schema};
use datafusion_common::config::ConfigOptions;
use datafusion_common::{DataFusionError, Result, ScalarValue, SharedResult};
use datafusion_expr::Operator;
use datafusion_functions::core::r#struct as struct_func;
use datafusion_physical_expr::expressions::{
    BinaryExpr, CaseExpr, DynamicFilterPhysicalExpr, InListExpr, lit,
};
use datafusion_physical_expr::{
    PhysicalExpr, PhysicalExprRef, PhysicalSortExpr, RangePartitioning,
    ScalarFunctionExpr,
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
        repartition_random_state: SeededRandomState,
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

        let probe_range_partitioning = if partition_mode == PartitionMode::Partitioned {
            match right_child.output_partitioning() {
                crate::Partitioning::Range(range) => Some(range.clone()),
                _ => None,
            }
        } else {
            None
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
                let num_partitions = partitions.len();
                let mut partition_filters = Vec::with_capacity(num_partitions);
                let mut real_partition_ids = Vec::new();
                let mut empty_partition_ids = Vec::new();
                let mut has_canceled_unknown = false;

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
                            let membership_expr = create_membership_predicate(
                                &self.on_right,
                                partition.pushdown.clone(),
                                &HASH_JOIN_SEED,
                                self.probe_schema.as_ref(),
                            )?;
                            let bounds_expr = create_bounds_predicate(
                                &self.on_right,
                                &partition.bounds,
                            );
                            let then_expr = combine_membership_and_bounds(
                                membership_expr,
                                bounds_expr,
                            )
                            .unwrap_or_else(|| lit(true));
                            partition_filters.push(then_expr);
                        }
                        PartitionStatus::CanceledUnknown => {
                            has_canceled_unknown = true;
                            partition_filters.push(lit(true));
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
                    assert_eq!(
                        partition_filters.len(),
                        range_partitioning.partition_count()
                    );
                    assert_eq!(self.on_right.len(), range_partitioning.ordering().len());
                    let sort_exprs = self
                        .on_right
                        .iter()
                        .zip(range_partitioning.ordering())
                        .map(|(expr, sort_expr)| {
                            PhysicalSortExpr::new(Arc::clone(expr), sort_expr.options)
                        })
                        .collect::<Vec<_>>();
                    let else_expr = partition_filters
                        .pop()
                        .expect("Range partitioning always has at least one partition");
                    let mut when_then_expr = Vec::with_capacity(partition_filters.len());
                    // CASE evaluates in order
                    //
                    // CASE
                    //   WHEN key <range split[0] THEN F0
                    //   WHEN key <range split[1] THEN F1
                    //   ...
                    //   ELSE Fn
                    // END
                    for (split_point, then_expr) in range_partitioning
                        .split_points()
                        .iter()
                        .zip(partition_filters)
                    {
                        let when_expr = build_lexicographic_filter(
                            &sort_exprs,
                            split_point.values(),
                        )?;
                        when_then_expr.push((when_expr, then_expr));
                    }

                    Arc::new(CaseExpr::try_new(None, when_then_expr, Some(else_expr))?)
                        as Arc<dyn PhysicalExpr>
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

                self.dynamic_filter.update(filter_expr)?;
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

    use arrow::array::{ArrayRef, BooleanArray, Int32Array};
    use arrow::record_batch::RecordBatch;
    use datafusion_common::SplitPoint;
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
            repartition_random_state: SeededRandomState::with_seed(1),
            probe_schema: test_probe_schema(),
            probe_range_partitioning: None,
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
        PartitionStatus::Reported(PartitionData { pushdown, bounds })
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
    fn partitioned_range_dynamic_filter_routes_with_searched_case() -> Result<()> {
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
            case.expr().is_none(),
            "Range routing must use searched CASE"
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
}
