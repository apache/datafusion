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

use std::borrow::Cow;
use std::collections::{HashMap, VecDeque};
use std::marker::PhantomData;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, AsArray, BooleanArray, new_empty_array, new_null_array,
};
use arrow::compute::{filter_record_batch, prep_null_mask_filter};
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion_common::{Result, internal_err};
use datafusion_expr::{AggregateMetrics, EmitTo, GroupsAccumulator};
use datafusion_physical_expr::GroupsAccumulatorAdapter;
use datafusion_physical_expr::aggregate::AggregateFunctionExpr;
use log::debug;

use crate::PhysicalExpr;
use crate::aggregates::group_values::{
    AccumulatorPhase, AggregateAccumulatorMetrics, AggregateArgumentMetrics,
    GroupByMetrics,
};
use crate::aggregates::order::GroupOrdering;
use crate::aggregates::{
    AggregateExec, PhysicalGroupBy, aggregate_expressions, evaluate_group_by,
    group_id_array, max_duplicate_ordinal,
};

use super::AggregateTableMetrics;
use super::storage::{
    AccumulatorStorage, Emit, GroupIndices, GroupKeys, MaterializedBatch, block_size,
    materialize_batches, use_blocked_keys,
};

/// Marker for raw rows -> partial state aggregation.
pub(in crate::aggregates) struct PartialMarker;
/// Marker for raw rows -> final value aggregation.
pub(in crate::aggregates) struct SingleMarker;
/// Marker for partial state -> partial state aggregation.
pub(in crate::aggregates) struct PartialReduceMarker;
/// Marker for raw rows -> partial state conversion without aggregation.
pub(in crate::aggregates) struct PartialSkipMarker;
/// Marker for partial state -> final value aggregation.
pub(in crate::aggregates) struct FinalMarker;

/// Create an accumulator for `agg_expr` -- a [`GroupsAccumulator`] if
/// that is supported by the aggregate, or a
/// [`GroupsAccumulatorAdapter`] if not.
pub(in crate::aggregates) fn create_group_accumulator(
    agg_expr: &Arc<AggregateFunctionExpr>,
    metrics: Arc<dyn AggregateMetrics>,
) -> Result<Box<dyn GroupsAccumulator>> {
    if agg_expr.groups_accumulator_supported() {
        agg_expr.create_groups_accumulator_with_metrics(metrics)
    } else {
        // Note in the log when the slow path is used
        debug!(
            "Creating GroupsAccumulatorAdapter for {}: {agg_expr:?}",
            agg_expr.name()
        );
        let agg_expr = Arc::clone(agg_expr);
        let mut adapter =
            GroupsAccumulatorAdapter::new(move || agg_expr.create_accumulator());
        adapter.set_metrics(metrics);
        Ok(Box::new(adapter))
    }
}

/// Create the state storage for `agg_expr`: a [`BlockedGroupsAccumulator`]
/// with `block_size` groups per block if the aggregate has one, otherwise the
/// flat accumulator from [`create_group_accumulator`].
///
/// [`BlockedGroupsAccumulator`]: datafusion_expr::BlockedGroupsAccumulator
pub(super) fn create_accumulator_storage(
    agg_expr: &Arc<AggregateFunctionExpr>,
    metrics: Arc<dyn AggregateMetrics>,
    block_size: usize,
) -> Result<AccumulatorStorage> {
    if agg_expr.blocked_groups_accumulator_supported() {
        let mut accumulator = agg_expr.create_blocked_groups_accumulator(block_size)?;
        accumulator.set_metrics(metrics);
        Ok(AccumulatorStorage::Blocked(accumulator))
    } else {
        Ok(AccumulatorStorage::Flat(create_group_accumulator(
            agg_expr, metrics,
        )?))
    }
}

/// Grouped hash table shared by the partial and final paths.
///
/// While building, it consumes input batches and updates group / accumulator
/// state. While outputting, it incrementally drains that state into output
/// batches.
///
/// # Logical and Physical Model
///
/// Logically, this is a hash table that maps { group keys -> accumulator states }
/// For example, `AVG(v) GROUP BY k` stores one entry per `k`, where each
/// entry owns the `sum(v)` and `count(v)` state needed to compute the final
/// average.
///
/// Physically, the group keys and accumulators are backed by [`GroupValues`] and
/// [`GroupsAccumulator`]. Both use columnar storage so aggregation can stay
/// vectorized.
///
/// [`GroupValues`]: crate::aggregates::group_values::GroupValues
///
/// # Marker Type
/// `AggrMode` selects the aggregate semantics.
///
/// e.g. `AggregateHashTable::<PartialMarker>::new(...)` creates an aggregate hash table
/// for the partial hash aggregate stage, the input schema is raw rows and output
/// schema is intermediate states.
///
/// It is a zero-sized compile-time marker, so each stage keeps its update logic
/// in a separate impl block, to make the behavior difference explicit.
pub(in crate::aggregates) struct AggregateHashTable<AggrMode> {
    /// Grouping and accumulator-specific timing metrics.
    pub(super) group_by_metrics: GroupByMetrics,

    /// Per-aggregate timing metrics for evaluating aggregate arguments.
    pub(super) aggregate_argument_metrics: AggregateArgumentMetrics,

    /// Per-aggregate timing metrics for accumulator operations.
    pub(super) aggregate_accumulator_metrics: Arc<AggregateAccumulatorMetrics>,

    /// Optional internal metrics owned by each aggregate expression.
    pub(super) aggregate_submetrics: Vec<Arc<dyn AggregateMetrics>>,

    /// Raw input schema, used to evaluate expressions and synthesize empty
    /// grouping-set rows.
    pub(super) input_schema: SchemaRef,

    /// Output schema: group columns followed by aggregate state or final values.
    pub(super) output_schema: SchemaRef,

    /// Intermediate-state schema used when memory pressure requires the table
    /// to spill its current state.
    pub(super) state_schema: SchemaRef,

    /// Maximum rows per emitted output batch, from config `batch_size`.
    pub(super) batch_size: usize,

    /// Lifecycle-specific state: building stage / outputting stage.
    pub(super) state: AggregateHashTableState,

    pub(super) _mode: PhantomData<AggrMode>,
}

/// Methods shared by all aggregate hash table modes.
impl<AggrMode> AggregateHashTable<AggrMode> {
    pub(super) fn new_with_filters(
        agg: &AggregateExec,
        partition: usize,
        output_schema: SchemaRef,
        state_schema: SchemaRef,
        batch_size: usize,
        filters: Vec<Option<Arc<dyn PhysicalExpr>>>,
    ) -> Result<Self> {
        if batch_size == 0 {
            return internal_err!("AggregateHashTable requires config batch_size >= 1");
        }

        let input_schema = agg.input().schema();
        let metrics = AggregateTableMetrics::new(agg, partition);
        let aggregate_arguments = aggregate_expressions(
            agg.aggr_expr(),
            &agg.mode,
            agg.group_by().num_group_exprs(),
        )?;
        let group_schema = agg.group_by().group_schema(&input_schema)?;
        let block_size = block_size(batch_size);

        let accumulators: Vec<HashAggregateAccumulator> = agg
            .aggr_expr()
            .iter()
            .zip(aggregate_arguments)
            .zip(filters)
            .zip(metrics.submetrics.iter())
            .map(|(((agg_expr, arguments), filter), submetrics)| {
                let accumulator = create_accumulator_storage(
                    agg_expr,
                    Arc::clone(submetrics),
                    block_size,
                )?;
                Ok(HashAggregateAccumulator::new(
                    Arc::clone(agg_expr),
                    arguments,
                    filter,
                    accumulator,
                    Arc::clone(submetrics),
                ))
            })
            .collect::<Result<_>>()?;
        let keys = GroupKeys::try_new(
            Arc::clone(&group_schema),
            &GroupOrdering::None,
            use_blocked_keys(agg, &group_schema, block_size),
            block_size,
            &accumulators
                .iter()
                .map(|acc| acc.storage())
                .collect::<Vec<_>>(),
        )?;

        Ok(Self {
            group_by_metrics: metrics.group_by,
            aggregate_argument_metrics: metrics.aggregate_arguments,
            aggregate_accumulator_metrics: metrics.accumulator,
            aggregate_submetrics: metrics.submetrics,
            input_schema,
            output_schema,
            state_schema,
            batch_size,
            state: AggregateHashTableState::Building(AggregateHashTableBuffer {
                group_by: Arc::clone(agg.group_by()),
                keys,
                accumulators,
            }),
            _mode: PhantomData,
        })
    }

    /// See comments in [`EvaluatedAggregateBatch`]
    pub(super) fn evaluate_batch(
        &self,
        batch: &RecordBatch,
    ) -> Result<EvaluatedAggregateBatch> {
        let state = self.state.building();
        // Outer vec: one per grouping set; inner vec: group-by expressions.
        let grouping_set_args = self
            .group_by_metrics
            .time_group_key_preparation(|| evaluate_group_by(&state.group_by, batch))?;

        // The evaluated args for each accumulator.
        let accumulator_args = self.group_by_metrics.time_aggregate_arguments(|| {
            state
                .accumulators
                .iter()
                .enumerate()
                .map(|(idx, acc)| {
                    self.aggregate_argument_metrics
                        .time(idx, || acc.evaluate_compacted_args(batch))
                })
                .collect::<Result<Vec<_>>>()
        })?;

        Ok(EvaluatedAggregateBatch {
            grouping_set_args,
            accumulator_args,
        })
    }

    /// Aggregates one input batch after selecting the mode-specific accumulator
    /// operation.
    ///
    /// Each aggregation mode chooses a different `aggregate_fn` according to its
    /// semantics. For example, partial aggregation takes raw inputs, and update them
    /// into stored partial states, so [`GroupsAccumulator::update_batch`] is used.
    pub(super) fn aggregate_batch_inner(
        &mut self,
        batch: &RecordBatch,
        aggregate_fn: AggregateBatchFn,
        accumulator_phase: AccumulatorPhase,
    ) -> Result<()> {
        let evaluated_batch = self.evaluate_batch(batch)?;
        let accumulator_metrics = Arc::clone(&self.aggregate_accumulator_metrics);
        let group_by_metrics = self.group_by_metrics.clone();
        let state = self.state.building_mut();

        for group_values in &evaluated_batch.grouping_set_args {
            group_by_metrics
                .time_group_key_preparation(|| state.keys.intern(group_values))?;

            // Register groups from the full input. Each filtered aggregate compacts
            // this row-aligned vector independently immediately before its update.
            let keys = &state.keys;
            let total_num_groups = keys.len();
            group_by_metrics.time_aggregation(|| {
                for (idx, (acc, values)) in state
                    .accumulators
                    .iter_mut()
                    .zip(evaluated_batch.accumulator_args.iter())
                    .enumerate()
                {
                    let group_indices = keys.indices_for(acc.storage());
                    accumulator_metrics.time(idx, accumulator_phase, || {
                        aggregate_fn(acc, values, group_indices, total_num_groups)
                    })?;
                }
                Ok::<(), datafusion_common::DataFusionError>(())
            })?;
        }

        Ok(())
    }

    /// Materializes the full output once, then returns it downstream
    /// incrementally by slicing it into `batch_size` chunks.
    ///
    /// Flat storage materializes one batch. Blocked storage materializes one
    /// batch per block without copying, so each block's memory is released as
    /// soon as its last slice is handed out.
    ///
    /// Each aggregation mode chooses a different `materialize_accumulator_fn`
    /// according to its semantics. For example, partial aggregation emits
    /// partial states to feed the final stage, so it uses [`GroupsAccumulator::state`].
    pub(super) fn next_output_batch_inner(
        &mut self,
        materialize_accumulator_fn: MaterializeAccumulatorFn,
        accumulator_phase: AccumulatorPhase,
    ) -> Result<Option<RecordBatch>> {
        let output_schema = Arc::clone(&self.output_schema);
        let batch_size = self.batch_size;
        let accumulator_metrics = Arc::clone(&self.aggregate_accumulator_metrics);

        let mut output =
            match std::mem::replace(&mut self.state, AggregateHashTableState::Done) {
                AggregateHashTableState::Outputting(mut state) => {
                    if state.keys.is_empty() {
                        return Ok(None);
                    }

                    // Accumulator output consumes internal state. Materialize all
                    // groups once, then slice the materialized batches on later polls.
                    let batches = self.group_by_metrics.time_emitting(|| {
                        materialize_batches(
                            &mut state.keys,
                            &mut state.accumulators,
                            EmitTo::All,
                            materialize_accumulator_fn,
                            accumulator_phase,
                            &accumulator_metrics,
                            &output_schema,
                        )
                    })?;
                    debug_assert!(batches.iter().all(|b| b.batch.num_rows() > 0));
                    MaterializedAggregateOutput::new(batches)
                }
                AggregateHashTableState::OutputtingMaterialized(output) => output,
                AggregateHashTableState::Done => return Ok(None),
                AggregateHashTableState::Building(_) => {
                    return internal_err!(
                        "next_output_batch must be called in the outputting state"
                    );
                }
            };

        let batch = output.next_batch(batch_size);
        if output.is_exhausted() {
            self.state = AggregateHashTableState::Done;
        } else {
            self.state = AggregateHashTableState::OutputtingMaterialized(output);
        }
        Ok(batch)
    }

    pub(in crate::aggregates) fn memory_size(&self) -> usize {
        match &self.state {
            AggregateHashTableState::Building(state)
            | AggregateHashTableState::Outputting(state) => state.size(),
            AggregateHashTableState::OutputtingMaterialized(output) => {
                output.memory_size()
            }
            AggregateHashTableState::Done => 0,
        }
    }

    /// Returns the number of distinct groups accumulated so far.
    pub(in crate::aggregates) fn building_group_count(&self) -> usize {
        self.state.building().keys.len()
    }

    /// Takes every intermediate aggregate state and resets the table so it can
    /// continue accumulating raw input.
    ///
    /// Unlike normal single aggregation output, this materializes intermediate
    /// states rather than final values. The states can therefore be merged after
    /// spilling without finalizing the same group more than once.
    ///
    /// Returns no batch if there are no groups, one batch for flat storage,
    /// and one batch per block for blocked storage.
    pub(in crate::aggregates) fn take_state_batches(
        &mut self,
    ) -> Result<Vec<MaterializedBatch>> {
        let state_schema = Arc::clone(&self.state_schema);
        let accumulator_metrics = Arc::clone(&self.aggregate_accumulator_metrics);
        let group_by_metrics = self.group_by_metrics.clone();
        let state = self.state.building_mut();
        if state.keys.is_empty() {
            return Ok(vec![]);
        }

        let batches = group_by_metrics.time_emitting(|| {
            materialize_batches(
                &mut state.keys,
                &mut state.accumulators,
                EmitTo::All,
                HashAggregateAccumulator::state,
                AccumulatorPhase::State,
                &accumulator_metrics,
                &state_schema,
            )
        })?;
        debug_assert!(batches.iter().all(|b| b.batch.num_rows() > 0));

        // Emitting all groups resets accumulator state. Explicitly shrink the
        // key/index buffers too so the memory reservation can be released
        // before the batch is sorted for spilling.
        state.keys.clear_shrink(0);

        Ok(batches)
    }

    pub(in crate::aggregates) fn is_building(&self) -> bool {
        matches!(self.state, AggregateHashTableState::Building(_))
    }

    pub(in crate::aggregates) fn is_done(&self) -> bool {
        matches!(self.state, AggregateHashTableState::Done)
    }

    pub(super) fn start_outputting(&mut self) {
        let AggregateHashTableState::Building(mut state) =
            std::mem::replace(&mut self.state, AggregateHashTableState::Done)
        else {
            unreachable!("hash aggregate table is not building")
        };

        state.keys.free_indices();
        self.state = AggregateHashTableState::Outputting(state);
    }

    /// Creates the required empty grouping-set rows when the input is empty.
    ///
    /// For example, this query must still produce one grand-total group even if
    /// `t` has no rows:
    ///
    /// ```sql
    /// SELECT COUNT(v)
    /// FROM t
    /// GROUP BY GROUPING SETS (());
    /// ```
    ///
    /// Accumulators receive zero argument rows and zero group IDs, together with the
    /// full registered group count, so they produce the same state as empty input.
    ///
    /// Only the raw-input tables (partial and single aggregation) call this
    /// method: grouping sets are expanded while consuming raw rows, so the
    /// state-input stages (final and partial-reduce aggregation) receive the
    /// already expanded keys as plain group columns (see
    /// [`PhysicalGroupBy::as_final`]) and never own a grouping set.
    pub(super) fn init_empty_grouping_sets(&mut self) -> Result<()> {
        let group_by_metrics = self.group_by_metrics.clone();
        let state = self.state.building_mut();
        if !state.group_by.has_grouping_set() || !state.keys.is_empty() {
            return Ok(());
        }

        let accumulator_metrics = Arc::clone(&self.aggregate_accumulator_metrics);
        let any_interned = group_by_metrics.time_group_key_preparation(|| {
            let max_ordinal = max_duplicate_ordinal(state.group_by.groups());
            let mut ordinals: HashMap<&[bool], usize> = HashMap::new();
            let group_schema = state.group_by.group_schema(&self.input_schema)?;
            let n_expr = state.group_by.expr().len();
            let mut any_interned = false;

            for group in state.group_by.groups() {
                let ordinal = {
                    let entry = ordinals.entry(group.as_slice()).or_insert(0);
                    let ordinal = *entry;
                    *entry += 1;
                    ordinal
                };

                if !group.iter().all(|&is_null| is_null) {
                    continue;
                }

                let mut cols: Vec<ArrayRef> = group_schema
                    .fields()
                    .iter()
                    .take(n_expr)
                    .map(|field| new_null_array(field.data_type(), 1))
                    .collect();
                cols.push(group_id_array(group, ordinal, max_ordinal, 1)?);

                state.keys.intern(&cols)?;
                any_interned = true;
            }
            Ok::<_, datafusion_common::DataFusionError>(any_interned)
        })?;

        if any_interned {
            let total_groups = state.keys.len();
            let values = state
                .accumulators
                .iter()
                .map(|acc| {
                    Ok(CompactedAccumulatorArgs {
                        arguments: acc.null_arguments(&self.input_schema, 0)?,
                        selection: None,
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            group_by_metrics.time_aggregation(|| {
                for (idx, (acc, values)) in
                    state.accumulators.iter_mut().zip(values.iter()).enumerate()
                {
                    let no_groups = acc.storage().no_groups();
                    accumulator_metrics.time(idx, AccumulatorPhase::Update, || {
                        acc.update_batch(values, no_groups, total_groups)
                    })?;
                }
                Ok::<(), datafusion_common::DataFusionError>(())
            })?;
        }

        Ok(())
    }
}

/// State and argument information for a single Aggregate
///
/// For example, for `SELECT COUNT(x), SUM(y WHERE z > 10) ...`  there would be two
/// `HashAggregateAccumulator`, one each for `COUNT(x)` and `SUM(y WHERE z > 10)`
pub(super) struct HashAggregateAccumulator {
    /// Aggregate expression used to create a fresh accumulator for related
    /// hash tables, such as the partial-skip table.
    aggregate_expr: Arc<AggregateFunctionExpr>,

    /// Arguments to pass to this accumulator.
    ///
    /// Example: `CORR(x, y)` stores two expressions here, while `SUM(x)` stores one.
    arguments: Vec<Arc<dyn PhysicalExpr>>,

    /// Optional `FILTER` expression for this accumulator.
    ///
    /// Example: `SUM(x) FILTER (WHERE x > 10)` stores the `x > 10` predicate.
    filter: Option<Arc<dyn PhysicalExpr>>,

    /// Accumulator state for all groups for one aggregate expression.
    accumulator: AccumulatorStorage,

    /// Optional internal metrics owned by this aggregate expression.
    submetrics: Arc<dyn AggregateMetrics>,
}

pub(super) type AggregateAccumulator = HashAggregateAccumulator;

/// Function used by [`AggregateHashTable::aggregate_batch_inner`] to update one
/// accumulator with one evaluated input batch.
///
/// Arguments:
/// * accumulator to update.
/// * accumulator's compacted arguments and optional row-aligned selection.
/// * one group index per input row, mapping each row to its interned group.
/// * total number of groups currently interned in that buffer, including newly
///   interned groups.
pub(super) type AggregateBatchFn = fn(
    &mut AggregateAccumulator,
    &CompactedAccumulatorArgs,
    GroupIndices<'_>,
    usize,
) -> Result<()>;

/// Function used by [`AggregateHashTable::next_output_batch_inner`] to
/// materialize one accumulator's output columns.
///
/// Arguments:
/// * accumulator to materialize.
/// * group range to emit from the accumulator.
///
/// Returns the output columns indexed `[block][column]`; flat storage always
/// returns one block.
pub(super) type MaterializeAccumulatorFn =
    fn(&mut AggregateAccumulator, Emit) -> Result<Vec<Vec<ArrayRef>>>;

/// Aggregate arguments compacted according to one aggregate's `FILTER`.
pub(super) struct CompactedAccumulatorArgs {
    /// Argument arrays containing only selected rows. Some aggregate functions take
    /// multiple arguments.
    pub(super) arguments: Vec<ArrayRef>,
    /// Original row-aligned selection used only to compact the matching group IDs.
    pub(super) selection: Option<BooleanArray>,
}

/// Evaluated aggregate arguments that preserve one output row per input row.
pub(super) struct RowAlignedAccumulatorArgs {
    /// Row-aligned argument arrays. Rejected rows are represented as nulls.
    pub(super) arguments: Vec<ArrayRef>,
    /// Original row-aligned filter passed through to state conversion.
    pub(super) filter: Option<BooleanArray>,
}

/// Evaluated all group by keys and accumulator args.
///
/// e.g., `select k+1, sum(v*v) from t group by (k+1)`, this function evaluates
/// `k+1`, `v*v`
pub(super) struct EvaluatedAggregateBatch {
    /// One entry per grouping set; each entry contains all evaluated group key
    /// arrays for the current input batch.
    pub(super) grouping_set_args: Vec<Vec<ArrayRef>>,

    /// Compacted arguments and selections, one entry per aggregate expression.
    pub(super) accumulator_args: Vec<CompactedAccumulatorArgs>,
}

/// Buffer for the aggregate hash table's group keys and accumulator states.
///
/// It accumulates input during aggregation and emits final results during the
/// outputting stage.
///
/// [`GroupValues`] stores the physical group-key layout, while
/// [`GroupsAccumulator`] stores per-group aggregate state.
///
/// [`GroupValues`]: crate::aggregates::group_values::GroupValues
pub(super) struct AggregateHashTableBuffer {
    /// GROUP BY expressions evaluated for each input batch.
    pub(super) group_by: Arc<PhysicalGroupBy>,

    /// Interned group keys, and the group index of each row in the current
    /// input batch. Accumulator state is stored separately by group index, and
    /// the same index is used by every accumulator to update that group's
    /// aggregate state.
    pub(super) keys: GroupKeys,

    /// One item per aggregate expression.
    ///
    /// Example: `COUNT(x), SUM(y)` creates two items. Each item owns the input
    /// expressions, optional filter, and accumulator state for all groups.
    pub(super) accumulators: Vec<HashAggregateAccumulator>,
}

impl AggregateHashTableBuffer {
    /// Memory used by the group keys and every accumulator's state.
    pub(super) fn size(&self) -> usize {
        self.keys.size()
            + self
                .accumulators
                .iter()
                .map(|acc| acc.accumulator.size())
                .sum::<usize>()
    }
}

pub(super) enum AggregateHashTableState {
    /// Accumulating input rows into group keys and aggregate state.
    Building(AggregateHashTableBuffer),
    /// Emitting results directly from group keys and aggregate state.
    Outputting(AggregateHashTableBuffer),
    /// Materialize all the output results, and then incrementally output in the `OutputtingMaterialized` state.
    ///
    /// Note this is a temporary solution until the `GroupValues` issue is solved:
    /// Issue: <https://github.com/apache/datafusion/issues/23178>
    OutputtingMaterialized(MaterializedAggregateOutput),
    Done,
}

/// Fully evaluated aggregate output and the next row offset to emit.
///
/// Final aggregate evaluation consumes accumulator state, and partial terminal
/// output should not repeatedly renumber group values with `EmitTo::First`.
/// Materialize once and then slice to honor `batch_size` across output polls.
///
/// Holds one batch for flat storage and one batch per block for blocked
/// storage. A batch is dropped once all of it was handed out, so the memory of
/// each block is released block by block.
pub(super) struct MaterializedAggregateOutput {
    batches: VecDeque<MaterializedBatch>,
    /// Rows of the front batch already handed out.
    offset: usize,
}

impl MaterializedAggregateOutput {
    pub(super) fn new(batches: Vec<MaterializedBatch>) -> Self {
        Self {
            batches: batches.into(),
            offset: 0,
        }
    }

    /// Returns the next slice of at most `batch_size` rows. Never spans two
    /// batches, so the last slice of a block may be shorter.
    pub(super) fn next_batch(&mut self, batch_size: usize) -> Option<RecordBatch> {
        debug_assert!(batch_size > 0);
        let front = &self.batches.front()?.batch;
        let length = batch_size.min(front.num_rows() - self.offset);
        let batch = front.slice(self.offset, length);
        self.offset += length;
        if self.offset >= front.num_rows() {
            self.batches.pop_front();
            self.offset = 0;
        }
        Some(batch)
    }

    pub(super) fn is_exhausted(&self) -> bool {
        self.batches.is_empty()
    }

    /// Memory of the batches not fully handed out yet, see
    /// [`MaterializedBatch::memory_size`].
    pub(super) fn memory_size(&self) -> usize {
        self.batches.iter().map(|b| b.memory_size).sum()
    }
}

/// Compacts row-aligned group indices using an aggregate filter.
///
/// Returns `None` when every row is selected so callers can reuse the input
/// slice without allocating. At high selectivity, copying contiguous selected
/// ranges avoids branching once per row.
fn compact_group_indices<T: Copy>(
    group_indices: &[T],
    filter: &BooleanArray,
) -> Option<Vec<T>> {
    debug_assert_eq!(group_indices.len(), filter.len());

    let filter = match filter.null_count() {
        0 => Cow::Borrowed(filter),
        _ => Cow::Owned(prep_null_mask_filter(filter)),
    };
    let mask = filter.values();
    let selected_rows = mask.count_set_bits();

    if selected_rows == group_indices.len() {
        return None;
    }

    let mut compacted = Vec::with_capacity(selected_rows);
    // Match scatter's strategy: above 80% selectivity, copy contiguous ranges.
    if selected_rows * 5 > group_indices.len() * 4 {
        for (start, end) in mask.set_slices() {
            compacted.extend_from_slice(&group_indices[start..end]);
        }
    } else {
        compacted.extend(mask.set_indices().map(|index| group_indices[index]));
    }
    debug_assert_eq!(compacted.len(), selected_rows);

    Some(compacted)
}

impl HashAggregateAccumulator {
    pub(super) fn new(
        aggregate_expr: Arc<AggregateFunctionExpr>,
        arguments: Vec<Arc<dyn PhysicalExpr>>,
        filter: Option<Arc<dyn PhysicalExpr>>,
        accumulator: AccumulatorStorage,
        submetrics: Arc<dyn AggregateMetrics>,
    ) -> Self {
        Self {
            aggregate_expr,
            arguments,
            filter,
            accumulator,
            submetrics,
        }
    }

    /// Storage of this accumulator's state.
    pub(super) fn storage(&self) -> &AccumulatorStorage {
        &self.accumulator
    }

    /// Construct a new accumulator with the same definition, but with empty internal
    /// state buffers (empty flat [`GroupsAccumulator`]).
    pub(super) fn empty_like(&self) -> Result<Self> {
        let accumulator = AccumulatorStorage::Flat(create_group_accumulator(
            &self.aggregate_expr,
            Arc::clone(&self.submetrics),
        )?);
        Ok(Self::new(
            Arc::clone(&self.aggregate_expr),
            self.arguments.clone(),
            self.filter.clone(),
            accumulator,
            Arc::clone(&self.submetrics),
        ))
    }

    /// Evaluate aggregate arguments and filter for one input batch.
    ///
    /// For example, `AVG(2 / x) FILTER (WHERE x > 0)` evaluates `x > 0`
    /// first, then evaluates `2 / x` against a compact batch containing only
    /// selected rows. Filtered rows won't trigger errors such as divide by zero.
    ///
    /// Before updating [`GroupsAccumulator`], the retained selection is used to
    /// compact the matching group IDs and is not passed through.
    pub(super) fn evaluate_compacted_args(
        &self,
        batch: &RecordBatch,
    ) -> Result<CompactedAccumulatorArgs> {
        let selection = self.evaluate_filter(batch)?;
        let selected_rows = selection.as_ref().map(|selection| selection.true_count());
        let filtered_batch = match (selection.as_ref(), selected_rows) {
            (Some(selection), Some(selected_rows))
                if selected_rows > 0 && selected_rows < batch.num_rows() =>
            {
                Some(filter_record_batch(batch, selection)?)
            }
            _ => None,
        };
        let argument_batch = match selected_rows {
            None => Some(batch),
            Some(0) => None,
            Some(selected_rows) if selected_rows == batch.num_rows() => Some(batch),
            Some(_) => filtered_batch.as_ref(),
        };
        let arguments = self
            .arguments
            .iter()
            .map(|expr| {
                if let Some(argument_batch) = argument_batch {
                    expr.evaluate(argument_batch)
                        .and_then(|value| value.into_array(argument_batch.num_rows()))
                } else {
                    let data_type = expr.data_type(batch.schema_ref().as_ref())?;
                    Ok(new_empty_array(&data_type))
                }
            })
            .collect::<Result<_>>()?;

        Ok(CompactedAccumulatorArgs {
            arguments,
            selection,
        })
    }

    /// Evaluates selected arguments while preserving the input batch row count.
    ///
    /// Skip-partial conversion produces one state row per input row, so rejected
    /// rows remain as null argument values and the filter is passed to
    /// [`GroupsAccumulator::convert_to_state`].
    pub(super) fn evaluate_row_aligned_args(
        &self,
        batch: &RecordBatch,
    ) -> Result<RowAlignedAccumulatorArgs> {
        let filter = self.evaluate_filter(batch)?;
        let selection = filter.as_ref();
        let arguments = self
            .arguments
            .iter()
            .map(|expr| {
                selection
                    .map_or_else(
                        || expr.evaluate(batch),
                        |selection| expr.evaluate_selection(batch, selection),
                    )
                    .and_then(|value| value.into_array(batch.num_rows()))
            })
            .collect::<Result<_>>()?;

        Ok(RowAlignedAccumulatorArgs { arguments, filter })
    }

    fn evaluate_filter(&self, batch: &RecordBatch) -> Result<Option<BooleanArray>> {
        self.filter
            .as_ref()
            .map(|filter| {
                filter
                    .evaluate(batch)
                    .and_then(|value| value.into_array(batch.num_rows()))
                    .map(|filter| filter.as_boolean().clone())
            })
            .transpose()
    }

    pub(super) fn size(&self) -> usize {
        self.accumulator.size()
    }

    pub(super) fn update_batch(
        &mut self,
        values: &CompactedAccumulatorArgs,
        group_indices: GroupIndices<'_>,
        total_num_groups: usize,
    ) -> Result<()> {
        match (&mut self.accumulator, group_indices) {
            (AccumulatorStorage::Flat(acc), GroupIndices::Flat(group_indices)) => {
                let filtered_group_indices =
                    values.selection.as_ref().and_then(|selection| {
                        compact_group_indices(group_indices, selection)
                    });
                let group_indices =
                    filtered_group_indices.as_deref().unwrap_or(group_indices);
                acc.update_batch(&values.arguments, group_indices, None, total_num_groups)
            }
            (AccumulatorStorage::Blocked(acc), GroupIndices::Blocked(group_indices)) => {
                let filtered_group_indices =
                    values.selection.as_ref().and_then(|selection| {
                        compact_group_indices(group_indices, selection)
                    });
                let group_indices =
                    filtered_group_indices.as_deref().unwrap_or(group_indices);
                acc.update_batch(&values.arguments, group_indices, None, total_num_groups)
            }
            _ => mismatched_storage(),
        }
    }

    pub(super) fn merge_batch(
        &mut self,
        values: &CompactedAccumulatorArgs,
        group_indices: GroupIndices<'_>,
        total_num_groups: usize,
    ) -> Result<()> {
        debug_assert!(values.selection.is_none());
        match (&mut self.accumulator, group_indices) {
            (AccumulatorStorage::Flat(acc), GroupIndices::Flat(group_indices)) => {
                acc.merge_batch(&values.arguments, group_indices, total_num_groups)
            }
            (AccumulatorStorage::Blocked(acc), GroupIndices::Blocked(group_indices)) => {
                acc.merge_batch(&values.arguments, group_indices, total_num_groups)
            }
            _ => mismatched_storage(),
        }
    }

    /// Evaluating final aggregate results according to `emit`, and reset inner
    /// states. (e.g. after `evaluate(EmitTo::All)`, it returns all accumulated groups
    /// , and clear the inner buffers)
    ///
    /// Returns one final-value column per block.
    pub(super) fn evaluate_to_columns(
        &mut self,
        emit: Emit,
    ) -> Result<Vec<Vec<ArrayRef>>> {
        match (&mut self.accumulator, emit) {
            (AccumulatorStorage::Flat(acc), Emit::Flat(emit_to)) => {
                Ok(vec![vec![acc.evaluate(emit_to)?]])
            }
            (AccumulatorStorage::Blocked(acc), Emit::Blocked(emit_to)) => Ok(acc
                .evaluate(emit_to)?
                .into_iter()
                .map(|array| vec![array])
                .collect()),
            _ => mismatched_storage(),
        }
    }

    /// Evaluating partial aggregate results according to `emit`, and reset inner
    /// states. (e.g. after `state(EmitTo::All)`, it returns all accumulated groups
    /// , and clear the inner buffers)
    ///
    /// Returns the state columns indexed `[block][column]`.
    pub(super) fn state(&mut self, emit: Emit) -> Result<Vec<Vec<ArrayRef>>> {
        match (&mut self.accumulator, emit) {
            (AccumulatorStorage::Flat(acc), Emit::Flat(emit_to)) => {
                Ok(vec![acc.state(emit_to)?])
            }
            (AccumulatorStorage::Blocked(acc), Emit::Blocked(emit_to)) => {
                acc.state(emit_to)
            }
            _ => mismatched_storage(),
        }
    }

    /// Converts evaluated row-aligned arguments directly to partial state.
    pub(super) fn convert_to_state(
        &self,
        values: &RowAlignedAccumulatorArgs,
    ) -> Result<Vec<ArrayRef>> {
        match &self.accumulator {
            AccumulatorStorage::Flat(acc) => {
                acc.convert_to_state(&values.arguments, values.filter.as_ref())
            }
            AccumulatorStorage::Blocked(acc) => {
                acc.convert_to_state(&values.arguments, values.filter.as_ref())
            }
        }
    }

    pub(super) fn null_arguments(
        &self,
        input_schema: &SchemaRef,
        num_rows: usize,
    ) -> Result<Vec<ArrayRef>> {
        self.arguments
            .iter()
            .map(|expr| {
                let data_type = expr.data_type(input_schema)?;
                Ok(new_null_array(&data_type, num_rows))
            })
            .collect()
    }
}

/// Group keys and accumulators are always created with the same storage, so a
/// mismatch is a bug.
fn mismatched_storage<T>() -> Result<T> {
    internal_err!("accumulator storage does not match the group key storage")
}

impl AggregateHashTableState {
    pub(super) fn building(&self) -> &AggregateHashTableBuffer {
        let Self::Building(state) = self else {
            unreachable!("hash aggregate table is not building")
        };
        state
    }

    pub(super) fn building_mut(&mut self) -> &mut AggregateHashTableBuffer {
        let Self::Building(state) = self else {
            unreachable!("hash aggregate table is not building")
        };
        state
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Array, BooleanArray, Int32Array, Int64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_functions_aggregate::sum::sum_udaf;
    use datafusion_physical_expr::aggregate::AggregateExprBuilder;
    use datafusion_physical_expr::expressions::Column;

    use super::*;
    use crate::aggregates::group_values::aggregate_sub_metrics;
    use crate::metrics::ExecutionPlanMetricsSet;

    #[test]
    fn compact_group_indices_uses_filter_bitmap() {
        let group_indices = (0..10).collect::<Vec<_>>();
        let all_true = BooleanArray::from(vec![true; 10]);
        assert_eq!(compact_group_indices(&group_indices, &all_true), None);

        let high_selectivity =
            BooleanArray::from((0..10).map(|index| index != 4).collect::<Vec<_>>());
        assert_eq!(
            compact_group_indices(&group_indices, &high_selectivity),
            Some(vec![0, 1, 2, 3, 5, 6, 7, 8, 9])
        );

        let with_nulls =
            BooleanArray::from(vec![Some(true), None, Some(false), Some(true), None]);
        assert_eq!(
            compact_group_indices(&group_indices[..5], &with_nulls),
            Some(vec![0, 3])
        );
    }

    #[test]
    fn materialized_aggregate_output_slices_batches_until_exhausted() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "group_col",
            DataType::Int32,
            false,
        )]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5]))],
        )?;
        let memory_size = batch.get_array_memory_size();
        let mut output = MaterializedAggregateOutput::new(vec![MaterializedBatch {
            batch,
            memory_size,
        }]);

        assert_eq!(int32_values(&output.next_batch(2).unwrap(), 0), vec![1, 2]);
        assert_eq!(int32_values(&output.next_batch(2).unwrap(), 0), vec![3, 4]);
        assert_eq!(int32_values(&output.next_batch(2).unwrap(), 0), vec![5]);
        assert!(output.next_batch(2).is_none());
        assert!(output.is_exhausted());

        Ok(())
    }

    #[test]
    fn convert_to_state_preserves_rows_and_metrics() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("value", DataType::Int64, false),
            Field::new("include", DataType::Boolean, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(vec![10, 20, 30, 40])),
                Arc::new(BooleanArray::from(vec![true, false, true, false])),
            ],
        )?;
        let metrics = ExecutionPlanMetricsSet::new();
        let submetrics = aggregate_sub_metrics(&metrics, 0, ["SUM(value)"])
            .pop()
            .expect("one aggregate submetric factory");
        let accumulator = sum_accumulator(&schema, "include", 1, submetrics)?;
        let group_by_metrics = GroupByMetrics::new(&metrics, 0);
        let argument_metrics = AggregateArgumentMetrics::new(&metrics, 0, ["SUM(value)"]);
        let accumulator_metrics = AggregateAccumulatorMetrics::new(
            &metrics,
            0,
            ["SUM(value)"],
            &[AccumulatorPhase::ConvertToState],
        );

        let values = group_by_metrics.time_aggregate_arguments(|| {
            argument_metrics.time(0, || accumulator.evaluate_row_aligned_args(&batch))
        })?;
        let state =
            accumulator_metrics.time(0, AccumulatorPhase::ConvertToState, || {
                accumulator.convert_to_state(&values)
            })?;

        assert_eq!(
            int64_options(&state[0]),
            vec![Some(10), None, Some(30), None]
        );
        let metrics = metrics.clone_inner();
        for metric_name in [
            "aggregate_arguments_time",
            "agg_expr_0_arguments_time",
            "agg_expr_0_convert_to_state_time",
        ] {
            assert!(
                metrics
                    .sum_by_name(metric_name)
                    .is_some_and(|time| { time.as_usize() > 0 })
            );
        }

        Ok(())
    }

    fn sum_accumulator(
        schema: &SchemaRef,
        filter_name: &str,
        filter_index: usize,
        submetrics: Arc<dyn AggregateMetrics>,
    ) -> Result<HashAggregateAccumulator> {
        let argument: Arc<dyn PhysicalExpr> = Arc::new(Column::new("value", 0));
        let aggregate_expr = Arc::new(
            AggregateExprBuilder::new(sum_udaf(), vec![Arc::clone(&argument)])
                .schema(Arc::clone(schema))
                .alias("SUM(value)")
                .build()?,
        );
        let accumulator = AccumulatorStorage::Flat(create_group_accumulator(
            &aggregate_expr,
            Arc::clone(&submetrics),
        )?);
        Ok(HashAggregateAccumulator::new(
            aggregate_expr,
            vec![argument],
            Some(Arc::new(Column::new(filter_name, filter_index))),
            accumulator,
            submetrics,
        ))
    }

    fn int64_options(array: &ArrayRef) -> Vec<Option<i64>> {
        array
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .iter()
            .collect()
    }

    fn int32_values(batch: &RecordBatch, column: usize) -> Vec<i32> {
        let array = batch
            .column(column)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        (0..array.len()).map(|idx| array.value(idx)).collect()
    }
}
