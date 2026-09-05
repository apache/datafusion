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

use std::marker::PhantomData;
use std::sync::Arc;

use arrow::array::{ArrayRef, AsArray, new_null_array};
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use itertools::Itertools;
use datafusion_common::{internal_datafusion_err, internal_err, Result, arrow_datafusion_err, DataFusionError};
use datafusion_execution::memory_pool::proxy::VecAllocExt;
use datafusion_expr::{EmitTo, GroupsAccumulator};
use datafusion_expr_common::groups_accumulator::{BlockedEmitTo, BlockedGroupsAccumulator, BlocksIndex};
use datafusion_physical_expr::aggregate::AggregateFunctionExpr;
use crate::PhysicalExpr;
use crate::aggregates_blocked::group_values::{
    AccumulatorPhase, AggregateAccumulatorMetrics, AggregateArgumentMetrics,
    GroupByMetrics, BlockedGroupValues, new_group_values,
};
use crate::aggregates_blocked::grouped_hash_stream::create_group_accumulator;
use crate::aggregates_blocked::order::GroupOrdering;
use crate::aggregates_blocked::{aggregate_expressions, create_blocked_group_accumulator, evaluate_group_by, BlockedAggregateExec, PhysicalGroupBy};

use super::AggregateTableMetrics;

/// Marker for raw rows -> partial state aggregation.
pub(in crate::aggregates_blocked) struct PartialMarker;
/// Marker for raw rows -> final value aggregation.
pub(in crate::aggregates_blocked) struct SingleMarker;
/// Marker for partial state -> partial state aggregation.
pub(in crate::aggregates_blocked) struct PartialReduceMarker;
/// Marker for raw rows -> partial state conversion without aggregation.
pub(in crate::aggregates_blocked) struct PartialSkipMarker;
/// Marker for partial state -> final value aggregation.
pub(in crate::aggregates_blocked) struct FinalMarker;

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
/// Physically, the group keys and accumulators are backed by [`BlockedGroupValues`] and
/// [`GroupsAccumulator`]. Both use columnar storage so aggregation can stay
/// vectorized.
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
pub(in crate::aggregates_blocked) struct AggregateHashTable<AggrMode> {
    /// Grouping and accumulator-specific timing metrics.
    pub(super) group_by_metrics: GroupByMetrics,

    /// Per-aggregate timing metrics for evaluating aggregate arguments.
    pub(super) aggregate_argument_metrics: AggregateArgumentMetrics,

    /// Per-aggregate timing metrics for accumulator operations.
    pub(super) aggregate_accumulator_metrics: Arc<AggregateAccumulatorMetrics>,

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
        agg: &BlockedAggregateExec,
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
        let aggregate_arguments = aggregate_expressions(
            &agg.aggr_expr,
            &agg.mode,
            agg.group_by.num_group_exprs(),
        )?;
        let accumulators: Vec<_> = agg
            .aggr_expr
            .iter()
            .zip(aggregate_arguments)
            .zip(filters)
            .map(|((agg_expr, arguments), filter)| {
                let accumulator = create_blocked_group_accumulator(agg_expr, batch_size)?;
                Ok(HashAggregateAccumulator::new(
                    Arc::clone(agg_expr),
                    arguments,
                    filter,
                    accumulator,
                ))
            })
            .collect::<Result<_>>()?;

        let group_schema = agg.group_by.group_schema(&input_schema)?;
        let group_values = new_group_values(group_schema, &GroupOrdering::None, batch_size)?;

        let metrics = AggregateTableMetrics::new(agg, partition);

        Ok(Self {
            group_by_metrics: metrics.group_by,
            aggregate_argument_metrics: metrics.aggregate_arguments,
            aggregate_accumulator_metrics: metrics.accumulator,
            input_schema,
            output_schema,
            state_schema,
            batch_size,
            state: AggregateHashTableState::Building(AggregateHashTableBuffer {
                group_by: Arc::clone(&agg.group_by),
                group_values,
                batch_group_indices: Default::default(),
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
        let timer = self.group_by_metrics.time_calculating_group_ids.timer();
        // outer vec: one per each grouping set
        // inner vec: all group by exprs for the current grouping set
        let grouping_set_args = evaluate_group_by(&state.group_by, batch)?;
        drop(timer);

        let timer = self.group_by_metrics.aggregate_arguments_time.timer();
        // The evaluated args for each accumulator
        let accumulator_args = state
            .accumulators
            .iter()
            .enumerate()
            .map(|(idx, acc)| {
                self.aggregate_argument_metrics
                    .time(idx, || acc.evaluate_acc_args(batch))
            })
            .collect::<Result<Vec<_>>>()?;
        drop(timer);

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
        let state = self.state.building_mut();

        let _timer = self.group_by_metrics.aggregation_time.timer();
        for group_values in &evaluated_batch.grouping_set_args {
            state
              .group_values
              .intern(group_values, &mut state.batch_group_indices)?;

            let group_indices = &state.batch_group_indices;
            let total_num_groups = state.group_values.len();

            for (idx, (acc, values)) in state
                .accumulators
                .iter_mut()
                .zip(evaluated_batch.accumulator_args.iter())
                .enumerate()
            {
                accumulator_metrics.time(idx, accumulator_phase, || {
                    aggregate_fn(acc, values, group_indices, total_num_groups)
                })?;
            }
        }

        Ok(())
    }

    /// Materializes the full output once, then returns it downstream incrementally
    /// by slicing it into `batch_size` chunks.
    ///
    /// Each aggregation mode chooses a different `materialize_accumulator_fn`
    /// according to its semantics. For example, partial aggregation emits
    /// partial states to feed the final stage, so it uses [`GroupsAccumulator::state`].
    ///
    /// This is a temporary solution until blocked state management is implemented:
    /// Issue: <https://github.com/apache/datafusion/issues/7065>
    pub(super) fn next_output_batch_inner(
        &mut self,
        materialize_accumulator_fn: MaterializeAccumulatorFn,
        accumulator_phase: AccumulatorPhase,
    ) -> Result<Option<RecordBatch>> {
        let output_schema = Arc::clone(&self.output_schema);
        let accumulator_metrics = Arc::clone(&self.aggregate_accumulator_metrics);

        let (mut output_reversed, size) =
            match std::mem::replace(&mut self.state, AggregateHashTableState::Done) {
                AggregateHashTableState::Outputting(mut state) => {
                    if state.group_values.is_empty() {
                        return Ok(None);
                    }

                    // Accumulator output consumes internal state. Materialize all
                    // groups once, then slice the materialized batch on later polls.
                    let _timer = self.group_by_metrics.emitting_time.timer();
                    let mut columns_blocked = state.group_values.emit_all()?;

                    for (idx, acc) in state.accumulators.iter_mut().enumerate() {
                        let blocks = accumulator_metrics.time(
                            idx,
                            accumulator_phase,
                            || materialize_accumulator_fn(acc, BlockedEmitTo::All)
                        )?;

                        columns_blocked.iter_mut().zip_eq(blocks.into_iter()).for_each(|(column_blocked, materialized)| {
                            column_blocked.extend(materialized)
                        });
                    }

                    let mut size = 0;

                    let mut batches = columns_blocked.into_iter().map(|columns| {
                        let batch = RecordBatch::try_new(Arc::clone(&output_schema), columns).map_err(|e| {
                            arrow_datafusion_err!(e)
                        })?;
                        debug_assert!(batch.num_rows() > 0);

                        size += batch.get_array_memory_size();

                        Ok(batch)
                    }).collect::<Result<Vec<_>>>()?;


                    // Reverse so we can take from the start without shifting
                    batches.reverse();


                    (batches, size)
                }
                AggregateHashTableState::OutputtingMaterialized(output_reversed, size) => (output_reversed, size),
                AggregateHashTableState::Done => return Ok(None),
                AggregateHashTableState::Building(_) => {
                    return internal_err!(
                        "next_output_batch must be called in the outputting state"
                    );
                }
            };

        let batch = output_reversed.pop();
        if output_reversed.is_empty() {
            self.state = AggregateHashTableState::Done;
        } else {
            self.state = AggregateHashTableState::OutputtingMaterialized(output_reversed, size - batch.as_ref().map_or(0, |batch| batch.get_array_memory_size()));
        }
        Ok(batch)
    }

    pub(in crate::aggregates_blocked) fn memory_size(&self) -> usize {
        match &self.state {
            AggregateHashTableState::Building(state)
            | AggregateHashTableState::Outputting(state) => {
                let acc = state
                    .accumulators
                    .iter()
                    .map(|acc| acc.accumulator.size())
                    .sum::<usize>();

                acc + state.group_values.size()
                    + state.batch_group_indices.allocated_size()
            }
            AggregateHashTableState::OutputtingMaterialized(_, memory_size) => {
                *memory_size
            }
            AggregateHashTableState::Done => 0,
        }
    }

    /// Returns the number of distinct groups accumulated so far.
    pub(in crate::aggregates_blocked) fn building_group_count(&self) -> usize {
        self.state.building().group_values.len()
    }

    /// Takes every intermediate aggregate state and resets the table so it can
    /// continue accumulating raw input.
    ///
    /// Unlike normal single aggregation output, this materializes intermediate
    /// states rather than final values. The states can therefore be merged after
    /// spilling without finalizing the same group more than once.
    pub(in crate::aggregates_blocked) fn take_next_state_batch(
        &mut self,
    ) -> Result<Option<RecordBatch>> {
        let state_schema = Arc::clone(&self.state_schema);
        let accumulator_metrics = Arc::clone(&self.aggregate_accumulator_metrics);
        let state = self.state.building_mut();
        if state.group_values.is_empty() {

            // `emit(EmitTo::All)` resets accumulator state. Explicitly shrink the
            // key/index buffers too so the memory reservation can be released
            // before the batch is sorted for spilling.
            state.group_values.clear_shrink(0);
            state.batch_group_indices.clear();
            state.batch_group_indices.shrink_to_fit();

            return Ok(None);
        }

        // Accumulator output consumes internal state. Materialize all
        // groups once, then slice the materialized batch on later polls.
        let timer = self.group_by_metrics.emitting_time.timer();
        let emit_to = if state.group_values.len() <= self.batch_size {
            BlockedEmitTo::All
        } else {
            BlockedEmitTo::NextBlock
        };
        let mut output = state.group_values.emit_block()?.expect("must have groups since checked before that len is not empty");

        for (idx, acc) in state.accumulators.iter_mut().enumerate() {
            output.extend(accumulator_metrics.time(
                idx,
                AccumulatorPhase::State,
                || {
                    let state = acc.state(emit_to)?;

                    assert_eq!(state.len(), 1, "must have 1 block");

                    Ok::<_, DataFusionError>(state.into_iter().next().unwrap())
                },
            )?);
        }

        drop(timer);

        let batch = RecordBatch::try_new(state_schema, output).map_err(|e| {
            arrow_datafusion_err!(e)
        })?;
        debug_assert!(batch.num_rows() > 0);

        if emit_to == BlockedEmitTo::All {
            // `emit(EmitTo::All)` resets accumulator state. Explicitly shrink the
            // key/index buffers too so the memory reservation can be released
            // before the batch is sorted for spilling.
            state.group_values.clear_shrink(0);
            state.batch_group_indices.clear();
            state.batch_group_indices.shrink_to_fit();
        }

        Ok(Some(batch))
    }

    pub(in crate::aggregates_blocked) fn is_building(&self) -> bool {
        matches!(self.state, AggregateHashTableState::Building(_))
    }

    pub(in crate::aggregates_blocked) fn is_done(&self) -> bool {
        matches!(self.state, AggregateHashTableState::Done)
    }

    pub(super) fn start_outputting(&mut self) {
        let AggregateHashTableState::Building(mut state) =
            std::mem::replace(&mut self.state, AggregateHashTableState::Done)
        else {
            unreachable!("hash aggregate table is not building")
        };

        state.batch_group_indices = Vec::new();
        self.state = AggregateHashTableState::Outputting(state);
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
    accumulator: Box<dyn BlockedGroupsAccumulator>,
}

pub(super) type AggregateAccumulator = HashAggregateAccumulator;

/// Function used by [`AggregateHashTable::aggregate_batch_inner`] to update one
/// accumulator with one evaluated input batch.
///
/// Arguments:
/// * accumulator to update.
/// * accumulator's evaluated arguments and optional filter.
/// * one group index per input row, mapping each row to its interned group.
/// * total number of groups currently interned in that buffer, including newly
///   interned groups.
pub(super) type AggregateBatchFn = fn(
    &mut AggregateAccumulator,
    &EvaluatedAccumulatorArgs,
    &[BlocksIndex],
    usize,
) -> Result<()>;

/// Function used by [`AggregateHashTable::next_output_batch_inner`] to
/// materialize one accumulator's output columns.
///
/// Arguments:
/// * accumulator to materialize.
/// * group range to emit from the accumulator.
pub(super) type MaterializeAccumulatorFn =
    fn(&mut AggregateAccumulator, BlockedEmitTo) -> Result<Vec<Vec<ArrayRef>>>;

/// Evaluated aggregate arguments and filter for one input batch.
///
/// For example, `AVG(x + 1) FILTER (WHERE x > 0)` evaluates both `x + 1`
/// and `x > 0`.
///
/// These arrays can be passed directly to [`GroupsAccumulator`].
pub(super) struct EvaluatedAccumulatorArgs {
    /// Evaluated argument arrays. Some aggregate functions take multiple arguments.
    pub(super) arguments: Vec<ArrayRef>,
    /// Evaluated filter array, `Some` if the aggregate has a `FILTER` expression.
    pub(super) filter: Option<ArrayRef>,
}

/// Evaluated all group by keys and accumulator args.
///
/// e.g., `select k+1, sum(v*v) from t group by (k+1)`, this function evaluates
/// `k+1`, `v*v`
pub(super) struct EvaluatedAggregateBatch {
    /// One entry per grouping set; each entry contains all evaluated group key
    /// arrays for the current input batch.
    pub(super) grouping_set_args: Vec<Vec<ArrayRef>>,

    /// Evaluated arguments and filters, one entry per aggregate expression.
    pub(super) accumulator_args: Vec<EvaluatedAccumulatorArgs>,
}

/// Buffer for the aggregate hash table's group keys and accumulator states.
///
/// It accumulates input during aggregation and emits final results during the
/// outputting stage.
///
/// [`BlockedGroupValues`] stores the physical group-key layout, while
/// [`GroupsAccumulator`] stores per-group aggregate state.
pub(super) struct AggregateHashTableBuffer {
    /// GROUP BY expressions evaluated for each input batch.
    pub(super) group_by: Arc<PhysicalGroupBy>,

    /// Interned group keys. Accumulator state is stored separately by group index.
    pub(super) group_values: Box<dyn BlockedGroupValues>,

    /// Group index for each row in the current input batch.
    ///
    /// Each value indexes into `group_values`, and the same index is used by every
    /// accumulator to update that group's aggregate state.
    pub(super) batch_group_indices: Vec<BlocksIndex>,

    /// One item per aggregate expression.
    ///
    /// Example: `COUNT(x), SUM(y)` creates two items. Each item owns the input
    /// expressions, optional filter, and accumulator state for all groups.
    pub(super) accumulators: Vec<HashAggregateAccumulator>,
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
    ///
    /// Saving the output reversed so we can do pop without needing to shift
    OutputtingMaterialized(Vec<RecordBatch>, usize),
    Done,
}

/// Fully evaluated aggregate output and the next row offset to emit.
///
/// Final aggregate evaluation consumes accumulator state, and partial terminal
/// output should not repeatedly renumber group values with `EmitTo::First`.
/// Materialize once and then slice to honor `batch_size` across output polls.
pub(super) struct MaterializedAggregateOutput {
    batch: RecordBatch,
    offset: usize,
}

impl MaterializedAggregateOutput {
    pub(super) fn new(batch: RecordBatch) -> Self {
        Self { batch, offset: 0 }
    }

    pub(super) fn next_batch(&mut self, batch_size: usize) -> Option<RecordBatch> {
        debug_assert!(batch_size > 0);
        if self.is_exhausted() {
            return None;
        }

        let length = batch_size.min(self.batch.num_rows() - self.offset);
        let batch = self.batch.slice(self.offset, length);
        self.offset += length;
        Some(batch)
    }

    pub(super) fn is_exhausted(&self) -> bool {
        self.offset >= self.batch.num_rows()
    }

    pub(super) fn memory_size(&self) -> usize {
        self.batch.get_array_memory_size()
    }
}

impl HashAggregateAccumulator {
    pub(super) fn new(
        aggregate_expr: Arc<AggregateFunctionExpr>,
        arguments: Vec<Arc<dyn PhysicalExpr>>,
        filter: Option<Arc<dyn PhysicalExpr>>,
        accumulator: Box<dyn BlockedGroupsAccumulator>,
    ) -> Self {
        if let Some(batch_size) = aggregate_expr.batch_size() {
            assert_eq!(batch_size, accumulator.batch_size());
        }
        Self {
            aggregate_expr,
            arguments,
            filter,
            accumulator,
        }
    }

    /// Construct a new accumulator with the same definition, but with empty internal
    /// state buffers (empty [`GroupsAccumulator`]).
    pub(super) fn empty_like(&self) -> Result<Self> {
        let batch_size = self.accumulator.batch_size();

        let accumulator = create_blocked_group_accumulator(&self.aggregate_expr, batch_size)?;
        Ok(Self::new(
            Arc::clone(&self.aggregate_expr),
            self.arguments.clone(),
            self.filter.clone(),
            accumulator,
        ))
    }

    /// Evaluate aggregate arguments and filter for one input batch.
    ///
    /// For example, `AVG(2 / x) FILTER (WHERE x > 0)` evaluates `x > 0`
    /// first, then evaluates `2 / x` only for selected rows.
    /// Filtered rows will be evaluated to `NULL`, and won't trigger errors
    /// such as divide by zero.
    ///
    /// These arrays can be passed directly to [`GroupsAccumulator`] next.
    pub(super) fn evaluate_acc_args(
        &self,
        batch: &RecordBatch,
    ) -> Result<EvaluatedAccumulatorArgs> {
        let filter = self
            .filter
            .as_ref()
            .map(|filter| {
                filter
                    .evaluate(batch)
                    .and_then(|value| value.into_array(batch.num_rows()))
            })
            .transpose()?;
        let selection = filter.as_ref().map(|filter| filter.as_boolean());
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

        Ok(EvaluatedAccumulatorArgs { arguments, filter })
    }

    pub(super) fn size(&self) -> usize {
        self.accumulator.size()
    }

    pub(super) fn update_batch(
        &mut self,
        values: &EvaluatedAccumulatorArgs,
        group_indices: &[BlocksIndex],
        total_num_groups: usize,
    ) -> Result<()> {
        let filter = values.filter.as_ref().map(|filter| filter.as_boolean());
        self.accumulator.update_batch(
            &values.arguments,
            group_indices,
            filter,
            total_num_groups,
        )
    }

    pub(super) fn merge_batch(
        &mut self,
        values: &EvaluatedAccumulatorArgs,
        group_indices: &[BlocksIndex],
        total_num_groups: usize,
    ) -> Result<()> {
        debug_assert!(values.filter.is_none());
        self.accumulator
            .merge_batch(&values.arguments, group_indices, total_num_groups)
    }

    /// Evaluating final aggregate results according to `EmitTo`, and reset inner
    /// states. (e.g. after `evaluate(EmitTo::All)`, it returns all accumulated groups
    /// , and clear the inner buffers)
    pub(super) fn evaluate(&mut self, emit_to: BlockedEmitTo) -> Result<Vec<ArrayRef>> {
        self.accumulator.evaluate(emit_to)
    }

    pub(super) fn evaluate_to_columns(
        &mut self,
        emit_to: BlockedEmitTo,
    ) -> Result<Vec<Vec<ArrayRef>>> {
        let blocks = self.evaluate(emit_to)?;
        let blocks_in_columns = blocks.into_iter().map(|block| vec![block]).collect::<Vec<_>>();
        Ok(blocks_in_columns)
    }

    /// Evaluating partial aggregate results according to `EmitTo`, and reset inner
    /// states. (e.g. after `state(EmitTo::All)`, it returns all accumulated groups
    /// , and clear the inner buffers)
    pub(super) fn state(&mut self, emit_to: BlockedEmitTo) -> Result<Vec<Vec<ArrayRef>>> {
        self.accumulator.state(emit_to)
    }

    pub(super) fn convert_to_state(
        &mut self,
        values: &EvaluatedAccumulatorArgs,
    ) -> Result<Vec<ArrayRef>> {
        let opt_filter = values.filter.as_ref().map(|filter| filter.as_boolean());
        self.accumulator
            .convert_to_state(&values.arguments, opt_filter)
    }

    pub(super) fn null_arguments(
        &self,
        input_schema: &SchemaRef,
    ) -> Result<Vec<ArrayRef>> {
        self.arguments
            .iter()
            .map(|expr| {
                let data_type = expr.data_type(input_schema)?;
                Ok(new_null_array(&data_type, 1))
            })
            .collect()
    }
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

    use arrow::array::{Array, Int32Array};
    use arrow::datatypes::{DataType, Field, Schema};

    use super::*;

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
        let mut output = MaterializedAggregateOutput::new(batch);

        assert_eq!(int32_values(&output.next_batch(2).unwrap(), 0), vec![1, 2]);
        assert_eq!(int32_values(&output.next_batch(2).unwrap(), 0), vec![3, 4]);
        assert_eq!(int32_values(&output.next_batch(2).unwrap(), 0), vec![5]);
        assert!(output.next_batch(2).is_none());
        assert!(output.is_exhausted());

        Ok(())
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
