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

//! Common utilities for aggregate tables used in aggregations that inputs are ordered
//! by the groups.

use std::marker::PhantomData;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion_common::{arrow_datafusion_err, Result, DataFusionError};
use datafusion_common::assert_or_internal_err;
use datafusion_execution::memory_pool::proxy::VecAllocExt;
use datafusion_expr::EmitTo;
use datafusion_expr_common::groups_accumulator::{BlockedEmitTo, BlocksIndex};
use crate::InputOrderMode;
use crate::PhysicalExpr;
use crate::aggregates_blocked::group_values::{
    AccumulatorPhase, AggregateAccumulatorMetrics, AggregateArgumentMetrics,
    GroupByMetrics, BlockedGroupValues, new_group_values,
};
use crate::aggregates_blocked::grouped_hash_stream::create_group_accumulator;
use crate::aggregates_blocked::order::GroupOrdering;
use crate::aggregates_blocked::{BlockedAggregateExec, AggregateMode, PhysicalGroupBy, aggregate_expressions, evaluate_group_by, create_blocked_group_accumulator};

use super::AggregateTableMetrics;
use super::common::{
    AggregateAccumulator, AggregateBatchFn, AggregateHashTable, EvaluatedAggregateBatch,
    MaterializeAccumulatorFn,
};

#[derive(Clone)]
pub(in crate::aggregates_blocked) struct OrderedAggregateTableMetrics {
    pub(super) group_by: GroupByMetrics,
    pub(super) aggregate_arguments: AggregateArgumentMetrics,
    pub(super) accumulator: Arc<AggregateAccumulatorMetrics>,
}

impl OrderedAggregateTableMetrics {
    pub(in crate::aggregates_blocked) fn new(agg: &BlockedAggregateExec, partition: usize) -> Self {
        let metrics = AggregateTableMetrics::new(agg, partition);
        Self {
            group_by: metrics.group_by,
            aggregate_arguments: metrics.aggregate_arguments,
            accumulator: metrics.accumulator,
        }
    }

    pub(in crate::aggregates_blocked) fn from_hash_table<AggrMode>(
        table: &AggregateHashTable<AggrMode>,
    ) -> Self {
        Self {
            group_by: table.group_by_metrics.clone(),
            aggregate_arguments: table.aggregate_argument_metrics.clone(),
            accumulator: Arc::clone(&table.aggregate_accumulator_metrics),
        }
    }
}

/// Aggregate table shared by the ordered single, partial and final paths.
///
/// # Ordering optimization
///
/// The table consumes input batches while `GroupOrdering` tracks which groups
/// are proven complete. Completed groups can be emitted before the input stream
/// ends, which keeps memory bounded by the active ordered key range.
///
/// # Single, partial and final variant difference
///
/// The partial and final aggregate tables implement the two stages of grouped
/// aggregation, while the single aggregate table implements both stages in one
/// table. See
/// [`OrderedPartialAggregateStream`](crate::aggregates_blocked::ordered_partial_stream::OrderedPartialAggregateStream)
/// for the high-level plan shape.
///
/// Example: `AVG(v) FILTER (WHERE v>0) GROUP BY k`
///
/// Partial table ([`AggregateMode::Partial`], with optional filter from query):
/// - Input rows: `k, v`
/// - Table stores: `k, sum(v), count(v)`
/// - Output schema: `k, sum(v), count(v)`
///
/// Final table ([`AggregateMode::Final`], no filters):
/// - Input rows: `k, sum(v), count(v)`
/// - Table stores: `k, sum(v), count(v)`
/// - Output schema: `k, avg(v)`
///
/// Single table ([`AggregateMode::Single`], with optional filter from query):
/// - Input rows: `k, v`
/// - Table stores: `k, sum(v), count(v)`
/// - Output schema: `k, avg(v)`
///
/// # Marker Type
///
/// `OrderedAggrMode` selects the aggregate semantics. For example,
/// `OrderedAggregateTable::<PartialMarker>::new(...)` consumes raw rows
/// and emits partial states, while
/// `OrderedAggregateTable::<FinalMarker>::new_with_input_order(...)`
/// consumes partial states and emits final values.
///
/// Shared methods live on `impl<T>`; single/partial/final behavior lives on
/// marker-specific impls.
pub(in crate::aggregates_blocked) struct OrderedAggregateTable<OrderedAggrMode> {
    /// Output schema: group columns followed by aggregate state or final values.
    pub(super) output_schema: SchemaRef,

    /// Intermediate-state schema used when memory pressure requires the table
    /// to pass through or spill its current state.
    pub(super) state_schema: SchemaRef,

    /// Maximum rows per emitted output batch, from config `batch_size`.
    pub(super) batch_size: usize,

    /// Grouping and accumulator-specific timing metrics.
    pub(super) group_by_metrics: GroupByMetrics,

    /// Per-aggregate timing metrics for evaluating aggregate arguments.
    pub(super) aggregate_argument_metrics: AggregateArgumentMetrics,

    /// Per-aggregate timing metrics for accumulator operations.
    pub(super) aggregate_accumulator_metrics: Arc<AggregateAccumulatorMetrics>,

    /// Group keys, ordering state, and accumulator states.
    pub(super) buffer: OrderedAggregateTableBuffer,

    _mode: PhantomData<OrderedAggrMode>,
}

/// Buffer for the ordered aggregate table's group keys and accumulator states.
///
/// It accumulates input during aggregation and emits output rows as soon as the
/// input ordering proves those groups are complete.
///
/// [`GroupOrdering`] tracks when and how to do early emit.
/// [`BlockedGroupValues`] stores the physical group-key layout, while
/// [`datafusion_expr::GroupsAccumulator`] stores per-group aggregate state.
pub(super) struct OrderedAggregateTableBuffer {
    /// GROUP BY expressions evaluated against input batches.
    pub(super) group_by: Arc<PhysicalGroupBy>,

    /// Tracks how far ordered input allows this table to drain safely.
    pub(super) group_ordering: GroupOrdering,

    /// Interned group keys, in the same group-id order used by accumulators.
    pub(super) group_values: Box<dyn BlockedGroupValues>,

    /// Scratch group id vector for the current input batch.
    pub(super) group_indices: Vec<BlocksIndex>,

    /// One item per aggregate expression.
    ///
    /// Example: `COUNT(x), SUM(y)` creates two items. Each item owns the input
    /// expressions, optional filter, and accumulator state for all groups.
    pub(super) accumulators: Vec<AggregateAccumulator>,
}

/// Methods shared by all aggregate modes
impl<AggrMode> OrderedAggregateTable<AggrMode> {
    #[expect(
        clippy::too_many_arguments,
        reason = "keeps ordered single, partial and final table construction explicit"
    )]
    pub(super) fn new_for_mode(
      agg: &BlockedAggregateExec,
      input_schema: &SchemaRef,
      output_schema: SchemaRef,
      state_schema: SchemaRef,
      batch_size: usize,
      input_order_mode: &InputOrderMode,
      aggregate_mode: &AggregateMode,
      filters: Vec<Option<Arc<dyn PhysicalExpr>>>,
      metrics: OrderedAggregateTableMetrics,
    ) -> Result<Self> {
        assert_or_internal_err!(
            batch_size > 0,
            "OrderedAggregateTable requires config batch_size >= 1"
        );

        let group_ordering = GroupOrdering::try_new(input_order_mode, batch_size)?;
        let group_schema = agg.group_by.group_schema(input_schema)?;
        let group_values = new_group_values(group_schema, &group_ordering, batch_size)?;
        let aggregate_arguments = aggregate_expressions(
            &agg.aggr_expr,
            aggregate_mode,
            agg.group_by.num_group_exprs(),
        )?;
        let accumulators = agg
            .aggr_expr
            .iter()
            .zip(aggregate_arguments)
            .zip(filters)
            .map(|((agg_expr, arguments), filter)| {
                let accumulator = create_blocked_group_accumulator(agg_expr, batch_size)?;
                Ok(AggregateAccumulator::new(
                    Arc::clone(agg_expr),
                    arguments,
                    filter,
                    accumulator,
                ))
            })
            .collect::<Result<_>>()?;

        Ok(Self {
            output_schema,
            state_schema,
            batch_size,
            group_by_metrics: metrics.group_by,
            aggregate_argument_metrics: metrics.aggregate_arguments,
            aggregate_accumulator_metrics: metrics.accumulator,
            buffer: OrderedAggregateTableBuffer {
                group_by: Arc::clone(&agg.group_by),
                group_ordering,
                group_values,
                group_indices: vec![],
                accumulators,
            },
            _mode: PhantomData,
        })
    }

    /// Evaluates all group by keys and accumulator args.
    ///
    /// e.g., `select k+1, sum(v*v) from t group by (k+1)`, this function
    /// evaluates `k+1`, `v*v`.
    pub(super) fn evaluate_batch(
        &self,
        batch: &RecordBatch,
    ) -> Result<EvaluatedAggregateBatch> {
        let timer = self.group_by_metrics.time_calculating_group_ids.timer();
        let grouping_set_args = evaluate_group_by(&self.buffer.group_by, batch)?;
        drop(timer);

        let timer = self.group_by_metrics.aggregate_arguments_time.timer();
        let accumulator_args = self
            .buffer
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

    /// Called after the input stream is exhausted and the last batch has been
    /// aggregated.
    ///
    /// Updates the internal `GroupOrdering` so it can continue emitting until
    /// the buffer is empty.
    pub(in crate::aggregates_blocked) fn input_done(&mut self) {
        self.buffer.group_ordering.input_done();
    }

    /// Returns the ordering state used to decide how memory pressure is handled.
    pub(in crate::aggregates_blocked) fn group_ordering(&self) -> &GroupOrdering {
        &self.buffer.group_ordering
    }

    /// Number of groups currently buffered.
    pub(in crate::aggregates_blocked) fn num_groups(&self) -> usize {
        self.buffer.group_values.len()
    }

    /// Check if there is zero groups accumulated so far.
    pub(in crate::aggregates_blocked) fn is_empty(&self) -> bool {
        self.num_groups() == 0
    }

    /// All internal buffer's memory size.
    pub(in crate::aggregates_blocked) fn memory_size(&self) -> usize {
        self.buffer
            .accumulators
            .iter()
            .map(|acc| acc.size())
            .sum::<usize>()
            + self.buffer.group_values.size()
            + self.buffer.group_ordering.size()
            + self.buffer.group_indices.allocated_size()
    }

    pub(in crate::aggregates_blocked) fn metrics(&self) -> OrderedAggregateTableMetrics {
        OrderedAggregateTableMetrics {
            group_by: self.group_by_metrics.clone(),
            aggregate_arguments: self.aggregate_argument_metrics.clone(),
            accumulator: Arc::clone(&self.aggregate_accumulator_metrics),
        }
    }

    /// Takes every intermediate aggregate state and resets the table so it can
    /// continue with a new ordered input segment.
    ///
    /// Unlike normal ordered emission, this operation is allowed to take the
    /// active (incomplete) groups. Partial aggregation can pass those states to
    /// its final stage, while single and final aggregation sort and spill them
    /// before replay.
    pub(in crate::aggregates_blocked) fn take_next_state_batch(
        &mut self,
    ) -> Result<Option<RecordBatch>> {
        let state_schema = Arc::clone(&self.state_schema);
        let accumulator_metrics = Arc::clone(&self.aggregate_accumulator_metrics);
        if self.buffer.group_values.is_empty() {

            // `emit(EmitTo::All)` resets accumulator state. Explicitly shrink the
            // key/index buffers too so the memory reservation can be released
            // before the batch is passed downstream or sorted for spilling.
            self.buffer.group_values.clear_shrink(0);
            self.buffer.group_indices.clear();
            self.buffer.group_indices.shrink_to_fit();
            self.buffer.group_ordering.reset();

            return Ok(None);
        }

        // Accumulator output consumes internal state. Materialize all
        // groups once, then slice the materialized batch on later polls.
        let timer = self.group_by_metrics.emitting_time.timer();
        let emit_to = if self.buffer.group_values.len() <= self.batch_size {
            BlockedEmitTo::All
        } else {
            BlockedEmitTo::NextBlock
        };
        let mut output = self.buffer.group_values.emit_block()?.expect("must have groups since checked before that len is not empty");

        for (idx, acc) in self.buffer.accumulators.iter_mut().enumerate() {
            output.extend(accumulator_metrics.time(
                idx,
                AccumulatorPhase::State,
                || {
                    let state = acc.state(emit_to)?;

                    assert_eq!(state.len(), 1);

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
            // before the batch is passed downstream or sorted for spilling.
            self.buffer.group_values.clear_shrink(0);
            self.buffer.group_indices.clear();
            self.buffer.group_indices.shrink_to_fit();
            self.buffer.group_ordering.reset();
        }

        Ok(Some(batch))
    }

    /// Returns the [`EmitTo`], clamped to the specified batch size
    ///
    /// Returns `(emit_to, should_remove_groups)`, where `emit_to` is the number
    /// of groups to emit from `GroupValues` / accumulators, and
    /// `should_remove_groups` indicates whether `GroupOrdering` must also shift
    /// its tracked indexes.
    pub(super) fn clamp_emit_to(
        &self,
        group_count: usize,
        emit_to: EmitTo,
    ) -> (BlockedEmitTo, bool) {
        match emit_to {
            EmitTo::First(n) if n < self.batch_size => (BlockedEmitTo::First(n), true),
            EmitTo::First(_) => (BlockedEmitTo::NextBlock, true),
            EmitTo::All if group_count <= self.batch_size => (BlockedEmitTo::All, false),
            EmitTo::All => (BlockedEmitTo::NextBlock, !self.group_ordering().is_done()),
        }
    }

    /// Aggregates one evaluated input batch after selecting the mode-specific
    /// accumulator operation.
    ///
    /// Each aggregation mode chooses a different `aggregate_fn` according to its
    /// semantics. For example, partial aggregation takes raw inputs and updates
    /// stored partial states, so it uses
    /// [`datafusion_expr::GroupsAccumulator::update_batch`].
    pub(super) fn aggregate_evaluated_batch(
        &mut self,
        evaluated_batch: &EvaluatedAggregateBatch,
        aggregate_fn: AggregateBatchFn,
        accumulator_phase: AccumulatorPhase,
    ) -> Result<()> {
        let accumulator_metrics = Arc::clone(&self.aggregate_accumulator_metrics);
        for group_values in &evaluated_batch.grouping_set_args {
            let starting_num_groups = self.buffer.group_values.len();

            self.buffer
              .group_values
              .intern(group_values, &mut self.buffer.group_indices)?;
            let total_num_groups = self.buffer.group_values.len();
            if total_num_groups > starting_num_groups {
                self.buffer.group_ordering.new_groups(
                    group_values,
                    &self.buffer.group_indices,
                    total_num_groups,
                )?;
            }

            let timer = self.group_by_metrics.aggregation_time.timer();
            for (idx, (acc, values)) in self
                .buffer
                .accumulators
                .iter_mut()
                .zip(evaluated_batch.accumulator_args.iter())
                .enumerate()
            {
                accumulator_metrics.time(idx, accumulator_phase, || {
                    aggregate_fn(
                        acc,
                        values,
                        &self.buffer.group_indices,
                        total_num_groups,
                    )
                })?;
            }
            drop(timer);
        }

        Ok(())
    }

    /// Emits groups allowed by `GroupOrdering`, leaving only the current
    /// unfinished ordered-key range buffered.
    ///
    /// Each aggregation mode chooses a different `materialize_accumulator_fn`
    /// according to its semantics. For example, partial aggregation emits
    /// partial states to feed the final stage, so it uses
    /// [`datafusion_expr::GroupsAccumulator::state`].
    pub(super) fn next_output_batch_inner(
        &mut self,
        materialize_accumulator_fn: MaterializeAccumulatorFn,
        accumulator_phase: AccumulatorPhase,
    ) -> Result<Option<RecordBatch>> {
        if self.buffer.group_values.is_empty() {
            return Ok(None);
        }

        let Some(emit_to) = self.buffer.group_ordering.next_emit_to(self.buffer.group_values.len()) else {
            return Ok(None);
        };
        let should_remove_groups = !self.group_ordering().is_done() && emit_to != BlockedEmitTo::All;

        let accumulator_metrics = Arc::clone(&self.aggregate_accumulator_metrics);
        let timer = self.group_by_metrics.emitting_time.timer();
        let output = self.buffer.group_values.emit(emit_to)?;
        assert_eq!(output.len(), 1);
        let mut output = output.into_iter().next().unwrap();
        if should_remove_groups {
            match emit_to {
                BlockedEmitTo::First(n) => self.buffer.group_ordering.remove_groups(n),
                BlockedEmitTo::NextBlock => self.buffer.group_ordering.remove_groups(self.batch_size),

                // `BlockedEmitTo::All` is only used after `input_done`, when all
                // buffered groups are known complete and the ordering state is
                // no longer needed.
                BlockedEmitTo::All => {}
            }
        }

        for (idx, acc) in self.buffer.accumulators.iter_mut().enumerate() {
            output.extend(accumulator_metrics.time(idx, accumulator_phase, || {
                let output = materialize_accumulator_fn(acc, emit_to)?;

                assert_eq!(output.len(), 1, "must always get single block {emit_to:?}");

                Ok::<_, DataFusionError>(output.into_iter().next().unwrap())
            })?);
        }
        drop(timer);

        let batch = RecordBatch::try_new(Arc::clone(&self.output_schema), output).map_err(|e| {
            arrow_datafusion_err!(e)
        })?;
        debug_assert!(batch.num_rows() > 0);

        Ok(Some(batch))
    }
}
