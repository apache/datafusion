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
use datafusion_common::Result;
use datafusion_common::assert_or_internal_err;
use datafusion_execution::memory_pool::proxy::VecAllocExt;
use datafusion_expr::EmitTo;

use crate::InputOrderMode;
use crate::PhysicalExpr;
use crate::aggregates::group_values::{GroupByMetrics, GroupValues, new_group_values};
use crate::aggregates::grouped_hash_stream::create_group_accumulator;
use crate::aggregates::order::GroupOrdering;
use crate::aggregates::{
    AggregateExec, AggregateMode, GroupHashTracker, PhysicalGroupBy,
    aggregate_expressions, evaluate_group_by, outputs_group_hashes,
};
use crate::repartition::{ExpressionHasher, HashMetrics};

use super::common::{AggregateAccumulator, EvaluatedAggregateBatch};

/// Aggregate table shared by the ordered partial and final paths.
///
/// # Ordering optimization
///
/// The table consumes input batches while `GroupOrdering` tracks which groups
/// are proven complete. Completed groups can be emitted before the input stream
/// ends, which keeps memory bounded by the active ordered key range.
///
/// # Partial and final variant difference
///
/// The partial and final aggregate tables implement the two stages of grouped
/// aggregation. See
/// [`OrderedPartialAggregateStream`](crate::aggregates::ordered_partial_stream::OrderedPartialAggregateStream)
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
/// # Marker Type
///
/// `OrderedAggrMode` selects the aggregate semantics. For example,
/// `OrderedAggregateTable::<PartialMarker>::new(...)` consumes raw rows
/// and emits partial states, while
/// `OrderedAggregateTable::<FinalMarker>::new_with_input_order(...)`
/// consumes partial states and emits final values.
///
/// Shared methods live on `impl<T>`; partial/final behavior lives on
/// marker-specific impls.
pub(in crate::aggregates) struct OrderedAggregateTable<OrderedAggrMode> {
    /// Output schema: group columns followed by aggregate state or final values.
    pub(super) output_schema: SchemaRef,

    /// Maximum rows per emitted output batch, from config `batch_size`.
    pub(super) batch_size: usize,

    /// Grouping and accumulator-specific timing metrics.
    pub(super) group_by_metrics: GroupByMetrics,

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
/// [`GroupValues`] stores the physical group-key layout, while
/// [`datafusion_expr::GroupsAccumulator`] stores per-group aggregate state.
pub(super) struct OrderedAggregateTableBuffer {
    /// GROUP BY expressions evaluated against input batches.
    pub(super) group_by: Arc<PhysicalGroupBy>,

    /// Tracks how far ordered input allows this table to drain safely.
    pub(super) group_ordering: GroupOrdering,

    /// Interned group keys, in the same group-id order used by accumulators.
    pub(super) group_values: Box<dyn GroupValues>,

    /// Scratch group id vector for the current input batch.
    pub(super) group_indices: Vec<usize>,

    /// Hashes retained for the internal hash column when output requires it.
    pub(super) group_hash_tracker: Option<GroupHashTracker>,

    /// Computes group hashes when the input has no reusable hash column.
    pub(super) hasher: ExpressionHasher,

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
        reason = "keeps ordered partial and final table construction explicit"
    )]
    pub(super) fn new_for_mode(
        agg: &AggregateExec,
        partition: usize,
        input_schema: &SchemaRef,
        output_schema: SchemaRef,
        batch_size: usize,
        input_order_mode: &InputOrderMode,
        aggregate_mode: &AggregateMode,
        filters: Vec<Option<Arc<dyn PhysicalExpr>>>,
    ) -> Result<Self> {
        assert_or_internal_err!(
            batch_size > 0,
            "OrderedAggregateTable requires config batch_size >= 1"
        );

        let group_ordering = GroupOrdering::try_new(input_order_mode)?;
        let group_schema = agg.group_by.group_schema(input_schema)?;
        let group_values = new_group_values(group_schema, &group_ordering)?;
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
                let accumulator = create_group_accumulator(agg_expr)?;
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
            batch_size,
            group_by_metrics: GroupByMetrics::new(&agg.metrics, partition),
            buffer: OrderedAggregateTableBuffer {
                group_by: Arc::clone(&agg.group_by),
                group_ordering,
                group_values,
                group_indices: vec![],
                group_hash_tracker: outputs_group_hashes(*aggregate_mode, &agg.group_by)
                    .then(GroupHashTracker::default),
                hasher: ExpressionHasher::new_with_metrics(
                    agg.group_by.input_exprs(),
                    HashMetrics::new(&agg.metrics, partition),
                ),
                accumulators,
            },
            _mode: PhantomData,
        })
    }

    /// Evaluates all group by keys and accumulator args.
    ///
    /// e.g., `select k+1, sum(v*v) from t group by (k+1)`, this function
    /// evaluates `k+1`, `v*v`.
    pub(super) fn evaluate_batch<'a>(
        &self,
        batch: &'a RecordBatch,
    ) -> Result<EvaluatedAggregateBatch<'a>> {
        let timer = self.group_by_metrics.time_calculating_group_ids.timer();
        let grouping_set_args = evaluate_group_by(&self.buffer.group_by, batch)?;
        drop(timer);

        let timer = self.group_by_metrics.aggregate_arguments_time.timer();
        let accumulator_args = self
            .buffer
            .accumulators
            .iter()
            .map(|acc| acc.evaluate_acc_args(batch))
            .collect::<Result<Vec<_>>>()?;
        drop(timer);

        Ok(EvaluatedAggregateBatch {
            grouping_set_args,
            accumulator_args,
            precomputed_group_hashes: self
                .buffer
                .hasher
                .precomputed_group_by(&self.buffer.group_by, batch),
        })
    }

    /// Called after the input stream is exhausted and the last batch has been
    /// aggregated.
    ///
    /// Updates the internal `GroupOrdering` so it can continue emitting until
    /// the buffer is empty.
    pub(in crate::aggregates) fn input_done(&mut self) {
        self.buffer.group_ordering.input_done();
    }

    /// Check if there is zero groups accumulated so far.
    pub(in crate::aggregates) fn is_empty(&self) -> bool {
        self.buffer.group_values.is_empty()
    }

    /// All internal buffer's memory size.
    pub(in crate::aggregates) fn memory_size(&self) -> usize {
        self.buffer
            .accumulators
            .iter()
            .map(|acc| acc.size())
            .sum::<usize>()
            + self.buffer.group_values.size()
            + self.buffer.group_ordering.size()
            + self.buffer.group_indices.allocated_size()
            + self
                .buffer
                .group_hash_tracker
                .as_ref()
                .map_or(0, |v| v.allocated_size())
            + self.buffer.hasher.allocated_size()
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
    ) -> (EmitTo, bool) {
        match emit_to {
            EmitTo::First(n) => (EmitTo::First(n.min(self.batch_size)), true),
            EmitTo::All if group_count <= self.batch_size => (EmitTo::All, false),
            EmitTo::All => (EmitTo::First(self.batch_size), false),
        }
    }
    /// Aggregates one evaluated input batch.
    ///
    /// This common utility is used by ordered partial and ordered final aggregation.
    ///
    /// # Argument: `is_final`
    ///
    /// - `true`: merge partial aggregate states for final aggregation.
    /// - `false`: update aggregate states from raw input for partial aggregation.
    pub(super) fn aggregate_evaluated_batch(
        &mut self,
        evaluated_batch: &EvaluatedAggregateBatch<'_>,
        is_final: bool,
    ) -> Result<()> {
        for group_values in &evaluated_batch.grouping_set_args {
            let starting_num_groups = self.buffer.group_values.len();
            let hashes = match evaluated_batch.precomputed_group_hashes {
                Some(hashes) => hashes,
                None => self.buffer.hasher.compute_hashes(group_values)?,
            };
            self.buffer.group_values.intern(
                group_values,
                &mut self.buffer.group_indices,
                hashes,
            )?;
            let total_num_groups = self.buffer.group_values.len();
            if let Some(group_hash_tracker) = &mut self.buffer.group_hash_tracker {
                group_hash_tracker.record_new_groups(
                    starting_num_groups,
                    total_num_groups,
                    &self.buffer.group_indices,
                    hashes,
                );
            }
            if total_num_groups > starting_num_groups {
                self.buffer.group_ordering.new_groups(
                    group_values,
                    &self.buffer.group_indices,
                    total_num_groups,
                )?;
            }

            let timer = self.group_by_metrics.aggregation_time.timer();
            for (acc, values) in self
                .buffer
                .accumulators
                .iter_mut()
                .zip(evaluated_batch.accumulator_args.iter())
            {
                if is_final {
                    acc.merge_batch(
                        values,
                        &self.buffer.group_indices,
                        total_num_groups,
                    )?;
                } else {
                    acc.update_batch(
                        values,
                        &self.buffer.group_indices,
                        total_num_groups,
                    )?;
                }
            }
            drop(timer);
        }

        Ok(())
    }

    /// Emits groups allowed by `GroupOrdering`, leaving only the current
    /// unfinished ordered-key range buffered.
    ///
    /// This common utility is used by ordered partial and ordered final aggregation.
    ///
    /// # Argument: `is_final`
    ///
    /// - `true`: output final aggregate values.
    /// - `false`: output partial accumulator states.
    pub(super) fn next_output_batch_for_mode(
        &mut self,
        is_final: bool,
    ) -> Result<Option<RecordBatch>> {
        if self.buffer.group_values.is_empty() {
            return Ok(None);
        }

        let Some(emit_to) = self.buffer.group_ordering.emit_to() else {
            return Ok(None);
        };
        let (emit_to, should_remove_groups) =
            self.clamp_emit_to(self.buffer.group_values.len(), emit_to);

        let timer = self.group_by_metrics.emitting_time.timer();
        let mut output = self.buffer.group_values.emit(emit_to)?;
        if should_remove_groups {
            match emit_to {
                EmitTo::First(n) => self.buffer.group_ordering.remove_groups(n),
                // `EmitTo::All` is only used after `input_done`, when all
                // buffered groups are known complete and the ordering state is
                // no longer needed.
                EmitTo::All => {}
            }
        }

        for acc in &mut self.buffer.accumulators {
            if is_final {
                output.push(acc.evaluate(emit_to)?);
            } else {
                output.extend(acc.state(emit_to)?);
            }
        }
        if let Some(group_hash_tracker) = &mut self.buffer.group_hash_tracker {
            output.push(group_hash_tracker.emit(emit_to))
        }
        drop(timer);

        let batch = RecordBatch::try_new(Arc::clone(&self.output_schema), output)?;
        debug_assert!(batch.num_rows() > 0);

        Ok(Some(batch))
    }
}
