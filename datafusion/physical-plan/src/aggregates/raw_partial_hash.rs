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

//! Grouped hash aggregation for the first aggregation stage.
//!
//! This stream handles the basic grouped `raw rows -> partial state` path:
//!
//! ```text
//! input rows -> GROUP BY hash table -> accumulator state rows
//! ```
//!
//! `AggregateExec` keeps finite-memory, ordered, limit, grouping-set, later-stage
//! (`partial state -> final values`, `partial state -> partial state`) and
//! single-stage aggregation on `GroupedHashAggregateStream` for now.

use std::collections::HashMap;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow::array::{ArrayRef, AsArray, BooleanArray, new_null_array};
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion_common::Result;
use datafusion_execution::TaskContext;
use datafusion_execution::memory_pool::proxy::VecAllocExt;
use datafusion_execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion_expr::{EmitTo, GroupsAccumulator};
use futures::ready;
use futures::stream::{Stream, StreamExt};

use super::group_values::{GroupValues, new_unordered_group_values};
use super::row_hash::create_group_accumulator;
use super::{
    AggregateExec, PhysicalGroupBy, aggregate_expressions, evaluate_group_by,
    group_id_array, max_duplicate_ordinal,
};
use crate::metrics::{
    BaselineMetrics, MetricBuilder, MetricCategory, RecordOutput, SpillMetrics,
};
use crate::stream::EmptyRecordBatchStream;
use crate::{InputOrderMode, PhysicalExpr, RecordBatchStream, SendableRecordBatchStream};

#[derive(Debug, Clone)]
enum ExecutionState {
    ReadingInput,
    ProducingOutput,
    Done,
}

struct RawPartialAccumulator {
    /// Arguments to pass to this accumulator.
    ///
    /// Example: `CORR(x, y)` stores two expressions here, while `SUM(x)` stores one.
    arguments: Vec<Arc<dyn PhysicalExpr>>,

    /// Optional `FILTER` expression for this accumulator.
    ///
    /// Example: `SUM(x) FILTER (WHERE x > 10)` stores the `x > 10` predicate.
    filter: Option<Arc<dyn PhysicalExpr>>,

    /// Accumulator state for all groups for one aggregate expression.
    accumulator: Box<dyn GroupsAccumulator>,
}

struct EvaluatedRawPartialAccumulator {
    arguments: Vec<ArrayRef>,
    filter: Option<ArrayRef>,
}

impl RawPartialAccumulator {
    fn new(
        arguments: Vec<Arc<dyn PhysicalExpr>>,
        filter: Option<Arc<dyn PhysicalExpr>>,
        accumulator: Box<dyn GroupsAccumulator>,
    ) -> Self {
        Self {
            arguments,
            filter,
            accumulator,
        }
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<EvaluatedRawPartialAccumulator> {
        let arguments = self
            .arguments
            .iter()
            .map(|expr| {
                expr.evaluate(batch)
                    .and_then(|value| value.into_array(batch.num_rows()))
            })
            .collect::<Result<_>>()?;

        let filter = self
            .filter
            .as_ref()
            .map(|filter| {
                filter
                    .evaluate(batch)
                    .and_then(|value| value.into_array(batch.num_rows()))
            })
            .transpose()?;

        Ok(EvaluatedRawPartialAccumulator { arguments, filter })
    }

    fn update_batch(
        &mut self,
        values: &EvaluatedRawPartialAccumulator,
        group_indices: &[usize],
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

    fn state(&mut self, emit_to: EmitTo) -> Result<Vec<ArrayRef>> {
        self.accumulator.state(emit_to)
    }

    fn supports_convert_to_state(&self) -> bool {
        self.accumulator.supports_convert_to_state()
    }

    fn null_arguments(&self, input_schema: &SchemaRef) -> Result<Vec<ArrayRef>> {
        self.arguments
            .iter()
            .map(|expr| {
                let data_type = expr.data_type(input_schema)?;
                Ok(new_null_array(&data_type, 1))
            })
            .collect()
    }
}

/// Hash table state for grouped raw-partial aggregation.
///
/// This owns the coupled state for:
/// - evaluating group keys,
/// - interning each distinct group,
/// - mapping each input row to its group index,
/// - evaluating aggregate inputs,
/// - updating per-group accumulator state.
struct AggregateHashTable {
    /// Raw input schema, used to evaluate expressions and synthesize empty
    /// grouping-set rows.
    input_schema: SchemaRef,

    /// Output schema: group columns followed by partial aggregate state columns.
    output_schema: SchemaRef,

    /// Maximum rows per emitted output batch.
    batch_size: usize,

    /// GROUP BY expressions evaluated for each input batch.
    group_by: Arc<PhysicalGroupBy>,

    /// Interned group keys. Accumulator state is stored separately by group index.
    group_values: Box<dyn GroupValues>,

    /// Group index for each row in the current input batch.
    ///
    /// Each value indexes into `group_values`, and the same index is used by every
    /// accumulator to update that group's aggregate state.
    batch_group_indices: Vec<usize>,

    /// One item per aggregate expression.
    ///
    /// Example: `COUNT(x), SUM(y)` creates two items. Each item owns the input
    /// expressions, optional filter, and accumulator state for all groups.
    accumulators: Vec<RawPartialAccumulator>,

    /// Full partial-state output built once after input is exhausted.
    output_batch: Option<RecordBatch>,

    /// Offset of the next row to slice from `output_batch`.
    output_batch_offset: usize,

    /// True once all output rows have been emitted.
    output_finished: bool,
}

impl AggregateHashTable {
    fn new(
        agg: &AggregateExec,
        partition: usize,
        output_schema: SchemaRef,
        batch_size: usize,
    ) -> Result<Self> {
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
            .zip(agg.filter_expr.iter().cloned())
            .map(|((agg_expr, arguments), filter)| {
                let accumulator = create_group_accumulator(agg_expr)?;
                Ok(RawPartialAccumulator::new(arguments, filter, accumulator))
            })
            .collect::<Result<_>>()?;

        if accumulators
            .iter()
            .all(|acc| acc.supports_convert_to_state())
        {
            let _skipped_aggregation_rows = MetricBuilder::new(&agg.metrics)
                .with_category(MetricCategory::Rows)
                .counter("skipped_aggregation_rows", partition);
        }

        let group_schema = agg.group_by.group_schema(&input_schema)?;
        let group_values = new_unordered_group_values(group_schema)?;

        Ok(Self {
            input_schema,
            output_schema,
            batch_size,
            group_by: Arc::clone(&agg.group_by),
            group_values,
            batch_group_indices: Default::default(),
            accumulators,
            output_batch: None,
            output_batch_offset: 0,
            output_finished: false,
        })
    }

    fn aggregate_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        // outer vec: one per each grouping set
        // inner vec: all group by exprs for the current grouping set
        let grouping_set_args = evaluate_group_by(&self.group_by, batch)?;
        // The evaluated args for each accumulator
        let accumulator_args = self
            .accumulators
            .iter()
            .map(|acc| acc.evaluate(batch))
            .collect::<Result<Vec<_>>>()?;

        for group_values in &grouping_set_args {
            self.group_values
                .intern(group_values, &mut self.batch_group_indices)?;
            let group_indices = &self.batch_group_indices;
            let total_num_groups = self.group_values.len();

            for (acc, values) in self.accumulators.iter_mut().zip(accumulator_args.iter())
            {
                acc.update_batch(values, group_indices, total_num_groups)?;
            }
        }

        Ok(())
    }

    fn next_output_batch(&mut self) -> Result<Option<RecordBatch>> {
        if self.output_finished {
            return Ok(None);
        }

        if self.output_batch.is_none() {
            self.output_batch = self.build_output_batch()?;
            self.output_batch_offset = 0;
        }

        let Some(batch) = self.output_batch.as_ref() else {
            self.output_finished = true;
            return Ok(None);
        };

        debug_assert!(self.batch_size > 0);
        let output_len = self
            .batch_size
            .max(1)
            .min(batch.num_rows() - self.output_batch_offset);
        let output = batch.slice(self.output_batch_offset, output_len);
        self.output_batch_offset += output_len;

        if self.output_batch_offset == batch.num_rows() {
            self.output_batch = None;
            self.output_batch_offset = 0;
            self.output_finished = true;
        }

        debug_assert!(output.num_rows() > 0);
        debug_assert!(output.num_rows() <= self.batch_size.max(1));
        Ok(Some(output))
    }

    fn build_output_batch(&mut self) -> Result<Option<RecordBatch>> {
        self.init_empty_grouping_sets()?;

        if self.group_values.is_empty() {
            return Ok(None);
        }

        let mut output = self.group_values.emit(EmitTo::All)?;

        for acc in self.accumulators.iter_mut() {
            output.extend(acc.state(EmitTo::All)?);
        }

        let batch = RecordBatch::try_new(Arc::clone(&self.output_schema), output)?;
        debug_assert!(batch.num_rows() > 0);
        Ok(Some(batch))
    }

    fn memory_size(&self) -> usize {
        let acc = self
            .accumulators
            .iter()
            .map(|acc| acc.accumulator.size())
            .sum::<usize>();
        let output = self
            .output_batch
            .as_ref()
            .map(RecordBatch::get_array_memory_size)
            .unwrap_or_default();

        acc + self.group_values.size()
            + self.batch_group_indices.allocated_size()
            + output
    }

    fn clear(&mut self) {
        self.group_values.clear_shrink(0);
        self.batch_group_indices.clear();
        self.batch_group_indices.shrink_to(0);
        self.output_batch = None;
        self.output_batch_offset = 0;
        self.output_finished = false;
    }

    fn init_empty_grouping_sets(&mut self) -> Result<()> {
        if !self.group_by.has_grouping_set() || !self.group_values.is_empty() {
            return Ok(());
        }

        let max_ordinal = max_duplicate_ordinal(self.group_by.groups());
        let mut ordinals: HashMap<&[bool], usize> = HashMap::new();
        let group_schema = self.group_by.group_schema(&self.input_schema)?;
        let n_expr = self.group_by.expr().len();
        let mut any_interned = false;

        for group in self.group_by.groups() {
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

            self.group_values
                .intern(&cols, &mut self.batch_group_indices)?;
            any_interned = true;
        }

        if any_interned {
            let total_groups = self.group_values.len();
            let false_filter = BooleanArray::from(vec![false]);
            for acc in self.accumulators.iter_mut() {
                let null_args = acc.null_arguments(&self.input_schema)?;
                let values = EvaluatedRawPartialAccumulator {
                    arguments: null_args,
                    filter: Some(Arc::new(false_filter.clone())),
                };
                acc.update_batch(&values, &[0], total_groups)?;
            }
        }

        Ok(())
    }
}

/// Hash aggregate stream for grouped `AggregateMode::Partial`.
/// 
/// Input: raw rows
/// Output: partial state (e.g. for avg(x), it's sum(x), count(x))
pub(crate) struct RawPartialHashAggregateStream {
    // ========================================================================
    // PROPERTIES:
    // Initialized once for this input partition.
    // ========================================================================
    /// Output schema: group columns followed by partial aggregate state columns.
    schema: SchemaRef,

    /// Input batches containing raw rows, not partial aggregate state.
    input: SendableRecordBatchStream,

    // ========================================================================
    // STATE FLAGS:
    // Control whether the stream is reading input, emitting state, or done.
    // ========================================================================
    exec_state: ExecutionState,

    // ========================================================================
    // STATE BUFFERS:
    //
    // Hold intermediate groups and aggregate state while reading input.
    // Example: `SELECT z, COUNT(x), SUM(y) FROM t GROUP BY z` stores each distinct
    // `z` in `group_values` and keeps one partial-state accumulator for `COUNT(x)`
    // and one for `SUM(y)`.
    // ========================================================================
    /// Hash table and accumulator state for all groups seen so far.
    hash_table: AggregateHashTable,

    // ========================================================================
    // EXECUTION RESOURCES:
    // Metrics and memory accounting for this stream.
    // ========================================================================
    /// Execution metrics shared with the aggregate plan node.
    baseline_metrics: BaselineMetrics,

    /// Memory reservation for group keys and accumulators.
    reservation: MemoryReservation,
}

impl RawPartialHashAggregateStream {
    pub fn new(
        agg: &AggregateExec,
        context: &Arc<TaskContext>,
        partition: usize,
    ) -> Result<Self> {
        debug_assert_eq!(agg.mode, super::AggregateMode::Partial);
        debug_assert_eq!(agg.input_order_mode, InputOrderMode::Linear);

        let schema = Arc::clone(&agg.schema);
        let input = agg.input.execute(partition, Arc::clone(context))?;
        let batch_size = context.session_config().batch_size();
        let baseline_metrics = BaselineMetrics::new(&agg.metrics, partition);

        // Preserve the existing aggregate metric surface for this plan node.
        let _spill_metrics = SpillMetrics::new(&agg.metrics, partition);

        let hash_table =
            AggregateHashTable::new(agg, partition, Arc::clone(&schema), batch_size)?;

        let reservation =
            MemoryConsumer::new(format!("RawPartialHashAggregateStream[{partition}]"))
                .register(context.memory_pool());

        Ok(Self {
            schema,
            input,
            exec_state: ExecutionState::ReadingInput,
            hash_table,
            baseline_metrics,
            reservation,
        })
    }
}

impl Stream for RawPartialHashAggregateStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let elapsed_compute = self.baseline_metrics.elapsed_compute().clone();

        loop {
            match &self.exec_state {
                ExecutionState::ReadingInput => {
                    match ready!(self.input.poll_next_unpin(cx)) {
                        Some(Ok(batch)) => {
                            let timer = elapsed_compute.timer();
                            let result = self.hash_table.aggregate_batch(&batch);
                            timer.done();

                            if let Err(e) = result {
                                return Poll::Ready(Some(Err(e)));
                            }

                            // TODO: impl memory-limited aggr, when OOM directly send
                            // partial state to final aggregate stage
                            if let Err(e) =
                                self.reservation.try_resize(self.hash_table.memory_size())
                            {
                                return Poll::Ready(Some(Err(e)));
                            }
                        }
                        Some(Err(e)) => {
                            return Poll::Ready(Some(Err(e)));
                        }
                        None => {
                            let input_schema = self.input.schema();
                            self.input =
                                Box::pin(EmptyRecordBatchStream::new(input_schema));

                            self.exec_state = ExecutionState::ProducingOutput;
                        }
                    }
                }

                ExecutionState::ProducingOutput => {
                    let timer = elapsed_compute.timer();
                    let result = self.hash_table.next_output_batch();
                    timer.done();

                    match result {
                        Ok(Some(batch)) => {
                            let _ = self
                                .reservation
                                .try_resize(self.hash_table.memory_size());
                            debug_assert!(batch.num_rows() > 0);
                            return Poll::Ready(Some(Ok(
                                batch.record_output(&self.baseline_metrics)
                            )));
                        }
                        Ok(None) => {
                            let _ = self
                                .reservation
                                .try_resize(self.hash_table.memory_size());
                            self.exec_state = ExecutionState::Done;
                        }
                        Err(e) => return Poll::Ready(Some(Err(e))),
                    }
                }

                ExecutionState::Done => {
                    self.hash_table.clear();
                    let _ = self.reservation.try_resize(self.hash_table.memory_size());
                    return Poll::Ready(None);
                }
            }
        }
    }
}

impl RecordBatchStream for RawPartialHashAggregateStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}
