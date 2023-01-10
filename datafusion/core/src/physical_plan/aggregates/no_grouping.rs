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

//! Aggregate without grouping columns

use crate::execution::context::TaskContext;
use crate::physical_plan::aggregates::{
    aggregate_expressions, create_accumulators, create_accumulators_v2, evaluate_many,
    finalize_aggregation, AccumulatorItem, AccumulatorItemV2, AggregateMode,
};
use crate::physical_plan::metrics::{BaselineMetrics, RecordOutput};
use crate::physical_plan::{RecordBatchStream, SendableRecordBatchStream};
use arrow::datatypes::SchemaRef;
use arrow::error::{ArrowError, Result as ArrowResult};
use arrow::record_batch::RecordBatch;
use datafusion_common::Result;
use datafusion_physical_expr::{AggregateExpr, PhysicalExpr};
use futures::stream::BoxStream;
use std::sync::Arc;
use std::task::{Context, Poll};

use crate::execution::memory_pool::{MemoryConsumer, MemoryReservation};
use crate::physical_plan::aggregates::row_hash::{
    aggr_state_schema, RowAggregationState, RowGroupState,
};
use datafusion_row::accessor::RowAccessor;
use datafusion_row::layout::RowLayout;
use datafusion_row::RowType;
use futures::stream::{Stream, StreamExt};
use hashbrown::raw::RawTable;

/// stream struct for aggregation without grouping columns
pub(crate) struct AggregateStream {
    stream: BoxStream<'static, ArrowResult<RecordBatch>>,
    schema: SchemaRef,
}

/// Actual implementation of [`AggregateStream`].
///
/// This is wrapped into yet another struct because we need to interact with the async memory management subsystem
/// during poll. To have as little code "weirdness" as possible, we chose to just use [`BoxStream`] together with
/// [`futures::stream::unfold`]. The latter requires a state object, which is [`GroupedHashAggregateStreamV2Inner`].
struct AggregateStreamInner {
    schema: SchemaRef,
    mode: AggregateMode,
    input: SendableRecordBatchStream,
    baseline_metrics: BaselineMetrics,
    aggregate_expressions: Vec<Vec<Arc<dyn PhysicalExpr>>>,
    row_aggregate_expressions: Vec<Vec<Arc<dyn PhysicalExpr>>>,
    accumulators: Vec<AccumulatorItem>,
    row_accumulators: Vec<AccumulatorItemV2>,
    reservation: MemoryReservation,
    finished: bool,
    row_aggr_state: RowAggregationState,
    row_aggr_layout: Arc<RowLayout>,
    row_aggr_schema: SchemaRef,
    indices: Vec<Vec<(usize, (usize, usize))>>,
}

impl AggregateStream {
    /// Create a new AggregateStream
    pub fn new(
        mode: AggregateMode,
        schema: SchemaRef,
        aggr_expr: Vec<Arc<dyn AggregateExpr>>,
        input: SendableRecordBatchStream,
        baseline_metrics: BaselineMetrics,
        context: Arc<TaskContext>,
        partition: usize,
    ) -> Result<Self> {
        let mut row_agg_indices = vec![];
        let mut normal_agg_indices = vec![];
        let mut start_idx = 0;
        for (idx, expr) in aggr_expr.iter().enumerate() {
            let n_field = match mode {
                AggregateMode::Partial => expr.state_fields()?.len(),
                _ => 1,
            };
            if expr.row_accumulator_supported() {
                row_agg_indices.push((idx, (start_idx, start_idx + n_field)));
            } else {
                normal_agg_indices.push((idx, (start_idx, start_idx + n_field)));
            }
            start_idx += n_field;
        }
        let indices = vec![normal_agg_indices, row_agg_indices];
        let row_aggr_exprs = aggr_expr
            .clone()
            .into_iter()
            .filter(|elem| elem.row_accumulator_supported())
            .collect::<Vec<_>>();
        let normal_aggr_exprs = aggr_expr
            .clone()
            .into_iter()
            .filter(|elem| !elem.row_accumulator_supported())
            .collect::<Vec<_>>();

        let row_aggr_schema = aggr_state_schema(&row_aggr_exprs)?;

        let row_aggr_layout =
            Arc::new(RowLayout::new(&row_aggr_schema, RowType::WordAligned));
        let reservation =
            MemoryConsumer::new(format!("GroupedHashAggregateStreamV2[{}]", partition))
                .register(context.memory_pool());

        // Add new entry to group_states and save newly created index
        let group_state = RowGroupState {
            group_by_values: Box::new([]),
            aggregation_buffer: vec![0; row_aggr_layout.fixed_part_width()],
            accumulator_set: vec![],
            indices: vec![0_u32], // 1.3
        };
        let row_aggr_state = RowAggregationState {
            reservation,
            map: RawTable::with_capacity(0),
            group_states: vec![group_state],
        };

        let all_aggregate_expressions = aggregate_expressions(&aggr_expr, &mode, 0)?;
        let mut normal_aggregate_expressions = vec![];
        for (idx, _) in &indices[0] {
            normal_aggregate_expressions.push(all_aggregate_expressions[*idx].clone())
        }
        let mut row_aggregate_expressions = vec![];
        for (idx, _) in &indices[1] {
            row_aggregate_expressions.push(all_aggregate_expressions[*idx].clone())
        }
        let normal_accumulators = create_accumulators(&normal_aggr_exprs)?;
        let row_accumulators = create_accumulators_v2(&row_aggr_exprs)?;

        let reservation = MemoryConsumer::new(format!("AggregateStream[{partition}]"))
            .register(context.memory_pool());

        let inner = AggregateStreamInner {
            schema: Arc::clone(&schema),
            mode,
            input,
            baseline_metrics,
            aggregate_expressions: normal_aggregate_expressions,
            row_aggregate_expressions,
            accumulators: normal_accumulators,
            row_accumulators,
            reservation,
            finished: false,
            row_aggr_state,
            row_aggr_layout,
            row_aggr_schema,
            indices,
        };
        let stream = futures::stream::unfold(inner, |mut this| async move {
            if this.finished {
                return None;
            }

            let elapsed_compute = this.baseline_metrics.elapsed_compute();

            loop {
                let result = match this.input.next().await {
                    Some(Ok(batch)) => {
                        let timer = elapsed_compute.timer();
                        let result = aggregate_batch(
                            &this.mode,
                            &batch,
                            &mut this.accumulators,
                            &mut this.row_accumulators,
                            &this.aggregate_expressions,
                            &this.row_aggregate_expressions,
                            &mut this.row_aggr_state,
                            this.row_aggr_layout.clone(),
                        );

                        timer.done();

                        // allocate memory
                        // This happens AFTER we actually used the memory, but simplifies the whole accounting and we are OK with
                        // overshooting a bit. Also this means we either store the whole record batch or not.
                        match result
                            .and_then(|allocated| this.reservation.try_grow(allocated))
                        {
                            Ok(_) => continue,
                            Err(e) => Err(ArrowError::ExternalError(Box::new(e))),
                        }
                    }
                    Some(Err(e)) => Err(e),
                    None => {
                        this.finished = true;
                        let timer = this.baseline_metrics.elapsed_compute().timer();
                        let result = finalize_aggregation(
                            &this.accumulators,
                            &this.row_accumulators,
                            &this.mode,
                            &this.row_aggr_schema,
                            &mut this.row_aggr_state,
                            &this.schema,
                            &this.indices,
                        )
                        .map_err(|e| ArrowError::ExternalError(Box::new(e)))
                        .and_then(|columns| {
                            RecordBatch::try_new(this.schema.clone(), columns)
                        })
                        .record_output(&this.baseline_metrics);

                        timer.done();

                        result
                    }
                };

                this.finished = true;
                return Some((result, this));
            }
        });

        // seems like some consumers call this stream even after it returned `None`, so let's fuse the stream.
        let stream = stream.fuse();
        let stream = Box::pin(stream);

        Ok(Self { schema, stream })
    }
}

impl Stream for AggregateStream {
    type Item = ArrowResult<RecordBatch>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let this = &mut *self;
        this.stream.poll_next_unpin(cx)
    }
}

impl RecordBatchStream for AggregateStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

/// Perform group-by aggregation for the given [`RecordBatch`].
///
/// If successfull, this returns the additional number of bytes that were allocated during this process.
///
/// TODO: Make this a member function
#[allow(clippy::too_many_arguments)]
fn aggregate_batch(
    mode: &AggregateMode,
    batch: &RecordBatch,
    accumulators: &mut [AccumulatorItem],
    row_accumulators: &mut [AccumulatorItemV2],
    expressions: &[Vec<Arc<dyn PhysicalExpr>>],
    row_expressions: &[Vec<Arc<dyn PhysicalExpr>>],
    row_aggr_state: &mut RowAggregationState,
    state_layout: Arc<RowLayout>,
) -> Result<usize> {
    let RowAggregationState { group_states, .. } = row_aggr_state;
    let group_state = &mut group_states[0];
    let aggr_input_values = evaluate_many(row_expressions, batch)?;
    row_accumulators
        .iter_mut()
        .zip(aggr_input_values.iter())
        .map(|(accumulator, aggr_array)| (accumulator, aggr_array))
        .try_for_each(|(accumulator, values)| {
            let mut state_accessor = RowAccessor::new_from_layout(state_layout.clone());
            state_accessor.point_to(0, group_state.aggregation_buffer.as_mut_slice());
            match mode {
                AggregateMode::Partial => {
                    accumulator.update_batch(values, &mut state_accessor)
                }
                AggregateMode::FinalPartitioned | AggregateMode::Final => {
                    // note: the aggregation here is over states, not values, thus the merge
                    accumulator.merge_batch(values, &mut state_accessor)
                }
            }
        })
        // 2.5
        .and({
            group_state.indices.clear();
            Ok(())
        })?;

    let mut allocated = 0usize;

    // 1.1 iterate accumulators and respective expressions together
    // 1.2 evaluate expressions
    // 1.3 update / merge accumulators with the expressions' values

    // 1.1
    accumulators
        .iter_mut()
        .zip(expressions)
        .try_for_each(|(accum, expr)| {
            // 1.2
            let values = &expr
                .iter()
                .map(|e| e.evaluate(batch))
                .map(|r| r.map(|v| v.into_array(batch.num_rows())))
                .collect::<Result<Vec<_>>>()?;

            // 1.3
            let size_pre = accum.size();
            let res = match mode {
                AggregateMode::Partial => accum.update_batch(values),
                AggregateMode::Final | AggregateMode::FinalPartitioned => {
                    accum.merge_batch(values)
                }
            };
            let size_post = accum.size();
            allocated += size_post.saturating_sub(size_pre);
            res
        })?;

    Ok(allocated)
}
