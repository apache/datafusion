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

use super::AggregateExec;
use crate::aggregates::{
    aggregate_expressions, create_accumulators, finalize_aggregation, AccumulatorItem,
    AggregateMode,
};
use crate::filter::batch_filter;
use crate::metrics::{BaselineMetrics, RecordOutput};
use crate::stream::RecordBatchStreamAdapter;
use crate::SendableRecordBatchStream;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion_common::Result;
use datafusion_execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion_execution::TaskContext;
use datafusion_physical_expr::PhysicalExpr;
use futures::stream;
use futures::stream::StreamExt;
use std::borrow::Cow;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{ready, Context, Poll};

pub fn aggregate_stream(
    agg: &AggregateExec,
    context: Arc<TaskContext>,
    partition: usize,
) -> Result<SendableRecordBatchStream> {
    let aggregate = Aggregate::new(agg, context, partition)?;

    // Spawn a task the first time the stream is polled for the sort phase.
    // This ensures the consumer of the aggregate does not poll unnecessarily
    // while the aggregation is ongoing
    Ok(crate::stream::create_async_then_emit(
        Arc::clone(&agg.schema),
        aggregate,
    ))
}

/// The state of the aggregation.
struct Aggregate {
    schema: SchemaRef,
    mode: AggregateMode,
    input: SendableRecordBatchStream,
    baseline_metrics: BaselineMetrics,
    aggregate_expressions: Vec<Vec<Arc<dyn PhysicalExpr>>>,
    filter_expressions: Vec<Option<Arc<dyn PhysicalExpr>>>,
    accumulators: Vec<AccumulatorItem>,
    reservation: MemoryReservation,
}

impl Aggregate {
    fn new(
        agg: &AggregateExec,
        context: Arc<TaskContext>,
        partition: usize,
    ) -> Result<Self> {
        let agg_filter_expr = agg.filter_expr.clone();

        let baseline_metrics = BaselineMetrics::new(&agg.metrics, partition);
        let input = agg.input.execute(partition, Arc::clone(&context))?;

        let aggregate_expressions = aggregate_expressions(&agg.aggr_expr, &agg.mode, 0)?;
        let filter_expressions = match agg.mode {
            AggregateMode::Partial
            | AggregateMode::Single
            | AggregateMode::SinglePartitioned => agg_filter_expr,
            AggregateMode::Final | AggregateMode::FinalPartitioned => {
                vec![None; agg.aggr_expr.len()]
            }
        };
        let accumulators = create_accumulators(&agg.aggr_expr)?;

        let reservation = MemoryConsumer::new(format!("AggregateStream[{partition}]"))
            .register(context.memory_pool());

        Ok(Self {
            schema: Arc::clone(&agg.schema),
            mode: agg.mode,
            input,
            baseline_metrics,
            aggregate_expressions,
            filter_expressions,
            accumulators,
            reservation,
        })
    }
}

impl Future for Aggregate {
    type Output = Result<SendableRecordBatchStream>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let elapsed_compute = self.baseline_metrics.elapsed_compute().clone();

        loop {
            match ready!(self.input.poll_next_unpin(cx)) {
                Some(Ok(batch)) => {
                    let timer = elapsed_compute.timer();

                    let result = aggregate_batch(&mut self, &batch);

                    timer.done();

                    // allocate memory
                    // This happens AFTER we actually used the memory, but simplifies the whole accounting and we are OK with
                    // overshooting a bit. Also this means we either store the whole record batch or not.
                    match result
                        .and_then(|allocated| self.reservation.try_grow(allocated))
                    {
                        Ok(_) => continue,
                        Err(e) => return Poll::Ready(Err(e)),
                    }
                }
                Some(Err(e)) => return Poll::Ready(Err(e)),
                None => {
                    let timer = elapsed_compute.timer();
                    let mode = self.mode;
                    let result = finalize_aggregation(&mut self.accumulators, mode)
                        .and_then(|columns| {
                            RecordBatch::try_new(Arc::clone(&self.schema), columns)
                                .map_err(Into::into)
                        })
                        .record_output(&self.baseline_metrics);

                    timer.done();

                    return Poll::Ready(Ok(Box::pin(RecordBatchStreamAdapter::new(
                        Arc::clone(&self.schema),
                        stream::iter(vec![result]),
                    ))));
                }
            };
        }
    }
}

/// Perform group-by aggregation for the given [`RecordBatch`].
///
/// If successful, this returns the additional number of bytes that were allocated during this process.
///
/// TODO: Make this a member function
fn aggregate_batch(agg: &mut Aggregate, batch: &RecordBatch) -> Result<usize> {
    let mut allocated = 0usize;

    // 1.1 iterate accumulators and respective expressions together
    // 1.2 filter the batch if necessary
    // 1.3 evaluate expressions
    // 1.4 update / merge accumulators with the expressions' values

    // 1.1
    agg.accumulators
        .iter_mut()
        .zip(&agg.aggregate_expressions)
        .zip(&agg.filter_expressions)
        .try_for_each(|((accum, expr), filter)| {
            // 1.2
            let batch = match filter {
                Some(filter) => Cow::Owned(batch_filter(batch, filter)?),
                None => Cow::Borrowed(batch),
            };

            let n_rows = batch.num_rows();

            // 1.3
            let values = expr
                .iter()
                .map(|e| e.evaluate(&batch).and_then(|v| v.into_array(n_rows)))
                .collect::<Result<Vec<_>>>()?;

            // 1.4
            let size_pre = accum.size();
            let res = match agg.mode {
                AggregateMode::Partial
                | AggregateMode::Single
                | AggregateMode::SinglePartitioned => accum.update_batch(&values),
                AggregateMode::Final | AggregateMode::FinalPartitioned => {
                    accum.merge_batch(&values)
                }
            };
            let size_post = accum.size();
            allocated += size_post.saturating_sub(size_pre);
            res
        })?;

    Ok(allocated)
}
