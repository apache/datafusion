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

use crate::physical_plan::aggregates::{
    aggregate_expressions, create_accumulators, finalize_aggregation, AccumulatorItem,
    AggregateMode,
};
use crate::physical_plan::metrics::{BaselineMetrics, RecordOutput};
use crate::physical_plan::{RecordBatchStream, SendableRecordBatchStream};
use arrow::datatypes::SchemaRef;
use arrow::error::{ArrowError, Result as ArrowResult};
use arrow::record_batch::RecordBatch;
use datafusion_common::Result;
use datafusion_physical_expr::{AggregateExpr, PhysicalExpr};
use std::sync::Arc;
use std::task::{Context, Poll};

use futures::{
    ready,
    stream::{Stream, StreamExt},
};

/// stream struct for aggregation without grouping columns
pub(crate) struct AggregateStream {
    schema: SchemaRef,
    mode: AggregateMode,
    input: SendableRecordBatchStream,
    baseline_metrics: BaselineMetrics,
    aggregate_expressions: Vec<Vec<Arc<dyn PhysicalExpr>>>,
    accumulators: Vec<AccumulatorItem>,
    finished: bool,
}

impl AggregateStream {
    /// Create a new AggregateStream
    pub fn new(
        mode: AggregateMode,
        schema: SchemaRef,
        aggr_expr: Vec<Arc<dyn AggregateExpr>>,
        input: SendableRecordBatchStream,
        baseline_metrics: BaselineMetrics,
    ) -> datafusion_common::Result<Self> {
        let aggregate_expressions = aggregate_expressions(&aggr_expr, &mode, 0)?;
        let accumulators = create_accumulators(&aggr_expr)?;

        Ok(Self {
            schema,
            mode,
            input,
            baseline_metrics,
            aggregate_expressions,
            accumulators,
            finished: false,
        })
    }
}

impl Stream for AggregateStream {
    type Item = ArrowResult<RecordBatch>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let this = &mut *self;
        if this.finished {
            return Poll::Ready(None);
        }

        let elapsed_compute = this.baseline_metrics.elapsed_compute();

        loop {
            let result = match ready!(this.input.poll_next_unpin(cx)) {
                Some(Ok(batch)) => {
                    let timer = elapsed_compute.timer();
                    let result = aggregate_batch(
                        &this.mode,
                        &batch,
                        &mut this.accumulators,
                        &this.aggregate_expressions,
                    );

                    timer.done();

                    match result {
                        Ok(_) => continue,
                        Err(e) => Err(ArrowError::ExternalError(Box::new(e))),
                    }
                }
                Some(Err(e)) => Err(e),
                None => {
                    this.finished = true;
                    let timer = this.baseline_metrics.elapsed_compute().timer();
                    let result = finalize_aggregation(&this.accumulators, &this.mode)
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
            return Poll::Ready(Some(result));
        }
    }
}

impl RecordBatchStream for AggregateStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

/// TODO: Make this a member function
fn aggregate_batch(
    mode: &AggregateMode,
    batch: &RecordBatch,
    accumulators: &mut [AccumulatorItem],
    expressions: &[Vec<Arc<dyn PhysicalExpr>>],
) -> Result<()> {
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
            match mode {
                AggregateMode::Partial => accum.update_batch(values),
                AggregateMode::Final | AggregateMode::FinalPartitioned => {
                    accum.merge_batch(values)
                }
            }
        })
}
