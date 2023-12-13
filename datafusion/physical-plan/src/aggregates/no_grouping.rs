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

use crate::aggregates::{
    aggregate_expressions, create_accumulators, finalize_aggregation,
    reorder_aggregate_expr_results, AccumulatorItem, AggregateExprGroup, AggregateMode,
};
use crate::metrics::{BaselineMetrics, RecordOutput};
use crate::{RecordBatchStream, SendableRecordBatchStream};
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use arrow_array::ArrayRef;
use datafusion_common::Result;
use datafusion_execution::TaskContext;
use futures::stream::BoxStream;
use std::borrow::Cow;
use std::sync::Arc;
use std::task::{Context, Poll};

use crate::filter::batch_filter;
use crate::sorts::sort::sort_batch;
use datafusion_common::utils::get_at_indices;
use datafusion_execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion_physical_expr::{LexOrdering, PhysicalExpr};
use futures::stream::{Stream, StreamExt};
use itertools::izip;

use super::AggregateExec;

/// A structure storing necessary data for aggregate expr evaluation.
///
/// # Fields
///
/// - `expressions`: A vector expressions that aggregate expression refers e.g for CORR(a, b) this will be a, b.
/// - `filter_expression`: A vector of optional filter expression associated with aggregate expression.
/// - `accumulator`: The accumulator used to calculate aggregate expression result.
pub struct AggregateExprData {
    expressions: Vec<Arc<dyn PhysicalExpr>>,
    filter_expression: Option<Arc<dyn PhysicalExpr>>,
    accumulator: AccumulatorItem,
}

/// A structure representing an aggregate group.
///
/// The `AggregateGroup` struct is all aggregate expressions
/// where ordering requirement is satisfied by `requirement`.
/// This struct divides aggregate expressions according to their requirements.
/// Aggregate groups are constructed using `get_aggregate_expr_groups` function.
///
/// # Fields
///
/// - `aggregates`: A vector of `AggregateExprData` which stores necessary fields for successful evaluation of the each aggregate expression.
/// - `requirement`: A `LexOrdering` instance specifying the lexical ordering requirement of the group.
/// - `group_indices`: A vector of indices indicating position of each aggregation in the original aggregate expression.
pub struct AggregateGroup {
    aggregates: Vec<AggregateExprData>,
    requirement: LexOrdering,
    group_indices: Vec<usize>,
}

/// stream struct for aggregation without grouping columns
pub(crate) struct AggregateStream {
    stream: BoxStream<'static, Result<RecordBatch>>,
    schema: SchemaRef,
}

/// Actual implementation of [`AggregateStream`].
///
/// This is wrapped into yet another struct because we need to interact with the async memory management subsystem
/// during poll. To have as little code "weirdness" as possible, we chose to just use [`BoxStream`] together with
/// [`futures::stream::unfold`].
///
/// The latter requires a state object, which is [`AggregateStreamInner`].
struct AggregateStreamInner {
    schema: SchemaRef,
    mode: AggregateMode,
    input: SendableRecordBatchStream,
    baseline_metrics: BaselineMetrics,
    aggregate_groups: Vec<AggregateGroup>,
    reservation: MemoryReservation,
    finished: bool,
}

impl AggregateStream {
    /// Create a new AggregateStream
    pub fn new(
        aggregate_exec: &AggregateExec,
        context: Arc<TaskContext>,
        partition: usize,
    ) -> Result<Self> {
        let agg_schema = Arc::clone(&aggregate_exec.schema);
        let agg_filter_expr = aggregate_exec.filter_expr.clone();

        let baseline_metrics = BaselineMetrics::new(&aggregate_exec.metrics, partition);
        let input = aggregate_exec
            .input
            .execute(partition, Arc::clone(&context))?;

        let aggregate_expressions =
            aggregate_expressions(&aggregate_exec.aggr_expr, &aggregate_exec.mode, 0)?;
        let filter_expressions = match aggregate_exec.mode {
            AggregateMode::Partial
            | AggregateMode::Single
            | AggregateMode::SinglePartitioned => agg_filter_expr,
            AggregateMode::Final | AggregateMode::FinalPartitioned => {
                vec![None; aggregate_exec.aggr_expr.len()]
            }
        };

        let reservation = MemoryConsumer::new(format!("AggregateStream[{partition}]"))
            .register(context.memory_pool());
        let aggregate_groups = aggregate_exec
            .aggregate_groups
            .iter()
            .map(
                |AggregateExprGroup {
                     indices,
                     requirement,
                 }| {
                    let aggr_exprs = get_at_indices(&aggregate_exec.aggr_expr, indices)?;
                    let aggregate_expressions =
                        get_at_indices(&aggregate_expressions, indices)?;
                    let filter_expressions =
                        get_at_indices(&filter_expressions, indices)?;
                    let accumulators = create_accumulators(&aggr_exprs)?;
                    let aggregates = izip!(
                        aggregate_expressions.into_iter(),
                        filter_expressions.into_iter(),
                        accumulators.into_iter()
                    )
                    .map(|(expressions, filter_expression, accumulator)| {
                        AggregateExprData {
                            expressions,
                            filter_expression,
                            accumulator,
                        }
                    })
                    .collect::<Vec<_>>();
                    Ok(AggregateGroup {
                        aggregates,
                        requirement: requirement.to_vec(),
                        group_indices: indices.to_vec(),
                    })
                },
            )
            .collect::<Result<Vec<_>>>()?;

        let stream = create_aggregate_stream(
            aggregate_exec,
            input,
            baseline_metrics,
            aggregate_groups,
            reservation,
        )?;

        Ok(Self {
            schema: agg_schema,
            stream,
        })
    }
}

/// Creates a stream for processing aggregate expressions.
///
/// This function constructs a stream that processes batches from `input`, aggregates
/// them according to `aggregate_groups`, and finally yields the aggregated results.
/// It handles the aggregation logic depending on the aggregate mode defined in `agg`.
/// The function also accounts for memory consumption during aggregation using `reservation`.
///
/// # Parameters
/// - `aggregate_exec`: Reference to the `AggregateExec` struct which contains the aggregate execution plan.
/// - `input`: Stream of `RecordBatch` items representing the input data.
/// - `baseline_metrics`: Metrics for tracking the performance and resource usage.
/// - `aggregate_groups`: A vector of `AggregateGroup` structs, each representing a group of
///   aggregate expressions along with their corresponding indices and ordering requirements.
/// - `reservation`: Memory reservation handle for managing memory consumption during aggregation.
///
/// # Returns
/// A `Result` containing the constructed stream if successful, or an error if the stream
/// creation fails. The stream yields `RecordBatch` items, each representing a batch of aggregated results.
fn create_aggregate_stream(
    aggregate_exec: &AggregateExec,
    input: SendableRecordBatchStream,
    baseline_metrics: BaselineMetrics,
    aggregate_groups: Vec<AggregateGroup>,
    reservation: MemoryReservation,
) -> Result<BoxStream<'static, Result<RecordBatch>>> {
    let inner = AggregateStreamInner {
        schema: Arc::clone(&aggregate_exec.schema),
        mode: aggregate_exec.mode,
        input,
        baseline_metrics,
        aggregate_groups,
        reservation,
        finished: false,
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
                    let result = aggregate_batch_groups(
                        &this.mode,
                        batch,
                        &mut this.aggregate_groups,
                    );

                    timer.done();

                    // allocate memory
                    // This happens AFTER we actually used the memory, but simplifies the whole accounting and we are OK with
                    // overshooting a bit. Also this means we either store the whole record batch or not.
                    match result
                        .and_then(|allocated| this.reservation.try_grow(allocated))
                    {
                        Ok(_) => continue,
                        Err(e) => Err(e),
                    }
                }
                Some(Err(e)) => Err(e),
                None => {
                    this.finished = true;
                    let timer = this.baseline_metrics.elapsed_compute().timer();
                    let result =
                        finalize_aggregation_groups(&this.aggregate_groups, &this.mode)
                            .and_then(|columns| {
                                RecordBatch::try_new(this.schema.clone(), columns)
                                    .map_err(Into::into)
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
    Ok(Box::pin(stream.fuse()))
}

impl Stream for AggregateStream {
    type Item = Result<RecordBatch>;

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

/// Perform group-by aggregation for the given [`RecordBatch`] on all aggregate groups.
///
/// If successful, this returns the additional number of bytes that were allocated during this process.
fn aggregate_batch_groups(
    mode: &AggregateMode,
    batch: RecordBatch,
    aggregate_groups: &mut [AggregateGroup],
) -> Result<usize> {
    let allocated = aggregate_groups
        .iter_mut()
        .map(|aggregate_group| aggregate_batch(mode, &batch, aggregate_group))
        .collect::<Result<Vec<_>>>()?;
    Ok(allocated.into_iter().sum())
}

/// Perform group-by aggregation for the given [`RecordBatch`] on the aggregate group.
///
/// If successful, this returns the additional number of bytes that were allocated during this process.
///
/// TODO: Make this a member function
fn aggregate_batch(
    mode: &AggregateMode,
    batch: &RecordBatch,
    aggregate_group: &mut AggregateGroup,
) -> Result<usize> {
    let mut allocated = 0usize;

    // 1.1 iterate accumulators and respective expressions together
    // 1.2 filter the batch if necessary
    // 1.3 evaluate expressions
    // 1.4 update / merge accumulators with the expressions' values

    let requirement = &aggregate_group.requirement;
    let sorted_or_original_batch = if requirement.is_empty() {
        Cow::Borrowed(batch)
    } else {
        Cow::Owned(sort_batch(batch, requirement, None)?)
    };
    // 1.1
    aggregate_group.aggregates.iter_mut().try_for_each(
        |AggregateExprData {
             expressions,
             filter_expression,
             accumulator,
         }| {
            // 1.2
            let filtered_or_original_batch = match filter_expression {
                Some(filter) => {
                    Cow::Owned(batch_filter(&sorted_or_original_batch, filter)?)
                }
                None => Cow::Borrowed(&*sorted_or_original_batch),
            };
            // 1.3
            let values = &expressions
                .iter()
                .map(|e| {
                    e.evaluate(&filtered_or_original_batch)
                        .and_then(|v| v.into_array(filtered_or_original_batch.num_rows()))
                })
                .collect::<Result<Vec<_>>>()?;

            // 1.4
            let size_pre = accumulator.size();
            let res = match mode {
                AggregateMode::Partial
                | AggregateMode::Single
                | AggregateMode::SinglePartitioned => accumulator.update_batch(values),
                AggregateMode::Final | AggregateMode::FinalPartitioned => {
                    accumulator.merge_batch(values)
                }
            };
            let size_post = accumulator.size();
            allocated += size_post.saturating_sub(size_pre);
            res
        },
    )?;

    Ok(allocated)
}

/// returns a vector of ArrayRefs, where each entry corresponds to either the
/// final value (mode = Final, FinalPartitioned and Single) or states (mode = Partial)
fn finalize_aggregation_groups(
    aggregate_groups: &[AggregateGroup],
    mode: &AggregateMode,
) -> Result<Vec<ArrayRef>> {
    let aggregate_group_results = aggregate_groups
        .iter()
        .map(|aggregate_group| {
            let accumulators = aggregate_group
                .aggregates
                .iter()
                .map(|elem| &elem.accumulator)
                .collect::<Vec<_>>();
            finalize_aggregation(&accumulators, mode)
        })
        .collect::<Result<Vec<_>>>()?;
    let aggregate_group_indices = aggregate_groups
        .iter()
        .map(|aggregate_group| aggregate_group.group_indices.to_vec())
        .collect::<Vec<_>>();

    Ok(reorder_aggregate_expr_results(
        aggregate_group_results,
        aggregate_group_indices,
    ))
}
