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
    aggregate_expressions, create_accumulators, finalize_aggregation, AccumulatorItem,
    AggrDynFilter, AggregateMode, DynamicFilterAggregateType,
};
use crate::metrics::{BaselineMetrics, RecordOutput};
use crate::{RecordBatchStream, SendableRecordBatchStream};
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion_common::{internal_datafusion_err, internal_err, Result, ScalarValue};
use datafusion_execution::TaskContext;
use datafusion_expr::Operator;
use datafusion_physical_expr::expressions::{lit, BinaryExpr};
use datafusion_physical_expr::PhysicalExpr;
use futures::stream::BoxStream;
use std::borrow::Cow;
use std::cmp::Ordering;
use std::sync::Arc;
use std::task::{Context, Poll};

use super::AggregateExec;
use crate::filter::batch_filter;
use datafusion_execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion_physical_expr_common::utils::evaluate_expressions_to_arrays;
use futures::stream::{Stream, StreamExt};

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
    // ==== Properties ====
    schema: SchemaRef,
    mode: AggregateMode,
    input: SendableRecordBatchStream,
    aggregate_expressions: Vec<Vec<Arc<dyn PhysicalExpr>>>,
    filter_expressions: Vec<Option<Arc<dyn PhysicalExpr>>>,

    // ==== Runtime States/Buffers ====
    accumulators: Vec<AccumulatorItem>,
    // None if the dynamic filter is not applicable. See details in `AggrDynFilter`.
    agg_dyn_filter_state: Option<Arc<AggrDynFilter>>,
    finished: bool,

    // ==== Execution Resources ====
    baseline_metrics: BaselineMetrics,
    reservation: MemoryReservation,
}

impl AggregateStreamInner {
    // TODO: check if we get Null handling correct
    /// # Examples
    /// - Example 1
    ///   Accumulators: min(c1)
    ///   Current Bounds: min(c1)=10
    ///   --> dynamic filter PhysicalExpr: c1 < 10
    ///
    /// - Example 2
    ///   Accumulators: min(c1), max(c1), min(c2)
    ///   Current Bounds: min(c1)=10, max(c1)=100, min(c2)=20
    ///   --> dynamic filter PhysicalExpr: (c1 < 10) OR (c1>100) OR (c2 < 20)
    ///
    /// # Errors
    /// Returns internal errors if the dynamic filter is not enabled, or other
    /// invariant check fails.
    fn build_dynamic_filter_from_accumulator_bounds(
        &self,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        let Some(filter_state) = self.agg_dyn_filter_state.as_ref() else {
            return internal_err!("`build_dynamic_filter_from_accumulator_bounds()` is only called when dynamic filter is enabled");
        };

        let mut predicates: Vec<Arc<dyn PhysicalExpr>> =
            Vec::with_capacity(filter_state.supported_accumulators_info.len());

        for acc_info in &filter_state.supported_accumulators_info {
            // Skip if we don't yet have a meaningful bound
            let bound = {
                let guard = acc_info.shared_bound.lock();
                if (*guard).is_null() {
                    continue;
                }
                guard.clone()
            };

            let agg_exprs = self
                .aggregate_expressions
                .get(acc_info.aggr_index)
                .ok_or_else(|| {
                    internal_datafusion_err!(
                        "Invalid aggregate expression index {} for dynamic filter",
                        acc_info.aggr_index
                    )
                })?;
            // Only aggregates with a single argument are supported.
            let column_expr = agg_exprs.first().ok_or_else(|| {
                internal_datafusion_err!(
                    "Aggregate expression at index {} expected a single argument",
                    acc_info.aggr_index
                )
            })?;

            let literal = lit(bound);
            let predicate: Arc<dyn PhysicalExpr> = match acc_info.aggr_type {
                DynamicFilterAggregateType::Min => Arc::new(BinaryExpr::new(
                    Arc::clone(column_expr),
                    Operator::Lt,
                    literal,
                )),
                DynamicFilterAggregateType::Max => Arc::new(BinaryExpr::new(
                    Arc::clone(column_expr),
                    Operator::Gt,
                    literal,
                )),
            };
            predicates.push(predicate);
        }

        let combined = predicates.into_iter().reduce(|acc, pred| {
            Arc::new(BinaryExpr::new(acc, Operator::Or, pred)) as Arc<dyn PhysicalExpr>
        });

        Ok(combined.unwrap_or_else(|| lit(true)))
    }

    // If the dynamic filter is enabled, update it using the current accumulator's
    // values
    fn maybe_update_dyn_filter(&mut self) -> Result<()> {
        // Step 1: Update each partition's current bound
        let Some(filter_state) = self.agg_dyn_filter_state.as_ref() else {
            return Ok(());
        };

        for acc_info in &filter_state.supported_accumulators_info {
            let acc =
                self.accumulators
                    .get_mut(acc_info.aggr_index)
                    .ok_or_else(|| {
                        internal_datafusion_err!(
                            "Invalid accumulator index {} for dynamic filter",
                            acc_info.aggr_index
                        )
                    })?;
            // First get current partition's bound, then update the shared bound among
            // all partitions.
            let current_bound = acc.evaluate()?;
            {
                let mut bound = acc_info.shared_bound.lock();
                match acc_info.aggr_type {
                    DynamicFilterAggregateType::Max => {
                        *bound = scalar_max(&bound, &current_bound)?;
                    }
                    DynamicFilterAggregateType::Min => {
                        *bound = scalar_min(&bound, &current_bound)?;
                    }
                }
            }
        }

        // Step 2: Sync the dynamic filter physical expression with reader
        let predicate = self.build_dynamic_filter_from_accumulator_bounds()?;
        filter_state.filter.update(predicate)?;

        Ok(())
    }
}

/// Returns the element-wise minimum of two `ScalarValue`s.
///
/// # Null semantics
/// - `min(NULL, NULL)      = NULL`
/// - `min(NULL, x)         = x`
/// - `min(x, NULL)         = x`
///
/// # Errors
/// Returns internal error if v1 and v2 has incompatible types.
fn scalar_min(v1: &ScalarValue, v2: &ScalarValue) -> Result<ScalarValue> {
    if let Some(result) = scalar_cmp_null_short_circuit(v1, v2) {
        return Ok(result);
    }

    match v1.partial_cmp(v2) {
        Some(Ordering::Less | Ordering::Equal) => Ok(v1.clone()),
        Some(Ordering::Greater) => Ok(v2.clone()),
        None => datafusion_common::internal_err!(
            "cannot compare values of different or incompatible types: {v1:?} vs {v2:?}"
        ),
    }
}

/// Returns the element-wise maximum of two `ScalarValue`s.
///
/// # Null semantics
/// - `max(NULL, NULL)      = NULL`
/// - `max(NULL, x)         = x`
/// - `max(x, NULL)         = x`
///
/// # Errors
/// Returns internal error if v1 and v2 has incompatible types.
fn scalar_max(v1: &ScalarValue, v2: &ScalarValue) -> Result<ScalarValue> {
    if let Some(result) = scalar_cmp_null_short_circuit(v1, v2) {
        return Ok(result);
    }

    match v1.partial_cmp(v2) {
        Some(Ordering::Greater | Ordering::Equal) => Ok(v1.clone()),
        Some(Ordering::Less) => Ok(v2.clone()),
        None => datafusion_common::internal_err!(
            "cannot compare values of different or incompatible types: {v1:?} vs {v2:?}"
        ),
    }
}

fn scalar_cmp_null_short_circuit(
    v1: &ScalarValue,
    v2: &ScalarValue,
) -> Option<ScalarValue> {
    match (v1, v2) {
        (ScalarValue::Null, ScalarValue::Null) => Some(ScalarValue::Null),
        (ScalarValue::Null, other) | (other, ScalarValue::Null) => Some(other.clone()),
        _ => None,
    }
}

impl AggregateStream {
    /// Create a new AggregateStream
    pub fn new(
        agg: &AggregateExec,
        context: &Arc<TaskContext>,
        partition: usize,
    ) -> Result<Self> {
        let agg_schema = Arc::clone(&agg.schema);
        let agg_filter_expr = agg.filter_expr.clone();

        let baseline_metrics = BaselineMetrics::new(&agg.metrics, partition);
        let input = agg.input.execute(partition, Arc::clone(context))?;

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

        // Enable dynamic filter if:
        // 1. AggregateExec did the check and ensure it supports the dynamic filter
        //    (its dynamic_filter field will be Some(..))
        // 2. Aggregate dynamic filter is enabled from the config
        let mut maybe_dynamic_filter = match agg.dynamic_filter.as_ref() {
            Some(filter) => Some(Arc::clone(filter)),
            _ => None,
        };

        if !context
            .session_config()
            .options()
            .optimizer
            .enable_aggregate_dynamic_filter_pushdown
        {
            maybe_dynamic_filter = None;
        }

        let inner = AggregateStreamInner {
            schema: Arc::clone(&agg.schema),
            mode: agg.mode,
            input,
            baseline_metrics,
            aggregate_expressions,
            filter_expressions,
            accumulators,
            reservation,
            finished: false,
            agg_dyn_filter_state: maybe_dynamic_filter,
        };

        let stream = futures::stream::unfold(inner, |mut this| async move {
            if this.finished {
                return None;
            }

            loop {
                let result = match this.input.next().await {
                    Some(Ok(batch)) => {
                        let result = {
                            let elapsed_compute = this.baseline_metrics.elapsed_compute();
                            let _timer = elapsed_compute.timer(); // Stops on drop
                            aggregate_batch(
                                &this.mode,
                                &batch,
                                &mut this.accumulators,
                                &this.aggregate_expressions,
                                &this.filter_expressions,
                            )
                        };

                        let result = result.and_then(|allocated| {
                            this.maybe_update_dyn_filter()?;
                            Ok(allocated)
                        });

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
                            finalize_aggregation(&mut this.accumulators, &this.mode)
                                .and_then(|columns| {
                                    RecordBatch::try_new(
                                        Arc::clone(&this.schema),
                                        columns,
                                    )
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
        let stream = stream.fuse();
        let stream = Box::pin(stream);

        Ok(Self {
            schema: agg_schema,
            stream,
        })
    }
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
        Arc::clone(&self.schema)
    }
}

/// Perform group-by aggregation for the given [`RecordBatch`].
///
/// If successful, this returns the additional number of bytes that were allocated during this process.
///
/// TODO: Make this a member function
fn aggregate_batch(
    mode: &AggregateMode,
    batch: &RecordBatch,
    accumulators: &mut [AccumulatorItem],
    expressions: &[Vec<Arc<dyn PhysicalExpr>>],
    filters: &[Option<Arc<dyn PhysicalExpr>>],
) -> Result<usize> {
    let mut allocated = 0usize;

    // 1.1 iterate accumulators and respective expressions together
    // 1.2 filter the batch if necessary
    // 1.3 evaluate expressions
    // 1.4 update / merge accumulators with the expressions' values

    // 1.1
    accumulators
        .iter_mut()
        .zip(expressions)
        .zip(filters)
        .try_for_each(|((accum, expr), filter)| {
            // 1.2
            let batch = match filter {
                Some(filter) => Cow::Owned(batch_filter(batch, filter)?),
                None => Cow::Borrowed(batch),
            };

            // 1.3
            let values = evaluate_expressions_to_arrays(expr, batch.as_ref())?;

            // 1.4
            let size_pre = accum.size();
            let res = match mode {
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
