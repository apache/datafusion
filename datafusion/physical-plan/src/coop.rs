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

//! Utilities for improved cooperative scheduling.
//!
//! # Cooperative scheduling
//!
//! A single call to `poll_next` on a top-level [`Stream`] may potentially perform a lot of work
//! before it returns a `Poll::Pending`. Think for instance of calculating an aggregation over a
//! large dataset.
//! If a `Stream` runs for a long period of time without yielding back to the Tokio executor,
//! it can starve other tasks waiting on that executor to execute them.
//! Additionally, this prevents the query execution from being cancelled.
//!
//! To ensure that `Stream` implementations yield regularly, operators can insert explicit yield
//! points using the utilities in this module. For most operators this is **not** necessary. The
//! `Stream`s of the built-in DataFusion operators that generate (rather than manipulate)
//! `RecordBatch`es such as `DataSourceExec` and those that eagerly consume `RecordBatch`es
//! (for instance, `RepartitionExec`) contain yield points that will make most query `Stream`s yield
//! periodically.
//!
//! There are a couple of types of operators that _should_ insert yield points:
//! - New source operators that do not make use of Tokio resources
//! - Exchange like operators that do not use Tokio's `Channel` implementation to pass data between
//!   tasks
//!
//! ## Adding yield points
//!
//! Yield points can be inserted manually using the facilities provided by the
//! [Tokio coop module](https://docs.rs/tokio/latest/tokio/task/coop/index.html) such as
//! [`tokio::task::coop::consume_budget`](https://docs.rs/tokio/latest/tokio/task/coop/fn.consume_budget.html).
//!
//! Another option is to use the wrapper `Stream` implementation provided by this module which will
//! consume a unit of task budget every time a `RecordBatch` is produced.
//! Wrapper `Stream`s can be created using the [`cooperative`] and [`make_cooperative`] functions.
//!
//! [`cooperative`] is a generic function that takes ownership of the wrapped [`RecordBatchStream`].
//! This function has the benefit of not requiring an additional heap allocation and can avoid
//! dynamic dispatch.
//!
//! [`make_cooperative`] is a non-generic function that wraps a [`SendableRecordBatchStream`]. This
//! can be used to wrap dynamically typed, heap allocated [`RecordBatchStream`]s.
//!
//! ## Automatic cooperation
//!
//! The `EnsureCooperative` physical optimizer rule, which is included in the default set of
//! optimizer rules, inspects query plans for potential cooperative scheduling issues.
//! It injects the [`CooperativeExec`] wrapper `ExecutionPlan` into the query plan where necessary.
//! This `ExecutionPlan` uses [`make_cooperative`] to wrap the `Stream` of its input.
//!
//! The optimizer rule currently checks the plan for exchange-like operators and leave operators
//! that report [`SchedulingType::NonCooperative`] in their [plan properties](ExecutionPlan::properties).

use datafusion_common::config::ConfigOptions;
use datafusion_physical_expr::PhysicalExpr;
#[cfg(datafusion_coop = "tokio_fallback")]
use futures::Future;
use std::any::Any;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use crate::execution_plan::CardinalityEffect::{self, Equal};
use crate::filter_pushdown::{
    ChildPushdownResult, FilterDescription, FilterPushdownPhase,
    FilterPushdownPropagation,
};
use crate::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, RecordBatchStream,
    SendableRecordBatchStream,
};
use arrow::record_batch::RecordBatch;
use arrow_schema::Schema;
use datafusion_common::{assert_eq_or_internal_err, Result, Statistics};
use datafusion_execution::TaskContext;

use crate::execution_plan::SchedulingType;
use crate::stream::RecordBatchStreamAdapter;
use futures::{Stream, StreamExt};

/// A stream that passes record batches through unchanged while cooperating with the Tokio runtime.
/// It consumes cooperative scheduling budget for each returned [`RecordBatch`],
/// allowing other tasks to execute when the budget is exhausted.
///
/// See the [module level documentation](crate::coop) for an in-depth discussion.
pub struct CooperativeStream<T>
where
    T: RecordBatchStream + Unpin,
{
    inner: T,
    #[cfg(datafusion_coop = "per_stream")]
    budget: u8,
}

#[cfg(datafusion_coop = "per_stream")]
// Magic value that matches Tokio's task budget value
const YIELD_FREQUENCY: u8 = 128;

impl<T> CooperativeStream<T>
where
    T: RecordBatchStream + Unpin,
{
    /// Creates a new `CooperativeStream` that wraps the provided stream.
    /// The resulting stream will cooperate with the Tokio scheduler by consuming a unit of
    /// scheduling budget when the wrapped `Stream` returns a record batch.
    pub fn new(inner: T) -> Self {
        Self {
            inner,
            #[cfg(datafusion_coop = "per_stream")]
            budget: YIELD_FREQUENCY,
        }
    }
}

impl<T> Stream for CooperativeStream<T>
where
    T: RecordBatchStream + Unpin,
{
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        #[cfg(any(
            datafusion_coop = "tokio",
            not(any(
                datafusion_coop = "tokio_fallback",
                datafusion_coop = "per_stream"
            ))
        ))]
        {
            let coop = std::task::ready!(tokio::task::coop::poll_proceed(cx));
            let value = self.inner.poll_next_unpin(cx);
            if value.is_ready() {
                coop.made_progress();
            }
            value
        }

        #[cfg(datafusion_coop = "tokio_fallback")]
        {
            // This is a temporary placeholder implementation that may have slightly
            // worse performance compared to `poll_proceed`
            if !tokio::task::coop::has_budget_remaining() {
                cx.waker().wake_by_ref();
                return Poll::Pending;
            }

            let value = self.inner.poll_next_unpin(cx);
            if value.is_ready() {
                // In contrast to `poll_proceed` we are not able to consume
                // budget before proceeding to do work. Instead, we try to consume budget
                // after the work has been done and just assume that that succeeded.
                // The poll result is ignored because we don't want to discard
                // or buffer the Ready result we got from the inner stream.
                let consume = tokio::task::coop::consume_budget();
                let consume_ref = std::pin::pin!(consume);
                let _ = consume_ref.poll(cx);
            }
            value
        }

        #[cfg(datafusion_coop = "per_stream")]
        {
            if self.budget == 0 {
                self.budget = YIELD_FREQUENCY;
                cx.waker().wake_by_ref();
                return Poll::Pending;
            }

            let value = { self.inner.poll_next_unpin(cx) };

            if value.is_ready() {
                self.budget -= 1;
            } else {
                self.budget = YIELD_FREQUENCY;
            }
            value
        }
    }
}

impl<T> RecordBatchStream for CooperativeStream<T>
where
    T: RecordBatchStream + Unpin,
{
    fn schema(&self) -> Arc<Schema> {
        self.inner.schema()
    }
}

/// An execution plan decorator that enables cooperative multitasking.
/// It wraps the streams produced by its input execution plan using the [`make_cooperative`] function,
/// which makes the stream participate in Tokio cooperative scheduling.
#[derive(Debug)]
pub struct CooperativeExec {
    input: Arc<dyn ExecutionPlan>,
    properties: PlanProperties,
}

impl CooperativeExec {
    /// Creates a new `CooperativeExec` operator that wraps the given input execution plan.
    pub fn new(input: Arc<dyn ExecutionPlan>) -> Self {
        let properties = input
            .properties()
            .clone()
            .with_scheduling_type(SchedulingType::Cooperative);

        Self { input, properties }
    }

    /// Returns a reference to the wrapped input execution plan.
    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }
}

impl DisplayAs for CooperativeExec {
    fn fmt_as(
        &self,
        _t: DisplayFormatType,
        f: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result {
        write!(f, "CooperativeExec")
    }
}

impl ExecutionPlan for CooperativeExec {
    fn name(&self) -> &str {
        "CooperativeExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> Arc<Schema> {
        self.input.schema()
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true; self.children().len()]
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        assert_eq_or_internal_err!(
            children.len(),
            1,
            "CooperativeExec requires exactly one child"
        );
        Ok(Arc::new(CooperativeExec::new(children.swap_remove(0))))
    }

    fn execute(
        &self,
        partition: usize,
        task_ctx: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let child_stream = self.input.execute(partition, task_ctx)?;
        Ok(make_cooperative(child_stream))
    }

    fn partition_statistics(&self, partition: Option<usize>) -> Result<Statistics> {
        self.input.partition_statistics(partition)
    }

    fn supports_limit_pushdown(&self) -> bool {
        true
    }

    fn cardinality_effect(&self) -> CardinalityEffect {
        Equal
    }

    fn gather_filters_for_pushdown(
        &self,
        _phase: FilterPushdownPhase,
        parent_filters: Vec<Arc<dyn PhysicalExpr>>,
        _config: &ConfigOptions,
    ) -> Result<FilterDescription> {
        FilterDescription::from_children(parent_filters, &self.children())
    }

    fn handle_child_pushdown_result(
        &self,
        _phase: FilterPushdownPhase,
        child_pushdown_result: ChildPushdownResult,
        _config: &ConfigOptions,
    ) -> Result<FilterPushdownPropagation<Arc<dyn ExecutionPlan>>> {
        Ok(FilterPushdownPropagation::if_all(child_pushdown_result))
    }
}

/// Creates a [`CooperativeStream`] wrapper around the given [`RecordBatchStream`].
/// This wrapper collaborates with the Tokio cooperative scheduler by consuming a unit of
/// scheduling budget for each returned record batch.
pub fn cooperative<T>(stream: T) -> CooperativeStream<T>
where
    T: RecordBatchStream + Unpin + Send + 'static,
{
    CooperativeStream::new(stream)
}

/// Wraps a `SendableRecordBatchStream` inside a [`CooperativeStream`] to enable cooperative multitasking.
/// Since `SendableRecordBatchStream` is a `dyn RecordBatchStream` this requires the use of dynamic
/// method dispatch.
/// When the stream type is statically known, consider use the generic [`cooperative`] function
/// to allow static method dispatch.
pub fn make_cooperative(stream: SendableRecordBatchStream) -> SendableRecordBatchStream {
    // TODO is there a more elegant way to overload cooperative
    Box::pin(cooperative(RecordBatchStreamAdapter::new(
        stream.schema(),
        stream,
    )))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::stream::RecordBatchStreamAdapter;

    use arrow_schema::SchemaRef;

    use futures::{stream, StreamExt};

    // This is the hardcoded value Tokio uses
    const TASK_BUDGET: usize = 128;

    /// Helper: construct a SendableRecordBatchStream containing `n` empty batches
    fn make_empty_batches(n: usize) -> SendableRecordBatchStream {
        let schema: SchemaRef = Arc::new(Schema::empty());
        let schema_for_stream = Arc::clone(&schema);

        let s =
            stream::iter((0..n).map(move |_| {
                Ok(RecordBatch::new_empty(Arc::clone(&schema_for_stream)))
            }));

        Box::pin(RecordBatchStreamAdapter::new(schema, s))
    }

    #[tokio::test]
    async fn yield_less_than_threshold() -> Result<()> {
        let count = TASK_BUDGET - 10;
        let inner = make_empty_batches(count);
        let out = make_cooperative(inner).collect::<Vec<_>>().await;
        assert_eq!(out.len(), count);
        Ok(())
    }

    #[tokio::test]
    async fn yield_equal_to_threshold() -> Result<()> {
        let count = TASK_BUDGET;
        let inner = make_empty_batches(count);
        let out = make_cooperative(inner).collect::<Vec<_>>().await;
        assert_eq!(out.len(), count);
        Ok(())
    }

    #[tokio::test]
    async fn yield_more_than_threshold() -> Result<()> {
        let count = TASK_BUDGET + 20;
        let inner = make_empty_batches(count);
        let out = make_cooperative(inner).collect::<Vec<_>>().await;
        assert_eq!(out.len(), count);
        Ok(())
    }
}
