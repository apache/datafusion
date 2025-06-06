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

//! Support types for to help physical operators implement cooperative yielding.
//!
//! This module provides utilities to help physical operators implement cooperative yielding.
//! This can be necessary to prevent blocking Tokio executor threads for an extended period of
//! time when an operator polls child operators in a loop.

use arrow::array::RecordBatch;
use arrow_schema::Schema;
use datafusion_execution::config::SessionConfig;
use datafusion_execution::{RecordBatchStream, SendableRecordBatchStream, TaskContext};
use futures::{Stream, StreamExt};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Poll::{Pending, Ready};
use std::task::{Context, Poll};

/// A value describing the number of times an operator may poll a child operator without yielding.
/// Yielding means returning control to the calling code by returning any Poll value.
///
/// When the budget is `None`, polling is unconstrained.
#[derive(Copy, Clone)]
pub struct PollBudget {
    /// The maximum number of consecutive polls allowed, or `None` for unconstrained polling.
    /// This value is guaranteed to never be zero.
    budget: Option<u8>,
}

impl PollBudget {
    /// Creates a new `PollBudget` with the specified budget.
    ///
    /// # Arguments
    ///
    /// * `budget` - The maximum number of consecutive polls allowed, or `None` for unconstrained polling.
    ///   A value of `Some(0)` is treated as unconstrained.
    ///
    /// # Returns
    ///
    /// A new `PollBudget` instance.
    pub fn new(budget: Option<u8>) -> Self {
        match budget {
            None => Self::unconstrained(),
            Some(0) => Self::unconstrained(),
            budget @ Some(_) => Self { budget },
        }
    }

    /// Creates an unconstrained `PollBudget` that permits unlimited consecutive polls.
    ///
    /// # Returns
    ///
    /// A new unconstrained `PollBudget` instance.
    pub fn unconstrained() -> Self {
        Self { budget: None }
    }

    /// Checks if this `PollBudget` is unconstrained.
    ///
    /// # Returns
    ///
    /// `true` if this budget is unconstrained, `false` otherwise.
    pub fn is_unconstrained(&self) -> bool {
        self.budget.is_none()
    }

    /// Creates a `ConsumeBudget` future that can be used to track budget consumption.
    /// This is typically used in `Stream` implementations that do not use `async`.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::pin::Pin;
    /// use std::task::{ready, Context, Poll};
    /// use arrow::record_batch::RecordBatch;
    /// use futures::{FutureExt, Stream, StreamExt};
    /// use datafusion_execution::SendableRecordBatchStream;
    /// use datafusion_physical_plan::r#yield::PollBudget;
    ///
    /// struct SkipEmptyStream {
    ///     input: SendableRecordBatchStream,
    ///     poll_budget: PollBudget,
    /// }
    ///
    /// impl Stream for SkipEmptyStream {
    ///    type Item = datafusion_common::Result<RecordBatch>;
    ///
    ///    fn poll_next(
    ///        mut self: Pin<&mut Self>,
    ///        cx: &mut Context<'_>,
    ///    ) -> Poll<Option<Self::Item>> {
    ///        // Create a new budget tracker on poll_next entry  
    ///        let mut consume_budget = self.poll_budget.consume_budget();
    ///        loop {
    ///            return match ready!(self.input.poll_next_unpin(cx)) {
    ///                Some(Ok(batch)) => {
    ///                    if batch.num_rows() == 0 {
    ///                        // Skip empty batches
    ///                        // Consume budget since we're looping
    ///                        // If the budget is depleted Pending will be returned
    ///                        ready!(consume_budget.poll_unpin(cx));
    ///                        continue;
    ///                    }
    ///                    Poll::Ready(Some(Ok(batch)))
    ///                }
    ///                other @ _ => Poll::Ready(other),
    ///            }
    ///        }
    ///    }
    /// }
    /// ```
    ///
    /// # Returns
    ///
    /// A new `ConsumeBudget` instance.
    pub fn consume_budget(&self) -> ConsumeBudget {
        ConsumeBudget {
            remaining: self.budget,
        }
    }

    /// Wraps a record batch stream with a `YieldStream` that respects this budget.
    ///
    /// # Arguments
    ///
    /// * `inner` - The stream to wrap.
    ///
    /// # Returns
    ///
    /// A new `YieldStream` that wraps the input stream and respects this budget.
    pub fn wrap_stream(self, inner: SendableRecordBatchStream) -> YieldStream {
        YieldStream::new(inner, self)
    }
}

/// Creates a `PollBudget` from a session configuration.
///
/// This implementation extracts the poll budget from the session configuration's
/// execution options.
impl From<&SessionConfig> for PollBudget {
    fn from(session_config: &SessionConfig) -> Self {
        Self::new(session_config.options().execution.poll_budget)
    }
}

/// Creates a `PollBudget` from a task context.
///
/// This implementation extracts the poll budget from the task context's session
/// configuration.
impl From<&TaskContext> for PollBudget {
    fn from(context: &TaskContext) -> Self {
        Self::from(context.session_config())
    }
}

/// A future that consumes a poll budget.
///
/// When polled, this future will either:
/// - Complete immediately if the budget is unconstrained or has remaining capacity
/// - Yield to the executor if the budget has been exhausted
///
/// This allows code to respect the poll budget by awaiting this future at appropriate points.
pub struct ConsumeBudget {
    /// The remaining budget, or `None` if unconstrained.
    remaining: Option<u8>,
}

impl Future for ConsumeBudget {
    type Output = ();

    /// Polls this future to completion.
    ///
    /// If the budget is unconstrained (`None`), this future completes immediately.
    /// If the budget has remaining capacity, it decrements the budget and completes.
    /// If the budget is exhausted (0), it wakes the task and returns `Pending`,
    /// effectively yielding to the executor.
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.remaining {
            None => Ready(()),
            Some(remaining) => {
                if remaining == 0 {
                    cx.waker().wake_by_ref();
                    Pending
                } else {
                    self.remaining = Some(remaining - 1);
                    Ready(())
                }
            }
        }
    }
}

/// A stream wrapper that respects a poll budget.
///
/// This stream wraps another record batch stream and limits the number of consecutive
/// polls based on the configured budget. When the budget is exhausted, the stream
/// yields to the executor before continuing.
pub struct YieldStream {
    /// The inner stream being wrapped.
    inner: SendableRecordBatchStream,
    /// The maximum number of consecutive polls allowed, or `None` for unconstrained polling.
    budget: Option<u8>,
    /// The remaining budget for the current polling cycle, or `None` if unconstrained.
    remaining: Option<u8>,
}

impl YieldStream {
    /// Creates a new `YieldStream` that wraps the given stream with the specified budget.
    ///
    /// # Arguments
    ///
    /// * `inner` - The stream to wrap.
    /// * `poll_budget` - The poll budget the stream should respect.
    ///
    /// # Returns
    ///
    /// A new `YieldStream` instance.
    pub fn new(inner: SendableRecordBatchStream, poll_budget: PollBudget) -> Self {
        let budget = poll_budget.budget;
        Self {
            inner,
            budget,
            remaining: budget,
        }
    }
}

impl Stream for YieldStream {
    type Item = datafusion_common::Result<RecordBatch>;

    /// Polls the stream for the next item, respecting the configured budget.
    ///
    /// If the budget is unconstrained (`None`), this delegates directly to the inner stream.
    /// If the budget is exhausted (0), it resets the budget, wakes the task, and returns `Pending`,
    /// effectively yielding to the executor.
    /// Otherwise, it decrements the budget for each successful poll that returns an item.
    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        match self.remaining {
            None => self.inner.poll_next_unpin(cx),
            Some(0) => {
                self.remaining = self.budget;
                cx.waker().wake_by_ref();
                Pending
            }
            Some(remaining) => match self.inner.poll_next_unpin(cx) {
                ready @ Ready(Some(_)) => {
                    self.remaining = Some(remaining - 1);
                    ready
                }
                other => {
                    self.remaining = self.budget;
                    other
                }
            },
        }
    }
}

impl RecordBatchStream for YieldStream {
    /// Returns the schema of the inner stream.
    fn schema(&self) -> Arc<Schema> {
        self.inner.schema()
    }
}
