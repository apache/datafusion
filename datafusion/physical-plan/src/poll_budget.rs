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

#[derive(Copy, Clone)]
pub struct PollBudget {
    budget: Option<u8>,
}

impl PollBudget {
    pub fn new(budget: Option<u8>) -> Self {
        match budget {
            None => Self::unconstrained(),
            Some(0) => Self::unconstrained(),
            budget @ Some(_) => Self { budget },
        }
    }

    pub fn unconstrained() -> Self {
        Self { budget: None }
    }

    pub fn is_unconstrained(&self) -> bool {
        self.budget.is_none()
    }

    pub fn consume_budget(&self) -> ConsumeBudget {
        ConsumeBudget {
            remaining: self.budget,
        }
    }

    pub fn wrap_stream(
        &self,
        inner: SendableRecordBatchStream,
    ) -> SendableRecordBatchStream {
        match self.budget {
            None => inner,
            Some(budget) => {
                Box::pin(YieldStream::new(inner, budget)) as SendableRecordBatchStream
            }
        }
    }
}

impl From<&SessionConfig> for PollBudget {
    fn from(session_config: &SessionConfig) -> Self {
        Self::new(session_config.options().execution.poll_budget)
    }
}

impl From<&TaskContext> for PollBudget {
    fn from(context: &TaskContext) -> Self {
        Self::from(context.session_config())
    }
}

pub struct ConsumeBudget {
    remaining: Option<u8>,
}

impl Future for ConsumeBudget {
    type Output = ();

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

struct YieldStream {
    inner: SendableRecordBatchStream,
    budget: u8,
    remaining: u8,
}

impl YieldStream {
    pub fn new(inner: SendableRecordBatchStream, budget: u8) -> Self {
        Self {
            inner,
            budget,
            remaining: 0,
        }
    }
}

impl Stream for YieldStream {
    type Item = datafusion_common::Result<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        if self.remaining == 0 {
            self.remaining = self.budget;
            cx.waker().wake_by_ref();
            return Pending;
        }

        match self.inner.poll_next_unpin(cx) {
            ready @ Ready(Some(_)) => {
                self.remaining -= 1;
                ready
            }
            other => {
                self.remaining = self.budget;
                other
            }
        }
    }
}

impl RecordBatchStream for YieldStream {
    fn schema(&self) -> Arc<Schema> {
        self.inner.schema()
    }
}
