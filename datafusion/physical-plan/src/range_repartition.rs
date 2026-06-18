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

//! Skeleton operator that will eventually range-partition its input on a
//! single order-key into N output partitions, with halo overlap for
//! bounded RANGE-frame window functions sitting above it.
//!
//! Today this is a pass-through that wraps each forwarded stream with a
//! one-shot observer: when the first batch of partition `i` is yielded
//! (which guarantees the upstream sort has folded at least one chunk
//! into its `SortExtremes` slot), it calls
//! `child.runtime_sort_extremes(i)` and logs the result. Plan integration
//! is therefore a no-op for correctness today, but the log line confirms
//! the runtime-stats plumbing reaches the downstream consumer.
//
// TODO: replace pass-through with real range routing — gather all K
// per-input-partition `SortExtremes`, derive global boundaries, route
// rows by binary search on the leading sort key, emit halo-expanded
// per-output-partition streams.

use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow::array::RecordBatch;
use datafusion_common::Result;
use datafusion_execution::{RecordBatchStream, TaskContext};
use futures::Stream;
use log::info;

use crate::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties,
    SendableRecordBatchStream,
};
use arrow::datatypes::SchemaRef;

#[derive(Debug)]
pub struct RangeRepartitionExec {
    input: Arc<dyn ExecutionPlan>,
    cache: Arc<PlanProperties>,
}

impl RangeRepartitionExec {
    pub fn new(input: Arc<dyn ExecutionPlan>) -> Self {
        let cache = Arc::clone(input.properties());
        Self { input, cache }
    }

    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }
}

impl DisplayAs for RangeRepartitionExec {
    fn fmt_as(
        &self,
        _t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(f, "RangeRepartitionExec")
    }
}

impl ExecutionPlan for RangeRepartitionExec {
    fn name(&self) -> &'static str {
        "RangeRepartitionExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self::new(children.swap_remove(0))))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let inner = self.input.execute(partition, context)?;
        Ok(Box::pin(LogOnFirstBatch {
            inner,
            partition,
            input: Arc::clone(&self.input),
            logged: false,
        }))
    }
}

/// Pass-through stream that, on the first batch it yields, asks the child
/// operator for its `runtime_sort_extremes` and logs them. Confirms that
/// the upstream sort has populated its slot by the time downstream sees
/// data.
struct LogOnFirstBatch {
    inner: SendableRecordBatchStream,
    partition: usize,
    input: Arc<dyn ExecutionPlan>,
    logged: bool,
}

impl Stream for LogOnFirstBatch {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let item = Pin::new(&mut self.inner).poll_next(cx);
        if let Poll::Ready(Some(Ok(_))) = &item
            && !self.logged
        {
            self.logged = true;
            match self.input.runtime_sort_extremes(self.partition) {
                Ok(Some(extremes)) => info!(
                    "RangeRepartitionExec: input partition {} runtime_sort_extremes: \
                     min={:?} max={:?} rows={}",
                    self.partition, extremes.min, extremes.max, extremes.row_count
                ),
                Ok(None) => info!(
                    "RangeRepartitionExec: input partition {} runtime_sort_extremes: None",
                    self.partition
                ),
                Err(e) => info!(
                    "RangeRepartitionExec: input partition {} runtime_sort_extremes error: {e}",
                    self.partition
                ),
            }
        }
        item
    }
}

impl RecordBatchStream for LogOnFirstBatch {
    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }
}
