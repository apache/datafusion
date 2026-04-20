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

//! [`SortPipeline`] — single-child, same-partition-count breaker that
//! accumulates all input batches for a partition, sorts them on `close`,
//! and serves the result in `batch_size` chunks via `poll_partition`.
//!
//! This implementation is intentionally minimal: it concats all per-partition
//! batches in memory (no spilling) and uses the in-tree
//! [`sort_batch_chunked`](datafusion_physical_plan::sorts::sort::sort_batch_chunked)
//! helper to produce the sorted output chunks. If `fetch` is set, only the
//! first `fetch` rows of the partition are returned (via `sort_batch` with
//! the fetch argument).

use std::collections::VecDeque;
use std::sync::Arc;
use std::task::{Context, Poll, Waker};

use arrow::compute::concat_batches;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion_common::Result;
use datafusion_common::error::DataFusionError;
use datafusion_execution::TaskContext;
use datafusion_physical_expr::LexOrdering;
use datafusion_physical_plan::sorts::sort::{sort_batch, sort_batch_chunked};
use parking_lot::Mutex;

use crate::pipeline::Pipeline;

pub struct SortPipeline {
    expr: LexOrdering,
    fetch: Option<usize>,
    schema: SchemaRef,
    batch_size: usize,
    /// One slot per partition.
    partitions: Vec<Mutex<PartitionState>>,
}

#[derive(Default)]
struct PartitionState {
    /// Batches pushed so far. Replaced by a sorted output queue on close.
    buffered: Vec<RecordBatch>,
    sorted: VecDeque<RecordBatch>,
    closed: bool,
    error: Option<DataFusionError>,
    waker: Option<Waker>,
}

impl std::fmt::Debug for SortPipeline {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SortPipeline")
            .field("fetch", &self.fetch)
            .field("partitions", &self.partitions.len())
            .finish()
    }
}

impl SortPipeline {
    pub fn try_new(
        expr: LexOrdering,
        fetch: Option<usize>,
        schema: SchemaRef,
        partition_count: usize,
        task_context: &Arc<TaskContext>,
    ) -> Result<Self> {
        let batch_size = task_context.session_config().batch_size();
        Ok(Self {
            expr,
            fetch,
            schema,
            batch_size,
            partitions: (0..partition_count)
                .map(|_| Mutex::new(PartitionState::default()))
                .collect(),
        })
    }
}

impl Pipeline for SortPipeline {
    fn push(&self, input: RecordBatch, child: usize, partition: usize) -> Result<()> {
        debug_assert_eq!(child, 0, "SortPipeline has a single input child");
        let mut s = self.partitions[partition].lock();
        if s.closed {
            return Err(DataFusionError::Internal(
                "SortPipeline received push after close".to_string(),
            ));
        }
        s.buffered.push(input);
        Ok(())
    }

    fn close(&self, child: usize, partition: usize) {
        debug_assert_eq!(child, 0, "SortPipeline has a single input child");
        // Do the sort holding the lock. The cost is dominated by the
        // sort itself; any contention only arises when the downstream
        // pipeline's task is actively polling, which is exactly when
        // we want to wake it.
        let waker = {
            let mut s = self.partitions[partition].lock();
            if s.closed {
                return;
            }
            s.closed = true;
            let buffered = std::mem::take(&mut s.buffered);
            match build_sorted_chunks(
                &buffered,
                &self.schema,
                &self.expr,
                self.fetch,
                self.batch_size,
            ) {
                Ok(chunks) => s.sorted = chunks,
                Err(e) => s.error = Some(e),
            }
            s.waker.take()
        };
        if let Some(w) = waker {
            w.wake();
        }
    }

    fn output_partitions(&self) -> usize {
        self.partitions.len()
    }

    fn poll_partition(
        &self,
        cx: &mut Context<'_>,
        partition: usize,
    ) -> Poll<Option<Result<RecordBatch>>> {
        let mut s = self.partitions[partition].lock();
        if let Some(e) = s.error.take() {
            return Poll::Ready(Some(Err(e)));
        }
        if let Some(batch) = s.sorted.pop_front() {
            return Poll::Ready(Some(Ok(batch)));
        }
        if s.closed {
            return Poll::Ready(None);
        }
        s.waker = Some(cx.waker().clone());
        Poll::Pending
    }
}

fn build_sorted_chunks(
    batches: &[RecordBatch],
    schema: &SchemaRef,
    expr: &LexOrdering,
    fetch: Option<usize>,
    batch_size: usize,
) -> Result<VecDeque<RecordBatch>> {
    if batches.is_empty() {
        return Ok(VecDeque::new());
    }
    let concatenated = concat_batches(schema, batches)?;
    if fetch.is_some() {
        // Single-shot top-k path: sort_batch with fetch already limits the
        // output to `fetch` rows. Split into `batch_size` chunks for parity
        // with the non-fetch path.
        let sorted = sort_batch(&concatenated, expr, fetch)?;
        let chunks = split_batch(&sorted, batch_size);
        return Ok(chunks.into());
    }
    let chunks = sort_batch_chunked(&concatenated, expr, batch_size)?;
    Ok(chunks.into())
}

fn split_batch(batch: &RecordBatch, batch_size: usize) -> Vec<RecordBatch> {
    let rows = batch.num_rows();
    if rows <= batch_size {
        return vec![batch.clone()];
    }
    let mut out = Vec::with_capacity(rows.div_ceil(batch_size));
    let mut offset = 0;
    while offset < rows {
        let len = (rows - offset).min(batch_size);
        out.push(batch.slice(offset, len));
        offset += len;
    }
    out
}
