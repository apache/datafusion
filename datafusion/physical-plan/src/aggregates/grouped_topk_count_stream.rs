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

//! TopK aggregation specialized for `count(*)` / `count(col)`.
//!
//! # Why this stream exists
//!
//! `MIN`/`MAX` aggregates are idempotent under partition merge, so
//! [`GroupedTopKAggregateStream`] can safely cap its hash table at `K` and
//! evict groups whose partial value is already worse than the heap's worst.
//!
//! `COUNT` is additive: a group with a small partial count today may
//! accumulate more rows tomorrow and become top-K. Any early eviction
//! endangers correctness. This stream therefore keeps a full unbounded hash
//! aggregate (delegated to [`GroupedHashAggregateStream`]) but overlays a
//! bookkeeping top-K accumulator at emit time, so downstream sees only `K`
//! rows and the wrapper itself holds at most `K` rows of state.
//!
//! # Correctness
//!
//! Enabled only at `Final`/`FinalPartitioned`/`Single`/`SinglePartitioned`
//! mode; the optimizer never marks a `Partial` aggregate. At those modes,
//! `RepartitionExec::Hash([group_keys])` guarantees each group's rows all
//! land in one final partition, so per-partition top-K is safe to combine
//! downstream.
//!
//! # Roadmap
//!
//! A follow-up commit will add a "new-group gate": before inserting a
//! previously-unseen group, check `remaining_rows < heap.min`; skip
//! insertion in that case. That is what actually saves hash-table memory
//! and CPU on long-tail-dominant queries like ClickBench Q19.
//!
//! [`GroupedTopKAggregateStream`]: crate::aggregates::grouped_topk_stream::GroupedTopKAggregateStream

use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow::array::{Array, ArrayRef, Int64Array, RecordBatch};
use arrow::compute::{SortOptions, concat, sort_to_indices, take};
use arrow::datatypes::SchemaRef;
use datafusion_common::{Result, internal_datafusion_err};
use datafusion_execution::TaskContext;
use futures::stream::{Stream, StreamExt};

use crate::aggregates::AggregateExec;
use crate::aggregates::grouped_hash_stream::GroupedHashAggregateStream;
use crate::metrics::BaselineMetrics;
use crate::stream::EmptyRecordBatchStream;
use crate::{RecordBatchStream, SendableRecordBatchStream};

/// A hash aggregate + top-K row buffer over its output.
///
/// Wraps [`GroupedHashAggregateStream`] so all correctness-critical hash-
/// aggregate machinery (spilling, memory tracking, ordering) is inherited,
/// and layers on a sort-by-count reduction that never buffers more than `K`
/// rows internally.
pub struct GroupedTopKCountAggregateStream {
    inner: SendableRecordBatchStream,
    /// K rows to emit
    limit: usize,
    /// true = ORDER BY count(*) DESC; false = ASC
    descending: bool,
    /// Index of the `count` column in the aggregate's output schema.
    /// Group keys occupy `0..count_col_idx`.
    count_col_idx: usize,
    /// Aggregate output schema (same as `inner`'s schema).
    schema: SchemaRef,
    /// At most `limit` rows currently retained as the running top-K.
    /// `None` before we've seen any input, empty otherwise means the inner
    /// produced no rows.
    accumulator: Option<RecordBatch>,
    baseline_metrics: BaselineMetrics,
    emitted: bool,
    input_done: bool,
}

impl GroupedTopKCountAggregateStream {
    pub fn new(
        agg: &AggregateExec,
        context: &Arc<TaskContext>,
        partition: usize,
        limit: usize,
        descending: bool,
    ) -> Result<Self> {
        // Count aggregate's output field lives immediately after the group
        // keys. `group_by.expr.len()` gives the count of group-key columns.
        let count_col_idx = agg.group_by.expr.len();
        let schema = Arc::clone(&agg.schema);
        let baseline_metrics = BaselineMetrics::new(&agg.metrics, partition);
        // Delegate full hash aggregation to the standard stream. This
        // preserves correctness for the additive `count` semantics: every
        // group ends up in the hash table with its final count.
        let inner = Box::pin(GroupedHashAggregateStream::new(agg, context, partition)?)
            as SendableRecordBatchStream;
        Ok(Self {
            inner,
            limit,
            descending,
            count_col_idx,
            schema,
            accumulator: None,
            baseline_metrics,
            emitted: false,
            input_done: false,
        })
    }

    /// Consume one batch from `inner`; merge with the running top-K
    /// accumulator and keep at most `limit` rows.
    fn ingest(&mut self, batch: RecordBatch) -> Result<()> {
        if batch.num_rows() == 0 {
            return Ok(());
        }
        let combined = match self.accumulator.take() {
            Some(acc) if acc.num_rows() > 0 => self.concat_batches(&[&acc, &batch])?,
            _ => batch,
        };
        self.accumulator = Some(self.select_top_k(combined)?);
        Ok(())
    }

    fn concat_batches(&self, batches: &[&RecordBatch]) -> Result<RecordBatch> {
        let num_cols = self.schema.fields().len();
        let mut cols: Vec<ArrayRef> = Vec::with_capacity(num_cols);
        for col in 0..num_cols {
            let arrays: Vec<&dyn Array> =
                batches.iter().map(|b| b.column(col).as_ref()).collect();
            cols.push(concat(&arrays)?);
        }
        Ok(RecordBatch::try_new(Arc::clone(&self.schema), cols)?)
    }

    /// Sort `batch` by count column in the requested direction and keep the
    /// first `limit` rows. Uses [`sort_to_indices`] with a limit so the sort
    /// itself is `O(N log K)` rather than `O(N log N)`.
    fn select_top_k(&self, batch: RecordBatch) -> Result<RecordBatch> {
        if batch.num_rows() <= self.limit {
            return Ok(batch);
        }
        let counts = batch.column(self.count_col_idx);
        // NULL counts sort last so they only end up in the emitted set when
        // there aren't `limit` non-NULL rows to fill it. Matches SortExec's
        // default `NULLS LAST for ASC` / `NULLS FIRST for DESC` — this is
        // conservative (drops NULL groups first).
        let opts = SortOptions {
            descending: self.descending,
            nulls_first: false,
        };
        let indices = sort_to_indices(counts, Some(opts), Some(self.limit))?;
        let num_cols = self.schema.fields().len();
        let mut cols: Vec<ArrayRef> = Vec::with_capacity(num_cols);
        for col in 0..num_cols {
            cols.push(take(batch.column(col).as_ref(), &indices, None)?);
        }
        Ok(RecordBatch::try_new(Arc::clone(&self.schema), cols)?)
    }

    /// Sort the final accumulator in the requested direction. `select_top_k`
    /// only guarantees the first `limit` positions contain the top-K rows
    /// (any order among them). The caller expects an ordered output.
    fn build_output(&mut self) -> Result<RecordBatch> {
        let acc = match self.accumulator.take() {
            Some(a) => a,
            None => return Ok(RecordBatch::new_empty(Arc::clone(&self.schema))),
        };
        if acc.num_rows() == 0 {
            return Ok(acc);
        }
        let opts = SortOptions {
            descending: self.descending,
            nulls_first: false,
        };
        let indices = sort_to_indices(acc.column(self.count_col_idx), Some(opts), None)?;
        // Debug-only sanity check: count column must be Int64 as `count(*)`
        // always produces `Int64` in DataFusion. Bail if we ever encounter a
        // mismatch — we should have failed at optimizer time.
        debug_assert!(
            acc.column(self.count_col_idx)
                .as_any()
                .downcast_ref::<Int64Array>()
                .is_some(),
            "top-K count column at index {} is not Int64",
            self.count_col_idx,
        );
        if acc
            .column(self.count_col_idx)
            .as_any()
            .downcast_ref::<Int64Array>()
            .is_none()
        {
            return Err(internal_datafusion_err!(
                "GroupedTopKCountAggregateStream expects the count column at \
                 index {} to be Int64",
                self.count_col_idx
            ));
        }
        let num_cols = self.schema.fields().len();
        let mut cols: Vec<ArrayRef> = Vec::with_capacity(num_cols);
        for col in 0..num_cols {
            cols.push(take(acc.column(col).as_ref(), &indices, None)?);
        }
        Ok(RecordBatch::try_new(Arc::clone(&self.schema), cols)?)
    }
}

impl RecordBatchStream for GroupedTopKCountAggregateStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

impl Stream for GroupedTopKCountAggregateStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        if self.emitted {
            return Poll::Ready(None);
        }
        let elapsed = self.baseline_metrics.elapsed_compute().clone();
        while !self.input_done {
            match self.inner.poll_next_unpin(cx) {
                Poll::Ready(Some(Ok(batch))) => {
                    let _timer = elapsed.timer();
                    self.ingest(batch)?;
                }
                Poll::Ready(Some(Err(e))) => return Poll::Ready(Some(Err(e))),
                Poll::Ready(None) => {
                    self.input_done = true;
                    let schema = self.inner.schema();
                    self.inner = Box::pin(EmptyRecordBatchStream::new(schema));
                }
                Poll::Pending => return Poll::Pending,
            }
        }

        let _timer = elapsed.timer();
        let out = self.build_output()?;
        self.emitted = true;
        if out.num_rows() == 0 {
            return Poll::Ready(None);
        }
        Poll::Ready(Some(Ok(out)))
    }
}
