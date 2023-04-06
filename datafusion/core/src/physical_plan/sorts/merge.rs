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

use crate::common::Result;
use crate::physical_plan::metrics::MemTrackingMetrics;
use crate::physical_plan::sorts::builder::BatchBuilder;
use crate::physical_plan::sorts::cursor::Cursor;
use crate::physical_plan::sorts::stream::{PartitionedStream, SortKeyCursorStream};
use crate::physical_plan::{
    PhysicalSortExpr, RecordBatchStream, SendableRecordBatchStream,
};
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use futures::Stream;
use std::pin::Pin;
use std::task::{ready, Context, Poll};

/// Perform a streaming merge of [`SendableRecordBatchStream`]
pub(crate) fn streaming_merge(
    streams: Vec<SendableRecordBatchStream>,
    schema: SchemaRef,
    expressions: &[PhysicalSortExpr],
    tracking_metrics: MemTrackingMetrics,
    batch_size: usize,
) -> Result<SendableRecordBatchStream> {
    let streams = SortKeyCursorStream::try_new(schema.as_ref(), expressions, streams)?;

    Ok(Box::pin(SortPreservingMergeStream::new(
        Box::new(streams),
        schema,
        tracking_metrics,
        batch_size,
    )))
}

/// A fallible [`PartitionedStream`] of [`Cursor`] and [`RecordBatch`]
type CursorStream<C> = Box<dyn PartitionedStream<Output = Result<(C, RecordBatch)>>>;

#[derive(Debug)]
struct SortPreservingMergeStream<C> {
    in_progress: BatchBuilder,

    /// The sorted input streams to merge together
    streams: CursorStream<C>,

    /// used to record execution metrics
    tracking_metrics: MemTrackingMetrics,

    /// If the stream has encountered an error
    aborted: bool,

    /// A loser tree that always produces the minimum cursor
    ///
    /// Node 0 stores the top winner, Nodes 1..num_streams store
    /// the loser nodes
    ///
    /// This implements a "Tournament Tree" (aka Loser Tree) to keep
    /// track of the current smallest element at the top. When the top
    /// record is taken, the tree structure is not modified, and only
    /// the path from bottom to top is visited, keeping the number of
    /// comparisons close to the theoretical limit of `log(S)`.
    ///
    /// reference: <https://en.wikipedia.org/wiki/K-way_merge_algorithm#Tournament_Tree>
    loser_tree: Vec<usize>,

    /// If the most recently yielded overall winner has been replaced
    /// within the loser tree. A value of `false` indicates that the
    /// overall winner has been yielded but the loser tree has not
    /// been updated
    loser_tree_adjusted: bool,

    /// target batch size
    batch_size: usize,

    /// Vector that holds cursors for each non-exhausted input partition
    cursors: Vec<Option<C>>,
}

impl<C: Cursor> SortPreservingMergeStream<C> {
    fn new(
        streams: CursorStream<C>,
        schema: SchemaRef,
        tracking_metrics: MemTrackingMetrics,
        batch_size: usize,
    ) -> Self {
        let stream_count = streams.partitions();

        Self {
            in_progress: BatchBuilder::new(schema, stream_count, batch_size),
            streams,
            tracking_metrics,
            aborted: false,
            cursors: (0..stream_count).map(|_| None).collect(),
            loser_tree: vec![],
            loser_tree_adjusted: false,
            batch_size,
        }
    }

    /// If the stream at the given index is not exhausted, and the last cursor for the
    /// stream is finished, poll the stream for the next RecordBatch and create a new
    /// cursor for the stream from the returned result
    fn maybe_poll_stream(
        &mut self,
        cx: &mut Context<'_>,
        idx: usize,
    ) -> Poll<Result<()>> {
        if self.cursors[idx]
            .as_ref()
            .map(|cursor| !cursor.is_finished())
            .unwrap_or(false)
        {
            // Cursor is not finished - don't need a new RecordBatch yet
            return Poll::Ready(Ok(()));
        }

        match futures::ready!(self.streams.poll_next(cx, idx)) {
            None => Poll::Ready(Ok(())),
            Some(Err(e)) => Poll::Ready(Err(e)),
            Some(Ok((cursor, batch))) => {
                self.cursors[idx] = Some(cursor);
                self.in_progress.push_batch(idx, batch);
                Poll::Ready(Ok(()))
            }
        }
    }

    fn poll_next_inner(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<RecordBatch>>> {
        if self.aborted {
            return Poll::Ready(None);
        }
        // try to initialize the loser tree
        if self.loser_tree.is_empty() {
            // Ensure all non-exhausted streams have a cursor from which
            // rows can be pulled
            for i in 0..self.streams.partitions() {
                if let Err(e) = ready!(self.maybe_poll_stream(cx, i)) {
                    self.aborted = true;
                    return Poll::Ready(Some(Err(e)));
                }
            }
            self.init_loser_tree();
        }

        // NB timer records time taken on drop, so there are no
        // calls to `timer.done()` below.
        let elapsed_compute = self.tracking_metrics.elapsed_compute().clone();
        let _timer = elapsed_compute.timer();

        loop {
            // Adjust the loser tree if necessary, returning control if needed
            if !self.loser_tree_adjusted {
                let winner = self.loser_tree[0];
                if let Err(e) = ready!(self.maybe_poll_stream(cx, winner)) {
                    self.aborted = true;
                    return Poll::Ready(Some(Err(e)));
                }
                self.update_loser_tree();
            }

            let stream_idx = self.loser_tree[0];
            let cursor = self.cursors[stream_idx].as_mut();
            if let Some(_) = cursor.and_then(Cursor::advance) {
                self.loser_tree_adjusted = false;
                self.in_progress.push_row(stream_idx);
                if self.in_progress.len() < self.batch_size {
                    continue;
                }
            }

            return Poll::Ready(self.in_progress.build_record_batch().transpose());
        }
    }

    /// Returns `true` if the cursor at index `a` is greater than at index `b`
    #[inline]
    fn is_gt(&self, a: usize, b: usize) -> bool {
        match (&self.cursors[a], &self.cursors[b]) {
            (None, _) => true,
            (_, None) => false,
            (Some(a), Some(b)) => b < a,
        }
    }

    /// Attempts to initialize the loser tree with one value from each
    /// non exhausted input, if possible
    fn init_loser_tree(&mut self) {
        // Init loser tree
        self.loser_tree = vec![usize::MAX; self.cursors.len()];
        for i in 0..self.cursors.len() {
            let mut winner = i;
            let mut cmp_node = (self.cursors.len() + i) / 2;
            while cmp_node != 0 && self.loser_tree[cmp_node] != usize::MAX {
                let challenger = self.loser_tree[cmp_node];
                if self.is_gt(winner, challenger) {
                    self.loser_tree[cmp_node] = winner;
                    winner = challenger;
                }

                cmp_node /= 2;
            }
            self.loser_tree[cmp_node] = winner;
        }
        self.loser_tree_adjusted = true;
    }

    /// Attempts to update the loser tree, following winner replacement, if possible
    fn update_loser_tree(&mut self) {
        let mut winner = self.loser_tree[0];
        // Replace overall winner by walking tree of losers
        let mut cmp_node = (self.cursors.len() + winner) / 2;
        while cmp_node != 0 {
            let challenger = self.loser_tree[cmp_node];
            if self.is_gt(winner, challenger) {
                self.loser_tree[cmp_node] = winner;
                winner = challenger;
            }
            cmp_node /= 2;
        }
        self.loser_tree[0] = winner;
        self.loser_tree_adjusted = true;
    }
}

impl<C: Cursor + Unpin> Stream for SortPreservingMergeStream<C> {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let poll = self.poll_next_inner(cx);
        self.tracking_metrics.record_poll(poll)
    }
}

impl<C: Cursor + Unpin> RecordBatchStream for SortPreservingMergeStream<C> {
    fn schema(&self) -> SchemaRef {
        self.in_progress.schema().clone()
    }
}
