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

//! Merge that deals with an arbitrary size of streaming inputs.
//! This is an order-preserving merge.

use std::pin::Pin;
use std::sync::Arc;
use std::task::{ready, Context, Poll};

use crate::metrics::BaselineMetrics;
use crate::sorts::builder::BatchBuilder;
use crate::sorts::cursor::{Cursor, CursorValues};
use crate::sorts::stream::PartitionedStream;
use crate::RecordBatchStream;

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion_common::Result;
use datafusion_execution::memory_pool::MemoryReservation;

use futures::Stream;

/// A fallible [`PartitionedStream`] of [`Cursor`] and [`RecordBatch`]
type CursorStream<C> = Box<dyn PartitionedStream<Output = Result<(C, RecordBatch)>>>;

/// Merges a stream of sorted cursors and record batches into a single sorted stream
#[derive(Debug)]
pub(crate) struct SortPreservingMergeStream<C: CursorValues> {
    in_progress: BatchBuilder,

    /// The sorted input streams to merge together
    streams: CursorStream<C>,

    /// used to record execution metrics
    metrics: BaselineMetrics,

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
    /// The current implementation uses a vector to store the tree.
    /// Conceptually, it looks like this (assuming 8 streams):
    ///
    /// ```text
    ///     0 (winner)
    ///
    ///     1
    ///    / \
    ///   2   3
    ///  / \ / \
    /// 4  5 6  7
    /// ```
    ///
    /// Where element at index 0 in the vector is the current winner. Element
    /// at index 1 is the root of the loser tree, element at index 2 is the
    /// left child of the root, and element at index 3 is the right child of
    /// the root and so on.
    ///
    /// reference: <https://en.wikipedia.org/wiki/K-way_merge_algorithm#Tournament_Tree>
    loser_tree: Vec<usize>,

    /// If the most recently yielded overall winner has been replaced
    /// within the loser tree. A value of `false` indicates that the
    /// overall winner has been yielded but the loser tree has not
    /// been updated
    loser_tree_adjusted: bool,

    /// Target batch size
    batch_size: usize,

    /// Cursors for each input partition. `None` means the input is exhausted
    cursors: Vec<Option<Cursor<C>>>,

    /// Configuration parameter to enable round-robin selection of tied winners of loser tree.
    ///
    /// This option controls the tie-breaker strategy and attempts to avoid the
    /// issue of unbalanced polling between partitions
    ///
    /// If `true`, when multiple partitions have the same value, the partition
    /// that has the fewest poll counts is selected. This strategy ensures that
    /// multiple partitions with the same value are chosen equally, distributing
    /// the polling load in a round-robin fashion. This approach balances the
    /// workload more effectively across partitions and avoids excessive buffer
    /// growth.
    ///
    /// if `false`, partitions with smaller indices are consistently chosen as
    /// the winners, which can lead to an uneven distribution of polling and potentially
    /// causing upstream operator buffers for the other partitions to grow
    /// excessively, as they continued receiving data without consuming it.
    ///
    /// For example, an upstream operator like `RepartitionExec` execution would
    /// keep sending data to certain partitions, but those partitions wouldn't
    /// consume the data if they weren't selected as winners. This resulted in
    /// inefficient buffer usage.
    enable_round_robin_tie_breaker: bool,

    /// Flag indicating whether we are in the mode of round-robin
    /// tie breaker for the loser tree winners.
    round_robin_tie_breaker_mode: bool,

    /// Total number of polls returning the same value, as per partition.
    /// We select the one that has less poll counts for tie-breaker in loser tree.
    num_of_polled_with_same_value: Vec<usize>,

    /// To keep track of reset counts
    poll_reset_epochs: Vec<usize>,

    /// Current reset count
    current_reset_epoch: usize,

    /// Stores the previous value of each partitions for tracking the poll counts on the same value.
    prev_cursors: Vec<Option<Cursor<C>>>,

    /// Optional number of rows to fetch
    fetch: Option<usize>,

    /// number of rows produced
    produced: usize,

    /// This vector contains the indices of the partitions that have not started emitting yet.
    uninitiated_partitions: Vec<usize>,
}

impl<C: CursorValues> SortPreservingMergeStream<C> {
    pub(crate) fn new(
        streams: CursorStream<C>,
        schema: SchemaRef,
        metrics: BaselineMetrics,
        batch_size: usize,
        fetch: Option<usize>,
        reservation: MemoryReservation,
        enable_round_robin_tie_breaker: bool,
    ) -> Self {
        let stream_count = streams.partitions();

        Self {
            in_progress: BatchBuilder::new(schema, stream_count, batch_size, reservation),
            streams,
            metrics,
            aborted: false,
            cursors: (0..stream_count).map(|_| None).collect(),
            prev_cursors: (0..stream_count).map(|_| None).collect(),
            round_robin_tie_breaker_mode: false,
            num_of_polled_with_same_value: vec![0; stream_count],
            current_reset_epoch: 0,
            poll_reset_epochs: vec![0; stream_count],
            loser_tree: vec![],
            loser_tree_adjusted: false,
            batch_size,
            fetch,
            produced: 0,
            uninitiated_partitions: (0..stream_count).collect(),
            enable_round_robin_tie_breaker,
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
        if self.cursors[idx].is_some() {
            // Cursor is not finished - don't need a new RecordBatch yet
            return Poll::Ready(Ok(()));
        }

        match futures::ready!(self.streams.poll_next(cx, idx)) {
            None => Poll::Ready(Ok(())),
            Some(Err(e)) => Poll::Ready(Err(e)),
            Some(Ok((cursor, batch))) => {
                self.cursors[idx] = Some(Cursor::new(cursor));
                Poll::Ready(self.in_progress.push_batch(idx, batch))
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
        // Once all partitions have set their corresponding cursors for the loser tree,
        // we skip the following block. Until then, this function may be called multiple
        // times and can return Poll::Pending if any partition returns Poll::Pending.

        if self.loser_tree.is_empty() {
            // Manual indexing since we're iterating over the vector and shrinking it in the loop
            let mut idx = 0;
            while idx < self.uninitiated_partitions.len() {
                let partition_idx = self.uninitiated_partitions[idx];
                match self.maybe_poll_stream(cx, partition_idx) {
                    Poll::Ready(Err(e)) => {
                        self.aborted = true;
                        return Poll::Ready(Some(Err(e)));
                    }
                    Poll::Pending => {
                        // The polled stream is pending which means we're already set up to
                        // be woken when necessary
                        // Try the next stream
                        idx += 1;
                    }
                    _ => {
                        // The polled stream is ready
                        // Remove it from uninitiated_partitions
                        // Don't bump idx here, since a new element will have taken its
                        // place which we'll try in the next loop iteration
                        // swap_remove will change the partition poll order, but that shouldn't
                        // make a difference since we're waiting for all streams to be ready.
                        self.uninitiated_partitions.swap_remove(idx);
                    }
                }
            }

            if self.uninitiated_partitions.is_empty() {
                // If there are no more uninitiated partitions, set up the loser tree and continue
                // to the next phase.

                // Claim the memory for the uninitiated partitions
                self.uninitiated_partitions.shrink_to_fit();
                self.init_loser_tree();
            } else {
                // There are still uninitiated partitions so return pending.
                // We only get here if we've polled all uninitiated streams and at least one of them
                // returned pending itself. That means we will be woken as soon as one of the
                // streams would like to be polled again.
                // There is no need to reschedule ourselves eagerly.
                return Poll::Pending;
            }
        }

        // NB timer records time taken on drop, so there are no
        // calls to `timer.done()` below.
        let elapsed_compute = self.metrics.elapsed_compute().clone();
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
            if self.advance_cursors(stream_idx) {
                self.loser_tree_adjusted = false;
                self.in_progress.push_row(stream_idx);

                // stop sorting if fetch has been reached
                if self.fetch_reached() {
                    self.aborted = true;
                } else if self.in_progress.len() < self.batch_size {
                    continue;
                }
            }

            self.produced += self.in_progress.len();

            return Poll::Ready(self.in_progress.build_record_batch().transpose());
        }
    }

    /// For the given partition, updates the poll count. If the current value is the same
    /// of the previous value, it increases the count by 1; otherwise, it is reset as 0.
    fn update_poll_count_on_the_same_value(&mut self, partition_idx: usize) {
        let cursor = &mut self.cursors[partition_idx];

        // Check if the current partition's poll count is logically "reset"
        if self.poll_reset_epochs[partition_idx] != self.current_reset_epoch {
            self.poll_reset_epochs[partition_idx] = self.current_reset_epoch;
            self.num_of_polled_with_same_value[partition_idx] = 0;
        }

        if let Some(c) = cursor.as_mut() {
            // Compare with the last row in the previous batch
            let prev_cursor = &self.prev_cursors[partition_idx];
            if c.is_eq_to_prev_one(prev_cursor.as_ref()) {
                self.num_of_polled_with_same_value[partition_idx] += 1;
            } else {
                self.num_of_polled_with_same_value[partition_idx] = 0;
            }
        }
    }

    fn fetch_reached(&mut self) -> bool {
        self.fetch
            .map(|fetch| self.produced + self.in_progress.len() >= fetch)
            .unwrap_or(false)
    }

    /// Advances the actual cursor. If it reaches its end, update the
    /// previous cursor with it.
    ///
    /// If the given partition is not exhausted, the function returns `true`.
    fn advance_cursors(&mut self, stream_idx: usize) -> bool {
        if let Some(cursor) = &mut self.cursors[stream_idx] {
            let _ = cursor.advance();
            if cursor.is_finished() {
                // Take the current cursor, leaving `None` in its place
                self.prev_cursors[stream_idx] = self.cursors[stream_idx].take();
            }
            true
        } else {
            false
        }
    }

    /// Returns `true` if the cursor at index `a` is greater than at index `b`.
    /// In an equality case, it compares the partition indices given.
    #[inline]
    fn is_gt(&self, a: usize, b: usize) -> bool {
        match (&self.cursors[a], &self.cursors[b]) {
            (None, _) => true,
            (_, None) => false,
            (Some(ac), Some(bc)) => ac.cmp(bc).then_with(|| a.cmp(&b)).is_gt(),
        }
    }

    #[inline]
    fn is_poll_count_gt(&self, a: usize, b: usize) -> bool {
        let poll_a = self.num_of_polled_with_same_value[a];
        let poll_b = self.num_of_polled_with_same_value[b];
        poll_a.cmp(&poll_b).then_with(|| a.cmp(&b)).is_gt()
    }

    #[inline]
    fn update_winner(&mut self, cmp_node: usize, winner: &mut usize, challenger: usize) {
        self.loser_tree[cmp_node] = *winner;
        *winner = challenger;
    }

    /// Find the leaf node index in the loser tree for the given cursor index
    ///
    /// Note that this is not necessarily a leaf node in the tree, but it can
    /// also be a half-node (a node with only one child). This happens when the
    /// number of cursors/streams is not a power of two. Thus, the loser tree
    /// will be unbalanced, but it will still work correctly.
    ///
    /// For example, with 5 streams, the loser tree will look like this:
    ///
    /// ```text
    ///           0 (winner)
    ///
    ///           1
    ///        /     \
    ///       2       3
    ///     /  \     / \
    ///    4    |   |   |
    ///   / \   |   |   |
    /// -+---+--+---+---+---- Below is not a part of loser tree
    ///  S3 S4 S0   S1  S2
    /// ```
    ///
    /// S0, S1, ... S4 are the streams (read: stream at index 0, stream at
    /// index 1, etc.)
    ///
    /// Zooming in at node 2 in the loser tree as an example, we can see that
    /// it takes as input the next item at (S0) and the loser of (S3, S4).
    ///
    #[inline]
    fn lt_leaf_node_index(&self, cursor_index: usize) -> usize {
        (self.cursors.len() + cursor_index) / 2
    }

    /// Find the parent node index for the given node index
    #[inline]
    fn lt_parent_node_index(&self, node_idx: usize) -> usize {
        node_idx / 2
    }

    /// Attempts to initialize the loser tree with one value from each
    /// non exhausted input, if possible
    fn init_loser_tree(&mut self) {
        // Init loser tree
        self.loser_tree = vec![usize::MAX; self.cursors.len()];
        for i in 0..self.cursors.len() {
            let mut winner = i;
            let mut cmp_node = self.lt_leaf_node_index(i);
            while cmp_node != 0 && self.loser_tree[cmp_node] != usize::MAX {
                let challenger = self.loser_tree[cmp_node];
                if self.is_gt(winner, challenger) {
                    self.loser_tree[cmp_node] = winner;
                    winner = challenger;
                }

                cmp_node = self.lt_parent_node_index(cmp_node);
            }
            self.loser_tree[cmp_node] = winner;
        }
        self.loser_tree_adjusted = true;
    }

    /// Resets the poll count by incrementing the reset epoch.
    fn reset_poll_counts(&mut self) {
        self.current_reset_epoch += 1;
    }

    /// Handles tie-breaking logic during the adjustment of the loser tree.
    ///
    /// When comparing elements from multiple partitions in the `update_loser_tree` process, a tie can occur
    /// between the current winner and a challenger. This function is invoked when such a tie needs to be
    /// resolved according to the round-robin tie-breaker mode.
    ///
    /// If round-robin tie-breaking is not active, it is enabled, and the poll counts for all elements are reset.
    /// The function then compares the poll counts of the current winner and the challenger:
    /// - If the winner remains at the top after the final comparison, it increments the winner's poll count.
    /// - If the challenger has a lower poll count than the current winner, the challenger becomes the new winner.
    /// - If the poll counts are equal but the challenger's index is smaller, the challenger is preferred.
    ///
    /// # Parameters
    /// - `cmp_node`: The index of the comparison node in the loser tree where the tie-breaking is happening.
    /// - `winner`: A mutable reference to the current winner, which may be updated based on the tie-breaking result.
    /// - `challenger`: The index of the challenger being compared against the winner.
    ///
    /// This function ensures fair selection among elements with equal values when tie-breaking mode is enabled,
    /// aiming to balance the polling across different partitions.
    #[inline]
    fn handle_tie(&mut self, cmp_node: usize, winner: &mut usize, challenger: usize) {
        if !self.round_robin_tie_breaker_mode {
            self.round_robin_tie_breaker_mode = true;
            // Reset poll count for tie-breaker
            self.reset_poll_counts();
        }
        // Update poll count if the winner survives in the final match
        if *winner == self.loser_tree[0] {
            self.update_poll_count_on_the_same_value(*winner);
            if self.is_poll_count_gt(*winner, challenger) {
                self.update_winner(cmp_node, winner, challenger);
            }
        } else if challenger < *winner {
            // If the winner doesn’t survive in the final match, it indicates that the original winner
            // has moved up in value, so the challenger now becomes the new winner.
            // This also means that we’re in a new round of the tie breaker,
            // and the polls count is outdated (though not yet cleaned up).
            //
            // By the time we reach this code, both the new winner and the current challenger
            // have the same value, and neither has an updated polls count.
            // Therefore, we simply select the one with the smaller index.
            self.update_winner(cmp_node, winner, challenger);
        }
    }

    /// Updates the loser tree to reflect the new winner after the previous winner is consumed.
    /// This function adjusts the tree by comparing the current winner with challengers from
    /// other partitions.
    ///
    /// If `enable_round_robin_tie_breaker` is true and a tie occurs at the final level, the
    /// tie-breaker logic will be applied to ensure fair selection among equal elements.
    fn update_loser_tree(&mut self) {
        // Start with the current winner
        let mut winner = self.loser_tree[0];

        // Find the leaf node index of the winner in the loser tree.
        let mut cmp_node = self.lt_leaf_node_index(winner);

        // Traverse up the tree to adjust comparisons until reaching the root.
        while cmp_node != 0 {
            let challenger = self.loser_tree[cmp_node];
            // If round-robin tie-breaker is enabled and we're at the final comparison (cmp_node == 1)
            if self.enable_round_robin_tie_breaker && cmp_node == 1 {
                match (&self.cursors[winner], &self.cursors[challenger]) {
                    (Some(ac), Some(bc)) => {
                        if ac == bc {
                            self.handle_tie(cmp_node, &mut winner, challenger);
                        } else {
                            // Ends of tie breaker
                            self.round_robin_tie_breaker_mode = false;
                            if ac > bc {
                                self.update_winner(cmp_node, &mut winner, challenger);
                            }
                        }
                    }
                    (None, _) => {
                        // Challenger wins, update winner
                        // Ends of tie breaker
                        self.round_robin_tie_breaker_mode = false;
                        self.update_winner(cmp_node, &mut winner, challenger);
                    }
                    (_, None) => {
                        // Winner wins again
                        // Ends of tie breaker
                        self.round_robin_tie_breaker_mode = false;
                    }
                }
            } else if self.is_gt(winner, challenger) {
                self.update_winner(cmp_node, &mut winner, challenger);
            }
            cmp_node = self.lt_parent_node_index(cmp_node);
        }
        self.loser_tree[0] = winner;
        self.loser_tree_adjusted = true;
    }
}

impl<C: CursorValues + Unpin> Stream for SortPreservingMergeStream<C> {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let poll = self.poll_next_inner(cx);
        self.metrics.record_poll(poll)
    }
}

impl<C: CursorValues + Unpin> RecordBatchStream for SortPreservingMergeStream<C> {
    fn schema(&self) -> SchemaRef {
        Arc::clone(self.in_progress.schema())
    }
}
