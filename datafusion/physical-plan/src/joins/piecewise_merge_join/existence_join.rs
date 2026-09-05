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

//! PiecewiseMergeJoin stream specialized for existence joins.
//!
//! Instantiated by [`PiecewiseMergeJoinExec`] when the join type is `LeftSemi` or `LeftAnti`.
//! The other existence joins are rejected in `PiecewiseMergeJoinExec::try_new`:
//! `RightSemi`/`RightAnti`/`RightMark` mark the right side and so need an input swap, while
//! `LeftMark` marks the left side but needs an extra boolean column rather than a slice.
//!
//! # Motivation
//!
//! `ClassicPWMJStream` (see `classic_join.rs`) materializes `(buffered, streamed)` row
//! pairs: on the first matching buffered row it emits the whole matching suffix joined
//! with the current streamed row. Existence joins only need a boolean per buffered row —
//! does any match exist? — so pair materialization is pure waste, and the resume state it
//! requires (partial output batches, per-row buffered/streamed cursors) is dead weight.
//!
//! This stream instead records matches as a single index -- the start of the matched
//! suffix -- and emits nothing at all while scanning. Output is produced once, at the end,
//! by slicing the buffered batch at that index.
//!
//! # Algorithm
//!
//! The buffered (left) side arrives globally sorted, enforced by `required_input_ordering`
//! (the streamed side carries no ordering requirement). For a streamed key, a binary search
//! finds the first matching buffered row; because the buffered side is sorted, every buffered
//! row from that position onward matches too:
//!
//! ```text
//!   buffered (sorted):  [1, 3, 5, 7]
//!                           ▲
//!                           first match for this streamed row
//!                           → mark [1..4), i.e. buffered 3, 5, 7
//! ```
//!
//! Marking that suffix covers every match the batch can produce, so only one row of the
//! batch is ever compared against the buffered side: the extreme key, which reaches the
//! smallest matching `buffer_idx`, while every other row matches a subset of that suffix.
//!
//! The search stops at `min_marked`: rows from there on were already marked, by this
//! partition or another, so a match found among them would record nothing. Marking is then a
//! single `fetch_min`, and finding the first match is a binary search, so a batch costs
//! `O(log buffered)` rather than `O(buffered)`.
//!
//! `min_marked` lives in the shared buffered data, so partitions benefit from each other's
//! marking rather than each rediscovering it. It only ever decreases, so a partition that
//! reads a stale value scans a wider range than it had to -- never a narrower one.
//!
//! Once `min_marked` reaches the first non-null buffered row nothing can ever be marked
//! again, and every partition stops reading the streamed side -- checked before each poll,
//! so a partition that starts late reads nothing at all.
//!
//! Rows whose join key is NULL never satisfy a comparison predicate. Buffered NULLs sort to
//! the front, so the scan starts past them and null-keyed buffered rows are left unmarked —
//! correctly excluded from `LeftSemi` and included in `LeftAnti`. The extreme key is picked
//! from each streamed batch with NULLs ignored, so it is non-null unless the whole batch is.
//!
//! # Output
//!
//! Marking only ever covers a suffix, and each mark lowers the watermark to its own start,
//! so the matched set is always exactly `[min_marked, buffered_len)`. A bitmap would be a
//! less compact encoding of that one index, so none is allocated. `ClassicPWMJStream` marks
//! the same way, which is why the watermark lives in `BufferedSideData` rather than here.
//!
//! Once every streamed partition has been consumed, the last one to finish slices the
//! buffered batch: `LeftSemi` takes `[min_marked, len)`, `LeftAnti` the complementary
//! prefix `[0, min_marked)`, which is where the null-keyed rows live. Only the buffered
//! (left) columns are produced.
//!
//! [`PiecewiseMergeJoinExec`]: super::PiecewiseMergeJoinExec

use std::cmp::Ordering;
use std::sync::Arc;
use std::sync::atomic::Ordering as AtomicOrdering;
use std::task::{Poll, ready};

use arrow::array::{Array, ArrayRef, RecordBatch};
use arrow::compute::BatchCoalescer;
use arrow_schema::{SchemaRef, SortOptions};
use datafusion_common::{NullEquality, Result, internal_err};
use datafusion_execution::{RecordBatchStream, SendableRecordBatchStream};
use datafusion_expr::{JoinType, Operator};
use datafusion_functions_aggregate_common::min_max::{max_batch, min_batch};
use datafusion_physical_expr::PhysicalExprRef;
use futures::{Stream, StreamExt};

use crate::handle_state;
use crate::joins::piecewise_merge_join::exec::{BufferedSide, BufferedSideReadyState};
use crate::joins::utils::{
    BuildProbeJoinMetrics, JoinKeyComparator, StatefulStreamResult,
};
use crate::stream::EmptyRecordBatchStream;

pub(super) enum ExistencePWMJStreamState {
    /// Load the buffered side into memory.
    WaitBufferedSide,
    /// Fetch and scan streamed batches, lowering the watermark. Emits nothing.
    ScanStreamBatches,
    /// Emit the result. Reached only by the last streamed partition.
    EmitMatched,
    Completed,
}

pub(super) struct ExistencePWMJStream {
    /// Output schema, which for `LeftSemi`/`LeftAnti` is the buffered side's schema
    schema: SchemaRef,
    /// Physical expression evaluated on the streamed side. The buffered side's
    /// equivalent is already evaluated when the buffered side is collected.
    on_streamed: PhysicalExprRef,
    /// `LeftSemi` or `LeftAnti`
    join_type: JoinType,
    /// Comparison operator
    operator: Operator,
    streamed: SendableRecordBatchStream,
    buffered_side: BufferedSide,
    state: ExistencePWMJStreamState,
    /// Whether the buffered side is sorted ascending or descending, per the operator
    sort_option: SortOptions,
    join_metrics: BuildProbeJoinMetrics,
    /// Chunks the single final-pass batch to `batch_size`
    output_batches: Box<BatchCoalescer>,
    /// Whether the final pass has already pushed its result into `output_batches`
    emitted: bool,
}

impl ExistencePWMJStream {
    #[expect(clippy::too_many_arguments)]
    pub(super) fn try_new(
        schema: SchemaRef,
        on_streamed: PhysicalExprRef,
        join_type: JoinType,
        operator: Operator,
        streamed: SendableRecordBatchStream,
        buffered_side: BufferedSide,
        sort_option: SortOptions,
        join_metrics: BuildProbeJoinMetrics,
        batch_size: usize,
    ) -> Self {
        Self {
            output_batches: Box::new(BatchCoalescer::new(
                Arc::clone(&schema),
                batch_size,
            )),
            schema,
            on_streamed,
            join_type,
            operator,
            streamed,
            buffered_side,
            state: ExistencePWMJStreamState::WaitBufferedSide,
            sort_option,
            join_metrics,
            emitted: false,
        }
    }

    fn poll_next_impl(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Result<RecordBatch>>> {
        loop {
            return match self.state {
                ExistencePWMJStreamState::WaitBufferedSide => {
                    handle_state!(ready!(self.collect_buffered_side(cx)))
                }
                ExistencePWMJStreamState::ScanStreamBatches => {
                    handle_state!(ready!(self.scan_stream_batch(cx)))
                }
                ExistencePWMJStreamState::EmitMatched => {
                    handle_state!(self.emit_matched())
                }
                ExistencePWMJStreamState::Completed => Poll::Ready(None),
            };
        }
    }

    /// Collects the buffered side into memory.
    fn collect_buffered_side(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<StatefulStreamResult<Option<RecordBatch>>>> {
        let build_timer = self.join_metrics.build_time.timer();
        let buffered_data = ready!(
            self.buffered_side
                .try_as_initial_mut()?
                .buffered_fut
                .get_shared(cx)
        )?;
        build_timer.done();

        self.buffered_side =
            BufferedSide::Ready(BufferedSideReadyState { buffered_data });
        self.state = ExistencePWMJStreamState::ScanStreamBatches;

        Poll::Ready(Ok(StatefulStreamResult::Continue))
    }

    /// Fetches one streamed batch, reduces it to its extreme compare key, and marks the
    /// buffered rows that key matches.
    /// Never produces output: existence results come from the watermark in `emit_matched`.
    fn scan_stream_batch(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<StatefulStreamResult<Option<RecordBatch>>>> {
        // Every buffered row that can ever be marked is already marked -- by this
        // partition or any other, since the watermark is shared -- so no batch can lower
        // the watermark further. Stop reading rather than scanning batches that provably
        // cannot contribute. Checked before polling so a partition that starts after another
        // has saturated the watermark reads nothing at all.
        if self.nothing_left_to_mark()? {
            self.finish_streamed_side()?;
            return Poll::Ready(Ok(StatefulStreamResult::Continue));
        }

        let next_batch = ready!(self.streamed.poll_next_unpin(cx));
        let join_time = self.join_metrics.join_time.clone();
        let _join_timer = join_time.timer();
        match next_batch {
            None => self.finish_streamed_side()?,
            Some(Ok(batch)) => {
                let stream_values: ArrayRef = self
                    .on_streamed
                    .evaluate(&batch)?
                    .into_array(batch.num_rows())?;

                self.join_metrics.input_batches.add(1);
                self.join_metrics.input_rows.add(batch.num_rows());

                // An empty batch has no extreme key to compare, so it can neither match
                // nor miss -- counting it either way would understate the real hit rate.
                if batch.num_rows() > 0 {
                    let join_time = self.join_metrics.join_time.clone();
                    let join_timer = join_time.timer();
                    // Only the batch's extreme key is ever compared against the buffered
                    // side, so reduce the batch to that one key.
                    let stream_values =
                        extreme_key(&stream_values, self.sort_option.descending)?;

                    self.mark_matched_buffered_rows(&stream_values)?;
                    join_timer.done();
                }
            }
            Some(Err(err)) => return Poll::Ready(Err(err)),
        }

        Poll::Ready(Ok(StatefulStreamResult::Continue))
    }

    /// Whether no buffered row can ever be marked again, in which case the streamed side
    /// no longer needs reading. Reads the shared watermark, so one partition saturating it
    /// lets every other partition stop too.
    fn nothing_left_to_mark(&self) -> Result<bool> {
        let buffered_data = &self.buffered_side.try_as_ready()?.buffered_data;
        let min_marked = buffered_data.min_marked.load(AtomicOrdering::SeqCst);
        let buffered_values = buffered_data.values();

        Ok(min_marked.min(buffered_values.len()) <= buffered_values.null_count())
    }

    /// Marks this partition done with the streamed side: releases the input pipeline and,
    /// if this is the last streamed partition to finish, moves on to the final pass.
    fn finish_streamed_side(&mut self) -> Result<()> {
        // Release the streamed input pipeline's resources.
        let streamed_schema = self.streamed.schema();
        self.streamed = Box::pin(EmptyRecordBatchStream::new(streamed_schema));

        // The final pass must run exactly once, on the last streamed partition to finish.
        if self
            .buffered_side
            .try_as_ready()?
            .buffered_data
            .remaining_partitions
            .fetch_sub(1, AtomicOrdering::SeqCst)
            == 1
        {
            self.state = ExistencePWMJStreamState::EmitMatched;
        } else {
            self.state = ExistencePWMJStreamState::Completed;
        }

        Ok(())
    }

    /// Marks every buffered row matched by `stream_values`, a one-row array holding the
    /// batch's extreme compare key (null only if the whole batch was null).
    fn mark_matched_buffered_rows(&mut self, stream_values: &ArrayRef) -> Result<()> {
        let operator = self.operator;
        let sort_option = self.sort_option;
        self.join_metrics.probe_hit_rate.add_total(1);

        {
            let buffered_data = &self.buffered_side.try_as_ready()?.buffered_data;
            let buffered_values = buffered_data.values();
            let buffered_len = buffered_values.len();

            // NULL keys can never match, and `sort_options` uses `nulls_first` for every
            // operator (see `try_new`), so buffered nulls sit at the front -- skip past them.
            let first_non_null_buffered = buffered_values.null_count();

            // `[min_marked, buffered_len)` was already marked, by this partition or
            // another, so a match found there would write nothing. Stop the scan at the
            // watermark: that bounds the comparisons this batch performs, not just the
            // bits it writes.
            let scan_limit = buffered_data
                .min_marked
                .load(AtomicOrdering::SeqCst)
                .min(buffered_len);

            // The extreme key is the only one that can decide anything: it reaches the
            // smallest matching `buffer_idx`, and every other row in the batch matches a
            // subset of the same buffered suffix, so could only re-mark it. `null_count()`
            // is 0 for a real key and 1 for an all-null batch, which skips the scan.
            let row_idx = stream_values.null_count();

            // `<=`/`>=` also match on equality; validated once here rather than inside
            // the search below.
            let match_on_equal = match operator {
                Operator::Gt | Operator::Lt => false,
                Operator::GtEq | Operator::LtEq => true,
                _ => {
                    return internal_err!(
                        "PiecewiseMergeJoin should not contain operator, {}",
                        operator
                    );
                }
            };

            if row_idx < stream_values.len() && first_non_null_buffered < scan_limit {
                let cmp = JoinKeyComparator::new(
                    &[Arc::clone(stream_values)],
                    &[Arc::clone(buffered_values)],
                    &[sort_option],
                    NullEquality::NullEqualsNothing,
                )?;
                let is_match = |buffer_idx: usize| {
                    let compare = cmp.compare(row_idx, buffer_idx);
                    compare == Ordering::Less
                        || (match_on_equal && compare == Ordering::Equal)
                };

                // Because the buffered side is sorted, `is_match` is monotone over it:
                // false while the buffered key has not yet passed the streamed key, true
                // from there on. So the first match is a partition point and can be found
                // by binary search instead of a walk -- `O(log buffered)` per batch rather
                // than `O(buffered)`.
                let mut lo = first_non_null_buffered;
                let mut hi = scan_limit;
                while lo < hi {
                    let mid = lo + (hi - lo) / 2;
                    if is_match(mid) {
                        hi = mid;
                    } else {
                        lo = mid + 1;
                    }
                }

                // `lo` is now the first matching buffered index, or `scan_limit` if this
                // batch matches nothing new.
                let buffer_idx = lo;
                if buffer_idx < scan_limit {
                    self.join_metrics.probe_hit_rate.add_part(1);
                    // Everything from `buffer_idx` on matches, so lowering the
                    // watermark to it records the match: the marked set is exactly
                    // `[min_marked, buffered_len)` and needs no bitmap.
                    //
                    // INVARIANT: sound only because the buffered side and each
                    // streamed batch are sorted the same way for this operator
                    // (`try_new` derives `sort_option`: descending for `<`/`<=`,
                    // ascending for `>`/`>=`). That makes this the smallest reachable
                    // `buffer_idx`, so the marked suffix is maximal. Only the ordering
                    // *within* a batch matters; batches themselves may arrive in any
                    // order, which is why the watermark takes a `min` rather than just
                    // decreasing.
                    buffered_data
                        .min_marked
                        .fetch_min(buffer_idx, AtomicOrdering::SeqCst);
                }
            }
        }

        Ok(())
    }

    /// Emits the existence result by slicing at the watermark: the marked buffered rows for
    /// `LeftSemi`, the unmarked ones for `LeftAnti`.
    fn emit_matched(&mut self) -> Result<StatefulStreamResult<Option<RecordBatch>>> {
        let _join_timer = self.join_metrics.join_time.timer();
        if !self.emitted {
            self.emitted = true;

            let buffered_data =
                Arc::clone(&self.buffered_side.try_as_ready()?.buffered_data);
            let buffered_batch = buffered_data.batch();
            let buffered_len = buffered_batch.num_rows();

            // The marked rows are always the contiguous suffix `[min_marked, len)`: each
            // match covers `[k, previous min_marked)` and then lowers the watermark to
            // `k`, so the union is `[k, len)`. The result is therefore a slice, with no
            // index array to materialize and no `take`.
            let min_marked = buffered_data
                .min_marked
                .load(AtomicOrdering::SeqCst)
                .min(buffered_len);

            let sliced = match self.join_type {
                JoinType::LeftSemi => {
                    buffered_batch.slice(min_marked, buffered_len - min_marked)
                }
                // The unmarked prefix, which includes every null-keyed row: nulls sort
                // first and the watermark never drops below the buffered null count.
                _ => buffered_batch.slice(0, min_marked),
            };

            if sliced.num_rows() > 0 {
                // Existence joins output the buffered (left) columns only; rebuild against
                // the join's own schema, which keeps the slice zero-copy.
                let batch = RecordBatch::try_new(
                    Arc::clone(&self.schema),
                    sliced.columns().to_vec(),
                )?;
                self.output_batches.push_batch(batch)?;
                self.output_batches.finish_buffered_batch()?;
            }
        }

        // Drain one coalesced batch per poll; `emitted` keeps the block above from
        // re-running, so this always terminates.
        match self.output_batches.next_completed_batch() {
            Some(batch) => Ok(StatefulStreamResult::Ready(Some(batch))),
            None => {
                self.state = ExistencePWMJStreamState::Completed;
                Ok(StatefulStreamResult::Ready(None))
            }
        }
    }
}

/// Reduces `values` to a one-row array holding the batch's extreme compare key: the maximum
/// when the batch's sort is `descending`, the minimum otherwise. Nulls are ignored, so that
/// row is null only when every key in the batch is (or the batch is empty), which marks
/// nothing.
///
/// Ordered the same way as [`JoinKeyComparator`]: both use IEEE 754 totalOrder for floats,
/// and the comparator normalizes `-0.0` on either side of it.
///
/// Numeric, temporal, string, binary and boolean keys get a typed arrow kernel -- a linear
/// scan that allocates nothing. Dictionary and nested keys fall to `min_max_batch_generic`, a
/// `ScalarValue`-per-row comparator loop; specializing those is left to a follow-up.
fn extreme_key(values: &ArrayRef, descending: bool) -> Result<ArrayRef> {
    let extreme = if descending {
        max_batch(values)?
    } else {
        min_batch(values)?
    };
    extreme.to_array_of_size(1)
}

impl RecordBatchStream for ExistencePWMJStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

impl Stream for ExistencePWMJStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        // `record_poll` fills in `output_rows` and `end_time`; `elapsed_compute` is handled
        // by `BuildProbeJoinMetrics::drop`.
        let poll = self.poll_next_impl(cx);
        self.join_metrics.baseline.record_poll(poll)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        ExecutionPlan, common,
        joins::PiecewiseMergeJoinExec,
        test::{TestMemoryExec, assert_join_metrics, build_table_i32},
    };
    use arrow_schema::{DataType, Field, Schema};
    use datafusion_common::test_util::batches_to_string;
    use datafusion_execution::TaskContext;
    use datafusion_execution::config::SessionConfig;
    use datafusion_physical_expr::expressions::Column;
    use insta::assert_snapshot;

    // Coverage for existence joins lives in `pwmj.slt`: operators, NULL handling, empty
    // sides, key types, both correlation orientations, and the streamed-side batch and
    // partition layouts (which SQL pins via one `INSERT` per batch plus an `EXPLAIN`
    // asserting `partition_sizes`). The two tests here are demos of the operator itself.

    fn build_table(
        a: (&str, &Vec<i32>),
        b: (&str, &Vec<i32>),
        c: (&str, &Vec<i32>),
    ) -> Arc<dyn ExecutionPlan> {
        let batch = build_table_i32(a, b, c);
        let schema = batch.schema();
        TestMemoryExec::try_new_exec(&[vec![batch]], schema, None).unwrap()
    }

    async fn join_collect(join_type: JoinType) -> Result<Vec<RecordBatch>> {
        // Buffered (left) side pre-sorted ascending, as `>` requires. These tests build
        // the exec directly, so there is no `SortExec` to enforce it.
        let left = build_table(
            ("a1", &vec![1, 2, 3]),
            ("b1", &vec![1, 2, 5]),
            ("c1", &vec![7, 8, 9]),
        );
        let right = build_table(
            ("a2", &vec![10, 20, 30]),
            ("b1", &vec![2, 3, 4]),
            ("c2", &vec![70, 80, 90]),
        );
        let on = (
            Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
            Arc::new(Column::new_with_schema("b1", &right.schema())?) as _,
        );
        let join =
            PiecewiseMergeJoinExec::try_new(left, right, on, Operator::Gt, join_type, 1)?;

        let stream = join.execute(0, Arc::new(TaskContext::default()))?;
        common::collect(stream).await
    }

    /// `LeftSemi` keeps the buffered rows with at least one match, and outputs only the
    /// buffered columns. Of b1 = {1,2,5} against streamed {2,3,4}, only 5 > some streamed
    /// value.
    #[tokio::test]
    async fn join_left_semi() -> Result<()> {
        let batches = join_collect(JoinType::LeftSemi).await?;

        assert_snapshot!(batches_to_string(&batches), @r"
        +----+----+----+
        | a1 | b1 | c1 |
        +----+----+----+
        | 3  | 5  | 9  |
        +----+----+----+
        ");
        Ok(())
    }

    /// `LeftAnti` is the complement: the buffered rows with no match.
    #[tokio::test]
    async fn join_left_anti() -> Result<()> {
        let batches = join_collect(JoinType::LeftAnti).await?;

        assert_snapshot!(batches_to_string(&batches), @r"
        +----+----+----+
        | a1 | b1 | c1 |
        +----+----+----+
        | 1  | 1  | 7  |
        | 2  | 2  | 8  |
        +----+----+----+
        ");
        Ok(())
    }

    /// Once every markable buffered row is marked, no later streamed batch can lower the
    /// watermark, so the stream stops reading rather than scanning batches that provably
    /// cannot contribute. The first batch below is smaller than every buffered value, so it
    /// marks all four rows; the two batches after it must never be read. Asserted through
    /// the `input_batches` metric, which SQL cannot observe.
    #[tokio::test]
    async fn early_exit_stops_reading_streamed_batches() -> Result<()> {
        let left = build_table(
            ("a1", &vec![1, 2, 3, 4]),
            ("b1", &vec![1, 3, 5, 7]),
            ("c1", &vec![10, 20, 30, 40]),
        );

        let streamed_schema = Schema::new(vec![
            Field::new("a2", DataType::Int32, false),
            Field::new("b1", DataType::Int32, false),
            Field::new("c2", DataType::Int32, false),
        ]);
        // b1=0 is below every buffered value, so batch 1 marks the whole buffered side.
        let batch1 =
            build_table_i32(("a2", &vec![10]), ("b1", &vec![0]), ("c2", &vec![70]));
        let batch2 =
            build_table_i32(("a2", &vec![20]), ("b1", &vec![2]), ("c2", &vec![80]));
        let batch3 =
            build_table_i32(("a2", &vec![30]), ("b1", &vec![6]), ("c2", &vec![90]));
        let right = TestMemoryExec::try_new_exec(
            &[vec![batch1, batch2, batch3]],
            Arc::new(streamed_schema),
            None,
        )?;

        let on = (
            Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
            Arc::new(Column::new_with_schema("b1", &right.schema())?) as _,
        );
        let join = PiecewiseMergeJoinExec::try_new(
            left,
            right,
            on,
            Operator::Gt,
            JoinType::LeftSemi,
            1,
        )?;

        let stream = join.execute(0, Arc::new(TaskContext::default()))?;
        let batches = common::collect(stream).await?;

        // All four buffered rows still come out.
        assert_snapshot!(batches_to_string(&batches), @r"
        +----+----+----+
        | a1 | b1 | c1 |
        +----+----+----+
        | 1  | 1  | 10 |
        | 2  | 3  | 20 |
        | 3  | 5  | 30 |
        | 4  | 7  | 40 |
        +----+----+----+
        ");

        // ...but only the first of the three streamed batches was ever read.
        let consumed = join
            .metrics()
            .unwrap()
            .sum_by_name("input_batches")
            .expect("input_batches metric")
            .as_usize();
        assert_eq!(consumed, 1, "expected early exit after the first batch");
        Ok(())
    }

    /// The final pass pushes one slice of the buffered batch into a `BatchCoalescer`, so a
    /// result wider than `batch_size` has to be drained over several polls. Also pins the
    /// `output_rows` metric, which `record_poll` in `poll_next` is what supplies, and
    /// `join_time`, which the streamed-side scan and the final pass record.
    #[tokio::test]
    async fn final_pass_chunks_output_and_records_output_rows() -> Result<()> {
        let left = build_table(
            ("a1", &vec![1, 2, 3, 4]),
            ("b1", &vec![1, 3, 5, 7]),
            ("c1", &vec![10, 20, 30, 40]),
        );

        let streamed_schema = Schema::new(vec![
            Field::new("a2", DataType::Int32, false),
            Field::new("b1", DataType::Int32, false),
            Field::new("c2", DataType::Int32, false),
        ]);
        // b1=0 is below every buffered value, so every buffered row matches `>`.
        let batch =
            build_table_i32(("a2", &vec![10]), ("b1", &vec![0]), ("c2", &vec![70]));
        let right = TestMemoryExec::try_new_exec(
            &[vec![batch]],
            Arc::new(streamed_schema),
            None,
        )?;

        let on = (
            Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
            Arc::new(Column::new_with_schema("b1", &right.schema())?) as _,
        );
        let join = PiecewiseMergeJoinExec::try_new(
            left,
            right,
            on,
            Operator::Gt,
            JoinType::LeftSemi,
            1,
        )?;

        // batch_size 2 over a 4-row result forces the coalescer to hand back two batches.
        let task_ctx = Arc::new(
            TaskContext::default()
                .with_session_config(SessionConfig::new().with_batch_size(2)),
        );
        let stream = join.execute(0, task_ctx)?;
        let batches = common::collect(stream).await?;

        assert_eq!(batches.len(), 2, "expected the final pass to be chunked");
        assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 4);

        let metrics = join.metrics().expect("metrics should be available");
        assert_join_metrics!(metrics, 4);
        assert!(
            metrics
                .sum_by_name("join_time")
                .expect("join_time metric")
                .as_usize()
                > 0
        );
        Ok(())
    }

    /// The watermark lives in the shared buffered data, so one partition saturating it
    /// lets the others stop too. Partition 0's single batch marks every buffered row;
    /// partition 1 is then driven and must read none of its three batches. Only the shared
    /// watermark makes that possible -- with a per-partition watermark, partition 1 would
    /// rescan all three.
    #[tokio::test]
    async fn early_exit_is_shared_across_streamed_partitions() -> Result<()> {
        let left = build_table(
            ("a1", &vec![1, 2, 3, 4]),
            ("b1", &vec![1, 3, 5, 7]),
            ("c1", &vec![10, 20, 30, 40]),
        );

        let streamed_schema = Schema::new(vec![
            Field::new("a2", DataType::Int32, false),
            Field::new("b1", DataType::Int32, false),
            Field::new("c2", DataType::Int32, false),
        ]);
        // Partition 0: one batch below every buffered value -> marks the whole buffered side.
        let p0 = build_table_i32(("a2", &vec![10]), ("b1", &vec![0]), ("c2", &vec![70]));
        // Partition 1: three batches that can now contribute nothing.
        let p1_b0 =
            build_table_i32(("a2", &vec![20]), ("b1", &vec![2]), ("c2", &vec![80]));
        let p1_b1 =
            build_table_i32(("a2", &vec![30]), ("b1", &vec![4]), ("c2", &vec![90]));
        let p1_b2 =
            build_table_i32(("a2", &vec![40]), ("b1", &vec![6]), ("c2", &vec![100]));
        let right = TestMemoryExec::try_new_exec(
            &[vec![p0], vec![p1_b0, p1_b1, p1_b2]],
            Arc::new(streamed_schema),
            None,
        )?;

        let on = (
            Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
            Arc::new(Column::new_with_schema("b1", &right.schema())?) as _,
        );
        let join = PiecewiseMergeJoinExec::try_new(
            left,
            right,
            on,
            Operator::Gt,
            JoinType::LeftSemi,
            2,
        )?;

        // Driven in order so partition 0 saturates the watermark before partition 1 starts.
        let task_ctx = Arc::new(TaskContext::default());
        let mut batches = Vec::new();
        for partition in 0..2 {
            let stream = join.execute(partition, Arc::clone(&task_ctx))?;
            batches.extend(common::collect(stream).await?);
        }
        let out = arrow::compute::concat_batches(&join.schema(), batches.iter())?;

        assert_snapshot!(batches_to_string(&[out]), @r"
        +----+----+----+
        | a1 | b1 | c1 |
        +----+----+----+
        | 1  | 1  | 10 |
        | 2  | 3  | 20 |
        | 3  | 5  | 30 |
        | 4  | 7  | 40 |
        +----+----+----+
        ");

        // Partition 0 read its one batch; partition 1 read none of its three.
        let consumed = join
            .metrics()
            .unwrap()
            .sum_by_name("input_batches")
            .expect("input_batches metric")
            .as_usize();
        assert_eq!(consumed, 1, "partition 1 should not have read any batch");
        Ok(())
    }

    /// The unsupported existence joins must be rejected at construction, not deeper in.
    /// `required_input_ordering` still has an `unimplemented!()` for right existence joins
    /// and cannot return an error, so this test is what keeps that panic unreachable: if
    /// someone opens the gate for RightSemi/RightAnti without also supplying an ordering
    /// requirement, this fails instead of panicking the optimizer at runtime.
    #[test]
    fn try_new_rejects_unsupported_existence_joins() -> Result<()> {
        let left = build_table(
            ("a1", &vec![1, 2, 3]),
            ("b1", &vec![1, 2, 5]),
            ("c1", &vec![7, 8, 9]),
        );
        let right = build_table(
            ("a2", &vec![10, 20, 30]),
            ("b1", &vec![2, 3, 4]),
            ("c2", &vec![70, 80, 90]),
        );
        let on = (
            Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
            Arc::new(Column::new_with_schema("b1", &right.schema())?) as _,
        );

        for join_type in [
            JoinType::RightSemi,
            JoinType::RightAnti,
            JoinType::LeftMark,
            JoinType::RightMark,
        ] {
            let err = PiecewiseMergeJoinExec::try_new(
                Arc::clone(&left),
                Arc::clone(&right),
                on.clone(),
                Operator::Gt,
                join_type,
                1,
            )
            .expect_err(&format!("{join_type} should be rejected"))
            .to_string();
            assert!(
                err.contains("not supported for PiecewiseMergeJoin"),
                "unexpected error for {join_type}: {err}"
            );
        }
        Ok(())
    }

    /// Existence join never populated `probe_hit_rate`, so a streamed batch whose extreme
    /// key failed to lower the watermark was indistinguishable from one that did. Two
    /// batches here: the first lowers the watermark, the second lands entirely inside the
    /// already-marked region and must count as a miss.
    #[tokio::test]
    async fn probe_hit_rate_counts_batches_that_advance_the_watermark() -> Result<()> {
        let left = build_table(
            ("a1", &vec![1, 2, 3, 4, 5]),
            ("b1", &vec![1, 2, 3, 4, 5]),
            ("c1", &vec![10, 20, 30, 40, 50]),
        );

        let streamed_schema = Schema::new(vec![
            Field::new("a2", DataType::Int32, false),
            Field::new("b1", DataType::Int32, false),
            Field::new("c2", DataType::Int32, false),
        ]);
        // b1=3 lowers the watermark to buffered index 3 (value 4).
        let batch1 =
            build_table_i32(("a2", &vec![10]), ("b1", &vec![3]), ("c2", &vec![70]));
        // b1=4 only matches within the already-marked suffix, so it can't lower the
        // watermark further -- a miss.
        let batch2 =
            build_table_i32(("a2", &vec![20]), ("b1", &vec![4]), ("c2", &vec![80]));
        let right = TestMemoryExec::try_new_exec(
            &[vec![batch1, batch2]],
            Arc::new(streamed_schema),
            None,
        )?;

        let on = (
            Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
            Arc::new(Column::new_with_schema("b1", &right.schema())?) as _,
        );
        let join = PiecewiseMergeJoinExec::try_new(
            left,
            right,
            on,
            Operator::Gt,
            JoinType::LeftSemi,
            1,
        )?;

        let stream = join.execute(0, Arc::new(TaskContext::default()))?;
        let batches = common::collect(stream).await?;

        assert_snapshot!(batches_to_string(&batches), @r"
        +----+----+----+
        | a1 | b1 | c1 |
        +----+----+----+
        | 4  | 4  | 40 |
        | 5  | 5  | 50 |
        +----+----+----+
        ");

        let metrics = join.metrics().unwrap();
        let hit_rate = metrics
            .iter()
            .find_map(|m| match m.value() {
                crate::metrics::MetricValue::Ratio {
                    name,
                    ratio_metrics,
                } if name == "probe_hit_rate" => {
                    Some((ratio_metrics.part(), ratio_metrics.total()))
                }
                _ => None,
            })
            .expect("probe_hit_rate metric");
        assert_eq!(hit_rate, (1, 2), "one hit, one miss");

        Ok(())
    }

    /// An empty streamed batch has no extreme key to compare against the buffered side,
    /// so it must count as neither a hit nor a miss. Before the guard in `scan_stream_batch`,
    /// `mark_matched_buffered_rows` ran unconditionally and inflated `probe_hit_rate`'s
    /// denominator with misses for batches that never actually scanned anything.
    #[tokio::test]
    async fn probe_hit_rate_ignores_empty_streamed_batches() -> Result<()> {
        let left = build_table(
            ("a1", &vec![1, 2, 3]),
            ("b1", &vec![1, 2, 5]),
            ("c1", &vec![7, 8, 9]),
        );
        let streamed_schema = Schema::new(vec![
            Field::new("a2", DataType::Int32, false),
            Field::new("b1", DataType::Int32, false),
            Field::new("c2", DataType::Int32, false),
        ]);
        let empty_batch = build_table_i32(
            ("a2", &Vec::new()),
            ("b1", &Vec::new()),
            ("c2", &Vec::new()),
        );
        let real_batch =
            build_table_i32(("a2", &vec![10]), ("b1", &vec![0]), ("c2", &vec![70]));
        let right = TestMemoryExec::try_new_exec(
            &[vec![empty_batch, real_batch]],
            Arc::new(streamed_schema),
            None,
        )?;

        let on = (
            Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
            Arc::new(Column::new_with_schema("b1", &right.schema())?) as _,
        );
        let join = PiecewiseMergeJoinExec::try_new(
            left,
            right,
            on,
            Operator::Gt,
            JoinType::LeftSemi,
            1,
        )?;

        let stream = join.execute(0, Arc::new(TaskContext::default()))?;
        let batches = common::collect(stream).await?;

        // b1=0 is below every buffered value, so all three buffered rows match.
        assert_snapshot!(batches_to_string(&batches), @r"
        +----+----+----+
        | a1 | b1 | c1 |
        +----+----+----+
        | 1  | 1  | 7  |
        | 2  | 2  | 8  |
        | 3  | 5  | 9  |
        +----+----+----+
        ");

        let metrics = join.metrics().unwrap();
        let hit_rate = metrics
            .iter()
            .find_map(|m| match m.value() {
                crate::metrics::MetricValue::Ratio {
                    name,
                    ratio_metrics,
                } if name == "probe_hit_rate" => {
                    Some((ratio_metrics.part(), ratio_metrics.total()))
                }
                _ => None,
            })
            .expect("probe_hit_rate metric");
        // Only the real batch counts -- the empty batch contributes to neither part nor total.
        assert_eq!(hit_rate, (1, 1));

        Ok(())
    }
}
