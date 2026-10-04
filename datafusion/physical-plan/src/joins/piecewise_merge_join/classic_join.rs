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

//! Stream Implementation for PiecewiseMergeJoin's Classic Join (Left, Right, Full, Inner)

use arrow::array::{Array, AsArray, new_null_array};
use arrow::buffer::BooleanBuffer;
use arrow::compute::{BatchCoalescer, filter, not, prep_null_mask_filter, take};
use arrow::{
    array::{ArrayRef, BooleanArray, RecordBatch, UInt32Array},
    compute::{filter_record_batch, sort_to_indices, take_record_batch},
};
use arrow_schema::{DataType, Schema, SchemaRef, SortOptions};
use datafusion_common::NullEquality;
use datafusion_common::utils::normalize_float_zero;
use datafusion_common::{Result, ScalarValue, internal_err};
use datafusion_execution::{RecordBatchStream, SendableRecordBatchStream};
use datafusion_expr::{ColumnarValue, JoinType, Operator};
use datafusion_physical_expr::PhysicalExprRef;
use datafusion_physical_expr_common::datum::apply_cmp;
use futures::{Stream, StreamExt};
use std::sync::atomic::Ordering as AtomicOrdering;
use std::task::ready;
use std::{sync::Arc, task::Poll};

use crate::handle_state;
use crate::joins::piecewise_merge_join::exec::{BufferedSide, BufferedSideReadyState};
use crate::joins::piecewise_merge_join::utils::{
    first_match, is_match, matches_on_equal, need_produce_result_in_final,
};
use crate::joins::utils::JoinKeyComparator;
use crate::joins::utils::{BuildProbeJoinMetrics, StatefulStreamResult};
use crate::stream::EmptyRecordBatchStream;

pub(super) enum PiecewiseMergeJoinStreamState {
    WaitBufferedSide,
    FetchStreamBatch,
    ProcessStreamBatch(SortedStreamBatch),
    ProcessUnmatched,
    Completed,
}

impl PiecewiseMergeJoinStreamState {
    // Grab mutable reference to the current stream batch
    fn try_as_process_stream_batch_mut(&mut self) -> Result<&mut SortedStreamBatch> {
        match self {
            PiecewiseMergeJoinStreamState::ProcessStreamBatch(state) => Ok(state),
            _ => internal_err!("Expected streamed batch in StreamBatch"),
        }
    }
}

/// The stream side incoming batch with required sort order.
///
/// Note the compare key in the join predicate might include expressions on the original
/// columns, so we store the evaluated compare key separately.
/// e.g. For join predicate `buffer.v1 < (stream.v1 + 1)`, the `compare_key_values` field stores
/// the evaluated `stream.v1 + 1` array.
pub(super) struct SortedStreamBatch {
    pub batch: RecordBatch,
    compare_key_values: Vec<ArrayRef>,
}

impl SortedStreamBatch {
    fn new(batch: RecordBatch, compare_key_values: Vec<ArrayRef>) -> Self {
        Self {
            batch,
            compare_key_values,
        }
    }

    fn compare_key_values(&self) -> &Vec<ArrayRef> {
        &self.compare_key_values
    }
}

pub(super) struct ClassicPWMJStream {
    // Output schema of the `PiecewiseMergeJoin`
    pub schema: Arc<Schema>,

    // Physical expression that is evaluated on the streamed side
    // We do not need on_buffered as this is already evaluated when
    // creating the buffered side which happens before initializing
    // `PiecewiseMergeJoinStream`
    pub on_streamed: PhysicalExprRef,
    // Type of join
    pub join_type: JoinType,
    // Comparison operator
    pub operator: Operator,
    // Streamed batch
    pub streamed: SendableRecordBatchStream,
    // Streamed schema
    streamed_schema: SchemaRef,
    // Buffered side data
    buffered_side: BufferedSide,
    // Tracks the state of the `PiecewiseMergeJoin`
    state: PiecewiseMergeJoinStreamState,
    // Sort option for streamed side (specifies whether
    // the sort is ascending or descending)
    sort_option: SortOptions,
    // Metrics for build + probe joins
    join_metrics: BuildProbeJoinMetrics,
    // Tracking incremental state for emitting record batches
    batch_process_state: BatchProcessState,
    // The last buffered key, which every match set contains (see `matchable_rows`), or
    // `None` when no buffered key is non-null and nothing can match. Set once the buffered
    // side is ready.
    buffered_extreme: Option<ColumnarValue>,
}

impl RecordBatchStream for ClassicPWMJStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

// `PiecewiseMergeJoinStreamState` is separated into `WaitBufferedSide`, `FetchStreamBatch`,
// `ProcessStreamBatch`, `ProcessUnmatched` and `Completed`.
//
// Classic Joins
//  1. `WaitBufferedSide` - Load in the buffered side data into memory.
//  2. `FetchStreamBatch` -  Fetch + sort incoming stream batches. We switch the state to
//     `Completed` if there are still remaining partitions to process. It is only switched to
//     `ExhaustedStreamBatch` if all partitions have been processed.
//  3. `ProcessStreamBatch` - Compare stream batch row values against the buffered side data.
//  4. `ExhaustedStreamBatch` - If the join type is Left or Inner we will return state as
//      `Completed` however for Full and Right we will need to process the unmatched buffered rows.
impl ClassicPWMJStream {
    // Creates a new `PiecewiseMergeJoinStream` instance
    #[expect(clippy::too_many_arguments)]
    pub fn try_new(
        schema: Arc<Schema>,
        on_streamed: PhysicalExprRef,
        join_type: JoinType,
        operator: Operator,
        streamed: SendableRecordBatchStream,
        buffered_side: BufferedSide,
        state: PiecewiseMergeJoinStreamState,
        sort_option: SortOptions,
        join_metrics: BuildProbeJoinMetrics,
        batch_size: usize,
    ) -> Self {
        Self {
            schema: Arc::clone(&schema),
            on_streamed,
            join_type,
            operator,
            streamed_schema: streamed.schema(),
            streamed,
            buffered_side,
            state,
            sort_option,
            join_metrics,
            batch_process_state: BatchProcessState::new(schema, batch_size),
            buffered_extreme: None,
        }
    }

    fn poll_next_impl(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Result<RecordBatch>>> {
        loop {
            return match self.state {
                PiecewiseMergeJoinStreamState::WaitBufferedSide => {
                    handle_state!(ready!(self.collect_buffered_side(cx)))
                }
                PiecewiseMergeJoinStreamState::FetchStreamBatch => {
                    handle_state!(ready!(self.fetch_stream_batch(cx)))
                }
                PiecewiseMergeJoinStreamState::ProcessStreamBatch(_) => {
                    handle_state!(self.process_stream_batch())
                }
                PiecewiseMergeJoinStreamState::ProcessUnmatched => {
                    handle_state!(self.process_unmatched_buffered_batch())
                }
                PiecewiseMergeJoinStreamState::Completed => Poll::Ready(None),
            };
        }
    }

    // Collects buffered side data
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

        self.buffered_extreme = buffered_extreme(buffered_data.values())?;

        // We will start fetching stream batches for classic joins
        self.state = PiecewiseMergeJoinStreamState::FetchStreamBatch;

        self.buffered_side =
            BufferedSide::Ready(BufferedSideReadyState { buffered_data });

        Poll::Ready(Ok(StatefulStreamResult::Continue))
    }

    // Fetches incoming stream batches
    fn fetch_stream_batch(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<StatefulStreamResult<Option<RecordBatch>>>> {
        let next_batch = ready!(self.streamed.poll_next_unpin(cx));
        let _join_timer = self.join_metrics.join_time.timer();
        match next_batch {
            None => {
                // Release the streamed input pipeline's resources.
                let streamed_schema = self.streamed.schema();
                self.streamed = Box::pin(EmptyRecordBatchStream::new(streamed_schema));
                if self
                    .buffered_side
                    .try_as_ready_mut()?
                    .buffered_data
                    .remaining_partitions
                    .fetch_sub(1, std::sync::atomic::Ordering::SeqCst)
                    == 1
                {
                    self.batch_process_state.reset();
                    self.state = PiecewiseMergeJoinStreamState::ProcessUnmatched;
                } else {
                    self.state = PiecewiseMergeJoinStreamState::Completed;
                }
            }
            Some(Ok(batch)) => {
                // Evaluate the streamed physical expression on the stream batch
                let stream_values: ArrayRef = self
                    .on_streamed
                    .evaluate(&batch)?
                    .into_array(batch.num_rows())?;

                self.join_metrics.input_batches.add(1);
                self.join_metrics.input_rows.add(batch.num_rows());

                // Rows that can match no buffered row are settled before the sort (see
                // `split_off_never_matching`); only the rest are sorted and scanned.
                let buffered_values =
                    self.buffered_side.try_as_ready()?.buffered_data.values();
                let matchable = matchable_rows(
                    &stream_values,
                    buffered_values,
                    self.buffered_extreme.as_ref(),
                    self.operator,
                    self.sort_option,
                )?;
                let candidates = split_off_never_matching(
                    &matchable,
                    self.join_type,
                    &self.schema,
                    &batch,
                    &stream_values,
                    &mut self.batch_process_state.output_batches,
                )?;

                // Sort the candidates' keys, and gather the batch once: when rows were split
                // off, the sort indices are mapped back to positions in `batch` first.
                let (keys, positions) = match candidates {
                    None => (stream_values, None),
                    Some((positions, keys)) => (keys, Some(positions)),
                };
                let sorted =
                    sort_to_indices(keys.as_ref(), Some(self.sort_option), None)?;
                let stream_batch = match positions {
                    None => take_record_batch(&batch, &sorted)?,
                    Some(positions) => take_record_batch(
                        &batch,
                        take(&positions, &sorted, None)?.as_ref(),
                    )?,
                };
                let stream_values = take(keys.as_ref(), &sorted, None)?;

                // Reset BatchProcessState before processing a new stream batch. NULL keys
                // never match and sort first, so the scan starts past the buffered ones.
                self.batch_process_state.reset();
                self.batch_process_state.start_buffer_idx = buffered_values.null_count();
                self.state = PiecewiseMergeJoinStreamState::ProcessStreamBatch(
                    SortedStreamBatch::new(stream_batch, vec![stream_values]),
                );
            }
            Some(Err(err)) => return Poll::Ready(Err(err)),
        }

        Poll::Ready(Ok(StatefulStreamResult::Continue))
    }

    // Only classic join will call. This function will process stream batches and evaluate against
    // the buffered side data.
    fn process_stream_batch(
        &mut self,
    ) -> Result<StatefulStreamResult<Option<RecordBatch>>> {
        let _join_timer = self.join_metrics.join_time.timer();
        let buffered_side = self.buffered_side.try_as_ready_mut()?;
        let stream_batch = self.state.try_as_process_stream_batch_mut()?;

        if let Some(batch) = self
            .batch_process_state
            .output_batches
            .next_completed_batch()
        {
            return Ok(StatefulStreamResult::Ready(Some(batch)));
        }

        // A finished scan can leave several completed batches queued; emit
        // them one per poll and transition only once the queue is empty, so
        // no output is lost.
        if !self.batch_process_state.continue_process {
            if let Some(batch) = self.batch_process_state.next_drained_batch()? {
                return Ok(StatefulStreamResult::Ready(Some(batch)));
            }

            self.state = PiecewiseMergeJoinStreamState::FetchStreamBatch;
            return Ok(StatefulStreamResult::Continue);
        }

        // Produce more work
        let batch = resolve_classic_join(
            buffered_side,
            stream_batch,
            &self.schema,
            self.operator,
            self.sort_option,
            self.join_type,
            &mut self.batch_process_state,
        )?;

        if !self.batch_process_state.continue_process {
            // Scan finished; re-enter through the drain guard above.
            return Ok(StatefulStreamResult::Continue);
        }

        Ok(StatefulStreamResult::Ready(Some(batch)))
    }

    // Process remaining unmatched rows
    fn process_unmatched_buffered_batch(
        &mut self,
    ) -> Result<StatefulStreamResult<Option<RecordBatch>>> {
        let _join_timer = self.join_metrics.join_time.timer();
        // Return early for `JoinType::Right` and `JoinType::Inner`
        if matches!(self.join_type, JoinType::Right | JoinType::Inner) {
            self.state = PiecewiseMergeJoinStreamState::Completed;
            return Ok(StatefulStreamResult::Ready(None));
        }

        if !self.batch_process_state.continue_process {
            if let Some(batch) = self.batch_process_state.next_drained_batch()? {
                return Ok(StatefulStreamResult::Ready(Some(batch)));
            }

            // Fully drained; finish instead of re-running the pass.
            self.state = PiecewiseMergeJoinStreamState::Completed;
            return Ok(StatefulStreamResult::Continue);
        }

        let buffered_data = Arc::clone(&self.buffered_side.try_as_ready()?.buffered_data);
        let buffered_batch = buffered_data.batch();

        // Every match marks the suffix `[k, buffered_len)`, so the buffered rows that were
        // never matched are exactly the complementary prefix `[0, min_marked)` -- which
        // includes the null-keyed rows, since nulls sort first and the scan starts past
        // them. That makes the final pass a zero-copy slice instead of building an index
        // array and running `take` over it.
        let min_marked = buffered_data
            .min_marked
            .load(AtomicOrdering::SeqCst)
            .min(buffered_batch.num_rows());
        let new_buffered_batch = buffered_batch.slice(0, min_marked);
        let mut buffered_columns = new_buffered_batch.columns().to_vec();

        let streamed_columns: Vec<ArrayRef> = self
            .streamed_schema
            .fields()
            .iter()
            .map(|f| new_null_array(f.data_type(), new_buffered_batch.num_rows()))
            .collect();

        buffered_columns.extend(streamed_columns);

        let batch = RecordBatch::try_new(Arc::clone(&self.schema), buffered_columns)?;

        self.batch_process_state.output_batches.push_batch(batch)?;

        self.batch_process_state.continue_process = false;
        // Re-enter through the drain guard above.
        Ok(StatefulStreamResult::Continue)
    }
}

struct BatchProcessState {
    // Used to pick up from the last index on the stream side
    output_batches: Box<BatchCoalescer>,
    // Used to store the start index on the buffered side; used to resume processing on the correct
    // row
    start_buffer_idx: usize,
    // Used to store the start index on the stream side; used to resume processing on the correct
    // row
    start_stream_idx: usize,
    // Signals to continue processing the current stream batch
    continue_process: bool,
    // Smallest buffered index marked while scanning the current stream batch, or
    // `usize::MAX` if nothing has been marked yet. Because `buffer_idx` only moves forward
    // within a batch, this lets all but the batch's first match skip the shared atomic.
    batch_min_marked: usize,
}

impl BatchProcessState {
    pub(crate) fn new(schema: Arc<Schema>, batch_size: usize) -> Self {
        Self {
            output_batches: Box::new(BatchCoalescer::new(schema, batch_size)),
            start_buffer_idx: 0,
            start_stream_idx: 0,
            continue_process: true,
            batch_min_marked: usize::MAX,
        }
    }

    pub(crate) fn reset(&mut self) {
        self.start_buffer_idx = 0;
        self.start_stream_idx = 0;
        self.continue_process = true;
        self.batch_min_marked = usize::MAX;
    }

    // `None` guarantees the coalescer holds no pending rows, so the caller
    // may safely transition state without losing output.
    fn next_drained_batch(&mut self) -> Result<Option<RecordBatch>> {
        self.output_batches.finish_buffered_batch()?;
        Ok(self.output_batches.next_completed_batch())
    }
}

impl Stream for ClassicPWMJStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let poll = self.poll_next_impl(cx);
        self.join_metrics.baseline.record_poll(poll)
    }
}

// For Left, Right, Full, and Inner joins, incoming stream batches will already be sorted.
fn resolve_classic_join(
    buffered_side: &mut BufferedSideReadyState,
    stream_batch: &SortedStreamBatch,
    join_schema: &SchemaRef,
    operator: Operator,
    sort_options: SortOptions,
    join_type: JoinType,
    batch_process_state: &mut BatchProcessState,
) -> Result<RecordBatch> {
    let buffered_len = buffered_side.buffered_data.values().len();
    let stream_values = stream_batch.compare_key_values();

    // Build comparator once for the batch pair
    let cmp = JoinKeyComparator::new(
        &[Arc::clone(&stream_values[0])],
        &[Arc::clone(buffered_side.buffered_data.values())],
        &[sort_options],
        NullEquality::NullEqualsNothing,
    )?;

    let mut buffer_idx = batch_process_state.start_buffer_idx;
    let stream_idx = batch_process_state.start_stream_idx;

    // Streamed NULL keys are settled before the sort (see `split_off_never_matching`). One
    // that got here would sort before every buffered key and match all of them. Logical, as a
    // run-end encoded NULL has no physical null buffer.
    if stream_values[0].logical_null_count() > 0 {
        return internal_err!(
            "PiecewiseMergeJoin: streamed NULL keys must be settled before the scan"
        );
    }

    let match_on_equal = matches_on_equal(operator)?;

    // `buffer_idx` resumes where the previous stream row's first match was: rows are sorted
    // so that each one's first match is at or after the previous one's, which bounds the
    // search below to `[buffer_idx, buffered_len)`.
    //
    // Every row here matches the last buffered row, as `matchable_rows` kept only those.
    for row_idx in stream_idx..stream_batch.batch.num_rows() {
        buffer_idx = first_match(buffer_idx, buffered_len, |idx| {
            is_match(cmp.compare(row_idx, idx), match_on_equal)
        });
        // A row with no match would emit nothing here, and for `Right`/`Full` would never
        // be emitted as unmatched either.
        if buffer_idx >= buffered_len {
            return internal_err!(
                "PiecewiseMergeJoin: streamed row {row_idx} reached the scan without a match"
            );
        }
        let count = buffered_len - buffer_idx;

        let batch = build_matched_indices_and_mark_buffered(
            (buffer_idx, count),
            (row_idx, count),
            buffered_side,
            stream_batch,
            join_type,
            join_schema,
            &mut batch_process_state.batch_min_marked,
        )?;

        batch_process_state.output_batches.push_batch(batch)?;

        // Flush batch and update pointers if we have a completed batch
        if let Some(batch) = batch_process_state.output_batches.next_completed_batch() {
            batch_process_state.start_buffer_idx = buffer_idx;
            batch_process_state.start_stream_idx = row_idx + 1;
            return Ok(batch);
        }
    }

    batch_process_state.continue_process = false;
    Ok(RecordBatch::new_empty(Arc::clone(join_schema)))
}

// Builds a record batch from indices ranges on the buffered and streamed side.
//
// The two ranges are: buffered_range: (start index, count) and streamed_range: (start index, count) due
// to batch.slice(start, count).
fn build_matched_indices_and_mark_buffered(
    buffered_range: (usize, usize),
    streamed_range: (usize, usize),
    buffered_side: &mut BufferedSideReadyState,
    stream_batch: &SortedStreamBatch,
    join_type: JoinType,
    join_schema: &SchemaRef,
    batch_min_marked: &mut usize,
) -> Result<RecordBatch> {
    // Mark the matched buffered rows. `buffered_range` is always the suffix
    // `[start, buffered_len)` -- a match emits every buffered row from the first match on --
    // so the union of everything marked is `[min over matches, buffered_len)` and lowering a
    // single watermark records it exactly. That replaces a mutex plus one `set_bit` per
    // matched row, which was `O(buffered_len)` work for *every* matched streamed row.
    //
    // `buffer_idx` is monotone non-decreasing across a stream batch, so only the batch's
    // first match can lower the watermark; `batch_min_marked` keeps the atomic off the hot
    // path for all the others. It survives the early returns that hand back a completed
    // output batch mid-scan, and `reset()` clears it for the next stream batch.
    if need_produce_result_in_final(join_type) && buffered_range.0 < *batch_min_marked {
        *batch_min_marked = buffered_range.0;
        buffered_side
            .buffered_data
            .min_marked
            .fetch_min(buffered_range.0, AtomicOrdering::SeqCst);
    }

    let new_buffered_batch = buffered_side
        .buffered_data
        .batch()
        .slice(buffered_range.0, buffered_range.1);
    let mut buffered_columns = new_buffered_batch.columns().to_vec();

    let indices = UInt32Array::from_value(streamed_range.0 as u32, streamed_range.1);
    let new_stream_batch = take_record_batch(&stream_batch.batch, &indices)?;
    let streamed_columns = new_stream_batch.columns().to_vec();

    buffered_columns.extend(streamed_columns);

    Ok(RecordBatch::try_new(
        Arc::clone(join_schema),
        buffered_columns,
    )?)
}

// The last key of the sorted buffered side, or `None` when it is empty or every key is NULL:
// NULLs sort first, so the last key is NULL only when every buffered key is. Logical, as a
// run-end encoded or `Null`-typed NULL has no physical null buffer.
fn buffered_extreme(values: &ArrayRef) -> Result<Option<ColumnarValue>> {
    let Some(last) = values.len().checked_sub(1) else {
        return Ok(None);
    };
    let last = values.slice(last, 1);
    if last.logical_null_count() > 0 {
        return Ok(None);
    }
    // `apply_cmp` normalizes `-0.0` only in flat float scalars, not inside a
    // `ScalarValue::Dictionary`, so normalize the key before taking it.
    let extreme = normalize_float_zero(&last);
    Ok(Some(ColumnarValue::Scalar(ScalarValue::try_from_array(
        &extreme, 0,
    )?)))
}

// Which rows of `stream_values` can match at least one buffered row.
//
// Every match set is a suffix `[k, buffered_len)` of the sorted buffered side, so a streamed
// row matches anything at all iff it matches the last buffered row, `buffered_extreme`
// (`None` when no buffered key is non-null, so nothing can match). A NULL on either side
// compares to NULL, which is no match.
//
// This must agree exactly with the scan's `JoinKeyComparator`, or it would change which rows
// match. For flat keys one vectorized `apply_cmp` does: it normalizes `-0.0` to `+0.0` as the
// comparator does. For nested keys it does not -- `apply_cmp` orders NULL elements inside a
// key ascending, while the comparator applies the sort options (descending for `<`/`<=`) at
// every level -- so those are decided with the scan's own comparator.
fn matchable_rows(
    stream_values: &ArrayRef,
    buffered_values: &ArrayRef,
    buffered_extreme: Option<&ColumnarValue>,
    operator: Operator,
    sort_options: SortOptions,
) -> Result<BooleanArray> {
    let num_rows = stream_values.len();
    let Some(extreme) = buffered_extreme else {
        return Ok(BooleanArray::new(BooleanBuffer::new_unset(num_rows), None));
    };

    // The comparator's nested ordering is itself wrong for `<`/`<=` (#25957). Once that is
    // fixed, nested keys can go through `apply_cmp` too and this branch can be removed.
    //
    // Run-end encoded keys are decided here too: arrow's run-end comparison kernel overflows
    // on an empty batch sliced past its first run, which the scan never reaches.
    if stream_values.data_type().is_nested()
        || matches!(stream_values.data_type(), DataType::RunEndEncoded(_, _))
    {
        let match_on_equal = matches_on_equal(operator)?;
        let cmp = JoinKeyComparator::new(
            &[Arc::clone(stream_values)],
            &[Arc::clone(buffered_values)],
            &[sort_options],
            NullEquality::NullEqualsNothing,
        )?;
        let last = buffered_values.len() - 1;
        // Logical NULLs: a run-end encoded NULL has no physical null buffer.
        let nulls = stream_values.logical_nulls();
        let matchable = BooleanBuffer::collect_bool(num_rows, |row| {
            nulls.as_ref().is_none_or(|nulls| nulls.is_valid(row))
                && is_match(cmp.compare(row, last), match_on_equal)
        });
        return Ok(BooleanArray::new(matchable, None));
    }

    // The predicate is oriented `buffered OP streamed`.
    let result = apply_cmp(
        operator,
        extreme,
        &ColumnarValue::Array(Arc::clone(stream_values)),
    )?
    .into_array(num_rows)?;
    let result = result.as_boolean();
    Ok(if result.null_count() > 0 {
        prep_null_mask_filter(result)
    } else {
        result.clone()
    })
}

// Settles the rows of `batch` that `matchable` rules out: pushed to `output_batches` as
// unmatched rows for `Right`/`Full`, dropped for `Inner`/`Left`, as they produce nothing and
// mark nothing. Returns `None` when every row is a candidate, otherwise the candidates'
// positions in `batch` and their compare keys.
fn split_off_never_matching(
    matchable: &BooleanArray,
    join_type: JoinType,
    join_schema: &SchemaRef,
    batch: &RecordBatch,
    stream_values: &ArrayRef,
    output_batches: &mut BatchCoalescer,
) -> Result<Option<(UInt32Array, ArrayRef)>> {
    if matchable.true_count() == batch.num_rows() {
        return Ok(None);
    }

    if matches!(join_type, JoinType::Right | JoinType::Full) {
        let unmatched = filter_record_batch(batch, &not(matchable)?)?;
        output_batches
            .push_batch(null_padded_streamed_batch(&unmatched, join_schema)?)?;
    }

    let positions =
        UInt32Array::from_iter_values(matchable.values().set_indices().map(|i| i as u32));
    let keys = filter(stream_values.as_ref(), matchable)?;
    Ok(Some((positions, keys)))
}

// Joins streamed rows with NULLs for every buffered column.
fn null_padded_streamed_batch(
    streamed_batch: &RecordBatch,
    join_schema: &SchemaRef,
) -> Result<RecordBatch> {
    let streamed_columns = streamed_batch.columns().to_vec();
    let buffered_cols_len = join_schema.fields().len() - streamed_columns.len();

    let num_rows = streamed_batch.num_rows();
    let mut buffered_columns: Vec<ArrayRef> = join_schema
        .fields()
        .iter()
        .take(buffered_cols_len)
        .map(|field| new_null_array(field.data_type(), num_rows))
        .collect();

    buffered_columns.extend(streamed_columns);

    Ok(RecordBatch::try_new(
        Arc::clone(join_schema),
        buffered_columns,
    )?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, common,
        joins::PiecewiseMergeJoinExec,
        metrics::MetricsSet,
        stream::RecordBatchStreamAdapter,
        test::{TestMemoryExec, assert_join_metrics, build_table_i32},
    };
    use arrow::array::{Date32Array, Date64Array};
    use arrow_schema::Field;
    use datafusion_common::instant::Instant;
    use datafusion_common::test_util::batches_to_string;
    use datafusion_common::tree_node::TreeNodeRecursion;
    use datafusion_execution::TaskContext;
    use datafusion_execution::config::SessionConfig;
    use datafusion_physical_expr::{PhysicalExpr, expressions::Column};
    use futures::TryStreamExt;
    use insta::assert_snapshot;
    use std::sync::Arc;
    use std::time::Duration;

    fn columns(schema: &Schema) -> Vec<String> {
        schema.fields().iter().map(|f| f.name().clone()).collect()
    }

    fn build_table(
        a: (&str, &Vec<i32>),
        b: (&str, &Vec<i32>),
        c: (&str, &Vec<i32>),
    ) -> Arc<dyn ExecutionPlan> {
        let batch = build_table_i32(a, b, c);
        let schema = batch.schema();
        TestMemoryExec::try_new_exec(&[vec![batch]], schema, None).unwrap()
    }

    fn build_date_table(
        a: (&str, &Vec<i32>),
        b: (&str, &Vec<i32>),
        c: (&str, &Vec<i32>),
    ) -> Arc<dyn ExecutionPlan> {
        let schema = Schema::new(vec![
            Field::new(a.0, DataType::Date32, false),
            Field::new(b.0, DataType::Date32, false),
            Field::new(c.0, DataType::Date32, false),
        ]);

        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(Date32Array::from(a.1.clone())),
                Arc::new(Date32Array::from(b.1.clone())),
                Arc::new(Date32Array::from(c.1.clone())),
            ],
        )
        .unwrap();

        let schema = batch.schema();
        TestMemoryExec::try_new_exec(&[vec![batch]], schema, None).unwrap()
    }

    fn build_date64_table(
        a: (&str, &Vec<i64>),
        b: (&str, &Vec<i64>),
        c: (&str, &Vec<i64>),
    ) -> Arc<dyn ExecutionPlan> {
        let schema = Schema::new(vec![
            Field::new(a.0, DataType::Date64, false),
            Field::new(b.0, DataType::Date64, false),
            Field::new(c.0, DataType::Date64, false),
        ]);

        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![
                Arc::new(Date64Array::from(a.1.clone())),
                Arc::new(Date64Array::from(b.1.clone())),
                Arc::new(Date64Array::from(c.1.clone())),
            ],
        )
        .unwrap();

        let schema = batch.schema();
        TestMemoryExec::try_new_exec(&[vec![batch]], schema, None).unwrap()
    }

    fn join(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        on: (Arc<dyn PhysicalExpr>, Arc<dyn PhysicalExpr>),
        operator: Operator,
        join_type: JoinType,
    ) -> Result<PiecewiseMergeJoinExec> {
        PiecewiseMergeJoinExec::try_new(left, right, on, operator, join_type, 1)
    }

    async fn join_collect(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        on: (PhysicalExprRef, PhysicalExprRef),
        operator: Operator,
        join_type: JoinType,
    ) -> Result<(Vec<String>, Vec<RecordBatch>, MetricsSet)> {
        join_collect_with_options(left, right, on, operator, join_type).await
    }

    async fn join_collect_with_options(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        on: (PhysicalExprRef, PhysicalExprRef),
        operator: Operator,
        join_type: JoinType,
    ) -> Result<(Vec<String>, Vec<RecordBatch>, MetricsSet)> {
        let task_ctx = Arc::new(TaskContext::default());
        let join = join(left, right, on, operator, join_type)?;
        let columns = columns(&join.schema());

        let stream = join.execute(0, task_ctx)?;
        let batches = common::collect(stream).await?;
        let metrics = join.metrics().expect("metrics should be available");
        Ok((columns, batches, metrics))
    }

    #[tokio::test]
    async fn join_inner_less_than() -> Result<()> {
        // +----+----+----+
        // | a1 | b1 | c1 |
        // +----+----+----+
        // | 1  | 3  | 7  |
        // | 2  | 2  | 8  |
        // | 3  | 1  | 9  |
        // +----+----+----+
        let left = build_table(
            ("a1", &vec![1, 2, 3]),
            ("b1", &vec![3, 2, 1]), // this has a repetition
            ("c1", &vec![7, 8, 9]),
        );

        // +----+----+----+
        // | a2 | b1 | c2 |
        // +----+----+----+
        // | 10 | 2  | 70 |
        // | 20 | 3  | 80 |
        // | 30 | 4  | 90 |
        // +----+----+----+
        let right = build_table(
            ("a2", &vec![10, 20, 30]),
            ("b1", &vec![2, 3, 4]),
            ("c2", &vec![70, 80, 90]),
        );

        let on = (
            Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
            Arc::new(Column::new_with_schema("b1", &right.schema())?) as _,
        );

        let (_, batches, metrics) =
            join_collect(left, right, on, Operator::Lt, JoinType::Inner).await?;

        assert_snapshot!(batches_to_string(&batches), @r"
        +----+----+----+----+----+----+
        | a1 | b1 | c1 | a2 | b1 | c2 |
        +----+----+----+----+----+----+
        | 1  | 3  | 7  | 30 | 4  | 90 |
        | 2  | 2  | 8  | 30 | 4  | 90 |
        | 3  | 1  | 9  | 30 | 4  | 90 |
        | 2  | 2  | 8  | 20 | 3  | 80 |
        | 3  | 1  | 9  | 20 | 3  | 80 |
        | 3  | 1  | 9  | 10 | 2  | 70 |
        +----+----+----+----+----+----+
        ");

        assert_join_metrics!(metrics, 6);
        Ok(())
    }

    #[tokio::test]
    async fn join_inner_less_than_unsorted() -> Result<()> {
        // +----+----+----+
        // | a1 | b1 | c1 |
        // +----+----+----+
        // | 1  | 3  | 7  |
        // | 2  | 2  | 8  |
        // | 3  | 1  | 9  |
        // +----+----+----+
        let left = build_table(
            ("a1", &vec![1, 2, 3]),
            ("b1", &vec![3, 2, 1]), // this has a repetition
            ("c1", &vec![7, 8, 9]),
        );

        // +----+----+----+
        // | a2 | b1 | c2 |
        // +----+----+----+
        // | 10 | 3  | 70 |
        // | 20 | 2  | 80 |
        // | 30 | 4  | 90 |
        // +----+----+----+
        let right = build_table(
            ("a2", &vec![10, 20, 30]),
            ("b1", &vec![3, 2, 4]),
            ("c2", &vec![70, 80, 90]),
        );

        let on = (
            Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
            Arc::new(Column::new_with_schema("b1", &right.schema())?) as _,
        );

        let (_, batches, metrics) =
            join_collect(left, right, on, Operator::Lt, JoinType::Inner).await?;

        assert_snapshot!(batches_to_string(&batches), @r"
        +----+----+----+----+----+----+
        | a1 | b1 | c1 | a2 | b1 | c2 |
        +----+----+----+----+----+----+
        | 1  | 3  | 7  | 30 | 4  | 90 |
        | 2  | 2  | 8  | 30 | 4  | 90 |
        | 3  | 1  | 9  | 30 | 4  | 90 |
        | 2  | 2  | 8  | 10 | 3  | 70 |
        | 3  | 1  | 9  | 10 | 3  | 70 |
        | 3  | 1  | 9  | 20 | 2  | 80 |
        +----+----+----+----+----+----+
        ");

        assert_join_metrics!(metrics, 6);
        Ok(())
    }

    #[tokio::test]
    async fn join_inner_greater_than_equal_to() -> Result<()> {
        // +----+----+----+
        // | a1 | b1 | c1 |
        // +----+----+----+
        // | 1  | 2  | 7  |
        // | 2  | 3  | 8  |
        // | 3  | 4  | 9  |
        // +----+----+----+
        let left = build_table(
            ("a1", &vec![1, 2, 3]),
            ("b1", &vec![2, 3, 4]),
            ("c1", &vec![7, 8, 9]),
        );

        // +----+----+----+
        // | a2 | b1 | c2 |
        // +----+----+----+
        // | 10 | 3  | 70 |
        // | 20 | 2  | 80 |
        // | 30 | 1  | 90 |
        // +----+----+----+
        let right = build_table(
            ("a2", &vec![10, 20, 30]),
            ("b1", &vec![3, 2, 1]),
            ("c2", &vec![70, 80, 90]),
        );

        let on = (
            Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
            Arc::new(Column::new_with_schema("b1", &right.schema())?) as _,
        );

        let (_, batches, metrics) =
            join_collect(left, right, on, Operator::GtEq, JoinType::Inner).await?;

        assert_snapshot!(batches_to_string(&batches), @r"
        +----+----+----+----+----+----+
        | a1 | b1 | c1 | a2 | b1 | c2 |
        +----+----+----+----+----+----+
        | 1  | 2  | 7  | 30 | 1  | 90 |
        | 2  | 3  | 8  | 30 | 1  | 90 |
        | 3  | 4  | 9  | 30 | 1  | 90 |
        | 1  | 2  | 7  | 20 | 2  | 80 |
        | 2  | 3  | 8  | 20 | 2  | 80 |
        | 3  | 4  | 9  | 20 | 2  | 80 |
        | 2  | 3  | 8  | 10 | 3  | 70 |
        | 3  | 4  | 9  | 10 | 3  | 70 |
        +----+----+----+----+----+----+
        ");

        assert_join_metrics!(metrics, 8);
        Ok(())
    }

    #[tokio::test]
    async fn join_inner_empty_left() -> Result<()> {
        // +----+----+----+
        // | a1 | b1 | c1 |
        // +----+----+----+
        // (empty)
        // +----+----+----+
        let left = build_table(
            ("a1", &Vec::<i32>::new()),
            ("b1", &Vec::<i32>::new()),
            ("c1", &Vec::<i32>::new()),
        );

        // +----+----+----+
        // | a2 | b1 | c2 |
        // +----+----+----+
        // | 1  | 1  | 1  |
        // | 2  | 2  | 2  |
        // +----+----+----+
        let right = build_table(
            ("a2", &vec![1, 2]),
            ("b1", &vec![1, 2]),
            ("c2", &vec![1, 2]),
        );

        let on = (
            Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
            Arc::new(Column::new_with_schema("b1", &right.schema())?) as _,
        );
        let (_, batches, metrics) =
            join_collect(left, right, on, Operator::LtEq, JoinType::Inner).await?;
        // An empty join result produces no batches at all, not an empty batch.
        assert!(batches.is_empty());

        assert_join_metrics!(metrics, 0);
        Ok(())
    }

    #[tokio::test]
    async fn join_full_greater_than_equal_to() -> Result<()> {
        // +----+----+-----+
        // | a1 | b1 | c1  |
        // +----+----+-----+
        // | 1  | 1  | 100 |
        // | 2  | 2  | 200 |
        // +----+----+-----+
        let left = build_table(
            ("a1", &vec![1, 2]),
            ("b1", &vec![1, 2]),
            ("c1", &vec![100, 200]),
        );

        // +----+----+-----+
        // | a2 | b1 | c2  |
        // +----+----+-----+
        // | 10 | 3  | 300 |
        // | 20 | 2  | 400 |
        // +----+----+-----+
        let right = build_table(
            ("a2", &vec![10, 20]),
            ("b1", &vec![3, 2]),
            ("c2", &vec![300, 400]),
        );

        let on = (
            Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
            Arc::new(Column::new_with_schema("b1", &right.schema())?) as _,
        );

        let (_, batches, metrics) =
            join_collect(left, right, on, Operator::GtEq, JoinType::Full).await?;

        assert_snapshot!(batches_to_string(&batches), @r"
        +----+----+-----+----+----+-----+
        | a1 | b1 | c1  | a2 | b1 | c2  |
        +----+----+-----+----+----+-----+
        |    |    |     | 10 | 3  | 300 |
        | 2  | 2  | 200 | 20 | 2  | 400 |
        | 1  | 1  | 100 |    |    |     |
        +----+----+-----+----+----+-----+
        ");

        assert_join_metrics!(metrics, 3);
        Ok(())
    }

    #[tokio::test]
    async fn join_left_greater_than() -> Result<()> {
        // +----+----+----+
        // | a1 | b1 | c1 |
        // +----+----+----+
        // | 1  | 1  | 7  |
        // | 2  | 3  | 8  |
        // | 3  | 4  | 9  |
        // +----+----+----+
        let left = build_table(
            ("a1", &vec![1, 2, 3]),
            ("b1", &vec![1, 3, 4]),
            ("c1", &vec![7, 8, 9]),
        );

        // +----+----+----+
        // | a2 | b1 | c2 |
        // +----+----+----+
        // | 10 | 3  | 70 |
        // | 20 | 2  | 80 |
        // | 30 | 1  | 90 |
        // +----+----+----+
        let right = build_table(
            ("a2", &vec![10, 20, 30]),
            ("b1", &vec![3, 2, 1]),
            ("c2", &vec![70, 80, 90]),
        );

        let on = (
            Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
            Arc::new(Column::new_with_schema("b1", &right.schema())?) as _,
        );

        let (_, batches, metrics) =
            join_collect(left, right, on, Operator::Gt, JoinType::Left).await?;

        assert_snapshot!(batches_to_string(&batches), @r"
        +----+----+----+----+----+----+
        | a1 | b1 | c1 | a2 | b1 | c2 |
        +----+----+----+----+----+----+
        | 2  | 3  | 8  | 30 | 1  | 90 |
        | 3  | 4  | 9  | 30 | 1  | 90 |
        | 2  | 3  | 8  | 20 | 2  | 80 |
        | 3  | 4  | 9  | 20 | 2  | 80 |
        | 3  | 4  | 9  | 10 | 3  | 70 |
        | 1  | 1  | 7  |    |    |    |
        +----+----+----+----+----+----+
        ");

        assert_join_metrics!(metrics, 6);
        Ok(())
    }

    #[tokio::test]
    async fn join_right_greater_than() -> Result<()> {
        // +----+----+----+
        // | a1 | b1 | c1 |
        // +----+----+----+
        // | 1  | 1  | 7  |
        // | 2  | 3  | 8  |
        // | 3  | 4  | 9  |
        // +----+----+----+
        let left = build_table(
            ("a1", &vec![1, 2, 3]),
            ("b1", &vec![1, 3, 4]),
            ("c1", &vec![7, 8, 9]),
        );

        // +----+----+----+
        // | a2 | b1 | c2 |
        // +----+----+----+
        // | 10 | 5  | 70 |
        // | 20 | 3  | 80 |
        // | 30 | 2  | 90 |
        // +----+----+----+
        let right = build_table(
            ("a2", &vec![10, 20, 30]),
            ("b1", &vec![5, 3, 2]),
            ("c2", &vec![70, 80, 90]),
        );

        let on = (
            Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
            Arc::new(Column::new_with_schema("b1", &right.schema())?) as _,
        );

        let (_, batches, metrics) =
            join_collect(left, right, on, Operator::Gt, JoinType::Right).await?;

        assert_snapshot!(batches_to_string(&batches), @r"
        +----+----+----+----+----+----+
        | a1 | b1 | c1 | a2 | b1 | c2 |
        +----+----+----+----+----+----+
        |    |    |    | 10 | 5  | 70 |
        | 2  | 3  | 8  | 30 | 2  | 90 |
        | 3  | 4  | 9  | 30 | 2  | 90 |
        | 3  | 4  | 9  | 20 | 3  | 80 |
        +----+----+----+----+----+----+
        ");

        assert_join_metrics!(metrics, 4);
        Ok(())
    }

    #[tokio::test]
    async fn join_right_less_than() -> Result<()> {
        // +----+----+----+
        // | a1 | b1 | c1 |
        // +----+----+----+
        // | 1  | 4  | 7  |
        // | 2  | 3  | 8  |
        // | 3  | 1  | 9  |
        // +----+----+----+
        let left = build_table(
            ("a1", &vec![1, 2, 3]),
            ("b1", &vec![4, 3, 1]),
            ("c1", &vec![7, 8, 9]),
        );

        // +----+----+----+
        // | a2 | b1 | c2 |
        // +----+----+----+
        // | 10 | 2  | 70 |
        // | 20 | 3  | 80 |
        // | 30 | 5  | 90 |
        // +----+----+----+
        let right = build_table(
            ("a2", &vec![10, 20, 30]),
            ("b1", &vec![2, 3, 5]),
            ("c2", &vec![70, 80, 90]),
        );

        let on = (
            Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
            Arc::new(Column::new_with_schema("b1", &right.schema())?) as _,
        );

        let (_, batches, metrics) =
            join_collect(left, right, on, Operator::Lt, JoinType::Right).await?;

        assert_snapshot!(batches_to_string(&batches), @r"
        +----+----+----+----+----+----+
        | a1 | b1 | c1 | a2 | b1 | c2 |
        +----+----+----+----+----+----+
        | 1  | 4  | 7  | 30 | 5  | 90 |
        | 2  | 3  | 8  | 30 | 5  | 90 |
        | 3  | 1  | 9  | 30 | 5  | 90 |
        | 3  | 1  | 9  | 20 | 3  | 80 |
        | 3  | 1  | 9  | 10 | 2  | 70 |
        +----+----+----+----+----+----+
        ");

        assert_join_metrics!(metrics, 5);
        Ok(())
    }

    /// An empty streamed batch of run-end encoded keys, sliced past the first run, must not
    /// reach arrow's run-end comparison kernel, which overflows on it. The streamed NULL key
    /// (`a2 = 0`) is a logical NULL with no physical null buffer, and must come out unmatched
    /// rather than join every buffered row.
    #[tokio::test]
    async fn join_right_run_end_encoded_keys_empty_sliced_batch() -> Result<()> {
        use arrow::array::{Int32Array, RunArray};
        use arrow::datatypes::Int32Type;

        let ree_exec = |ids: &str,
                        keys: &str,
                        run_ends: Vec<i32>,
                        values: Vec<Option<i32>>,
                        slices: &[(usize, usize)]|
         -> Result<Arc<dyn ExecutionPlan>> {
            let keys_array = RunArray::<Int32Type>::try_new(
                &Int32Array::from(run_ends),
                &Int32Array::from(values),
            )?;
            let schema = Arc::new(Schema::new(vec![
                Field::new(ids, DataType::Int32, false),
                Field::new(keys, keys_array.data_type().clone(), true),
            ]));
            let len = keys_array.len() as i32;
            let batch = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(Int32Array::from((0..len).collect::<Vec<_>>())),
                    Arc::new(keys_array),
                ],
            )?;
            let batches = slices
                .iter()
                .map(|&(offset, len)| batch.slice(offset, len))
                .collect::<Vec<_>>();
            Ok(TestMemoryExec::try_new_exec(&[batches], schema, None)?)
        };
        // Buffered keys sorted for `<` (descending, NULLs first): {NULL, 5, 5, 1, 1}.
        let left = ree_exec(
            "a1",
            "b1",
            vec![1, 3, 5],
            vec![None, Some(5), Some(1)],
            &[(0, 5)],
        )?;
        // Streamed keys {NULL, 2, 2, 0, 6}, as batches of 3, 0 and 2 rows. The empty one
        // starts at offset 3, past the end of the first run.
        let right = ree_exec(
            "a2",
            "b2",
            vec![1, 3, 4, 5],
            vec![None, Some(2), Some(0), Some(6)],
            &[(0, 3), (3, 0), (3, 2)],
        )?;
        let on = (
            Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
            Arc::new(Column::new_with_schema("b2", &right.schema())?) as _,
        );

        let (_, batches, _) =
            join_collect(left, right, on, Operator::Lt, JoinType::Right).await?;
        assert_snapshot!(batches_to_string(&batches), @r"
        +----+----+----+----+
        | a1 | b1 | a2 | b2 |
        +----+----+----+----+
        |    |    | 0  |    |
        | 3  | 1  | 1  | 2  |
        | 4  | 1  | 1  | 2  |
        | 3  | 1  | 2  | 2  |
        | 4  | 1  | 2  | 2  |
        |    |    | 3  | 0  |
        | 1  | 5  | 4  | 6  |
        | 2  | 5  | 4  | 6  |
        | 3  | 1  | 4  | 6  |
        | 4  | 1  | 4  | 6  |
        +----+----+----+----+
        ");
        Ok(())
    }

    /// `buffered_extreme` is `None` when the last buffered key is logically NULL, including
    /// run-end encoded, `Null`-typed and dictionary keys, which have no physical NULL there.
    #[test]
    fn buffered_extreme_is_none_for_logically_null_keys() -> Result<()> {
        use arrow::array::{DictionaryArray, Int32Array, NullArray, RunArray};
        use arrow::datatypes::Int32Type;

        let null_keys: Vec<(&str, ArrayRef)> = vec![
            ("null", Arc::new(NullArray::new(3))),
            (
                "run_end_encoded",
                Arc::new(RunArray::<Int32Type>::try_new(
                    &Int32Array::from(vec![3]),
                    &Int32Array::from(vec![None::<i32>]),
                )?),
            ),
            (
                "dictionary_null_value",
                Arc::new(DictionaryArray::<Int32Type>::try_new(
                    Int32Array::from(vec![0, 0]),
                    Arc::new(Int32Array::from(vec![None::<i32>])),
                )?),
            ),
        ];
        for (name, values) in null_keys {
            assert!(buffered_extreme(&values)?.is_none(), "{name}");
        }

        let values: ArrayRef = Arc::new(Int32Array::from(vec![None, Some(3)]));
        assert!(matches!(
            buffered_extreme(&values)?,
            Some(ColumnarValue::Scalar(ScalarValue::Int32(Some(3))))
        ));
        Ok(())
    }

    /// `matchable_rows` against the scan's own definition of a match -- some non-NULL
    /// buffered row `idx` with `is_match(cmp.compare(row, idx))` under the scan's
    /// `JoinKeyComparator` -- for every operator and every kind of key it dispatches on:
    /// flat keys through `apply_cmp` (incl. signed zeros, NaN, strings, dictionaries) and
    /// nested keys with NULL elements through the comparator. Each is also checked against
    /// an all-NULL and an empty buffered side.
    #[test]
    fn matchable_rows_agrees_with_scan() -> Result<()> {
        use arrow::array::{
            BinaryArray, BooleanArray, Decimal128Array, DictionaryArray, Float64Array,
            Int32Array, ListArray, StringArray, StringViewArray,
            TimestampMicrosecondArray,
        };
        use arrow::datatypes::Int32Type;

        let strings =
            |v: &[Option<&str>]| Arc::new(StringArray::from(v.to_vec())) as ArrayRef;
        let cases: Vec<(&str, ArrayRef, ArrayRef)> = vec![
            (
                "int32",
                Arc::new(Int32Array::from(vec![
                    Some(5),
                    None,
                    Some(1),
                    Some(9),
                    Some(3),
                ])),
                Arc::new(Int32Array::from(vec![
                    Some(0),
                    Some(1),
                    Some(2),
                    Some(3),
                    Some(5),
                    Some(9),
                    Some(10),
                    None,
                ])),
            ),
            (
                "float64",
                Arc::new(Float64Array::from(vec![
                    Some(0.0),
                    Some(-0.0),
                    Some(1.5),
                    Some(f64::NAN),
                    None,
                ])),
                Arc::new(Float64Array::from(vec![
                    Some(-0.0),
                    Some(0.0),
                    Some(1.5),
                    Some(f64::NAN),
                    Some(-1.0),
                    Some(2.0),
                    Some(f64::INFINITY),
                    Some(f64::NEG_INFINITY),
                    None,
                ])),
            ),
            (
                "utf8",
                strings(&[Some("b"), Some(""), Some("d"), None]),
                strings(&[
                    Some(""),
                    Some("a"),
                    Some("b"),
                    Some("c"),
                    Some("d"),
                    Some("e"),
                    None,
                ]),
            ),
            (
                "utf8_view",
                Arc::new(StringViewArray::from(vec![
                    Some("b"),
                    Some(""),
                    Some("d"),
                    None,
                ])),
                Arc::new(StringViewArray::from(vec![
                    Some(""),
                    Some("a"),
                    Some("b"),
                    Some("d"),
                    Some("e"),
                    None,
                ])),
            ),
            (
                "binary",
                Arc::new(BinaryArray::from(vec![Some(&b"b"[..]), Some(b""), None])),
                Arc::new(BinaryArray::from(vec![
                    Some(&b""[..]),
                    Some(b"a"),
                    Some(b"b"),
                    Some(b"c"),
                    None,
                ])),
            ),
            (
                "dictionary",
                Arc::new(
                    vec![Some("b"), None, Some("d"), Some("b")]
                        .into_iter()
                        .collect::<DictionaryArray<Int32Type>>(),
                ),
                Arc::new(
                    vec![Some("a"), Some("b"), Some("c"), Some("d"), Some("e"), None]
                        .into_iter()
                        .collect::<DictionaryArray<Int32Type>>(),
                ),
            ),
            (
                // `-0.0` as the buffered extreme inside a dictionary: the smallest
                // key for `<`/`<=`, the largest for `>`/`>=`.
                "dictionary_float64_neg_zero_min",
                Arc::new(DictionaryArray::<Int32Type>::new(
                    Int32Array::from(vec![0, 1]),
                    Arc::new(Float64Array::from(vec![-0.0, 0.5])),
                )),
                Arc::new(DictionaryArray::<Int32Type>::new(
                    Int32Array::from(vec![0, 1, 2]),
                    Arc::new(Float64Array::from(vec![0.0, -0.0, 0.25])),
                )),
            ),
            (
                "dictionary_float64_neg_zero_max",
                Arc::new(DictionaryArray::<Int32Type>::new(
                    Int32Array::from(vec![0, 1]),
                    Arc::new(Float64Array::from(vec![-1.0, -0.0])),
                )),
                Arc::new(DictionaryArray::<Int32Type>::new(
                    Int32Array::from(vec![0, 1, 2]),
                    Arc::new(Float64Array::from(vec![0.0, -0.0, -0.5])),
                )),
            ),
            (
                // NULL *values* behind valid dictionary keys, on both sides.
                "dictionary_null_values",
                Arc::new(DictionaryArray::<Int32Type>::new(
                    Int32Array::from(vec![0, 1, 2]),
                    Arc::new(StringArray::from(vec![Some("b"), None, Some("d")])),
                )),
                Arc::new(DictionaryArray::<Int32Type>::new(
                    Int32Array::from(vec![0, 1, 2, 3, 1]),
                    Arc::new(StringArray::from(vec![
                        Some("a"),
                        None,
                        Some("c"),
                        Some("e"),
                    ])),
                )),
            ),
            (
                "decimal128",
                Arc::new(
                    Decimal128Array::from(vec![Some(150), None, Some(-25)])
                        .with_precision_and_scale(10, 2)?,
                ),
                Arc::new(
                    Decimal128Array::from(vec![
                        Some(-26),
                        Some(-25),
                        Some(0),
                        Some(150),
                        Some(151),
                        None,
                    ])
                    .with_precision_and_scale(10, 2)?,
                ),
            ),
            (
                "date32",
                Arc::new(Date32Array::from(vec![Some(10), None, Some(20)])),
                Arc::new(Date32Array::from(vec![
                    Some(9),
                    Some(10),
                    Some(15),
                    Some(20),
                    Some(21),
                    None,
                ])),
            ),
            (
                "timestamp_us",
                Arc::new(TimestampMicrosecondArray::from(vec![
                    Some(-5),
                    Some(7),
                    None,
                ])),
                Arc::new(TimestampMicrosecondArray::from(vec![
                    Some(-6),
                    Some(-5),
                    Some(7),
                    Some(8),
                    None,
                ])),
            ),
            (
                "boolean",
                Arc::new(BooleanArray::from(vec![Some(true), None])),
                Arc::new(BooleanArray::from(vec![Some(false), Some(true), None])),
            ),
            (
                "list_with_null_elements",
                Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
                    Some(vec![Some(5)]),
                    Some(vec![None]),
                    Some(vec![Some(1), Some(2)]),
                    Some(vec![]),
                    None,
                ])),
                Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
                    Some(vec![None]),
                    Some(vec![Some(7)]),
                    Some(vec![Some(1)]),
                    Some(vec![Some(5)]),
                    Some(vec![]),
                    Some(vec![None, Some(1)]),
                    Some(vec![Some(5), None]),
                    None,
                ])),
            ),
            (
                // No empty list, so the extreme key has a first element to compare
                // against the streamed NULL elements.
                "list_null_elements_vs_extreme",
                Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
                    Some(vec![Some(5)]),
                    Some(vec![Some(3)]),
                    None,
                ])),
                Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
                    Some(vec![None]),
                    Some(vec![Some(3), None]),
                    Some(vec![Some(4)]),
                    Some(vec![Some(7)]),
                    Some(vec![Some(1)]),
                    None,
                ])),
            ),
        ];

        for (name, buffered, streamed) in cases {
            let all_null = new_null_array(buffered.data_type(), 3);
            let empty = buffered.slice(0, 0);
            for (side, buffered) in
                [("full", buffered), ("all_null", all_null), ("empty", empty)]
            {
                for operator in
                    [Operator::Lt, Operator::LtEq, Operator::Gt, Operator::GtEq]
                {
                    // As `PiecewiseMergeJoinExec::try_new` sorts both sides.
                    let sort_options = match operator {
                        Operator::Lt | Operator::LtEq => SortOptions::new(true, true),
                        _ => SortOptions::new(false, true),
                    };
                    let sorted = take(
                        buffered.as_ref(),
                        &sort_to_indices(buffered.as_ref(), Some(sort_options), None)?,
                        None,
                    )?;
                    let extreme = buffered_extreme(&sorted)?;

                    let actual = matchable_rows(
                        &streamed,
                        &sorted,
                        extreme.as_ref(),
                        operator,
                        sort_options,
                    )?;

                    let match_on_equal = matches_on_equal(operator)?;
                    let cmp = JoinKeyComparator::new(
                        &[Arc::clone(&streamed)],
                        &[Arc::clone(&sorted)],
                        &[sort_options],
                        NullEquality::NullEqualsNothing,
                    )?;
                    // Logical NULLs, so a dictionary key whose *value* is NULL counts as one.
                    let nulls = streamed.logical_nulls();
                    for row in 0..streamed.len() {
                        let expected = nulls.as_ref().is_none_or(|n| n.is_valid(row))
                            && (sorted.logical_null_count()..sorted.len()).any(|idx| {
                                is_match(cmp.compare(row, idx), match_on_equal)
                            });
                        assert_eq!(
                            actual.value(row),
                            expected,
                            "{name}/{side} {operator}: streamed row {row}"
                        );
                    }
                }
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn join_inner_less_than_equal_with_dups() -> Result<()> {
        // +----+----+----+
        // | a1 | b1 | c1 |
        // +----+----+----+
        // | 1  | 4  | 7  |
        // | 2  | 4  | 8  |
        // | 3  | 2  | 9  |
        // +----+----+----+
        let left = build_table(
            ("a1", &vec![1, 2, 3]),
            ("b1", &vec![4, 4, 2]),
            ("c1", &vec![7, 8, 9]),
        );

        // +----+----+----+
        // | a2 | b1 | c2 |
        // +----+----+----+
        // | 10 | 4  | 70 |
        // | 20 | 3  | 80 |
        // | 30 | 2  | 90 |
        // +----+----+----+
        let right = build_table(
            ("a2", &vec![10, 20, 30]),
            ("b1", &vec![4, 3, 2]),
            ("c2", &vec![70, 80, 90]),
        );

        let on = (
            Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
            Arc::new(Column::new_with_schema("b1", &right.schema())?) as _,
        );

        let (_, batches, metrics) =
            join_collect(left, right, on, Operator::LtEq, JoinType::Inner).await?;

        // Expected grouping follows right.b1 descending (4, 3, 2)
        assert_snapshot!(batches_to_string(&batches), @r"
        +----+----+----+----+----+----+
        | a1 | b1 | c1 | a2 | b1 | c2 |
        +----+----+----+----+----+----+
        | 1  | 4  | 7  | 10 | 4  | 70 |
        | 2  | 4  | 8  | 10 | 4  | 70 |
        | 3  | 2  | 9  | 10 | 4  | 70 |
        | 3  | 2  | 9  | 20 | 3  | 80 |
        | 3  | 2  | 9  | 30 | 2  | 90 |
        +----+----+----+----+----+----+
        ");

        assert_join_metrics!(metrics, 5);
        Ok(())
    }

    #[tokio::test]
    async fn join_inner_greater_than_unsorted_right() -> Result<()> {
        // +----+----+----+
        // | a1 | b1 | c1 |
        // +----+----+----+
        // | 1  | 1  | 7  |
        // | 2  | 2  | 8  |
        // | 3  | 4  | 9  |
        // +----+----+----+
        let left = build_table(
            ("a1", &vec![1, 2, 3]),
            ("b1", &vec![1, 2, 4]),
            ("c1", &vec![7, 8, 9]),
        );

        // +----+----+----+
        // | a2 | b1 | c2 |
        // +----+----+----+
        // | 10 | 3  | 70 |
        // | 20 | 1  | 80 |
        // | 30 | 2  | 90 |
        // +----+----+----+
        let right = build_table(
            ("a2", &vec![10, 20, 30]),
            ("b1", &vec![3, 1, 2]),
            ("c2", &vec![70, 80, 90]),
        );

        let on = (
            Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
            Arc::new(Column::new_with_schema("b1", &right.schema())?) as _,
        );

        let (_, batches, metrics) =
            join_collect(left, right, on, Operator::Gt, JoinType::Inner).await?;

        // Grouped by right in ascending evaluation for > (1,2,3)
        assert_snapshot!(batches_to_string(&batches), @r"
        +----+----+----+----+----+----+
        | a1 | b1 | c1 | a2 | b1 | c2 |
        +----+----+----+----+----+----+
        | 2  | 2  | 8  | 20 | 1  | 80 |
        | 3  | 4  | 9  | 20 | 1  | 80 |
        | 3  | 4  | 9  | 30 | 2  | 90 |
        | 3  | 4  | 9  | 10 | 3  | 70 |
        +----+----+----+----+----+----+
        ");

        assert_join_metrics!(metrics, 4);
        Ok(())
    }

    #[tokio::test]
    async fn join_left_less_than_equal_with_left_nulls_on_no_match() -> Result<()> {
        // +----+----+----+
        // | a1 | b1 | c1 |
        // +----+----+----+
        // | 1  | 5  | 7  |
        // | 2  | 4  | 8  |
        // | 3  | 1  | 9  |
        // +----+----+----+
        let left = build_table(
            ("a1", &vec![1, 2, 3]),
            ("b1", &vec![5, 4, 1]),
            ("c1", &vec![7, 8, 9]),
        );

        // +----+----+----+
        // | a2 | b1 | c2 |
        // +----+----+----+
        // | 10 | 3  | 70 |
        // +----+----+----+
        let right = build_table(("a2", &vec![10]), ("b1", &vec![3]), ("c2", &vec![70]));

        let on = (
            Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
            Arc::new(Column::new_with_schema("b1", &right.schema())?) as _,
        );

        let task_ctx = Arc::new(
            TaskContext::default()
                .with_session_config(SessionConfig::new().with_batch_size(1)),
        );
        // Bound collection so the old duplicate loop becomes a snapshot mismatch.
        let batches = join(left, right, on, Operator::LtEq, JoinType::Left)?
            .execute(0, task_ctx)?
            .take(6)
            .try_collect::<Vec<_>>()
            .await?;

        assert_snapshot!(batches_to_string(&batches), @r"
        +----+----+----+----+----+----+
        | a1 | b1 | c1 | a2 | b1 | c2 |
        +----+----+----+----+----+----+
        | 3  | 1  | 9  | 10 | 3  | 70 |
        | 1  | 5  | 7  |    |    |    |
        | 2  | 4  | 8  |    |    |    |
        +----+----+----+----+----+----+
        ");
        Ok(())
    }

    #[tokio::test]
    async fn join_right_greater_than_equal_with_right_nulls_on_no_match() -> Result<()> {
        // +----+----+----+
        // | a1 | b1 | c1 |
        // +----+----+----+
        // | 1  | 1  | 7  |
        // | 2  | 2  | 8  |
        // +----+----+----+
        let left = build_table(
            ("a1", &vec![1, 2]),
            ("b1", &vec![1, 2]),
            ("c1", &vec![7, 8]),
        );

        // +----+----+----+
        // | a2 | b1 | c2 |
        // +----+----+----+
        // | 10 | 3  | 70 |
        // | 20 | 5  | 80 |
        // +----+----+----+
        let right = build_table(
            ("a2", &vec![10, 20]),
            ("b1", &vec![3, 5]),
            ("c2", &vec![70, 80]),
        );

        let on = (
            Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
            Arc::new(Column::new_with_schema("b1", &right.schema())?) as _,
        );

        let task_ctx = Arc::new(
            TaskContext::default()
                .with_session_config(SessionConfig::new().with_batch_size(1)),
        );
        let join = join(left, right, on, Operator::GtEq, JoinType::Right)?;
        let batches = common::collect(join.execute(0, task_ctx)?).await?;

        assert_snapshot!(batches_to_string(&batches), @r"
        +----+----+----+----+----+----+
        | a1 | b1 | c1 | a2 | b1 | c2 |
        +----+----+----+----+----+----+
        |    |    |    | 10 | 3  | 70 |
        |    |    |    | 20 | 5  | 80 |
        +----+----+----+----+----+----+
        ");

        let metrics = join.metrics().expect("metrics should be available");
        assert_join_metrics!(metrics, 2);
        assert!(
            metrics
                .sum_by_name("join_time")
                .expect("join_time metric")
                .as_usize()
                > 0
        );
        Ok(())
    }

    #[tokio::test]
    async fn join_inner_single_row_left_less_than() -> Result<()> {
        let left = build_table(("a1", &vec![42]), ("b1", &vec![5]), ("c1", &vec![999]));

        let right = build_table(
            ("a2", &vec![10, 20, 30]),
            ("b1", &vec![1, 5, 7]),
            ("c2", &vec![70, 80, 90]),
        );

        let on = (
            Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
            Arc::new(Column::new_with_schema("b1", &right.schema())?) as _,
        );

        let (_, batches, metrics) =
            join_collect(left, right, on, Operator::Lt, JoinType::Inner).await?;

        assert_snapshot!(batches_to_string(&batches), @r"
        +----+----+-----+----+----+----+
        | a1 | b1 | c1  | a2 | b1 | c2 |
        +----+----+-----+----+----+----+
        | 42 | 5  | 999 | 30 | 7  | 90 |
        +----+----+-----+----+----+----+
        ");

        assert_join_metrics!(metrics, 1);
        Ok(())
    }

    #[tokio::test]
    async fn join_inner_empty_right() -> Result<()> {
        let left = build_table(
            ("a1", &vec![1, 2, 3]),
            ("b1", &vec![1, 2, 3]),
            ("c1", &vec![7, 8, 9]),
        );

        let right = build_table(
            ("a2", &Vec::<i32>::new()),
            ("b1", &Vec::<i32>::new()),
            ("c2", &Vec::<i32>::new()),
        );

        let on = (
            Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
            Arc::new(Column::new_with_schema("b1", &right.schema())?) as _,
        );

        let (_, batches, metrics) =
            join_collect(left, right, on, Operator::Gt, JoinType::Inner).await?;

        // An empty join result produces no batches at all, not an empty batch.
        assert!(batches.is_empty());

        assert_join_metrics!(metrics, 0);
        Ok(())
    }

    #[tokio::test]
    async fn join_date32_inner_less_than() -> Result<()> {
        // +----+-------+----+
        // | a1 |  b1   | c1 |
        // +----+-------+----+
        // | 1  | 19107 | 7  |
        // | 2  | 19107 | 8  |
        // | 3  | 19105 | 9  |
        // +----+-------+----+
        let left = build_date_table(
            ("a1", &vec![1, 2, 3]),
            ("b1", &vec![19107, 19107, 19105]),
            ("c1", &vec![7, 8, 9]),
        );

        // +----+-------+----+
        // | a2 |  b1   | c2 |
        // +----+-------+----+
        // | 10 | 19105 | 70 |
        // | 20 | 19103 | 80 |
        // | 30 | 19107 | 90 |
        // +----+-------+----+
        let right = build_date_table(
            ("a2", &vec![10, 20, 30]),
            ("b1", &vec![19105, 19103, 19107]),
            ("c2", &vec![70, 80, 90]),
        );

        let on = (
            Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
            Arc::new(Column::new_with_schema("b1", &right.schema())?) as _,
        );

        let (_, batches, metrics) =
            join_collect(left, right, on, Operator::Lt, JoinType::Inner).await?;

        assert_snapshot!(batches_to_string(&batches), @r"
        +------------+------------+------------+------------+------------+------------+
        | a1         | b1         | c1         | a2         | b1         | c2         |
        +------------+------------+------------+------------+------------+------------+
        | 1970-01-04 | 2022-04-23 | 1970-01-10 | 1970-01-31 | 2022-04-25 | 1970-04-01 |
        +------------+------------+------------+------------+------------+------------+
        ");

        assert_join_metrics!(metrics, 1);
        Ok(())
    }

    #[tokio::test]
    async fn join_date64_inner_less_than() -> Result<()> {
        // +----+---------------+----+
        // | a1 |     b1        | c1 |
        // +----+---------------+----+
        // | 1  | 1650903441000 |  7 |
        // | 2  | 1650903441000 |  8 |
        // | 3  | 1650703441000 |  9 |
        // +----+---------------+----+
        let left = build_date64_table(
            ("a1", &vec![1, 2, 3]),
            ("b1", &vec![1650903441000, 1650903441000, 1650703441000]),
            ("c1", &vec![7, 8, 9]),
        );

        // +----+---------------+----+
        // | a2 |     b1        | c2 |
        // +----+---------------+----+
        // | 10 | 1650703441000 | 70 |
        // | 20 | 1650503441000 | 80 |
        // | 30 | 1650903441000 | 90 |
        // +----+---------------+----+
        let right = build_date64_table(
            ("a2", &vec![10, 20, 30]),
            ("b1", &vec![1650703441000, 1650503441000, 1650903441000]),
            ("c2", &vec![70, 80, 90]),
        );

        let on = (
            Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
            Arc::new(Column::new_with_schema("b1", &right.schema())?) as _,
        );

        let (_, batches, metrics) =
            join_collect(left, right, on, Operator::Lt, JoinType::Inner).await?;

        assert_snapshot!(batches_to_string(&batches), @r"
        +-------------------------+---------------------+-------------------------+-------------------------+---------------------+-------------------------+
        | a1                      | b1                  | c1                      | a2                      | b1                  | c2                      |
        +-------------------------+---------------------+-------------------------+-------------------------+---------------------+-------------------------+
        | 1970-01-01T00:00:00.003 | 2022-04-23T08:44:01 | 1970-01-01T00:00:00.009 | 1970-01-01T00:00:00.030 | 2022-04-25T16:17:21 | 1970-01-01T00:00:00.090 |
        +-------------------------+---------------------+-------------------------+-------------------------+---------------------+-------------------------+
        ");

        assert_join_metrics!(metrics, 1);
        Ok(())
    }

    #[tokio::test]
    async fn join_date64_right_less_than() -> Result<()> {
        // +----+---------------+----+
        // | a1 |     b1        | c1 |
        // +----+---------------+----+
        // | 1  | 1650903441000 |  7 |
        // | 2  | 1650703441000 |  8 |
        // +----+---------------+----+
        let left = build_date64_table(
            ("a1", &vec![1, 2]),
            ("b1", &vec![1650903441000, 1650703441000]),
            ("c1", &vec![7, 8]),
        );

        // +----+---------------+----+
        // | a2 |     b1        | c2 |
        // +----+---------------+----+
        // | 10 | 1650703441000 | 80 |
        // | 20 | 1650903441000 | 90 |
        // +----+---------------+----+
        let right = build_date64_table(
            ("a2", &vec![10, 20]),
            ("b1", &vec![1650703441000, 1650903441000]),
            ("c2", &vec![80, 90]),
        );

        let on = (
            Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
            Arc::new(Column::new_with_schema("b1", &right.schema())?) as _,
        );

        let (_, batches, metrics) =
            join_collect(left, right, on, Operator::Lt, JoinType::Right).await?;

        assert_snapshot!(batches_to_string(&batches), @r"
        +-------------------------+---------------------+-------------------------+-------------------------+---------------------+-------------------------+
        | a1                      | b1                  | c1                      | a2                      | b1                  | c2                      |
        +-------------------------+---------------------+-------------------------+-------------------------+---------------------+-------------------------+
        |                         |                     |                         | 1970-01-01T00:00:00.010 | 2022-04-23T08:44:01 | 1970-01-01T00:00:00.080 |
        | 1970-01-01T00:00:00.002 | 2022-04-23T08:44:01 | 1970-01-01T00:00:00.008 | 1970-01-01T00:00:00.020 | 2022-04-25T16:17:21 | 1970-01-01T00:00:00.090 |
        +-------------------------+---------------------+-------------------------+-------------------------+---------------------+-------------------------+
        ");

        assert_join_metrics!(metrics, 2);
        Ok(())
    }

    /// Wraps an input plan and sleeps `delay` before yielding each of its batches, to
    /// simulate a slow streamed input.
    #[derive(Debug)]
    struct DelayedExec {
        input: Arc<dyn ExecutionPlan>,
        delay: Duration,
    }

    impl DisplayAs for DelayedExec {
        fn fmt_as(
            &self,
            _t: DisplayFormatType,
            f: &mut std::fmt::Formatter,
        ) -> std::fmt::Result {
            write!(f, "DelayedExec")
        }
    }

    impl ExecutionPlan for DelayedExec {
        fn name(&self) -> &str {
            "DelayedExec"
        }

        fn properties(&self) -> &Arc<PlanProperties> {
            self.input.properties()
        }

        fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
            vec![&self.input]
        }

        fn apply_expressions(
            &self,
            _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
        ) -> Result<TreeNodeRecursion> {
            Ok(TreeNodeRecursion::Continue)
        }

        fn with_new_children(
            self: Arc<Self>,
            children: Vec<Arc<dyn ExecutionPlan>>,
        ) -> Result<Arc<dyn ExecutionPlan>> {
            Ok(Arc::new(Self {
                input: Arc::clone(&children[0]),
                delay: self.delay,
            }))
        }

        fn execute(
            &self,
            partition: usize,
            context: Arc<TaskContext>,
        ) -> Result<SendableRecordBatchStream> {
            let stream = self.input.execute(partition, context)?;
            let schema = stream.schema();
            let delay = self.delay;
            Ok(Box::pin(RecordBatchStreamAdapter::new(
                schema,
                stream.then(move |item| async move {
                    tokio::time::sleep(delay).await;
                    item
                }),
            )))
        }
    }

    fn join_time_of(metrics: &MetricsSet) -> Duration {
        Duration::from_nanos(
            metrics
                .sum_by_name("join_time")
                .map(|m| m.as_usize())
                .unwrap_or(0) as u64,
        )
    }

    /// `join_time` must not include time spent waiting for the streamed input: the timer
    /// in `fetch_stream_batch` starts only once the input's poll returns `Ready`.
    ///
    /// Retries with 4x the delay (up to 3 attempts) when the `join_time < delay` check
    /// fails. This de-flakes the check without masking real bugs: a genuine exclusion bug
    /// makes `join_time` absorb the injected waits, so it scales with the delay and fails
    /// at every escalation level, while a fixed-size disturbance (e.g. the OS preempting
    /// the thread while a `join_time` timer is running) cannot grow 4x with it.
    /// Deterministic invariants (row count, wall-time lower bound) are asserted on every
    /// run and never retried. Mirrors the sort-merge join's `check_join_time_excluded`.
    #[tokio::test]
    async fn join_time_excludes_streamed_input_wait() -> Result<()> {
        let mut delay = Duration::from_millis(50);
        for attempt in 0..3 {
            // Same data as `join_inner_less_than`, with the streamed side split into
            // three batches so each one incurs the injected delay.
            let left = build_table(
                ("a1", &vec![1, 2, 3]),
                ("b1", &vec![3, 2, 1]),
                ("c1", &vec![7, 8, 9]),
            );

            let streamed_schema = Schema::new(vec![
                Field::new("a2", DataType::Int32, false),
                Field::new("b1", DataType::Int32, false),
                Field::new("c2", DataType::Int32, false),
            ]);
            let streamed_batches = vec![
                build_table_i32(("a2", &vec![10]), ("b1", &vec![2]), ("c2", &vec![70])),
                build_table_i32(("a2", &vec![20]), ("b1", &vec![3]), ("c2", &vec![80])),
                build_table_i32(("a2", &vec![30]), ("b1", &vec![4]), ("c2", &vec![90])),
            ];
            let right_mem = TestMemoryExec::try_new_exec(
                &[streamed_batches],
                Arc::new(streamed_schema),
                None,
            )?;
            let right = Arc::new(DelayedExec {
                input: right_mem,
                delay,
            });

            let on = (
                Arc::new(Column::new_with_schema("b1", &left.schema())?) as _,
                Arc::new(Column::new_with_schema("b1", &right.schema())?) as _,
            );
            let join = join(left, right, on, Operator::Lt, JoinType::Inner)?;

            let start = Instant::now();
            let batches =
                common::collect(join.execute(0, Arc::new(TaskContext::default()))?)
                    .await?;
            let wall = start.elapsed();

            let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
            assert_eq!(rows, 6, "all streamed rows should find their matches");
            assert!(
                wall >= delay * 3,
                "streamed delays should dominate wall time, got {wall:?}"
            );

            let join_time =
                join_time_of(&join.metrics().expect("metrics should be available"));
            if join_time < delay {
                return Ok(());
            }
            assert!(
                attempt < 2,
                "join_time ({join_time:?}) should be well below the injected \
                 delay ({delay:?}) even after escalating retries; wall {wall:?}"
            );
            delay *= 4;
        }
        unreachable!()
    }
}
