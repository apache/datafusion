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

use arrow::array::{
    new_null_array, Array, PrimitiveArray, PrimitiveBuilder, RecordBatchOptions,
};
use arrow::compute::take;
use arrow::datatypes::{UInt32Type, UInt64Type};
use arrow::{
    array::{
        ArrayRef, RecordBatch, UInt32Array, UInt32Builder, UInt64Array, UInt64Builder,
    },
    compute::{sort_to_indices, take_record_batch},
};
use arrow_schema::{ArrowError, Schema, SchemaRef, SortOptions};
use datafusion_common::NullEquality;
use datafusion_common::{exec_err, internal_err, Result};
use datafusion_execution::{RecordBatchStream, SendableRecordBatchStream};
use datafusion_expr::{JoinType, Operator};
use datafusion_physical_expr::PhysicalExprRef;
use futures::{Stream, StreamExt};
use std::{cmp::Ordering, task::ready};
use std::{sync::Arc, task::Poll};

use crate::handle_state;
use crate::joins::piecewise_merge_join::exec::{BufferedSide, BufferedSideReadyState};
use crate::joins::piecewise_merge_join::utils::need_produce_result_in_final;
use crate::joins::utils::{compare_join_arrays, get_final_indices_from_shared_bitmap};
use crate::joins::utils::{BuildProbeJoinMetrics, StatefulStreamResult};
pub(super) enum PiecewiseMergeJoinStreamState {
    WaitBufferedSide,
    FetchStreamBatch,
    ProcessStreamBatch(StreamedBatch),
    ExhaustedStreamSide,
    Completed,
}

impl PiecewiseMergeJoinStreamState {
    // Grab mutable reference to the current stream batch
    fn try_as_process_stream_batch_mut(&mut self) -> Result<&mut StreamedBatch> {
        match self {
            PiecewiseMergeJoinStreamState::ProcessStreamBatch(state) => Ok(state),
            _ => internal_err!("Expected streamed batch in StreamBatch"),
        }
    }
}

pub(super) struct StreamedBatch {
    pub batch: RecordBatch,
    values: Vec<ArrayRef>,
}

impl StreamedBatch {
    #[allow(dead_code)]
    fn new(batch: RecordBatch, values: Vec<ArrayRef>) -> Self {
        Self { batch, values }
    }

    fn values(&self) -> &Vec<ArrayRef> {
        &self.values
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
    // Sort option for buffered and streamed side (specifies whether
    // the sort is ascending or descending)
    sort_option: SortOptions,
    // Metrics for build + probe joins
    join_metrics: BuildProbeJoinMetrics,
    // Tracking incremental state for emitting record batches
    batch_process_state: BatchProcessState,
    // Creates batch size
    batch_size: usize,
}

impl RecordBatchStream for ClassicPWMJStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

// `PiecewiseMergeJoinStreamState` is separated into `WaitBufferedSide`, `FetchStreamBatch`,
// `ProcessStreamBatch`, `ExhaustedStreamSide` and `Completed`.
//
// Classic Joins
//  1. `WaitBufferedSide` - Load in the buffered side data into memory.
//  2. `FetchStreamBatch` -  Fetch + sort incoming stream batches. We switch the state to
//      `ExhaustedStreamBatch` once stream batches are exhausted.
//  3. `ProcessStreamBatch` - Compare stream batch row values against the buffered side data.
//  4. `ExhaustedStreamBatch` - If the join type is Left or Inner we will return state as
//      `Completed` however for Full and Right we will need to process the matched/unmatched rows.
impl ClassicPWMJStream {
    // Creates a new `PiecewiseMergeJoinStream` instance
    #[allow(clippy::too_many_arguments)]
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
        let streamed_schema = streamed.schema();
        Self {
            schema,
            on_streamed,
            join_type,
            operator,
            streamed_schema,
            streamed,
            buffered_side,
            state,
            sort_option,
            join_metrics,
            batch_process_state: BatchProcessState::new(),
            batch_size,
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
                PiecewiseMergeJoinStreamState::ExhaustedStreamSide => {
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
        let buffered_data = ready!(self
            .buffered_side
            .try_as_initial_mut()?
            .buffered_fut
            .get_shared(cx))?;
        build_timer.done();

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
        match ready!(self.streamed.poll_next_unpin(cx)) {
            None => {
                self.state = PiecewiseMergeJoinStreamState::ExhaustedStreamSide;
            }
            Some(Ok(batch)) => {
                // Evaluate the streamed physical expression on the stream batch
                let stream_values: ArrayRef = self
                    .on_streamed
                    .evaluate(&batch)?
                    .into_array(batch.num_rows())?;

                self.join_metrics.input_batches.add(1);
                self.join_metrics.input_rows.add(batch.num_rows());

                // Sort stream values and change the streamed record batch accordingly
                let indices = sort_to_indices(
                    stream_values.as_ref(),
                    Some(self.sort_option),
                    None,
                )?;
                let stream_batch = take_record_batch(&batch, &indices)?;
                let stream_values = take(stream_values.as_ref(), &indices, None)?;

                self.state =
                    PiecewiseMergeJoinStreamState::ProcessStreamBatch(StreamedBatch {
                        batch: stream_batch,
                        values: vec![stream_values],
                    });
            }
            Some(Err(err)) => return Poll::Ready(Err(err)),
        };

        Poll::Ready(Ok(StatefulStreamResult::Continue))
    }

    // Only classic join will call. This function will process stream batches and evaluate against
    // the buffered side data.
    fn process_stream_batch(
        &mut self,
    ) -> Result<StatefulStreamResult<Option<RecordBatch>>> {
        let buffered_side = self.buffered_side.try_as_ready_mut()?;
        let stream_batch = self.state.try_as_process_stream_batch_mut()?;

        let batch = resolve_classic_join(
            buffered_side,
            stream_batch,
            Arc::clone(&self.schema),
            self.operator,
            self.sort_option,
            self.join_type,
            &mut self.batch_process_state,
            self.batch_size,
        )?;

        if self.batch_process_state.continue_process {
            return Ok(StatefulStreamResult::Ready(Some(batch)));
        }

        self.state = PiecewiseMergeJoinStreamState::FetchStreamBatch;
        Ok(StatefulStreamResult::Ready(Some(batch)))
    }

    // Process remaining unmatched rows
    fn process_unmatched_buffered_batch(
        &mut self,
    ) -> Result<StatefulStreamResult<Option<RecordBatch>>> {
        // Return early for `JoinType::Right` and `JoinType::Inner`
        if matches!(self.join_type, JoinType::Right | JoinType::Inner) {
            self.state = PiecewiseMergeJoinStreamState::Completed;
            return Ok(StatefulStreamResult::Ready(None));
        }

        let timer = self.join_metrics.join_time.timer();

        let buffered_data =
            Arc::clone(&self.buffered_side.try_as_ready().unwrap().buffered_data);

        // Check if the same batch needs to be checked for values again
        if let Some(start_idx) = self.batch_process_state.process_rest {
            if let Some(buffered_indices) = &self.batch_process_state.buffered_indices {
                let remaining = buffered_indices.len() - start_idx;

                // Branch into this and return value if there are more rows to deal with
                if remaining > self.batch_size {
                    let buffered_batch = buffered_data.batch();
                    let empty_stream_batch =
                        RecordBatch::new_empty(Arc::clone(&self.streamed_schema));

                    let buffered_chunk_ref =
                        buffered_indices.slice(start_idx, self.batch_size);
                    let new_buffered_indices = buffered_chunk_ref
                        .as_any()
                        .downcast_ref::<UInt64Array>()
                        .expect("downcast to UInt64Array after slice");

                    let streamed_indices: UInt32Array =
                        (0..new_buffered_indices.len() as u32).collect();

                    let batch = build_matched_indices(
                        Arc::clone(&self.schema),
                        &empty_stream_batch,
                        buffered_batch,
                        streamed_indices,
                        new_buffered_indices.clone(),
                    )?;

                    self.batch_process_state
                        .set_process_rest(Some(start_idx + self.batch_size));
                    self.batch_process_state.continue_process = true;

                    return Ok(StatefulStreamResult::Ready(Some(batch)));
                }

                let buffered_batch = buffered_data.batch();
                let empty_stream_batch =
                    RecordBatch::new_empty(Arc::clone(&self.streamed_schema));

                let buffered_chunk_ref = buffered_indices.slice(start_idx, remaining);
                let new_buffered_indices = buffered_chunk_ref
                    .as_any()
                    .downcast_ref::<UInt64Array>()
                    .expect("downcast to UInt64Array after slice");

                let streamed_indices: UInt32Array =
                    (0..new_buffered_indices.len() as u32).collect();

                let batch = build_matched_indices(
                    Arc::clone(&self.schema),
                    &empty_stream_batch,
                    buffered_batch,
                    streamed_indices,
                    new_buffered_indices.clone(),
                )?;

                self.batch_process_state.reset();

                timer.done();
                self.join_metrics.output_batches.add(1);
                self.state = PiecewiseMergeJoinStreamState::Completed;

                return Ok(StatefulStreamResult::Ready(Some(batch)));
            }

            return exec_err!("Batch process state should hold buffered indices");
        }

        // Pass in piecewise flag to allow Right Semi/Anti/Mark joins to also be processed
        let (buffered_indices, streamed_indices) = get_final_indices_from_shared_bitmap(
            &buffered_data.visited_indices_bitmap,
            self.join_type,
            true,
        );

        // If the output indices is larger than the limit for the incremental batching then
        // proceed to outputting all matches up to that index, return batch, and the matching
        // will start next on the updated index (`process_rest`)
        if buffered_indices.len() > self.batch_size {
            let buffered_batch = buffered_data.batch();
            let empty_stream_batch =
                RecordBatch::new_empty(Arc::clone(&self.streamed_schema));

            let indices_chunk_ref = buffered_indices
                .slice(self.batch_process_state.start_idx, self.batch_size);

            let indices_chunk = indices_chunk_ref
                .as_any()
                .downcast_ref::<UInt64Array>()
                .expect("downcast to UInt64Array after slice");

            let batch = build_matched_indices(
                Arc::clone(&self.schema),
                &empty_stream_batch,
                buffered_batch,
                streamed_indices,
                indices_chunk.clone(),
            )?;

            self.batch_process_state.buffered_indices = Some(buffered_indices);
            self.batch_process_state
                .set_process_rest(Some(self.batch_size));
            self.batch_process_state.continue_process = true;

            return Ok(StatefulStreamResult::Ready(Some(batch)));
        }

        let buffered_batch = buffered_data.batch();
        let empty_stream_batch =
            RecordBatch::new_empty(Arc::clone(&self.streamed_schema));

        let batch = build_matched_indices(
            Arc::clone(&self.schema),
            &empty_stream_batch,
            buffered_batch,
            streamed_indices,
            buffered_indices,
        )?;

        timer.done();
        self.join_metrics.output_batches.add(1);
        self.state = PiecewiseMergeJoinStreamState::Completed;

        Ok(StatefulStreamResult::Ready(Some(batch)))
    }
}

// Holds all information for processing incremental output
struct BatchProcessState {
    // Used to pick up from the last index on the stream side
    start_idx: usize,
    // Used to pick up from the last index on the buffered side
    pivot: usize,
    // Tracks the number of rows processed; default starts at 0
    num_rows: usize,
    // Processes the rest of the batch
    process_rest: Option<usize>,
    // Used to skip fully processing the row
    not_found: bool,
    // Signals whether to call `ProcessStreamBatch` again
    continue_process: bool,
    // Holding the buffered indices when processing the remaining marked rows.
    buffered_indices: Option<PrimitiveArray<UInt64Type>>,
}

impl BatchProcessState {
    pub fn new() -> Self {
        Self {
            start_idx: 0,
            num_rows: 0,
            pivot: 0,
            process_rest: None,
            not_found: false,
            continue_process: false,
            buffered_indices: None,
        }
    }

    fn reset(&mut self) {
        self.start_idx = 0;
        self.num_rows = 0;
        self.pivot = 0;
        self.process_rest = None;
        self.not_found = false;
        self.continue_process = false;
        self.buffered_indices = None;
    }

    fn pivot(&self) -> usize {
        self.pivot
    }

    fn set_pivot(&mut self, pivot: usize) {
        self.pivot = pivot;
    }

    fn set_start_idx(&mut self, start_idx: usize) {
        self.start_idx = start_idx;
    }

    fn set_rows(&mut self, num_rows: usize) {
        self.num_rows = num_rows;
    }

    fn set_process_rest(&mut self, process_rest: Option<usize>) {
        self.process_rest = process_rest;
    }
}

impl Stream for ClassicPWMJStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        self.poll_next_impl(cx)
    }
}

// For Left, Right, Full, and Inner joins, incoming stream batches will already be sorted.
fn resolve_classic_join(
    buffered_side: &mut BufferedSideReadyState,
    stream_batch: &StreamedBatch,
    join_schema: Arc<Schema>,
    operator: Operator,
    sort_options: SortOptions,
    join_type: JoinType,
    batch_process_state: &mut BatchProcessState,
    batch_size: usize,
) -> Result<RecordBatch> {
    let buffered_values = buffered_side.buffered_data.values();
    let buffered_len = buffered_values.len();
    let stream_values = stream_batch.values();

    let mut buffered_indices = UInt64Builder::default();
    let mut stream_indices = UInt32Builder::default();

    // Our pivot variable allows us to start probing on the buffered side where we last matched
    // in the previous stream row.
    let mut pivot = batch_process_state.pivot();
    for row_idx in batch_process_state.start_idx..stream_values[0].len() {
        let mut found = false;

        // Check once to see if it is a redo of a null value if not we do not try to process the batch
        if !batch_process_state.not_found {
            while pivot < buffered_values.len()
                || batch_process_state.process_rest.is_some()
            {
                // If there is still data left in the batch to process, use the index and output
                if let Some(start_idx) = batch_process_state.process_rest {
                    let count = buffered_values.len() - start_idx;
                    if count >= batch_size {
                        let stream_repeated = vec![row_idx as u32; batch_size];
                        batch_process_state
                            .set_process_rest(Some(start_idx + batch_size));
                        batch_process_state
                            .set_rows(batch_process_state.num_rows + batch_size);
                        let buffered_range: Vec<u64> = (start_idx as u64
                            ..((start_idx as u64) + (batch_size as u64)))
                            .collect();
                        stream_indices.append_slice(&stream_repeated);
                        buffered_indices.append_slice(&buffered_range);

                        let batch = process_batch(
                            &mut buffered_indices,
                            &mut stream_indices,
                            stream_batch,
                            buffered_side,
                            join_type,
                            join_schema,
                        )?;
                        batch_process_state.continue_process = true;
                        batch_process_state.set_rows(0);

                        return Ok(batch);
                    }

                    batch_process_state.set_rows(batch_process_state.num_rows + count);
                    let stream_repeated = vec![row_idx as u32; count];
                    let buffered_range: Vec<u64> =
                        (start_idx as u64..buffered_len as u64).collect();
                    stream_indices.append_slice(&stream_repeated);
                    buffered_indices.append_slice(&buffered_range);
                    batch_process_state.process_rest = None;

                    found = true;

                    break;
                }

                let compare = compare_join_arrays(
                    &[Arc::clone(&stream_values[0])],
                    row_idx,
                    &[Arc::clone(buffered_values)],
                    pivot,
                    &[sort_options],
                    NullEquality::NullEqualsNothing,
                )?;

                // If we find a match we append all indices and move to the next stream row index
                match operator {
                    Operator::Gt | Operator::Lt => {
                        if matches!(compare, Ordering::Less) {
                            let count = buffered_values.len() - pivot;

                            // If the current output + new output is over our process value then we want to be
                            // able to change that
                            if batch_process_state.num_rows + count >= batch_size {
                                let process_batch_size =
                                    batch_size - batch_process_state.num_rows;
                                let stream_repeated =
                                    vec![row_idx as u32; process_batch_size];
                                batch_process_state.set_rows(
                                    batch_process_state.num_rows + process_batch_size,
                                );

                                let buffered_range: Vec<u64> = (pivot as u64
                                    ..(pivot + process_batch_size) as u64)
                                    .collect();
                                stream_indices.append_slice(&stream_repeated);
                                buffered_indices.append_slice(&buffered_range);

                                let batch = process_batch(
                                    &mut buffered_indices,
                                    &mut stream_indices,
                                    stream_batch,
                                    buffered_side,
                                    join_type,
                                    join_schema,
                                )?;

                                batch_process_state
                                    .set_process_rest(Some(pivot + process_batch_size));
                                batch_process_state.continue_process = true;
                                // Update the start index so it repeats the process
                                batch_process_state.set_start_idx(row_idx);
                                batch_process_state.set_pivot(pivot);
                                batch_process_state.set_rows(0);

                                return Ok(batch);
                            }

                            // Update the number of rows processed
                            batch_process_state
                                .set_rows(batch_process_state.num_rows + count);

                            let stream_repeated = vec![row_idx as u32; count];
                            let buffered_range: Vec<u64> =
                                (pivot as u64..buffered_len as u64).collect();

                            stream_indices.append_slice(&stream_repeated);
                            buffered_indices.append_slice(&buffered_range);
                            found = true;

                            break;
                        }
                    }
                    Operator::GtEq | Operator::LtEq => {
                        if matches!(compare, Ordering::Equal | Ordering::Less) {
                            let count = buffered_values.len() - pivot;

                            // If the current output + new output is over our process value then we want to be
                            // able to change that
                            if batch_process_state.num_rows + count >= batch_size {
                                // Update the start index so it repeats the process
                                batch_process_state.set_start_idx(row_idx);
                                batch_process_state.set_pivot(pivot);

                                let process_batch_size =
                                    batch_size - batch_process_state.num_rows;
                                let stream_repeated =
                                    vec![row_idx as u32; process_batch_size];
                                batch_process_state
                                    .set_process_rest(Some(pivot + process_batch_size));
                                batch_process_state.set_rows(
                                    batch_process_state.num_rows + process_batch_size,
                                );
                                let buffered_range: Vec<u64> = (pivot as u64
                                    ..(pivot + process_batch_size) as u64)
                                    .collect();
                                stream_indices.append_slice(&stream_repeated);
                                buffered_indices.append_slice(&buffered_range);

                                let batch = process_batch(
                                    &mut buffered_indices,
                                    &mut stream_indices,
                                    stream_batch,
                                    buffered_side,
                                    join_type,
                                    join_schema,
                                )?;

                                batch_process_state.continue_process = true;
                                batch_process_state.set_rows(0);

                                return Ok(batch);
                            }

                            // Update the number of rows processed
                            batch_process_state
                                .set_rows(batch_process_state.num_rows + count);
                            let stream_repeated = vec![row_idx as u32; count];
                            let buffered_range: Vec<u64> =
                                (pivot as u64..buffered_len as u64).collect();

                            stream_indices.append_slice(&stream_repeated);
                            buffered_indices.append_slice(&buffered_range);
                            found = true;

                            break;
                        }
                    }
                    _ => {
                        return exec_err!(
                            "PiecewiseMergeJoin should not contain operator, {}",
                            operator
                        )
                    }
                };

                // Increment pivot after every row
                pivot += 1;
            }
        }

        // If not found we append a null value for `JoinType::Right` and `JoinType::Full`
        if (!found || batch_process_state.not_found)
            && matches!(join_type, JoinType::Right | JoinType::Full)
        {
            let remaining = batch_size.saturating_sub(batch_process_state.num_rows);
            if remaining == 0 {
                let batch = process_batch(
                    &mut buffered_indices,
                    &mut stream_indices,
                    stream_batch,
                    buffered_side,
                    join_type,
                    join_schema,
                )?;

                // Update the start index so it repeats the process
                batch_process_state.set_start_idx(row_idx);
                batch_process_state.set_pivot(pivot);
                batch_process_state.not_found = true;
                batch_process_state.continue_process = true;
                batch_process_state.set_rows(0);

                return Ok(batch);
            }

            // Append right side value + null value for left
            stream_indices.append_value(row_idx as u32);
            buffered_indices.append_null();
            batch_process_state.set_rows(batch_process_state.num_rows + 1);
            batch_process_state.not_found = false;
        }
    }

    let batch = process_batch(
        &mut buffered_indices,
        &mut stream_indices,
        stream_batch,
        buffered_side,
        join_type,
        join_schema,
    )?;

    // Resets batch process state for processing `Left` + `Full` join
    batch_process_state.reset();

    Ok(batch)
}

fn process_batch(
    buffered_indices: &mut PrimitiveBuilder<UInt64Type>,
    stream_indices: &mut PrimitiveBuilder<UInt32Type>,
    stream_batch: &StreamedBatch,
    buffered_side: &mut BufferedSideReadyState,
    join_type: JoinType,
    join_schema: Arc<Schema>,
) -> Result<RecordBatch> {
    let stream_indices_array = stream_indices.finish();
    let buffered_indices_array = buffered_indices.finish();

    // We need to mark the buffered side matched indices for `JoinType::Full` and `JoinType::Left`
    if need_produce_result_in_final(join_type) {
        let mut bitmap = buffered_side.buffered_data.visited_indices_bitmap.lock();

        buffered_indices_array.iter().flatten().for_each(|i| {
            bitmap.set_bit(i as usize, true);
        });
    }

    let batch = build_matched_indices(
        join_schema,
        &stream_batch.batch,
        &buffered_side.buffered_data.batch,
        stream_indices_array,
        buffered_indices_array,
    )?;

    Ok(batch)
}

fn build_matched_indices(
    schema: Arc<Schema>,
    streamed_batch: &RecordBatch,
    buffered_batch: &RecordBatch,
    streamed_indices: UInt32Array,
    buffered_indices: UInt64Array,
) -> Result<RecordBatch> {
    if schema.fields().is_empty() {
        // Build an “empty” RecordBatch with just row‐count metadata
        let options = RecordBatchOptions::new()
            .with_match_field_names(true)
            .with_row_count(Some(streamed_indices.len()));
        return Ok(RecordBatch::try_new_with_options(
            Arc::new((*schema).clone()),
            vec![],
            &options,
        )?);
    }

    // Gather stream columns after applying filter specified with stream indices
    let streamed_columns = streamed_batch
        .columns()
        .iter()
        .map(|column_array| {
            if column_array.is_empty()
                || streamed_indices.null_count() == streamed_indices.len()
            {
                assert_eq!(streamed_indices.null_count(), streamed_indices.len());
                Ok(new_null_array(
                    column_array.data_type(),
                    streamed_indices.len(),
                ))
            } else {
                take(column_array, &streamed_indices, None)
            }
        })
        .collect::<Result<Vec<_>, ArrowError>>()?;

    let mut buffered_columns = buffered_batch
        .columns()
        .iter()
        .map(|column_array| take(column_array, &buffered_indices, None))
        .collect::<Result<Vec<_>, ArrowError>>()?;

    buffered_columns.extend(streamed_columns);

    Ok(RecordBatch::try_new(
        Arc::new((*schema).clone()),
        buffered_columns,
    )?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        common,
        joins::PiecewiseMergeJoinExec,
        test::{build_table_i32, TestMemoryExec},
        ExecutionPlan,
    };
    use arrow::array::{Date32Array, Date64Array};
    use arrow_schema::{DataType, Field};
    use datafusion_common::test_util::batches_to_string;
    use datafusion_execution::TaskContext;
    use datafusion_expr::JoinType;
    use datafusion_physical_expr::{expressions::Column, PhysicalExpr};
    use insta::assert_snapshot;
    use std::sync::Arc;

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
        PiecewiseMergeJoinExec::try_new(left, right, on, operator, join_type)
    }

    async fn join_collect(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        on: (PhysicalExprRef, PhysicalExprRef),
        operator: Operator,
        join_type: JoinType,
    ) -> Result<(Vec<String>, Vec<RecordBatch>)> {
        join_collect_with_options(left, right, on, operator, join_type).await
    }

    async fn join_collect_with_options(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        on: (PhysicalExprRef, PhysicalExprRef),
        operator: Operator,
        join_type: JoinType,
    ) -> Result<(Vec<String>, Vec<RecordBatch>)> {
        let task_ctx = Arc::new(TaskContext::default());
        let join = join(left, right, on, operator, join_type)?;
        let columns = columns(&join.schema());

        let stream = join.execute(0, task_ctx)?;
        let batches = common::collect(stream).await?;
        Ok((columns, batches))
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

        let (_, batches) =
            join_collect(left, right, on, Operator::Lt, JoinType::Inner).await?;

        assert_snapshot!(batches_to_string(&batches), @r#"
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
        "#);
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

        let (_, batches) =
            join_collect(left, right, on, Operator::Lt, JoinType::Inner).await?;

        assert_snapshot!(batches_to_string(&batches), @r#"
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
        "#);
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

        let (_, batches) =
            join_collect(left, right, on, Operator::GtEq, JoinType::Inner).await?;

        assert_snapshot!(batches_to_string(&batches), @r#"
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
        "#);
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
        let (_, batches) =
            join_collect(left, right, on, Operator::LtEq, JoinType::Inner).await?;
        assert_snapshot!(batches_to_string(&batches), @r#"
        +----+----+----+----+----+----+
        | a1 | b1 | c1 | a2 | b1 | c2 |
        +----+----+----+----+----+----+
        +----+----+----+----+----+----+
        "#);
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

        let (_, batches) =
            join_collect(left, right, on, Operator::GtEq, JoinType::Full).await?;

        assert_snapshot!(batches_to_string(&batches), @r#"
        +----+----+-----+----+----+-----+
        | a1 | b1 | c1  | a2 | b1 | c2  |
        +----+----+-----+----+----+-----+
        | 2  | 2  | 200 | 20 | 2  | 400 |
        |    |    |     | 10 | 3  | 300 |
        | 1  | 1  | 100 |    |    |     |
        +----+----+-----+----+----+-----+
        "#);

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

        let (_, batches) =
            join_collect(left, right, on, Operator::Gt, JoinType::Left).await?;

        assert_snapshot!(batches_to_string(&batches), @r#"
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
        "#);
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

        let (_, batches) =
            join_collect(left, right, on, Operator::Gt, JoinType::Right).await?;

        assert_snapshot!(batches_to_string(&batches), @r#"
        +----+----+----+----+----+----+
        | a1 | b1 | c1 | a2 | b1 | c2 |
        +----+----+----+----+----+----+
        | 2  | 3  | 8  | 30 | 2  | 90 |
        | 3  | 4  | 9  | 30 | 2  | 90 |
        | 3  | 4  | 9  | 20 | 3  | 80 |
        |    |    |    | 10 | 5  | 70 |
        +----+----+----+----+----+----+
        "#);
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

        let (_, batches) =
            join_collect(left, right, on, Operator::Lt, JoinType::Right).await?;

        assert_snapshot!(batches_to_string(&batches), @r#"
        +----+----+----+----+----+----+
        | a1 | b1 | c1 | a2 | b1 | c2 |
        +----+----+----+----+----+----+
        | 1  | 4  | 7  | 30 | 5  | 90 |
        | 2  | 3  | 8  | 30 | 5  | 90 |
        | 3  | 1  | 9  | 30 | 5  | 90 |
        | 3  | 1  | 9  | 20 | 3  | 80 |
        | 3  | 1  | 9  | 10 | 2  | 70 |
        +----+----+----+----+----+----+
        "#);
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

        let (_, batches) =
            join_collect(left, right, on, Operator::Lt, JoinType::Inner).await?;

        assert_snapshot!(batches_to_string(&batches), @r#"
    +------------+------------+------------+------------+------------+------------+
    | a1         | b1         | c1         | a2         | b1         | c2         |
    +------------+------------+------------+------------+------------+------------+
    | 1970-01-04 | 2022-04-23 | 1970-01-10 | 1970-01-31 | 2022-04-25 | 1970-04-01 |
    +------------+------------+------------+------------+------------+------------+
    "#);
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

        let (_, batches) =
            join_collect(left, right, on, Operator::Lt, JoinType::Inner).await?;

        assert_snapshot!(batches_to_string(&batches), @r#"
        +-------------------------+---------------------+-------------------------+-------------------------+---------------------+-------------------------+
        | a1                      | b1                  | c1                      | a2                      | b1                  | c2                      |
        +-------------------------+---------------------+-------------------------+-------------------------+---------------------+-------------------------+
        | 1970-01-01T00:00:00.003 | 2022-04-23T08:44:01 | 1970-01-01T00:00:00.009 | 1970-01-01T00:00:00.030 | 2022-04-25T16:17:21 | 1970-01-01T00:00:00.090 |
        +-------------------------+---------------------+-------------------------+-------------------------+---------------------+-------------------------+
        "#);
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

        let (_, batches) =
            join_collect(left, right, on, Operator::Lt, JoinType::Right).await?;

        assert_snapshot!(batches_to_string(&batches), @r#"
    +-------------------------+---------------------+-------------------------+-------------------------+---------------------+-------------------------+
    | a1                      | b1                  | c1                      | a2                      | b1                  | c2                      |
    +-------------------------+---------------------+-------------------------+-------------------------+---------------------+-------------------------+
    | 1970-01-01T00:00:00.002 | 2022-04-23T08:44:01 | 1970-01-01T00:00:00.008 | 1970-01-01T00:00:00.020 | 2022-04-25T16:17:21 | 1970-01-01T00:00:00.090 |
    |                         |                     |                         | 1970-01-01T00:00:00.010 | 2022-04-23T08:44:01 | 1970-01-01T00:00:00.080 |
    +-------------------------+---------------------+-------------------------+-------------------------+---------------------+-------------------------+
"#);
        Ok(())
    }
}
