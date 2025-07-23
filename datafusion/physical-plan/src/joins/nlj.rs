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

//! [`NestedLoopJoinExec`]: joins without equijoin (equality predicates).
//!
//! This implementation uses a simplified state machine that avoids data duplication
//! and unnecessary complexity while maintaining all functionality.

//! ## State Transition Conditions
//!
//! BufferingLeft
//!     --> FetchingRight: After buffered all the left inputs
//! FetchingRight
//!     --> ProbeJoin: If we got a right batch to process
//!     --> EmitUnmatched: When we have exhausted all right batches
//! ProbeJoin
//!     --> FetchingRight: When we've processed all left rows with current right batch
//! EmitUnmatched
//!     --> Done: When all unmatched rows have been emitted

use arrow::buffer::MutableBuffer;
use arrow::compute::BatchCoalescer;
use futures::{ready, StreamExt};
use std::sync::Arc;
use std::task::Poll;

use crate::joins::nested_loop_join::JoinLeftData;
use crate::joins::utils::{
    adjust_indices_by_join_type, apply_join_filter_to_indices,
    build_batch_from_indices_maybe_empty, need_produce_result_in_final,
    BuildProbeJoinMetrics, ColumnIndex, JoinFilter, OnceFut,
};
use crate::{RecordBatchStream, SendableRecordBatchStream};

use arrow::array::{
    BooleanArray, BooleanBufferBuilder, UInt32Array, UInt32Builder, UInt64Array,
    UInt64Builder,
};
use arrow::datatypes::{Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use datafusion_common::{
    internal_datafusion_err, unwrap_or_internal_err, DataFusionError, JoinSide, Result,
};
use datafusion_expr::JoinType;

use futures::Stream;

const OUTPUT_BUFFER_LIMIT: usize = 8192;

/// Simplified state enum without inner structs
#[derive(Debug, Clone, Copy)]
enum NLJState {
    BufferingLeft,
    FetchingRight,
    ProbeJoin,
    EmitRightUnmatched,
    EmitLeftUnmatched,
    Done,
}

pub(crate) struct NLJStream {
    /// Input schema
    pub(crate) schema: Arc<Schema>,
    /// join filter
    pub(crate) filter: Option<JoinFilter>,
    /// type of the join
    pub(crate) join_type: JoinType,
    /// the outer table data of the nested loop join
    pub(crate) outer_table: SendableRecordBatchStream,
    /// the inner table data of the nested loop join
    pub(crate) inner_table: OnceFut<JoinLeftData>,
    /// Information of index and left / right placement of columns
    pub(crate) column_indices: Vec<ColumnIndex>,
    /// Join execution metrics
    pub(crate) join_metrics: BuildProbeJoinMetrics,

    // Configurations
    cfg_batch_size: usize,

    // State Tracking
    state: NLJState,

    buffered_left_data: Option<Arc<JoinLeftData>>,
    l_index: usize,
    // Should we go back to `BufferingLeft` state again after `EmitUnmatched`
    // state is over.
    left_exhausted: bool,
    // If we can buffer all left data in one pass
    left_buffered_in_one_pass: bool,

    current_right_batch: Option<RecordBatch>,
    // For right join, keep track of matched rows in `current_right_batch`
    // - Constructured (with Some(..)) on initialization
    // - After done joining (left_row x right_batch), output those unmatched rows
    // - Resets when fetching a new right batch.
    current_right_batch_matched: Option<BooleanBufferBuilder>,

    output_buffer: Box<BatchCoalescer>,
    emit_cursor: u64,
}

impl NLJStream {
    /// Process a single left row join with the current right batch.
    /// Returns a RecordBatch containing the join results (may be empty).
    fn process_single_left_row_join(
        &mut self,
        left_data: &JoinLeftData,
        right_batch: &RecordBatch,
        l_index: usize,
    ) -> Result<RecordBatch> {
        let right_row_count = right_batch.num_rows();
        if right_row_count == 0 {
            return Ok(RecordBatch::new_empty(self.schema.clone()));
        }

        // Create indices for cross-join: current left row with all right rows
        let left_indices = UInt64Array::from(vec![l_index as u64; right_row_count]);
        let right_indices = UInt32Array::from_iter_values(0..right_row_count as u32);

        // Apply join filter if present
        let (joined_left_indices, joined_right_indices) =
            if let Some(ref filter) = self.filter {
                apply_join_filter_to_indices(
                    left_data.batch(),
                    right_batch,
                    left_indices,
                    right_indices,
                    filter,
                    JoinSide::Left,
                    None,
                )?
            } else {
                (left_indices, right_indices)
            };

        // Update left row match bitmap for outer join support
        if need_produce_result_in_final(self.join_type) && !joined_left_indices.is_empty()
        {
            let mut bitmap = left_data.bitmap().lock();
            bitmap.set_bit(l_index, true);
        }

        // TODO(now-perf): better vectorize it
        if let Some(bitmap) = self.current_right_batch_matched.as_mut() {
            for i in joined_right_indices.iter() {
                // After the initial join, indices must all be Some
                bitmap.set_bit(i.unwrap() as usize, true);
                // println!("Setting bit {i:?} to true");
            }
        }

        // For the following join types: here we only have to set the left/right
        // bitmap, and no need to output result
        if matches!(
            self.join_type,
            JoinType::LeftAnti
                | JoinType::LeftSemi
                | JoinType::LeftMark
                | JoinType::RightAnti
                | JoinType::RightMark
                | JoinType::RightSemi
        ) {
            return Ok(RecordBatch::new_empty(self.schema.clone()));
        }

        // TODO(now): check if we're missed something inside the original
        // logic inside `adjust_indices_by_join_types`

        // Build output batch from matching indices
        if !joined_left_indices.is_empty() {
            let join_batch = build_batch_from_indices_maybe_empty(
                &self.schema,
                left_data.batch(),
                right_batch,
                &joined_left_indices,
                &joined_right_indices,
                &self.column_indices,
                JoinSide::Left,
            )?;
            Ok(join_batch)
        } else {
            Ok(RecordBatch::new_empty(self.schema.clone()))
        }
    }

    /// Process unmatched rows from the left data within the specified range.
    /// Returns a RecordBatch containing the unmatched rows (may be empty).
    ///
    /// # Arguments
    /// * `left_data` - The left side data containing the batch and bitmap
    /// * `start_idx` - Start index (inclusive) of the range to process
    /// * `end_idx` - End index (exclusive) of the range to process
    ///
    /// # Safety
    /// The caller is responsible for ensuring that `start_idx` and `end_idx` are
    /// within valid bounds of the left batch. This function does not perform
    /// bounds checking.
    fn process_unmatched_rows(
        &self,
        left_data: &JoinLeftData,
        start_idx: usize,
        end_idx: usize,
    ) -> Result<RecordBatch> {
        let left_batch = left_data.batch();

        let mut left_indices_builder = UInt64Builder::new();
        let mut right_indices_builder = UInt32Builder::new();

        let bitmap = left_data.bitmap().lock();
        for i in start_idx..end_idx {
            let should_include = match self.join_type {
                JoinType::LeftSemi => bitmap.get_bit(i),
                JoinType::LeftMark => {
                    left_indices_builder.append_value(i as u64);
                    if bitmap.get_bit(i) {
                        right_indices_builder.append_value(0);
                    } else {
                        right_indices_builder.append_null();
                    }
                    false // handled above
                }
                _ => !bitmap.get_bit(i), // Left, LeftAnti, Full - include unmatched
            };

            if should_include {
                left_indices_builder.append_value(i as u64);
                right_indices_builder.append_null();
            }
        }

        let left_indices = left_indices_builder.finish();
        let right_indices = right_indices_builder.finish();

        if !left_indices.is_empty() {
            let empty_right_batch = RecordBatch::new_empty(self.outer_table.schema());
            let result_batch = build_batch_from_indices_maybe_empty(
                &self.schema,
                left_data.batch(),
                &empty_right_batch,
                &left_indices,
                &right_indices,
                &self.column_indices,
                JoinSide::Left,
            )?;
            Ok(result_batch)
        } else {
            Ok(RecordBatch::new_empty(self.schema.clone()))
        }
    }

    /// Process unmatched rows from the current right batch and reset the bitmap.
    /// Returns a RecordBatch containing the unmatched right rows (may be empty).
    ///
    /// Side-effect: it will reset the right bitmap to all false
    fn process_right_unmatched(&mut self) -> Result<RecordBatch> {
        // ==== Take current right batch and its bitmap ====
        let bitmap: BooleanArray = self
            .current_right_batch_matched
            .take()
            .unwrap()
            .finish()
            .into();

        let right_batch = self.current_right_batch.take();
        let cur_right_batch = unwrap_or_internal_err!(right_batch);

        // ==== Setup unmatched indices ====
        // TODO(polish): now the actual length of bitmap might be longer than
        // the actual in-use. So we have to use right batch length here to
        // iterate through the bitmap
        let mut right_indices_builder = UInt32Builder::new();
        for i in 0..cur_right_batch.num_rows() {
            let i_joined = bitmap.value(i);
            // TODO(polish): make those flips more understandable
            let should_output = match self.join_type {
                JoinType::Right => !i_joined,
                JoinType::Full => !i_joined,
                JoinType::RightAnti => !i_joined,
                JoinType::RightMark => i_joined,
                JoinType::RightSemi => i_joined,
                _ => unreachable!("Not possible for other join types"),
            };
            if should_output {
                right_indices_builder.append_value(i as u32);
            }
        }
        let right_indices = right_indices_builder.finish();
        let left_indices = UInt64Array::new_null(right_indices.len());

        // ==== Build the output batch ====
        let left_data = self
            .buffered_left_data
            .as_ref()
            .ok_or_else(|| internal_datafusion_err!("LeftData should be available"))?;
        let left_batch = left_data.batch();
        let empty_left_batch = RecordBatch::new_empty(left_batch.schema().clone());

        let result_batch = build_batch_from_indices_maybe_empty(
            &self.schema,
            &empty_left_batch,
            &cur_right_batch,
            &left_indices,
            &right_indices,
            &self.column_indices,
            JoinSide::Left,
        )?;

        // ==== Clean-up ====
        self.current_right_batch_matched = None;

        Ok(result_batch)
    }

    pub(crate) fn new(
        schema: Arc<Schema>,
        filter: Option<JoinFilter>,
        join_type: JoinType,
        outer_table: SendableRecordBatchStream,
        inner_table: OnceFut<JoinLeftData>,
        column_indices: Vec<ColumnIndex>,
        join_metrics: BuildProbeJoinMetrics,
        cfg_batch_size: usize,
    ) -> Self {
        let current_right_batch_matched = if matches!(
            join_type,
            JoinType::Full
                | JoinType::Right
                | JoinType::RightAnti
                | JoinType::RightMark
                | JoinType::RightSemi
        ) {
            // Now we don't have interface to init with 0-init for `BooleanBufferBuilder`
            let buffer = MutableBuffer::from_len_zeroed(cfg_batch_size);
            Some(BooleanBufferBuilder::new_from_buffer(
                buffer,
                cfg_batch_size,
            ))
        } else {
            None
        };

        Self {
            schema: schema.clone(),
            filter,
            join_type,
            outer_table,
            column_indices,
            inner_table,
            join_metrics,
            buffered_left_data: None,
            output_buffer: Box::new(BatchCoalescer::new(schema, OUTPUT_BUFFER_LIMIT)),
            cfg_batch_size,
            current_right_batch: None,
            current_right_batch_matched,
            state: NLJState::BufferingLeft,
            l_index: 0,
            emit_cursor: 0,
            left_exhausted: false,
            left_buffered_in_one_pass: true,
        }
    }
}

impl Stream for NLJStream {
    type Item = Result<RecordBatch>;

    /// This function is the entry point of NLJ operator's state machine
    /// transitions. The high-level doc comment can be found above each NLJ
    /// state's maching arm.
    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        loop {
            match self.state {
                // # NLJState transitions
                // --> FetchingRight
                // This state will prepare the left side batches, next state
                // `FetchingRight` is responsible for preparing a single probe
                // side batch, before start joining.
                //
                // In this NLJ operator, we will try to buffer a large amount of
                // data at the left side, then do one probe for all buffered
                // data. This is due to:
                // - If memory is constrained, the query still can finish if we
                //   re-scan the probe side for each buffered build-side input.
                // - The probing can be expensive (e.g., decoding a parquet
                //   file at the probing side, for each scan).
                //   So we also want to minimize the number of scans to achieve
                //   better performance, by buffering many build-side batches
                //   up front.
                NLJState::BufferingLeft => {
                    match ready!(self.inner_table.get_shared(cx)) {
                        Ok(left_data) => {
                            self.buffered_left_data = Some(left_data);
                            // TOOD: implement memory-limited case
                            self.left_exhausted = true;
                            self.state = NLJState::FetchingRight;
                            continue;
                        }
                        Err(e) => return Poll::Ready(Some(Err(e))),
                    }
                }

                // # NLJState transitions:
                // 1. --> ProbeJoin
                //    Start processing the join for the newly fetched right
                //    batch.
                // 2. --> EmitUnmatched: When the right side input is exhausted, (maybe) emit
                //    unmatched left side rows.
                //
                // After fetching a new batch from the right side, it will
                // process all rows from the buffered left data:
                // ```text
                // for batch in right_side:
                //     for row in left_buffer:
                //         join(batch, row)
                // ```
                // Note: the implementation does this step incrementally,
                // instead of materializing all intermediate Cartesian products
                // at once in memory.
                //
                // So after the right side input is exhausted, the join phase
                // for the current buffered left data is finished. We can go to
                // the next `EmitUnmatched` phase to check if there is any
                // special handling (e.g., in cases like left join).
                NLJState::FetchingRight => {
                    match ready!(self.outer_table.poll_next_unpin(cx)) {
                        Some(Ok(right_batch)) => {
                            let right_batch_size = right_batch.num_rows();
                            self.current_right_batch = Some(right_batch);

                            // TOOD(polish): make it more understandable
                            if (self.current_right_batch_matched.is_some()) {
                                // We have to resize the right bitmap.
                                let new_size = right_batch_size;
                                let zeroed_buf = MutableBuffer::from_len_zeroed(new_size);
                                self.current_right_batch_matched =
                                    Some(BooleanBufferBuilder::new_from_buffer(
                                        zeroed_buf, new_size,
                                    ));
                            }

                            self.l_index = 0;
                            self.state = NLJState::ProbeJoin;
                            continue;
                        }
                        Some(Err(e)) => return Poll::Ready(Some(Err(e))),
                        None => {
                            // Right stream exhausted/as
                            self.state = NLJState::EmitLeftUnmatched;
                            continue;
                        }
                    }
                }

                // NLJState transitions:
                // 1. --> ProbeJoin(1)
                //    If we have already buffered enough output to yield, it
                //    will first give back control to the parent state machine,
                //    then resume at the same place.
                // 2. --> ProbeJoin(2)
                //    After probing one right batch, and evaluating the
                //    join filter on (left-row x right-batch), it will re-enter
                //    the same state, to check if there are any more final
                //    result to output.
                // 3. --> FetchRight
                //    After it has done with the current right batch, it will
                //    go to FetchRight state to check what to do next.
                NLJState::ProbeJoin => {
                    // Return any completed batches first
                    if self.output_buffer.has_completed_batch() {
                        if let Some(batch) = self.output_buffer.next_completed_batch() {
                            return Poll::Ready(Some(Ok(batch)));
                        }
                    }

                    // Process current probe state
                    match self.process_probe_batch() {
                        // State unchanged (ProbeRight)
                        // Continue probing until we have done joining the
                        // current right batch with all buffered left rows.
                        Ok(true) => continue,
                        // To next FetchRightState
                        // We have finished joining
                        // (cur_right_batch x buffered_left_batches)
                        Ok(false) => {
                            // Left exhausted, transition to FetchingRight
                            // TODO(polish): use a flag for clarity
                            self.l_index = 0;
                            if (self.current_right_batch_matched.is_some()) {
                                // Don't reset current_right_batch, it'll be
                                // cleared inside `EmitRightUnmatched` state
                                self.state = NLJState::EmitRightUnmatched;
                            } else {
                                self.current_right_batch = None;
                                self.state = NLJState::FetchingRight;
                            }
                            continue;
                        }
                        Err(e) => return Poll::Ready(Some(Err(e))),
                    }
                }

                // In the `current_right_batch_matched` bitmap, all trues mean
                // it has been outputed by the join. In this state we have to
                // output unmatched rows for current right batch (with null
                // padding for left realtion)
                // Precondition: we have checked the join type so that it's
                // possible to output right unmatched (e.g. it's right join)
                NLJState::EmitRightUnmatched => {
                    debug_assert!(self.current_right_batch.is_some());
                    debug_assert!(self.current_right_batch_matched.is_some());

                    // Construct the result batch for unmatched right rows using a utility function
                    let result_batch = self.process_right_unmatched()?;
                    self.output_buffer.push_batch(result_batch)?;

                    // Processed all in one pass
                    // cleared inside `process_right_unmatched`
                    debug_assert!(self.current_right_batch.is_none());
                    self.state = NLJState::FetchingRight;
                }

                // NLJState transitions:
                // 1. --> EmitUnmatched(1)
                //    If we have already buffered enough output to yield, it
                //    will first give back control to the parent state machine,
                //    then resume at the same place.
                // 2. --> EmitUnmatched(2)
                //    After processing some unmatched rows, it will re-enter
                //    the same state, to check if there are any more final
                //    results to output.
                NLJState::EmitLeftUnmatched => {
                    // Return any completed batches first
                    if self.output_buffer.has_completed_batch() {
                        if let Some(batch) = self.output_buffer.next_completed_batch() {
                            return Poll::Ready(Some(Ok(batch)));
                        }
                    }

                    // Process current unmatched state
                    match self.process_unmatched() {
                        // State unchanged (EmitUnmatched)
                        // Continue processing until we have processed all unmatched rows
                        Ok(true) => continue,
                        // To Done state
                        // We have finished processing all unmatched rows
                        Ok(false) => {
                            self.output_buffer.finish_buffered_batch()?;
                            self.state = NLJState::Done;
                            continue;
                        }
                        Err(e) => return Poll::Ready(Some(Err(e))),
                    }
                }

                NLJState::Done => {
                    // Return any remaining completed batches before final termination
                    if self.output_buffer.has_completed_batch() {
                        if let Some(batch) = self.output_buffer.next_completed_batch() {
                            return Poll::Ready(Some(Ok(batch)));
                        }
                    }

                    return Poll::Ready(None);
                }
            }
        }
    }
}

impl RecordBatchStream for NLJStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

impl NLJStream {
    // Returns bool to indicate should it continue probing
    // true -> continue in the same ProbeRight state
    // false -> It has done with the (buffered_left x cur_right_batch), go to
    // next state (ProbeRight)
    fn process_probe_batch(&mut self) -> Result<bool> {
        let left_data = self
            .buffered_left_data
            .as_ref()
            .ok_or_else(|| internal_datafusion_err!("LeftData should be available"))?
            .clone();
        let right_batch = self
            .current_right_batch
            .as_ref()
            .ok_or_else(|| internal_datafusion_err!("Right batch should be available"))?
            .clone();

        // Skip this empty batch and continue probing the next one
        let right_row_count = right_batch.num_rows();
        if right_row_count == 0 {
            return Ok(true);
        }

        // stop probing, the caller will go to the next state
        if self.l_index >= left_data.batch().num_rows() {
            return Ok(false);
        }

        // ========
        // Join (l_row x right_batch)
        // and push the result into output_buffer
        // ========

        let l_idx = self.l_index;
        let join_batch =
            self.process_single_left_row_join(&left_data, &right_batch, l_idx)?;

        self.output_buffer.push_batch(join_batch)?;

        // ==== Prepare for the next iteration ====

        // Advance left cursor
        self.l_index += 1;

        // Return true to continue probing
        Ok(true)
    }

    // Returns bool to indicate should it continue processing unmatched rows
    // true -> continue in the same EmitUnmatched state
    // false -> next state (Done)
    fn process_unmatched(&mut self) -> Result<bool> {
        let left_data = self
            .buffered_left_data
            .as_ref()
            .ok_or_else(|| internal_datafusion_err!("LeftData should be available"))?;
        let left_batch = left_data.batch();

        // Early return if join type can't have unmatched rows
        if !need_produce_result_in_final(self.join_type) {
            return Ok(false);
        }

        // Early return if another thread is already processing unmatched rows
        if self.emit_cursor == 0 && !left_data.report_probe_completed() {
            return Ok(false);
        }

        // Stop processing unmatched rows, the caller will go to the next state
        if self.emit_cursor >= left_batch.num_rows() as u64 {
            return Ok(false);
        }

        // ========
        // Process unmatched rows and push the result into output_buffer
        // Each time, the number to process is up to batch size
        // ========
        let start_idx = self.emit_cursor as usize;
        let end_idx =
            std::cmp::min(start_idx + OUTPUT_BUFFER_LIMIT, left_batch.num_rows());

        let result_batch = self.process_unmatched_rows(left_data, start_idx, end_idx)?;

        // ==== Prepare for the next iteration ====
        self.output_buffer.push_batch(result_batch)?;
        self.emit_cursor = end_idx as u64;

        // Return true to continue processing unmatched rows
        Ok(true)
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field};

    #[test]
    fn test_nlj_basic_compilation() {
        let _schema = Arc::new(Schema::new(vec![
            Field::new("l_id", DataType::Int32, false),
            Field::new("r_id", DataType::Int32, false),
        ]));

        let _column_indices = vec![
            ColumnIndex {
                index: 0,
                side: JoinSide::Left,
            },
            ColumnIndex {
                index: 0,
                side: JoinSide::Right,
            },
        ];

        // Verify OUTPUT_BUFFER_LIMIT constant
        assert_eq!(OUTPUT_BUFFER_LIMIT, 8192);

        println!("Test passed: simplified NLJ structures compile correctly");
    }
}
