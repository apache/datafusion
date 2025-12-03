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

use arrow::array::{new_null_array, Array, PrimitiveBuilder};
use arrow::compute::{take, BatchCoalescer};
use arrow::datatypes::UInt32Type;
use arrow::{
    array::{ArrayRef, RecordBatch, UInt32Array},
    compute::{sort_to_indices, take_record_batch},
};
use arrow_schema::{Schema, SchemaRef, SortOptions};
use datafusion_common::NullEquality;
use datafusion_common::{internal_err, Result};
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
//     `Completed` if there are are still remaining partitions to process. It is only switched to
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

                // Sort stream values and change the streamed record batch accordingly
                let indices = sort_to_indices(
                    stream_values.as_ref(),
                    Some(self.sort_option),
                    None,
                )?;
                let stream_batch = take_record_batch(&batch, &indices)?;
                let stream_values = take(stream_values.as_ref(), &indices, None)?;

                // Reset BatchProcessState before processing a new stream batch
                self.batch_process_state.reset();
                self.state = PiecewiseMergeJoinStreamState::ProcessStreamBatch(
                    SortedStreamBatch::new(stream_batch, vec![stream_values]),
                );
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

        if let Some(batch) = self
            .batch_process_state
            .output_batches
            .next_completed_batch()
        {
            return Ok(StatefulStreamResult::Ready(Some(batch)));
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
            // We finished scanning this stream batch.
            self.batch_process_state
                .output_batches
                .finish_buffered_batch()?;
            if let Some(b) = self
                .batch_process_state
                .output_batches
                .next_completed_batch()
            {
                self.state = PiecewiseMergeJoinStreamState::FetchStreamBatch;
                return Ok(StatefulStreamResult::Ready(Some(b)));
            }

            // Nothing pending; hand back whatever `resolve` returned (often empty) and move on.
            if self.batch_process_state.output_batches.is_empty() {
                self.state = PiecewiseMergeJoinStreamState::FetchStreamBatch;

                return Ok(StatefulStreamResult::Ready(Some(batch)));
            }
        }

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

        if !self.batch_process_state.continue_process {
            if let Some(batch) = self
                .batch_process_state
                .output_batches
                .next_completed_batch()
            {
                return Ok(StatefulStreamResult::Ready(Some(batch)));
            }

            self.batch_process_state
                .output_batches
                .finish_buffered_batch()?;
            if let Some(batch) = self
                .batch_process_state
                .output_batches
                .next_completed_batch()
            {
                self.state = PiecewiseMergeJoinStreamState::Completed;
                return Ok(StatefulStreamResult::Ready(Some(batch)));
            }
        }

        let buffered_data =
            Arc::clone(&self.buffered_side.try_as_ready().unwrap().buffered_data);

        let (buffered_indices, _streamed_indices) = get_final_indices_from_shared_bitmap(
            &buffered_data.visited_indices_bitmap,
            self.join_type,
            true,
        );

        let new_buffered_batch =
            take_record_batch(buffered_data.batch(), &buffered_indices)?;
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
        if let Some(batch) = self
            .batch_process_state
            .output_batches
            .next_completed_batch()
        {
            return Ok(StatefulStreamResult::Ready(Some(batch)));
        }

        self.batch_process_state
            .output_batches
            .finish_buffered_batch()?;
        if let Some(batch) = self
            .batch_process_state
            .output_batches
            .next_completed_batch()
        {
            self.state = PiecewiseMergeJoinStreamState::Completed;
            return Ok(StatefulStreamResult::Ready(Some(batch)));
        }

        self.state = PiecewiseMergeJoinStreamState::Completed;
        self.batch_process_state.reset();
        Ok(StatefulStreamResult::Ready(None))
    }
}

struct BatchProcessState {
    // Used to pick up from the last index on the stream side
    output_batches: Box<BatchCoalescer>,
    // Used to store the unmatched stream indices for `JoinType::Right` and `JoinType::Full`
    unmatched_indices: PrimitiveBuilder<UInt32Type>,
    // Used to store the start index on the buffered side; used to resume processing on the correct
    // row
    start_buffer_idx: usize,
    // Used to store the start index on the stream side; used to resume processing on the correct
    // row
    start_stream_idx: usize,
    // Signals if we found a match for the current stream row
    found: bool,
    // Signals to continue processing the current stream batch
    continue_process: bool,
    // Skip nulls
    processed_null_count: bool,
}

impl BatchProcessState {
    pub(crate) fn new(schema: Arc<Schema>, batch_size: usize) -> Self {
        Self {
            output_batches: Box::new(BatchCoalescer::new(schema, batch_size)),
            unmatched_indices: PrimitiveBuilder::new(),
            start_buffer_idx: 0,
            start_stream_idx: 0,
            found: false,
            continue_process: true,
            processed_null_count: false,
        }
    }

    pub(crate) fn reset(&mut self) {
        self.unmatched_indices = PrimitiveBuilder::new();
        self.start_buffer_idx = 0;
        self.start_stream_idx = 0;
        self.found = false;
        self.continue_process = true;
        self.processed_null_count = false;
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
    stream_batch: &SortedStreamBatch,
    join_schema: &SchemaRef,
    operator: Operator,
    sort_options: SortOptions,
    join_type: JoinType,
    batch_process_state: &mut BatchProcessState,
) -> Result<RecordBatch> {
    let buffered_len = buffered_side.buffered_data.values().len();
    let stream_values = stream_batch.compare_key_values();

    let mut buffer_idx = batch_process_state.start_buffer_idx;
    let mut stream_idx = batch_process_state.start_stream_idx;

    if !batch_process_state.processed_null_count {
        let buffered_null_idx = buffered_side.buffered_data.values().null_count();
        let stream_null_idx = stream_values[0].null_count();
        buffer_idx = buffered_null_idx;
        stream_idx = stream_null_idx;
        batch_process_state.processed_null_count = true;
    }

    // Our buffer_idx variable allows us to start probing on the buffered side where we last matched
    // in the previous stream row.
    for row_idx in stream_idx..stream_batch.batch.num_rows() {
        while buffer_idx < buffered_len {
            let compare = {
                let buffered_values = buffered_side.buffered_data.values();
                compare_join_arrays(
                    &[Arc::clone(&stream_values[0])],
                    row_idx,
                    &[Arc::clone(buffered_values)],
                    buffer_idx,
                    &[sort_options],
                    NullEquality::NullEqualsNothing,
                )?
            };

            // If we find a match we append all indices and move to the next stream row index
            match operator {
                Operator::Gt | Operator::Lt => {
                    if matches!(compare, Ordering::Less) {
                        batch_process_state.found = true;
                        let count = buffered_len - buffer_idx;

                        let batch = build_matched_indices_and_set_buffered_bitmap(
                            (buffer_idx, count),
                            (row_idx, count),
                            buffered_side,
                            stream_batch,
                            join_type,
                            join_schema,
                        )?;

                        batch_process_state.output_batches.push_batch(batch)?;

                        // Flush batch and update pointers if we have a completed batch
                        if let Some(batch) =
                            batch_process_state.output_batches.next_completed_batch()
                        {
                            batch_process_state.found = false;
                            batch_process_state.start_buffer_idx = buffer_idx;
                            batch_process_state.start_stream_idx = row_idx + 1;
                            return Ok(batch);
                        }

                        break;
                    }
                }
                Operator::GtEq | Operator::LtEq => {
                    if matches!(compare, Ordering::Equal | Ordering::Less) {
                        batch_process_state.found = true;
                        let count = buffered_len - buffer_idx;
                        let batch = build_matched_indices_and_set_buffered_bitmap(
                            (buffer_idx, count),
                            (row_idx, count),
                            buffered_side,
                            stream_batch,
                            join_type,
                            join_schema,
                        )?;

                        // Flush batch and update pointers if we have a completed batch
                        batch_process_state.output_batches.push_batch(batch)?;
                        if let Some(batch) =
                            batch_process_state.output_batches.next_completed_batch()
                        {
                            batch_process_state.found = false;
                            batch_process_state.start_buffer_idx = buffer_idx;
                            batch_process_state.start_stream_idx = row_idx + 1;
                            return Ok(batch);
                        }

                        break;
                    }
                }
                _ => {
                    return internal_err!(
                        "PiecewiseMergeJoin should not contain operator, {}",
                        operator
                    )
                }
            };

            // Increment buffer_idx after every row
            buffer_idx += 1;
        }

        // If a match was not found for the current stream row index the stream indice is appended
        // to the unmatched indices to be flushed later.
        if matches!(join_type, JoinType::Right | JoinType::Full)
            && !batch_process_state.found
        {
            batch_process_state
                .unmatched_indices
                .append_value(row_idx as u32);
        }

        batch_process_state.found = false;
    }

    // Flushed all unmatched indices on the streamed side
    if matches!(join_type, JoinType::Right | JoinType::Full) {
        let batch = create_unmatched_batch(
            &mut batch_process_state.unmatched_indices,
            stream_batch,
            join_schema,
        )?;

        batch_process_state.output_batches.push_batch(batch)?;
    }

    batch_process_state.continue_process = false;
    Ok(RecordBatch::new_empty(Arc::clone(join_schema)))
}

// Builds a record batch from indices ranges on the buffered and streamed side.
//
// The two ranges are: buffered_range: (start index, count) and streamed_range: (start index, count) due
// to batch.slice(start, count).
fn build_matched_indices_and_set_buffered_bitmap(
    buffered_range: (usize, usize),
    streamed_range: (usize, usize),
    buffered_side: &mut BufferedSideReadyState,
    stream_batch: &SortedStreamBatch,
    join_type: JoinType,
    join_schema: &SchemaRef,
) -> Result<RecordBatch> {
    // Mark the buffered indices as visited
    if need_produce_result_in_final(join_type) {
        let mut bitmap = buffered_side.buffered_data.visited_indices_bitmap.lock();
        for i in buffered_range.0..buffered_range.0 + buffered_range.1 {
            bitmap.set_bit(i, true);
        }
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

// Creates a record batch from the unmatched indices on the streamed side
fn create_unmatched_batch(
    streamed_indices: &mut PrimitiveBuilder<UInt32Type>,
    stream_batch: &SortedStreamBatch,
    join_schema: &SchemaRef,
) -> Result<RecordBatch> {
    let streamed_indices = streamed_indices.finish();
    let new_stream_batch = take_record_batch(&stream_batch.batch, &streamed_indices)?;
    let streamed_columns = new_stream_batch.columns().to_vec();
    let buffered_cols_len = join_schema.fields().len() - streamed_columns.len();

    let num_rows = new_stream_batch.num_rows();
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
        PiecewiseMergeJoinExec::try_new(left, right, on, operator, join_type, 1)
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

        let (_, batches) =
            join_collect(left, right, on, Operator::LtEq, JoinType::Inner).await?;

        // Expected grouping follows right.b1 descending (4, 3, 2)
        assert_snapshot!(batches_to_string(&batches), @r#"
        +----+----+----+----+----+----+
        | a1 | b1 | c1 | a2 | b1 | c2 |
        +----+----+----+----+----+----+
        | 1  | 4  | 7  | 10 | 4  | 70 |
        | 2  | 4  | 8  | 10 | 4  | 70 |
        | 3  | 2  | 9  | 10 | 4  | 70 |
        | 3  | 2  | 9  | 20 | 3  | 80 |
        | 3  | 2  | 9  | 30 | 2  | 90 |
        +----+----+----+----+----+----+
        "#);
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

        let (_, batches) =
            join_collect(left, right, on, Operator::Gt, JoinType::Inner).await?;

        // Grouped by right in ascending evaluation for > (1,2,3)
        assert_snapshot!(batches_to_string(&batches), @r#"
        +----+----+----+----+----+----+
        | a1 | b1 | c1 | a2 | b1 | c2 |
        +----+----+----+----+----+----+
        | 2  | 2  | 8  | 20 | 1  | 80 |
        | 3  | 4  | 9  | 20 | 1  | 80 |
        | 3  | 4  | 9  | 30 | 2  | 90 |
        | 3  | 4  | 9  | 10 | 3  | 70 |
        +----+----+----+----+----+----+
        "#);
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

        let (_, batches) =
            join_collect(left, right, on, Operator::LtEq, JoinType::Left).await?;

        assert_snapshot!(batches_to_string(&batches), @r#"
        +----+----+----+----+----+----+
        | a1 | b1 | c1 | a2 | b1 | c2 |
        +----+----+----+----+----+----+
        | 3  | 1  | 9  | 10 | 3  | 70 |
        | 1  | 5  | 7  |    |    |    |
        | 2  | 4  | 8  |    |    |    |
        +----+----+----+----+----+----+
        "#);
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

        let (_, batches) =
            join_collect(left, right, on, Operator::GtEq, JoinType::Right).await?;

        assert_snapshot!(batches_to_string(&batches), @r#"
        +----+----+----+----+----+----+
        | a1 | b1 | c1 | a2 | b1 | c2 |
        +----+----+----+----+----+----+
        |    |    |    | 10 | 3  | 70 |
        |    |    |    | 20 | 5  | 80 |
        +----+----+----+----+----+----+
        "#);
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

        let (_, batches) =
            join_collect(left, right, on, Operator::Lt, JoinType::Inner).await?;

        assert_snapshot!(batches_to_string(&batches), @r#"
        +----+----+-----+----+----+----+
        | a1 | b1 | c1  | a2 | b1 | c2 |
        +----+----+-----+----+----+----+
        | 42 | 5  | 999 | 30 | 7  | 90 |
        +----+----+-----+----+----+----+
        "#);
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

        let (_, batches) =
            join_collect(left, right, on, Operator::Gt, JoinType::Inner).await?;

        assert_snapshot!(batches_to_string(&batches), @r#"
        +----+----+----+----+----+----+
        | a1 | b1 | c1 | a2 | b1 | c2 |
        +----+----+----+----+----+----+
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
