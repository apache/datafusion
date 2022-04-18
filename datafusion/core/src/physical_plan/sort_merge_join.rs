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

//! Defines the Sort-Merge join execution plan.
//! A sort-merge join plan consumes two sorted children plan and produces
//! joined output by given join type and other options.

use std::any::Any;
use std::cmp::Ordering;
use std::collections::VecDeque;
use std::ops::Range;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow::array::*;
use arrow::compute::SortOptions;
use arrow::datatypes::{DataType, Schema, SchemaRef};
use arrow::error::{ArrowError, Result as ArrowResult};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use futures::{Stream, StreamExt};

use crate::error::DataFusionError;
use crate::error::Result;
use crate::execution::context::TaskContext;
use crate::logical_plan::JoinType;
use crate::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use crate::physical_plan::expressions::Column;
use crate::physical_plan::expressions::PhysicalSortExpr;
use crate::physical_plan::join_utils::{build_join_schema, check_join_is_valid, JoinOn};
use crate::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricBuilder, MetricsSet};
use crate::physical_plan::{
    metrics, ExecutionPlan, Partitioning, RecordBatchStream, SendableRecordBatchStream,
    Statistics,
};

/// join execution plan executes partitions in parallel and combines them into a set of
/// partitions.
#[derive(Debug)]
pub struct SortMergeJoinExec {
    /// Left sorted joining execution plan
    left: Arc<dyn ExecutionPlan>,
    /// Right sorting joining execution plan
    right: Arc<dyn ExecutionPlan>,
    /// Set of common columns used to join on
    on: JoinOn,
    /// How the join is performed
    join_type: JoinType,
    /// The schema once the join is applied
    schema: SchemaRef,
    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,
    /// Sort options used in sorting left and right execution plans
    sort_options: SortOptions,
    /// If null_equals_null is true, null == null else null != null
    null_equals_null: bool,
}

impl SortMergeJoinExec {
    /// Tries to create a new [SortMergeJoinExec].
    /// # Error
    /// This function errors when it is not possible to join the left and right sides on keys `on`.
    pub fn try_new(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        on: JoinOn,
        join_type: JoinType,
        sort_options: SortOptions,
        null_equals_null: bool,
    ) -> Result<Self> {
        let left_schema = left.schema();
        let right_schema = right.schema();

        check_join_is_valid(&left_schema, &right_schema, &on)?;
        let schema =
            Arc::new(build_join_schema(&left_schema, &right_schema, &join_type).0);

        Ok(Self {
            left,
            right,
            on,
            join_type,
            schema,
            metrics: ExecutionPlanMetricsSet::new(),
            sort_options,
            null_equals_null,
        })
    }
}

#[async_trait]
impl ExecutionPlan for SortMergeJoinExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn output_partitioning(&self) -> Partitioning {
        self.right.output_partitioning()
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        self.right.output_ordering()
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![self.left.clone(), self.right.clone()]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        match &children[..] {
            [left, right] => Ok(Arc::new(SortMergeJoinExec::try_new(
                left.clone(),
                right.clone(),
                self.on.clone(),
                self.join_type,
                self.sort_options,
                self.null_equals_null,
            )?)),
            _ => Err(DataFusionError::Internal(
                "SortMergeJoin wrong number of children".to_string(),
            )),
        }
    }

    async fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let (streamed, buffered, on_streamed, on_buffered) = match self.join_type {
            JoinType::Inner
            | JoinType::Left
            | JoinType::Full
            | JoinType::Anti
            | JoinType::Semi => (
                self.left.clone(),
                self.right.clone(),
                self.on.iter().map(|on| on.0.clone()).collect(),
                self.on.iter().map(|on| on.1.clone()).collect(),
            ),
            JoinType::Right => (
                self.right.clone(),
                self.left.clone(),
                self.on.iter().map(|on| on.1.clone()).collect(),
                self.on.iter().map(|on| on.0.clone()).collect(),
            ),
        };

        // execute children plans
        let streamed = CoalescePartitionsExec::new(streamed)
            .execute(0, context.clone())
            .await?;
        let buffered = buffered.execute(partition, context.clone()).await?;

        // create output buffer
        let batch_size = context.session_config().batch_size;
        let output_buffer = new_array_builders(self.schema(), batch_size)
            .map_err(DataFusionError::ArrowError)?;

        // create join stream
        Ok(Box::pin(SMJStream::try_new(
            self.schema.clone(),
            self.sort_options,
            self.null_equals_null,
            streamed,
            buffered,
            on_streamed,
            on_buffered,
            self.join_type,
            output_buffer,
            batch_size,
            SortMergeJoinMetrics::new(partition, &self.metrics),
        )?))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn statistics(&self) -> Statistics {
        todo!()
    }
}

/// Metrics for SortMergeJoinExec (Not yet implemented)
#[allow(dead_code)]
struct SortMergeJoinMetrics {
    /// Total time for joining probe-side batches to the build-side batches
    join_time: metrics::Time,
    /// Number of batches consumed by this operator
    input_batches: metrics::Count,
    /// Number of rows consumed by this operator
    input_rows: metrics::Count,
    /// Number of batches produced by this operator
    output_batches: metrics::Count,
    /// Number of rows produced by this operator
    output_rows: metrics::Count,
}

impl SortMergeJoinMetrics {
    #[allow(dead_code)]
    pub fn new(partition: usize, metrics: &ExecutionPlanMetricsSet) -> Self {
        let join_time = MetricBuilder::new(metrics).subset_time("join_time", partition);
        let input_batches =
            MetricBuilder::new(metrics).counter("input_batches", partition);
        let input_rows = MetricBuilder::new(metrics).counter("input_rows", partition);
        let output_batches =
            MetricBuilder::new(metrics).counter("output_batches", partition);
        let output_rows = MetricBuilder::new(metrics).output_rows(partition);

        Self {
            join_time,
            input_batches,
            input_rows,
            output_batches,
            output_rows,
        }
    }
}

/// State of SMJ stream
#[derive(Debug, PartialEq, Eq)]
enum SMJState {
    /// Init joining with a new streamed row or a new buffered batches
    Init,
    /// Polling one streamed row or one buffered batch, or both
    Polling,
    /// Joining polled data and making output
    JoinOutput,
    /// No more output
    Exhausted,
}

/// State of streamed data stream
#[derive(Debug, PartialEq, Eq)]
enum StreamedState {
    /// Init polling
    Init,
    /// Polling one streamed row
    Polling,
    /// Ready to produce one streamed row
    Ready,
    /// No more streamed row
    Exhausted,
}

/// State of buffered data stream
#[derive(Debug, PartialEq, Eq)]
enum BufferedState {
    /// Init polling
    Init,
    /// Polling first row in the next batch
    PollingFirst,
    /// Polling rest rows in the next batch
    PollingRest,
    /// Ready to produce one batch
    Ready,
    /// No more buffered batches
    Exhausted,
}

/// A buffered batch that contains contiguous rows with same join key
struct BufferedBatch {
    /// The buffered record batch
    pub batch: RecordBatch,
    /// The range in which the rows share the same join key
    pub range: Range<usize>,
    /// Array refs of the join key
    pub join_arrays: Vec<ArrayRef>,
}
impl BufferedBatch {
    fn new(batch: RecordBatch, range: Range<usize>, on_column: &[Column]) -> Self {
        let join_arrays = join_arrays(&batch, on_column);
        BufferedBatch {
            batch,
            range,
            join_arrays,
        }
    }
}

/// Sort-merge join stream that consumes streamed and buffered data stream
/// and produces joined output
struct SMJStream {
    /// Current state of the stream
    pub state: SMJState,
    /// Output schema
    pub schema: SchemaRef,
    /// Sort options used to sort streamed and buffered data stream
    pub sort_options: SortOptions,
    /// null == null?
    pub null_equals_null: bool,
    /// Input schema of streamed
    pub streamed_schema: SchemaRef,
    /// Input schema of buffered
    pub buffered_schema: SchemaRef,
    /// Number of columns of streamed
    pub num_streamed_columns: usize,
    /// Number of columns of buffered
    pub num_buffered_columns: usize,
    /// Streamed data stream
    pub streamed: SendableRecordBatchStream,
    /// Buffered data stream
    pub buffered: SendableRecordBatchStream,
    /// Current processing record batch of streamed
    pub streamed_batch: RecordBatch,
    /// Current processing streamed join arrays
    pub streamed_join_arrays: Vec<ArrayRef>,
    /// Current processing row of streamed
    pub streamed_idx: usize,
    /// Currrent buffered data
    pub buffered_data: BufferedData,
    /// (used in outer join) Is current streamed row joined at least once?
    pub streamed_joined: bool,
    /// (used in outer join) Is current buffered batches joined at least once?
    pub buffered_joined: bool,
    /// State of streamed
    pub streamed_state: StreamedState,
    /// State of buffered
    pub buffered_state: BufferedState,
    /// The comparison result of current streamed row and buffered batches
    pub current_ordering: Ordering,
    /// Join key columns of streamed
    pub on_streamed: Vec<Column>,
    /// Join key columns of buffered
    pub on_buffered: Vec<Column>,
    /// Staging output array builders
    pub output_buffer: Vec<Box<dyn ArrayBuilder>>,
    /// Staging output size
    pub output_size: usize,
    /// Target output batch size
    pub batch_size: usize,
    /// How the join is performed
    pub join_type: JoinType,
    /// Metrics
    pub join_metrics: SortMergeJoinMetrics,
}

impl RecordBatchStream for SMJStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl Stream for SMJStream {
    type Item = ArrowResult<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        self.join_metrics.join_time.timer();
        loop {
            match &self.state {
                SMJState::Init => {
                    self.buffered_data.scanning_reset();
                    let streamed_exhausted =
                        self.streamed_state == StreamedState::Exhausted;
                    let buffered_exhausted =
                        self.buffered_state == BufferedState::Exhausted;
                    self.state = if streamed_exhausted && buffered_exhausted {
                        SMJState::Exhausted
                    } else {
                        match self.current_ordering {
                            Ordering::Less | Ordering::Equal => {
                                if !streamed_exhausted {
                                    self.streamed_joined = false;
                                    self.streamed_state = StreamedState::Init;
                                }
                            }
                            Ordering::Greater => {
                                if !buffered_exhausted {
                                    self.buffered_joined = false;
                                    self.buffered_state = BufferedState::Init;
                                }
                            }
                        }
                        SMJState::Polling
                    };
                }
                SMJState::Polling => {
                    if ![StreamedState::Exhausted, StreamedState::Ready]
                        .contains(&self.streamed_state)
                    {
                        match self.poll_streamed_row(cx) {
                            Poll::Ready(Some(Ok(()))) => {}
                            Poll::Ready(Some(Err(e))) => {
                                return Poll::Ready(Some(Err(e)))
                            }
                            Poll::Ready(None) => {}
                            Poll::Pending => return Poll::Pending,
                        }
                    }

                    if ![BufferedState::Exhausted, BufferedState::Ready]
                        .contains(&self.buffered_state)
                    {
                        match self.poll_buffered_batches(cx) {
                            Poll::Ready(Some(Ok(()))) => {}
                            Poll::Ready(Some(Err(e))) => {
                                return Poll::Ready(Some(Err(e)))
                            }
                            Poll::Ready(None) => {}
                            Poll::Pending => return Poll::Pending,
                        }
                    }
                    let streamed_exhausted =
                        self.streamed_state == StreamedState::Exhausted;
                    let buffered_exhausted =
                        self.buffered_state == BufferedState::Exhausted;
                    if streamed_exhausted && buffered_exhausted {
                        self.state = SMJState::Exhausted;
                        continue;
                    }
                    self.current_ordering = self.compare_streamed_buffered()?;
                    self.state = SMJState::JoinOutput;
                }
                SMJState::JoinOutput => {
                    self.join_partial()?;
                    if self.output_size == self.batch_size {
                        let record_batch = self.output_record_batch_and_reset()?;
                        self.join_metrics.output_batches.add(1);
                        self.join_metrics.output_rows.add(record_batch.num_rows());
                        return Poll::Ready(Some(Ok(record_batch)));
                    }
                    if self.buffered_data.scanning_finished() {
                        if self.current_ordering.is_le() {
                            self.streamed_joined = true;
                        }
                        if self.current_ordering.is_ge() {
                            self.buffered_joined = true;
                        }
                        self.state = SMJState::Init;
                    }
                }
                SMJState::Exhausted => {
                    if self.output_size > 0 {
                        let record_batch = self.output_record_batch_and_reset()?;
                        self.join_metrics.output_batches.add(1);
                        self.join_metrics.output_rows.add(record_batch.num_rows());
                        return Poll::Ready(Some(Ok(record_batch)));
                    }
                    return Poll::Ready(None);
                }
            }
        }
    }
}

impl SMJStream {
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        schema: SchemaRef,
        sort_options: SortOptions,
        null_equals_null: bool,
        streamed: SendableRecordBatchStream,
        buffered: SendableRecordBatchStream,
        on_streamed: Vec<Column>,
        on_buffered: Vec<Column>,
        join_type: JoinType,
        output_buffer: Vec<Box<dyn ArrayBuilder>>,
        batch_size: usize,
        join_metrics: SortMergeJoinMetrics,
    ) -> Result<Self> {
        Ok(Self {
            state: SMJState::Init,
            sort_options,
            null_equals_null,
            schema: schema.clone(),
            streamed_schema: streamed.schema(),
            buffered_schema: buffered.schema(),
            num_streamed_columns: streamed.schema().fields().len(),
            num_buffered_columns: buffered.schema().fields().len(),
            streamed,
            buffered,
            streamed_batch: RecordBatch::new_empty(schema),
            streamed_join_arrays: vec![],
            streamed_idx: 0,
            buffered_data: BufferedData::default(),
            streamed_joined: false,
            buffered_joined: false,
            streamed_state: StreamedState::Init,
            buffered_state: BufferedState::Init,
            current_ordering: Ordering::Equal,
            on_streamed,
            on_buffered,
            output_buffer,
            output_size: 0,
            batch_size,
            join_type,
            join_metrics,
        })
    }

    /// Poll next streamed row
    fn poll_streamed_row(&mut self, cx: &mut Context) -> Poll<Option<ArrowResult<()>>> {
        loop {
            match &self.streamed_state {
                StreamedState::Init => {
                    if self.streamed_idx + 1 < self.streamed_batch.num_rows() {
                        self.streamed_idx += 1;
                        self.streamed_state = StreamedState::Ready;
                        return Poll::Ready(Some(Ok(())));
                    } else {
                        self.streamed_state = StreamedState::Polling;
                    }
                    continue;
                }
                StreamedState::Polling => match self.streamed.poll_next_unpin(cx)? {
                    Poll::Pending => {
                        return Poll::Pending;
                    }
                    Poll::Ready(None) => {
                        self.streamed_state = StreamedState::Exhausted;
                    }
                    Poll::Ready(Some(batch)) => {
                        if batch.num_rows() > 0 {
                            self.join_metrics.input_batches.add(1);
                            self.join_metrics.input_rows.add(batch.num_rows());
                            self.streamed_batch = batch;
                            self.streamed_join_arrays =
                                join_arrays(&self.streamed_batch, &self.on_streamed);
                            self.streamed_idx = 0;
                            self.streamed_state = StreamedState::Ready;
                        }
                    }
                },
                StreamedState::Ready => {
                    return Poll::Ready(Some(Ok(())));
                }
                StreamedState::Exhausted => {
                    return Poll::Ready(None);
                }
            }
        }
    }

    /// Poll next buffered batches
    fn poll_buffered_batches(
        &mut self,
        cx: &mut Context,
    ) -> Poll<Option<ArrowResult<()>>> {
        loop {
            match &self.buffered_state {
                BufferedState::Init => {
                    // pop previous buffered batches
                    while !self.buffered_data.batches.is_empty() {
                        let head_batch = self.buffered_data.head_batch();
                        if head_batch.range.end == head_batch.batch.num_rows() {
                            self.buffered_data.batches.pop_front();
                        } else {
                            break;
                        }
                    }
                    if self.buffered_data.batches.is_empty() {
                        self.buffered_state = BufferedState::PollingFirst;
                    } else {
                        let tail_batch = self.buffered_data.tail_batch_mut();
                        tail_batch.range.start = tail_batch.range.end;
                        tail_batch.range.end += 1;
                        self.buffered_state = BufferedState::PollingRest;
                    }
                }
                BufferedState::PollingFirst => match self.buffered.poll_next_unpin(cx)? {
                    Poll::Pending => {
                        return Poll::Pending;
                    }
                    Poll::Ready(None) => {
                        self.buffered_state = BufferedState::Exhausted;
                        return Poll::Ready(None);
                    }
                    Poll::Ready(Some(batch)) => {
                        self.join_metrics.input_batches.add(1);
                        self.join_metrics.input_rows.add(batch.num_rows());
                        if batch.num_rows() > 0 {
                            self.buffered_data.batches.push_back(BufferedBatch::new(
                                batch,
                                0..1,
                                &self.on_buffered,
                            ));
                            self.buffered_state = BufferedState::PollingRest;
                        }
                    }
                },
                BufferedState::PollingRest => {
                    if self.buffered_data.tail_batch().range.end
                        < self.buffered_data.tail_batch().batch.num_rows()
                    {
                        while self.buffered_data.tail_batch().range.end
                            < self.buffered_data.tail_batch().batch.num_rows()
                        {
                            if is_join_arrays_equal(
                                self.buffered_data.head_batch().batch.columns(),
                                self.buffered_data.head_batch().range.start,
                                self.buffered_data.tail_batch().batch.columns(),
                                self.buffered_data.tail_batch().range.end,
                            )? {
                                self.buffered_data.tail_batch_mut().range.end += 1;
                            } else {
                                self.buffered_state = BufferedState::Ready;
                                return Poll::Ready(Some(Ok(())));
                            }
                        }
                    } else {
                        match self.buffered.poll_next_unpin(cx)? {
                            Poll::Pending => {
                                return Poll::Pending;
                            }
                            Poll::Ready(None) => {
                                self.buffered_state = BufferedState::Ready;
                            }
                            Poll::Ready(Some(batch)) => {
                                self.join_metrics.input_batches.add(1);
                                self.join_metrics.input_rows.add(batch.num_rows());
                                self.buffered_data.batches.push_back(BufferedBatch::new(
                                    batch,
                                    0..0,
                                    &self.on_buffered,
                                ));
                            }
                        }
                    }
                }
                BufferedState::Ready => {
                    return Poll::Ready(Some(Ok(())));
                }
                BufferedState::Exhausted => {
                    return Poll::Ready(None);
                }
            }
        }
    }

    /// Get comparison result of streamed row and buffered batches
    fn compare_streamed_buffered(&self) -> ArrowResult<Ordering> {
        if self.streamed_state == StreamedState::Exhausted {
            return Ok(Ordering::Greater);
        }
        if !self.buffered_data.has_buffered_rows() {
            return Ok(Ordering::Less);
        }

        return compare_join_arrays(
            &self.streamed_join_arrays,
            self.streamed_idx,
            &self.buffered_data.head_batch().join_arrays,
            self.buffered_data.head_batch().range.start,
            self.sort_options,
            self.null_equals_null,
        );
    }

    /// Produce join and fill output buffer until reaching target batch size
    /// or the join is finished
    fn join_partial(&mut self) -> ArrowResult<()> {
        // decide streamed/buffered output columns by join type
        let output_parts =
            self.output_buffer
                .split_at_mut(if self.join_type != JoinType::Right {
                    self.num_streamed_columns
                } else {
                    self.num_buffered_columns
                });
        let (streamed_output, buffered_output) = if self.join_type != JoinType::Right {
            (output_parts.0, output_parts.1)
        } else {
            (output_parts.1, output_parts.0)
        };

        match self.current_ordering {
            Ordering::Less => {
                let output_streamed_join = match self.join_type {
                    JoinType::Inner | JoinType::Semi => false,
                    JoinType::Left
                    | JoinType::Right
                    | JoinType::Full
                    | JoinType::Anti => !self.streamed_joined,
                };

                // streamed joins null
                if output_streamed_join {
                    append_row_to_output(
                        &self.streamed_batch,
                        self.streamed_idx,
                        streamed_output,
                    )?;
                    append_nulls_row_to_output(&self.buffered_schema, buffered_output)?;
                    self.output_size += 1;
                }
                self.buffered_data.scanning_finish();
            }
            Ordering::Equal => {
                let output_equal_join = match self.join_type {
                    JoinType::Inner
                    | JoinType::Left
                    | JoinType::Right
                    | JoinType::Full
                    | JoinType::Semi => true,
                    JoinType::Anti => false,
                };

                // streamed joins buffered
                if !output_equal_join {
                    self.buffered_data.scanning_finish();
                }
            }
            Ordering::Greater => {
                let output_buffered_join = match self.join_type {
                    JoinType::Inner
                    | JoinType::Left
                    | JoinType::Right
                    | JoinType::Anti
                    | JoinType::Semi => false,
                    JoinType::Full => !self.buffered_joined,
                };

                // null joins buffered
                if !output_buffered_join {
                    self.buffered_data.scanning_finish();
                }
            }
        }

        // scan buffered stream and write to output buffer
        while !self.buffered_data.scanning_finished()
            && self.output_size < self.batch_size
        {
            if self.current_ordering == Ordering::Equal {
                append_row_to_output(
                    &self.streamed_batch,
                    self.streamed_idx,
                    streamed_output,
                )?;
            } else {
                append_nulls_row_to_output(&self.streamed_schema, streamed_output)?;
            }

            append_row_to_output(
                &self.buffered_data.scanning_batch().batch,
                self.buffered_data.scanning_idx(),
                buffered_output,
            )?;
            self.output_size += 1;
            self.buffered_data.scanning_advance();
        }
        Ok(())
    }

    fn output_record_batch_and_reset(&mut self) -> ArrowResult<RecordBatch> {
        let record_batch =
            make_batch(self.schema.clone(), self.output_buffer.drain(..).collect())?;
        self.output_size = 0;
        self.output_buffer
            .extend(new_array_builders(self.schema.clone(), self.batch_size)?);
        Ok(record_batch)
    }
}

/// Buffered data contains all buffered batches with one unique join key
#[derive(Default)]
struct BufferedData {
    /// Buffered batches with the same key
    pub batches: VecDeque<BufferedBatch>,
    /// current scanning batch index used in join_partial()
    pub scanning_batch_idx: usize,
    /// current scanning offset used in join_partial()
    pub scanning_offset: usize,
}
impl BufferedData {
    pub fn head_batch(&self) -> &BufferedBatch {
        self.batches.front().unwrap()
    }

    pub fn tail_batch(&self) -> &BufferedBatch {
        self.batches.back().unwrap()
    }

    pub fn tail_batch_mut(&mut self) -> &mut BufferedBatch {
        self.batches.back_mut().unwrap()
    }

    pub fn has_buffered_rows(&self) -> bool {
        self.batches.iter().any(|batch| !batch.range.is_empty())
    }

    pub fn scanning_reset(&mut self) {
        self.scanning_batch_idx = 0;
        self.scanning_offset = 0;
    }

    pub fn scanning_advance(&mut self) {
        self.scanning_offset += 1;
        while !self.scanning_finished() && self.scanning_batch_finished() {
            self.scanning_batch_idx += 1;
            self.scanning_offset = 0;
        }
    }

    pub fn scanning_batch(&self) -> &BufferedBatch {
        &self.batches[self.scanning_batch_idx]
    }

    pub fn scanning_idx(&self) -> usize {
        self.scanning_batch().range.start + self.scanning_offset
    }

    pub fn scanning_batch_finished(&self) -> bool {
        self.scanning_offset == self.scanning_batch().range.len()
    }

    pub fn scanning_finished(&self) -> bool {
        self.scanning_batch_idx == self.batches.len()
    }

    pub fn scanning_finish(&mut self) {
        self.scanning_batch_idx = self.batches.len();
        self.scanning_offset = 0;
    }
}

/// Get join array refs of given batch and join columns
fn join_arrays(batch: &RecordBatch, on_column: &[Column]) -> Vec<ArrayRef> {
    on_column
        .iter()
        .map(|c| batch.column(c.index()).clone())
        .collect()
}

/// Get comparison result of two rows of join arrays
fn compare_join_arrays(
    left_arrays: &[ArrayRef],
    left: usize,
    right_arrays: &[ArrayRef],
    right: usize,
    sort_options: SortOptions,
    null_equals_null: bool,
) -> ArrowResult<Ordering> {
    let mut res = Ordering::Equal;
    for (left_array, right_array) in left_arrays.iter().zip(right_arrays) {
        macro_rules! compare_value {
            ($T:ty) => {{
                let left_array = left_array.as_any().downcast_ref::<$T>().unwrap();
                let right_array = right_array.as_any().downcast_ref::<$T>().unwrap();
                match (left_array.is_null(left), right_array.is_null(right)) {
                    (false, false) => {
                        let left_value = &left_array.value(left);
                        let right_value = &right_array.value(right);
                        res = left_value.partial_cmp(right_value).unwrap();
                        if sort_options.descending {
                            res = res.reverse();
                        }
                    }
                    (true, false) => {
                        res = if sort_options.nulls_first {
                            Ordering::Less
                        } else {
                            Ordering::Greater
                        };
                    }
                    (false, true) => {
                        res = if sort_options.nulls_first {
                            Ordering::Greater
                        } else {
                            Ordering::Less
                        };
                    }
                    _ => {
                        res = if null_equals_null {
                            Ordering::Equal
                        } else {
                            Ordering::Less
                        };
                    }
                }
            }};
        }

        match left_array.data_type() {
            DataType::Null => {}
            DataType::Boolean => compare_value!(BooleanArray),
            DataType::Int8 => compare_value!(Int8Array),
            DataType::Int16 => compare_value!(Int16Array),
            DataType::Int32 => compare_value!(Int32Array),
            DataType::Int64 => compare_value!(Int64Array),
            DataType::UInt8 => compare_value!(UInt8Array),
            DataType::UInt16 => compare_value!(UInt16Array),
            DataType::UInt32 => compare_value!(UInt32Array),
            DataType::UInt64 => compare_value!(UInt64Array),
            DataType::Timestamp(_, None) => compare_value!(Int64Array),
            DataType::Utf8 => compare_value!(StringArray),
            DataType::LargeUtf8 => compare_value!(LargeStringArray),
            _ => {
                return Err(ArrowError::NotYetImplemented(
                    "Unsupported data type in sort merge join comparator".to_owned(),
                ));
            }
        }
        if !res.is_eq() {
            break;
        }
    }
    Ok(res)
}

/// A faster version of compare_join_arrays() that only output whether
/// the given two rows are equal
fn is_join_arrays_equal(
    left_arrays: &[ArrayRef],
    left: usize,
    right_arrays: &[ArrayRef],
    right: usize,
) -> ArrowResult<bool> {
    let mut is_equal = true;
    for (left_array, right_array) in left_arrays.iter().zip(right_arrays) {
        macro_rules! compare_value {
            ($T:ty) => {{
                let left_array = left_array.as_any().downcast_ref::<$T>().unwrap();
                let right_array = right_array.as_any().downcast_ref::<$T>().unwrap();
                match (left_array.is_null(left), right_array.is_null(right)) {
                    (false, false) => {
                        if left_array.value(left) != right_array.value(right) {
                            is_equal = false;
                        }
                    }
                    (true, false) => is_equal = false,
                    (false, true) => is_equal = false,
                    _ => {}
                }
            }};
        }

        match left_array.data_type() {
            DataType::Null => {}
            DataType::Boolean => compare_value!(BooleanArray),
            DataType::Int8 => compare_value!(Int8Array),
            DataType::Int16 => compare_value!(Int16Array),
            DataType::Int32 => compare_value!(Int32Array),
            DataType::Int64 => compare_value!(Int64Array),
            DataType::UInt8 => compare_value!(UInt8Array),
            DataType::UInt16 => compare_value!(UInt16Array),
            DataType::UInt32 => compare_value!(UInt32Array),
            DataType::UInt64 => compare_value!(UInt64Array),
            DataType::Timestamp(_, None) => compare_value!(Int64Array),
            DataType::Utf8 => compare_value!(StringArray),
            DataType::LargeUtf8 => compare_value!(LargeStringArray),
            _ => {
                return Err(ArrowError::NotYetImplemented(
                    "Unsupported data type in sort merge join comparator".to_owned(),
                ));
            }
        }
        if !is_equal {
            return Ok(false);
        }
    }
    Ok(true)
}

/// Create new array builders of given schema and batch size
fn new_array_builders(
    schema: SchemaRef,
    batch_size: usize,
) -> ArrowResult<Vec<Box<dyn ArrayBuilder>>> {
    let arrays: Vec<Box<dyn ArrayBuilder>> = schema
        .fields()
        .iter()
        .map(|field| {
            let dt = field.data_type();
            make_builder(dt, batch_size)
        })
        .collect();
    Ok(arrays)
}

/// Append one row to part of output buffer (the array builders)
fn append_row_to_output(
    batch: &RecordBatch,
    idx: usize,
    arrays: &mut [Box<dyn ArrayBuilder>],
) -> ArrowResult<()> {
    if !arrays.is_empty() {
        return batch
            .columns()
            .iter()
            .zip(batch.schema().fields())
            .enumerate()
            .try_for_each(|(i, (column, f))| {
                array_append_value(f.data_type(), &mut arrays[i], &*column, idx)
            });
    }
    Ok(())
}

/// Append one row which all values are null to part of output buffer (the
/// array builders), used in outer join
fn append_nulls_row_to_output(
    schema: &Schema,
    arrays: &mut [Box<dyn ArrayBuilder>],
) -> ArrowResult<()> {
    if !arrays.is_empty() {
        return schema
            .fields()
            .iter()
            .enumerate()
            .try_for_each(|(i, f)| array_append_null(f.data_type(), &mut arrays[i]));
    }
    Ok(())
}

/// Finish output buffer and produce one record batch
fn make_batch(
    schema: SchemaRef,
    mut arrays: Vec<Box<dyn ArrayBuilder>>,
) -> ArrowResult<RecordBatch> {
    let columns = arrays.iter_mut().map(|array| array.finish()).collect();
    RecordBatch::try_new(schema, columns)
}

/// Append null value to a array builder
fn array_append_null(
    data_type: &DataType,
    to: &mut Box<dyn ArrayBuilder>,
) -> ArrowResult<()> {
    macro_rules! append_null {
        ($TO:ty) => {{
            to.as_any_mut().downcast_mut::<$TO>().unwrap().append_null()
        }};
    }
    match data_type {
        DataType::Boolean => append_null!(BooleanBuilder),
        DataType::Int8 => append_null!(Int8Builder),
        DataType::Int16 => append_null!(Int16Builder),
        DataType::Int32 => append_null!(Int32Builder),
        DataType::Int64 => append_null!(Int64Builder),
        DataType::UInt8 => append_null!(UInt8Builder),
        DataType::UInt16 => append_null!(UInt16Builder),
        DataType::UInt32 => append_null!(UInt32Builder),
        DataType::UInt64 => append_null!(UInt64Builder),
        DataType::Float32 => append_null!(Float32Builder),
        DataType::Float64 => append_null!(Float64Builder),
        DataType::Utf8 => append_null!(GenericStringBuilder<i32>),
        _ => todo!(),
    }
}

/// Append value to a array builder
fn array_append_value(
    data_type: &DataType,
    to: &mut Box<dyn ArrayBuilder>,
    from: &dyn Array,
    idx: usize,
) -> ArrowResult<()> {
    macro_rules! append_value {
        ($TO:ty, $FROM:ty) => {{
            let to = to.as_any_mut().downcast_mut::<$TO>().unwrap();
            let from = from.as_any().downcast_ref::<$FROM>().unwrap();
            if from.is_valid(idx) {
                to.append_value(from.value(idx))
            } else {
                to.append_null()
            }
        }};
    }

    match data_type {
        DataType::Boolean => append_value!(BooleanBuilder, BooleanArray),
        DataType::Int8 => append_value!(Int8Builder, Int8Array),
        DataType::Int16 => append_value!(Int16Builder, Int16Array),
        DataType::Int32 => append_value!(Int32Builder, Int32Array),
        DataType::Int64 => append_value!(Int64Builder, Int64Array),
        DataType::UInt8 => append_value!(UInt8Builder, UInt8Array),
        DataType::UInt16 => append_value!(UInt16Builder, UInt16Array),
        DataType::UInt32 => append_value!(UInt32Builder, UInt32Array),
        DataType::UInt64 => append_value!(UInt64Builder, UInt64Array),
        DataType::Float32 => append_value!(Float32Builder, Float32Array),
        DataType::Float64 => append_value!(Float64Builder, Float64Array),
        DataType::Utf8 => {
            append_value!(GenericStringBuilder<i32>, GenericStringArray<i32>)
        }
        _ => todo!(),
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::Int32Array;
    use arrow::compute::SortOptions;
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    use arrow::record_batch::RecordBatch;

    use crate::assert_batches_sorted_eq;
    use crate::error::Result;
    use crate::logical_plan::JoinType;
    use crate::physical_plan::expressions::Column;
    use crate::physical_plan::join_utils::JoinOn;
    use crate::physical_plan::memory::MemoryExec;
    use crate::physical_plan::sort_merge_join::SortMergeJoinExec;
    use crate::physical_plan::{common, ExecutionPlan};
    use crate::prelude::{SessionConfig, SessionContext};
    use crate::test::{build_table_i32, columns};

    fn build_table(
        a: (&str, &Vec<i32>),
        b: (&str, &Vec<i32>),
        c: (&str, &Vec<i32>),
    ) -> Arc<dyn ExecutionPlan> {
        let batch = build_table_i32(a, b, c);
        let schema = batch.schema();
        Arc::new(MemoryExec::try_new(&[vec![batch]], schema, None).unwrap())
    }

    /// returns a table with 3 columns of i32 in memory
    pub fn build_table_i32_nullable(
        a: (&str, &Vec<Option<i32>>),
        b: (&str, &Vec<Option<i32>>),
        c: (&str, &Vec<Option<i32>>),
    ) -> Arc<dyn ExecutionPlan> {
        let schema = Arc::new(Schema::new(vec![
            Field::new(a.0, DataType::Int32, true),
            Field::new(b.0, DataType::Int32, true),
            Field::new(c.0, DataType::Int32, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(a.1.clone())),
                Arc::new(Int32Array::from(b.1.clone())),
                Arc::new(Int32Array::from(c.1.clone())),
            ],
        )
        .unwrap();
        Arc::new(MemoryExec::try_new(&[vec![batch]], schema, None).unwrap())
    }

    fn join(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        on: JoinOn,
        join_type: JoinType,
    ) -> Result<SortMergeJoinExec> {
        SortMergeJoinExec::try_new(
            left,
            right,
            on,
            join_type,
            SortOptions::default(),
            false,
        )
    }

    fn join_with_options(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        on: JoinOn,
        join_type: JoinType,
        sort_options: SortOptions,
        null_equals_null: bool,
    ) -> Result<SortMergeJoinExec> {
        SortMergeJoinExec::try_new(
            left,
            right,
            on,
            join_type,
            sort_options,
            null_equals_null,
        )
    }

    async fn join_collect(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        on: JoinOn,
        join_type: JoinType,
    ) -> Result<(Vec<String>, Vec<RecordBatch>)> {
        join_collect_with_options(
            left,
            right,
            on,
            join_type,
            SortOptions::default(),
            false,
        )
        .await
    }

    async fn join_collect_with_options(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        on: JoinOn,
        join_type: JoinType,
        sort_options: SortOptions,
        null_equals_null: bool,
    ) -> Result<(Vec<String>, Vec<RecordBatch>)> {
        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
        let join = join_with_options(
            left,
            right,
            on,
            join_type,
            sort_options,
            null_equals_null,
        )?;
        let columns = columns(&join.schema());

        let stream = join.execute(0, task_ctx).await?;
        let batches = common::collect(stream).await?;
        Ok((columns, batches))
    }

    async fn join_collect_batch_size_equals_two(
        left: Arc<dyn ExecutionPlan>,
        right: Arc<dyn ExecutionPlan>,
        on: JoinOn,
        join_type: JoinType,
    ) -> Result<(Vec<String>, Vec<RecordBatch>)> {
        let session_ctx =
            SessionContext::with_config(SessionConfig::new().with_batch_size(2));
        let task_ctx = session_ctx.task_ctx();
        let join = join(left, right, on, join_type)?;
        let columns = columns(&join.schema());

        let stream = join.execute(0, task_ctx).await?;
        let batches = common::collect(stream).await?;
        Ok((columns, batches))
    }

    #[tokio::test]
    async fn join_inner_one() -> Result<()> {
        let left = build_table(
            ("a1", &vec![1, 2, 3]),
            ("b1", &vec![4, 5, 5]), // this has a repetition
            ("c1", &vec![7, 8, 9]),
        );
        let right = build_table(
            ("a2", &vec![10, 20, 30]),
            ("b1", &vec![4, 5, 6]),
            ("c2", &vec![70, 80, 90]),
        );

        let on = vec![(
            Column::new_with_schema("b1", &left.schema())?,
            Column::new_with_schema("b1", &right.schema())?,
        )];

        let (_, batches) = join_collect(left, right, on, JoinType::Inner).await?;

        let expected = vec![
            "+----+----+----+----+----+----+",
            "| a1 | b1 | c1 | a2 | b1 | c2 |",
            "+----+----+----+----+----+----+",
            "| 1  | 4  | 7  | 10 | 4  | 70 |",
            "| 2  | 5  | 8  | 20 | 5  | 80 |",
            "| 3  | 5  | 9  | 20 | 5  | 80 |",
            "+----+----+----+----+----+----+",
        ];
        assert_batches_sorted_eq!(expected, &batches);
        Ok(())
    }

    #[tokio::test]
    async fn join_inner_two() -> Result<()> {
        let left = build_table(
            ("a1", &vec![1, 2, 2]),
            ("b2", &vec![1, 2, 2]),
            ("c1", &vec![7, 8, 9]),
        );
        let right = build_table(
            ("a1", &vec![1, 2, 3]),
            ("b2", &vec![1, 2, 2]),
            ("c2", &vec![70, 80, 90]),
        );
        let on = vec![
            (
                Column::new_with_schema("a1", &left.schema())?,
                Column::new_with_schema("a1", &right.schema())?,
            ),
            (
                Column::new_with_schema("b2", &left.schema())?,
                Column::new_with_schema("b2", &right.schema())?,
            ),
        ];

        let (_columns, batches) = join_collect(left, right, on, JoinType::Inner).await?;
        let expected = vec![
            "+----+----+----+----+----+----+",
            "| a1 | b2 | c1 | a1 | b2 | c2 |",
            "+----+----+----+----+----+----+",
            "| 1  | 1  | 7  | 1  | 1  | 70 |",
            "| 2  | 2  | 8  | 2  | 2  | 80 |",
            "| 2  | 2  | 9  | 2  | 2  | 80 |",
            "+----+----+----+----+----+----+",
        ];
        assert_batches_sorted_eq!(expected, &batches);
        Ok(())
    }

    #[tokio::test]
    async fn join_inner_with_nulls() -> Result<()> {
        let left = build_table_i32_nullable(
            ("a1", &vec![Some(1), Some(1), Some(2), Some(2)]),
            ("b2", &vec![None, Some(1), Some(2), Some(2)]), // null in key field
            ("c1", &vec![Some(1), None, Some(8), Some(9)]), // null in non-key field
        );
        let right = build_table_i32_nullable(
            ("a1", &vec![Some(1), Some(1), Some(2), Some(3)]),
            ("b2", &vec![None, Some(1), Some(2), Some(2)]),
            ("c2", &vec![Some(10), Some(70), Some(80), Some(90)]),
        );
        let on = vec![
            (
                Column::new_with_schema("a1", &left.schema())?,
                Column::new_with_schema("a1", &right.schema())?,
            ),
            (
                Column::new_with_schema("b2", &left.schema())?,
                Column::new_with_schema("b2", &right.schema())?,
            ),
        ];

        let (_, batches) = join_collect(left, right, on, JoinType::Inner).await?;
        let expected = vec![
            "+----+----+----+----+----+----+",
            "| a1 | b2 | c1 | a1 | b2 | c2 |",
            "+----+----+----+----+----+----+",
            "| 1  | 1  |    | 1  | 1  | 70 |",
            "| 2  | 2  | 8  | 2  | 2  | 80 |",
            "| 2  | 2  | 9  | 2  | 2  | 80 |",
            "+----+----+----+----+----+----+",
        ];
        assert_batches_sorted_eq!(expected, &batches);
        Ok(())
    }

    #[tokio::test]
    async fn join_inner_with_nulls_with_options() -> Result<()> {
        let left = build_table_i32_nullable(
            ("a1", &vec![Some(2), Some(2), Some(1), Some(1)]),
            ("b2", &vec![Some(2), Some(2), Some(1), None]), // null in key field
            ("c1", &vec![Some(9), Some(8), None, Some(1)]), // null in non-key field
        );
        let right = build_table_i32_nullable(
            ("a1", &vec![Some(3), Some(2), Some(1), Some(1)]),
            ("b2", &vec![Some(2), Some(2), Some(1), None]),
            ("c2", &vec![Some(90), Some(80), Some(70), Some(10)]),
        );
        let on = vec![
            (
                Column::new_with_schema("a1", &left.schema())?,
                Column::new_with_schema("a1", &right.schema())?,
            ),
            (
                Column::new_with_schema("b2", &left.schema())?,
                Column::new_with_schema("b2", &right.schema())?,
            ),
        ];

        let (_, batches) = join_collect_with_options(
            left,
            right,
            on,
            JoinType::Inner,
            SortOptions {
                descending: true,
                nulls_first: false,
            },
            true,
        )
        .await?;
        let expected = vec![
            "+----+----+----+----+----+----+",
            "| a1 | b2 | c1 | a1 | b2 | c2 |",
            "+----+----+----+----+----+----+",
            "| 1  |    | 1  | 1  |    | 10 |",
            "| 1  | 1  |    | 1  | 1  | 70 |",
            "| 2  | 2  | 8  | 2  | 2  | 80 |",
            "| 2  | 2  | 9  | 2  | 2  | 80 |",
            "+----+----+----+----+----+----+",
        ];
        //assert_eq!(batches.len(), 1);
        assert_batches_sorted_eq!(expected, &batches);
        Ok(())
    }
    #[tokio::test]
    async fn join_inner_output_two_batches() -> Result<()> {
        let left = build_table(
            ("a1", &vec![1, 2, 2]),
            ("b2", &vec![1, 2, 2]),
            ("c1", &vec![7, 8, 9]),
        );
        let right = build_table(
            ("a1", &vec![1, 2, 3]),
            ("b2", &vec![1, 2, 2]),
            ("c2", &vec![70, 80, 90]),
        );
        let on = vec![
            (
                Column::new_with_schema("a1", &left.schema())?,
                Column::new_with_schema("a1", &right.schema())?,
            ),
            (
                Column::new_with_schema("b2", &left.schema())?,
                Column::new_with_schema("b2", &right.schema())?,
            ),
        ];

        let (_, batches) =
            join_collect_batch_size_equals_two(left, right, on, JoinType::Inner).await?;
        let expected = vec![
            "+----+----+----+----+----+----+",
            "| a1 | b2 | c1 | a1 | b2 | c2 |",
            "+----+----+----+----+----+----+",
            "| 1  | 1  | 7  | 1  | 1  | 70 |",
            "| 2  | 2  | 8  | 2  | 2  | 80 |",
            "| 2  | 2  | 9  | 2  | 2  | 80 |",
            "+----+----+----+----+----+----+",
        ];
        assert_eq!(batches.len(), 2);
        assert_eq!(batches[0].num_rows(), 2);
        assert_eq!(batches[1].num_rows(), 1);
        assert_batches_sorted_eq!(expected, &batches);
        Ok(())
    }
    /// Test where the left has 1 part, the right has 2 parts => 2 parts
    #[tokio::test]
    async fn join_inner_one_two_parts_right() -> Result<()> {
        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
        let left = build_table(
            ("a1", &vec![1, 2, 3]),
            ("b1", &vec![4, 5, 5]), // this has a repetition
            ("c1", &vec![7, 8, 9]),
        );

        let batch1 = build_table_i32(
            ("a2", &vec![10, 20]),
            ("b1", &vec![4, 6]),
            ("c2", &vec![70, 80]),
        );
        let batch2 =
            build_table_i32(("a2", &vec![30]), ("b1", &vec![5]), ("c2", &vec![90]));
        let schema = batch1.schema();
        let right = Arc::new(
            MemoryExec::try_new(&[vec![batch1], vec![batch2]], schema, None).unwrap(),
        );

        let on = vec![(
            Column::new_with_schema("b1", &left.schema())?,
            Column::new_with_schema("b1", &right.schema())?,
        )];

        let join = join(left, right, on, JoinType::Inner)?;
        let columns = columns(&join.schema());
        assert_eq!(columns, vec!["a1", "b1", "c1", "a2", "b1", "c2"]);

        // first part
        let stream = join.execute(0, task_ctx.clone()).await?;
        let batches = common::collect(stream).await?;
        assert_eq!(batches.len(), 1);

        let expected = vec![
            "+----+----+----+----+----+----+",
            "| a1 | b1 | c1 | a2 | b1 | c2 |",
            "+----+----+----+----+----+----+",
            "| 1  | 4  | 7  | 10 | 4  | 70 |",
            "+----+----+----+----+----+----+",
        ];
        assert_batches_sorted_eq!(expected, &batches);

        // second part
        let stream = join.execute(1, task_ctx.clone()).await?;
        let batches = common::collect(stream).await?;
        assert_eq!(batches.len(), 1);
        let expected = vec![
            "+----+----+----+----+----+----+",
            "| a1 | b1 | c1 | a2 | b1 | c2 |",
            "+----+----+----+----+----+----+",
            "| 2  | 5  | 8  | 30 | 5  | 90 |",
            "| 3  | 5  | 9  | 30 | 5  | 90 |",
            "+----+----+----+----+----+----+",
        ];
        assert_batches_sorted_eq!(expected, &batches);

        let metrics = join.metrics().unwrap();
        assert!(
            0 < metrics
                .sum(|m| m.value().name() == "join_time")
                .map(|v| v.as_usize())
                .unwrap()
        );
        assert_eq!(
            2,
            metrics
                .sum(|m| m.value().name() == "output_batches")
                .map(|v| v.as_usize())
                .unwrap()
        ); // 1+1
        assert_eq!(
            3,
            metrics
                .sum(|m| m.value().name() == "output_rows")
                .map(|v| v.as_usize())
                .unwrap()
        ); // 2+1
        assert_eq!(
            4,
            metrics
                .sum(|m| m.value().name() == "input_batches")
                .map(|v| v.as_usize())
                .unwrap()
        ); // (1+1) + (1+1)
        assert_eq!(
            9,
            metrics
                .sum(|m| m.value().name() == "input_rows")
                .map(|v| v.as_usize())
                .unwrap()
        ); // (3+2) + (3+1)
        Ok(())
    }

    #[tokio::test]
    async fn join_left_one() -> Result<()> {
        let left = build_table(
            ("a1", &vec![1, 2, 3]),
            ("b1", &vec![4, 5, 7]), // 7 does not exist on the right
            ("c1", &vec![7, 8, 9]),
        );
        let right = build_table(
            ("a2", &vec![10, 20, 30]),
            ("b1", &vec![4, 5, 6]),
            ("c2", &vec![70, 80, 90]),
        );
        let on = vec![(
            Column::new_with_schema("b1", &left.schema())?,
            Column::new_with_schema("b1", &right.schema())?,
        )];

        let (_, batches) = join_collect(left, right, on, JoinType::Left).await?;
        let expected = vec![
            "+----+----+----+----+----+----+",
            "| a1 | b1 | c1 | a2 | b1 | c2 |",
            "+----+----+----+----+----+----+",
            "| 1  | 4  | 7  | 10 | 4  | 70 |",
            "| 2  | 5  | 8  | 20 | 5  | 80 |",
            "| 3  | 7  | 9  |    |    |    |",
            "+----+----+----+----+----+----+",
        ];
        assert_batches_sorted_eq!(expected, &batches);
        Ok(())
    }

    #[tokio::test]
    async fn join_right_one() -> Result<()> {
        let left = build_table(
            ("a1", &vec![1, 2, 3]),
            ("b1", &vec![4, 5, 7]),
            ("c1", &vec![7, 8, 9]),
        );
        let right = build_table(
            ("a2", &vec![10, 20, 30]),
            ("b1", &vec![4, 5, 6]), // 6 does not exist on the left
            ("c2", &vec![70, 80, 90]),
        );
        let on = vec![(
            Column::new_with_schema("b1", &left.schema())?,
            Column::new_with_schema("b1", &right.schema())?,
        )];

        let (_, batches) = join_collect(left, right, on, JoinType::Right).await?;
        let expected = vec![
            "+----+----+----+----+----+----+",
            "| a1 | b1 | c1 | a2 | b1 | c2 |",
            "+----+----+----+----+----+----+",
            "|    |    |    | 30 | 6  | 90 |",
            "| 1  | 4  | 7  | 10 | 4  | 70 |",
            "| 2  | 5  | 8  | 20 | 5  | 80 |",
            "+----+----+----+----+----+----+",
        ];
        assert_batches_sorted_eq!(expected, &batches);
        Ok(())
    }

    #[tokio::test]
    async fn join_full_one() -> Result<()> {
        let left = build_table(
            ("a1", &vec![1, 2, 3]),
            ("b1", &vec![4, 5, 7]), // 7 does not exist on the right
            ("c1", &vec![7, 8, 9]),
        );
        let right = build_table(
            ("a2", &vec![10, 20, 30]),
            ("b2", &vec![4, 5, 6]),
            ("c2", &vec![70, 80, 90]),
        );
        let on = vec![(
            Column::new_with_schema("b1", &left.schema()).unwrap(),
            Column::new_with_schema("b2", &right.schema()).unwrap(),
        )];

        let (_, batches) = join_collect(left, right, on, JoinType::Full).await?;
        let expected = vec![
            "+----+----+----+----+----+----+",
            "| a1 | b1 | c1 | a2 | b2 | c2 |",
            "+----+----+----+----+----+----+",
            "|    |    |    | 30 | 6  | 90 |",
            "| 1  | 4  | 7  | 10 | 4  | 70 |",
            "| 2  | 5  | 8  | 20 | 5  | 80 |",
            "| 3  | 7  | 9  |    |    |    |",
            "+----+----+----+----+----+----+",
        ];
        assert_batches_sorted_eq!(expected, &batches);
        Ok(())
    }

    #[tokio::test]
    async fn join_anti() -> Result<()> {
        let left = build_table(
            ("a1", &vec![1, 2, 2, 3, 5]),
            ("b1", &vec![4, 5, 5, 7, 7]), // 7 does not exist on the right
            ("c1", &vec![7, 8, 8, 9, 11]),
        );
        let right = build_table(
            ("a2", &vec![10, 20, 30]),
            ("b1", &vec![4, 5, 6]),
            ("c2", &vec![70, 80, 90]),
        );
        let on = vec![(
            Column::new_with_schema("b1", &left.schema())?,
            Column::new_with_schema("b1", &right.schema())?,
        )];

        let (_, batches) = join_collect(left, right, on, JoinType::Anti).await?;
        let expected = vec![
            "+----+----+----+",
            "| a1 | b1 | c1 |",
            "+----+----+----+",
            "| 3  | 7  | 9  |",
            "| 5  | 7  | 11 |",
            "+----+----+----+",
        ];
        assert_batches_sorted_eq!(expected, &batches);
        Ok(())
    }

    #[tokio::test]
    async fn join_semi() -> Result<()> {
        let left = build_table(
            ("a1", &vec![1, 2, 2, 3]),
            ("b1", &vec![4, 5, 5, 7]), // 7 does not exist on the right
            ("c1", &vec![7, 8, 8, 9]),
        );
        let right = build_table(
            ("a2", &vec![10, 20, 30]),
            ("b1", &vec![4, 5, 6]), // 5 is double on the right
            ("c2", &vec![70, 80, 90]),
        );
        let on = vec![(
            Column::new_with_schema("b1", &left.schema())?,
            Column::new_with_schema("b1", &right.schema())?,
        )];

        let (_, batches) = join_collect(left, right, on, JoinType::Semi).await?;
        let expected = vec![
            "+----+----+----+",
            "| a1 | b1 | c1 |",
            "+----+----+----+",
            "| 1  | 4  | 7  |",
            "| 2  | 5  | 8  |",
            "| 2  | 5  | 8  |",
            "+----+----+----+",
        ];
        assert_batches_sorted_eq!(expected, &batches);
        Ok(())
    }

    #[tokio::test]
    async fn join_with_duplicated_column_names() -> Result<()> {
        let left = build_table(
            ("a", &vec![1, 2, 3]),
            ("b", &vec![4, 5, 7]),
            ("c", &vec![7, 8, 9]),
        );
        let right = build_table(
            ("a", &vec![10, 20, 30]),
            ("b", &vec![1, 2, 7]),
            ("c", &vec![70, 80, 90]),
        );
        let on = vec![(
            // join on a=b so there are duplicate column names on unjoined columns
            Column::new_with_schema("a", &left.schema())?,
            Column::new_with_schema("b", &right.schema())?,
        )];

        let (_, batches) = join_collect(left, right, on, JoinType::Inner).await?;
        let expected = vec![
            "+---+---+---+----+---+----+",
            "| a | b | c | a  | b | c  |",
            "+---+---+---+----+---+----+",
            "| 1 | 4 | 7 | 10 | 1 | 70 |",
            "| 2 | 5 | 8 | 20 | 2 | 80 |",
            "+---+---+---+----+---+----+",
        ];
        assert_batches_sorted_eq!(expected, &batches);
        Ok(())
    }
}
