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

//! Defines the sort preserving merge plan

use super::common::AbortOnDropMany;
use super::metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet};
use std::any::Any;
use std::cmp::Ordering;
use std::collections::VecDeque;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow::array::DynComparator;
use arrow::{
    array::{make_array as make_arrow_array, ArrayRef, MutableArrayData},
    compute::SortOptions,
    datatypes::SchemaRef,
    error::{ArrowError, Result as ArrowResult},
    record_batch::RecordBatch,
};
use async_trait::async_trait;
use futures::channel::mpsc;
use futures::stream::FusedStream;
use futures::{Stream, StreamExt};
use hashbrown::HashMap;

use crate::error::{DataFusionError, Result};
use crate::physical_plan::{
    common::spawn_execution, expressions::PhysicalSortExpr, DisplayFormatType,
    Distribution, ExecutionPlan, Partitioning, PhysicalExpr, RecordBatchStream,
    SendableRecordBatchStream, Statistics,
};

/// Sort preserving merge execution plan
///
/// This takes an input execution plan and a list of sort expressions, and
/// provided each partition of the input plan is sorted with respect to
/// these sort expressions, this operator will yield a single partition
/// that is also sorted with respect to them
#[derive(Debug)]
pub struct SortPreservingMergeExec {
    /// Input plan
    input: Arc<dyn ExecutionPlan>,
    /// Sort expressions
    expr: Vec<PhysicalSortExpr>,
    /// The target size of yielded batches
    target_batch_size: usize,
    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,
}

impl SortPreservingMergeExec {
    /// Create a new sort execution plan
    pub fn new(
        expr: Vec<PhysicalSortExpr>,
        input: Arc<dyn ExecutionPlan>,
        target_batch_size: usize,
    ) -> Self {
        Self {
            input,
            expr,
            target_batch_size,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }

    /// Input schema
    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }

    /// Sort expressions
    pub fn expr(&self) -> &[PhysicalSortExpr] {
        &self.expr
    }
}

#[async_trait]
impl ExecutionPlan for SortPreservingMergeExec {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.input.schema()
    }

    /// Get the output partitioning of this plan
    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(1)
    }

    fn required_child_distribution(&self) -> Distribution {
        Distribution::UnspecifiedDistribution
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![self.input.clone()]
    }

    fn with_new_children(
        &self,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        match children.len() {
            1 => Ok(Arc::new(SortPreservingMergeExec::new(
                self.expr.clone(),
                children[0].clone(),
                self.target_batch_size,
            ))),
            _ => Err(DataFusionError::Internal(
                "SortPreservingMergeExec wrong number of children".to_string(),
            )),
        }
    }

    async fn execute(&self, partition: usize) -> Result<SendableRecordBatchStream> {
        if 0 != partition {
            return Err(DataFusionError::Internal(format!(
                "SortPreservingMergeExec invalid partition {}",
                partition
            )));
        }

        let baseline_metrics = BaselineMetrics::new(&self.metrics, partition);

        let input_partitions = self.input.output_partitioning().partition_count();
        match input_partitions {
            0 => Err(DataFusionError::Internal(
                "SortPreservingMergeExec requires at least one input partition"
                    .to_owned(),
            )),
            1 => {
                // bypass if there is only one partition to merge (no metrics in this case either)
                self.input.execute(0).await
            }
            _ => {
                let (receivers, join_handles) = (0..input_partitions)
                    .into_iter()
                    .map(|part_i| {
                        let (sender, receiver) = mpsc::channel(1);
                        let join_handle =
                            spawn_execution(self.input.clone(), sender, part_i);
                        (receiver, join_handle)
                    })
                    .unzip();

                Ok(Box::pin(SortPreservingMergeStream::new(
                    receivers,
                    AbortOnDropMany(join_handles),
                    self.schema(),
                    &self.expr,
                    self.target_batch_size,
                    baseline_metrics,
                )))
            }
        }
    }

    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default => {
                let expr: Vec<String> = self.expr.iter().map(|e| e.to_string()).collect();
                write!(f, "SortPreservingMergeExec: [{}]", expr.join(","))
            }
        }
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn statistics(&self) -> Statistics {
        self.input.statistics()
    }
}

/// A `SortKeyCursor` is created from a `RecordBatch`, and a set of
/// `PhysicalExpr` that when evaluated on the `RecordBatch` yield the sort keys.
///
/// Additionally it maintains a row cursor that can be advanced through the rows
/// of the provided `RecordBatch`
///
/// `SortKeyCursor::compare` can then be used to compare the sort key pointed to
/// by this row cursor, with that of another `SortKeyCursor`. A cursor stores
/// a row comparator for each other cursor that it is compared to.
struct SortKeyCursor {
    columns: Vec<ArrayRef>,
    cur_row: usize,
    num_rows: usize,

    // An index uniquely identifying the record batch scanned by this cursor.
    batch_idx: usize,
    batch: RecordBatch,

    // A collection of comparators that compare rows in this cursor's batch to
    // the cursors in other batches. Other batches are uniquely identified by
    // their batch_idx.
    batch_comparators: HashMap<usize, Vec<DynComparator>>,
}

impl<'a> std::fmt::Debug for SortKeyCursor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SortKeyCursor")
            .field("columns", &self.columns)
            .field("cur_row", &self.cur_row)
            .field("num_rows", &self.num_rows)
            .field("batch_idx", &self.batch_idx)
            .field("batch", &self.batch)
            .field("batch_comparators", &"<FUNC>")
            .finish()
    }
}

impl SortKeyCursor {
    fn new(
        batch_idx: usize,
        batch: RecordBatch,
        sort_key: &[Arc<dyn PhysicalExpr>],
    ) -> Result<Self> {
        let columns = sort_key
            .iter()
            .map(|expr| Ok(expr.evaluate(&batch)?.into_array(batch.num_rows())))
            .collect::<Result<_>>()?;
        Ok(Self {
            cur_row: 0,
            num_rows: batch.num_rows(),
            columns,
            batch,
            batch_idx,
            batch_comparators: HashMap::new(),
        })
    }

    fn is_finished(&self) -> bool {
        self.num_rows == self.cur_row
    }

    fn advance(&mut self) -> usize {
        assert!(!self.is_finished());
        let t = self.cur_row;
        self.cur_row += 1;
        t
    }

    /// Compares the sort key pointed to by this instance's row cursor with that of another
    fn compare(
        &mut self,
        other: &SortKeyCursor,
        options: &[SortOptions],
    ) -> Result<Ordering> {
        if self.columns.len() != other.columns.len() {
            return Err(DataFusionError::Internal(format!(
                "SortKeyCursors had inconsistent column counts: {} vs {}",
                self.columns.len(),
                other.columns.len()
            )));
        }

        if self.columns.len() != options.len() {
            return Err(DataFusionError::Internal(format!(
                "Incorrect number of SortOptions provided to SortKeyCursor::compare, expected {} got {}",
                self.columns.len(),
                options.len()
            )));
        }

        let zipped = self
            .columns
            .iter()
            .zip(other.columns.iter())
            .zip(options.iter());

        // Recall or initialise a collection of comparators for comparing
        // columnar arrays of this cursor and "other".
        let cmp = self
            .batch_comparators
            .entry(other.batch_idx)
            .or_insert_with(|| Vec::with_capacity(other.columns.len()));

        for (i, ((l, r), sort_options)) in zipped.enumerate() {
            if i >= cmp.len() {
                // initialise comparators as potentially needed
                cmp.push(arrow::array::build_compare(l.as_ref(), r.as_ref())?);
            }

            match (l.is_valid(self.cur_row), r.is_valid(other.cur_row)) {
                (false, true) if sort_options.nulls_first => return Ok(Ordering::Less),
                (false, true) => return Ok(Ordering::Greater),
                (true, false) if sort_options.nulls_first => {
                    return Ok(Ordering::Greater)
                }
                (true, false) => return Ok(Ordering::Less),
                (false, false) => {}
                (true, true) => match cmp[i](self.cur_row, other.cur_row) {
                    Ordering::Equal => {}
                    o if sort_options.descending => return Ok(o.reverse()),
                    o => return Ok(o),
                },
            }
        }

        Ok(Ordering::Equal)
    }
}

/// A `RowIndex` identifies a specific row from those buffered
/// by a `SortPreservingMergeStream`
#[derive(Debug, Clone)]
struct RowIndex {
    /// The index of the stream
    stream_idx: usize,
    /// The index of the cursor within the stream's VecDequeue
    cursor_idx: usize,
    /// The row index
    row_idx: usize,
}

#[derive(Debug)]
struct SortPreservingMergeStream {
    /// The schema of the RecordBatches yielded by this stream
    schema: SchemaRef,

    /// The sorted input streams to merge together
    receivers: Vec<mpsc::Receiver<ArrowResult<RecordBatch>>>,

    /// Drop helper for tasks feeding the [`receivers`](Self::receivers)
    _drop_helper: AbortOnDropMany<()>,

    /// For each input stream maintain a dequeue of SortKeyCursor
    ///
    /// Exhausted cursors will be popped off the front once all
    /// their rows have been yielded to the output
    cursors: Vec<VecDeque<SortKeyCursor>>,

    /// The accumulated row indexes for the next record batch
    in_progress: Vec<RowIndex>,

    /// The physical expressions to sort by
    column_expressions: Vec<Arc<dyn PhysicalExpr>>,

    /// The sort options for each expression
    sort_options: Vec<SortOptions>,

    /// The desired RecordBatch size to yield
    target_batch_size: usize,

    /// used to record execution metrics
    baseline_metrics: BaselineMetrics,

    /// If the stream has encountered an error
    aborted: bool,

    /// An index to uniquely identify the input stream batch
    next_batch_index: usize,
}

impl SortPreservingMergeStream {
    fn new(
        receivers: Vec<mpsc::Receiver<ArrowResult<RecordBatch>>>,
        _drop_helper: AbortOnDropMany<()>,
        schema: SchemaRef,
        expressions: &[PhysicalSortExpr],
        target_batch_size: usize,
        baseline_metrics: BaselineMetrics,
    ) -> Self {
        let cursors = (0..receivers.len())
            .into_iter()
            .map(|_| VecDeque::new())
            .collect();

        Self {
            schema,
            cursors,
            receivers,
            _drop_helper,
            column_expressions: expressions.iter().map(|x| x.expr.clone()).collect(),
            sort_options: expressions.iter().map(|x| x.options).collect(),
            target_batch_size,
            baseline_metrics,
            aborted: false,
            in_progress: vec![],
            next_batch_index: 0,
        }
    }

    /// If the stream at the given index is not exhausted, and the last cursor for the
    /// stream is finished, poll the stream for the next RecordBatch and create a new
    /// cursor for the stream from the returned result
    fn maybe_poll_stream(
        &mut self,
        cx: &mut Context<'_>,
        idx: usize,
    ) -> Poll<ArrowResult<()>> {
        if let Some(cursor) = &self.cursors[idx].back() {
            if !cursor.is_finished() {
                // Cursor is not finished - don't need a new RecordBatch yet
                return Poll::Ready(Ok(()));
            }
        }

        let stream = &mut self.receivers[idx];
        if stream.is_terminated() {
            return Poll::Ready(Ok(()));
        }

        // Fetch a new input record and create a cursor from it
        match futures::ready!(stream.poll_next_unpin(cx)) {
            None => return Poll::Ready(Ok(())),
            Some(Err(e)) => {
                return Poll::Ready(Err(e));
            }
            Some(Ok(batch)) => {
                let cursor = match SortKeyCursor::new(
                    self.next_batch_index, // assign this batch an ID
                    batch,
                    &self.column_expressions,
                ) {
                    Ok(cursor) => cursor,
                    Err(e) => {
                        return Poll::Ready(Err(ArrowError::ExternalError(Box::new(e))));
                    }
                };
                self.next_batch_index += 1;
                self.cursors[idx].push_back(cursor)
            }
        }

        Poll::Ready(Ok(()))
    }

    /// Returns the index of the next stream to pull a row from, or None
    /// if all cursors for all streams are exhausted
    fn next_stream_idx(&mut self) -> Result<Option<usize>> {
        let mut min_cursor: Option<(usize, &mut SortKeyCursor)> = None;
        for (idx, candidate) in self.cursors.iter_mut().enumerate() {
            if let Some(candidate) = candidate.back_mut() {
                if candidate.is_finished() {
                    continue;
                }

                match min_cursor {
                    None => min_cursor = Some((idx, candidate)),
                    Some((_, ref mut min)) => {
                        if min.compare(candidate, &self.sort_options)?
                            == Ordering::Greater
                        {
                            min_cursor = Some((idx, candidate))
                        }
                    }
                }
            }
        }

        Ok(min_cursor.map(|(idx, _)| idx))
    }

    /// Drains the in_progress row indexes, and builds a new RecordBatch from them
    ///
    /// Will then drop any cursors for which all rows have been yielded to the output
    fn build_record_batch(&mut self) -> ArrowResult<RecordBatch> {
        // Mapping from stream index to the index of the first buffer from that stream
        let mut buffer_idx = 0;
        let mut stream_to_buffer_idx = Vec::with_capacity(self.cursors.len());

        for cursors in &self.cursors {
            stream_to_buffer_idx.push(buffer_idx);
            buffer_idx += cursors.len();
        }

        let columns = self
            .schema
            .fields()
            .iter()
            .enumerate()
            .map(|(column_idx, field)| {
                let arrays = self
                    .cursors
                    .iter()
                    .flat_map(|cursor| {
                        cursor
                            .iter()
                            .map(|cursor| cursor.batch.column(column_idx).data())
                    })
                    .collect();

                let mut array_data = MutableArrayData::new(
                    arrays,
                    field.is_nullable(),
                    self.in_progress.len(),
                );

                if self.in_progress.is_empty() {
                    return make_arrow_array(array_data.freeze());
                }

                let first = &self.in_progress[0];
                let mut buffer_idx =
                    stream_to_buffer_idx[first.stream_idx] + first.cursor_idx;
                let mut start_row_idx = first.row_idx;
                let mut end_row_idx = start_row_idx + 1;

                for row_index in self.in_progress.iter().skip(1) {
                    let next_buffer_idx =
                        stream_to_buffer_idx[row_index.stream_idx] + row_index.cursor_idx;

                    if next_buffer_idx == buffer_idx && row_index.row_idx == end_row_idx {
                        // subsequent row in same batch
                        end_row_idx += 1;
                        continue;
                    }

                    // emit current batch of rows for current buffer
                    array_data.extend(buffer_idx, start_row_idx, end_row_idx);

                    // start new batch of rows
                    buffer_idx = next_buffer_idx;
                    start_row_idx = row_index.row_idx;
                    end_row_idx = start_row_idx + 1;
                }

                // emit final batch of rows
                array_data.extend(buffer_idx, start_row_idx, end_row_idx);
                make_arrow_array(array_data.freeze())
            })
            .collect();

        self.in_progress.clear();

        // New cursors are only created once the previous cursor for the stream
        // is finished. This means all remaining rows from all but the last cursor
        // for each stream have been yielded to the newly created record batch
        //
        // Additionally as `in_progress` has been drained, there are no longer
        // any RowIndex's reliant on the cursor indexes
        //
        // We can therefore drop all but the last cursor for each stream
        for cursors in &mut self.cursors {
            if cursors.len() > 1 {
                // Drain all but the last cursor
                cursors.drain(0..(cursors.len() - 1));
            }
        }

        RecordBatch::try_new(self.schema.clone(), columns)
    }
}

impl Stream for SortPreservingMergeStream {
    type Item = ArrowResult<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let poll = self.poll_next_inner(cx);
        self.baseline_metrics.record_poll(poll)
    }
}

impl SortPreservingMergeStream {
    #[inline]
    fn poll_next_inner(
        self: &mut Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<ArrowResult<RecordBatch>>> {
        if self.aborted {
            return Poll::Ready(None);
        }

        // Ensure all non-exhausted streams have a cursor from which
        // rows can be pulled
        for i in 0..self.cursors.len() {
            match futures::ready!(self.maybe_poll_stream(cx, i)) {
                Ok(_) => {}
                Err(e) => {
                    self.aborted = true;
                    return Poll::Ready(Some(Err(e)));
                }
            }
        }

        loop {
            // NB timer records time taken on drop, so there are no
            // calls to `timer.done()` below.
            let elapsed_compute = self.baseline_metrics.elapsed_compute().clone();
            let _timer = elapsed_compute.timer();

            let stream_idx = match self.next_stream_idx() {
                Ok(Some(idx)) => idx,
                Ok(None) if self.in_progress.is_empty() => return Poll::Ready(None),
                Ok(None) => return Poll::Ready(Some(self.build_record_batch())),
                Err(e) => {
                    self.aborted = true;
                    return Poll::Ready(Some(Err(ArrowError::ExternalError(Box::new(
                        e,
                    )))));
                }
            };

            let cursors = &mut self.cursors[stream_idx];
            let cursor_idx = cursors.len() - 1;
            let cursor = cursors.back_mut().unwrap();
            let row_idx = cursor.advance();
            let cursor_finished = cursor.is_finished();

            self.in_progress.push(RowIndex {
                stream_idx,
                cursor_idx,
                row_idx,
            });

            if self.in_progress.len() == self.target_batch_size {
                return Poll::Ready(Some(self.build_record_batch()));
            }

            // If removed the last row from the cursor, need to fetch a new record
            // batch if possible, before looping round again
            if cursor_finished {
                match futures::ready!(self.maybe_poll_stream(cx, stream_idx)) {
                    Ok(_) => {}
                    Err(e) => {
                        self.aborted = true;
                        return Poll::Ready(Some(Err(e)));
                    }
                }
            }
        }
    }
}

impl RecordBatchStream for SortPreservingMergeStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

#[cfg(test)]
mod tests {
    use crate::datasource::object_store::local::LocalFileSystem;
    use crate::physical_plan::metrics::MetricValue;
    use crate::test::exec::{assert_strong_count_converges_to_zero, BlockingExec};
    use std::iter::FromIterator;

    use crate::arrow::array::{Int32Array, StringArray, TimestampNanosecondArray};
    use crate::physical_plan::coalesce_partitions::CoalescePartitionsExec;
    use crate::physical_plan::expressions::col;
    use crate::physical_plan::file_format::{CsvExec, PhysicalPlanConfig};
    use crate::physical_plan::memory::MemoryExec;
    use crate::physical_plan::sort::SortExec;
    use crate::physical_plan::{collect, common};
    use crate::test::{self, assert_is_pending};
    use crate::{assert_batches_eq, test_util};

    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};
    use futures::{FutureExt, SinkExt};
    use tokio_stream::StreamExt;

    #[tokio::test]
    async fn test_merge_interleave() {
        let a: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 7, 9, 3]));
        let b: ArrayRef = Arc::new(StringArray::from_iter(vec![
            Some("a"),
            Some("c"),
            Some("e"),
            Some("g"),
            Some("j"),
        ]));
        let c: ArrayRef = Arc::new(TimestampNanosecondArray::from(vec![8, 7, 6, 5, 8]));
        let b1 = RecordBatch::try_from_iter(vec![("a", a), ("b", b), ("c", c)]).unwrap();

        let a: ArrayRef = Arc::new(Int32Array::from(vec![10, 20, 70, 90, 30]));
        let b: ArrayRef = Arc::new(StringArray::from_iter(vec![
            Some("b"),
            Some("d"),
            Some("f"),
            Some("h"),
            Some("j"),
        ]));
        let c: ArrayRef = Arc::new(TimestampNanosecondArray::from(vec![4, 6, 2, 2, 6]));
        let b2 = RecordBatch::try_from_iter(vec![("a", a), ("b", b), ("c", c)]).unwrap();

        _test_merge(
            &[vec![b1], vec![b2]],
            &[
                "+----+---+-------------------------------+",
                "| a  | b | c                             |",
                "+----+---+-------------------------------+",
                "| 1  | a | 1970-01-01 00:00:00.000000008 |",
                "| 10 | b | 1970-01-01 00:00:00.000000004 |",
                "| 2  | c | 1970-01-01 00:00:00.000000007 |",
                "| 20 | d | 1970-01-01 00:00:00.000000006 |",
                "| 7  | e | 1970-01-01 00:00:00.000000006 |",
                "| 70 | f | 1970-01-01 00:00:00.000000002 |",
                "| 9  | g | 1970-01-01 00:00:00.000000005 |",
                "| 90 | h | 1970-01-01 00:00:00.000000002 |",
                "| 30 | j | 1970-01-01 00:00:00.000000006 |", // input b2 before b1
                "| 3  | j | 1970-01-01 00:00:00.000000008 |",
                "+----+---+-------------------------------+",
            ],
        )
        .await;
    }

    #[tokio::test]
    async fn test_merge_some_overlap() {
        let a: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 7, 9, 3]));
        let b: ArrayRef = Arc::new(StringArray::from_iter(vec![
            Some("a"),
            Some("b"),
            Some("c"),
            Some("d"),
            Some("e"),
        ]));
        let c: ArrayRef = Arc::new(TimestampNanosecondArray::from(vec![8, 7, 6, 5, 8]));
        let b1 = RecordBatch::try_from_iter(vec![("a", a), ("b", b), ("c", c)]).unwrap();

        let a: ArrayRef = Arc::new(Int32Array::from(vec![70, 90, 30, 100, 110]));
        let b: ArrayRef = Arc::new(StringArray::from_iter(vec![
            Some("c"),
            Some("d"),
            Some("e"),
            Some("f"),
            Some("g"),
        ]));
        let c: ArrayRef = Arc::new(TimestampNanosecondArray::from(vec![4, 6, 2, 2, 6]));
        let b2 = RecordBatch::try_from_iter(vec![("a", a), ("b", b), ("c", c)]).unwrap();

        _test_merge(
            &[vec![b1], vec![b2]],
            &[
                "+-----+---+-------------------------------+",
                "| a   | b | c                             |",
                "+-----+---+-------------------------------+",
                "| 1   | a | 1970-01-01 00:00:00.000000008 |",
                "| 2   | b | 1970-01-01 00:00:00.000000007 |",
                "| 70  | c | 1970-01-01 00:00:00.000000004 |",
                "| 7   | c | 1970-01-01 00:00:00.000000006 |",
                "| 9   | d | 1970-01-01 00:00:00.000000005 |",
                "| 90  | d | 1970-01-01 00:00:00.000000006 |",
                "| 30  | e | 1970-01-01 00:00:00.000000002 |",
                "| 3   | e | 1970-01-01 00:00:00.000000008 |",
                "| 100 | f | 1970-01-01 00:00:00.000000002 |",
                "| 110 | g | 1970-01-01 00:00:00.000000006 |",
                "+-----+---+-------------------------------+",
            ],
        )
        .await;
    }

    #[tokio::test]
    async fn test_merge_no_overlap() {
        let a: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 7, 9, 3]));
        let b: ArrayRef = Arc::new(StringArray::from_iter(vec![
            Some("a"),
            Some("b"),
            Some("c"),
            Some("d"),
            Some("e"),
        ]));
        let c: ArrayRef = Arc::new(TimestampNanosecondArray::from(vec![8, 7, 6, 5, 8]));
        let b1 = RecordBatch::try_from_iter(vec![("a", a), ("b", b), ("c", c)]).unwrap();

        let a: ArrayRef = Arc::new(Int32Array::from(vec![10, 20, 70, 90, 30]));
        let b: ArrayRef = Arc::new(StringArray::from_iter(vec![
            Some("f"),
            Some("g"),
            Some("h"),
            Some("i"),
            Some("j"),
        ]));
        let c: ArrayRef = Arc::new(TimestampNanosecondArray::from(vec![4, 6, 2, 2, 6]));
        let b2 = RecordBatch::try_from_iter(vec![("a", a), ("b", b), ("c", c)]).unwrap();

        _test_merge(
            &[vec![b1], vec![b2]],
            &[
                "+----+---+-------------------------------+",
                "| a  | b | c                             |",
                "+----+---+-------------------------------+",
                "| 1  | a | 1970-01-01 00:00:00.000000008 |",
                "| 2  | b | 1970-01-01 00:00:00.000000007 |",
                "| 7  | c | 1970-01-01 00:00:00.000000006 |",
                "| 9  | d | 1970-01-01 00:00:00.000000005 |",
                "| 3  | e | 1970-01-01 00:00:00.000000008 |",
                "| 10 | f | 1970-01-01 00:00:00.000000004 |",
                "| 20 | g | 1970-01-01 00:00:00.000000006 |",
                "| 70 | h | 1970-01-01 00:00:00.000000002 |",
                "| 90 | i | 1970-01-01 00:00:00.000000002 |",
                "| 30 | j | 1970-01-01 00:00:00.000000006 |",
                "+----+---+-------------------------------+",
            ],
        )
        .await;
    }

    #[tokio::test]
    async fn test_merge_three_partitions() {
        let a: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 7, 9, 3]));
        let b: ArrayRef = Arc::new(StringArray::from_iter(vec![
            Some("a"),
            Some("b"),
            Some("c"),
            Some("d"),
            Some("f"),
        ]));
        let c: ArrayRef = Arc::new(TimestampNanosecondArray::from(vec![8, 7, 6, 5, 8]));
        let b1 = RecordBatch::try_from_iter(vec![("a", a), ("b", b), ("c", c)]).unwrap();

        let a: ArrayRef = Arc::new(Int32Array::from(vec![10, 20, 70, 90, 30]));
        let b: ArrayRef = Arc::new(StringArray::from_iter(vec![
            Some("e"),
            Some("g"),
            Some("h"),
            Some("i"),
            Some("j"),
        ]));
        let c: ArrayRef =
            Arc::new(TimestampNanosecondArray::from(vec![40, 60, 20, 20, 60]));
        let b2 = RecordBatch::try_from_iter(vec![("a", a), ("b", b), ("c", c)]).unwrap();

        let a: ArrayRef = Arc::new(Int32Array::from(vec![100, 200, 700, 900, 300]));
        let b: ArrayRef = Arc::new(StringArray::from_iter(vec![
            Some("f"),
            Some("g"),
            Some("h"),
            Some("i"),
            Some("j"),
        ]));
        let c: ArrayRef = Arc::new(TimestampNanosecondArray::from(vec![4, 6, 2, 2, 6]));
        let b3 = RecordBatch::try_from_iter(vec![("a", a), ("b", b), ("c", c)]).unwrap();

        _test_merge(
            &[vec![b1], vec![b2], vec![b3]],
            &[
                "+-----+---+-------------------------------+",
                "| a   | b | c                             |",
                "+-----+---+-------------------------------+",
                "| 1   | a | 1970-01-01 00:00:00.000000008 |",
                "| 2   | b | 1970-01-01 00:00:00.000000007 |",
                "| 7   | c | 1970-01-01 00:00:00.000000006 |",
                "| 9   | d | 1970-01-01 00:00:00.000000005 |",
                "| 10  | e | 1970-01-01 00:00:00.000000040 |",
                "| 100 | f | 1970-01-01 00:00:00.000000004 |",
                "| 3   | f | 1970-01-01 00:00:00.000000008 |",
                "| 200 | g | 1970-01-01 00:00:00.000000006 |",
                "| 20  | g | 1970-01-01 00:00:00.000000060 |",
                "| 700 | h | 1970-01-01 00:00:00.000000002 |",
                "| 70  | h | 1970-01-01 00:00:00.000000020 |",
                "| 900 | i | 1970-01-01 00:00:00.000000002 |",
                "| 90  | i | 1970-01-01 00:00:00.000000020 |",
                "| 300 | j | 1970-01-01 00:00:00.000000006 |",
                "| 30  | j | 1970-01-01 00:00:00.000000060 |",
                "+-----+---+-------------------------------+",
            ],
        )
        .await;
    }

    async fn _test_merge(partitions: &[Vec<RecordBatch>], exp: &[&str]) {
        let schema = partitions[0][0].schema();
        let sort = vec![
            PhysicalSortExpr {
                expr: col("b", &schema).unwrap(),
                options: Default::default(),
            },
            PhysicalSortExpr {
                expr: col("c", &schema).unwrap(),
                options: Default::default(),
            },
        ];
        let exec = MemoryExec::try_new(partitions, schema, None).unwrap();
        let merge = Arc::new(SortPreservingMergeExec::new(sort, Arc::new(exec), 1024));

        let collected = collect(merge).await.unwrap();
        assert_batches_eq!(exp, collected.as_slice());
    }

    async fn sorted_merge(
        input: Arc<dyn ExecutionPlan>,
        sort: Vec<PhysicalSortExpr>,
    ) -> RecordBatch {
        let merge = Arc::new(SortPreservingMergeExec::new(sort, input, 1024));
        let mut result = collect(merge).await.unwrap();
        assert_eq!(result.len(), 1);
        result.remove(0)
    }

    async fn partition_sort(
        input: Arc<dyn ExecutionPlan>,
        sort: Vec<PhysicalSortExpr>,
    ) -> RecordBatch {
        let sort_exec =
            Arc::new(SortExec::new_with_partitioning(sort.clone(), input, true));
        sorted_merge(sort_exec, sort).await
    }

    async fn basic_sort(
        src: Arc<dyn ExecutionPlan>,
        sort: Vec<PhysicalSortExpr>,
    ) -> RecordBatch {
        let merge = Arc::new(CoalescePartitionsExec::new(src));
        let sort_exec = Arc::new(SortExec::try_new(sort, merge).unwrap());
        let mut result = collect(sort_exec).await.unwrap();
        assert_eq!(result.len(), 1);
        result.remove(0)
    }

    #[tokio::test]
    async fn test_partition_sort() {
        let schema = test_util::aggr_test_schema();
        let partitions = 4;
        let (_, files) =
            test::create_partitioned_csv("aggregate_test_100.csv", partitions).unwrap();

        let csv = Arc::new(CsvExec::new(
            PhysicalPlanConfig {
                object_store: Arc::new(LocalFileSystem {}),
                file_schema: Arc::clone(&schema),
                file_groups: files,
                statistics: Statistics::default(),
                projection: None,
                batch_size: 1024,
                limit: None,
                table_partition_cols: vec![],
            },
            true,
            b',',
        ));

        let sort = vec![
            PhysicalSortExpr {
                expr: col("c1", &schema).unwrap(),
                options: SortOptions {
                    descending: true,
                    nulls_first: true,
                },
            },
            PhysicalSortExpr {
                expr: col("c2", &schema).unwrap(),
                options: Default::default(),
            },
            PhysicalSortExpr {
                expr: col("c7", &schema).unwrap(),
                options: SortOptions::default(),
            },
            PhysicalSortExpr {
                expr: col("c12", &schema).unwrap(),
                options: SortOptions::default(),
            },
        ];

        let basic = basic_sort(csv.clone(), sort.clone()).await;
        let partition = partition_sort(csv, sort).await;

        let basic = arrow::util::pretty::pretty_format_batches(&[basic]).unwrap();
        let partition = arrow::util::pretty::pretty_format_batches(&[partition]).unwrap();

        assert_eq!(
            basic, partition,
            "basic:\n\n{}\n\npartition:\n\n{}\n\n",
            basic, partition
        );
    }

    // Split the provided record batch into multiple batch_size record batches
    fn split_batch(sorted: &RecordBatch, batch_size: usize) -> Vec<RecordBatch> {
        let batches = (sorted.num_rows() + batch_size - 1) / batch_size;

        // Split the sorted RecordBatch into multiple
        (0..batches)
            .into_iter()
            .map(|batch_idx| {
                let columns = (0..sorted.num_columns())
                    .map(|column_idx| {
                        let length =
                            batch_size.min(sorted.num_rows() - batch_idx * batch_size);

                        sorted
                            .column(column_idx)
                            .slice(batch_idx * batch_size, length)
                    })
                    .collect();

                RecordBatch::try_new(sorted.schema(), columns).unwrap()
            })
            .collect()
    }

    async fn sorted_partitioned_input(
        sort: Vec<PhysicalSortExpr>,
        sizes: &[usize],
    ) -> Arc<dyn ExecutionPlan> {
        let schema = test_util::aggr_test_schema();
        let partitions = 4;
        let (_, files) =
            test::create_partitioned_csv("aggregate_test_100.csv", partitions).unwrap();

        let csv = Arc::new(CsvExec::new(
            PhysicalPlanConfig {
                object_store: Arc::new(LocalFileSystem {}),
                file_schema: schema,
                file_groups: files,
                statistics: Statistics::default(),
                projection: None,
                batch_size: 1024,
                limit: None,
                table_partition_cols: vec![],
            },
            true,
            b',',
        ));

        let sorted = basic_sort(csv, sort).await;
        let split: Vec<_> = sizes.iter().map(|x| split_batch(&sorted, *x)).collect();

        Arc::new(MemoryExec::try_new(&split, sorted.schema(), None).unwrap())
    }

    #[tokio::test]
    async fn test_partition_sort_streaming_input() {
        let schema = test_util::aggr_test_schema();
        let sort = vec![
            // uint8
            PhysicalSortExpr {
                expr: col("c7", &schema).unwrap(),
                options: Default::default(),
            },
            // int16
            PhysicalSortExpr {
                expr: col("c4", &schema).unwrap(),
                options: Default::default(),
            },
            // utf-8
            PhysicalSortExpr {
                expr: col("c1", &schema).unwrap(),
                options: SortOptions::default(),
            },
            // utf-8
            PhysicalSortExpr {
                expr: col("c13", &schema).unwrap(),
                options: SortOptions::default(),
            },
        ];

        let input = sorted_partitioned_input(sort.clone(), &[10, 3, 11]).await;
        let basic = basic_sort(input.clone(), sort.clone()).await;
        let partition = sorted_merge(input, sort).await;

        assert_eq!(basic.num_rows(), 300);
        assert_eq!(partition.num_rows(), 300);

        let basic = arrow::util::pretty::pretty_format_batches(&[basic]).unwrap();
        let partition = arrow::util::pretty::pretty_format_batches(&[partition]).unwrap();

        assert_eq!(basic, partition);
    }

    #[tokio::test]
    async fn test_partition_sort_streaming_input_output() {
        let schema = test_util::aggr_test_schema();

        let sort = vec![
            // float64
            PhysicalSortExpr {
                expr: col("c12", &schema).unwrap(),
                options: Default::default(),
            },
            // utf-8
            PhysicalSortExpr {
                expr: col("c13", &schema).unwrap(),
                options: Default::default(),
            },
        ];

        let input = sorted_partitioned_input(sort.clone(), &[10, 5, 13]).await;
        let basic = basic_sort(input.clone(), sort.clone()).await;

        let merge = Arc::new(SortPreservingMergeExec::new(sort, input, 23));
        let merged = collect(merge).await.unwrap();

        assert_eq!(merged.len(), 14);

        assert_eq!(basic.num_rows(), 300);
        assert_eq!(merged.iter().map(|x| x.num_rows()).sum::<usize>(), 300);

        let basic = arrow::util::pretty::pretty_format_batches(&[basic]).unwrap();
        let partition =
            arrow::util::pretty::pretty_format_batches(merged.as_slice()).unwrap();

        assert_eq!(basic, partition);
    }

    #[tokio::test]
    async fn test_nulls() {
        let a: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 7, 9, 3]));
        let b: ArrayRef = Arc::new(StringArray::from_iter(vec![
            None,
            Some("a"),
            Some("b"),
            Some("d"),
            Some("e"),
        ]));
        let c: ArrayRef = Arc::new(TimestampNanosecondArray::from(vec![
            Some(8),
            None,
            Some(6),
            None,
            Some(4),
        ]));
        let b1 = RecordBatch::try_from_iter(vec![("a", a), ("b", b), ("c", c)]).unwrap();

        let a: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5]));
        let b: ArrayRef = Arc::new(StringArray::from_iter(vec![
            None,
            Some("b"),
            Some("g"),
            Some("h"),
            Some("i"),
        ]));
        let c: ArrayRef = Arc::new(TimestampNanosecondArray::from(vec![
            Some(8),
            None,
            Some(5),
            None,
            Some(4),
        ]));
        let b2 = RecordBatch::try_from_iter(vec![("a", a), ("b", b), ("c", c)]).unwrap();
        let schema = b1.schema();

        let sort = vec![
            PhysicalSortExpr {
                expr: col("b", &schema).unwrap(),
                options: SortOptions {
                    descending: false,
                    nulls_first: true,
                },
            },
            PhysicalSortExpr {
                expr: col("c", &schema).unwrap(),
                options: SortOptions {
                    descending: false,
                    nulls_first: false,
                },
            },
        ];
        let exec = MemoryExec::try_new(&[vec![b1], vec![b2]], schema, None).unwrap();
        let merge = Arc::new(SortPreservingMergeExec::new(sort, Arc::new(exec), 1024));

        let collected = collect(merge).await.unwrap();
        assert_eq!(collected.len(), 1);

        assert_batches_eq!(
            &[
                "+---+---+-------------------------------+",
                "| a | b | c                             |",
                "+---+---+-------------------------------+",
                "| 1 |   | 1970-01-01 00:00:00.000000008 |",
                "| 1 |   | 1970-01-01 00:00:00.000000008 |",
                "| 2 | a |                               |",
                "| 7 | b | 1970-01-01 00:00:00.000000006 |",
                "| 2 | b |                               |",
                "| 9 | d |                               |",
                "| 3 | e | 1970-01-01 00:00:00.000000004 |",
                "| 3 | g | 1970-01-01 00:00:00.000000005 |",
                "| 4 | h |                               |",
                "| 5 | i | 1970-01-01 00:00:00.000000004 |",
                "+---+---+-------------------------------+",
            ],
            collected.as_slice()
        );
    }

    #[tokio::test]
    async fn test_async() {
        let schema = test_util::aggr_test_schema();
        let sort = vec![PhysicalSortExpr {
            expr: col("c12", &schema).unwrap(),
            options: SortOptions::default(),
        }];

        let batches = sorted_partitioned_input(sort.clone(), &[5, 7, 3]).await;

        let partition_count = batches.output_partitioning().partition_count();
        let mut join_handles = Vec::with_capacity(partition_count);
        let mut receivers = Vec::with_capacity(partition_count);

        for partition in 0..partition_count {
            let (mut sender, receiver) = mpsc::channel(1);
            let mut stream = batches.execute(partition).await.unwrap();
            let join_handle = tokio::spawn(async move {
                while let Some(batch) = stream.next().await {
                    sender.send(batch).await.unwrap();
                    // This causes the MergeStream to wait for more input
                    tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
                }
            });
            join_handles.push(join_handle);
            receivers.push(receiver);
        }

        let metrics = ExecutionPlanMetricsSet::new();
        let baseline_metrics = BaselineMetrics::new(&metrics, 0);

        let merge_stream = SortPreservingMergeStream::new(
            receivers,
            // Use empty vector since we want to use the join handles ourselves
            AbortOnDropMany(vec![]),
            batches.schema(),
            sort.as_slice(),
            1024,
            baseline_metrics,
        );

        let mut merged = common::collect(Box::pin(merge_stream)).await.unwrap();

        // Propagate any errors
        for join_handle in join_handles {
            join_handle.await.unwrap();
        }

        assert_eq!(merged.len(), 1);
        let merged = merged.remove(0);
        let basic = basic_sort(batches, sort.clone()).await;

        let basic = arrow::util::pretty::pretty_format_batches(&[basic]).unwrap();
        let partition = arrow::util::pretty::pretty_format_batches(&[merged]).unwrap();

        assert_eq!(
            basic, partition,
            "basic:\n\n{}\n\npartition:\n\n{}\n\n",
            basic, partition
        );
    }

    #[tokio::test]
    async fn test_merge_metrics() {
        let a: ArrayRef = Arc::new(Int32Array::from(vec![1, 2]));
        let b: ArrayRef = Arc::new(StringArray::from_iter(vec![Some("a"), Some("c")]));
        let b1 = RecordBatch::try_from_iter(vec![("a", a), ("b", b)]).unwrap();

        let a: ArrayRef = Arc::new(Int32Array::from(vec![10, 20]));
        let b: ArrayRef = Arc::new(StringArray::from_iter(vec![Some("b"), Some("d")]));
        let b2 = RecordBatch::try_from_iter(vec![("a", a), ("b", b)]).unwrap();

        let schema = b1.schema();
        let sort = vec![PhysicalSortExpr {
            expr: col("b", &schema).unwrap(),
            options: Default::default(),
        }];
        let exec = MemoryExec::try_new(&[vec![b1], vec![b2]], schema, None).unwrap();
        let merge = Arc::new(SortPreservingMergeExec::new(sort, Arc::new(exec), 1024));

        let collected = collect(merge.clone()).await.unwrap();
        let expected = vec![
            "+----+---+",
            "| a  | b |",
            "+----+---+",
            "| 1  | a |",
            "| 10 | b |",
            "| 2  | c |",
            "| 20 | d |",
            "+----+---+",
        ];
        assert_batches_eq!(expected, collected.as_slice());

        // Now, validate metrics
        let metrics = merge.metrics().unwrap();

        assert_eq!(metrics.output_rows().unwrap(), 4);
        assert!(metrics.elapsed_compute().unwrap() > 0);

        let mut saw_start = false;
        let mut saw_end = false;
        metrics.iter().for_each(|m| match m.value() {
            MetricValue::StartTimestamp(ts) => {
                saw_start = true;
                assert!(ts.value().unwrap().timestamp_nanos() > 0);
            }
            MetricValue::EndTimestamp(ts) => {
                saw_end = true;
                assert!(ts.value().unwrap().timestamp_nanos() > 0);
            }
            _ => {}
        });

        assert!(saw_start);
        assert!(saw_end);
    }

    #[tokio::test]
    async fn test_drop_cancel() -> Result<()> {
        let schema =
            Arc::new(Schema::new(vec![Field::new("a", DataType::Float32, true)]));

        let blocking_exec = Arc::new(BlockingExec::new(Arc::clone(&schema), 2));
        let refs = blocking_exec.refs();
        let sort_preserving_merge_exec = Arc::new(SortPreservingMergeExec::new(
            vec![PhysicalSortExpr {
                expr: col("a", &schema)?,
                options: SortOptions::default(),
            }],
            blocking_exec,
            1,
        ));

        let fut = collect(sort_preserving_merge_exec);
        let mut fut = fut.boxed();

        assert_is_pending(&mut fut);
        drop(fut);
        assert_strong_count_converges_to_zero(refs).await;

        Ok(())
    }
}
