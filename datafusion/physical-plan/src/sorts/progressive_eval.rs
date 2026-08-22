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

//! Defines the progressive eval plan

use std::borrow::Cow::Borrowed;
use std::collections::VecDeque;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use crate::common::spawn_buffered;
use crate::execution_plan::{Boundedness, EmissionType};
use crate::metrics::{
    BaselineMetrics, Count, ExecutionPlanMetricsSet, Metric, MetricBuilder, MetricValue,
    MetricsSet,
};
use crate::statistics::{ChildStats, StatisticsArgs};
use crate::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
};
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion_common::tree_node::TreeNodeRecursion;
use datafusion_common::{Result, Statistics, internal_err};
use datafusion_execution::{RecordBatchStream, SendableRecordBatchStream, TaskContext};
use datafusion_physical_expr::{Distribution, OrderingRequirements, Partitioning};
use datafusion_physical_expr_common::physical_expr::PhysicalExpr;
use futures::{Stream, StreamExt, ready};
use log::{debug, trace};

/// ProgressiveEval returns a stream of record batches in the order of its inputs.
/// It will stop when the number of output rows reaches the given limit.
///
/// This takes an input execution plan and an optional limit N, and provided each partition of
/// the input plan is in the expected order, this operator will return the top N rows
/// in the order of the input plan (truncating the record batch that crosses the limit).
///
/// ```text
/// ┌─────────────────────────┐
/// │ ┌───┬───┬───┬───┐       │
/// │ │ A │ B │ C │ D │       │──┐
/// │ └───┴───┴───┴───┘       │  │
/// └─────────────────────────┘  │  ┌───────────────────┐    ┌───────────────────────────────┐
///   Stream 1                   │  │                   │    │ ┌───┬───╦═══╦───┬───╦═══╗     │
///                              ├─▶│  ProgressiveEval  │───▶│ │ A │ B ║ C ║ D │ M ║ N ║ ... │
///                              │  │                   │    │ └───┴─▲─╩═══╩───┴───╩═══╝     │
/// ┌─────────────────────────┐  │  └───────────────────┘    └─┬─────┴───────────────────────┘
/// │ ╔═══╦═══╗               │  │
/// │ ║ M ║ N ║               │──┘                             │
/// │ ╚═══╩═══╝               │        Output only includes the top record batches that cover top N rows
/// └─────────────────────────┘
///   Stream 2
///
///
///  Input Streams                                             Output stream
///  (in some order)                                           (in same order)
/// ```
#[derive(Debug, Clone)]
pub struct ProgressiveEvalExec {
    /// Input plan
    input: Arc<dyn ExecutionPlan>,

    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,

    /// Optional number of rows to fetch. Stops producing rows after this fetch
    fetch: Option<usize>,

    /// Cache holding plan properties like equivalences, output partitioning, output ordering etc.
    cache: Arc<PlanProperties>,
}

impl ProgressiveEvalExec {
    /// Create a new progressive-evaluation execution plan.
    ///
    // Requires that the input partitions are in order with respect to the input ordering,
    // and non-overlapping.
    pub fn new(input: Arc<dyn ExecutionPlan>, fetch: Option<usize>) -> Self {
        let cache = Arc::new(Self::compute_properties(&input, fetch));
        Self {
            input,
            metrics: ExecutionPlanMetricsSet::new(),
            fetch,
            cache,
        }
    }

    /// Input plan
    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }

    /// Creates the cache object that stores the plan properties such as equivalence properties, partitioning, ordering, etc.
    fn compute_properties(
        input: &Arc<dyn ExecutionPlan>,
        fetch: Option<usize>,
    ) -> PlanProperties {
        // Progressive eval does not change the equivalence properties of its input.
        // This assumes that if the input is ordered, then the input partitions are non-overlapping
        // with respect to the ordering and in-order.
        let eq_properties = input.equivalence_properties().clone();

        // This node serializes all the data to a single partition
        let output_partitioning = Partitioning::UnknownPartitioning(1);

        // A fetch limit makes the output finite even if the input is unbounded
        let boundedness = if fetch.is_some() {
            Boundedness::Bounded
        } else {
            input.boundedness()
        };

        PlanProperties::new(
            eq_properties,
            output_partitioning,
            EmissionType::Incremental,
            boundedness,
        )
    }
}

impl DisplayAs for ProgressiveEvalExec {
    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "ProgressiveEvalExec: ")?;
                if let Some(fetch) = self.fetch {
                    write!(f, "fetch={fetch}, ")?;
                };
            }
            DisplayFormatType::TreeRender => {
                writeln!(f, "ProgressiveEvalExec")?;
                if let Some(fetch) = self.fetch {
                    writeln!(f, "fetch={fetch}")?;
                };
            }
        }
        Ok(())
    }
}

impl ExecutionPlan for ProgressiveEvalExec {
    fn name(&self) -> &'static str {
        "ProgressiveEvalExec"
    }

    fn schema(&self) -> SchemaRef {
        self.input.schema()
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.cache
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![Distribution::UnspecifiedDistribution]
    }

    fn required_input_ordering(&self) -> Vec<Option<OrderingRequirements>> {
        let input_ordering = self
            .input()
            .properties()
            .output_ordering()
            .cloned()
            .map(OrderingRequirements::from);

        vec![input_ordering]
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false]
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
        if children.len() != 1 {
            return internal_err!(
                "ProgressiveEvalExec expected 1 child, got {}",
                children.len()
            );
        }
        Ok(Arc::new(Self::new(
            Arc::<dyn ExecutionPlan>::clone(&children[0]),
            self.fetch,
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        trace!("Start ProgressiveEvalExec::execute for partition: {partition}");
        if 0 != partition {
            return internal_err!("ProgressiveEvalExec invalid partition {partition}");
        }

        let input_partitions = self
            .input
            .properties()
            .output_partitioning()
            .partition_count();
        trace!(
            "Number of input partitions of ProgressiveEvalExec::execute: {input_partitions}"
        );
        let schema = self.schema();

        // Add a metric to record the number of inputs
        let num_inputs = Count::new();
        num_inputs.add(input_partitions);
        self.metrics.register(Arc::new(Metric::new(
            MetricValue::Count {
                name: Borrowed("num_inputs"),
                count: num_inputs,
            },
            None,
        )));
        // Add a metric to record the number of inputs that are actually read which is <= num_inputs
        let num_read_inputs_counter =
            MetricBuilder::new(&self.metrics).global_counter("num_read_inputs");
        // Add other baseline metrics
        let baseline_metrics = BaselineMetrics::new(&self.metrics, partition);

        let result = ProgressiveEvalStream::new(
            Arc::clone(&self.input),
            Arc::clone(&context),
            schema,
            baseline_metrics,
            num_read_inputs_counter,
            self.fetch,
        )?;

        debug!("Got stream result from ProgressiveEvalStream::new_from_receivers");

        Ok(Box::pin(result))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn statistics_from_inputs(
        &self,
        input_stats: &[Arc<Statistics>],
        _args: &StatisticsArgs,
    ) -> Result<Arc<Statistics>> {
        // The single output partition carries the input's combined statistics,
        // capped by the fetch limit if one is set.
        let stats = input_stats[0].as_ref().clone();
        Ok(Arc::new(stats.with_fetch(self.fetch, 0, 1)?))
    }

    fn child_stats_requests(&self, _partition: Option<usize>) -> Vec<ChildStats> {
        vec![ChildStats::At(None)]
    }

    fn with_fetch(&self, limit: Option<usize>) -> Option<Arc<dyn ExecutionPlan>> {
        // Rebuild rather than clone so the cached plan properties reflect the new fetch
        Some(Arc::new(Self::new(
            Arc::<dyn ExecutionPlan>::clone(&self.input),
            limit,
        )))
    }

    fn fetch(&self) -> Option<usize> {
        self.fetch
    }
}

/// Handle when to prefetch input streams and how to poll next record batch
struct InputStreams {
    /// Input plan of the progressive eval exec
    input_plan: Arc<dyn ExecutionPlan>,

    /// Context of the progressive eval exec
    context: Arc<TaskContext>,

    /// Total input streams
    input_stream_count: usize,

    /// Number of input streams to prefetch ahead of time
    num_input_streams_to_prefetch: usize,

    /// Index of current stream
    current_stream_idx: usize,

    /// Input stream to poll data
    current_input_stream: Option<SendableRecordBatchStream>,

    /// Prefetched Input streams
    prefetched_input_streams: VecDeque<SendableRecordBatchStream>,

    /// Used to record number of actually read input streams
    num_read_inputs_counter: Count,
}

impl InputStreams {
    fn new(
        input_plan: Arc<dyn ExecutionPlan>,
        context: Arc<TaskContext>,
        num_input_streams_to_prefetch: usize,
        num_read_inputs_counter: Count,
    ) -> Result<Self> {
        let input_stream_count = input_plan
            .properties()
            .output_partitioning()
            .partition_count();

        let current_stream_idx = 0;
        let mut current_input_stream = None;
        // The capacity required for prefetched streams is 1 more than the number of streams to
        // prefetch, because we push a new stream before popping the new current stream. It is
        // also bounded by the total number of inputs, excluding the current stream.
        let prefetch_capacity = num_input_streams_to_prefetch
            .saturating_add(1)
            .min(input_stream_count.saturating_sub(1));
        let mut prefetched_input_streams = VecDeque::with_capacity(prefetch_capacity);

        // Always start fetching the first input stream, and also start
        // fetching an additional `num_input_streams_to_prefetch` inputs.
        for i in 0..=num_input_streams_to_prefetch {
            if i >= input_stream_count {
                break;
            }

            let input_stream = spawn_buffered(
                input_plan.execute(i, Arc::<TaskContext>::clone(&context))?,
                1,
            );
            num_read_inputs_counter.add(1);

            if i == 0 {
                current_input_stream = Some(input_stream);
            } else {
                prefetched_input_streams.push_back(input_stream);
            }
        }

        Ok(Self {
            input_plan,
            context,
            input_stream_count,
            num_input_streams_to_prefetch,
            current_stream_idx,
            current_input_stream,
            prefetched_input_streams,
            num_read_inputs_counter,
        })
    }

    /// Set next available stream to current_input_stream
    /// Also prefetch one more input stream if not all of them are prefetched yet
    fn next_stream(&mut self) -> Result<()> {
        // No more input stream
        if self.current_stream_idx >= self.input_stream_count {
            // all input streams must have been consumed already
            if !self.prefetched_input_streams.is_empty() {
                return internal_err!(
                    "Internal error in ProgressiveEvalStream: Expected no input streams left to read"
                );
            }

            self.current_input_stream = None;
        } else {
            // prefetch one more input stream before setting next stream to the current input stream
            let next_prefetch_idx = self
                .current_stream_idx
                .saturating_add(self.num_input_streams_to_prefetch)
                .saturating_add(1);
            if next_prefetch_idx < self.input_stream_count {
                self.num_read_inputs_counter.add(1);
                self.prefetched_input_streams.push_back(spawn_buffered(
                    self.input_plan.execute(
                        next_prefetch_idx,
                        Arc::<TaskContext>::clone(&self.context),
                    )?,
                    1,
                ));
            }

            self.current_stream_idx += 1;
            self.current_input_stream = self.prefetched_input_streams.pop_front();
        }
        Ok(())
    }

    fn poll_next(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<RecordBatch>>> {
        // All input streams have been read
        if self.current_input_stream.is_none() {
            return Poll::Ready(None);
        }

        // Get next record batch
        let mut poll;
        loop {
            poll = self
                .current_input_stream
                .as_mut()
                .unwrap()
                .poll_next_unpin(cx);
            match poll {
                // This input stream no longer has data, move to next stream
                Poll::Ready(None) => {
                    if let Err(e) = self.next_stream() {
                        return Poll::Ready(Some(Err(e)));
                    }
                    if self.current_input_stream.is_none() {
                        // Have reached the end of all input streams
                        return Poll::Ready(None);
                    }
                }
                _ => break,
            }
        }

        poll
    }
}

/// Concat input streams until reaching the fetch limit
struct ProgressiveEvalStream {
    /// Input streams
    input_streams: InputStreams,

    /// The schema of the input and output.
    schema: SchemaRef,

    /// used to record execution baseline metrics
    baseline_metrics: BaselineMetrics,

    /// If the stream has encountered an error
    aborted: bool,

    /// Optional number of rows to fetch
    fetch: Option<usize>,

    /// number of rows produced
    produced: usize,
}

impl ProgressiveEvalStream {
    fn new(
        input_plan: Arc<dyn ExecutionPlan>,
        context: Arc<TaskContext>,
        schema: SchemaRef,
        baseline_metrics: BaselineMetrics,
        num_read_inputs_counter: Count,
        fetch: Option<usize>,
    ) -> Result<Self> {
        let num_input_streams_to_prefetch = context
            .session_config()
            .options()
            .execution
            .progressive_eval_num_prefetch_input_streams;
        let input_streams = InputStreams::new(
            input_plan,
            context,
            num_input_streams_to_prefetch,
            num_read_inputs_counter,
        )?;

        Ok(Self {
            input_streams,
            schema,
            baseline_metrics,
            aborted: false,
            fetch,
            produced: 0,
        })
    }
}

impl Stream for ProgressiveEvalStream {
    type Item = Result<RecordBatch>;

    // Return the next record batch until reaching the fetch limit or the end of all input streams
    // Return pending if the next record batch is not ready
    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        // Error in previous poll
        if self.aborted {
            return Poll::Ready(None);
        }

        // Have reached the fetch limit
        if self.produced >= self.fetch.unwrap_or(usize::MAX) {
            return Poll::Ready(None);
        }

        let poll = self.input_streams.poll_next(cx);

        let poll = match ready!(poll) {
            // This input stream has data, return its next record batch,
            // truncated to the remaining fetch budget
            Some(Ok(batch)) => {
                let remaining = self.fetch.unwrap_or(usize::MAX) - self.produced;
                let batch = if batch.num_rows() > remaining {
                    batch.slice(0, remaining)
                } else {
                    batch
                };
                self.produced += batch.num_rows();
                Poll::Ready(Some(Ok(batch)))
            }
            // This input stream has an error, return the error and set aborted to true to stop polling next round
            Some(Err(e)) => {
                self.aborted = true;
                Poll::Ready(Some(Err(e)))
            }
            // This input stream has no more data, return None (aka finished)
            None => {
                // Reaching here means data of all streams have read
                Poll::Ready(None)
            }
        };

        self.baseline_metrics.record_poll(poll)
    }
}

impl RecordBatchStream for ProgressiveEvalStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::collect;
    use crate::metrics::Timestamp;
    use crate::statistics::StatisticsContext;
    use crate::stream::RecordBatchStreamAdapter;
    use crate::streaming::{PartitionStream, StreamingTableExec};
    use crate::test::exec::{BlockingExec, assert_strong_count_converges_to_zero};
    use crate::test::{TestMemoryExec, TestPartitionStream};
    use arrow::array::ArrayRef;
    use arrow::array::{Int32Array, StringArray, TimestampNanosecondArray};
    use arrow::datatypes::Schema;
    use arrow::datatypes::{DataType, Field};
    use arrow::record_batch::RecordBatch;
    use datafusion_common::DataFusionError;
    use datafusion_common::assert_batches_eq;
    use datafusion_common::stats::Precision;
    use datafusion_execution::config::SessionConfig;
    use futures::FutureExt;
    use std::iter::FromIterator;

    #[tokio::test]
    async fn test_no_input_stream() {
        let task_ctx = Arc::new(TaskContext::default());

        let empty_table_result = ["++", "++"];

        // no fetch limit --> return all rows
        run_progressive_eval_test(
            &[],
            None,
            &empty_table_result,
            0, // 0 input streams
            0, // 0 input streams are fetched and polled
            Arc::clone(&task_ctx),
        )
        .await;

        // limit = 0 means select nothing
        run_progressive_eval_test(
            &[],
            Some(0),
            &empty_table_result,
            0, // 0 input streams
            0, // 0 input streams are fetched and polled
            Arc::clone(&task_ctx),
        )
        .await;

        // limit = 1 on no data
        run_progressive_eval_test(
            &[],
            Some(1),
            &empty_table_result,
            0, // 0 input streams
            0, // 0 input streams are fetched and polled
            task_ctx,
        )
        .await;
    }

    #[tokio::test]
    async fn test_one_input_stream() {
        let task_ctx = Arc::new(TaskContext::default());
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

        let all_rows = [
            "+---+---+-------------------------------+",
            "| a | b | c                             |",
            "+---+---+-------------------------------+",
            "| 1 | a | 1970-01-01T00:00:00.000000008 |",
            "| 2 | c | 1970-01-01T00:00:00.000000007 |",
            "| 7 | e | 1970-01-01T00:00:00.000000006 |",
            "| 9 | g | 1970-01-01T00:00:00.000000005 |",
            "| 3 | j | 1970-01-01T00:00:00.000000008 |",
            "+---+---+-------------------------------+",
        ];

        // return all
        run_progressive_eval_test(
            &[vec![b1.clone()]],
            None, // no fetch limit --> return all rows
            &all_rows,
            1, // 1 input stream
            1, // 1 input stream is fetched and polled
            Arc::clone(&task_ctx),
        )
        .await;

        // fetch no rows
        run_progressive_eval_test(
            &[vec![b1.clone()]],
            Some(0),
            &["++", "++"],
            1,
            1,
            Arc::clone(&task_ctx),
        )
        .await;

        // return exactly 3 rows: the first record batch is truncated at the limit
        run_progressive_eval_test(
            &[vec![b1.clone()]],
            Some(3),
            &[
                "+---+---+-------------------------------+",
                "| a | b | c                             |",
                "+---+---+-------------------------------+",
                "| 1 | a | 1970-01-01T00:00:00.000000008 |",
                "| 2 | c | 1970-01-01T00:00:00.000000007 |",
                "| 7 | e | 1970-01-01T00:00:00.000000006 |",
                "+---+---+-------------------------------+",
            ],
            1, // 1 input stream
            1, // 1 input stream is fetched and polled
            Arc::clone(&task_ctx),
        )
        .await;

        // return all because fetch limit is larger
        run_progressive_eval_test(
            &[vec![b1.clone()]],
            Some(7),
            &all_rows,
            1, // 1 input stream
            1, // 1 input stream is fetched and polled
            Arc::clone(&task_ctx),
        )
        .await;
    }

    #[tokio::test]
    async fn test_return_all() {
        let task_ctx = Arc::new(TaskContext::default());
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

        let b1_b2 = [
            "+----+---+-------------------------------+",
            "| a  | b | c                             |",
            "+----+---+-------------------------------+",
            "| 1  | a | 1970-01-01T00:00:00.000000008 |",
            "| 2  | c | 1970-01-01T00:00:00.000000007 |",
            "| 7  | e | 1970-01-01T00:00:00.000000006 |",
            "| 9  | g | 1970-01-01T00:00:00.000000005 |",
            "| 3  | j | 1970-01-01T00:00:00.000000008 |",
            "| 10 | b | 1970-01-01T00:00:00.000000004 |",
            "| 20 | d | 1970-01-01T00:00:00.000000006 |",
            "| 70 | f | 1970-01-01T00:00:00.000000002 |",
            "| 90 | h | 1970-01-01T00:00:00.000000002 |",
            "| 30 | j | 1970-01-01T00:00:00.000000006 |",
            "+----+---+-------------------------------+",
        ];

        let b2_b1 = [
            "+----+---+-------------------------------+",
            "| a  | b | c                             |",
            "+----+---+-------------------------------+",
            "| 10 | b | 1970-01-01T00:00:00.000000004 |",
            "| 20 | d | 1970-01-01T00:00:00.000000006 |",
            "| 70 | f | 1970-01-01T00:00:00.000000002 |",
            "| 90 | h | 1970-01-01T00:00:00.000000002 |",
            "| 30 | j | 1970-01-01T00:00:00.000000006 |",
            "| 1  | a | 1970-01-01T00:00:00.000000008 |",
            "| 2  | c | 1970-01-01T00:00:00.000000007 |",
            "| 7  | e | 1970-01-01T00:00:00.000000006 |",
            "| 9  | g | 1970-01-01T00:00:00.000000005 |",
            "| 3  | j | 1970-01-01T00:00:00.000000008 |",
            "+----+---+-------------------------------+",
        ];

        // [b1, b2]
        // return all by not specifying fetch limit
        run_progressive_eval_test(
            &[vec![b1.clone()], vec![b2.clone()]],
            None, // no fetch limit --> return all rows
            &b1_b2,
            2, // 2 input streams
            2, // all 2 input streams are fetched and polled
            Arc::clone(&task_ctx),
        )
        .await;

        // [b1, b2]
        // return all by specifying large limit
        run_progressive_eval_test(
            &[vec![b1.clone()], vec![b2.clone()]],
            Some(10), // limit = max num rows --> return all rows
            &b1_b2,
            2, // 2 input streams
            2, // all 2 input streams are fetched and polled
            Arc::clone(&task_ctx),
        )
        .await;

        // [b2, b1]
        // return all by not specifying fetch limit
        run_progressive_eval_test(
            &[vec![b2.clone()], vec![b1.clone()]],
            None,
            &b2_b1,
            2, // 2 input streams
            2, // all 2 input streams are fetched and polled
            Arc::clone(&task_ctx),
        )
        .await;

        // [b2, b1]
        // return all by specifying large limit
        run_progressive_eval_test(
            &[vec![b2], vec![b1]],
            Some(20),
            &b2_b1,
            2, // 2 input streams
            2, // all 2 input streams are fetched and polled
            task_ctx,
        )
        .await;
    }

    #[tokio::test]
    async fn test_return_all_on_different_length_batches() {
        let task_ctx = Arc::new(TaskContext::default());
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

        let a: ArrayRef = Arc::new(Int32Array::from(vec![70, 90, 30]));
        let b: ArrayRef = Arc::new(StringArray::from_iter(vec![
            Some("c"),
            Some("d"),
            Some("e"),
        ]));
        let c: ArrayRef = Arc::new(TimestampNanosecondArray::from(vec![4, 6, 2]));
        let b2 = RecordBatch::try_from_iter(vec![("a", a), ("b", b), ("c", c)]).unwrap();

        // [b1, b2]
        run_progressive_eval_test(
            &[vec![b1.clone()], vec![b2.clone()]],
            None,
            &[
                "+----+---+-------------------------------+",
                "| a  | b | c                             |",
                "+----+---+-------------------------------+",
                "| 1  | a | 1970-01-01T00:00:00.000000008 |",
                "| 2  | b | 1970-01-01T00:00:00.000000007 |",
                "| 7  | c | 1970-01-01T00:00:00.000000006 |",
                "| 9  | d | 1970-01-01T00:00:00.000000005 |",
                "| 3  | e | 1970-01-01T00:00:00.000000008 |",
                "| 70 | c | 1970-01-01T00:00:00.000000004 |",
                "| 90 | d | 1970-01-01T00:00:00.000000006 |",
                "| 30 | e | 1970-01-01T00:00:00.000000002 |",
                "+----+---+-------------------------------+",
            ],
            2, // 2 input streams
            2, // all 2 input streams are fetched and polled
            Arc::clone(&task_ctx),
        )
        .await;

        // [b2, b1]
        run_progressive_eval_test(
            &[vec![b2], vec![b1]],
            None,
            &[
                "+----+---+-------------------------------+",
                "| a  | b | c                             |",
                "+----+---+-------------------------------+",
                "| 70 | c | 1970-01-01T00:00:00.000000004 |",
                "| 90 | d | 1970-01-01T00:00:00.000000006 |",
                "| 30 | e | 1970-01-01T00:00:00.000000002 |",
                "| 1  | a | 1970-01-01T00:00:00.000000008 |",
                "| 2  | b | 1970-01-01T00:00:00.000000007 |",
                "| 7  | c | 1970-01-01T00:00:00.000000006 |",
                "| 9  | d | 1970-01-01T00:00:00.000000005 |",
                "| 3  | e | 1970-01-01T00:00:00.000000008 |",
                "+----+---+-------------------------------+",
            ],
            2, // 2 input streams
            2, // all 2 input streams are fetched and polled
            task_ctx,
        )
        .await;
    }

    #[tokio::test]
    async fn test_multiple_batches_per_partition() {
        let task_ctx = Arc::new(TaskContext::default());
        let make_batch = |values: Vec<i32>| {
            let a: ArrayRef = Arc::new(Int32Array::from(values));
            RecordBatch::try_from_iter(vec![("a", a)]).unwrap()
        };
        let partitions = [
            vec![make_batch(vec![1, 2]), make_batch(vec![3, 4])],
            vec![make_batch(vec![5, 6]), make_batch(vec![7, 8])],
        ];

        // No fetch limit: all batches of all partitions are returned in
        // partition order
        run_progressive_eval_test(
            &partitions,
            None,
            &[
                "+---+", "| a |", "+---+", "| 1 |", "| 2 |", "| 3 |", "| 4 |", "| 5 |",
                "| 6 |", "| 7 |", "| 8 |", "+---+",
            ],
            2, // 2 input streams
            2, // all 2 input streams are fetched and polled
            Arc::clone(&task_ctx),
        )
        .await;

        // Fetch limit in the middle of the first partition's second batch:
        // that batch is truncated
        run_progressive_eval_test(
            &partitions,
            Some(3),
            &[
                "+---+", "| a |", "+---+", "| 1 |", "| 2 |", "| 3 |", "+---+",
            ],
            2, // 2 input streams
            2, // the second stream is prefetched even though it is never polled
            Arc::clone(&task_ctx),
        )
        .await;

        // Fetch limit exactly at the end of the first partition: both of its
        // batches are returned untruncated and nothing from the second
        // partition is emitted
        run_progressive_eval_test(
            &partitions,
            Some(4),
            &[
                "+---+", "| a |", "+---+", "| 1 |", "| 2 |", "| 3 |", "| 4 |", "+---+",
            ],
            2, // 2 input streams
            2, // the second stream is prefetched even though it is never polled
            Arc::clone(&task_ctx),
        )
        .await;

        // Fetch limit in the middle of the second partition's first batch:
        // all of the first partition plus a truncated batch from the second
        run_progressive_eval_test(
            &partitions,
            Some(5),
            &[
                "+---+", "| a |", "+---+", "| 1 |", "| 2 |", "| 3 |", "| 4 |", "| 5 |",
                "+---+",
            ],
            2, // 2 input streams
            2, // all 2 input streams are fetched and polled
            Arc::clone(&task_ctx),
        )
        .await;

        // With prefetch disabled, a fetch limit satisfied part-way through
        // the first partition's batches never starts the second stream
        run_progressive_eval_test(
            &partitions,
            Some(3),
            &[
                "+---+", "| a |", "+---+", "| 1 |", "| 2 |", "| 3 |", "+---+",
            ],
            2, // 2 input streams
            1, // only the first stream is started
            task_ctx_with_prefetch_depth(0),
        )
        .await;
    }

    #[tokio::test]
    async fn test_fetch_limit_1() {
        let task_ctx = Arc::new(TaskContext::default());
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

        let a: ArrayRef = Arc::new(Int32Array::from(vec![70, 90, 30]));
        let b: ArrayRef = Arc::new(StringArray::from_iter(vec![
            Some("c"),
            Some("d"),
            Some("e"),
        ]));
        let c: ArrayRef = Arc::new(TimestampNanosecondArray::from(vec![4, 6, 2]));
        let b2 = RecordBatch::try_from_iter(vec![("a", a), ("b", b), ("c", c)]).unwrap();

        // [b2, b1]
        // b2 has 3 rows. b1 has 5 rows
        // Fetch limit is 1 --> return the first row of the first batch (b2)
        run_progressive_eval_test(
            &[vec![b2.clone()], vec![b1.clone()]],
            Some(1),
            &[
                "+----+---+-------------------------------+",
                "| a  | b | c                             |",
                "+----+---+-------------------------------+",
                "| 70 | c | 1970-01-01T00:00:00.000000004 |",
                "+----+---+-------------------------------+",
            ],
            2, // 2 input streams
            2, // all 2 input streams are fetched by default even though only the first one is actually polled
            Arc::clone(&task_ctx),
        )
        .await;

        // [b1, b2]
        // b1 has 5 rows. b2 has 3 rows
        // Fetch limit is 1 --> return the first row of the first batch (b1)
        run_progressive_eval_test(
            &[vec![b1], vec![b2]],
            Some(1),
            &[
                "+---+---+-------------------------------+",
                "| a | b | c                             |",
                "+---+---+-------------------------------+",
                "| 1 | a | 1970-01-01T00:00:00.000000008 |",
                "+---+---+-------------------------------+",
            ],
            2, // 2 input streams
            2, // all 2 input streams are fetched by default even though only the first one is actually polled
            task_ctx,
        )
        .await;
    }

    #[tokio::test]
    async fn test_fetch_limit_equal_first_batch_size() {
        let task_ctx = Arc::new(TaskContext::default());
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

        let a: ArrayRef = Arc::new(Int32Array::from(vec![70, 90, 30]));
        let b: ArrayRef = Arc::new(StringArray::from_iter(vec![
            Some("c"),
            Some("d"),
            Some("e"),
        ]));
        let c: ArrayRef = Arc::new(TimestampNanosecondArray::from(vec![4, 6, 2]));
        let b2 = RecordBatch::try_from_iter(vec![("a", a), ("b", b), ("c", c)]).unwrap();

        // [b2, b1]
        // b2 has 3 rows. b1 has 5 rows
        // Fetch limit is 3 --> return all 3 rows of the first batch (b2) that covers that limit
        run_progressive_eval_test(
            &[vec![b2.clone()], vec![b1.clone()]],
            Some(3),
            &[
                "+----+---+-------------------------------+",
                "| a  | b | c                             |",
                "+----+---+-------------------------------+",
                "| 70 | c | 1970-01-01T00:00:00.000000004 |",
                "| 90 | d | 1970-01-01T00:00:00.000000006 |",
                "| 30 | e | 1970-01-01T00:00:00.000000002 |",
                "+----+---+-------------------------------+",
            ],
            2, // 2 input streams
            2, // all 2 input streams are fetched by default even though only the first one is actually polled
            Arc::clone(&task_ctx),
        )
        .await;

        // [b1, b2]
        // b1 has 5 rows. b2 has 3 rows
        // Fetch limit is 5 --> return all 5 rows of first batch (b1) that covers that limit
        run_progressive_eval_test(
            &[vec![b1], vec![b2]],
            Some(5),
            &[
                "+---+---+-------------------------------+",
                "| a | b | c                             |",
                "+---+---+-------------------------------+",
                "| 1 | a | 1970-01-01T00:00:00.000000008 |",
                "| 2 | b | 1970-01-01T00:00:00.000000007 |",
                "| 7 | c | 1970-01-01T00:00:00.000000006 |",
                "| 9 | d | 1970-01-01T00:00:00.000000005 |",
                "| 3 | e | 1970-01-01T00:00:00.000000008 |",
                "+---+---+-------------------------------+",
            ],
            2, // 2 input streams
            2, // all 2 input streams are fetched by default even though only the first one is actually polled
            task_ctx,
        )
        .await;
    }

    #[tokio::test]
    async fn test_fetch_limit_over_first_batch_size() {
        let task_ctx = Arc::new(TaskContext::default());
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

        let a: ArrayRef = Arc::new(Int32Array::from(vec![70, 90, 30]));
        let b: ArrayRef = Arc::new(StringArray::from_iter(vec![
            Some("c"),
            Some("d"),
            Some("e"),
        ]));
        let c: ArrayRef = Arc::new(TimestampNanosecondArray::from(vec![4, 6, 2]));
        let b2 = RecordBatch::try_from_iter(vec![("a", a), ("b", b), ("c", c)]).unwrap();

        // [b2, b1]
        // b2 has 3 rows. b1 has 5 rows
        // Fetch limit is 4 --> return all of b2 plus the first row of b1
        run_progressive_eval_test(
            &[vec![b2.clone()], vec![b1.clone()]],
            Some(4),
            &[
                "+----+---+-------------------------------+",
                "| a  | b | c                             |",
                "+----+---+-------------------------------+",
                "| 70 | c | 1970-01-01T00:00:00.000000004 |",
                "| 90 | d | 1970-01-01T00:00:00.000000006 |",
                "| 30 | e | 1970-01-01T00:00:00.000000002 |",
                "| 1  | a | 1970-01-01T00:00:00.000000008 |",
                "+----+---+-------------------------------+",
            ],
            2, // 2 input streams
            2, // all 2 input streams are fetched and polled
            Arc::clone(&task_ctx),
        )
        .await;

        // [b1, b2]
        // b1 has 5 rows. b2 has 3 rows
        // Fetch limit is 6 --> return all of b1 plus the first row of b2
        run_progressive_eval_test(
            &[vec![b1], vec![b2]],
            Some(6),
            &[
                "+----+---+-------------------------------+",
                "| a  | b | c                             |",
                "+----+---+-------------------------------+",
                "| 1  | a | 1970-01-01T00:00:00.000000008 |",
                "| 2  | b | 1970-01-01T00:00:00.000000007 |",
                "| 7  | c | 1970-01-01T00:00:00.000000006 |",
                "| 9  | d | 1970-01-01T00:00:00.000000005 |",
                "| 3  | e | 1970-01-01T00:00:00.000000008 |",
                "| 70 | c | 1970-01-01T00:00:00.000000004 |",
                "+----+---+-------------------------------+",
            ],
            2, // 2 input streams
            2, // all 2 input streams are fetched and polled
            task_ctx,
        )
        .await;
    }

    #[tokio::test]
    async fn test_three_partitions_with_nulls() {
        let task_ctx = Arc::new(TaskContext::default());
        let a: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 7, 9, 3]));
        let b: ArrayRef = Arc::new(StringArray::from_iter(vec![
            Some("a"),
            Some("b"),
            Some("c"),
            None,
            Some("f"),
        ]));
        let c: ArrayRef = Arc::new(TimestampNanosecondArray::from(vec![8, 7, 6, 5, 8]));
        let b1 = RecordBatch::try_from_iter(vec![("a", a), ("b", b), ("c", c)]).unwrap();

        let a: ArrayRef = Arc::new(Int32Array::from(vec![10, 20, 70]));
        let b: ArrayRef = Arc::new(StringArray::from_iter(vec![
            Some("e"),
            Some("g"),
            Some("h"),
        ]));
        let c: ArrayRef = Arc::new(TimestampNanosecondArray::from(vec![40, 60, 20]));
        let b2 = RecordBatch::try_from_iter(vec![("a", a), ("b", b), ("c", c)]).unwrap();

        let a: ArrayRef = Arc::new(Int32Array::from(vec![100, 200, 700, 900]));
        let b: ArrayRef = Arc::new(StringArray::from_iter(vec![
            None,
            Some("g"),
            Some("h"),
            Some("i"),
        ]));
        let c: ArrayRef = Arc::new(TimestampNanosecondArray::from(vec![4, 6, 2, 2]));
        let b3 = RecordBatch::try_from_iter(vec![("a", a), ("b", b), ("c", c)]).unwrap();

        // [b1, b2, b3]
        // b1 has 5 rows. b2 has 3 rows. b3 has 4 rows
        // Fetch limit is 1 --> return the first row of b1
        run_progressive_eval_test(
            &[vec![b1.clone()], vec![b2.clone()], vec![b3.clone()]],
            Some(1),
            &[
                "+---+---+-------------------------------+",
                "| a | b | c                             |",
                "+---+---+-------------------------------+",
                "| 1 | a | 1970-01-01T00:00:00.000000008 |",
                "+---+---+-------------------------------+",
            ],
            3, // 3 input streams
            2, // 2 input streams are fetched by default even though only the first one is polled
            Arc::clone(&task_ctx),
        )
        .await;

        // [b1, b2, b3]
        // b1 has 5 rows. b2 has 3 rows. b3 has 4 rows
        // Fetch limit is 7 --> return all rows of b1 plus the first 2 rows of b2
        run_progressive_eval_test(
            &[vec![b1.clone()], vec![b2.clone()], vec![b3.clone()]],
            Some(7),
            &[
                "+----+---+-------------------------------+",
                "| a  | b | c                             |",
                "+----+---+-------------------------------+",
                "| 1  | a | 1970-01-01T00:00:00.000000008 |",
                "| 2  | b | 1970-01-01T00:00:00.000000007 |",
                "| 7  | c | 1970-01-01T00:00:00.000000006 |",
                "| 9  |   | 1970-01-01T00:00:00.000000005 |",
                "| 3  | f | 1970-01-01T00:00:00.000000008 |",
                "| 10 | e | 1970-01-01T00:00:00.000000040 |",
                "| 20 | g | 1970-01-01T00:00:00.000000060 |",
                "+----+---+-------------------------------+",
            ],
            3, // 3 input streams
            3, // since we need to poll 2 input streams, 1 extra stream is prefetched
            Arc::clone(&task_ctx),
        )
        .await;

        // [b1, b2, b3]
        // b1 has 5 rows. b2 has 3 rows. b3 has 4 rows
        // Fetch limit is 50 --> return all rows of all batches in the order of b1, b2, b3
        run_progressive_eval_test(
            &[vec![b1], vec![b2], vec![b3]],
            Some(50),
            &[
                "+-----+---+-------------------------------+",
                "| a   | b | c                             |",
                "+-----+---+-------------------------------+",
                "| 1   | a | 1970-01-01T00:00:00.000000008 |",
                "| 2   | b | 1970-01-01T00:00:00.000000007 |",
                "| 7   | c | 1970-01-01T00:00:00.000000006 |",
                "| 9   |   | 1970-01-01T00:00:00.000000005 |",
                "| 3   | f | 1970-01-01T00:00:00.000000008 |",
                "| 10  | e | 1970-01-01T00:00:00.000000040 |",
                "| 20  | g | 1970-01-01T00:00:00.000000060 |",
                "| 70  | h | 1970-01-01T00:00:00.000000020 |",
                "| 100 |   | 1970-01-01T00:00:00.000000004 |",
                "| 200 | g | 1970-01-01T00:00:00.000000006 |",
                "| 700 | h | 1970-01-01T00:00:00.000000002 |",
                "| 900 | i | 1970-01-01T00:00:00.000000002 |",
                "+-----+---+-------------------------------+",
            ],
            3, // 3 input streams
            3, // 3 input streams are fetched and polled
            task_ctx,
        )
        .await;
    }

    #[tokio::test]
    async fn test_four_partitions_with_nulls() {
        let task_ctx = Arc::new(TaskContext::default());

        // partition 1
        let a: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 7, 9, 3]));
        let b: ArrayRef = Arc::new(StringArray::from_iter(vec![
            Some("a"),
            Some("b"),
            Some("c"),
            None,
            Some("f"),
        ]));
        let c: ArrayRef = Arc::new(TimestampNanosecondArray::from(vec![8, 7, 6, 5, 8]));
        let b1 = RecordBatch::try_from_iter(vec![("a", a), ("b", b), ("c", c)]).unwrap();

        // partition 2
        let a: ArrayRef = Arc::new(Int32Array::from(vec![10, 20, 70]));
        let b: ArrayRef = Arc::new(StringArray::from_iter(vec![
            Some("e"),
            Some("g"),
            Some("h"),
        ]));
        let c: ArrayRef = Arc::new(TimestampNanosecondArray::from(vec![40, 60, 20]));
        let b2 = RecordBatch::try_from_iter(vec![("a", a), ("b", b), ("c", c)]).unwrap();

        // partition 3
        let a: ArrayRef = Arc::new(Int32Array::from(vec![100, 200, 700, 900]));
        let b: ArrayRef = Arc::new(StringArray::from_iter(vec![
            None,
            Some("g"),
            Some("h"),
            Some("i"),
        ]));
        let c: ArrayRef = Arc::new(TimestampNanosecondArray::from(vec![4, 6, 2, 2]));
        let b3 = RecordBatch::try_from_iter(vec![("a", a), ("b", b), ("c", c)]).unwrap();

        // partition 4
        let a: ArrayRef = Arc::new(Int32Array::from(vec![1000, 2000]));
        let b: ArrayRef = Arc::new(StringArray::from_iter(vec![None, Some("x")]));
        let c: ArrayRef = Arc::new(TimestampNanosecondArray::from(vec![40, 60]));
        let b4 = RecordBatch::try_from_iter(vec![("a", a), ("b", b), ("c", c)]).unwrap();

        // [b1, b2, b3, b4]
        // b1 has 5 rows. b2 has 3 rows. b3 has 4 rows. b4 has 2 rows
        // Fetch limit is 0 --> return nothing.
        run_progressive_eval_test(
            &[
                vec![b1.clone()],
                vec![b2.clone()],
                vec![b3.clone()],
                vec![b4.clone()],
            ],
            Some(0),
            &["++", "++"],
            4, // 4 input streams
            2, // 2 input streams are fetched by default even though nothing is polled
            Arc::clone(&task_ctx),
        )
        .await;

        // [b1, b2, b3, b4]
        // b1 has 5 rows. b2 has 3 rows. b3 has 4 rows. b4 has 2 rows
        // Fetch limit is 3 --> return the first 3 rows of b1
        run_progressive_eval_test(
            &[
                vec![b1.clone()],
                vec![b2.clone()],
                vec![b3.clone()],
                vec![b4.clone()],
            ],
            Some(3),
            &[
                "+---+---+-------------------------------+",
                "| a | b | c                             |",
                "+---+---+-------------------------------+",
                "| 1 | a | 1970-01-01T00:00:00.000000008 |",
                "| 2 | b | 1970-01-01T00:00:00.000000007 |",
                "| 7 | c | 1970-01-01T00:00:00.000000006 |",
                "+---+---+-------------------------------+",
            ],
            4, // 4 input streams
            2, // 2 input streams are fetched and one stream is polled
            Arc::clone(&task_ctx),
        )
        .await;

        // [b1, b2, b3, b4]
        // b1 has 5 rows. b2 has 3 rows. b3 has 4 rows. b4 has 2 rows
        // Fetch limit is 5 --> return all 5 rows of b1
        run_progressive_eval_test(
            &[
                vec![b1.clone()],
                vec![b2.clone()],
                vec![b3.clone()],
                vec![b4.clone()],
            ],
            Some(5),
            &[
                "+---+---+-------------------------------+",
                "| a | b | c                             |",
                "+---+---+-------------------------------+",
                "| 1 | a | 1970-01-01T00:00:00.000000008 |",
                "| 2 | b | 1970-01-01T00:00:00.000000007 |",
                "| 7 | c | 1970-01-01T00:00:00.000000006 |",
                "| 9 |   | 1970-01-01T00:00:00.000000005 |",
                "| 3 | f | 1970-01-01T00:00:00.000000008 |",
                "+---+---+-------------------------------+",
            ],
            4, // 4 input streams
            2, // 2 input streams are fetched and one stream is polled
            Arc::clone(&task_ctx),
        )
        .await;

        // [b1, b2, b3, b4]
        // b1 has 5 rows. b2 has 3 rows. b3 has 4 rows. b4 has 2 rows
        // Fetch limit is 8 --> return all 8 rows of b1 and b2
        // Fetched 3 input streams since we will always prefetch one extra one
        run_progressive_eval_test(
            &[
                vec![b1.clone()],
                vec![b2.clone()],
                vec![b3.clone()],
                vec![b4.clone()],
            ],
            Some(8),
            &[
                "+----+---+-------------------------------+",
                "| a  | b | c                             |",
                "+----+---+-------------------------------+",
                "| 1  | a | 1970-01-01T00:00:00.000000008 |",
                "| 2  | b | 1970-01-01T00:00:00.000000007 |",
                "| 7  | c | 1970-01-01T00:00:00.000000006 |",
                "| 9  |   | 1970-01-01T00:00:00.000000005 |",
                "| 3  | f | 1970-01-01T00:00:00.000000008 |",
                "| 10 | e | 1970-01-01T00:00:00.000000040 |",
                "| 20 | g | 1970-01-01T00:00:00.000000060 |",
                "| 70 | h | 1970-01-01T00:00:00.000000020 |",
                "+----+---+-------------------------------+",
            ],
            4, // 4 input streams
            3, // 3 input streams are fetched and 2 streams are polled
            Arc::clone(&task_ctx),
        )
        .await;

        // [b1, b2, b3, b4]
        // b1 has 5 rows. b2 has 3 rows. b3 has 4 rows. b4 has 2 rows
        // Fetch limit is 12 --> return all 12 rows of b1, b2 and b3
        // Fetches 4 input streams since we will always prefetch one extra one
        run_progressive_eval_test(
            &[
                vec![b1.clone()],
                vec![b2.clone()],
                vec![b3.clone()],
                vec![b4.clone()],
            ],
            Some(12),
            &[
                "+-----+---+-------------------------------+",
                "| a   | b | c                             |",
                "+-----+---+-------------------------------+",
                "| 1   | a | 1970-01-01T00:00:00.000000008 |",
                "| 2   | b | 1970-01-01T00:00:00.000000007 |",
                "| 7   | c | 1970-01-01T00:00:00.000000006 |",
                "| 9   |   | 1970-01-01T00:00:00.000000005 |",
                "| 3   | f | 1970-01-01T00:00:00.000000008 |",
                "| 10  | e | 1970-01-01T00:00:00.000000040 |",
                "| 20  | g | 1970-01-01T00:00:00.000000060 |",
                "| 70  | h | 1970-01-01T00:00:00.000000020 |",
                "| 100 |   | 1970-01-01T00:00:00.000000004 |",
                "| 200 | g | 1970-01-01T00:00:00.000000006 |",
                "| 700 | h | 1970-01-01T00:00:00.000000002 |",
                "| 900 | i | 1970-01-01T00:00:00.000000002 |",
                "+-----+---+-------------------------------+",
            ],
            4, // 4 input streams
            4, // 4 input streams are fetched and 3 streams are polled
            Arc::clone(&task_ctx),
        )
        .await;

        // [b1, b2, b3, b4]
        // b1 has 5 rows. b2 has 3 rows. b3 has 4 rows. b4 has 2 rows
        // Fetch limit is 15 --> return all 15 rows of b1, b2, b3 and b4
        // Fetches all 4 input streams
        run_progressive_eval_test(
            &[
                vec![b1.clone()],
                vec![b2.clone()],
                vec![b3.clone()],
                vec![b4.clone()],
            ],
            Some(15),
            &[
                "+------+---+-------------------------------+",
                "| a    | b | c                             |",
                "+------+---+-------------------------------+",
                "| 1    | a | 1970-01-01T00:00:00.000000008 |",
                "| 2    | b | 1970-01-01T00:00:00.000000007 |",
                "| 7    | c | 1970-01-01T00:00:00.000000006 |",
                "| 9    |   | 1970-01-01T00:00:00.000000005 |",
                "| 3    | f | 1970-01-01T00:00:00.000000008 |",
                "| 10   | e | 1970-01-01T00:00:00.000000040 |",
                "| 20   | g | 1970-01-01T00:00:00.000000060 |",
                "| 70   | h | 1970-01-01T00:00:00.000000020 |",
                "| 100  |   | 1970-01-01T00:00:00.000000004 |",
                "| 200  | g | 1970-01-01T00:00:00.000000006 |",
                "| 700  | h | 1970-01-01T00:00:00.000000002 |",
                "| 900  | i | 1970-01-01T00:00:00.000000002 |",
                "| 1000 |   | 1970-01-01T00:00:00.000000040 |",
                "| 2000 | x | 1970-01-01T00:00:00.000000060 |",
                "+------+---+-------------------------------+",
            ],
            4, // 4 input streams
            4, // 4 input streams are fetched and polled
            Arc::clone(&task_ctx),
        )
        .await;

        // [b1, b2, b3, b4]
        // b1 has 5 rows. b2 has 3 rows. b3 has 4 rows. b4 has 2 rows
        // No fetch limit--> return all 15 rows of b1, b2, b3 and b4
        run_progressive_eval_test(
            &[
                vec![b1.clone()],
                vec![b2.clone()],
                vec![b3.clone()],
                vec![b4.clone()],
            ],
            None, // No fetch limit
            &[
                "+------+---+-------------------------------+",
                "| a    | b | c                             |",
                "+------+---+-------------------------------+",
                "| 1    | a | 1970-01-01T00:00:00.000000008 |",
                "| 2    | b | 1970-01-01T00:00:00.000000007 |",
                "| 7    | c | 1970-01-01T00:00:00.000000006 |",
                "| 9    |   | 1970-01-01T00:00:00.000000005 |",
                "| 3    | f | 1970-01-01T00:00:00.000000008 |",
                "| 10   | e | 1970-01-01T00:00:00.000000040 |",
                "| 20   | g | 1970-01-01T00:00:00.000000060 |",
                "| 70   | h | 1970-01-01T00:00:00.000000020 |",
                "| 100  |   | 1970-01-01T00:00:00.000000004 |",
                "| 200  | g | 1970-01-01T00:00:00.000000006 |",
                "| 700  | h | 1970-01-01T00:00:00.000000002 |",
                "| 900  | i | 1970-01-01T00:00:00.000000002 |",
                "| 1000 |   | 1970-01-01T00:00:00.000000040 |",
                "| 2000 | x | 1970-01-01T00:00:00.000000060 |",
                "+------+---+-------------------------------+",
            ],
            4, // 4 input streams
            4, // all input streams end up read (lazily, 2 at a time) because no fetch limit stops early
            Arc::clone(&task_ctx),
        )
        .await;
    }

    #[tokio::test]
    async fn test_prefetch_depth_config() {
        let make_partition = |values: Vec<i32>| {
            let a: ArrayRef = Arc::new(Int32Array::from(values));
            vec![RecordBatch::try_from_iter(vec![("a", a)]).unwrap()]
        };
        let partitions = [
            make_partition(vec![1, 2]),
            make_partition(vec![3, 4]),
            make_partition(vec![5, 6]),
            make_partition(vec![7, 8]),
        ];

        let first_row = ["+---+", "| a |", "+---+", "| 1 |", "+---+"];
        let first_batch = ["+---+", "| a |", "+---+", "| 1 |", "| 2 |", "+---+"];
        let all_rows = [
            "+---+", "| a |", "+---+", "| 1 |", "| 2 |", "| 3 |", "| 4 |", "| 5 |",
            "| 6 |", "| 7 |", "| 8 |", "+---+",
        ];

        // Prefetch depth 0: only the stream being polled is started, so a
        // fetch limit satisfied by the first stream reads nothing else
        run_progressive_eval_test(
            &partitions,
            Some(1),
            &first_row,
            4, // 4 input streams
            1, // only the first stream is started
            task_ctx_with_prefetch_depth(0),
        )
        .await;

        // Prefetch depth 0 without a fetch limit: streams are started one at
        // a time until all of them have been read
        run_progressive_eval_test(
            &partitions,
            None,
            &all_rows,
            4, // 4 input streams
            4, // all streams are eventually read
            task_ctx_with_prefetch_depth(0),
        )
        .await;

        // Prefetch depth 2: the current stream plus two more are started up
        // front. The fetch limit is satisfied by the first stream, so no
        // further streams are started
        run_progressive_eval_test(
            &partitions,
            Some(2),
            &first_batch,
            4, // 4 input streams
            3, // the current stream plus 2 prefetched streams are started
            task_ctx_with_prefetch_depth(2),
        )
        .await;

        // Prefetch depth 3: all four streams are started up front even
        // though only the first one is polled
        run_progressive_eval_test(
            &partitions,
            Some(1),
            &first_row,
            4, // 4 input streams
            4, // all streams are started up front
            task_ctx_with_prefetch_depth(3),
        )
        .await;

        // A prefetch depth larger than the number of streams is capped
        run_progressive_eval_test(
            &partitions,
            Some(1),
            &first_row,
            4, // 4 input streams
            4, // all streams are started up front
            task_ctx_with_prefetch_depth(10),
        )
        .await;
    }

    #[test]
    fn test_partition_statistics_account_for_fetch() {
        let a: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5]));
        let batch = RecordBatch::try_from_iter(vec![("a", a)]).unwrap();
        let schema = batch.schema();
        let input = TestMemoryExec::try_new_exec(&[vec![batch]], schema, None).unwrap();

        // Without a fetch limit the input statistics pass through unchanged
        let progressive = ProgressiveEvalExec::new(Arc::clone(&input) as _, None);
        let stats = StatisticsContext::new()
            .compute(&progressive, &StatisticsArgs::new().with_partition(Some(0)))
            .unwrap();
        assert_eq!(stats.num_rows, Precision::Exact(5));

        // A fetch limit below the input row count caps the reported row count
        let progressive = ProgressiveEvalExec::new(Arc::clone(&input) as _, Some(3));
        let stats = StatisticsContext::new()
            .compute(&progressive, &StatisticsArgs::new())
            .unwrap();
        assert_eq!(stats.num_rows, Precision::Exact(3));

        // A fetch limit above the input row count has no effect
        let progressive = ProgressiveEvalExec::new(Arc::clone(&input) as _, Some(10));
        let stats = StatisticsContext::new()
            .compute(&progressive, &StatisticsArgs::new())
            .unwrap();
        assert_eq!(stats.num_rows, Precision::Exact(5));

        // Setting a fetch limit on an existing plan is reflected in its statistics
        let progressive = ProgressiveEvalExec::new(Arc::clone(&input) as _, None);
        let limited = progressive.with_fetch(Some(2)).unwrap();
        let stats = StatisticsContext::new()
            .compute(limited.as_ref(), &StatisticsArgs::new())
            .unwrap();
        assert_eq!(stats.num_rows, Precision::Exact(2));
    }

    #[test]
    fn test_boundedness_accounts_for_fetch() {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        // An infinite streaming table reports `Boundedness::Unbounded`. The
        // stream is never polled, so it doesn't need to produce any batches.
        let input = Arc::new(
            StreamingTableExec::try_new(
                Arc::clone(&schema),
                vec![Arc::new(TestPartitionStream {
                    schema,
                    batches: vec![],
                }) as _],
                None,
                None,
                true,
                None,
            )
            .unwrap(),
        );

        // Without a fetch limit an unbounded input makes the output unbounded
        let progressive = ProgressiveEvalExec::new(Arc::clone(&input) as _, None);
        assert!(matches!(
            progressive.properties().boundedness,
            Boundedness::Unbounded { .. }
        ));

        // A fetch limit makes the output finite regardless of the input
        let progressive = ProgressiveEvalExec::new(Arc::clone(&input) as _, Some(10));
        assert!(matches!(
            progressive.properties().boundedness,
            Boundedness::Bounded
        ));

        // Removing the fetch limit from an existing plan updates its boundedness
        let unlimited = progressive.with_fetch(None).unwrap();
        assert!(matches!(
            unlimited.properties().boundedness,
            Boundedness::Unbounded { .. }
        ));
    }

    /// Create a task context whose session config sets
    /// `execution.progressive_eval_num_prefetch_input_streams` to `depth`
    fn task_ctx_with_prefetch_depth(depth: usize) -> Arc<TaskContext> {
        let mut config = SessionConfig::new();
        config
            .options_mut()
            .execution
            .progressive_eval_num_prefetch_input_streams = depth;
        Arc::new(TaskContext::default().with_session_config(config))
    }

    async fn run_progressive_eval_test(
        partitions: &[Vec<RecordBatch>],
        fetch: Option<usize>,
        expected_result: &[&str],
        expected_num_input_streams: usize,
        expected_num_read_input_streams: usize,
        context: Arc<TaskContext>,
    ) {
        let schema = if partitions.is_empty() {
            // Schema is arbitrary
            let a: ArrayRef = Arc::new(Int32Array::from(vec![1, 2]));
            let batch = RecordBatch::try_from_iter(vec![("a", a)]).unwrap();
            batch.schema()
        } else {
            partitions[0][0].schema()
        };

        let exec = TestMemoryExec::try_new_exec(partitions, schema, None).unwrap();
        let progressive = Arc::new(ProgressiveEvalExec::new(exec, fetch));

        let progressive_clone = Arc::clone(&progressive);

        let collected = collect(progressive, context).await.unwrap();
        assert_batches_eq!(expected_result, collected.as_slice());

        // verify metrics
        let metrics = progressive_clone.metrics().unwrap();
        let num_input_streams = Count::new();
        num_input_streams.add(expected_num_input_streams);
        let num_read_input_streams = Count::new();
        num_read_input_streams.add(expected_num_read_input_streams);

        assert_eq!(
            metrics.sum_by_name("num_inputs"),
            Some(MetricValue::Count {
                name: Borrowed("num_inputs"),
                count: num_input_streams
            })
        );
        assert_eq!(
            metrics.sum_by_name("num_read_inputs"),
            Some(MetricValue::Count {
                name: Borrowed("num_read_inputs"),
                count: num_read_input_streams
            })
        );
    }

    #[tokio::test]
    async fn test_merge_metrics() {
        let task_ctx = Arc::new(TaskContext::default());
        let a: ArrayRef = Arc::new(Int32Array::from(vec![1, 2]));
        let b: ArrayRef = Arc::new(StringArray::from_iter(vec![Some("a"), Some("c")]));
        let b1 = RecordBatch::try_from_iter(vec![("a", a), ("b", b)]).unwrap();

        let a: ArrayRef = Arc::new(Int32Array::from(vec![10, 20]));
        let b: ArrayRef = Arc::new(StringArray::from_iter(vec![Some("b"), Some("d")]));
        let b2 = RecordBatch::try_from_iter(vec![("a", a), ("b", b)]).unwrap();

        let schema = b1.schema();
        let exec =
            TestMemoryExec::try_new_exec(&[vec![b1], vec![b2]], schema, None).unwrap();
        let progressive = Arc::new(ProgressiveEvalExec::new(exec, None));

        let collected =
            collect(Arc::<ProgressiveEvalExec>::clone(&progressive), task_ctx)
                .await
                .unwrap();
        let expected = [
            "+----+---+",
            "| a  | b |",
            "+----+---+",
            "| 1  | a |",
            "| 2  | c |",
            "| 10 | b |",
            "| 20 | d |",
            "+----+---+",
        ];
        assert_batches_eq!(expected, collected.as_slice());

        // Now, validate metrics
        let metrics = progressive.metrics().unwrap();

        assert_eq!(metrics.output_rows().unwrap(), 4);
        assert!(metrics.elapsed_compute().unwrap() > 0);

        let num_input_streams = Count::new();
        num_input_streams.add(2);
        assert_eq!(
            metrics.sum_by_name("num_inputs"),
            Some(MetricValue::Count {
                name: Borrowed("num_inputs"),
                count: num_input_streams
            })
        );

        let num_read_input_streams = Count::new();
        num_read_input_streams.add(2);
        assert_eq!(
            metrics.sum_by_name("num_read_inputs"),
            Some(MetricValue::Count {
                name: Borrowed("num_read_inputs"),
                count: num_read_input_streams
            })
        );

        let mut saw_start = false;
        let mut saw_end = false;
        metrics.iter().for_each(|m| match m.value() {
            MetricValue::StartTimestamp(ts) => {
                saw_start = true;
                assert!(nanos_from_timestamp(ts) > 0);
            }
            MetricValue::EndTimestamp(ts) => {
                saw_end = true;
                assert!(nanos_from_timestamp(ts) > 0);
            }
            _ => {}
        });

        assert!(saw_start);
        assert!(saw_end);
    }

    fn nanos_from_timestamp(ts: &Timestamp) -> i64 {
        ts.value().unwrap().timestamp_nanos_opt().unwrap()
    }

    #[tokio::test]
    async fn test_drop_cancel() -> Result<()> {
        let task_ctx = Arc::new(TaskContext::default());
        let schema =
            Arc::new(Schema::new(vec![Field::new("a", DataType::Float32, true)]));

        let blocking_exec = Arc::new(BlockingExec::new(Arc::clone(&schema), 2));
        let refs = blocking_exec.refs();
        let progressive_exec = Arc::new(ProgressiveEvalExec::new(blocking_exec, None));

        let fut = collect(progressive_exec, task_ctx);
        let mut fut = fut.boxed();

        assert!(
            fut.as_mut().now_or_never().is_none(),
            "future should be pending"
        );
        drop(fut);

        // The plan and its streams should be dropped along with the future;
        // wait for the spawn_buffered tasks to notice and release their
        // references.
        assert_strong_count_converges_to_zero(refs).await;

        Ok(())
    }

    #[tokio::test]
    async fn test_error_in_first_stream_aborts_output() {
        let task_ctx = Arc::new(TaskContext::default());
        let exec = error_exec(2, 0);
        let progressive = ProgressiveEvalExec::new(exec, None);

        let mut stream = progressive.execute(0, task_ctx).unwrap();

        let batch = stream.next().await.unwrap().unwrap();
        assert_eq!(batch.num_rows(), 2);

        let err = stream.next().await.unwrap().unwrap_err();
        assert!(err.to_string().contains("error in partition 0"), "{err}");

        // The error aborts the output stream: the second input stream still
        // holds valid data, but it must not be emitted
        assert!(stream.next().await.is_none());
        assert!(stream.next().await.is_none());
    }

    #[tokio::test]
    async fn test_error_in_later_stream_propagates() {
        let task_ctx = Arc::new(TaskContext::default());
        let exec = error_exec(3, 1);
        let progressive = ProgressiveEvalExec::new(exec, None);

        let mut stream = progressive.execute(0, task_ctx).unwrap();

        // Data before the error arrives intact: all of partition 0
        // and the first batch of partition 1
        let batch = stream.next().await.unwrap().unwrap();
        assert_eq!(batch.num_rows(), 2);
        let batch = stream.next().await.unwrap().unwrap();
        assert_eq!(batch.num_rows(), 2);

        let err = stream.next().await.unwrap().unwrap_err();
        assert!(err.to_string().contains("error in partition 1"), "{err}");

        // Partition 2 is not emitted after the error
        assert!(stream.next().await.is_none());
    }

    /// A [`PartitionStream`] that yields one two-row batch, followed by an
    /// error if `error` is set. Used to verify error propagation.
    #[derive(Debug)]
    struct ErrorPartitionStream {
        schema: SchemaRef,
        partition: usize,
        error: bool,
    }

    impl PartitionStream for ErrorPartitionStream {
        fn schema(&self) -> &SchemaRef {
            &self.schema
        }

        fn execute(&self, _ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
            let a: ArrayRef = Arc::new(Int32Array::from(vec![
                (self.partition * 2 + 1) as i32,
                (self.partition * 2 + 2) as i32,
            ]));
            let batch = RecordBatch::try_new(Arc::clone(&self.schema), vec![a]).unwrap();
            let mut items = vec![Ok(batch)];
            if self.error {
                items.push(Err(DataFusionError::Execution(format!(
                    "error in partition {}",
                    self.partition
                ))));
            }
            Box::pin(RecordBatchStreamAdapter::new(
                Arc::clone(&self.schema),
                futures::stream::iter(items),
            ))
        }
    }

    /// Create an execution plan whose partitions each yield one two-row
    /// batch, with the `err_partition` stream yielding an error after its
    /// batch.
    fn error_exec(n_partitions: usize, err_partition: usize) -> Arc<dyn ExecutionPlan> {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, true)]));
        let partitions = (0..n_partitions)
            .map(|partition| {
                Arc::new(ErrorPartitionStream {
                    schema: Arc::clone(&schema),
                    partition,
                    error: partition == err_partition,
                }) as _
            })
            .collect();
        Arc::new(
            StreamingTableExec::try_new(schema, partitions, None, None, false, None)
                .unwrap(),
        )
    }
}
