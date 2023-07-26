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

use std::any::Any;
use std::sync::Arc;

use crate::physical_plan::common::spawn_buffered;
use crate::physical_plan::expressions::PhysicalSortExpr;
use crate::physical_plan::metrics::{
    BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet,
};
use crate::physical_plan::sorts::streaming_merge;
use crate::physical_plan::{
    DisplayAs, DisplayFormatType, Distribution, ExecutionPlan, Partitioning,
    SendableRecordBatchStream, Statistics,
};
use datafusion_execution::memory_pool::MemoryConsumer;

use arrow::datatypes::SchemaRef;
use datafusion_common::{DataFusionError, Result};
use datafusion_execution::TaskContext;
use datafusion_physical_expr::{
    EquivalenceProperties, OrderingEquivalenceProperties, PhysicalSortRequirement,
};

use log::{debug, trace};

/// Sort preserving merge execution plan
///
/// This takes an input execution plan and a list of sort expressions, and
/// provided each partition of the input plan is sorted with respect to
/// these sort expressions, this operator will yield a single partition
/// that is also sorted with respect to them
///
/// ```text
/// ┌─────────────────────────┐
/// │ ┌───┬───┬───┬───┐       │
/// │ │ A │ B │ C │ D │ ...   │──┐
/// │ └───┴───┴───┴───┘       │  │
/// └─────────────────────────┘  │  ┌───────────────────┐    ┌───────────────────────────────┐
///   Stream 1                   │  │                   │    │ ┌───┬───╦═══╦───┬───╦═══╗     │
///                              ├─▶│SortPreservingMerge│───▶│ │ A │ B ║ B ║ C │ D ║ E ║ ... │
///                              │  │                   │    │ └───┴─▲─╩═══╩───┴───╩═══╝     │
/// ┌─────────────────────────┐  │  └───────────────────┘    └─┬─────┴───────────────────────┘
/// │ ╔═══╦═══╗               │  │
/// │ ║ B ║ E ║     ...       │──┘                             │
/// │ ╚═══╩═══╝               │              Note Stable Sort: the merged stream
/// └─────────────────────────┘                places equal rows from stream 1
///   Stream 2
///
///
///  Input Streams                                             Output stream
///    (sorted)                                                  (sorted)
/// ```
#[derive(Debug)]
pub struct SortPreservingMergeExec {
    /// Input plan
    input: Arc<dyn ExecutionPlan>,
    /// Sort expressions
    expr: Vec<PhysicalSortExpr>,
    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,
    /// Optional number of rows to fetch. Stops producing rows after this fetch
    fetch: Option<usize>,
}

impl SortPreservingMergeExec {
    /// Create a new sort execution plan
    pub fn new(expr: Vec<PhysicalSortExpr>, input: Arc<dyn ExecutionPlan>) -> Self {
        Self {
            input,
            expr,
            metrics: ExecutionPlanMetricsSet::new(),
            fetch: None,
        }
    }
    /// Sets the number of rows to fetch
    pub fn with_fetch(mut self, fetch: Option<usize>) -> Self {
        self.fetch = fetch;
        self
    }

    /// Input schema
    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }

    /// Sort expressions
    pub fn expr(&self) -> &[PhysicalSortExpr] {
        &self.expr
    }

    /// Fetch
    pub fn fetch(&self) -> Option<usize> {
        self.fetch
    }
}

impl DisplayAs for SortPreservingMergeExec {
    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                let expr: Vec<String> = self.expr.iter().map(|e| e.to_string()).collect();
                write!(f, "SortPreservingMergeExec: [{}]", expr.join(","))?;
                if let Some(fetch) = self.fetch {
                    write!(f, ", fetch={fetch}")?;
                };

                Ok(())
            }
        }
    }
}

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

    /// Specifies whether this plan generates an infinite stream of records.
    /// If the plan does not support pipelining, but its input(s) are
    /// infinite, returns an error to indicate this.
    fn unbounded_output(&self, children: &[bool]) -> Result<bool> {
        Ok(children[0])
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![Distribution::UnspecifiedDistribution]
    }

    fn required_input_ordering(&self) -> Vec<Option<Vec<PhysicalSortRequirement>>> {
        vec![Some(PhysicalSortRequirement::from_sort_exprs(&self.expr))]
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        self.input.output_ordering()
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }

    fn equivalence_properties(&self) -> EquivalenceProperties {
        self.input.equivalence_properties()
    }

    fn ordering_equivalence_properties(&self) -> OrderingEquivalenceProperties {
        self.input.ordering_equivalence_properties()
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![self.input.clone()]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(
            SortPreservingMergeExec::new(self.expr.clone(), children[0].clone())
                .with_fetch(self.fetch),
        ))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        trace!(
            "Start SortPreservingMergeExec::execute for partition: {}",
            partition
        );
        if 0 != partition {
            return Err(DataFusionError::Internal(format!(
                "SortPreservingMergeExec invalid partition {partition}"
            )));
        }

        let input_partitions = self.input.output_partitioning().partition_count();
        trace!(
            "Number of input partitions of  SortPreservingMergeExec::execute: {}",
            input_partitions
        );
        let schema = self.schema();

        let reservation =
            MemoryConsumer::new(format!("SortPreservingMergeExec[{partition}]"))
                .register(&context.runtime_env().memory_pool);

        match input_partitions {
            0 => Err(DataFusionError::Internal(
                "SortPreservingMergeExec requires at least one input partition"
                    .to_owned(),
            )),
            1 => {
                // bypass if there is only one partition to merge (no metrics in this case either)
                let result = self.input.execute(0, context);
                debug!("Done getting stream for SortPreservingMergeExec::execute with 1 input");
                result
            }
            _ => {
                let receivers = (0..input_partitions)
                    .map(|partition| {
                        let stream = self.input.execute(partition, context.clone())?;
                        Ok(spawn_buffered(stream, 1))
                    })
                    .collect::<Result<_>>()?;

                debug!("Done setting up sender-receiver for SortPreservingMergeExec::execute");

                let result = streaming_merge(
                    receivers,
                    schema,
                    &self.expr,
                    BaselineMetrics::new(&self.metrics, partition),
                    context.session_config().batch_size(),
                    self.fetch,
                    reservation,
                )?;

                debug!("Got stream result from SortPreservingMergeStream::new_from_receivers");

                Ok(result)
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

#[cfg(test)]
mod tests {
    use std::char::from_u32;
    use std::iter::FromIterator;
    use rand::Rng;
    use std::pin::Pin;

    use arrow::array::ArrayRef;
    use arrow::compute::SortOptions;
    use arrow::datatypes::{DataType, Field, Schema, Int32Type};
    use arrow::record_batch::RecordBatch;
    use datafusion_execution::config::SessionConfig;
    use tempfile::TempDir;
    use futures::{FutureExt, StreamExt, stream::BoxStream};

    use crate::physical_plan::coalesce_partitions::CoalescePartitionsExec;
    use crate::physical_plan::expressions::col;
    use crate::physical_plan::memory::MemoryExec;
    use crate::physical_plan::metrics::MetricValue;
    use crate::physical_plan::sorts::sort::SortExec;
    use crate::physical_plan::stream::RecordBatchReceiverStream;
    use crate::physical_plan::{collect, common};
    use crate::test::exec::{assert_strong_count_converges_to_zero, BlockingExec};
    use crate::test::{self, assert_is_pending};
    use crate::{assert_batches_eq, test_util};
    use arrow::array::{Int32Array, StringArray, TimestampNanosecondArray, DictionaryArray};

    use crate::physical_plan::streaming::PartitionStream;
    use crate::physical_plan::stream::RecordBatchStreamAdapter;
    use crate::datasource::{streaming::StreamingTable, TableProvider};

    use super::*;
    
    fn make_infinite_sorted_stream() -> BoxStream<'static, RecordBatch> {
        futures::stream::unfold(0, |state| async move {
            // stop the stream at 1 batch now. 
            // Need to figure out how all the columns in the batches are sorted.
            if state >= 1 {
                return None;
            }

            let next_state = state + 1;
            
            // building col `a`
            let values = 
                StringArray::from_iter_values([
                    "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx", 
                    "yyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyy", 
                    "zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz"
                ]);
            let mut keys_vector: Vec<i32> = Vec::new();
            for _i in 1..=8192 {
                keys_vector.push(rand::thread_rng().gen_range(0..=2));
            }
            let keys = Int32Array::from(keys_vector);
            let col_a: ArrayRef = Arc::new(DictionaryArray::<Int32Type>::try_new(keys, Arc::new(values)).unwrap());        

            // building col `b`
            let mut values: Vec<String> = Vec::new();
            for _i in 1..=8192 {
                let ascii_value = rand::thread_rng().gen_range(97..=110);
                values.push(String::from(from_u32(ascii_value).unwrap()));
                values.sort();
            }
            let col_b: ArrayRef = Arc::new(StringArray::from(values));

            // build a record batch out of col `a` and col `b`
            let batch: RecordBatch = RecordBatch::try_from_iter(vec![("a", col_a), ("b", col_b)]).unwrap();
            Some((batch, next_state))
        }).boxed()
    }
    
    struct InfiniteStream {
        schema: SchemaRef,
    }

    impl PartitionStream for InfiniteStream {
        fn schema(&self) -> &SchemaRef {
            &self.schema
        }
    
        fn execute(&self, _ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
            // We create an iterator from the record batches and map them into Ok values,
            // converting the iterator into a futures::stream::Stream
            Box::pin(RecordBatchStreamAdapter::new(
                self.schema.clone(),
                make_infinite_sorted_stream().map(Ok)
            ))
        }
    }

    #[tokio::test]
    async fn test_dict_merge_infinite() {
        let session_ctx = SessionContext::new();
        let task_ctx: Arc<TaskContext> = session_ctx.task_ctx();

        let schema = SchemaRef::new(Schema::new(vec![
            Field::new("a", DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)), false),
            Field::new("b", DataType::Utf8, false),
        ]));

        let stream_1 = Arc::new(InfiniteStream {
            schema: schema.clone(),
        });

        let stream_2 = Arc::new(InfiniteStream {
            schema: schema.clone(),
        });

        println!("SortPreservingMergeExec result: ");

        let sort = vec![
            PhysicalSortExpr {
                expr: col("b", &schema).unwrap(),
                options: Default::default(),
            }
        ];
        
        let provider = StreamingTable::try_new(schema, vec![stream_1, stream_2]).unwrap();

        let plan = provider.scan(&session_ctx.state(), None, &[], None).await.unwrap();
        let exec = Arc::new(SortPreservingMergeExec::new(sort, plan));
        let mut stream = exec.execute(0, task_ctx).unwrap();
        while let Some(batch) = stream.next().await {
            println!("{}", arrow::util::pretty::pretty_format_batches(&[batch.unwrap().clone()])
                .unwrap()
                .to_string());
        }
    }

    #[tokio::test]
    async fn test_dict_merge() {
        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();

        let values = StringArray::from_iter_values(["a", "b", "c"]);
        let keys = Int32Array::from(vec![0, 0, 1, 2, 2, 1, 1, 0, 2]);
        let a: ArrayRef = Arc::new(DictionaryArray::<Int32Type>::try_new(keys, Arc::new(values)).unwrap());
        let b: ArrayRef = Arc::new(Int32Array::from(vec![10, 15, 12, 56, 34, 76, 2, 15, 29]));
        let batch_1 = RecordBatch::try_from_iter(vec![("a", a), ("b", b)]).unwrap();

        let values = StringArray::from_iter_values(["d", "e", "f"]);
        let keys = Int32Array::from(vec![0, 0, 1, 2, 2, 1, 1, 0, 2]);
        let a: ArrayRef = Arc::new(DictionaryArray::<Int32Type>::try_new(keys, Arc::new(values)).unwrap());
        let b: ArrayRef = Arc::new(Int32Array::from(vec![11, 16, 13, 57, 35, 77, 4, 17, 34]));

        let batch_2 = RecordBatch::try_from_iter(vec![("a", a), ("b", b)]).unwrap();

        let schema = batch_1.schema();
        let sort = vec![
            PhysicalSortExpr {
                expr: col("b", &schema).unwrap(),
                options: Default::default(),
            }
        ];
        let exec = MemoryExec::try_new(&[vec![batch_1], vec![batch_2]], schema, None).unwrap();
        let merge = Arc::new(SortPreservingMergeExec::new(sort, Arc::new(exec)));
        let collected = collect(merge, task_ctx).await.unwrap();
        collected.iter().for_each(|batch| {
            println!("{}", arrow::util::pretty::pretty_format_batches(&[batch.clone()])
                .unwrap()
                .to_string());
        });

        // let expected = vec![
        //     "+---+---+",
        //     "| a | b |",
        //     "+---+---+",
        //     "| a | a |",
        //     "| d | b |",
        //     "| a | c |",
        //     "| d | d |",
        //     "| b | e |",
        //     "| e | f |",
        //     "| c | g |",
        //     "| f | h |",
        //     "| c | i |",
        //     "| f | j |",
        //     "| b | k |",
        //     "| e | l |",
        //     "| b | m |",
        //     "| e | n |",
        //     "| a | o |",
        //     "| d | p |",
        //     "| c | q |",
        //     "| f | r |",
        //     "+---+---+",
        // ];
        // assert_batches_eq!(expected, collected.as_slice());
    }

    #[tokio::test]
    async fn test_merge_interleave() {
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

        _test_merge(
            &[vec![b1], vec![b2]],
            &[
                "+----+---+-------------------------------+",
                "| a  | b | c                             |",
                "+----+---+-------------------------------+",
                "| 1  | a | 1970-01-01T00:00:00.000000008 |",
                "| 10 | b | 1970-01-01T00:00:00.000000004 |",
                "| 2  | c | 1970-01-01T00:00:00.000000007 |",
                "| 20 | d | 1970-01-01T00:00:00.000000006 |",
                "| 7  | e | 1970-01-01T00:00:00.000000006 |",
                "| 70 | f | 1970-01-01T00:00:00.000000002 |",
                "| 9  | g | 1970-01-01T00:00:00.000000005 |",
                "| 90 | h | 1970-01-01T00:00:00.000000002 |",
                "| 30 | j | 1970-01-01T00:00:00.000000006 |", // input b2 before b1
                "| 3  | j | 1970-01-01T00:00:00.000000008 |",
                "+----+---+-------------------------------+",
            ],
            task_ctx,
        )
        .await;
    }

    #[tokio::test]
    async fn test_merge_some_overlap() {
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
                "| 1   | a | 1970-01-01T00:00:00.000000008 |",
                "| 2   | b | 1970-01-01T00:00:00.000000007 |",
                "| 70  | c | 1970-01-01T00:00:00.000000004 |",
                "| 7   | c | 1970-01-01T00:00:00.000000006 |",
                "| 9   | d | 1970-01-01T00:00:00.000000005 |",
                "| 90  | d | 1970-01-01T00:00:00.000000006 |",
                "| 30  | e | 1970-01-01T00:00:00.000000002 |",
                "| 3   | e | 1970-01-01T00:00:00.000000008 |",
                "| 100 | f | 1970-01-01T00:00:00.000000002 |",
                "| 110 | g | 1970-01-01T00:00:00.000000006 |",
                "+-----+---+-------------------------------+",
            ],
            task_ctx,
        )
        .await;
    }

    #[tokio::test]
    async fn test_merge_no_overlap() {
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
                "| 1  | a | 1970-01-01T00:00:00.000000008 |",
                "| 2  | b | 1970-01-01T00:00:00.000000007 |",
                "| 7  | c | 1970-01-01T00:00:00.000000006 |",
                "| 9  | d | 1970-01-01T00:00:00.000000005 |",
                "| 3  | e | 1970-01-01T00:00:00.000000008 |",
                "| 10 | f | 1970-01-01T00:00:00.000000004 |",
                "| 20 | g | 1970-01-01T00:00:00.000000006 |",
                "| 70 | h | 1970-01-01T00:00:00.000000002 |",
                "| 90 | i | 1970-01-01T00:00:00.000000002 |",
                "| 30 | j | 1970-01-01T00:00:00.000000006 |",
                "+----+---+-------------------------------+",
            ],
            task_ctx,
        )
        .await;
    }

    #[tokio::test]
    async fn test_merge_three_partitions() {
        let task_ctx = Arc::new(TaskContext::default());
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
                "| 1   | a | 1970-01-01T00:00:00.000000008 |",
                "| 2   | b | 1970-01-01T00:00:00.000000007 |",
                "| 7   | c | 1970-01-01T00:00:00.000000006 |",
                "| 9   | d | 1970-01-01T00:00:00.000000005 |",
                "| 10  | e | 1970-01-01T00:00:00.000000040 |",
                "| 100 | f | 1970-01-01T00:00:00.000000004 |",
                "| 3   | f | 1970-01-01T00:00:00.000000008 |",
                "| 200 | g | 1970-01-01T00:00:00.000000006 |",
                "| 20  | g | 1970-01-01T00:00:00.000000060 |",
                "| 700 | h | 1970-01-01T00:00:00.000000002 |",
                "| 70  | h | 1970-01-01T00:00:00.000000020 |",
                "| 900 | i | 1970-01-01T00:00:00.000000002 |",
                "| 90  | i | 1970-01-01T00:00:00.000000020 |",
                "| 300 | j | 1970-01-01T00:00:00.000000006 |",
                "| 30  | j | 1970-01-01T00:00:00.000000060 |",
                "+-----+---+-------------------------------+",
            ],
            task_ctx,
        )
        .await;
    }

    async fn _test_merge(
        partitions: &[Vec<RecordBatch>],
        exp: &[&str],
        context: Arc<TaskContext>,
    ) {
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
        let merge = Arc::new(SortPreservingMergeExec::new(sort, Arc::new(exec)));

        let collected = collect(merge, context).await.unwrap();
        assert_batches_eq!(exp, collected.as_slice());
    }

    async fn sorted_merge(
        input: Arc<dyn ExecutionPlan>,
        sort: Vec<PhysicalSortExpr>,
        context: Arc<TaskContext>,
    ) -> RecordBatch {
        let merge = Arc::new(SortPreservingMergeExec::new(sort, input));
        let mut result = collect(merge, context).await.unwrap();
        assert_eq!(result.len(), 1);
        result.remove(0)
    }

    async fn partition_sort(
        input: Arc<dyn ExecutionPlan>,
        sort: Vec<PhysicalSortExpr>,
        context: Arc<TaskContext>,
    ) -> RecordBatch {
        let sort_exec =
            Arc::new(SortExec::new(sort.clone(), input).with_preserve_partitioning(true));
        sorted_merge(sort_exec, sort, context).await
    }

    async fn basic_sort(
        src: Arc<dyn ExecutionPlan>,
        sort: Vec<PhysicalSortExpr>,
        context: Arc<TaskContext>,
    ) -> RecordBatch {
        let merge = Arc::new(CoalescePartitionsExec::new(src));
        let sort_exec = Arc::new(SortExec::new(sort, merge));
        let mut result = collect(sort_exec, context).await.unwrap();
        assert_eq!(result.len(), 1);
        result.remove(0)
    }

    #[tokio::test]
    async fn test_partition_sort() -> Result<()> {
        let task_ctx = Arc::new(TaskContext::default());
        let partitions = 4;
        let tmp_dir = TempDir::new()?;
        let csv = test::scan_partitioned_csv(partitions, tmp_dir.path()).unwrap();
        let schema = csv.schema();

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

        let basic = basic_sort(csv.clone(), sort.clone(), task_ctx.clone()).await;
        let partition = partition_sort(csv, sort, task_ctx.clone()).await;

        let basic = arrow::util::pretty::pretty_format_batches(&[basic])
            .unwrap()
            .to_string();
        let partition = arrow::util::pretty::pretty_format_batches(&[partition])
            .unwrap()
            .to_string();

        assert_eq!(
            basic, partition,
            "basic:\n\n{basic}\n\npartition:\n\n{partition}\n\n"
        );

        Ok(())
    }

    // Split the provided record batch into multiple batch_size record batches
    fn split_batch(sorted: &RecordBatch, batch_size: usize) -> Vec<RecordBatch> {
        let batches = (sorted.num_rows() + batch_size - 1) / batch_size;

        // Split the sorted RecordBatch into multiple
        (0..batches)
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
        context: Arc<TaskContext>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let partitions = 4;
        let tmp_dir = TempDir::new()?;
        let csv = test::scan_partitioned_csv(partitions, tmp_dir.path()).unwrap();

        let sorted = basic_sort(csv, sort, context).await;
        let split: Vec<_> = sizes.iter().map(|x| split_batch(&sorted, *x)).collect();

        Ok(Arc::new(
            MemoryExec::try_new(&split, sorted.schema(), None).unwrap(),
        ))
    }

    #[tokio::test]
    async fn test_partition_sort_streaming_input() -> Result<()> {
        let task_ctx = Arc::new(TaskContext::default());
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

        let input =
            sorted_partitioned_input(sort.clone(), &[10, 3, 11], task_ctx.clone())
                .await?;
        let basic = basic_sort(input.clone(), sort.clone(), task_ctx.clone()).await;
        let partition = sorted_merge(input, sort, task_ctx.clone()).await;

        assert_eq!(basic.num_rows(), 300);
        assert_eq!(partition.num_rows(), 300);

        let basic = arrow::util::pretty::pretty_format_batches(&[basic])
            .unwrap()
            .to_string();
        let partition = arrow::util::pretty::pretty_format_batches(&[partition])
            .unwrap()
            .to_string();

        assert_eq!(basic, partition);

        Ok(())
    }

    #[tokio::test]
    async fn test_partition_sort_streaming_input_output() -> Result<()> {
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

        // Test streaming with default batch size
        let task_ctx = Arc::new(TaskContext::default());
        let input =
            sorted_partitioned_input(sort.clone(), &[10, 5, 13], task_ctx.clone())
                .await?;
        let basic = basic_sort(input.clone(), sort.clone(), task_ctx).await;

        // batch size of 23
        let task_ctx = TaskContext::default()
            .with_session_config(SessionConfig::new().with_batch_size(23));
        let task_ctx = Arc::new(task_ctx);

        let merge = Arc::new(SortPreservingMergeExec::new(sort, input));
        let merged = collect(merge, task_ctx).await.unwrap();

        assert_eq!(merged.len(), 14);

        assert_eq!(basic.num_rows(), 300);
        assert_eq!(merged.iter().map(|x| x.num_rows()).sum::<usize>(), 300);

        let basic = arrow::util::pretty::pretty_format_batches(&[basic])
            .unwrap()
            .to_string();
        let partition = arrow::util::pretty::pretty_format_batches(merged.as_slice())
            .unwrap()
            .to_string();

        assert_eq!(basic, partition);

        Ok(())
    }

    #[tokio::test]
    async fn test_nulls() {
        let task_ctx = Arc::new(TaskContext::default());
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
        let merge = Arc::new(SortPreservingMergeExec::new(sort, Arc::new(exec)));

        let collected = collect(merge, task_ctx).await.unwrap();
        assert_eq!(collected.len(), 1);

        assert_batches_eq!(
            &[
                "+---+---+-------------------------------+",
                "| a | b | c                             |",
                "+---+---+-------------------------------+",
                "| 1 |   | 1970-01-01T00:00:00.000000008 |",
                "| 1 |   | 1970-01-01T00:00:00.000000008 |",
                "| 2 | a |                               |",
                "| 7 | b | 1970-01-01T00:00:00.000000006 |",
                "| 2 | b |                               |",
                "| 9 | d |                               |",
                "| 3 | e | 1970-01-01T00:00:00.000000004 |",
                "| 3 | g | 1970-01-01T00:00:00.000000005 |",
                "| 4 | h |                               |",
                "| 5 | i | 1970-01-01T00:00:00.000000004 |",
                "+---+---+-------------------------------+",
            ],
            collected.as_slice()
        );
    }

    #[tokio::test]
    async fn test_async() -> Result<()> {
        let task_ctx = Arc::new(TaskContext::default());
        let schema = test_util::aggr_test_schema();
        let sort = vec![PhysicalSortExpr {
            expr: col("c12", &schema).unwrap(),
            options: SortOptions::default(),
        }];

        let batches =
            sorted_partitioned_input(sort.clone(), &[5, 7, 3], task_ctx.clone()).await?;

        let partition_count = batches.output_partitioning().partition_count();
        let mut streams = Vec::with_capacity(partition_count);

        for partition in 0..partition_count {
            let mut builder = RecordBatchReceiverStream::builder(schema.clone(), 1);

            let sender = builder.tx();

            let mut stream = batches.execute(partition, task_ctx.clone()).unwrap();
            builder.spawn(async move {
                while let Some(batch) = stream.next().await {
                    sender.send(batch).await.unwrap();
                    // This causes the MergeStream to wait for more input
                    tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;
                }
            });

            streams.push(builder.build());
        }

        let metrics = ExecutionPlanMetricsSet::new();
        let reservation =
            MemoryConsumer::new("test").register(&task_ctx.runtime_env().memory_pool);

        let fetch = None;
        let merge_stream = streaming_merge(
            streams,
            batches.schema(),
            sort.as_slice(),
            BaselineMetrics::new(&metrics, 0),
            task_ctx.session_config().batch_size(),
            fetch,
            reservation,
        )
        .unwrap();

        let mut merged = common::collect(merge_stream).await.unwrap();

        assert_eq!(merged.len(), 1);
        let merged = merged.remove(0);
        let basic = basic_sort(batches, sort.clone(), task_ctx.clone()).await;

        let basic = arrow::util::pretty::pretty_format_batches(&[basic])
            .unwrap()
            .to_string();
        let partition = arrow::util::pretty::pretty_format_batches(&[merged])
            .unwrap()
            .to_string();

        assert_eq!(
            basic, partition,
            "basic:\n\n{basic}\n\npartition:\n\n{partition}\n\n"
        );

        Ok(())
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
        let sort = vec![PhysicalSortExpr {
            expr: col("b", &schema).unwrap(),
            options: Default::default(),
        }];
        let exec = MemoryExec::try_new(&[vec![b1], vec![b2]], schema, None).unwrap();
        let merge = Arc::new(SortPreservingMergeExec::new(sort, Arc::new(exec)));

        let collected = collect(merge.clone(), task_ctx).await.unwrap();
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
        let task_ctx = Arc::new(TaskContext::default());
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
        ));

        let fut = collect(sort_preserving_merge_exec, task_ctx);
        let mut fut = fut.boxed();

        assert_is_pending(&mut fut);
        drop(fut);
        assert_strong_count_converges_to_zero(refs).await;

        Ok(())
    }

    #[tokio::test]
    async fn test_stable_sort() {
        let task_ctx = Arc::new(TaskContext::default());

        // Create record batches like:
        // batch_number |value
        // -------------+------
        //    1         | A
        //    1         | B
        //
        // Ensure that the output is in the same order the batches were fed
        let partitions: Vec<Vec<RecordBatch>> = (0..10)
            .map(|batch_number| {
                let batch_number: Int32Array =
                    vec![Some(batch_number), Some(batch_number)]
                        .into_iter()
                        .collect();
                let value: StringArray = vec![Some("A"), Some("B")].into_iter().collect();

                let batch = RecordBatch::try_from_iter(vec![
                    ("batch_number", Arc::new(batch_number) as ArrayRef),
                    ("value", Arc::new(value) as ArrayRef),
                ])
                .unwrap();

                vec![batch]
            })
            .collect();

        let schema = partitions[0][0].schema();

        let sort = vec![PhysicalSortExpr {
            expr: col("value", &schema).unwrap(),
            options: SortOptions {
                descending: false,
                nulls_first: true,
            },
        }];

        let exec = MemoryExec::try_new(&partitions, schema, None).unwrap();
        let merge = Arc::new(SortPreservingMergeExec::new(sort, Arc::new(exec)));

        let collected = collect(merge, task_ctx).await.unwrap();
        assert_eq!(collected.len(), 1);

        // Expect the data to be sorted first by "batch_number" (because
        // that was the order it was fed in, even though only "value"
        // is in the sort key)
        assert_batches_eq!(
            &[
                "+--------------+-------+",
                "| batch_number | value |",
                "+--------------+-------+",
                "| 0            | A     |",
                "| 1            | A     |",
                "| 2            | A     |",
                "| 3            | A     |",
                "| 4            | A     |",
                "| 5            | A     |",
                "| 6            | A     |",
                "| 7            | A     |",
                "| 8            | A     |",
                "| 9            | A     |",
                "| 0            | B     |",
                "| 1            | B     |",
                "| 2            | B     |",
                "| 3            | B     |",
                "| 4            | B     |",
                "| 5            | B     |",
                "| 6            | B     |",
                "| 7            | B     |",
                "| 8            | B     |",
                "| 9            | B     |",
                "+--------------+-------+",
            ],
            collected.as_slice()
        );
    }
}
