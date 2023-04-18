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

//! Execution plan for writing in-memory batches of data

use std::any::Any;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::{fmt, mem};

use crate::physical_plan::{
    DisplayFormatType, Distribution, ExecutionPlan, Partitioning, RecordBatchStream,
    SendableRecordBatchStream,
};

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion_common::{Result, Statistics};
use datafusion_execution::TaskContext;
use datafusion_physical_expr::PhysicalSortExpr;
use futures::FutureExt;
use futures::{ready, Stream, StreamExt};
use tokio::sync::{OwnedRwLockWriteGuard, RwLock};

// Type alias for partition data
type PartitionData = Arc<RwLock<Vec<RecordBatch>>>;

// Macro to simplify polling a future
macro_rules! ready_poll {
    ($e:expr, $cx:expr) => {
        ready!(Box::pin($e).poll_unpin($cx))
    };
}

/// Execution plan for writing record batches to an in-memory table.
pub struct MemoryWriteExec {
    /// Input plan that produces the record batches to be written.
    input: Arc<dyn ExecutionPlan>,
    /// Reference to the MemTable's partition data.
    batches: Arc<Vec<PartitionData>>,
    /// Schema describing the structure of the data.
    schema: SchemaRef,
}

impl fmt::Debug for MemoryWriteExec {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "schema: {:?}", self.schema)
    }
}

impl ExecutionPlan for MemoryWriteExec {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    /// Get the schema for this execution plan
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(
            self.input.output_partitioning().partition_count(),
        )
    }

    fn benefits_from_input_partitioning(&self) -> bool {
        false
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        self.input.output_ordering()
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        // If the partition count of the MemTable is one, we want to require SinglePartition
        // since it would induce better plans in plan optimizer.
        if self.batches.len() == 1 {
            vec![Distribution::SinglePartition]
        } else {
            vec![Distribution::UnspecifiedDistribution]
        }
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        // In theory, if MemTable partition count equals the input plans output partition count,
        // the Execution plan can preserve the order inside the partitions.
        vec![self.batches.len() == self.input.output_partitioning().partition_count()]
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![self.input.clone()]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(MemoryWriteExec::try_new(
            children[0].clone(),
            self.batches.clone(),
            self.schema.clone(),
        )?))
    }

    /// Execute the plan and return a stream of record batches for the specified partition.
    /// Depending on the number of input partitions and MemTable partitions, it will choose
    /// either a less lock acquiring or a locked implementation.
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let batch_count = self.batches.len();
        let data = self.input.execute(partition, context)?;
        if batch_count >= self.input.output_partitioning().partition_count() {
            // If the number of input partitions matches the number of MemTable partitions,
            // use a lightweight implementation that doesn't utilize as many locks.
            let table_partition = self.batches[partition].clone();
            Ok(Box::pin(MemorySinkOneToOneStream::try_new(
                table_partition,
                data,
                self.schema.clone(),
            )?))
        } else {
            // Otherwise, use the locked implementation.
            let table_partition = self.batches[partition % batch_count].clone();
            Ok(Box::pin(MemorySinkStream::try_new(
                table_partition,
                data,
                self.schema.clone(),
            )?))
        }
    }

    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default => {
                write!(
                    f,
                    "MemoryWriteExec: partitions={}, input_partition={}",
                    self.batches.len(),
                    self.input.output_partitioning().partition_count()
                )
            }
        }
    }

    fn statistics(&self) -> Statistics {
        Statistics::default()
    }
}

impl MemoryWriteExec {
    /// Create a new execution plan for reading in-memory record batches
    /// The provided `schema` should not have the projection applied.
    pub fn try_new(
        plan: Arc<dyn ExecutionPlan>,
        batches: Arc<Vec<Arc<RwLock<Vec<RecordBatch>>>>>,
        schema: SchemaRef,
    ) -> Result<Self> {
        Ok(Self {
            input: plan,
            batches,
            schema,
        })
    }
}

/// This object encodes the different states of the [`MemorySinkStream`] when
/// processing record batches.
enum MemorySinkStreamState {
    /// The stream is pulling data from the input.
    Pull,
    /// The stream is writing data to the table partition.
    Write { maybe_batch: Option<RecordBatch> },
}

/// A stream that saves record batches in memory-backed storage.
/// Can work even when multiple input partitions map to the same table
/// partition, achieves buffer exclusivity by locking before writing.
pub struct MemorySinkStream {
    /// Stream of record batches to be inserted into the memory table.
    data: SendableRecordBatchStream,
    /// Memory table partition that stores the record batches.
    table_partition: Arc<RwLock<Vec<RecordBatch>>>,
    /// Schema representing the structure of the data.
    schema: SchemaRef,
    /// State of the iterator when processing multiple polls.
    state: MemorySinkStreamState,
}

impl MemorySinkStream {
    /// Create a new `MemorySinkStream` with the provided parameters.
    pub fn try_new(
        table_partition: Arc<RwLock<Vec<RecordBatch>>>,
        data: SendableRecordBatchStream,
        schema: SchemaRef,
    ) -> Result<Self> {
        Ok(Self {
            table_partition,
            data,
            schema,
            state: MemorySinkStreamState::Pull,
        })
    }

    /// Implementation of the `poll_next` method. Continuously polls the record
    /// batch stream, switching between the Pull and Write states. In case of
    /// an error, returns the error immediately.
    fn poll_next_impl(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Result<RecordBatch>>> {
        loop {
            match &mut self.state {
                MemorySinkStreamState::Pull => {
                    // Pull data from the input stream.
                    if let Some(result) = ready!(self.data.as_mut().poll_next(cx)) {
                        match result {
                            Ok(batch) => {
                                // Switch to the Write state with the received batch.
                                self.state = MemorySinkStreamState::Write {
                                    maybe_batch: Some(batch),
                                }
                            }
                            Err(e) => return Poll::Ready(Some(Err(e))), // Return the error immediately.
                        }
                    } else {
                        return Poll::Ready(None); // If the input stream is exhausted, return None.
                    }
                }
                MemorySinkStreamState::Write { maybe_batch } => {
                    // Acquire a write lock on the table partition.
                    let mut partition = ready_poll!(self.table_partition.write(), cx);
                    if let Some(b) = mem::take(maybe_batch) {
                        partition.push(b); // Insert the batch into the table partition.
                    }
                    self.state = MemorySinkStreamState::Pull; // Switch back to the Pull state.
                }
            }
        }
    }
}

impl Stream for MemorySinkStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        self.poll_next_impl(cx)
    }
}

impl RecordBatchStream for MemorySinkStream {
    /// Get the schema
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

/// This object encodes the different states of the [`MemorySinkOneToOneStream`]
/// when processing record batches.
enum MemorySinkOneToOneStreamState {
    /// The `Acquire` variant represents the state where the [`MemorySinkOneToOneStream`]
    /// is waiting to acquire the write lock on the shared partition to store the record batches.
    Acquire,

    /// The `Pull` variant represents the state where the [`MemorySinkOneToOneStream`] has
    /// acquired the write lock on the shared partition and can pull record batches from
    /// the input stream to store in the partition.
    Pull {
        /// The `partition` field contains an [`OwnedRwLockWriteGuard`] which wraps the
        /// shared partition, providing exclusive write access to the underlying `Vec<RecordBatch>`.
        partition: OwnedRwLockWriteGuard<Vec<RecordBatch>>,
    },
}

/// A stream that saves record batches in memory-backed storage.
/// Assumes that every table partition has at most one corresponding input
/// partition, so it locks the table partition only once.
pub struct MemorySinkOneToOneStream {
    /// Stream of record batches to be inserted into the memory table.
    data: SendableRecordBatchStream,
    /// Memory table partition that stores the record batches.
    table_partition: Arc<RwLock<Vec<RecordBatch>>>,
    /// Schema representing the structure of the data.
    schema: SchemaRef,
    /// State of the iterator when processing multiple polls.
    state: MemorySinkOneToOneStreamState,
}

impl MemorySinkOneToOneStream {
    /// Create a new `MemorySinkOneToOneStream` with the provided parameters.
    pub fn try_new(
        table_partition: Arc<RwLock<Vec<RecordBatch>>>,
        data: SendableRecordBatchStream,
        schema: SchemaRef,
    ) -> Result<Self> {
        Ok(Self {
            table_partition,
            data,
            schema,
            state: MemorySinkOneToOneStreamState::Acquire,
        })
    }

    /// Implementation of the `poll_next` method. Continuously polls the record
    /// batch stream and pushes batches to their corresponding table partition,
    /// which are lock-acquired only once. In case of an error, returns the
    /// error immediately.
    fn poll_next_impl(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Result<RecordBatch>>> {
        loop {
            match &mut self.state {
                MemorySinkOneToOneStreamState::Acquire => {
                    // Acquire a write lock on the table partition.
                    self.state = MemorySinkOneToOneStreamState::Pull {
                        partition: ready_poll!(
                            self.table_partition.clone().write_owned(),
                            cx
                        ),
                    };
                }
                MemorySinkOneToOneStreamState::Pull { partition } => {
                    // Iterate over the batches in the input data stream.
                    while let Some(result) = ready!(self.data.poll_next_unpin(cx)) {
                        match result {
                            Ok(batch) => {
                                partition.push(batch);
                            } // Insert the batch into the table partition.
                            Err(e) => return Poll::Ready(Some(Err(e))), // Return the error immediately.
                        }
                    }
                    // If the input stream is exhausted, return None to indicate the end of the stream.
                    return Poll::Ready(None);
                }
            }
        }
    }
}

impl Stream for MemorySinkOneToOneStream {
    type Item = Result<RecordBatch>;

    /// Poll the stream for the next item.
    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        self.poll_next_impl(cx)
    }
}

impl RecordBatchStream for MemorySinkOneToOneStream {
    /// Get the schema of the record batches in the stream.
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

#[cfg(test)]
mod tests {
    use crate::datasource::streaming::PartitionStream;
    use crate::datasource::{MemTable, TableProvider};
    use crate::physical_plan::stream::RecordBatchStreamAdapter;
    use crate::physical_plan::streaming::StreamingTableExec;
    use crate::physical_plan::{collect, displayable, SendableRecordBatchStream};
    use crate::prelude::{CsvReadOptions, SessionContext};
    use crate::test_util;
    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use arrow::record_batch::RecordBatch;
    use datafusion_common::from_slice::FromSlice;
    use datafusion_common::Result;
    use datafusion_execution::config::SessionConfig;
    use datafusion_execution::TaskContext;
    use futures::StreamExt;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_insert_into() -> Result<()> {
        // Create session context
        let config = SessionConfig::new().with_target_partitions(8);
        let ctx = SessionContext::with_config(config);
        let testdata = test_util::arrow_test_data();
        let schema = test_util::aggr_test_schema();
        ctx.register_csv(
            "aggregate_test_100",
            &format!("{testdata}/csv/aggregate_test_100.csv"),
            CsvReadOptions::new().schema(&schema),
        )
        .await?;
        ctx.sql(
            "CREATE TABLE table_without_values(field1 BIGINT NULL, field2 BIGINT NULL)",
        )
        .await?;

        let sql = "INSERT INTO table_without_values SELECT
                SUM(c4) OVER(PARTITION BY c1 ORDER BY c9 ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING),
                COUNT(*) OVER(PARTITION BY c1 ORDER BY c9 ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING)
                FROM aggregate_test_100
                ORDER by c1
            ";
        let msg = format!("Creating logical plan for '{sql}'");
        let dataframe = ctx.sql(sql).await.expect(&msg);
        let physical_plan = dataframe.create_physical_plan().await?;
        let formatted = displayable(physical_plan.as_ref()).indent().to_string();
        let expected = {
            vec![
                "MemoryWriteExec: partitions=1, input_partition=1",
                "  ProjectionExec: expr=[SUM(aggregate_test_100.c4) PARTITION BY [aggregate_test_100.c1] ORDER BY [aggregate_test_100.c9 ASC NULLS LAST] ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING@0 as field1, COUNT(UInt8(1)) PARTITION BY [aggregate_test_100.c1] ORDER BY [aggregate_test_100.c9 ASC NULLS LAST] ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING@1 as field2]",
                "    SortPreservingMergeExec: [c1@2 ASC NULLS LAST]",
                "      ProjectionExec: expr=[SUM(aggregate_test_100.c4) PARTITION BY [aggregate_test_100.c1] ORDER BY [aggregate_test_100.c9 ASC NULLS LAST] ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING@13 as SUM(aggregate_test_100.c4), COUNT(UInt8(1)) PARTITION BY [aggregate_test_100.c1] ORDER BY [aggregate_test_100.c9 ASC NULLS LAST] ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING@14 as COUNT(UInt8(1)), c1@0 as c1]",
                "        BoundedWindowAggExec: wdw=[SUM(aggregate_test_100.c4): Ok(Field { name: \"SUM(aggregate_test_100.c4)\", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(1)), end_bound: Following(UInt64(1)) }, COUNT(UInt8(1)): Ok(Field { name: \"COUNT(UInt8(1))\", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(1)), end_bound: Following(UInt64(1)) }]",
                "          SortExec: expr=[c1@0 ASC NULLS LAST,c9@8 ASC NULLS LAST]",
                "            CoalesceBatchesExec: target_batch_size=8192",
                "              RepartitionExec: partitioning=Hash([Column { name: \"c1\", index: 0 }], 8), input_partitions=8",
                "                RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            ]
        };

        let actual: Vec<&str> = formatted.trim().lines().collect();
        let actual_len = actual.len();
        let actual_trim_last = &actual[..actual_len - 1];
        assert_eq!(
            expected, actual_trim_last,
            "\n\nexpected:\n\n{expected:#?}\nactual:\n\n{actual:#?}\n\n"
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_insert_into_as_select_multi_partitioned() -> Result<()> {
        // Create session context
        let config = SessionConfig::new().with_target_partitions(8);
        let ctx = SessionContext::with_config(config);
        let testdata = test_util::arrow_test_data();
        let schema = test_util::aggr_test_schema();
        ctx.register_csv(
            "aggregate_test_100",
            &format!("{testdata}/csv/aggregate_test_100.csv"),
            CsvReadOptions::new().schema(&schema),
        )
        .await?;
        ctx.sql(
            "CREATE TABLE table_without_values(field1 BIGINT NULL, field2 BIGINT NULL)",
        )
        .await?;

        let sql = "INSERT INTO table_without_values SELECT
                SUM(c4) OVER(PARTITION BY c1 ORDER BY c9 ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) as a1,
                COUNT(*) OVER(PARTITION BY c1 ORDER BY c9 ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) as a2
                FROM aggregate_test_100";
        let msg = format!("Creating logical plan for '{sql}'");
        let dataframe = ctx.sql(sql).await.expect(&msg);
        let physical_plan = dataframe.create_physical_plan().await?;
        let formatted = displayable(physical_plan.as_ref()).indent().to_string();
        let expected = {
            vec![
                "MemoryWriteExec: partitions=1, input_partition=1",
                "  CoalescePartitionsExec",
                "    ProjectionExec: expr=[SUM(aggregate_test_100.c4) PARTITION BY [aggregate_test_100.c1] ORDER BY [aggregate_test_100.c9 ASC NULLS LAST] ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING@13 as field1, COUNT(UInt8(1)) PARTITION BY [aggregate_test_100.c1] ORDER BY [aggregate_test_100.c9 ASC NULLS LAST] ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING@14 as field2]",
                "      BoundedWindowAggExec: wdw=[SUM(aggregate_test_100.c4): Ok(Field { name: \"SUM(aggregate_test_100.c4)\", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(1)), end_bound: Following(UInt64(1)) }, COUNT(UInt8(1)): Ok(Field { name: \"COUNT(UInt8(1))\", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(1)), end_bound: Following(UInt64(1)) }]",
                "        SortExec: expr=[c1@0 ASC NULLS LAST,c9@8 ASC NULLS LAST]",
                "          CoalesceBatchesExec: target_batch_size=8192",
                "            RepartitionExec: partitioning=Hash([Column { name: \"c1\", index: 0 }], 8), input_partitions=8",
                "              RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            ]
        };

        let actual: Vec<&str> = formatted.trim().lines().collect();
        let actual_len = actual.len();
        let actual_trim_last = &actual[..actual_len - 1];
        assert_eq!(
            expected, actual_trim_last,
            "\n\nexpected:\n\n{expected:#?}\nactual:\n\n{actual:#?}\n\n"
        );
        Ok(())
    }

    // TODO: The generated plan is suboptimal since SortExec is in global state.
    #[tokio::test]
    async fn test_insert_into_as_select_single_partition() -> Result<()> {
        // Create session context
        let config = SessionConfig::new().with_target_partitions(8);
        let ctx = SessionContext::with_config(config);
        let testdata = test_util::arrow_test_data();
        let schema = test_util::aggr_test_schema();
        ctx.register_csv(
            "aggregate_test_100",
            &format!("{testdata}/csv/aggregate_test_100.csv"),
            CsvReadOptions::new().schema(&schema),
        )
        .await?;
        ctx.sql("CREATE TABLE table_without_values AS SELECT
                SUM(c4) OVER(PARTITION BY c1 ORDER BY c9 ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) as a1,
                COUNT(*) OVER(PARTITION BY c1 ORDER BY c9 ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) as a2
                FROM aggregate_test_100")
            .await?;

        let sql = "INSERT INTO table_without_values SELECT
                SUM(c4) OVER(PARTITION BY c1 ORDER BY c9 ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) as a1,
                COUNT(*) OVER(PARTITION BY c1 ORDER BY c9 ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) as a2
                FROM aggregate_test_100
                ORDER BY c1";
        let msg = format!("Creating logical plan for '{sql}'");
        let dataframe = ctx.sql(sql).await.expect(&msg);
        let physical_plan = dataframe.create_physical_plan().await?;
        let formatted = displayable(physical_plan.as_ref()).indent().to_string();
        let expected = {
            vec![
                "MemoryWriteExec: partitions=8, input_partition=8",
                "  RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
                "    ProjectionExec: expr=[a1@0 as a1, a2@1 as a2]",
                "      SortPreservingMergeExec: [c1@2 ASC NULLS LAST]",
                "        ProjectionExec: expr=[SUM(aggregate_test_100.c4) PARTITION BY [aggregate_test_100.c1] ORDER BY [aggregate_test_100.c9 ASC NULLS LAST] ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING@13 as a1, COUNT(UInt8(1)) PARTITION BY [aggregate_test_100.c1] ORDER BY [aggregate_test_100.c9 ASC NULLS LAST] ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING@14 as a2, c1@0 as c1]",
                "          BoundedWindowAggExec: wdw=[SUM(aggregate_test_100.c4): Ok(Field { name: \"SUM(aggregate_test_100.c4)\", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(1)), end_bound: Following(UInt64(1)) }, COUNT(UInt8(1)): Ok(Field { name: \"COUNT(UInt8(1))\", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(1)), end_bound: Following(UInt64(1)) }]",
                "            SortExec: expr=[c1@0 ASC NULLS LAST,c9@8 ASC NULLS LAST]",
                "              CoalesceBatchesExec: target_batch_size=8192",
                "                RepartitionExec: partitioning=Hash([Column { name: \"c1\", index: 0 }], 8), input_partitions=8",
                "                  RepartitionExec: partitioning=RoundRobinBatch(8), input_partitions=1",
            ]
        };

        let actual: Vec<&str> = formatted.trim().lines().collect();
        let actual_len = actual.len();
        let actual_trim_last = &actual[..actual_len - 1];
        assert_eq!(
            expected, actual_trim_last,
            "\n\nexpected:\n\n{expected:#?}\nactual:\n\n{actual:#?}\n\n"
        );
        Ok(())
    }

    // DummyPartition is a simple implementation of the PartitionStream trait.
    // It produces a stream of record batches with a fixed schema and the same content.
    struct DummyPartition {
        schema: SchemaRef,
        batch: RecordBatch,
        num_batches: usize,
    }

    impl PartitionStream for DummyPartition {
        // Return a reference to the schema of this partition.
        fn schema(&self) -> &SchemaRef {
            &self.schema
        }

        // Execute the partition stream, producing a stream of record batches.
        fn execute(&self, _ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
            let batches = itertools::repeat_n(self.batch.clone(), self.num_batches);
            Box::pin(RecordBatchStreamAdapter::new(
                self.schema.clone(),
                futures::stream::iter(batches).map(Ok),
            ))
        }
    }

    // Test the less-lock mode by inserting a large number of batches into a table.
    #[tokio::test]
    async fn test_one_to_one_mode() -> Result<()> {
        let num_batches = 10000;
        // Create a new session context
        let session_ctx = SessionContext::new();
        // Create a new schema with one field called "a" of type Int32
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));

        // Create a new batch of data to insert into the table
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from_slice([1, 2, 3]))],
        )?;
        let initial_table = Arc::new(MemTable::try_new(schema.clone(), vec![vec![]])?);

        let single_partition = Arc::new(DummyPartition {
            schema: schema.clone(),
            batch,
            num_batches,
        });
        let input = Arc::new(StreamingTableExec::try_new(
            schema.clone(),
            vec![single_partition],
            None,
            false,
        )?);
        let plan = initial_table
            .insert_into(&session_ctx.state(), input)
            .await?;
        let res = collect(plan, session_ctx.task_ctx()).await?;
        assert!(res.is_empty());
        // Ensure that the table now contains two batches of data in the same partition
        assert_eq!(initial_table.batches[0].read().await.len(), num_batches);
        Ok(())
    }

    // Test the locked mode by inserting a large number of batches into a table. It tests
    // where the table partition count is not equal to the input's output partition count.
    #[tokio::test]
    async fn test_locked_mode() -> Result<()> {
        let num_batches = 10000;
        // Create a new session context
        let session_ctx = SessionContext::new();
        // Create a new schema with one field called "a" of type Int32
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));

        // Create a new batch of data to insert into the table
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from_slice([1, 2, 3]))],
        )?;
        let initial_table = Arc::new(MemTable::try_new(schema.clone(), vec![vec![]])?);

        let single_partition = Arc::new(DummyPartition {
            schema: schema.clone(),
            batch,
            num_batches,
        });
        let input = Arc::new(StreamingTableExec::try_new(
            schema.clone(),
            vec![
                single_partition.clone(),
                single_partition.clone(),
                single_partition,
            ],
            None,
            false,
        )?);
        let plan = initial_table
            .insert_into(&session_ctx.state(), input)
            .await?;
        let res = collect(plan, session_ctx.task_ctx()).await?;
        assert!(res.is_empty());
        // Ensure that the table now contains two batches of data in the same partition
        assert_eq!(initial_table.batches[0].read().await.len(), num_batches * 3);
        Ok(())
    }
}
