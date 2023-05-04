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

//! Execution plan for reading in-memory batches of data

use super::expressions::PhysicalSortExpr;
use super::{
    common, project_schema, DisplayFormatType, ExecutionPlan, Partitioning,
    RecordBatchStream, SendableRecordBatchStream, Statistics,
};
use crate::error::Result;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use core::fmt;
use futures::StreamExt;
use std::any::Any;
use std::sync::Arc;
use std::task::{Context, Poll};

use crate::datasource::memory::PartitionData;
use crate::execution::context::TaskContext;
use crate::physical_plan::stream::RecordBatchStreamAdapter;
use crate::physical_plan::Distribution;
use datafusion_common::DataFusionError;
use futures::Stream;
use tokio::sync::RwLock;

/// Execution plan for reading in-memory batches of data
pub struct MemoryExec {
    /// The partitions to query
    partitions: Vec<Vec<RecordBatch>>,
    /// Schema representing the data before projection
    schema: SchemaRef,
    /// Schema representing the data after the optional projection is applied
    projected_schema: SchemaRef,
    /// Optional projection
    projection: Option<Vec<usize>>,
    // Optional sort information
    sort_information: Option<Vec<PhysicalSortExpr>>,
}

impl fmt::Debug for MemoryExec {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "partitions: [...]")?;
        write!(f, "schema: {:?}", self.projected_schema)?;
        write!(f, "projection: {:?}", self.projection)
    }
}

impl ExecutionPlan for MemoryExec {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    /// Get the schema for this execution plan
    fn schema(&self) -> SchemaRef {
        self.projected_schema.clone()
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        // this is a leaf node and has no children
        vec![]
    }

    /// Get the output partitioning of this plan
    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(self.partitions.len())
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        self.sort_information.as_deref()
    }

    fn with_new_children(
        self: Arc<Self>,
        _: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Err(DataFusionError::Internal(format!(
            "Children cannot be replaced in {self:?}"
        )))
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        Ok(Box::pin(MemoryStream::try_new(
            self.partitions[partition].clone(),
            self.projected_schema.clone(),
            self.projection.clone(),
        )?))
    }

    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default => {
                let partitions: Vec<_> =
                    self.partitions.iter().map(|b| b.len()).collect();
                write!(
                    f,
                    "MemoryExec: partitions={}, partition_sizes={:?}",
                    partitions.len(),
                    partitions
                )
            }
        }
    }

    /// We recompute the statistics dynamically from the arrow metadata as it is pretty cheap to do so
    fn statistics(&self) -> Statistics {
        common::compute_record_batch_statistics(
            &self.partitions,
            &self.schema,
            self.projection.clone(),
        )
    }
}

impl MemoryExec {
    /// Create a new execution plan for reading in-memory record batches
    /// The provided `schema` should not have the projection applied.
    pub fn try_new(
        partitions: &[Vec<RecordBatch>],
        schema: SchemaRef,
        projection: Option<Vec<usize>>,
    ) -> Result<Self> {
        let projected_schema = project_schema(&schema, projection.as_ref())?;
        Ok(Self {
            partitions: partitions.to_vec(),
            schema,
            projected_schema,
            projection,
            sort_information: None,
        })
    }

    /// Create a new execution plan for reading in-memory record batches
    /// The provided `schema` should not have the projection applied.
    pub fn try_new_owned_data(
        partitions: Vec<Vec<RecordBatch>>,
        schema: SchemaRef,
        projection: Option<Vec<usize>>,
    ) -> Result<Self> {
        let projected_schema = project_schema(&schema, projection.as_ref())?;
        Ok(Self {
            partitions,
            schema,
            projected_schema,
            projection,
            sort_information: None,
        })
    }

    /// Set sort information
    pub fn with_sort_information(
        mut self,
        sort_information: Vec<PhysicalSortExpr>,
    ) -> Self {
        self.sort_information = Some(sort_information);
        self
    }
}

/// Iterator over batches
pub struct MemoryStream {
    /// Vector of record batches
    data: Vec<RecordBatch>,
    /// Schema representing the data
    schema: SchemaRef,
    /// Optional projection for which columns to load
    projection: Option<Vec<usize>>,
    /// Index into the data
    index: usize,
}

impl MemoryStream {
    /// Create an iterator for a vector of record batches
    pub fn try_new(
        data: Vec<RecordBatch>,
        schema: SchemaRef,
        projection: Option<Vec<usize>>,
    ) -> Result<Self> {
        Ok(Self {
            data,
            schema,
            projection,
            index: 0,
        })
    }
}

impl Stream for MemoryStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        Poll::Ready(if self.index < self.data.len() {
            self.index += 1;
            let batch = &self.data[self.index - 1];

            // return just the columns requested
            let batch = match self.projection.as_ref() {
                Some(columns) => batch.project(columns)?,
                None => batch.clone(),
            };

            Some(Ok(batch))
        } else {
            None
        })
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.data.len(), Some(self.data.len()))
    }
}

impl RecordBatchStream for MemoryStream {
    /// Get the schema
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

/// Execution plan for writing record batches to an in-memory table.
pub struct MemoryWriteExec {
    /// Input plan that produces the record batches to be written.
    input: Arc<dyn ExecutionPlan>,
    /// Reference to the MemTable's partition data.
    batches: Vec<PartitionData>,
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
        let schema = self.schema.clone();
        let state = (data, self.batches[partition % batch_count].clone());

        let stream = futures::stream::unfold(state, |mut state| async move {
            // hold lock during the entire write
            let mut locked = state.1.write().await;
            loop {
                let batch = match state.0.next().await {
                    Some(Ok(batch)) => batch,
                    Some(Err(e)) => {
                        drop(locked);
                        return Some((Err(e), state));
                    }
                    None => return None,
                };
                locked.push(batch)
            }
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
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
        batches: Vec<Arc<RwLock<Vec<RecordBatch>>>>,
        schema: SchemaRef,
    ) -> Result<Self> {
        Ok(Self {
            input: plan,
            batches,
            schema,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::datasource::streaming::PartitionStream;
    use crate::datasource::{MemTable, TableProvider};
    use crate::from_slice::FromSlice;
    use crate::physical_plan::stream::RecordBatchStreamAdapter;
    use crate::physical_plan::streaming::StreamingTableExec;
    use crate::physical_plan::ColumnStatistics;
    use crate::physical_plan::{collect, displayable, SendableRecordBatchStream};
    use crate::prelude::{CsvReadOptions, SessionContext};
    use crate::test_util;
    use arrow::array::Int32Array;
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use arrow::record_batch::RecordBatch;
    use datafusion_common::Result;
    use datafusion_execution::config::SessionConfig;
    use datafusion_execution::TaskContext;
    use futures::StreamExt;
    use std::sync::Arc;

    fn mock_data() -> Result<(SchemaRef, RecordBatch)> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
            Field::new("c", DataType::Int32, true),
            Field::new("d", DataType::Int32, false),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from_slice([1, 2, 3])),
                Arc::new(Int32Array::from_slice([4, 5, 6])),
                Arc::new(Int32Array::from(vec![None, None, Some(9)])),
                Arc::new(Int32Array::from_slice([7, 8, 9])),
            ],
        )?;

        Ok((schema, batch))
    }

    #[tokio::test]
    async fn test_with_projection() -> Result<()> {
        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
        let (schema, batch) = mock_data()?;

        let executor = MemoryExec::try_new(&[vec![batch]], schema, Some(vec![2, 1]))?;
        let statistics = executor.statistics();

        assert_eq!(statistics.num_rows, Some(3));
        assert_eq!(
            statistics.column_statistics,
            Some(vec![
                ColumnStatistics {
                    null_count: Some(2),
                    max_value: None,
                    min_value: None,
                    distinct_count: None,
                },
                ColumnStatistics {
                    null_count: Some(0),
                    max_value: None,
                    min_value: None,
                    distinct_count: None,
                },
            ])
        );

        // scan with projection
        let mut it = executor.execute(0, task_ctx)?;
        let batch2 = it.next().await.unwrap()?;
        assert_eq!(2, batch2.schema().fields().len());
        assert_eq!("c", batch2.schema().field(0).name());
        assert_eq!("b", batch2.schema().field(1).name());
        assert_eq!(2, batch2.num_columns());

        Ok(())
    }

    #[tokio::test]
    async fn test_without_projection() -> Result<()> {
        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
        let (schema, batch) = mock_data()?;

        let executor = MemoryExec::try_new(&[vec![batch]], schema, None)?;
        let statistics = executor.statistics();

        assert_eq!(statistics.num_rows, Some(3));
        assert_eq!(
            statistics.column_statistics,
            Some(vec![
                ColumnStatistics {
                    null_count: Some(0),
                    max_value: None,
                    min_value: None,
                    distinct_count: None,
                },
                ColumnStatistics {
                    null_count: Some(0),
                    max_value: None,
                    min_value: None,
                    distinct_count: None,
                },
                ColumnStatistics {
                    null_count: Some(2),
                    max_value: None,
                    min_value: None,
                    distinct_count: None,
                },
                ColumnStatistics {
                    null_count: Some(0),
                    max_value: None,
                    min_value: None,
                    distinct_count: None,
                },
            ])
        );

        let mut it = executor.execute(0, task_ctx)?;
        let batch1 = it.next().await.unwrap()?;
        assert_eq!(4, batch1.schema().fields().len());
        assert_eq!(4, batch1.num_columns());

        Ok(())
    }

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
                "      ProjectionExec: expr=[SUM(aggregate_test_100.c4) PARTITION BY [aggregate_test_100.c1] ORDER BY [aggregate_test_100.c9 ASC NULLS LAST] ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING@3 as SUM(aggregate_test_100.c4), COUNT(UInt8(1)) PARTITION BY [aggregate_test_100.c1] ORDER BY [aggregate_test_100.c9 ASC NULLS LAST] ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING@4 as COUNT(UInt8(1)), c1@0 as c1]",
                "        BoundedWindowAggExec: wdw=[SUM(aggregate_test_100.c4): Ok(Field { name: \"SUM(aggregate_test_100.c4)\", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(1)), end_bound: Following(UInt64(1)) }, COUNT(UInt8(1)): Ok(Field { name: \"COUNT(UInt8(1))\", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(1)), end_bound: Following(UInt64(1)) }], mode=[Sorted]",
                "          SortExec: expr=[c1@0 ASC NULLS LAST,c9@2 ASC NULLS LAST]",
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
                "    ProjectionExec: expr=[SUM(aggregate_test_100.c4) PARTITION BY [aggregate_test_100.c1] ORDER BY [aggregate_test_100.c9 ASC NULLS LAST] ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING@3 as field1, COUNT(UInt8(1)) PARTITION BY [aggregate_test_100.c1] ORDER BY [aggregate_test_100.c9 ASC NULLS LAST] ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING@4 as field2]",
                "      BoundedWindowAggExec: wdw=[SUM(aggregate_test_100.c4): Ok(Field { name: \"SUM(aggregate_test_100.c4)\", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(1)), end_bound: Following(UInt64(1)) }, COUNT(UInt8(1)): Ok(Field { name: \"COUNT(UInt8(1))\", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(1)), end_bound: Following(UInt64(1)) }], mode=[Sorted]",
                "        SortExec: expr=[c1@0 ASC NULLS LAST,c9@2 ASC NULLS LAST]",
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
                "        ProjectionExec: expr=[SUM(aggregate_test_100.c4) PARTITION BY [aggregate_test_100.c1] ORDER BY [aggregate_test_100.c9 ASC NULLS LAST] ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING@3 as a1, COUNT(UInt8(1)) PARTITION BY [aggregate_test_100.c1] ORDER BY [aggregate_test_100.c9 ASC NULLS LAST] ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING@4 as a2, c1@0 as c1]",
                "          BoundedWindowAggExec: wdw=[SUM(aggregate_test_100.c4): Ok(Field { name: \"SUM(aggregate_test_100.c4)\", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(1)), end_bound: Following(UInt64(1)) }, COUNT(UInt8(1)): Ok(Field { name: \"COUNT(UInt8(1))\", data_type: Int64, nullable: true, dict_id: 0, dict_is_ordered: false, metadata: {} }), frame: WindowFrame { units: Rows, start_bound: Preceding(UInt64(1)), end_bound: Following(UInt64(1)) }], mode=[Sorted]",
                "            SortExec: expr=[c1@0 ASC NULLS LAST,c9@2 ASC NULLS LAST]",
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
