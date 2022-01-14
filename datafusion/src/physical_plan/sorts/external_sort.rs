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

//! Defines the External-Sort plan

use crate::error::{DataFusionError, Result};
use crate::execution::memory_manager::{
    ConsumerType, MemoryConsumer, MemoryConsumerId, MemoryManager,
};
use crate::execution::runtime_env::RuntimeEnv;
use crate::physical_plan::common::{batch_byte_size, IPCWriter, SizedRecordBatchStream};
use crate::physical_plan::expressions::PhysicalSortExpr;
use crate::physical_plan::metrics::{
    BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet,
};
use crate::physical_plan::sorts::in_mem_sort::InMemSortStream;
use crate::physical_plan::sorts::sort::sort_batch;
use crate::physical_plan::sorts::sort_preserving_merge::SortPreservingMergeStream;
use crate::physical_plan::sorts::SortedStream;
use crate::physical_plan::stream::RecordBatchReceiverStream;
use crate::physical_plan::{
    DisplayFormatType, Distribution, ExecutionPlan, Partitioning,
    SendableRecordBatchStream, Statistics,
};
use arrow::datatypes::SchemaRef;
use arrow::error::Result as ArrowResult;
use arrow::ipc::reader::FileReader;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use futures::lock::Mutex;
use futures::StreamExt;
use log::{error, info};
use std::any::Any;
use std::fmt;
use std::fmt::{Debug, Formatter};
use std::fs::File;
use std::io::BufReader;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use tokio::sync::mpsc::{Receiver as TKReceiver, Sender as TKSender};
use tokio::task;

/// Sort arbitrary size of data to get an total order (may spill several times during sorting based on free memory available).
///
/// The basic architecture of the algorithm:
///
/// let spills = vec![];
/// let in_mem_batches = vec![];
/// while (input.has_next()) {
///     let batch = input.next();
///     // no enough memory available, spill first.
///     if exec_memory_available < size_of(batch) {
///         let ordered_stream = in_mem_heap_sort(in_mem_batches.drain(..));
///         let tmp_file = spill_write(ordered_stream);
///         spills.push(tmp_file);
///     }
///     // sort the batch while it's probably still in cache and buffer it.
///     let sorted = sort_by_key(batch);
///     in_mem_batches.push(sorted);
/// }
///
/// let partial_ordered_streams = vec![];
/// let in_mem_stream = in_mem_heap_sort(in_mem_batches.drain(..));
/// partial_ordered_streams.push(in_mem_stream);
/// partial_ordered_streams.extend(spills.drain(..).map(read_as_stream));
/// let result = sort_preserving_merge(partial_ordered_streams);
struct ExternalSorter {
    id: MemoryConsumerId,
    schema: SchemaRef,
    in_mem_batches: Mutex<Vec<RecordBatch>>,
    spills: Mutex<Vec<String>>,
    /// Sort expressions
    expr: Vec<PhysicalSortExpr>,
    runtime: Arc<RuntimeEnv>,
    metrics: ExecutionPlanMetricsSet,
    used: AtomicUsize,
    spilled_bytes: AtomicUsize,
    spilled_count: AtomicUsize,
}

impl ExternalSorter {
    pub fn new(
        partition_id: usize,
        schema: SchemaRef,
        expr: Vec<PhysicalSortExpr>,
        runtime: Arc<RuntimeEnv>,
    ) -> Self {
        Self {
            id: MemoryConsumerId::new(partition_id),
            schema,
            in_mem_batches: Mutex::new(vec![]),
            spills: Mutex::new(vec![]),
            expr,
            runtime,
            metrics: ExecutionPlanMetricsSet::new(),
            used: AtomicUsize::new(0),
            spilled_bytes: AtomicUsize::new(0),
            spilled_count: AtomicUsize::new(0),
        }
    }

    async fn insert_batch(&self, input: RecordBatch) -> Result<()> {
        let size = batch_byte_size(&input);
        self.try_grow(size).await?;
        self.used.fetch_add(size, Ordering::SeqCst);
        // sort each batch as it's inserted, more probably to be cache-resident
        let sorted_batch = sort_batch(input, self.schema.clone(), &*self.expr)?;
        let mut in_mem_batches = self.in_mem_batches.lock().await;
        in_mem_batches.push(sorted_batch);
        Ok(())
    }

    /// MergeSort in mem batches as well as spills into total order with `SortPreservingMergeStream`.
    async fn sort(&self) -> Result<SendableRecordBatchStream> {
        let partition = self.partition_id();
        let mut in_mem_batches = self.in_mem_batches.lock().await;
        let baseline_metrics = BaselineMetrics::new(&self.metrics, partition);
        let mut streams: Vec<SortedStream> = vec![];
        let in_mem_stream = in_mem_partial_sort(
            &mut *in_mem_batches,
            self.schema.clone(),
            &self.expr,
            self.runtime.batch_size(),
            baseline_metrics,
        )
        .await?;
        streams.push(SortedStream::new(in_mem_stream, self.used()));

        let mut spills = self.spills.lock().await;

        for spill in spills.drain(..) {
            let stream = read_spill_as_stream(spill, self.schema.clone()).await?;
            streams.push(SortedStream::new(stream, 0));
        }
        let baseline_metrics = BaselineMetrics::new(&self.metrics, partition);

        Ok(Box::pin(
            SortPreservingMergeStream::new_from_stream(
                streams,
                self.schema.clone(),
                &self.expr,
                baseline_metrics,
                partition,
                self.runtime.clone(),
            )
            .await,
        ))
    }

    fn used(&self) -> usize {
        self.used.load(Ordering::SeqCst)
    }

    fn spilled_bytes(&self) -> usize {
        self.spilled_bytes.load(Ordering::SeqCst)
    }

    fn spilled_count(&self) -> usize {
        self.spilled_count.load(Ordering::SeqCst)
    }
}

impl Debug for ExternalSorter {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        f.debug_struct("ExternalSorter")
            .field("id", &self.id())
            .field("memory_used", &self.used())
            .field("spilled_bytes", &self.spilled_bytes())
            .field("spilled_count", &self.spilled_count())
            .finish()
    }
}

#[async_trait]
impl MemoryConsumer for ExternalSorter {
    fn name(&self) -> String {
        "ExternalSorter".to_owned()
    }

    fn id(&self) -> &MemoryConsumerId {
        &self.id
    }

    fn memory_manager(&self) -> Arc<MemoryManager> {
        self.runtime.memory_manager.clone()
    }

    fn type_(&self) -> &ConsumerType {
        &ConsumerType::Requesting
    }

    async fn spill(&self) -> Result<usize> {
        info!(
            "{}[{}] spilling sort data of {} to disk while inserting ({} time(s) so far)",
            self.name(),
            self.id(),
            self.used(),
            self.spilled_count()
        );

        let partition = self.partition_id();
        let mut in_mem_batches = self.in_mem_batches.lock().await;
        // we could always get a chance to free some memory as long as we are holding some
        if in_mem_batches.len() == 0 {
            return Ok(0);
        }

        let baseline_metrics = BaselineMetrics::new(&self.metrics, partition);

        let path = self.runtime.disk_manager.create_tmp_file()?;
        let stream = in_mem_partial_sort(
            &mut *in_mem_batches,
            self.schema.clone(),
            &*self.expr,
            self.runtime.batch_size(),
            baseline_metrics,
        )
        .await;

        let total_size =
            spill_partial_sorted_stream(&mut stream?, path.clone(), self.schema.clone())
                .await?;

        let mut spills = self.spills.lock().await;
        let used = self.used.swap(0, Ordering::SeqCst);
        self.spilled_count.fetch_add(1, Ordering::SeqCst);
        self.spilled_bytes.fetch_add(total_size, Ordering::SeqCst);
        spills.push(path);
        Ok(used)
    }

    fn mem_used(&self) -> usize {
        self.used.load(Ordering::SeqCst)
    }
}

/// consume the `sorted_bathes` and do in_mem_sort
async fn in_mem_partial_sort(
    sorted_bathes: &mut Vec<RecordBatch>,
    schema: SchemaRef,
    expressions: &[PhysicalSortExpr],
    target_batch_size: usize,
    baseline_metrics: BaselineMetrics,
) -> Result<SendableRecordBatchStream> {
    if sorted_bathes.len() == 1 {
        Ok(Box::pin(SizedRecordBatchStream::new(
            schema,
            vec![Arc::new(sorted_bathes.pop().unwrap())],
        )))
    } else {
        let new = sorted_bathes.drain(..).collect();
        assert_eq!(sorted_bathes.len(), 0);
        Ok(Box::pin(InMemSortStream::new(
            new,
            schema,
            expressions,
            target_batch_size,
            baseline_metrics,
        )?))
    }
}

async fn spill_partial_sorted_stream(
    in_mem_stream: &mut SendableRecordBatchStream,
    path: String,
    schema: SchemaRef,
) -> Result<usize> {
    let (sender, receiver) = tokio::sync::mpsc::channel(2);
    while let Some(item) = in_mem_stream.next().await {
        sender.send(Some(item)).await.ok();
    }
    sender.send(None).await.ok();
    let path_clone = path.clone();
    let res =
        task::spawn_blocking(move || write_sorted(receiver, path_clone, schema)).await;
    match res {
        Ok(r) => r,
        Err(e) => Err(DataFusionError::Execution(format!(
            "Error occurred while spilling {}",
            e
        ))),
    }
}

async fn read_spill_as_stream(
    path: String,
    schema: SchemaRef,
) -> Result<SendableRecordBatchStream> {
    let (sender, receiver): (
        TKSender<ArrowResult<RecordBatch>>,
        TKReceiver<ArrowResult<RecordBatch>>,
    ) = tokio::sync::mpsc::channel(2);
    let path_clone = path.clone();
    let join_handle = task::spawn_blocking(move || {
        if let Err(e) = read_spill(sender, path_clone) {
            error!("Failure while reading spill file: {}. Error: {}", path, e);
        }
    });
    Ok(RecordBatchReceiverStream::create(
        &schema,
        receiver,
        join_handle,
    ))
}

fn write_sorted(
    mut receiver: TKReceiver<Option<ArrowResult<RecordBatch>>>,
    path: String,
    schema: SchemaRef,
) -> Result<usize> {
    let mut writer = IPCWriter::new(path.as_ref(), schema.as_ref())?;
    while let Some(Some(batch)) = receiver.blocking_recv() {
        writer.write(&batch?)?;
    }
    writer.finish()?;
    info!(
        "Spilled {} batches of total {} rows to disk, memory released {}",
        writer.num_batches, writer.num_rows, writer.num_bytes
    );
    Ok(writer.num_bytes as usize)
}

fn read_spill(sender: TKSender<ArrowResult<RecordBatch>>, path: String) -> Result<()> {
    let file = BufReader::new(File::open(&path)?);
    let reader = FileReader::try_new(file)?;
    for batch in reader {
        sender
            .blocking_send(batch)
            .map_err(|e| DataFusionError::Execution(format!("{}", e)))?;
    }
    Ok(())
}

/// External Sort execution plan
#[derive(Debug)]
pub struct ExternalSortExec {
    /// Input schema
    input: Arc<dyn ExecutionPlan>,
    /// Sort expressions
    expr: Vec<PhysicalSortExpr>,
    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,
    /// Preserve partitions of input plan
    preserve_partitioning: bool,
}

impl ExternalSortExec {
    /// Create a new sort execution plan
    pub fn try_new(
        expr: Vec<PhysicalSortExpr>,
        input: Arc<dyn ExecutionPlan>,
    ) -> Result<Self> {
        Ok(Self::new_with_partitioning(expr, input, false))
    }

    /// Create a new sort execution plan with the option to preserve
    /// the partitioning of the input plan
    pub fn new_with_partitioning(
        expr: Vec<PhysicalSortExpr>,
        input: Arc<dyn ExecutionPlan>,
        preserve_partitioning: bool,
    ) -> Self {
        Self {
            expr,
            input,
            metrics: ExecutionPlanMetricsSet::new(),
            preserve_partitioning,
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
impl ExecutionPlan for ExternalSortExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.input.schema()
    }

    /// Get the output partitioning of this plan
    fn output_partitioning(&self) -> Partitioning {
        if self.preserve_partitioning {
            self.input.output_partitioning()
        } else {
            Partitioning::UnknownPartitioning(1)
        }
    }

    fn required_child_distribution(&self) -> Distribution {
        if self.preserve_partitioning {
            Distribution::UnspecifiedDistribution
        } else {
            Distribution::SinglePartition
        }
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![self.input.clone()]
    }

    fn with_new_children(
        &self,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        match children.len() {
            1 => Ok(Arc::new(ExternalSortExec::try_new(
                self.expr.clone(),
                children[0].clone(),
            )?)),
            _ => Err(DataFusionError::Internal(
                "SortExec wrong number of children".to_string(),
            )),
        }
    }

    async fn execute(
        &self,
        partition: usize,
        runtime: Arc<RuntimeEnv>,
    ) -> Result<SendableRecordBatchStream> {
        if !self.preserve_partitioning {
            if 0 != partition {
                return Err(DataFusionError::Internal(format!(
                    "SortExec invalid partition {}",
                    partition
                )));
            }

            // sort needs to operate on a single partition currently
            if 1 != self.input.output_partitioning().partition_count() {
                return Err(DataFusionError::Internal(
                    "SortExec requires a single input partition".to_owned(),
                ));
            }
        }

        let _baseline_metrics = BaselineMetrics::new(&self.metrics, partition);
        let input = self.input.execute(partition, runtime.clone()).await?;

        external_sort(input, partition, self.expr.clone(), runtime).await
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default => {
                let expr: Vec<String> = self.expr.iter().map(|e| e.to_string()).collect();
                write!(f, "SortExec: [{}]", expr.join(","))
            }
        }
    }

    fn statistics(&self) -> Statistics {
        self.input.statistics()
    }
}

async fn external_sort(
    mut input: SendableRecordBatchStream,
    partition_id: usize,
    expr: Vec<PhysicalSortExpr>,
    runtime: Arc<RuntimeEnv>,
) -> Result<SendableRecordBatchStream> {
    let schema = input.schema();
    let sorter = Arc::new(ExternalSorter::new(
        partition_id,
        schema.clone(),
        expr,
        runtime.clone(),
    ));
    runtime.register_consumer(&(sorter.clone() as Arc<dyn MemoryConsumer>));

    while let Some(batch) = input.next().await {
        let batch = batch?;
        sorter.insert_batch(batch).await?;
    }

    let result = sorter.sort().await;
    runtime.drop_consumer(sorter.id());
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::datasource::object_store::local::LocalFileSystem;
    use crate::execution::runtime_env::RuntimeConfig;
    use crate::physical_plan::coalesce_partitions::CoalescePartitionsExec;
    use crate::physical_plan::expressions::col;
    use crate::physical_plan::{
        collect,
        file_format::{CsvExec, FileScanConfig},
    };
    use crate::test;
    use crate::test_util;
    use arrow::array::*;
    use arrow::compute::SortOptions;
    use arrow::datatypes::*;

    async fn sort_with_runtime(runtime: Arc<RuntimeEnv>) -> Result<Vec<RecordBatch>> {
        let schema = test_util::aggr_test_schema();
        let partitions = 4;
        let (_, files) =
            test::create_partitioned_csv("aggregate_test_100.csv", partitions)?;

        let csv = CsvExec::new(
            FileScanConfig {
                object_store: Arc::new(LocalFileSystem {}),
                file_schema: Arc::clone(&schema),
                file_groups: files,
                statistics: Statistics::default(),
                projection: None,
                limit: None,
                table_partition_cols: vec![],
            },
            true,
            b',',
        );

        let sort_exec = Arc::new(ExternalSortExec::try_new(
            vec![
                // c1 string column
                PhysicalSortExpr {
                    expr: col("c1", &schema)?,
                    options: SortOptions::default(),
                },
                // c2 uin32 column
                PhysicalSortExpr {
                    expr: col("c2", &schema)?,
                    options: SortOptions::default(),
                },
                // c7 uin8 column
                PhysicalSortExpr {
                    expr: col("c7", &schema)?,
                    options: SortOptions::default(),
                },
            ],
            Arc::new(CoalescePartitionsExec::new(Arc::new(csv))),
        )?);

        collect(sort_exec, runtime).await
    }

    #[tokio::test]
    async fn test_in_mem_sort() -> Result<()> {
        let runtime = Arc::new(RuntimeEnv::default());
        let result = sort_with_runtime(runtime).await?;

        assert_eq!(result.len(), 1);

        let columns = result[0].columns();

        let c1 = as_string_array(&columns[0]);
        assert_eq!(c1.value(0), "a");
        assert_eq!(c1.value(c1.len() - 1), "e");

        let c2 = as_primitive_array::<UInt32Type>(&columns[1]);
        assert_eq!(c2.value(0), 1);
        assert_eq!(c2.value(c2.len() - 1), 5,);

        let c7 = as_primitive_array::<UInt8Type>(&columns[6]);
        assert_eq!(c7.value(0), 15);
        assert_eq!(c7.value(c7.len() - 1), 254,);

        Ok(())
    }

    #[tokio::test]
    async fn test_sort_spill() -> Result<()> {
        let config = RuntimeConfig::new()
            .with_memory_fraction(1.0)
            // trigger spill there will be 4 batches with 5.5KB for each
            .with_max_execution_memory(12288);
        let runtime = Arc::new(RuntimeEnv::new(config)?);
        let result = sort_with_runtime(runtime).await?;

        assert_eq!(result.len(), 1);

        let columns = result[0].columns();

        let c1 = as_string_array(&columns[0]);
        assert_eq!(c1.value(0), "a");
        assert_eq!(c1.value(c1.len() - 1), "e");

        let c2 = as_primitive_array::<UInt32Type>(&columns[1]);
        assert_eq!(c2.value(0), 1);
        assert_eq!(c2.value(c2.len() - 1), 5,);

        let c7 = as_primitive_array::<UInt8Type>(&columns[6]);
        assert_eq!(c7.value(0), 15);
        assert_eq!(c7.value(c7.len() - 1), 254,);

        Ok(())
    }

    #[tokio::test]
    async fn test_multi_output_batch() -> Result<()> {
        let config = RuntimeConfig::new().with_batch_size(26);
        let runtime = Arc::new(RuntimeEnv::new(config)?);
        let result = sort_with_runtime(runtime).await?;

        assert_eq!(result.len(), 4);

        let columns_b1 = result[0].columns();
        let columns_b3 = result[3].columns();

        let c1 = as_string_array(&columns_b1[0]);
        let c13 = as_string_array(&columns_b3[0]);
        assert_eq!(c1.value(0), "a");
        assert_eq!(c13.value(c13.len() - 1), "e");

        let c2 = as_primitive_array::<UInt32Type>(&columns_b1[1]);
        let c23 = as_primitive_array::<UInt32Type>(&columns_b3[1]);
        assert_eq!(c2.value(0), 1);
        assert_eq!(c23.value(c23.len() - 1), 5,);

        let c7 = as_primitive_array::<UInt8Type>(&columns_b1[6]);
        let c73 = as_primitive_array::<UInt8Type>(&columns_b3[6]);
        assert_eq!(c7.value(0), 15);
        assert_eq!(c73.value(c73.len() - 1), 254,);

        Ok(())
    }
}
