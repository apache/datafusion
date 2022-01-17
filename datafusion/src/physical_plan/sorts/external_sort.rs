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
    BaselineMetrics, Count, ExecutionPlanMetricsSet, MetricsSet, Time,
};
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
use std::time::Duration;
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
///         let ordered_stream = sort_preserving_merge(in_mem_batches.drain(..));
///         let tmp_file = spill_write(ordered_stream);
///         spills.push(tmp_file);
///     }
///     // sort the batch while it's probably still in cache and buffer it.
///     let sorted = sort_by_key(batch);
///     in_mem_batches.push(sorted);
/// }
///
/// let partial_ordered_streams = vec![];
/// let in_mem_stream = sort_preserving_merge(in_mem_batches.drain(..));
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
    metrics: AggregatedMetricsSet,
    inner_metrics: BaselineMetrics,
    used: AtomicUsize,
    spilled_bytes: AtomicUsize,
    spilled_count: AtomicUsize,
}

impl ExternalSorter {
    pub fn new(
        partition_id: usize,
        schema: SchemaRef,
        expr: Vec<PhysicalSortExpr>,
        metrics: AggregatedMetricsSet,
        runtime: Arc<RuntimeEnv>,
    ) -> Self {
        let inner_metrics = metrics.new_intermediate_baseline(partition_id);
        Self {
            id: MemoryConsumerId::new(partition_id),
            schema,
            in_mem_batches: Mutex::new(vec![]),
            spills: Mutex::new(vec![]),
            expr,
            runtime,
            metrics,
            inner_metrics,
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
        let elapsed_compute = self.inner_metrics.elapsed_compute().clone();
        let timer = elapsed_compute.timer();
        let sorted_batch = sort_batch(input, self.schema.clone(), &*self.expr)?;
        timer.done();
        let mut in_mem_batches = self.in_mem_batches.lock().await;
        in_mem_batches.push(sorted_batch);
        Ok(())
    }

    async fn spilled_before(&self) -> bool {
        let spills = self.spills.lock().await;
        !spills.is_empty()
    }

    /// MergeSort in mem batches as well as spills into total order with `SortPreservingMergeStream`.
    async fn sort(&self) -> Result<SendableRecordBatchStream> {
        let partition = self.partition_id();
        let mut in_mem_batches = self.in_mem_batches.lock().await;

        if self.spilled_before().await {
            let baseline_metrics = self.metrics.new_intermediate_baseline(partition);
            let mut streams: Vec<SortedStream> = vec![];
            let in_mem_stream = in_mem_partial_sort(
                &mut *in_mem_batches,
                self.schema.clone(),
                &self.expr,
                self.runtime.batch_size(),
                baseline_metrics,
                self.runtime.clone(),
            )
            .await?;
            streams.push(SortedStream::new(in_mem_stream, self.used()));

            let mut spills = self.spills.lock().await;

            for spill in spills.drain(..) {
                let stream = read_spill_as_stream(spill, self.schema.clone()).await?;
                streams.push(SortedStream::new(stream, 0));
            }
            let baseline_metrics = self.metrics.new_final_baseline(partition);
            Ok(Box::pin(
                SortPreservingMergeStream::new_from_streams(
                    streams,
                    self.schema.clone(),
                    &self.expr,
                    self.runtime.batch_size(),
                    baseline_metrics,
                    partition,
                    self.runtime.clone(),
                )
                .await,
            ))
        } else {
            let baseline_metrics = self.metrics.new_final_baseline(partition);
            in_mem_partial_sort(
                &mut *in_mem_batches,
                self.schema.clone(),
                &self.expr,
                self.runtime.batch_size(),
                baseline_metrics,
                self.runtime.clone(),
            )
            .await
        }
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

        let baseline_metrics = self.metrics.new_intermediate_baseline(partition);

        let path = self.runtime.disk_manager.create_tmp_file()?;
        let stream = in_mem_partial_sort(
            &mut *in_mem_batches,
            self.schema.clone(),
            &*self.expr,
            self.runtime.batch_size(),
            baseline_metrics,
            self.runtime.clone(),
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
    runtime: Arc<RuntimeEnv>,
) -> Result<SendableRecordBatchStream> {
    if sorted_bathes.len() == 1 {
        Ok(Box::pin(SizedRecordBatchStream::new(
            schema,
            vec![Arc::new(sorted_bathes.pop().unwrap())],
            baseline_metrics,
        )))
    } else {
        let batches = sorted_bathes.drain(..).collect();
        assert_eq!(sorted_bathes.len(), 0);
        Ok(Box::pin(
            SortPreservingMergeStream::new_from_batches(
                batches,
                schema,
                expressions,
                target_batch_size,
                baseline_metrics,
                runtime,
            )
            .await,
        ))
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
    /// Containing all metrics set created for sort, such as all sets for `sort_merge_join`s
    all_metrics: AggregatedMetricsSet,
    /// Preserve partitions of input plan
    preserve_partitioning: bool,
}

#[derive(Debug, Clone)]
struct AggregatedMetricsSet {
    intermediate: Arc<std::sync::Mutex<Vec<ExecutionPlanMetricsSet>>>,
    final_: Arc<std::sync::Mutex<Vec<ExecutionPlanMetricsSet>>>,
}

impl AggregatedMetricsSet {
    fn new() -> Self {
        Self {
            intermediate: Arc::new(std::sync::Mutex::new(vec![])),
            final_: Arc::new(std::sync::Mutex::new(vec![])),
        }
    }

    fn new_intermediate_baseline(&self, partition: usize) -> BaselineMetrics {
        let ms = ExecutionPlanMetricsSet::new();
        let result = BaselineMetrics::new(&ms, partition);
        self.intermediate.lock().unwrap().push(ms);
        result
    }

    fn new_final_baseline(&self, partition: usize) -> BaselineMetrics {
        let ms = ExecutionPlanMetricsSet::new();
        let result = BaselineMetrics::new(&ms, partition);
        self.final_.lock().unwrap().push(ms);
        result
    }

    fn merge_compute_time(&self, dest: &Time) {
        let time1 = self
            .intermediate
            .lock()
            .unwrap()
            .iter()
            .map(|es| {
                es.clone_inner()
                    .elapsed_compute()
                    .map_or(0u64, |v| v as u64)
            })
            .sum();
        let time2 = self
            .final_
            .lock()
            .unwrap()
            .iter()
            .map(|es| {
                es.clone_inner()
                    .elapsed_compute()
                    .map_or(0u64, |v| v as u64)
            })
            .sum();
        dest.add_duration(Duration::from_nanos(time1));
        dest.add_duration(Duration::from_nanos(time2));
    }

    fn merge_output_count(&self, dest: &Count) {
        let count = self
            .final_
            .lock()
            .unwrap()
            .iter()
            .map(|es| es.clone_inner().output_rows().map_or(0, |v| v))
            .sum();
        dest.add(count);
    }
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
            all_metrics: AggregatedMetricsSet::new(),
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

        let input = self.input.execute(partition, runtime.clone()).await?;

        external_sort(
            input,
            partition,
            self.expr.clone(),
            self.all_metrics.clone(),
            runtime,
        )
        .await
    }

    fn metrics(&self) -> Option<MetricsSet> {
        let metrics = ExecutionPlanMetricsSet::new();
        let baseline = BaselineMetrics::new(&metrics, 0);
        self.all_metrics
            .merge_compute_time(baseline.elapsed_compute());
        self.all_metrics.merge_output_count(baseline.output_rows());
        Some(metrics.clone_inner())
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
    metrics: AggregatedMetricsSet,
    runtime: Arc<RuntimeEnv>,
) -> Result<SendableRecordBatchStream> {
    let schema = input.schema();
    let sorter = Arc::new(ExternalSorter::new(
        partition_id,
        schema.clone(),
        expr,
        metrics,
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
    use crate::physical_plan::memory::MemoryExec;
    use crate::physical_plan::{
        collect,
        file_format::{CsvExec, PhysicalPlanConfig},
    };
    use crate::test;
    use crate::test::assert_is_pending;
    use crate::test::exec::{assert_strong_count_converges_to_zero, BlockingExec};
    use crate::test_util;
    use arrow::array::*;
    use arrow::compute::SortOptions;
    use arrow::datatypes::*;
    use futures::FutureExt;
    use std::collections::{BTreeMap, HashMap};

    async fn sort_with_runtime(runtime: Arc<RuntimeEnv>) -> Result<Vec<RecordBatch>> {
        let schema = test_util::aggr_test_schema();
        let partitions = 4;
        let (_, files) =
            test::create_partitioned_csv("aggregate_test_100.csv", partitions)?;

        let csv = CsvExec::new(
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

    #[tokio::test]
    async fn test_sort_metadata() -> Result<()> {
        let runtime = Arc::new(RuntimeEnv::default());
        let field_metadata: BTreeMap<String, String> =
            vec![("foo".to_string(), "bar".to_string())]
                .into_iter()
                .collect();
        let schema_metadata: HashMap<String, String> =
            vec![("baz".to_string(), "barf".to_string())]
                .into_iter()
                .collect();

        let mut field = Field::new("field_name", DataType::UInt64, true);
        field.set_metadata(Some(field_metadata.clone()));
        let schema = Schema::new_with_metadata(vec![field], schema_metadata.clone());
        let schema = Arc::new(schema);

        let data: ArrayRef =
            Arc::new(vec![3, 2, 1].into_iter().map(Some).collect::<UInt64Array>());

        let batch = RecordBatch::try_new(schema.clone(), vec![data]).unwrap();
        let input =
            Arc::new(MemoryExec::try_new(&[vec![batch]], schema.clone(), None).unwrap());

        let sort_exec = Arc::new(ExternalSortExec::try_new(
            vec![PhysicalSortExpr {
                expr: col("field_name", &schema)?,
                options: SortOptions::default(),
            }],
            input,
        )?);

        let result: Vec<RecordBatch> = collect(sort_exec, runtime).await?;

        let expected_data: ArrayRef =
            Arc::new(vec![1, 2, 3].into_iter().map(Some).collect::<UInt64Array>());
        let expected_batch =
            RecordBatch::try_new(schema.clone(), vec![expected_data]).unwrap();

        // Data is correct
        assert_eq!(&vec![expected_batch], &result);

        // explicitlty ensure the metadata is present
        assert_eq!(
            result[0].schema().fields()[0].metadata(),
            &Some(field_metadata)
        );
        assert_eq!(result[0].schema().metadata(), &schema_metadata);

        Ok(())
    }

    #[tokio::test]
    async fn test_lex_sort_by_float() -> Result<()> {
        let runtime = Arc::new(RuntimeEnv::default());
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Float32, true),
            Field::new("b", DataType::Float64, true),
        ]));

        // define data.
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Float32Array::from(vec![
                    Some(f32::NAN),
                    None,
                    None,
                    Some(f32::NAN),
                    Some(1.0_f32),
                    Some(1.0_f32),
                    Some(2.0_f32),
                    Some(3.0_f32),
                ])),
                Arc::new(Float64Array::from(vec![
                    Some(200.0_f64),
                    Some(20.0_f64),
                    Some(10.0_f64),
                    Some(100.0_f64),
                    Some(f64::NAN),
                    None,
                    None,
                    Some(f64::NAN),
                ])),
            ],
        )?;

        let sort_exec = Arc::new(ExternalSortExec::try_new(
            vec![
                PhysicalSortExpr {
                    expr: col("a", &schema)?,
                    options: SortOptions {
                        descending: true,
                        nulls_first: true,
                    },
                },
                PhysicalSortExpr {
                    expr: col("b", &schema)?,
                    options: SortOptions {
                        descending: false,
                        nulls_first: false,
                    },
                },
            ],
            Arc::new(MemoryExec::try_new(&[vec![batch]], schema, None)?),
        )?);

        assert_eq!(DataType::Float32, *sort_exec.schema().field(0).data_type());
        assert_eq!(DataType::Float64, *sort_exec.schema().field(1).data_type());

        let result: Vec<RecordBatch> = collect(sort_exec.clone(), runtime).await?;
        let metrics = sort_exec.metrics().unwrap();
        assert!(metrics.elapsed_compute().unwrap() > 0);
        assert_eq!(metrics.output_rows().unwrap(), 8);
        assert_eq!(result.len(), 1);

        let columns = result[0].columns();

        assert_eq!(DataType::Float32, *columns[0].data_type());
        assert_eq!(DataType::Float64, *columns[1].data_type());

        let a = as_primitive_array::<Float32Type>(&columns[0]);
        let b = as_primitive_array::<Float64Type>(&columns[1]);

        // convert result to strings to allow comparing to expected result containing NaN
        let result: Vec<(Option<String>, Option<String>)> = (0..result[0].num_rows())
            .map(|i| {
                let aval = if a.is_valid(i) {
                    Some(a.value(i).to_string())
                } else {
                    None
                };
                let bval = if b.is_valid(i) {
                    Some(b.value(i).to_string())
                } else {
                    None
                };
                (aval, bval)
            })
            .collect();

        let expected: Vec<(Option<String>, Option<String>)> = vec![
            (None, Some("10".to_owned())),
            (None, Some("20".to_owned())),
            (Some("NaN".to_owned()), Some("100".to_owned())),
            (Some("NaN".to_owned()), Some("200".to_owned())),
            (Some("3".to_owned()), Some("NaN".to_owned())),
            (Some("2".to_owned()), None),
            (Some("1".to_owned()), Some("NaN".to_owned())),
            (Some("1".to_owned()), None),
        ];

        assert_eq!(expected, result);

        Ok(())
    }

    #[tokio::test]
    async fn test_drop_cancel() -> Result<()> {
        let runtime = Arc::new(RuntimeEnv::default());
        let schema =
            Arc::new(Schema::new(vec![Field::new("a", DataType::Float32, true)]));

        let blocking_exec = Arc::new(BlockingExec::new(Arc::clone(&schema), 1));
        let refs = blocking_exec.refs();
        let sort_exec = Arc::new(ExternalSortExec::try_new(
            vec![PhysicalSortExpr {
                expr: col("a", &schema)?,
                options: SortOptions::default(),
            }],
            blocking_exec,
        )?);

        let fut = collect(sort_exec, runtime);
        let mut fut = fut.boxed();

        assert_is_pending(&mut fut);
        drop(fut);
        assert_strong_count_converges_to_zero(refs).await;

        Ok(())
    }
}
