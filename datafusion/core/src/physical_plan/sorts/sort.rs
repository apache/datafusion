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

//! Sort that deals with an arbitrary size of the input.
//! It will do in-memory sorting if it has enough memory budget
//! but spills to disk if needed.

use crate::error::{DataFusionError, Result};
use crate::execution::context::TaskContext;
use crate::execution::memory_pool::{
    human_readable_size, MemoryConsumer, MemoryReservation,
};
use crate::execution::runtime_env::RuntimeEnv;
use crate::physical_plan::common::{batch_byte_size, spawn_buffered, IPCWriter};
use crate::physical_plan::expressions::PhysicalSortExpr;
use crate::physical_plan::metrics::{
    BaselineMetrics, CompositeMetricsSet, MemTrackingMetrics, MetricsSet,
};
use crate::physical_plan::sorts::merge::streaming_merge;
use crate::physical_plan::stream::{RecordBatchReceiverStream, RecordBatchStreamAdapter};
use crate::physical_plan::{
    DisplayFormatType, Distribution, EmptyRecordBatchStream, ExecutionPlan, Partitioning,
    SendableRecordBatchStream, Statistics,
};
use crate::prelude::SessionConfig;
pub use arrow::compute::SortOptions;
use arrow::compute::{concat_batches, lexsort_to_indices, take};
use arrow::datatypes::SchemaRef;
use arrow::ipc::reader::FileReader;
use arrow::record_batch::RecordBatch;
use datafusion_physical_expr::EquivalenceProperties;
use futures::{StreamExt, TryStreamExt};
use log::{debug, error, trace};
use std::any::Any;
use std::fmt;
use std::fmt::{Debug, Formatter};
use std::fs::File;
use std::io::BufReader;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tempfile::NamedTempFile;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::task;

/// Sort arbitrary size of data to get a total order (may spill several times during sorting based on free memory available).
///
/// The basic architecture of the algorithm:
/// 1. get a non-empty new batch from input
/// 2. check with the memory manager if we could buffer the batch in memory
/// 2.1 if memory sufficient, then buffer batch in memory, go to 1.
/// 2.2 if the memory threshold is reached, sort all buffered batches and spill to file.
///     buffer the batch in memory, go to 1.
/// 3. when input is exhausted, merge all in memory batches and spills to get a total order.
struct ExternalSorter {
    schema: SchemaRef,
    in_mem_batches: Vec<RecordBatch>,
    in_mem_batches_sorted: bool,
    spills: Vec<NamedTempFile>,
    /// Sort expressions
    expr: Arc<[PhysicalSortExpr]>,
    session_config: Arc<SessionConfig>,
    runtime: Arc<RuntimeEnv>,
    metrics_set: CompositeMetricsSet,
    metrics: BaselineMetrics,
    fetch: Option<usize>,
    reservation: MemoryReservation,
    partition_id: usize,
}

impl ExternalSorter {
    pub fn new(
        partition_id: usize,
        schema: SchemaRef,
        expr: Vec<PhysicalSortExpr>,
        metrics_set: CompositeMetricsSet,
        session_config: Arc<SessionConfig>,
        runtime: Arc<RuntimeEnv>,
        fetch: Option<usize>,
    ) -> Self {
        let metrics = metrics_set.new_intermediate_baseline(partition_id);

        let reservation = MemoryConsumer::new(format!("ExternalSorter[{partition_id}]"))
            .with_can_spill(true)
            .register(&runtime.memory_pool);

        Self {
            schema,
            in_mem_batches: vec![],
            in_mem_batches_sorted: true,
            spills: vec![],
            expr: expr.into(),
            session_config,
            runtime,
            metrics_set,
            metrics,
            fetch,
            reservation,
            partition_id,
        }
    }

    /// Appends an unsorted [`RecordBatch`] to `in_mem_batches`
    ///
    /// Updates memory usage metrics, and possibly triggers spilling to disk
    async fn insert_batch(&mut self, input: RecordBatch) -> Result<()> {
        if input.num_rows() == 0 {
            return Ok(());
        }

        let size = batch_byte_size(&input);
        if self.reservation.try_grow(size).is_err() {
            let before = self.reservation.size();
            self.in_mem_sort().await?;
            // Sorting may have freed memory, especially if fetch is not `None`
            //
            // As such we check again, and if the memory usage has dropped by
            // a factor of 2, and we can allocate the necessary capacity,
            // we don't spill
            //
            // The factor of 2 aims to avoid a degenerate case where the
            // memory required for `fetch` is just under the memory available,
            // causing repeated resorting of data
            if self.reservation.size() > before / 2
                || self.reservation.try_grow(size).is_err()
            {
                self.spill().await?;
                self.reservation.try_grow(size)?
            }
        }
        self.metrics.mem_used().add(size);
        self.in_mem_batches.push(input);
        self.in_mem_batches_sorted = false;
        Ok(())
    }

    fn spilled_before(&self) -> bool {
        !self.spills.is_empty()
    }

    /// MergeSort in mem batches as well as spills into total order with `SortPreservingMergeStream`.
    fn sort(&mut self) -> Result<SendableRecordBatchStream> {
        if self.spilled_before() {
            let intermediate_metrics = self
                .metrics_set
                .new_intermediate_tracking(self.partition_id, &self.runtime.memory_pool);

            let merge_metrics = self
                .metrics_set
                .new_final_tracking(self.partition_id, &self.runtime.memory_pool);

            let mut streams = vec![];
            if !self.in_mem_batches.is_empty() {
                let in_mem_stream = self.in_mem_sort_stream(intermediate_metrics)?;
                streams.push(in_mem_stream);
            }

            for spill in self.spills.drain(..) {
                let stream = read_spill_as_stream(spill, self.schema.clone())?;
                streams.push(stream);
            }

            streaming_merge(
                streams,
                self.schema.clone(),
                &self.expr,
                merge_metrics,
                self.session_config.batch_size(),
            )
        } else if !self.in_mem_batches.is_empty() {
            let tracking_metrics = self
                .metrics_set
                .new_final_tracking(self.partition_id, &self.runtime.memory_pool);
            let result = self.in_mem_sort_stream(tracking_metrics);
            // Report to the memory manager we are no longer using memory
            self.reservation.free();
            result
        } else {
            Ok(Box::pin(EmptyRecordBatchStream::new(self.schema.clone())))
        }
    }

    fn used(&self) -> usize {
        self.metrics.mem_used().value()
    }

    fn spilled_bytes(&self) -> usize {
        self.metrics.spilled_bytes().value()
    }

    fn spill_count(&self) -> usize {
        self.metrics.spill_count().value()
    }

    async fn spill(&mut self) -> Result<usize> {
        // we could always get a chance to free some memory as long as we are holding some
        if self.in_mem_batches.is_empty() {
            return Ok(0);
        }

        debug!("Spilling sort data of ExternalSorter to disk whilst inserting");

        self.in_mem_sort().await?;

        let spillfile = self.runtime.disk_manager.create_tmp_file("Sorting")?;
        let batches = std::mem::take(&mut self.in_mem_batches);
        spill_sorted_batches(batches, spillfile.path(), self.schema.clone()).await?;
        self.reservation.free();
        let used = self.metrics.mem_used().set(0);
        self.metrics.record_spill(used);
        self.spills.push(spillfile);
        Ok(used)
    }

    /// Sorts the in_mem_batches in place
    async fn in_mem_sort(&mut self) -> Result<()> {
        if self.in_mem_batches_sorted {
            return Ok(());
        }

        let tracking_metrics = self
            .metrics_set
            .new_intermediate_tracking(self.partition_id, &self.runtime.memory_pool);

        self.in_mem_batches = self
            .in_mem_sort_stream(tracking_metrics)?
            .try_collect()
            .await?;

        let size: usize = self
            .in_mem_batches
            .iter()
            .map(|x| x.get_array_memory_size())
            .sum();

        self.metrics.mem_used().set(size);
        self.reservation.resize(size);
        self.in_mem_batches_sorted = true;
        Ok(())
    }

    /// Consumes in_mem_batches returning a sorted stream
    fn in_mem_sort_stream(
        &mut self,
        metrics: MemTrackingMetrics,
    ) -> Result<SendableRecordBatchStream> {
        assert_ne!(self.in_mem_batches.len(), 0);
        if self.in_mem_batches.len() == 1 {
            let batch = self.in_mem_batches.remove(0);
            let stream =
                sort_batch_stream(batch, self.expr.clone(), self.fetch, metrics)?;
            self.in_mem_batches.clear();
            return Ok(stream);
        }

        // If less than 1MB of in-memory data, concatenate and sort in place
        //
        // This is a very rough heuristic and likely could be refined further
        if self.reservation.size() < 1048576 {
            // Concatenate memory batches together and sort
            let batch = concat_batches(&self.schema, &self.in_mem_batches)?;
            self.in_mem_batches.clear();
            return sort_batch_stream(batch, self.expr.clone(), self.fetch, metrics);
        }

        let streams = self
            .in_mem_batches
            .drain(..)
            .map(|batch| {
                let metrics = self.metrics_set.new_intermediate_tracking(
                    self.partition_id,
                    &self.runtime.memory_pool,
                );
                Ok(spawn_buffered(
                    sort_batch_stream(batch, self.expr.clone(), self.fetch, metrics)?,
                    1,
                ))
            })
            .collect::<Result<_>>()?;

        // TODO: Pushdown fetch to streaming merge (#6000)

        streaming_merge(
            streams,
            self.schema.clone(),
            &self.expr,
            metrics,
            self.session_config.batch_size(),
        )
    }
}

impl Debug for ExternalSorter {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        f.debug_struct("ExternalSorter")
            .field("memory_used", &self.used())
            .field("spilled_bytes", &self.spilled_bytes())
            .field("spill_count", &self.spill_count())
            .finish()
    }
}

fn sort_batch_stream(
    batch: RecordBatch,
    expressions: Arc<[PhysicalSortExpr]>,
    fetch: Option<usize>,
    mut tracking_metrics: MemTrackingMetrics,
) -> Result<SendableRecordBatchStream> {
    let schema = batch.schema();
    tracking_metrics.init_mem_used(batch.get_array_memory_size());
    let stream = futures::stream::once(futures::future::lazy(move |_| {
        let sorted = sort_batch(&batch, &expressions, fetch)?;
        tracking_metrics.record_output(sorted.num_rows());
        Ok(sorted)
    }));
    Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
}

fn sort_batch(
    batch: &RecordBatch,
    expressions: &[PhysicalSortExpr],
    fetch: Option<usize>,
) -> Result<RecordBatch> {
    let sort_columns = expressions
        .iter()
        .map(|expr| expr.evaluate_to_sort_column(batch))
        .collect::<Result<Vec<_>>>()?;

    let indices = lexsort_to_indices(&sort_columns, fetch)?;

    let columns = batch
        .columns()
        .iter()
        .map(|c| take(c.as_ref(), &indices, None))
        .collect::<Result<_, _>>()?;

    Ok(RecordBatch::try_new(batch.schema(), columns)?)
}

async fn spill_sorted_batches(
    batches: Vec<RecordBatch>,
    path: &Path,
    schema: SchemaRef,
) -> Result<()> {
    let path: PathBuf = path.into();
    let handle = task::spawn_blocking(move || write_sorted(batches, path, schema));
    match handle.await {
        Ok(r) => r,
        Err(e) => Err(DataFusionError::Execution(format!(
            "Error occurred while spilling {e}"
        ))),
    }
}

fn read_spill_as_stream(
    path: NamedTempFile,
    schema: SchemaRef,
) -> Result<SendableRecordBatchStream> {
    let (sender, receiver): (Sender<Result<RecordBatch>>, Receiver<Result<RecordBatch>>) =
        tokio::sync::mpsc::channel(2);
    let join_handle = task::spawn_blocking(move || {
        if let Err(e) = read_spill(sender, path.path()) {
            error!("Failure while reading spill file: {:?}. Error: {}", path, e);
        }
    });
    Ok(RecordBatchReceiverStream::create(
        &schema,
        receiver,
        join_handle,
    ))
}

fn write_sorted(
    batches: Vec<RecordBatch>,
    path: PathBuf,
    schema: SchemaRef,
) -> Result<()> {
    let mut writer = IPCWriter::new(path.as_ref(), schema.as_ref())?;
    for batch in batches {
        writer.write(&batch)?;
    }
    writer.finish()?;
    debug!(
        "Spilled {} batches of total {} rows to disk, memory released {}",
        writer.num_batches,
        writer.num_rows,
        human_readable_size(writer.num_bytes as usize),
    );
    Ok(())
}

fn read_spill(sender: Sender<Result<RecordBatch>>, path: &Path) -> Result<()> {
    let file = BufReader::new(File::open(path)?);
    let reader = FileReader::try_new(file, None)?;
    for batch in reader {
        sender
            .blocking_send(batch.map_err(Into::into))
            .map_err(|e| DataFusionError::Execution(format!("{e}")))?;
    }
    Ok(())
}

/// Sort execution plan.
///
/// This operator supports sorting datasets that are larger than the
/// memory allotted by the memory manager, by spilling to disk.
#[derive(Debug)]
pub struct SortExec {
    /// Input schema
    pub(crate) input: Arc<dyn ExecutionPlan>,
    /// Sort expressions
    expr: Vec<PhysicalSortExpr>,
    /// Containing all metrics set created during sort
    metrics_set: CompositeMetricsSet,
    /// Preserve partitions of input plan. If false, the input partitions
    /// will be sorted and merged into a single output partition.
    preserve_partitioning: bool,
    /// Fetch highest/lowest n results
    fetch: Option<usize>,
}

impl SortExec {
    /// Create a new sort execution plan
    #[deprecated(since = "22.0.0", note = "use `new` and `with_fetch`")]
    pub fn try_new(
        expr: Vec<PhysicalSortExpr>,
        input: Arc<dyn ExecutionPlan>,
        fetch: Option<usize>,
    ) -> Result<Self> {
        Ok(Self::new(expr, input).with_fetch(fetch))
    }

    /// Create a new sort execution plan that produces a single,
    /// sorted output partition.
    pub fn new(expr: Vec<PhysicalSortExpr>, input: Arc<dyn ExecutionPlan>) -> Self {
        Self {
            expr,
            input,
            metrics_set: CompositeMetricsSet::new(),
            preserve_partitioning: false,
            fetch: None,
        }
    }

    /// Create a new sort execution plan with the option to preserve
    /// the partitioning of the input plan
    #[deprecated(
        since = "22.0.0",
        note = "use `new`, `with_fetch` and `with_preserve_partioning` instead"
    )]
    pub fn new_with_partitioning(
        expr: Vec<PhysicalSortExpr>,
        input: Arc<dyn ExecutionPlan>,
        preserve_partitioning: bool,
        fetch: Option<usize>,
    ) -> Self {
        Self::new(expr, input)
            .with_fetch(fetch)
            .with_preserve_partitioning(preserve_partitioning)
    }

    /// Whether this `SortExec` preserves partitioning of the children
    pub fn preserve_partitioning(&self) -> bool {
        self.preserve_partitioning
    }

    /// Specify the partitioning behavior of this sort exec
    ///
    /// If `preserve_partitioning` is true, sorts each partition
    /// individually, producing one sorted strema for each input partition.
    ///
    /// If `preserve_partitioning` is false, sorts and merges all
    /// input partitions producing a single, sorted partition.
    pub fn with_preserve_partitioning(mut self, preserve_partitioning: bool) -> Self {
        self.preserve_partitioning = preserve_partitioning;
        self
    }

    /// Whether this `SortExec` preserves partitioning of the children
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

    /// If `Some(fetch)`, limits output to only the first "fetch" items
    pub fn fetch(&self) -> Option<usize> {
        self.fetch
    }
}

impl ExecutionPlan for SortExec {
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

    /// Specifies whether this plan generates an infinite stream of records.
    /// If the plan does not support pipelining, but its input(s) are
    /// infinite, returns an error to indicate this.
    fn unbounded_output(&self, children: &[bool]) -> Result<bool> {
        if children[0] {
            Err(DataFusionError::Plan(
                "Sort Error: Can not sort unbounded inputs.".to_string(),
            ))
        } else {
            Ok(false)
        }
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        if self.preserve_partitioning {
            vec![Distribution::UnspecifiedDistribution]
        } else {
            // global sort
            // TODO support RangePartition and OrderedDistribution
            vec![Distribution::SinglePartition]
        }
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![self.input.clone()]
    }

    fn benefits_from_input_partitioning(&self) -> bool {
        false
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        Some(&self.expr)
    }

    fn equivalence_properties(&self) -> EquivalenceProperties {
        self.input.equivalence_properties()
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let new_sort = SortExec::new(self.expr.clone(), children[0].clone())
            .with_fetch(self.fetch)
            .with_preserve_partitioning(self.preserve_partitioning);

        Ok(Arc::new(new_sort))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        trace!("Start SortExec::execute for partition {} of context session_id {} and task_id {:?}", partition, context.session_id(), context.task_id());

        let input = self.input.execute(partition, context.clone())?;

        trace!("End SortExec's input.execute for partition: {}", partition);

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            futures::stream::once(do_sort(
                input,
                partition,
                self.expr.clone(),
                self.metrics_set.clone(),
                context,
                self.fetch(),
            ))
            .try_flatten(),
        )))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics_set.aggregate_all())
    }

    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default => {
                let expr: Vec<String> = self.expr.iter().map(|e| e.to_string()).collect();
                match self.fetch {
                    Some(fetch) => {
                        write!(f, "SortExec: fetch={fetch}, expr=[{}]", expr.join(","))
                    }
                    None => write!(f, "SortExec: expr=[{}]", expr.join(",")),
                }
            }
        }
    }

    fn statistics(&self) -> Statistics {
        self.input.statistics()
    }
}

async fn do_sort(
    mut input: SendableRecordBatchStream,
    partition_id: usize,
    expr: Vec<PhysicalSortExpr>,
    metrics_set: CompositeMetricsSet,
    context: Arc<TaskContext>,
    fetch: Option<usize>,
) -> Result<SendableRecordBatchStream> {
    trace!(
        "Start do_sort for partition {} of context session_id {} and task_id {:?}",
        partition_id,
        context.session_id(),
        context.task_id()
    );
    let schema = input.schema();
    let mut sorter = ExternalSorter::new(
        partition_id,
        schema.clone(),
        expr,
        metrics_set,
        Arc::new(context.session_config().clone()),
        context.runtime_env(),
        fetch,
    );
    while let Some(batch) = input.next().await {
        let batch = batch?;
        sorter.insert_batch(batch).await?;
    }
    let result = sorter.sort();
    trace!(
        "End do_sort for partition {} of context session_id {} and task_id {:?}",
        partition_id,
        context.session_id(),
        context.task_id()
    );
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution::context::SessionConfig;
    use crate::execution::runtime_env::RuntimeConfig;
    use crate::physical_plan::coalesce_partitions::CoalescePartitionsExec;
    use crate::physical_plan::collect;
    use crate::physical_plan::expressions::col;
    use crate::physical_plan::memory::MemoryExec;
    use crate::prelude::SessionContext;
    use crate::test;
    use crate::test::assert_is_pending;
    use crate::test::exec::{assert_strong_count_converges_to_zero, BlockingExec};
    use arrow::array::*;
    use arrow::compute::SortOptions;
    use arrow::datatypes::*;
    use datafusion_common::cast::{as_primitive_array, as_string_array};
    use futures::FutureExt;
    use std::collections::HashMap;

    #[tokio::test]
    async fn test_in_mem_sort() -> Result<()> {
        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
        let partitions = 4;
        let csv = test::scan_partitioned_csv(partitions)?;
        let schema = csv.schema();

        let sort_exec = Arc::new(SortExec::new(
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
            Arc::new(CoalescePartitionsExec::new(csv)),
        ));

        let result = collect(sort_exec, task_ctx).await?;

        assert_eq!(result.len(), 1);

        let columns = result[0].columns();

        let c1 = as_string_array(&columns[0])?;
        assert_eq!(c1.value(0), "a");
        assert_eq!(c1.value(c1.len() - 1), "e");

        let c2 = as_primitive_array::<UInt32Type>(&columns[1])?;
        assert_eq!(c2.value(0), 1);
        assert_eq!(c2.value(c2.len() - 1), 5,);

        let c7 = as_primitive_array::<UInt8Type>(&columns[6])?;
        assert_eq!(c7.value(0), 15);
        assert_eq!(c7.value(c7.len() - 1), 254,);

        assert_eq!(
            session_ctx.runtime_env().memory_pool.reserved(),
            0,
            "The sort should have returned all memory used back to the memory manager"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_sort_spill() -> Result<()> {
        // trigger spill there will be 4 batches with 5.5KB for each
        let config = RuntimeConfig::new().with_memory_limit(12288, 1.0);
        let runtime = Arc::new(RuntimeEnv::new(config)?);
        let session_ctx = SessionContext::with_config_rt(SessionConfig::new(), runtime);

        let partitions = 4;
        let csv = test::scan_partitioned_csv(partitions)?;
        let schema = csv.schema();

        let sort_exec = Arc::new(SortExec::new(
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
            Arc::new(CoalescePartitionsExec::new(csv)),
        ));

        let task_ctx = session_ctx.task_ctx();
        let result = collect(sort_exec.clone(), task_ctx).await?;

        assert_eq!(result.len(), 1);

        // Now, validate metrics
        let metrics = sort_exec.metrics().unwrap();

        assert_eq!(metrics.output_rows().unwrap(), 100);
        assert!(metrics.elapsed_compute().unwrap() > 0);
        assert!(metrics.spill_count().unwrap() > 0);
        assert!(metrics.spilled_bytes().unwrap() > 0);

        let columns = result[0].columns();

        let c1 = as_string_array(&columns[0])?;
        assert_eq!(c1.value(0), "a");
        assert_eq!(c1.value(c1.len() - 1), "e");

        let c2 = as_primitive_array::<UInt32Type>(&columns[1])?;
        assert_eq!(c2.value(0), 1);
        assert_eq!(c2.value(c2.len() - 1), 5,);

        let c7 = as_primitive_array::<UInt8Type>(&columns[6])?;
        assert_eq!(c7.value(0), 15);
        assert_eq!(c7.value(c7.len() - 1), 254,);

        assert_eq!(
            session_ctx.runtime_env().memory_pool.reserved(),
            0,
            "The sort should have returned all memory used back to the memory manager"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_sort_fetch_memory_calculation() -> Result<()> {
        // This test mirrors down the size from the example above.
        let avg_batch_size = 4000;
        let partitions = 4;

        // A tuple of (fetch, expect_spillage)
        let test_options = vec![
            // Since we don't have a limit (and the memory is less than the total size of
            // all the batches we are processing, we expect it to spill.
            (None, true),
            // When we have a limit however, the buffered size of batches should fit in memory
            // since it is much lower than the total size of the input batch.
            (Some(1), false),
        ];

        for (fetch, expect_spillage) in test_options {
            let config = RuntimeConfig::new()
                .with_memory_limit(avg_batch_size * (partitions - 1), 1.0);
            let runtime = Arc::new(RuntimeEnv::new(config)?);
            let session_ctx =
                SessionContext::with_config_rt(SessionConfig::new(), runtime);

            let csv = test::scan_partitioned_csv(partitions)?;
            let schema = csv.schema();

            let sort_exec = Arc::new(
                SortExec::new(
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
                    Arc::new(CoalescePartitionsExec::new(csv)),
                )
                .with_fetch(fetch),
            );

            let task_ctx = session_ctx.task_ctx();
            let result = collect(sort_exec.clone(), task_ctx).await?;
            assert_eq!(result.len(), 1);

            let metrics = sort_exec.metrics().unwrap();
            let did_it_spill = metrics.spill_count().unwrap() > 0;
            assert_eq!(did_it_spill, expect_spillage, "with fetch: {fetch:?}");
        }
        Ok(())
    }

    #[tokio::test]
    async fn test_sort_metadata() -> Result<()> {
        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
        let field_metadata: HashMap<String, String> =
            vec![("foo".to_string(), "bar".to_string())]
                .into_iter()
                .collect();
        let schema_metadata: HashMap<String, String> =
            vec![("baz".to_string(), "barf".to_string())]
                .into_iter()
                .collect();

        let mut field = Field::new("field_name", DataType::UInt64, true);
        field.set_metadata(field_metadata.clone());
        let schema = Schema::new_with_metadata(vec![field], schema_metadata.clone());
        let schema = Arc::new(schema);

        let data: ArrayRef =
            Arc::new(vec![3, 2, 1].into_iter().map(Some).collect::<UInt64Array>());

        let batch = RecordBatch::try_new(schema.clone(), vec![data]).unwrap();
        let input =
            Arc::new(MemoryExec::try_new(&[vec![batch]], schema.clone(), None).unwrap());

        let sort_exec = Arc::new(SortExec::new(
            vec![PhysicalSortExpr {
                expr: col("field_name", &schema)?,
                options: SortOptions::default(),
            }],
            input,
        ));

        let result: Vec<RecordBatch> = collect(sort_exec, task_ctx).await?;

        let expected_data: ArrayRef =
            Arc::new(vec![1, 2, 3].into_iter().map(Some).collect::<UInt64Array>());
        let expected_batch =
            RecordBatch::try_new(schema.clone(), vec![expected_data]).unwrap();

        // Data is correct
        assert_eq!(&vec![expected_batch], &result);

        // explicitlty ensure the metadata is present
        assert_eq!(result[0].schema().fields()[0].metadata(), &field_metadata);
        assert_eq!(result[0].schema().metadata(), &schema_metadata);

        Ok(())
    }

    #[tokio::test]
    async fn test_lex_sort_by_float() -> Result<()> {
        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
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

        let sort_exec = Arc::new(SortExec::new(
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
        ));

        assert_eq!(DataType::Float32, *sort_exec.schema().field(0).data_type());
        assert_eq!(DataType::Float64, *sort_exec.schema().field(1).data_type());

        let result: Vec<RecordBatch> = collect(sort_exec.clone(), task_ctx).await?;
        let metrics = sort_exec.metrics().unwrap();
        assert!(metrics.elapsed_compute().unwrap() > 0);
        assert_eq!(metrics.output_rows().unwrap(), 8);
        assert_eq!(result.len(), 1);

        let columns = result[0].columns();

        assert_eq!(DataType::Float32, *columns[0].data_type());
        assert_eq!(DataType::Float64, *columns[1].data_type());

        let a = as_primitive_array::<Float32Type>(&columns[0])?;
        let b = as_primitive_array::<Float64Type>(&columns[1])?;

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
        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
        let schema =
            Arc::new(Schema::new(vec![Field::new("a", DataType::Float32, true)]));

        let blocking_exec = Arc::new(BlockingExec::new(Arc::clone(&schema), 1));
        let refs = blocking_exec.refs();
        let sort_exec = Arc::new(SortExec::new(
            vec![PhysicalSortExpr {
                expr: col("a", &schema)?,
                options: SortOptions::default(),
            }],
            blocking_exec,
        ));

        let fut = collect(sort_exec, task_ctx);
        let mut fut = fut.boxed();

        assert_is_pending(&mut fut);
        drop(fut);
        assert_strong_count_converges_to_zero(refs).await;

        assert_eq!(
            session_ctx.runtime_env().memory_pool.reserved(),
            0,
            "The sort should have returned all memory used back to the memory manager"
        );

        Ok(())
    }
}
