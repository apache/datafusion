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
use crate::execution::memory_manager::{
    human_readable_size, ConsumerType, MemoryConsumer, MemoryConsumerId, MemoryManager,
};
use crate::execution::runtime_env::RuntimeEnv;
use crate::physical_plan::common::{batch_byte_size, IPCWriter, SizedRecordBatchStream};
use crate::physical_plan::expressions::PhysicalSortExpr;
use crate::physical_plan::metrics::{
    BaselineMetrics, CompositeMetricsSet, MemTrackingMetrics, MetricsSet,
};
use crate::physical_plan::sorts::sort_preserving_merge::SortPreservingMergeStream;
use crate::physical_plan::sorts::SortedStream;
use crate::physical_plan::stream::{RecordBatchReceiverStream, RecordBatchStreamAdapter};
use crate::physical_plan::{
    DisplayFormatType, Distribution, EmptyRecordBatchStream, ExecutionPlan, Partitioning,
    RecordBatchStream, SendableRecordBatchStream, Statistics,
};
use crate::prelude::SessionConfig;
use arrow::array::{make_array, Array, ArrayRef, MutableArrayData};
pub use arrow::compute::SortOptions;
use arrow::compute::{concat, lexsort_to_indices, take, SortColumn, TakeOptions};
use arrow::datatypes::SchemaRef;
use arrow::error::{ArrowError, Result as ArrowResult};
use arrow::ipc::reader::FileReader;
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use futures::lock::Mutex;
use futures::{Stream, StreamExt, TryFutureExt, TryStreamExt};
use log::{debug, error};
use std::any::Any;
use std::cmp::min;
use std::fmt;
use std::fmt::{Debug, Formatter};
use std::fs::File;
use std::io::BufReader;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::task::{Context, Poll};
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
    id: MemoryConsumerId,
    schema: SchemaRef,
    in_mem_batches: Mutex<Vec<BatchWithSortArray>>,
    spills: Mutex<Vec<NamedTempFile>>,
    /// Sort expressions
    expr: Vec<PhysicalSortExpr>,
    session_config: Arc<SessionConfig>,
    runtime: Arc<RuntimeEnv>,
    metrics_set: CompositeMetricsSet,
    metrics: BaselineMetrics,
    fetch: Option<usize>,
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
        Self {
            id: MemoryConsumerId::new(partition_id),
            schema,
            in_mem_batches: Mutex::new(vec![]),
            spills: Mutex::new(vec![]),
            expr,
            session_config,
            runtime,
            metrics_set,
            metrics,
            fetch,
        }
    }

    async fn insert_batch(
        &self,
        input: RecordBatch,
        tracking_metrics: &MemTrackingMetrics,
    ) -> Result<()> {
        if input.num_rows() > 0 {
            let size = batch_byte_size(&input);
            self.try_grow(size).await?;
            self.metrics.mem_used().add(size);
            let mut in_mem_batches = self.in_mem_batches.lock().await;
            // NB timer records time taken on drop, so there are no
            // calls to `timer.done()` below.
            let _timer = tracking_metrics.elapsed_compute().timer();
            let partial = sort_batch(input, self.schema.clone(), &self.expr, self.fetch)?;
            in_mem_batches.push(partial);
        }
        Ok(())
    }

    async fn spilled_before(&self) -> bool {
        let spills = self.spills.lock().await;
        !spills.is_empty()
    }

    /// MergeSort in mem batches as well as spills into total order with `SortPreservingMergeStream`.
    async fn sort(&self) -> Result<SendableRecordBatchStream> {
        let partition = self.partition_id();
        let batch_size = self.session_config.batch_size();
        let mut in_mem_batches = self.in_mem_batches.lock().await;

        if self.spilled_before().await {
            let tracking_metrics = self
                .metrics_set
                .new_intermediate_tracking(partition, self.runtime.clone());
            let mut streams: Vec<SortedStream> = vec![];
            if in_mem_batches.len() > 0 {
                let in_mem_stream = in_mem_partial_sort(
                    &mut in_mem_batches,
                    self.schema.clone(),
                    &self.expr,
                    batch_size,
                    tracking_metrics,
                    self.fetch,
                )?;
                let prev_used = self.free_all_memory();
                streams.push(SortedStream::new(in_mem_stream, prev_used));
            }

            let mut spills = self.spills.lock().await;

            for spill in spills.drain(..) {
                let stream = read_spill_as_stream(spill, self.schema.clone())?;
                streams.push(SortedStream::new(stream, 0));
            }
            let tracking_metrics = self
                .metrics_set
                .new_final_tracking(partition, self.runtime.clone());
            Ok(Box::pin(SortPreservingMergeStream::new_from_streams(
                streams,
                self.schema.clone(),
                &self.expr,
                tracking_metrics,
                self.session_config.batch_size(),
            )))
        } else if in_mem_batches.len() > 0 {
            let tracking_metrics = self
                .metrics_set
                .new_final_tracking(partition, self.runtime.clone());
            let result = in_mem_partial_sort(
                &mut in_mem_batches,
                self.schema.clone(),
                &self.expr,
                batch_size,
                tracking_metrics,
                self.fetch,
            );
            // Report to the memory manager we are no longer using memory
            self.free_all_memory();
            result
        } else {
            Ok(Box::pin(EmptyRecordBatchStream::new(self.schema.clone())))
        }
    }

    fn free_all_memory(&self) -> usize {
        let used = self.metrics.mem_used().set(0);
        self.shrink(used);
        used
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
}

impl Debug for ExternalSorter {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        f.debug_struct("ExternalSorter")
            .field("id", &self.id())
            .field("memory_used", &self.used())
            .field("spilled_bytes", &self.spilled_bytes())
            .field("spill_count", &self.spill_count())
            .finish()
    }
}

impl Drop for ExternalSorter {
    fn drop(&mut self) {
        self.runtime.drop_consumer(self.id(), self.used());
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
        debug!(
            "{}[{}] spilling sort data of {} to disk while inserting ({} time(s) so far)",
            self.name(),
            self.id(),
            self.used(),
            self.spill_count()
        );

        let partition = self.partition_id();
        let mut in_mem_batches = self.in_mem_batches.lock().await;
        // we could always get a chance to free some memory as long as we are holding some
        if in_mem_batches.len() == 0 {
            return Ok(0);
        }

        let tracking_metrics = self
            .metrics_set
            .new_intermediate_tracking(partition, self.runtime.clone());

        let spillfile = self.runtime.disk_manager.create_tmp_file()?;
        let stream = in_mem_partial_sort(
            &mut in_mem_batches,
            self.schema.clone(),
            &self.expr,
            self.session_config.batch_size(),
            tracking_metrics,
            self.fetch,
        );

        spill_partial_sorted_stream(&mut stream?, spillfile.path(), self.schema.clone())
            .await?;
        let mut spills = self.spills.lock().await;
        let used = self.metrics.mem_used().set(0);
        self.metrics.record_spill(used);
        spills.push(spillfile);
        Ok(used)
    }

    fn mem_used(&self) -> usize {
        self.metrics.mem_used().value()
    }
}

/// consume the non-empty `sorted_batches` and do in_mem_sort
fn in_mem_partial_sort(
    buffered_batches: &mut Vec<BatchWithSortArray>,
    schema: SchemaRef,
    expressions: &[PhysicalSortExpr],
    batch_size: usize,
    tracking_metrics: MemTrackingMetrics,
    fetch: Option<usize>,
) -> Result<SendableRecordBatchStream> {
    assert_ne!(buffered_batches.len(), 0);
    if buffered_batches.len() == 1 {
        let result = buffered_batches.pop();
        Ok(Box::pin(SizedRecordBatchStream::new(
            schema,
            vec![Arc::new(result.unwrap().sorted_batch)],
            tracking_metrics,
        )))
    } else {
        let (sorted_arrays, batches): (Vec<Vec<ArrayRef>>, Vec<RecordBatch>) =
            buffered_batches
                .drain(..)
                .into_iter()
                .map(|b| {
                    let BatchWithSortArray {
                        sort_arrays,
                        sorted_batch: batch,
                    } = b;
                    (sort_arrays, batch)
                })
                .unzip();

        let sorted_iter = {
            // NB timer records time taken on drop, so there are no
            // calls to `timer.done()` below.
            let _timer = tracking_metrics.elapsed_compute().timer();
            get_sorted_iter(&sorted_arrays, expressions, batch_size, fetch)?
        };
        Ok(Box::pin(SortedSizedRecordBatchStream::new(
            schema,
            batches,
            sorted_iter,
            tracking_metrics,
        )))
    }
}

#[derive(Debug, Copy, Clone)]
struct CompositeIndex {
    batch_idx: u32,
    row_idx: u32,
}

/// Get sorted iterator by sort concatenated `SortColumn`s
fn get_sorted_iter(
    sort_arrays: &[Vec<ArrayRef>],
    expr: &[PhysicalSortExpr],
    batch_size: usize,
    fetch: Option<usize>,
) -> Result<SortedIterator> {
    let row_indices = sort_arrays
        .iter()
        .enumerate()
        .flat_map(|(i, arrays)| {
            (0..arrays[0].len()).map(move |r| CompositeIndex {
                // since we original use UInt32Array to index the combined mono batch,
                // component record batches won't overflow as well,
                // use u32 here for space efficiency.
                batch_idx: i as u32,
                row_idx: r as u32,
            })
        })
        .collect::<Vec<CompositeIndex>>();

    let sort_columns = expr
        .iter()
        .enumerate()
        .map(|(i, expr)| {
            let columns_i = sort_arrays
                .iter()
                .map(|cs| cs[i].as_ref())
                .collect::<Vec<&dyn Array>>();
            Ok(SortColumn {
                values: concat(columns_i.as_slice())?,
                options: Some(expr.options),
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let indices = lexsort_to_indices(&sort_columns, fetch)?;

    // Calculate composite index based on sorted indices
    let row_indices = indices
        .values()
        .iter()
        .map(|i| row_indices[*i as usize])
        .collect();

    Ok(SortedIterator::new(row_indices, batch_size))
}

struct SortedIterator {
    /// Current logical position in the iterator
    pos: usize,
    /// Sorted composite index of where to find the rows in buffered batches
    composite: Vec<CompositeIndex>,
    /// Maximum batch size to produce
    batch_size: usize,
}

impl SortedIterator {
    fn new(composite: Vec<CompositeIndex>, batch_size: usize) -> Self {
        Self {
            pos: 0,
            composite,
            batch_size,
        }
    }

    fn memory_size(&self) -> usize {
        std::mem::size_of_val(self) + std::mem::size_of_val(&self.composite[..])
    }
}

impl Iterator for SortedIterator {
    type Item = Vec<CompositeSlice>;

    /// Emit a max of `batch_size` positions each time
    fn next(&mut self) -> Option<Self::Item> {
        let length = self.composite.len();
        if self.pos >= length {
            return None;
        }

        let current_size = min(self.batch_size, length - self.pos);

        // Combine adjacent indexes from the same batch to make a slice,
        // for more efficient `extend` later.
        let mut last_batch_idx = self.composite[self.pos].batch_idx;
        let mut indices_in_batch = Vec::with_capacity(current_size);

        let mut slices = vec![];
        for ci in &self.composite[self.pos..self.pos + current_size] {
            if ci.batch_idx != last_batch_idx {
                group_indices(last_batch_idx, &mut indices_in_batch, &mut slices);
                last_batch_idx = ci.batch_idx;
            }
            indices_in_batch.push(ci.row_idx);
        }

        assert!(
            !indices_in_batch.is_empty(),
            "There should have at least one record in a sort output slice."
        );
        group_indices(last_batch_idx, &mut indices_in_batch, &mut slices);

        self.pos += current_size;
        Some(slices)
    }
}

/// Group continuous indices into a slice for better `extend` performance
fn group_indices(
    batch_idx: u32,
    positions: &mut Vec<u32>,
    output: &mut Vec<CompositeSlice>,
) {
    positions.sort_unstable();
    let mut last_pos = 0;
    let mut run_length = 0;
    for pos in positions.iter() {
        if run_length == 0 {
            last_pos = *pos;
            run_length = 1;
        } else if *pos == last_pos + 1 {
            run_length += 1;
            last_pos = *pos;
        } else {
            output.push(CompositeSlice {
                batch_idx,
                start_row_idx: last_pos + 1 - run_length,
                len: run_length as usize,
            });
            last_pos = *pos;
            run_length = 1;
        }
    }
    assert!(
        run_length > 0,
        "There should have at least one record in a sort output slice."
    );
    output.push(CompositeSlice {
        batch_idx,
        start_row_idx: last_pos + 1 - run_length,
        len: run_length as usize,
    });
    positions.clear()
}

/// Stream of sorted record batches
struct SortedSizedRecordBatchStream {
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
    sorted_iter: SortedIterator,
    num_cols: usize,
    metrics: MemTrackingMetrics,
}

impl SortedSizedRecordBatchStream {
    /// new
    pub fn new(
        schema: SchemaRef,
        batches: Vec<RecordBatch>,
        sorted_iter: SortedIterator,
        metrics: MemTrackingMetrics,
    ) -> Self {
        let size = batches.iter().map(batch_byte_size).sum::<usize>()
            + sorted_iter.memory_size();
        metrics.init_mem_used(size);
        let num_cols = batches[0].num_columns();
        SortedSizedRecordBatchStream {
            schema,
            batches,
            sorted_iter,
            num_cols,
            metrics,
        }
    }
}

impl Stream for SortedSizedRecordBatchStream {
    type Item = ArrowResult<RecordBatch>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        match self.sorted_iter.next() {
            None => Poll::Ready(None),
            Some(slices) => {
                let num_rows = slices.iter().map(|s| s.len).sum();
                let output = (0..self.num_cols)
                    .map(|i| {
                        let arrays = self
                            .batches
                            .iter()
                            .map(|b| b.column(i).data())
                            .collect::<Vec<_>>();
                        let mut mutable = MutableArrayData::new(arrays, false, num_rows);
                        for x in slices.iter() {
                            mutable.extend(
                                x.batch_idx as usize,
                                x.start_row_idx as usize,
                                x.start_row_idx as usize + x.len,
                            );
                        }
                        make_array(mutable.freeze())
                    })
                    .collect::<Vec<_>>();
                let batch = RecordBatch::try_new(self.schema.clone(), output);
                let poll = Poll::Ready(Some(batch));
                self.metrics.record_poll(poll)
            }
        }
    }
}

struct CompositeSlice {
    batch_idx: u32,
    start_row_idx: u32,
    len: usize,
}

impl RecordBatchStream for SortedSizedRecordBatchStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

async fn spill_partial_sorted_stream(
    in_mem_stream: &mut SendableRecordBatchStream,
    path: &Path,
    schema: SchemaRef,
) -> Result<()> {
    let (sender, receiver) = tokio::sync::mpsc::channel(2);
    let path: PathBuf = path.into();
    let handle = task::spawn_blocking(move || write_sorted(receiver, path, schema));
    while let Some(item) = in_mem_stream.next().await {
        sender.send(item).await.ok();
    }
    drop(sender);
    match handle.await {
        Ok(r) => r,
        Err(e) => Err(DataFusionError::Execution(format!(
            "Error occurred while spilling {}",
            e
        ))),
    }
}

fn read_spill_as_stream(
    path: NamedTempFile,
    schema: SchemaRef,
) -> Result<SendableRecordBatchStream> {
    let (sender, receiver): (
        Sender<ArrowResult<RecordBatch>>,
        Receiver<ArrowResult<RecordBatch>>,
    ) = tokio::sync::mpsc::channel(2);
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
    mut receiver: Receiver<ArrowResult<RecordBatch>>,
    path: PathBuf,
    schema: SchemaRef,
) -> Result<()> {
    let mut writer = IPCWriter::new(path.as_ref(), schema.as_ref())?;
    while let Some(batch) = receiver.blocking_recv() {
        writer.write(&batch?)?;
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

fn read_spill(sender: Sender<ArrowResult<RecordBatch>>, path: &Path) -> Result<()> {
    let file = BufReader::new(File::open(&path)?);
    let reader = FileReader::try_new(file, None)?;
    for batch in reader {
        sender
            .blocking_send(batch)
            .map_err(|e| DataFusionError::Execution(format!("{}", e)))?;
    }
    Ok(())
}

/// External Sort execution plan
#[derive(Debug)]
pub struct SortExec {
    /// Input schema
    input: Arc<dyn ExecutionPlan>,
    /// Sort expressions
    expr: Vec<PhysicalSortExpr>,
    /// Containing all metrics set created during sort
    metrics_set: CompositeMetricsSet,
    /// Preserve partitions of input plan
    preserve_partitioning: bool,
    /// Fetch highest/lowest n results
    fetch: Option<usize>,
}

impl SortExec {
    /// Create a new sort execution plan
    pub fn try_new(
        expr: Vec<PhysicalSortExpr>,
        input: Arc<dyn ExecutionPlan>,
        fetch: Option<usize>,
    ) -> Result<Self> {
        Ok(Self::new_with_partitioning(expr, input, false, fetch))
    }

    /// Whether this `SortExec` preserves partitioning of the children
    pub fn preserve_partitioning(&self) -> bool {
        self.preserve_partitioning
    }

    /// Create a new sort execution plan with the option to preserve
    /// the partitioning of the input plan
    pub fn new_with_partitioning(
        expr: Vec<PhysicalSortExpr>,
        input: Arc<dyn ExecutionPlan>,
        preserve_partitioning: bool,
        fetch: Option<usize>,
    ) -> Self {
        Self {
            expr,
            input,
            metrics_set: CompositeMetricsSet::new(),
            preserve_partitioning,
            fetch,
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

    fn relies_on_input_order(&self) -> bool {
        // this operator resorts everything
        false
    }

    fn benefits_from_input_partitioning(&self) -> bool {
        false
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        Some(&self.expr)
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(SortExec::new_with_partitioning(
            self.expr.clone(),
            children[0].clone(),
            self.preserve_partitioning,
            self.fetch,
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        debug!("Start SortExec::execute for partition {} of context session_id {} and task_id {:?}", partition, context.session_id(), context.task_id());

        debug!(
            "Start invoking SortExec's input.execute for partition: {}",
            partition
        );

        let input = self.input.execute(partition, context.clone())?;

        debug!("End SortExec's input.execute for partition: {}", partition);

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            futures::stream::once(
                do_sort(
                    input,
                    partition,
                    self.expr.clone(),
                    self.metrics_set.clone(),
                    context,
                    self.fetch(),
                )
                .map_err(|e| ArrowError::ExternalError(Box::new(e))),
            )
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
                write!(f, "SortExec: [{}]", expr.join(","))
            }
        }
    }

    fn statistics(&self) -> Statistics {
        self.input.statistics()
    }
}

struct BatchWithSortArray {
    sort_arrays: Vec<ArrayRef>,
    sorted_batch: RecordBatch,
}

fn sort_batch(
    batch: RecordBatch,
    schema: SchemaRef,
    expr: &[PhysicalSortExpr],
    fetch: Option<usize>,
) -> ArrowResult<BatchWithSortArray> {
    let sort_columns = expr
        .iter()
        .map(|e| e.evaluate_to_sort_column(&batch))
        .collect::<Result<Vec<SortColumn>>>()?;

    let indices = lexsort_to_indices(&sort_columns, fetch)?;

    // reorder all rows based on sorted indices
    let sorted_batch = RecordBatch::try_new(
        schema,
        batch
            .columns()
            .iter()
            .map(|column| {
                take(
                    column.as_ref(),
                    &indices,
                    // disable bound check overhead since indices are already generated from
                    // the same record batch
                    Some(TakeOptions {
                        check_bounds: false,
                    }),
                )
            })
            .collect::<ArrowResult<Vec<ArrayRef>>>()?,
    )?;

    let sort_arrays = sort_columns
        .into_iter()
        .map(|sc| {
            Ok(take(
                sc.values.as_ref(),
                &indices,
                Some(TakeOptions {
                    check_bounds: false,
                }),
            )?)
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(BatchWithSortArray {
        sort_arrays,
        sorted_batch,
    })
}

async fn do_sort(
    mut input: SendableRecordBatchStream,
    partition_id: usize,
    expr: Vec<PhysicalSortExpr>,
    metrics_set: CompositeMetricsSet,
    context: Arc<TaskContext>,
    fetch: Option<usize>,
) -> Result<SendableRecordBatchStream> {
    debug!(
        "Start do_sort for partition {} of context session_id {} and task_id {:?}",
        partition_id,
        context.session_id(),
        context.task_id()
    );
    let schema = input.schema();
    let tracking_metrics =
        metrics_set.new_intermediate_tracking(partition_id, context.runtime_env());
    let sorter = ExternalSorter::new(
        partition_id,
        schema.clone(),
        expr,
        metrics_set,
        Arc::new(context.session_config()),
        context.runtime_env(),
        fetch,
    );
    context.runtime_env().register_requester(sorter.id());
    while let Some(batch) = input.next().await {
        let batch = batch?;
        sorter.insert_batch(batch, &tracking_metrics).await?;
    }
    let result = sorter.sort().await;
    debug!(
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
    use futures::FutureExt;
    use std::collections::{BTreeMap, HashMap};

    #[tokio::test]
    async fn test_in_mem_sort() -> Result<()> {
        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
        let partitions = 4;
        let csv = test::scan_partitioned_csv(partitions)?;
        let schema = csv.schema();

        let sort_exec = Arc::new(SortExec::try_new(
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
            None,
        )?);

        let result = collect(sort_exec, task_ctx).await?;

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

        assert_eq!(
            session_ctx
                .runtime_env()
                .memory_manager
                .get_requester_total(),
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

        let sort_exec = Arc::new(SortExec::try_new(
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
            None,
        )?);

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

        let c1 = as_string_array(&columns[0]);
        assert_eq!(c1.value(0), "a");
        assert_eq!(c1.value(c1.len() - 1), "e");

        let c2 = as_primitive_array::<UInt32Type>(&columns[1]);
        assert_eq!(c2.value(0), 1);
        assert_eq!(c2.value(c2.len() - 1), 5,);

        let c7 = as_primitive_array::<UInt8Type>(&columns[6]);
        assert_eq!(c7.value(0), 15);
        assert_eq!(c7.value(c7.len() - 1), 254,);

        assert_eq!(
            session_ctx
                .runtime_env()
                .memory_manager
                .get_requester_total(),
            0,
            "The sort should have returned all memory used back to the memory manager"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_sort_metadata() -> Result<()> {
        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
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

        let sort_exec = Arc::new(SortExec::try_new(
            vec![PhysicalSortExpr {
                expr: col("field_name", &schema)?,
                options: SortOptions::default(),
            }],
            input,
            None,
        )?);

        let result: Vec<RecordBatch> = collect(sort_exec, task_ctx).await?;

        let expected_data: ArrayRef =
            Arc::new(vec![1, 2, 3].into_iter().map(Some).collect::<UInt64Array>());
        let expected_batch =
            RecordBatch::try_new(schema.clone(), vec![expected_data]).unwrap();

        // Data is correct
        assert_eq!(&vec![expected_batch], &result);

        // explicitlty ensure the metadata is present
        assert_eq!(
            result[0].schema().fields()[0].metadata(),
            Some(&field_metadata)
        );
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

        let sort_exec = Arc::new(SortExec::try_new(
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
            None,
        )?);

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
        let session_ctx = SessionContext::new();
        let task_ctx = session_ctx.task_ctx();
        let schema =
            Arc::new(Schema::new(vec![Field::new("a", DataType::Float32, true)]));

        let blocking_exec = Arc::new(BlockingExec::new(Arc::clone(&schema), 1));
        let refs = blocking_exec.refs();
        let sort_exec = Arc::new(SortExec::try_new(
            vec![PhysicalSortExpr {
                expr: col("a", &schema)?,
                options: SortOptions::default(),
            }],
            blocking_exec,
            None,
        )?);

        let fut = collect(sort_exec, task_ctx);
        let mut fut = fut.boxed();

        assert_is_pending(&mut fut);
        drop(fut);
        assert_strong_count_converges_to_zero(refs).await;

        assert_eq!(
            session_ctx
                .runtime_env()
                .memory_manager
                .get_requester_total(),
            0,
            "The sort should have returned all memory used back to the memory manager"
        );

        Ok(())
    }
}
