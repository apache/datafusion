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

use super::{RowBatch, RowSelection};
use super::{SendableSortStream, SortStreamItem};
use crate::error::{DataFusionError, Result};
use crate::execution::context::TaskContext;
use crate::execution::memory_pool::{
    human_readable_size, MemoryConsumer, MemoryReservation,
};
use crate::execution::runtime_env::RuntimeEnv;
use crate::physical_plan::common::{batch_byte_size, IPCWriter, SizedRecordBatchStream};
use crate::physical_plan::expressions::PhysicalSortExpr;
use crate::physical_plan::metrics::{
    BaselineMetrics, CompositeMetricsSet, MemTrackingMetrics, MetricsSet,
};
use crate::physical_plan::sorts::sort_preserving_merge::SortPreservingMergeStream;
use crate::physical_plan::sorts::SortedStream;
use crate::physical_plan::stream::RecordBatchStreamAdapter;
use crate::physical_plan::{
    displayable, DisplayFormatType, Distribution, ExecutionPlan, Partitioning,
    RecordBatchStream, SendableRecordBatchStream, Statistics,
};
use crate::prelude::SessionConfig;
use arrow::array::{make_array, Array, ArrayRef, MutableArrayData, UInt32Array};
pub use arrow::compute::SortOptions;
use arrow::compute::{concat, lexsort_to_indices, take, SortColumn, TakeOptions};
use arrow::datatypes::SchemaRef;
use arrow::error::ArrowError;
use arrow::ipc::reader::FileReader;
use arrow::record_batch::RecordBatch;
use arrow::row::{Row, RowConverter, SortField};
use datafusion_physical_expr::EquivalenceProperties;
use futures::{Stream, StreamExt, TryStreamExt};
use log::{debug, error};
use std::any::Any;
use std::cmp::{min, Ordering};
use std::fmt;
use std::fmt::{Debug, Formatter};
use std::fs::File;
use std::io::BufReader;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::task::{Context, Poll};
use tempfile::NamedTempFile;
use tokio::sync::mpsc::{self, Receiver, Sender};
use tokio::task::{self, JoinHandle};
use tokio_stream::wrappers::UnboundedReceiverStream;

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
    in_mem_batches: Vec<BatchWithSortArray>,
    spills: Vec<Spill>,
    /// Sort expressions
    expr: Vec<PhysicalSortExpr>,
    session_config: Arc<SessionConfig>,
    runtime: Arc<RuntimeEnv>,
    metrics_set: CompositeMetricsSet,
    metrics: BaselineMetrics,
    fetch: Option<usize>,
    reservation: MemoryReservation,
    partition_id: usize,
}
struct Spill {
    record_batch_file: NamedTempFile,
    // `None` when row encoding not preserved
    rows_file: Option<NamedTempFile>,
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
            spills: vec![],
            expr,
            session_config,
            runtime,
            metrics_set,
            metrics,
            fetch,
            reservation,
            partition_id,
        }
    }

    async fn insert_batch(
        &mut self,
        input: RecordBatch,
        tracking_metrics: &MemTrackingMetrics,
    ) -> Result<()> {
        if input.num_rows() > 0 {
            let size = batch_byte_size(&input);
            if self.reservation.try_grow(size).is_err() {
                self.spill().await?;
                self.reservation.try_grow(size)?
            }

            self.metrics.mem_used().add(size);
            // NB timer records time taken on drop, so there are no
            // calls to `timer.done()` below.
            let _timer = tracking_metrics.elapsed_compute().timer();
            let partial = sort_batch(input, self.schema.clone(), &self.expr, self.fetch)?;

            // The resulting batch might be smaller (or larger, see #3747) than the input
            // batch due to either a propagated limit or the re-construction of arrays. So
            // for being reliable, we need to reflect the memory usage of the partial batch.
            //
            // In addition, if it's row encoding was preserved, that would also change the size.
            let new_size = batch_byte_size(&partial.sorted_batch)
                + partial
                    .sort_data
                    .rows
                    .as_ref()
                    .map_or(0, |rows| rows.size());
            match new_size.cmp(&size) {
                Ordering::Greater => {
                    // We don't have to call try_grow here, since we have already used the
                    // memory (so spilling right here wouldn't help at all for the current
                    // operation). But we still have to record it so that other requesters
                    // would know about this unexpected increase in memory consumption.
                    let new_size_delta = new_size - size;
                    self.reservation.grow(new_size_delta);
                    self.metrics.mem_used().add(new_size_delta);
                }
                Ordering::Less => {
                    let size_delta = size - new_size;
                    self.reservation.shrink(size_delta);
                    self.metrics.mem_used().sub(size_delta);
                }
                Ordering::Equal => {}
            }
            self.in_mem_batches.push(partial);
        }
        Ok(())
    }

    fn spilled_before(&self) -> bool {
        !self.spills.is_empty()
    }

    /// MergeSort in mem batches as well as spills into total order with `SortPreservingMergeStream`.
    fn sort(&mut self) -> Result<SortedStream> {
        let batch_size = self.session_config.batch_size();

        if self.spilled_before() {
            let tracking_metrics = self
                .metrics_set
                .new_intermediate_tracking(self.partition_id, &self.runtime.memory_pool);
            let mut streams: Vec<SortedStream> = vec![];
            if !self.in_mem_batches.is_empty() {
                let mut stream = in_mem_partial_sort(
                    &mut self.in_mem_batches,
                    self.schema.clone(),
                    &self.expr,
                    batch_size,
                    tracking_metrics,
                    self.fetch,
                )?;
                let prev_used = self.reservation.free();
                stream.mem_used = prev_used;
                streams.push(stream);
            }
            let sort_fields = self
                .expr
                .iter()
                .map(|e| {
                    Ok(SortField::new_with_options(
                        e.expr.data_type(&self.schema)?,
                        e.options,
                    ))
                })
                .collect::<Result<Vec<_>>>()?;
            for spill in self.spills.drain(..) {
                let (rx, handle) = read_spill_as_stream(spill, sort_fields.to_owned())?;
                streams.push(SortedStream::new_from_rx(rx, handle, 0));
            }
            let tracking_metrics = self
                .metrics_set
                .new_final_tracking(self.partition_id, &self.runtime.memory_pool);
            let sort_stream = SortPreservingMergeStream::new_from_streams(
                streams,
                self.schema.clone(),
                &self.expr,
                tracking_metrics,
                self.session_config.batch_size(),
                true,
            )?;
            Ok(SortedStream::new(Box::pin(sort_stream), 0))
        } else if !self.in_mem_batches.is_empty() {
            let tracking_metrics = self
                .metrics_set
                .new_final_tracking(self.partition_id, &self.runtime.memory_pool);
            let stream = in_mem_partial_sort(
                &mut self.in_mem_batches,
                self.schema.clone(),
                &self.expr,
                batch_size,
                tracking_metrics,
                self.fetch,
            )?;
            // Report to the memory manager we are no longer using memory
            self.reservation.free();
            Ok(stream)
        } else {
            Ok(SortedStream::empty())
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

        let tracking_metrics = self
            .metrics_set
            .new_intermediate_tracking(self.partition_id, &self.runtime.memory_pool);
        let spillfile = self.runtime.disk_manager.create_tmp_file("Sorting")?;
        let mut stream = in_mem_partial_sort(
            &mut self.in_mem_batches,
            self.schema.clone(),
            &self.expr,
            self.session_config.batch_size(),
            tracking_metrics,
            self.fetch,
        )?;
        let rows_file = if stream.row_encoding_ignored {
            None
        } else {
            Some(
                self.runtime
                    .disk_manager
                    .create_tmp_file("Sorting row encodings")?,
            )
        };
        spill_partial_sorted_stream(
            &mut stream,
            spillfile.path(),
            rows_file.as_ref().map(|f| f.path()),
            self.schema.clone(),
        )
        .await?;
        self.reservation.free();
        let used = self.metrics.mem_used().set(0);
        self.metrics.record_spill(used);
        self.spills.push(Spill {
            record_batch_file: spillfile,
            rows_file,
        });
        Ok(used)
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

/// consume the non-empty `sorted_batches` and do in_mem_sort
fn in_mem_partial_sort(
    buffered_batches: &mut Vec<BatchWithSortArray>,
    schema: SchemaRef,
    expressions: &[PhysicalSortExpr],
    batch_size: usize,
    tracking_metrics: MemTrackingMetrics,
    fetch: Option<usize>,
) -> Result<SortedStream> {
    let (row_tx, row_rx) = mpsc::unbounded_channel();
    assert_ne!(buffered_batches.len(), 0);
    if buffered_batches.len() == 1 {
        let result = buffered_batches.pop().unwrap();
        let BatchWithSortArray {
            sort_data,
            sorted_batch,
        } = result;
        let rowbatch: Option<RowBatch> = sort_data.rows.map(Into::into);
        let stream = Box::pin(SizedRecordBatchStream::new(
            schema,
            vec![Arc::new(sorted_batch)],
            tracking_metrics,
        ));
        if let Some(rowbatch) = rowbatch {
            Ok(SortedStream::new_from_streams(
                stream,
                0,
                Box::pin(futures::stream::once(futures::future::ready(Some(
                    rowbatch,
                )))),
            ))
        } else {
            Ok(SortedStream::new_no_row_encoding(stream, 0))
        }
    } else {
        let (sort_data, batches): (Vec<SortData>, Vec<RecordBatch>) = buffered_batches
            .drain(..)
            .map(|b| {
                let BatchWithSortArray {
                    sort_data,
                    sorted_batch: batch,
                } = b;
                (sort_data, batch)
            })
            .unzip();

        let sorted_iter = {
            // NB timer records time taken on drop, so there are no
            // calls to `timer.done()` below.
            let _timer = tracking_metrics.elapsed_compute().timer();
            get_sorted_iter(&sort_data, expressions, batch_size, fetch)?
        };
        let rows = sort_data
            .into_iter()
            .map(|d| d.rows)
            .collect::<Option<Vec<_>>>();
        let used_rows = rows.is_some();
        let batch_stream = Box::pin(SortedSizedRecordBatchStream::new(
            schema,
            batches,
            sorted_iter,
            tracking_metrics,
            rows.map(|rs| rs.into_iter().map(Arc::new).collect()),
            Some(row_tx),
        ));
        if used_rows {
            let row_stream = UnboundedReceiverStream::new(row_rx).boxed();
            Ok(SortedStream::new_from_streams(batch_stream, 0, row_stream))
        } else {
            Ok(SortedStream::new_no_row_encoding(batch_stream, 0))
        }
    }
}

#[derive(Debug, Copy, Clone)]
struct CompositeIndex {
    batch_idx: u32,
    row_idx: u32,
}

/// Get sorted iterator by sort concatenated `SortColumn`s
fn get_sorted_iter(
    sort_data: &[SortData],
    expr: &[PhysicalSortExpr],
    batch_size: usize,
    fetch: Option<usize>,
) -> Result<SortedIterator> {
    let row_indices = sort_data
        .iter()
        .enumerate()
        .flat_map(|(i, d)| {
            (0..d.arrays[0].len()).map(move |r| CompositeIndex {
                // since we original use UInt32Array to index the combined mono batch,
                // component record batches won't overflow as well,
                // use u32 here for space efficiency.
                batch_idx: i as u32,
                row_idx: r as u32,
            })
        })
        .collect::<Vec<CompositeIndex>>();
    let rows_per_batch: Option<Vec<&RowSelection>> =
        sort_data.iter().map(|d| d.rows.as_ref()).collect();
    let indices = match rows_per_batch {
        Some(rows_per_batch) => {
            let mut to_sort = rows_per_batch
                .iter()
                .flat_map(|r| r.iter())
                .enumerate()
                .collect::<Vec<_>>();
            to_sort.sort_unstable_by(|(_, row_a), (_, row_b)| row_a.cmp(row_b));
            let limit = match fetch {
                Some(lim) => lim.min(to_sort.len()),
                None => to_sort.len(),
            };
            UInt32Array::from_iter(to_sort.iter().take(limit).map(|(idx, _)| *idx as u32))
        }
        None => {
            let sort_columns = expr
                .iter()
                .enumerate()
                .map(|(i, expr)| {
                    let columns_i = sort_data
                        .iter()
                        .map(|data| data.arrays[i].as_ref())
                        .collect::<Vec<&dyn Array>>();
                    Ok(SortColumn {
                        values: concat(columns_i.as_slice())?,
                        options: Some(expr.options),
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            lexsort_to_indices(&sort_columns, fetch)?
        }
    };

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
    rows: Option<Vec<Arc<RowSelection>>>,
    rows_tx: Option<mpsc::UnboundedSender<Option<RowBatch>>>,
}

impl SortedSizedRecordBatchStream {
    /// new
    pub fn new(
        schema: SchemaRef,
        batches: Vec<RecordBatch>,
        sorted_iter: SortedIterator,
        mut metrics: MemTrackingMetrics,
        rows: Option<Vec<Arc<RowSelection>>>,
        rows_tx: Option<mpsc::UnboundedSender<Option<RowBatch>>>,
    ) -> Self {
        let size = batches.iter().map(batch_byte_size).sum::<usize>()
            + sorted_iter.memory_size()
            // include rows if non-None
            + rows
                .as_ref()
                .map_or(0, |r| r.iter().map(|r| r.size()).sum());
        metrics.init_mem_used(size);
        let num_cols = batches[0].num_columns();
        SortedSizedRecordBatchStream {
            schema,
            batches,
            sorted_iter,
            rows,
            num_cols,
            metrics,
            rows_tx,
        }
    }
}

impl Stream for SortedSizedRecordBatchStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        match self.sorted_iter.next() {
            None => Poll::Ready(None),
            Some(slices) => {
                let num_rows = slices.iter().map(|s| s.len).sum();
                // create columns for record batch
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
                let batch =
                    RecordBatch::try_new(self.schema.clone(), output).map_err(Into::into);
                match batch {
                    Ok(batch) => {
                        // construct `RowBatch` batch if sorted row encodings were preserved
                        let row_batch = self.rows.as_ref().map(|rows| {
                            let row_refs =
                                rows.iter().map(Arc::clone).collect::<Vec<_>>();
                            let indices = slices
                                .iter()
                                .flat_map(|s| {
                                    (0..s.len).map(|i| {
                                        (
                                            s.batch_idx as usize,
                                            s.start_row_idx as usize + i,
                                        )
                                    })
                                })
                                .collect::<Vec<_>>();
                            RowBatch::new(row_refs, indices)
                        });

                        if let Some(ref tx) = self.rows_tx {
                            tx.send(row_batch).ok();
                        }
                        let poll = Poll::Ready(Some(Ok(batch)));
                        self.metrics.record_poll(poll)
                    }
                    Err(err) => {
                        let poll = Poll::Ready(Some(Err(err)));
                        self.metrics.record_poll(poll)
                    }
                }
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
    in_mem_stream: &mut SortedStream,
    path: &Path,
    row_path: Option<&Path>,
    schema: SchemaRef,
) -> Result<()> {
    let (sender, receiver) = mpsc::channel(2);
    let path: PathBuf = path.into();
    let row_path = row_path.map(|p| p.to_path_buf());
    let handle =
        task::spawn_blocking(move || write_sorted(receiver, path, row_path, schema));
    while let Some(item) = in_mem_stream.next().await {
        sender.send(item).await.ok();
    }
    drop(sender);
    match handle.await {
        Ok(r) => r,
        Err(e) => Err(DataFusionError::Execution(format!(
            "Error occurred while spilling {e}"
        ))),
    }
}

fn read_spill_as_stream(
    spill: Spill,
    sort_fields: Vec<SortField>,
) -> Result<(mpsc::Receiver<SortStreamItem>, JoinHandle<()>)> {
    let (sender, receiver) = mpsc::channel::<SortStreamItem>(2);
    let join_handle = task::spawn_blocking(move || {
        if let Err(e) = read_spill(sender, &spill, sort_fields) {
            error!(
                "Failure while reading spill file: ({:?}, {:?}). Error: {}",
                spill.record_batch_file, spill.rows_file, e
            );
        }
    });
    Ok((receiver, join_handle))
}

fn write_sorted(
    mut receiver: Receiver<SortStreamItem>,
    path: PathBuf,
    row_path: Option<PathBuf>,
    schema: SchemaRef,
) -> Result<()> {
    let mut writer = IPCWriter::new(path.as_ref(), schema.as_ref())?;
    let mut row_writer = RowWriter::try_new(row_path.as_ref())?;
    while let Some(batch) = receiver.blocking_recv() {
        let (recbatch, rows) = batch?;
        writer.write(&recbatch)?;
        row_writer.write(rows)?;
    }
    writer.finish()?;
    row_writer.finish()?;
    debug!(
        "Spilled {} batches of total {} rows to disk, memory released {}",
        writer.num_batches,
        writer.num_rows,
        human_readable_size(
            writer.num_bytes as usize + row_writer.num_row_bytes as usize
        ),
    );
    Ok(())
}

fn read_spill(
    sender: Sender<SortStreamItem>,
    spill: &Spill,
    sort_fields: Vec<SortField>,
) -> Result<()> {
    let file = BufReader::new(File::open(&spill.record_batch_file)?);
    let reader = FileReader::try_new(file, None)?;
    let row_reader = RowReader::try_new(spill.rows_file.as_ref(), sort_fields)?;
    for zipped in reader.zip(row_reader) {
        let item = match zipped {
            (Ok(batch), Ok(rows)) => Ok((batch, rows)),
            (Err(err), Ok(_)) | (Err(err), Err(_)) => Err(err.into()),
            (Ok(_), Err(err)) => Err(err),
        };
        sender
            .blocking_send(item)
            .map_err(|e| DataFusionError::Execution(format!("{e}")))?;
    }
    Ok(())
}

/// External Sort execution plan
#[derive(Debug)]
pub struct SortExec {
    /// Input schema
    pub(crate) input: Arc<dyn ExecutionPlan>,
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
    /// to be used by parent nodes to run execute that incldues the row
    /// encodings in the result stream
    pub(crate) fn execute_save_row_encoding(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableSortStream> {
        debug!("Start SortExec::execute for partition {} of context session_id {} and task_id {:?}", partition, context.session_id(), context.task_id());

        debug!(
            "Start invoking SortExec's input.execute for partition: {}",
            partition
        );

        let input = self.input.execute(partition, context.clone())?;

        debug!("End SortExec's input.execute for partition: {}", partition);
        Ok(Box::pin(
            futures::stream::once(do_sort(
                input,
                partition,
                self.expr.clone(),
                self.metrics_set.clone(),
                context,
                self.fetch(),
            ))
            .try_flatten(),
        ))
    }
    /// to be used by parent nodes to spawn execution into tokio threadpool
    /// and write results to `tx`
    pub(crate) fn execution_spawn_task(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
        tx: mpsc::Sender<SortStreamItem>,
    ) -> tokio::task::JoinHandle<()> {
        let input = self.input.clone();
        let expr = self.expr.clone();
        let metrics = self.metrics_set.clone();
        let fetch = self.fetch();
        let disp = displayable(input.as_ref()).one_line().to_string();
        tokio::spawn(async move {
            debug!("Start SortExec::execute for partition {} of context session_id {} and task_id {:?}", partition, context.session_id(), context.task_id());

            debug!(
                "Start invoking SortExec's input.execute for partition: {}",
                partition
            );
            let input = match input.execute(partition, context.clone()) {
                Err(e) => {
                    tx.send(Err(e)).await.ok();
                    return;
                }
                Ok(stream) => stream,
            };
            debug!("End SortExec's input.execute for partition: {}", partition);
            let mut sort_item_stream =
                match do_sort(input, partition, expr, metrics, context, fetch).await {
                    Ok(stream) => stream,
                    Err(err) => {
                        tx.send(Err(err)).await.ok();
                        return;
                    }
                };
            while let Some(item) = sort_item_stream.next().await {
                if tx.send(item).await.is_err() {
                    debug!("Stopping execution: output is gone, plan cancelling: {disp}");
                    return;
                }
            }
        })
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
    /// If the plan does not support pipelining, but it its input(s) are
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
            futures::stream::once(do_sort(
                input,
                partition,
                self.expr.clone(),
                self.metrics_set.clone(),
                context,
                self.fetch(),
            ))
            .try_flatten()
            .map_ok(|(record_batch, _rows)| record_batch),
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

struct SortData {
    arrays: Vec<ArrayRef>,
    rows: Option<RowSelection>,
}
struct BatchWithSortArray {
    sort_data: SortData,
    sorted_batch: RecordBatch,
}

fn sort_batch(
    batch: RecordBatch,
    schema: SchemaRef,
    expr: &[PhysicalSortExpr],
    fetch: Option<usize>,
) -> Result<BatchWithSortArray> {
    let sort_columns = expr
        .iter()
        .map(|e| e.evaluate_to_sort_column(&batch))
        .collect::<Result<Vec<SortColumn>>>()?;
    let (indices, rows) = match (sort_columns.len(), fetch) {
        // if single column or there's a limit, fallback to regular sort
        (1, None) | (_, Some(_)) => (lexsort_to_indices(&sort_columns, fetch)?, None),
        _ => {
            let sort_fields = sort_columns
                .iter()
                .map(|c| {
                    let datatype = c.values.data_type().to_owned();
                    SortField::new_with_options(datatype, c.options.unwrap_or_default())
                })
                .collect::<Vec<_>>();
            let arrays: Vec<ArrayRef> =
                sort_columns.iter().map(|c| c.values.clone()).collect();
            let mut row_converter = RowConverter::new(sort_fields)?;
            let rows = row_converter.convert_columns(&arrays)?;

            let mut to_sort: Vec<(usize, Row)> = rows.into_iter().enumerate().collect();
            to_sort.sort_unstable_by(|(_, row_a), (_, row_b)| row_a.cmp(row_b));
            let sorted_indices = to_sort.iter().map(|(idx, _)| *idx).collect::<Vec<_>>();
            (
                UInt32Array::from_iter(sorted_indices.iter().map(|i| *i as u32)),
                Some(RowSelection::new(rows, sorted_indices)),
            )
        }
    };

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
            .collect::<Result<Vec<ArrayRef>, ArrowError>>()?,
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
        sort_data: SortData {
            arrays: sort_arrays,
            rows,
        },
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
) -> Result<SortedStream> {
    debug!(
        "Start do_sort for partition {} of context session_id {} and task_id {:?}",
        partition_id,
        context.session_id(),
        context.task_id()
    );
    let schema = input.schema();
    let tracking_metrics =
        metrics_set.new_intermediate_tracking(partition_id, context.memory_pool());
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
        sorter.insert_batch(batch, &tracking_metrics).await?;
    }
    let result = sorter.sort();

    debug!(
        "End do_sort for partition {} of context session_id {} and task_id {:?}",
        partition_id,
        context.session_id(),
        context.task_id()
    );
    result
}

/// manages writing potential rows to and from disk
struct RowWriter {
    // serializing w/ arrow ipc format for maximum code simplicity... probably sub-optimal
    file: Option<File>,
    num_row_bytes: u32,
}
const MAGIC_BYTES: &[u8] = b"AROW";
impl RowWriter {
    fn try_new(path: Option<impl AsRef<Path>>) -> Result<Self> {
        match path {
            Some(p) => {
                let mut file = File::create(p)?;
                file.write_all(MAGIC_BYTES)?;
                Ok(Self {
                    file: Some(file),
                    num_row_bytes: 0,
                })
            }
            None => Ok(Self {
                file: None,
                num_row_bytes: 0,
            }),
        }
    }
    fn write(&mut self, rows: Option<RowBatch>) -> Result<()> {
        match (rows, self.file.as_mut()) {
            (Some(rows), Some(file)) => {
                file.write_all(&(rows.num_rows() as u32).to_le_bytes())?;
                for row in rows.iter() {
                    let bytes: &[u8] = row.as_ref();
                    let num_bytes = bytes.len() as u32;
                    self.num_row_bytes += num_bytes;
                    file.write_all(&num_bytes.to_le_bytes())?;
                    file.write_all(bytes)?;
                }
                Ok(())
            }
            // no-op
            _ => Ok(()),
        }
    }
    fn finish(&mut self) -> Result<()> {
        if let Some(file) = self.file.as_mut() {
            file.flush()?;
            Ok(())
        } else {
            Ok(())
        }
    }
}

/// manages reading potential rows to and from disk.
struct RowReader {
    /// temporary file format solution is storing it w/ arrow IPC
    file: Option<File>,
    row_conv: RowConverter,
    stopped: bool,
}
impl RowReader {
    fn try_new(
        path: Option<impl AsRef<Path>>,
        sort_fields: Vec<SortField>,
    ) -> Result<Self> {
        let row_conv = RowConverter::new(sort_fields)?;
        match path {
            Some(p) => {
                let mut file = File::open(p)?;
                let mut buf = [0_u8; 4];
                file.read_exact(&mut buf)?;
                if buf != MAGIC_BYTES {
                    return Err(DataFusionError::Internal(
                        "unexpected magic bytes in serialized rows file".to_owned(),
                    ));
                }
                Ok(Self {
                    file: Some(file),
                    row_conv,
                    stopped: false,
                })
            }
            None => Ok(Self {
                file: None,
                row_conv,
                stopped: false,
            }),
        }
    }

    fn read_batch(&mut self) -> Result<Option<RowBatch>> {
        let file = self.file.as_mut().unwrap();
        let mut buf = [0_u8; 4];
        match file.read_exact(&mut buf) {
            Ok(_) => {}
            Err(io_err) => {
                if io_err.kind() == std::io::ErrorKind::UnexpectedEof {
                    return Ok(None);
                }
                return Err(io_err.into());
            }
        }
        let num_rows = u32::from_le_bytes(buf);
        let mut bytes: Vec<Vec<u8>> = Vec::with_capacity(num_rows as usize);
        for _ in 0..num_rows {
            let mut buf = [0_u8; 4];
            file.read_exact(&mut buf)?;
            let n = u32::from_le_bytes(buf);
            let mut buf = vec![0_u8; n as usize];
            file.read_exact(&mut buf)?;
            bytes.push(buf);
        }
        Ok(Some(
            RowSelection::from_spilled(self.row_conv.parser(), bytes).into(),
        ))
    }
}
impl Iterator for RowReader {
    type Item = Result<Option<RowBatch>>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.stopped {
            return None;
        }
        if self.file.is_some() {
            let res = self.read_batch();
            match res {
                Ok(Some(batch)) => Some(Ok(Some(batch))),
                Ok(None) => None,
                Err(err) => {
                    self.stopped = true;
                    Some(Err(err))
                }
            }
        } else {
            // will be zipped with the main record batch reader so
            // just yield None forever
            Some(Ok(None))
        }
    }
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

    #[test]
    fn test_row_writer_reader() {
        use crate::prelude::SessionContext;
        use arrow::array::{Int64Array, StringArray};
        use arrow::datatypes::DataType;
        let sort_fields = vec![
            SortField::new(DataType::Int64),
            SortField::new(DataType::Utf8),
        ];
        let mut conv = RowConverter::new(sort_fields.to_owned()).unwrap();

        fn makebatch(n: i64) -> RecordBatch {
            let ints: Int64Array = (0..n).map(Some).collect();
            let varlengths: StringArray =
                StringArray::from_iter((0..n).map(|i| i + 100).map(|i| {
                    if i % 3 == 0 {
                        None
                    } else {
                        Some((i.pow(2)).to_string())
                    }
                }));
            RecordBatch::try_from_iter(vec![
                ("c1", Arc::new(ints) as _),
                ("c2", Arc::new(varlengths) as _),
            ])
            .unwrap()
        }
        let row_lens = vec![10, 0, 0, 1, 50];
        let batches = row_lens.iter().map(|i| makebatch(*i)).collect::<Vec<_>>();
        let rows = batches
            .iter()
            .map(|b| conv.convert_columns(b.columns()).unwrap())
            .collect::<Vec<_>>();

        let ctx = SessionContext::new();
        let runtime = ctx.runtime_env();
        let tempfile = runtime.disk_manager.create_tmp_file("Sorting").unwrap();
        let mut wr = RowWriter::try_new(Some(tempfile.path())).unwrap();
        for r in rows {
            wr.write(Some(r.into())).unwrap();
        }
        wr.finish().unwrap();

        let rdr = RowReader::try_new(Some(tempfile.path()), sort_fields).unwrap();
        let batches = rdr.collect::<Vec<_>>();
        assert_eq!(batches.len(), row_lens.len());
        let read_lens = batches
            .iter()
            .map(|b| {
                let rowbatch = b.as_ref().unwrap().as_ref().unwrap();
                rowbatch.num_rows() as i64
            })
            .collect::<Vec<_>>();
        assert_eq!(row_lens, read_lens);
    }

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
        // plus 1289 bytes of row data for each batch
        let row_size = 1289;
        let config = RuntimeConfig::new().with_memory_limit(12288 + (row_size * 4), 1.0);
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
        let avg_batch_size = 5336;
        let partitions = 4;
        let added_row_size = 1289 * partitions;

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
            let config = RuntimeConfig::new().with_memory_limit(
                avg_batch_size * (partitions - 1) + added_row_size,
                1.0,
            );
            let runtime = Arc::new(RuntimeEnv::new(config)?);
            let session_ctx =
                SessionContext::with_config_rt(SessionConfig::new(), runtime);

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
                fetch,
            )?);

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
            session_ctx.runtime_env().memory_pool.reserved(),
            0,
            "The sort should have returned all memory used back to the memory manager"
        );

        Ok(())
    }
}
