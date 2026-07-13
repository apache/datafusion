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

//! [`BatchNormalizer`]: re-chunk a stream of [`RecordBatch`]es towards a
//! target row count *and* byte size.
//!
//! Operators generally bound their output batches by row count
//! (`datafusion.execution.batch_size`) only. With wide rows (large strings,
//! many columns) a batch within the row limit can still be arbitrarily large
//! in bytes (multiple GB), which breaks memory accounting assumptions and
//! causes OOMs. Conversely, streams of tiny batches waste per-batch overhead.
//!
//! [`BatchNormalizer`] addresses both with a single component that, per input
//! batch, takes one of four actions:
//!
//! 1. **Pass through** (zero copy): the batch is already acceptably sized.
//!    The acceptance band is deliberately wide so that near-target batches
//!    are never copied.
//! 2. **Coalesce**: small batches are buffered (copied) and emitted once the
//!    buffer reaches the target row count *or* the target byte size,
//!    whichever comes first.
//! 3. **Split**: oversized batches (by rows or by bytes) are re-emitted as
//!    compact copies of roughly the target size. Note that copying is
//!    load-bearing: `RecordBatch::slice` is zero-copy, so a plain slice of a
//!    4GB batch still pins the entire 4GB of buffers. Splitting must
//!    materialize fresh buffers to actually release memory.
//! 4. **Compact**: batches whose logical (sliced) size is small but which
//!    pin much larger buffers (e.g. a small slice of a huge batch, or a
//!    StringView array referencing mostly-garbage data buffers) are copied
//!    so the underlying buffers can be freed.
//!
//! The copy machinery is arrow's [`BatchCoalescer`], which compacts all
//! buffered data (including StringView garbage collection).

use std::collections::VecDeque;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, ready};

use arrow::array::{
    Array, BinaryViewArray, GenericByteViewArray, RecordBatch, StringViewArray,
    UInt64Array,
};
use arrow::compute::{BatchCoalescer, take_record_batch};
use arrow::datatypes::{ByteViewType, SchemaRef};
use datafusion_common::utils::memory::get_record_batch_memory_size;
use datafusion_common::{Result, assert_or_internal_err};
use datafusion_execution::memory_pool::MemoryLimit;
use datafusion_execution::{RecordBatchStream, SendableRecordBatchStream, TaskContext};
use futures::{Stream, StreamExt};

use crate::metrics::{Count, ExecutionPlanMetricsSet, MetricBuilder};
use crate::spill::gc_view_arrays;
use crate::stream::EmptyRecordBatchStream;

/// Multiple of `target_bytes` above which a batch is split.
///
/// The generous slop avoids wasting work copying batches that are only
/// somewhat over target (splitting copies data; see module docs).
const SPLIT_SLOP_FACTOR: usize = 2;

/// A batch is "wasteful" (and gets compacted) when the memory it retains is
/// more than this multiple of its logical size...
const WASTE_RATIO: usize = 2;

/// ...and the absolute waste exceeds `max(MIN_WASTE_BYTES, target_bytes)`.
/// Compaction copies data, which only pays off when it frees memory that is
/// large on the scale the byte target cares about. In particular,
/// parquet-decoded string/view batches routinely retain 2-4x their logical
/// size because decode buffers are shared across consecutive batches;
/// compacting every such batch costs a copy per batch for no benefit (the
/// batches die immediately in streaming pipelines).
const MIN_WASTE_BYTES: usize = 1024 * 1024;

/// Divisor applied to the per-partition share of a finite memory pool to
/// derive an adaptive byte target: `pool_size / target_partitions /
/// ADAPTIVE_TARGET_DIVISOR`.
///
/// The margin covers what each partition's operators need per batch beyond
/// the batch itself: an external sort reserves ~2x a batch's size to buffer
/// it, its spill merge reserves ~4x the largest spilled batch per stream
/// (with a read-ahead of 2), and the final sort-preserving merge buffers
/// batches from every stream concurrently. /16 keeps several such batches
/// per partition comfortably inside the partition's fair share, empirically
/// turning "ResourcesExhausted on oversized batches" into completed queries
/// without shrinking batches so far that per-batch overhead dominates.
pub const ADAPTIVE_TARGET_DIVISOR: usize = 16;

/// Adaptive targeting only activates when the derived target reaches this
/// minimum (i.e. the pool holds at least 16MiB per partition). Below it the
/// pool is too small for re-chunking to change the outcome, so behavior is
/// left exactly as with no target -- this also keeps carefully-constructed
/// small-pool OOM scenarios (e.g. in tests) unchanged.
pub const MIN_ADAPTIVE_TARGET_BYTES: usize = 1024 * 1024;

/// Ceiling for the adaptive byte target. Batches beyond ~16MiB buy no
/// per-batch-overhead amortization, and empirically the sort/merge machinery
/// under a tight memory limit is only reliably stable when spilled batches
/// stay at or below this size; a larger pool share is headroom, not an
/// invitation for bigger batches.
pub const MAX_ADAPTIVE_TARGET_BYTES: usize = 16 * 1024 * 1024;

/// Resolve the effective byte-size target for batches under this task
/// context:
///
/// 1. An explicit `datafusion.execution.target_batch_size_bytes` always wins.
/// 2. Otherwise, if `datafusion.execution.adaptive_target_batch_size` is on
///    and the memory pool reports a finite limit, the target is derived from
///    the per-partition share of the pool (see [`ADAPTIVE_TARGET_DIVISOR`]).
///    With no memory limit there is nothing to protect, so no target is set
///    and batches are never split or copied.
/// 3. `None` otherwise: batches are chunked by row count only.
pub fn effective_target_batch_size_bytes(context: &TaskContext) -> Option<usize> {
    let options = context.session_config().options();
    if let Some(target) = options.execution.target_batch_size_bytes {
        return Some(target);
    }
    if !options.execution.adaptive_target_batch_size {
        return None;
    }
    match context.memory_pool().memory_limit() {
        MemoryLimit::Finite(pool_size) => {
            let partitions = options.execution.target_partitions.max(1);
            let target = pool_size / partitions / ADAPTIVE_TARGET_DIVISOR;
            (target >= MIN_ADAPTIVE_TARGET_BYTES)
                .then(|| target.min(MAX_ADAPTIVE_TARGET_BYTES))
        }
        _ => None,
    }
}

/// Metrics for [`BatchNormalizer`]
#[derive(Debug, Clone, Default)]
pub struct BatchNormalizerMetrics {
    /// Input batches emitted unchanged (zero copy)
    pub batches_passed_through: Count,
    /// Input batches buffered for coalescing
    pub batches_coalesced: Count,
    /// Input batches split because they exceeded the row or byte target
    pub batches_split: Count,
    /// Input batches copied to release pinned memory
    pub batches_compacted: Count,
}

impl BatchNormalizerMetrics {
    pub fn new(metrics: &ExecutionPlanMetricsSet, partition: usize) -> Self {
        Self {
            batches_passed_through: MetricBuilder::new(metrics)
                .counter("batches_passed_through", partition),
            batches_coalesced: MetricBuilder::new(metrics)
                .counter("batches_coalesced", partition),
            batches_split: MetricBuilder::new(metrics)
                .counter("batches_split", partition),
            batches_compacted: MetricBuilder::new(metrics)
                .counter("batches_compacted", partition),
        }
    }
}

/// Logical (materialized) size of a batch in bytes: the number of bytes
/// needed if exactly the rows in this batch were copied into fresh,
/// minimally-sized buffers.
///
/// This is intentionally different from
/// [`get_record_batch_memory_size`], which returns the memory the batch
/// *retains* (full backing buffers, including the parts a slice does not
/// reference). The ratio between the two is used to detect wasteful batches.
fn logical_batch_size(batch: &RecordBatch) -> Result<usize> {
    let mut total = 0;
    for array in batch.columns() {
        let data = array.to_data();
        total += data.get_slice_memory_size()?;
        // `get_slice_memory_size` does not count the variable-length data
        // buffers of view arrays (arrow-rs#8230): add the bytes actually
        // referenced by the (non-inlined) views. Nested view arrays are not
        // corrected, matching the spill code's `GetSlicedSize`.
        if let Some(a) = array.as_any().downcast_ref::<StringViewArray>() {
            total += referenced_view_bytes(a);
        } else if let Some(a) = array.as_any().downcast_ref::<BinaryViewArray>() {
            total += referenced_view_bytes(a);
        }
    }
    Ok(total)
}

/// Total bytes in the data buffers referenced by non-null, non-inlined views
fn referenced_view_bytes<T: ByteViewType>(array: &GenericByteViewArray<T>) -> usize {
    let nulls = array.nulls();
    array
        .views()
        .iter()
        .enumerate()
        .map(|(i, v)| {
            if nulls.map(|n| n.is_null(i)).unwrap_or(false) {
                return 0;
            }
            // low 32 bits of a view are the value's length; values longer
            // than 12 bytes live in the data buffers
            let len = (*v as u32) as usize;
            if len > 12 { len } else { 0 }
        })
        .sum()
}

/// Copy `length` rows of `batch` starting at `offset` into fresh,
/// minimally-sized buffers.
///
/// This must NOT be implemented with `slice`/`concat`: `RecordBatch::slice`
/// is zero-copy and arrow's `concat` short-circuits a single input array to
/// a zero-copy slice, so neither releases the parent batch's buffers. `take`
/// materializes fresh buffers for all types; view arrays additionally need a
/// garbage-collection pass because `take` copies their (small) views while
/// still sharing the underlying data buffers.
fn compact_slice(
    batch: &RecordBatch,
    offset: usize,
    length: usize,
) -> Result<RecordBatch> {
    let indices = UInt64Array::from_iter_values(offset as u64..(offset + length) as u64);
    let taken = take_record_batch(batch, &indices)?;
    gc_view_arrays(&taken)
}

/// An oversized batch being split incrementally: one chunk is copied out per
/// [`BatchNormalizer::produce`] call so that peak memory stays at the parent
/// batch plus ~one chunk, rather than the parent plus all its chunks.
#[derive(Debug)]
struct PendingSplit {
    batch: RecordBatch,
    offset: usize,
    chunk_rows: usize,
    /// If true the chunks are emitted as zero-copy slices instead of compact
    /// copies. Only used for batches with no columns (row count only), where
    /// a slice retains nothing.
    zero_copy: bool,
}

/// See [module docs](self) for details.
///
/// This is the sans-IO core; [`BatchNormalizerStream`] adapts it to a
/// [`SendableRecordBatchStream`].
///
/// Call [`push_batch`](Self::push_batch) when [`produce`](Self::produce)
/// returns `Ok(None)`, and [`finish`](Self::finish) at end of input to flush
/// any buffered rows.
#[derive(Debug)]
pub struct BatchNormalizer {
    schema: SchemaRef,
    /// Copy/compaction machinery: buffers and compacts pushed batches
    /// (including StringView garbage collection), auto-completing an output
    /// batch whenever `target_rows` rows are buffered.
    inner: BatchCoalescer,
    target_rows: usize,
    target_bytes: usize,
    /// Logical bytes currently buffered in `inner`
    buffered_bytes: usize,
    /// Output batches ready to be returned by `produce`
    completed: VecDeque<RecordBatch>,
    /// Oversized batch currently being split, if any
    pending_split: Option<PendingSplit>,
    finished: bool,
    metrics: BatchNormalizerMetrics,
}

impl BatchNormalizer {
    /// Create a new normalizer targeting `target_rows` rows and
    /// `target_bytes` (logical) bytes per output batch.
    pub fn new(
        schema: SchemaRef,
        target_rows: usize,
        target_bytes: usize,
        metrics: BatchNormalizerMetrics,
    ) -> Self {
        Self {
            inner: BatchCoalescer::new(Arc::clone(&schema), target_rows),
            schema,
            target_rows,
            target_bytes: target_bytes.max(1),
            buffered_bytes: 0,
            completed: VecDeque::new(),
            pending_split: None,
            finished: false,
            metrics,
        }
    }

    pub fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    /// Push an input batch.
    ///
    /// Must only be called when [`produce`](Self::produce) returned
    /// `Ok(None)` (i.e. all pending output has been drained) and before
    /// [`finish`](Self::finish).
    pub fn push_batch(&mut self, batch: RecordBatch) -> Result<()> {
        assert_or_internal_err!(
            !self.finished,
            "BatchNormalizer: push_batch after finish"
        );
        assert_or_internal_err!(
            self.pending_split.is_none(),
            "BatchNormalizer: push_batch while a split is pending; drain produce() first"
        );

        let rows = batch.num_rows();
        if rows == 0 {
            return Ok(());
        }

        // Row-count-only batches: nothing to copy, split by rows if needed
        if batch.num_columns() == 0 {
            if rows > self.target_rows {
                self.metrics.batches_split.add(1);
                self.pending_split = Some(PendingSplit {
                    batch,
                    offset: 0,
                    chunk_rows: self.target_rows,
                    zero_copy: true,
                });
            } else {
                self.metrics.batches_passed_through.add(1);
                self.completed.push_back(batch);
            }
            return Ok(());
        }

        let logical = logical_batch_size(&batch)?;

        // Oversized (by rows or bytes): split into ~target-sized chunks
        if rows > self.target_rows || logical > SPLIT_SLOP_FACTOR * self.target_bytes {
            let bytes_per_row = (logical / rows).max(1);
            let chunk_rows =
                (self.target_bytes / bytes_per_row).clamp(1, self.target_rows);
            if chunk_rows < rows {
                self.metrics.batches_split.add(1);
                // Chunks are emitted directly (not via the coalescing
                // buffer), so flush buffered rows first to preserve order
                self.flush_buffer()?;
                self.pending_split = Some(PendingSplit {
                    batch,
                    offset: 0,
                    chunk_rows,
                    zero_copy: false,
                });
                return Ok(());
            }
            // A single row wider than the split threshold cannot be split
            // further: emit as-is. (`chunk_rows >= rows` is only reachable
            // here for `rows == 1`.)
            return self.pass_through(batch);
        }

        // Acceptable size, but does it pin much more memory than it uses?
        // (e.g. a small slice of a huge batch): copy it so the backing
        // buffers can be freed.
        let retained = get_record_batch_memory_size(&batch);
        let min_waste = MIN_WASTE_BYTES.max(self.target_bytes);
        let wasteful = retained > WASTE_RATIO * logical
            && retained.saturating_sub(logical) >= min_waste;
        let batch = if wasteful {
            self.metrics.batches_compacted.add(1);
            compact_slice(&batch, 0, rows)?
        } else {
            batch
        };

        // In the acceptance band: emit as-is
        if rows >= self.target_rows / 2 || logical >= self.target_bytes / 2 {
            if !wasteful {
                self.metrics.batches_passed_through.add(1);
            }
            return self.emit(batch);
        }

        // Small: buffer for coalescing
        if !wasteful {
            self.metrics.batches_coalesced.add(1);
        }
        self.buffer(batch, logical)
    }

    /// Signal end of input, flushing any buffered rows.
    pub fn finish(&mut self) -> Result<()> {
        assert_or_internal_err!(
            self.pending_split.is_none(),
            "BatchNormalizer: finish while a split is pending; drain produce() first"
        );
        self.flush_buffer()?;
        self.finished = true;
        Ok(())
    }

    /// Return the next output batch, doing incremental split work if an
    /// oversized batch is pending. Returns `Ok(None)` when more input is
    /// needed (or, after [`finish`](Self::finish), when fully drained).
    pub fn produce(&mut self) -> Result<Option<RecordBatch>> {
        loop {
            if let Some(batch) = self.completed.pop_front() {
                return Ok(Some(batch));
            }
            let Some(mut pending) = self.pending_split.take() else {
                return Ok(None);
            };
            // Copy out the next chunk of the pending oversized batch
            let remaining = pending.batch.num_rows() - pending.offset;
            let take = remaining.min(pending.chunk_rows);
            let chunk = if pending.zero_copy {
                pending.batch.slice(pending.offset, take)
            } else {
                compact_slice(&pending.batch, pending.offset, take)?
            };
            pending.offset += take;
            if pending.offset < pending.batch.num_rows() {
                self.pending_split = Some(pending);
            }
            self.completed.push_back(chunk);
        }
    }

    /// Push `batch` (of logical size `logical`) into the copying buffer,
    /// flushing if the byte target is reached. Arrow's coalescer flushes by
    /// itself when the row target is reached.
    fn buffer(&mut self, batch: RecordBatch, logical: usize) -> Result<()> {
        self.inner.push_batch(batch)?;
        self.buffered_bytes += logical;
        self.drain_inner()?;
        if self.buffered_bytes >= self.target_bytes {
            self.flush_buffer()?;
        }
        Ok(())
    }

    /// Emit `batch` unchanged, zero copy
    fn pass_through(&mut self, batch: RecordBatch) -> Result<()> {
        self.metrics.batches_passed_through.add(1);
        self.emit(batch)
    }

    /// Emit `batch`, flushing any buffered rows first to preserve input order
    fn emit(&mut self, batch: RecordBatch) -> Result<()> {
        self.flush_buffer()?;
        self.completed.push_back(batch);
        Ok(())
    }

    /// Complete the currently buffered rows (if any) as an output batch
    fn flush_buffer(&mut self) -> Result<()> {
        if self.inner.get_buffered_rows() > 0 {
            self.inner.finish_buffered_batch()?;
            self.drain_inner()?;
        }
        self.buffered_bytes = 0;
        Ok(())
    }

    /// Move batches completed by the inner coalescer into the output queue
    fn drain_inner(&mut self) -> Result<()> {
        while let Some(batch) = self.inner.next_completed_batch() {
            // completed batches are freshly compacted: logical == retained
            self.buffered_bytes = self
                .buffered_bytes
                .saturating_sub(logical_batch_size(&batch)?);
            self.completed.push_back(batch);
        }
        Ok(())
    }
}

/// Stream adapter for [`BatchNormalizer`].
///
/// Applies the normalizer to `input`, forwarding errors and flushing
/// buffered rows when the input is exhausted.
pub struct BatchNormalizerStream {
    input: SendableRecordBatchStream,
    normalizer: BatchNormalizer,
    input_done: bool,
}

impl BatchNormalizerStream {
    pub fn new(
        input: SendableRecordBatchStream,
        target_rows: usize,
        target_bytes: usize,
        metrics: BatchNormalizerMetrics,
    ) -> Self {
        let normalizer =
            BatchNormalizer::new(input.schema(), target_rows, target_bytes, metrics);
        Self {
            input,
            normalizer,
            input_done: false,
        }
    }
}

impl Stream for BatchNormalizerStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        loop {
            match self.normalizer.produce() {
                Ok(Some(batch)) => return Poll::Ready(Some(Ok(batch))),
                Ok(None) => {}
                Err(e) => return Poll::Ready(Some(Err(e))),
            }
            if self.input_done {
                return Poll::Ready(None);
            }
            match ready!(self.input.poll_next_unpin(cx)) {
                Some(Ok(batch)) => {
                    if let Err(e) = self.normalizer.push_batch(batch) {
                        return Poll::Ready(Some(Err(e)));
                    }
                }
                Some(Err(e)) => return Poll::Ready(Some(Err(e))),
                None => {
                    if let Err(e) = self.normalizer.finish() {
                        return Poll::Ready(Some(Err(e)));
                    }
                    // Release the input pipeline's resources
                    let input_schema = self.input.schema();
                    self.input = Box::pin(EmptyRecordBatchStream::new(input_schema));
                    self.input_done = true;
                }
            }
        }
    }
}

impl RecordBatchStream for BatchNormalizerStream {
    fn schema(&self) -> SchemaRef {
        self.normalizer.schema()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{ArrayRef, Int64Array, StringArray};
    use arrow::compute::concat_batches;
    use arrow::datatypes::{DataType, Field, Schema};
    use futures::TryStreamExt;

    const TARGET_ROWS: usize = 100;
    const TARGET_BYTES: usize = 10 * 1024; // 10 KiB

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("s", DataType::Utf8, false),
        ]))
    }

    /// Batch with `rows` rows where each string value is `str_len` bytes
    fn string_batch(rows: usize, str_len: usize) -> RecordBatch {
        let ids = Int64Array::from_iter_values(0..rows as i64);
        let s =
            StringArray::from_iter_values((0..rows).map(|i| format!("{i:0>str_len$}")));
        RecordBatch::try_new(
            test_schema(),
            vec![Arc::new(ids) as ArrayRef, Arc::new(s) as ArrayRef],
        )
        .unwrap()
    }

    fn normalizer() -> BatchNormalizer {
        BatchNormalizer::new(
            test_schema(),
            TARGET_ROWS,
            TARGET_BYTES,
            BatchNormalizerMetrics::default(),
        )
    }

    /// Push all batches, then finish, collecting every produced batch.
    fn run(
        normalizer: &mut BatchNormalizer,
        batches: impl IntoIterator<Item = RecordBatch>,
    ) -> Vec<RecordBatch> {
        let mut out = vec![];
        for batch in batches {
            normalizer.push_batch(batch).unwrap();
            while let Some(b) = normalizer.produce().unwrap() {
                out.push(b);
            }
        }
        normalizer.finish().unwrap();
        while let Some(b) = normalizer.produce().unwrap() {
            out.push(b);
        }
        out
    }

    /// Assert that the concatenation of `outputs` equals concatenation of `inputs`
    fn assert_same_data(inputs: &[RecordBatch], outputs: &[RecordBatch]) {
        let schema = inputs[0].schema();
        let expected = concat_batches(&schema, inputs).unwrap();
        let actual = concat_batches(&schema, outputs).unwrap();
        assert_eq!(expected, actual);
    }

    fn logical_size(batch: &RecordBatch) -> usize {
        logical_batch_size(batch).unwrap()
    }

    // === adaptive target ===

    #[test]
    fn adaptive_target_from_memory_limit() {
        use datafusion_execution::TaskContext;
        use datafusion_execution::config::SessionConfig;
        use datafusion_execution::memory_pool::{FairSpillPool, GreedyMemoryPool};
        use datafusion_execution::runtime_env::RuntimeEnvBuilder;

        let ctx_with =
            |pool: Option<Arc<dyn datafusion_execution::memory_pool::MemoryPool>>,
             target: Option<usize>,
             adaptive: bool,
             partitions: usize| {
                let mut config = SessionConfig::new().with_target_partitions(partitions);
                config.options_mut().execution.target_batch_size_bytes = target;
                config.options_mut().execution.adaptive_target_batch_size = adaptive;
                let mut rt = RuntimeEnvBuilder::new();
                if let Some(pool) = pool {
                    rt = rt.with_memory_pool(pool);
                }
                TaskContext::default()
                    .with_session_config(config)
                    .with_runtime(Arc::new(rt.build().unwrap()))
            };

        // no limit, no explicit target -> disabled
        let ctx = ctx_with(None, None, true, 8);
        assert_eq!(effective_target_batch_size_bytes(&ctx), None);

        // finite pool -> pool_size / partitions / ADAPTIVE_TARGET_DIVISOR
        let pool: Arc<dyn datafusion_execution::memory_pool::MemoryPool> =
            Arc::new(FairSpillPool::new(2 * 1024 * 1024 * 1024));
        let ctx = ctx_with(Some(Arc::clone(&pool)), None, true, 8);
        assert_eq!(
            effective_target_batch_size_bytes(&ctx),
            Some(2 * 1024 * 1024 * 1024 / 8 / ADAPTIVE_TARGET_DIVISOR)
        );

        // explicit target always wins
        let ctx = ctx_with(Some(Arc::clone(&pool)), Some(123456), true, 8);
        assert_eq!(effective_target_batch_size_bytes(&ctx), Some(123456));

        // adaptive disabled -> None even with a finite pool
        let ctx = ctx_with(Some(Arc::clone(&pool)), None, false, 8);
        assert_eq!(effective_target_batch_size_bytes(&ctx), None);

        // huge pool / few partitions -> clamped to the 16MiB ceiling
        let pool: Arc<dyn datafusion_execution::memory_pool::MemoryPool> =
            Arc::new(GreedyMemoryPool::new(64 * 1024 * 1024 * 1024));
        let ctx = ctx_with(Some(pool), None, true, 4);
        assert_eq!(
            effective_target_batch_size_bytes(&ctx),
            Some(MAX_ADAPTIVE_TARGET_BYTES)
        );

        // pool too small for the minimum target -> adaptive stays off so
        // small-pool behavior (and crafted OOM scenarios) is unchanged
        let pool: Arc<dyn datafusion_execution::memory_pool::MemoryPool> =
            Arc::new(GreedyMemoryPool::new(64 * 1024 * 1024));
        let ctx = ctx_with(Some(pool), None, true, 512);
        assert_eq!(effective_target_batch_size_bytes(&ctx), None);
    }

    // === pass through ===

    #[test]
    fn passthrough_normal_batch_is_zero_copy() {
        // rows == target, bytes within band -> exact same arrays out
        let batch = string_batch(TARGET_ROWS, 20);
        let out = run(&mut normalizer(), [batch.clone()]);
        assert_eq!(out.len(), 1);
        for (in_col, out_col) in batch.columns().iter().zip(out[0].columns()) {
            assert!(
                Arc::ptr_eq(in_col, out_col),
                "expected zero-copy pass through"
            );
        }
    }

    #[test]
    fn passthrough_by_bytes_with_few_rows() {
        // Only 10 rows (< target_rows/2) but ~8KiB (> target_bytes/2):
        // "few wide rows" is an acceptable batch, not one to coalesce
        let batch = string_batch(10, 800);
        assert!(logical_size(&batch) >= TARGET_BYTES / 2);
        let out = run(&mut normalizer(), [batch.clone()]);
        assert_eq!(out.len(), 1);
        assert!(Arc::ptr_eq(batch.column(1), out[0].column(1)));
    }

    #[test]
    fn passthrough_slightly_oversized_batch() {
        // ~1.6x target bytes: within the slop band, must NOT be split/copied
        let batch = string_batch(TARGET_ROWS, 160);
        let size = logical_size(&batch);
        assert!(size > TARGET_BYTES && size < SPLIT_SLOP_FACTOR * TARGET_BYTES);
        let out = run(&mut normalizer(), [batch.clone()]);
        assert_eq!(out.len(), 1);
        assert!(Arc::ptr_eq(batch.column(1), out[0].column(1)));
    }

    // === coalesce ===

    #[test]
    fn coalesce_small_batches_by_rows() {
        // 10-row tiny batches accumulate to target_rows
        let inputs: Vec<_> = (0..25).map(|_| string_batch(10, 5)).collect();
        let out = run(&mut normalizer(), inputs.clone());
        assert_same_data(&inputs, &out);
        assert_eq!(
            out.iter().map(|b| b.num_rows()).collect::<Vec<_>>(),
            vec![100, 100, 50],
        );
    }

    #[test]
    fn coalesce_flushes_on_bytes_before_rows() {
        // 10 rows x ~400B = ~4KiB logical per batch; the byte target (10KiB)
        // is reached after ~3 batches, well before 100 buffered rows
        let inputs: Vec<_> = (0..6).map(|_| string_batch(10, 400)).collect();
        let out = run(&mut normalizer(), inputs.clone());
        assert_same_data(&inputs, &out);
        assert!(out.len() >= 2, "byte target should have forced a flush");
        for b in &out {
            assert!(b.num_rows() < TARGET_ROWS);
            assert!(logical_size(b) <= SPLIT_SLOP_FACTOR * TARGET_BYTES);
        }
    }

    // === split ===

    #[test]
    fn split_wide_batch_by_bytes() {
        // 50 rows x ~1KiB = ~51KiB >> 2x target (20KiB): split into ~10-row
        // chunks of ~target_bytes each
        let batch = string_batch(50, 1024);
        let inputs = [batch.clone()];
        let out = run(&mut normalizer(), inputs.clone());
        assert_same_data(&inputs, &out);
        assert!(out.len() > 1, "expected batch to be split");
        for b in &out {
            assert!(
                logical_size(b) <= SPLIT_SLOP_FACTOR * TARGET_BYTES,
                "chunk of {} bytes exceeds split threshold",
                logical_size(b)
            );
            // chunks must be compact copies: they must not retain the
            // original ~51KiB of buffers
            assert!(
                get_record_batch_memory_size(b) < logical_size(&batch) / 2,
                "chunk retains the parent batch's buffers"
            );
        }
    }

    #[test]
    fn split_by_rows() {
        // 1000 tiny rows > target_rows: split into 100-row chunks
        let batch = string_batch(1000, 5);
        let inputs = [batch.clone()];
        let out = run(&mut normalizer(), inputs.clone());
        assert_same_data(&inputs, &out);
        assert_eq!(out.len(), 10);
        for b in &out {
            assert_eq!(b.num_rows(), TARGET_ROWS);
        }
    }

    #[test]
    fn split_produces_output_incrementally() {
        // Oversized batches must not materialize all chunks eagerly:
        // after push, produce() yields chunks one at a time
        let batch = string_batch(50, 1024);
        let mut n = normalizer();
        n.push_batch(batch).unwrap();
        let first = n.produce().unwrap();
        assert!(first.is_some());
        // there must be more chunks pending
        let second = n.produce().unwrap();
        assert!(second.is_some());
    }

    #[test]
    fn giant_single_row_passes_through() {
        // One row larger than the split threshold cannot be split further;
        // it must be emitted as-is (and not loop or error)
        let batch = string_batch(1, 5 * TARGET_BYTES);
        let inputs = [batch.clone()];
        let out = run(&mut normalizer(), inputs.clone());
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].num_rows(), 1);
        assert_same_data(&inputs, &out);
    }

    #[test]
    fn split_string_view_batch_releases_data_buffers() {
        // View arrays need explicit GC when split: a copied chunk's views
        // would otherwise still reference the parent's large data buffers
        use arrow::array::StringViewArray;
        let rows = 50;
        let s =
            StringViewArray::from_iter_values((0..rows).map(|i| format!("{i:0>1024}")));
        let schema = Arc::new(Schema::new(vec![Field::new(
            "s",
            DataType::Utf8View,
            false,
        )]));
        let batch =
            RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(s) as ArrayRef])
                .unwrap();
        let parent_size = get_record_batch_memory_size(&batch);
        let mut n = BatchNormalizer::new(
            schema,
            TARGET_ROWS,
            TARGET_BYTES,
            BatchNormalizerMetrics::default(),
        );
        let inputs = [batch];
        let out = run(&mut n, inputs.clone());
        assert_same_data(&inputs, &out);
        assert!(out.len() > 1, "expected batch to be split");
        for b in &out {
            assert!(
                get_record_batch_memory_size(b) < parent_size / 2,
                "chunk retains the parent's view data buffers"
            );
        }
    }

    // === compact ===

    #[test]
    fn wasteful_slice_is_compacted() {
        // A small slice of a big batch pins the whole parent buffer.
        // logical size is in the pass-through band, but retained size is
        // ~50x larger -> must be copied so the parent can be freed.
        let parent = string_batch(10_000, 300); // ~3MB
        let slice = parent.slice(0, 20); // ~6KiB logical
        let logical = logical_size(&slice);
        assert!(logical >= TARGET_BYTES / 2, "slice must be in pass band");
        assert!(get_record_batch_memory_size(&slice) > WASTE_RATIO * logical);

        let inputs = [slice.clone()];
        let out = run(&mut normalizer(), inputs.clone());
        assert_same_data(&inputs, &out);
        assert_eq!(out.len(), 1);
        assert!(
            get_record_batch_memory_size(&out[0]) < logical * 2,
            "output still retains the parent's buffers"
        );
    }

    #[test]
    fn small_waste_is_not_compacted() {
        // Waste below MIN_WASTE_BYTES is not worth a copy: a slice of a
        // small parent should pass through zero-copy
        let parent = string_batch(200, 60);
        let slice = parent.slice(0, TARGET_ROWS);
        let out = run(&mut normalizer(), [slice.clone()]);
        assert_eq!(out.len(), 1);
        assert!(Arc::ptr_eq(slice.column(1), out[0].column(1)));
    }

    #[test]
    fn moderately_wasteful_batch_below_target_passes_through() {
        // ClickBench regression shape: parquet-decoded view/string batches
        // routinely retain 2-4x their logical size because decode buffers are
        // shared across consecutive batches. Waste that is large in ratio but
        // small relative to target_bytes must NOT trigger a per-batch copy --
        // in a streaming pipeline these batches die immediately and
        // compacting them is pure overhead.
        let target_bytes = 16 * 1024 * 1024;
        let parent = string_batch(16 * 1024, 1024); // ~17MB retained
        let slice = parent.slice(0, 5000); // ~5MB logical, ~12MB waste, ratio >2
        let logical = logical_size(&slice);
        let retained = get_record_batch_memory_size(&slice);
        assert!(retained > WASTE_RATIO * logical);
        assert!(retained - logical > MIN_WASTE_BYTES);
        assert!(retained - logical < target_bytes);

        let mut n = BatchNormalizer::new(
            test_schema(),
            8192,
            target_bytes,
            BatchNormalizerMetrics::default(),
        );
        let out = run(&mut n, [slice.clone()]);
        assert_eq!(out.len(), 1);
        assert!(
            Arc::ptr_eq(slice.column(1), out[0].column(1)),
            "moderately wasteful batch should pass through zero-copy"
        );
    }

    // === ordering / flush ===

    #[test]
    fn ordering_preserved_when_passthrough_interrupts_buffer() {
        // buffered small batches must be flushed (as a runt batch) before a
        // pass-through batch is emitted, preserving input order
        let small1 = string_batch(10, 5);
        let small2 = string_batch(10, 5);
        let big = string_batch(TARGET_ROWS, 20);
        let inputs = vec![small1, small2, big];
        let out = run(&mut normalizer(), inputs.clone());
        assert_same_data(&inputs, &out);
        assert_eq!(
            out.iter().map(|b| b.num_rows()).collect::<Vec<_>>(),
            vec![20, TARGET_ROWS],
        );
    }

    #[test]
    fn finish_flushes_buffered_rows() {
        let inputs = vec![string_batch(10, 5), string_batch(10, 5)];
        let out = run(&mut normalizer(), inputs.clone());
        assert_same_data(&inputs, &out);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].num_rows(), 20);
    }

    #[test]
    fn empty_batches_are_dropped() {
        let inputs = vec![string_batch(0, 5), string_batch(10, 5)];
        let out = run(&mut normalizer(), inputs);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].num_rows(), 10);
    }

    #[test]
    fn no_column_batches_split_by_rows() {
        use arrow::array::RecordBatchOptions;
        let schema = Arc::new(Schema::empty());
        let batch = RecordBatch::try_new_with_options(
            Arc::clone(&schema),
            vec![],
            &RecordBatchOptions::new().with_row_count(Some(250)),
        )
        .unwrap();
        let mut n = BatchNormalizer::new(
            schema,
            TARGET_ROWS,
            TARGET_BYTES,
            BatchNormalizerMetrics::default(),
        );
        let out = run(&mut n, [batch]);
        assert_eq!(
            out.iter().map(|b| b.num_rows()).collect::<Vec<_>>(),
            vec![100, 100, 50],
        );
    }

    #[test]
    fn metrics_are_recorded() {
        let metrics = ExecutionPlanMetricsSet::new();
        let m = BatchNormalizerMetrics::new(&metrics, 0);
        let mut n =
            BatchNormalizer::new(test_schema(), TARGET_ROWS, TARGET_BYTES, m.clone());
        run(
            &mut n,
            [
                string_batch(TARGET_ROWS, 20), // pass through
                string_batch(10, 5),           // coalesce
                string_batch(50, 1024),        // split
            ],
        );
        assert_eq!(m.batches_passed_through.value(), 1);
        assert_eq!(m.batches_coalesced.value(), 1);
        assert_eq!(m.batches_split.value(), 1);
    }

    // === stream adapter ===

    #[tokio::test]
    async fn stream_end_to_end() {
        use crate::ExecutionPlan;
        use crate::test::TestMemoryExec;
        use datafusion_execution::TaskContext;

        let inputs: Vec<_> = (0..5)
            .map(|_| string_batch(10, 5))
            .chain([string_batch(50, 1024)])
            .collect();
        let schema = test_schema();
        let exec = TestMemoryExec::try_new_exec(
            std::slice::from_ref(&inputs),
            Arc::clone(&schema),
            None,
        )
        .unwrap();
        let input_stream = exec.execute(0, Arc::new(TaskContext::default())).unwrap();

        let stream = BatchNormalizerStream::new(
            input_stream,
            TARGET_ROWS,
            TARGET_BYTES,
            BatchNormalizerMetrics::default(),
        );
        assert_eq!(stream.schema(), schema);
        let out: Vec<RecordBatch> = Box::pin(stream).try_collect().await.unwrap();
        assert_same_data(&inputs, &out);
        assert!(out.len() > 1);
        for b in &out {
            assert!(b.num_rows() <= TARGET_ROWS);
            assert!(logical_size(b) <= SPLIT_SLOP_FACTOR * TARGET_BYTES);
        }
    }
}
