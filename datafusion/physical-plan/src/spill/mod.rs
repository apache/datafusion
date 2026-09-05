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

//! Defines the spilling functions

pub(crate) mod in_progress_spill_file;
pub(crate) mod replayable_spill_input;
pub(crate) mod spill_manager;
pub mod spill_pool;
use datafusion_execution::spill_file::SpillWriter;
// Moved for refactor, re-export to keep the public API stable
pub use datafusion_common::utils::memory::get_record_batch_memory_size;
// Re-export SpillManager for doctests only (hidden from public docs)
#[doc(hidden)]
pub use spill_manager::SpillManager;

use std::collections::VecDeque;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow::array::{
    Array, ArrayRef, BinaryViewArray, BufferSpec, GenericByteViewArray, StringViewArray,
    builder::GenericByteViewBuilder, layout, make_array,
};
use arrow::buffer::Buffer;
use arrow::datatypes::DataType;
use arrow::datatypes::{ByteViewType, Schema, SchemaRef};
use arrow::ipc::{
    MetadataVersion,
    reader::StreamDecoder,
    writer::{IpcWriteOptions, StreamWriter},
};
use arrow::record_batch::RecordBatch;
use arrow_data::ArrayDataBuilder;
use arrow_ipc::{CompressionType, root_as_message};

use datafusion_common::Result;
use datafusion_common::config::SpillCompression;
use datafusion_execution::RecordBatchStream;
use datafusion_execution::spill_file::SpillFile;
use futures::Stream;
use log::debug;

/// Stream that reads spill files from a [`SpillFile`] backend as a stream of [`RecordBatch`]es.
/// Uses [`StreamDecoder`] to decode IPC bytes received from the backend's async byte stream.
/// Backends handle their own threading concerns internally - OS files use
/// `tokio::fs::File` which performs blocking IO per-syscall without holding a thread
/// for the file's lifetime, avoiding deadlocks when concurrent reads exceed thread pool limits.
struct SpillReaderStream {
    schema: SchemaRef,
    decoder: StreamDecoder,
    byte_stream: Pin<Box<dyn Stream<Item = Result<bytes::Bytes>> + Send>>,
    is_done: bool,

    /// Maximum memory size observed among spilling sorted record batches.
    /// This is used for validation purposes during reading each RecordBatch from spill.
    /// For context on why this value is recorded and validated,
    /// see `physical_plan/sort/multi_level_merge.rs`.
    max_record_batch_memory: Option<usize>,

    /// Holds leftover bytes from a chunk not yet consumed by `framer`
    current_buffer: Buffer,

    /// Assembles the chunks of `byte_stream` into exactly sized messages
    framer: MessageFramer,

    /// Framed message buffers not yet consumed by `decoder`
    pending: VecDeque<Buffer>,

    /// Keeps the file alive until the stream is dropped
    _spill_file: Arc<dyn SpillFile>,

    schema_validated: bool,
}

// Small margin allowed to accommodate slight memory accounting variation
const SPILL_BATCH_MEMORY_MARGIN: usize = 4096;

/// Reassembles an IPC stream read in arbitrary chunks into one exactly sized
/// allocation per message, so that the zero-copy [`StreamDecoder`] produces
/// batches whose buffers pin only their own message.
///
/// # Why
///
/// The decoder builds arrays on slices of whatever [`Buffer`] it is given,
/// and a slice keeps its whole backing allocation alive. Fed with the raw
/// chunks of the byte stream (128 KB for a file backed spill) that goes
/// wrong in two ways.
///
/// A message that fits inside a chunk pins the entire chunk. With ~5 KB
/// batches, one chunk holds ~27 of them, and each decoded batch retains, and
/// is accounted for, 128 KB:
///
/// ```text
/// chunk (128 KB allocation)
/// ┌──────┬──────┬──────┬─────┬───────┐
/// │ msg1 │ msg2 │ msg3 │ ... │ msg27 │
/// └──────┴──────┴──────┴─────┴───────┘
///    ▲
///    batch1's buffers slice here, yet keep all 128 KB alive
/// ```
///
/// A message that spans two chunks cannot be sliced, so the decoder gathers
/// it into a `Vec` grown by doubling, and the batch keeps the spare
/// capacity: a 256 KB body typically lands in a 512 KB allocation.
///
/// ```text
/// chunk N                      chunk N+1
/// ┌──────┬────────────────────┬──────────────┬────────┐
/// │ ...  │ msgK (first part)  │ msgK (rest)  │ msgK+1 │
/// └──────┴────────────────────┴──────────────┴────────┘
/// ```
///
/// Either way a batch uses several times the memory recorded for it at
/// spill time, breaking the `max_record_batch_memory` budgeting that the
/// multi-level merge relies on.
///
/// # How
///
/// Each message is copied out of the chunks into allocations sized from its
/// own headers: a head buffer (the length prefix and flatbuffer metadata,
/// whose `bodyLength` gives the body size) and, when non-empty, a body
/// buffer of exactly that size. The decoder then zero-copies from the body
/// buffer, so a batch pins exactly its own message:
///
/// ```text
/// body for msg1 (5 KB)      body for msgK (256 KB)
/// ┌──────┐                  ┌────────────────────┐
/// │ msg1 │ ◀── batch1       │ msgK               │ ◀── batchK
/// └──────┘                  └────────────────────┘
/// ```
///
/// This costs one copy per message, which the decoder already paid for
/// spanning messages, without the doubling reallocation. Measured on the
/// `spill_io` benchmark, the copy is not visible end to end for batches at
/// or above the 128 KB read chunk, where skipping the doubling gather makes
/// framing the faster of the two, and costs a few percent for batches small
/// enough that several share a chunk, which are exactly the batches whose
/// accounting it fixes.
struct MessageFramer {
    state: FramerState,
}

enum FramerState {
    /// Reading the 4 byte continuation marker or metadata length.
    Prefix {
        head: Vec<u8>,
        read: usize,
        continuation: bool,
    },
    /// Reading the flatbuffer metadata into `head`, which already holds the
    /// prefix and is allocated for `head_len` bytes.
    Metadata {
        head: Vec<u8>,
        metadata_start: usize,
        head_len: usize,
    },
    /// Reading the body into `body`, which is allocated for `body_len` bytes.
    Body {
        head: Vec<u8>,
        body: Vec<u8>,
        body_len: usize,
    },
    /// The end-of-stream marker was read.
    Finished,
}

impl MessageFramer {
    fn new() -> Self {
        Self {
            state: FramerState::prefix(),
        }
    }

    /// Consumes bytes from `input` until a message is complete or `input` is
    /// exhausted, returning the buffers of a completed message.
    fn push(&mut self, input: &mut Buffer) -> Result<Option<Vec<Buffer>>> {
        while !input.is_empty() {
            match &mut self.state {
                FramerState::Prefix {
                    head,
                    read,
                    continuation,
                } => {
                    let to_read = input.len().min(4 - *read);
                    head.extend_from_slice(&input[..to_read]);
                    input.advance(to_read);
                    *read += to_read;
                    if *read < 4 {
                        continue;
                    }
                    let word: [u8; 4] = head[head.len() - 4..].try_into().unwrap();
                    if !*continuation && word == CONTINUATION_MARKER {
                        *continuation = true;
                        *read = 0;
                        continue;
                    }
                    let metadata_len = u32::from_le_bytes(word) as usize;
                    let head = std::mem::take(head);
                    if metadata_len == 0 {
                        self.state = FramerState::Finished;
                        return Ok(Some(vec![Buffer::from_vec(head)]));
                    }
                    let metadata_start = head.len();
                    let head_len = metadata_start + metadata_len;
                    let mut sized = Vec::with_capacity(head_len);
                    sized.extend_from_slice(&head);
                    self.state = FramerState::Metadata {
                        head: sized,
                        metadata_start,
                        head_len,
                    };
                }
                FramerState::Metadata {
                    head,
                    metadata_start,
                    head_len,
                } => {
                    let to_read = input.len().min(*head_len - head.len());
                    head.extend_from_slice(&input[..to_read]);
                    input.advance(to_read);
                    if head.len() < *head_len {
                        continue;
                    }
                    let message =
                        root_as_message(&head[*metadata_start..]).map_err(|e| {
                            datafusion_common::exec_datafusion_err!(
                                "Invalid IPC message in spill file: {e}"
                            )
                        })?;
                    let body_len =
                        usize::try_from(message.bodyLength()).map_err(|_| {
                            datafusion_common::exec_datafusion_err!(
                                "Invalid IPC message body length in spill file: {}",
                                message.bodyLength()
                            )
                        })?;
                    let head = std::mem::take(head);
                    if body_len == 0 {
                        self.state = FramerState::prefix();
                        return Ok(Some(vec![Buffer::from_vec(head)]));
                    }
                    self.state = FramerState::Body {
                        head,
                        body: Vec::with_capacity(body_len),
                        body_len,
                    };
                }
                FramerState::Body {
                    head,
                    body,
                    body_len,
                } => {
                    let to_read = input.len().min(*body_len - body.len());
                    body.extend_from_slice(&input[..to_read]);
                    input.advance(to_read);
                    if body.len() < *body_len {
                        continue;
                    }
                    let (head, body) = (std::mem::take(head), std::mem::take(body));
                    self.state = FramerState::prefix();
                    return Ok(Some(vec![
                        Buffer::from_vec(head),
                        Buffer::from_vec(body),
                    ]));
                }
                FramerState::Finished => {
                    return datafusion_common::exec_err!(
                        "Unexpected bytes after the end of the IPC stream in spill file"
                    );
                }
            }
        }
        Ok(None)
    }

    /// Checks that the stream ended on a message boundary.
    ///
    /// This mirrors [`StreamDecoder::finish`]: a stream is complete either
    /// when the end-of-stream marker was read, or when it ends right after a
    /// message. Framing moves the partial bytes of a truncated file out of
    /// the decoder's own scratch space and into the framer, so without this
    /// check a truncated spill file would silently decode as a shorter
    /// stream instead of erroring.
    fn finish(&self) -> Result<()> {
        match &self.state {
            FramerState::Finished
            | FramerState::Prefix {
                read: 0,
                continuation: false,
                ..
            } => Ok(()),
            _ => datafusion_common::exec_err!(
                "Unexpected end of spill file: the IPC stream ends mid-message"
            ),
        }
    }
}

impl FramerState {
    fn prefix() -> Self {
        Self::Prefix {
            head: Vec::with_capacity(8),
            read: 0,
            continuation: false,
        }
    }
}

/// Marks a length prefix in the IPC stream format, see `arrow_ipc`.
const CONTINUATION_MARKER: [u8; 4] = [0xff; 4];

impl SpillReaderStream {
    fn new(
        schema: SchemaRef,
        spill_file: Arc<dyn SpillFile>,
        max_record_batch_memory: Option<usize>,
    ) -> Result<Self> {
        let byte_stream = spill_file.read_stream()?;
        // DataFusion controls what it writes so it can trust its own IPC output,
        // matching the behavior of the previous StreamReader-based implementation.
        let decoder = unsafe { StreamDecoder::new().with_skip_validation(true) };
        Ok(Self {
            schema,
            decoder,
            byte_stream,
            max_record_batch_memory,
            is_done: false,
            current_buffer: Buffer::from(&[]),
            framer: MessageFramer::new(),
            pending: VecDeque::new(),
            _spill_file: spill_file,
            schema_validated: false,
        })
    }
}

impl Stream for SpillReaderStream {
    type Item = Result<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();

        if this.is_done {
            return Poll::Ready(None);
        }

        loop {
            // Decode the framed messages first
            if let Some(buffer) = this.pending.front_mut() {
                if buffer.is_empty() {
                    this.pending.pop_front();
                    continue;
                }
                match this.decoder.decode(buffer) {
                    Ok(Some(batch)) => {
                        // One-time schema validation on the first decoded batch.
                        // The IPC stream embeds the writer's schema in its header;
                        // StreamDecoder surfaces it via the first batch's schema.
                        // We check here rather than in new() because schema bytes
                        // only arrive after decoding the IPC header from the stream.
                        if !this.schema_validated {
                            this.schema_validated = true;
                            let actual = batch.schema();
                            if actual != this.schema {
                                this.is_done = true;
                                return Poll::Ready(Some(Err(
                                    datafusion_common::exec_datafusion_err!(
                                        "Spill file schema mismatch: expected {}, got {}. \
                     The caller must use the same SpillManager that created \
                     the spill file to read it.",
                                        this.schema,
                                        actual
                                    ),
                                )));
                            }
                        }
                        if let Some(max_record_batch_memory) =
                            this.max_record_batch_memory
                        {
                            let actual_size = get_record_batch_memory_size(&batch);
                            if actual_size
                                > max_record_batch_memory + SPILL_BATCH_MEMORY_MARGIN
                            {
                                debug!(
                                    "Record batch memory usage ({actual_size} bytes) exceeds the expected limit ({max_record_batch_memory} bytes) \n\
                                        by more than the allowed tolerance ({SPILL_BATCH_MEMORY_MARGIN} bytes).\n\
                                        This likely indicates a bug in memory accounting during spilling."
                                );
                            }
                        }
                        return Poll::Ready(Some(Ok(batch)));
                    }
                    Ok(None) => {
                        // A schema or dictionary message, or the buffer was
                        // only part of a message. Carry on with the next one.
                    }
                    Err(e) => {
                        this.is_done = true;
                        return Poll::Ready(Some(Err(e.into())));
                    }
                }
                continue;
            }

            // Then frame the next message out of the current chunk
            if !this.current_buffer.is_empty() {
                match this.framer.push(&mut this.current_buffer) {
                    Ok(Some(buffers)) => this.pending.extend(buffers),
                    Ok(None) => {}
                    Err(e) => {
                        this.is_done = true;
                        return Poll::Ready(Some(Err(e)));
                    }
                }
                continue;
            }

            // Finally fetch another chunk
            match futures::ready!(this.byte_stream.as_mut().poll_next(cx)) {
                Some(Ok(chunk)) => {
                    this.current_buffer = Buffer::from(chunk);
                }
                Some(Err(e)) => {
                    this.is_done = true;
                    return Poll::Ready(Some(Err(e)));
                }
                None => {
                    this.is_done = true;

                    // The framer holds the bytes of an incomplete trailing
                    // message, so it is the one that detects truncation.
                    if let Err(e) = this.framer.finish() {
                        return Poll::Ready(Some(Err(e)));
                    }
                    if let Err(e) = this.decoder.finish() {
                        return Poll::Ready(Some(Err(e.into())));
                    }
                    return Poll::Ready(None);
                }
            }
        }
    }
}

impl RecordBatchStream for SpillReaderStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

/// A  wrapper that counts the exact compressed IPC bytes written by Arrow.
///
/// Arrow's `StreamWriter` does not return the number of bytes written during its
/// `write()` calls. To accurately track the `spilled_bytes` metrics (especially
/// when LZ4/ZSTD compression is applied), we must intercept the `std::io::Write`
/// trait boundary to count the final serialized payload size.
pub(crate) struct TrackingSpillWriter {
    inner: Box<dyn SpillWriter>,
    pub(crate) total_bytes_written: usize,
}

impl TrackingSpillWriter {
    pub fn new(inner: Box<dyn SpillWriter>) -> Self {
        Self {
            inner,
            total_bytes_written: 0,
        }
    }

    pub fn finish(mut self) -> Result<()> {
        self.inner.finish()
    }
}

impl std::io::Write for TrackingSpillWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let n = self.inner.write(buf)?;

        self.total_bytes_written += n;

        Ok(n)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

/// Write in Arrow IPC Stream format to an underlying `SpillWriter` backend.
/// Stream format also supports dictionary replacement.
struct IPCStreamWriter {
    /// Inner writer
    writer: Option<StreamWriter<TrackingSpillWriter>>,
    /// Batches written
    num_batches: usize,
    /// Rows written
    num_rows: usize,
    /// Bytes written
    num_bytes: usize,
}

impl IPCStreamWriter {
    /// Create new writer
    ///
    /// # Codec contract
    ///
    /// `arrow-ipc` must be compiled with the `lz4` and `zstd` features
    /// (declared explicitly in `datafusion-physical-plan/Cargo.toml`). If
    /// those features are absent, `try_with_compression` will return an
    /// error at runtime for [`SpillCompression::Lz4Frame`] and
    /// [`SpillCompression::Zstd`] variants. The Cargo dependency keeps this
    /// contract local and build-visible during Cargo feature resolution,
    /// rather than relying solely on workspace-level feature unification;
    /// see #21917.
    pub fn new(
        spill_writer: Box<dyn SpillWriter>,
        schema: &Schema,
        spill_compression: SpillCompression,
    ) -> Result<Self> {
        let metadata_version = MetadataVersion::V5;
        // Depending on the schema, some array types such as StringViewArray require larger (16 byte in this case) alignment.
        // If the actual buffer layout after IPC read does not satisfy the alignment requirement,
        // Arrow ArrayBuilder will copy the buffer into a newly allocated, properly aligned buffer.
        // This copying may lead to memory blowup during IPC read due to duplicated buffers.
        // To avoid this, we compute the maximum required alignment based on the schema and configure the IPCStreamWriter accordingly.
        let alignment = get_max_alignment_for_schema(schema);
        let mut write_options =
            IpcWriteOptions::try_new(alignment, false, metadata_version)?;

        let compression_type = Option::<CompressionType>::from(spill_compression);
        write_options = write_options.try_with_compression(compression_type)?;

        let adapter = TrackingSpillWriter::new(spill_writer);
        let writer = StreamWriter::try_new_with_options(adapter, schema, write_options)?;

        Ok(Self {
            num_batches: 0,
            num_rows: 0,
            num_bytes: 0,
            writer: Some(writer),
        })
    }

    /// Writes a single batch to the IPC stream and updates the internal counters.
    ///
    /// Returns a tuple containing the change in the number of rows and bytes written.
    pub fn write(&mut self, batch: &RecordBatch) -> Result<(usize, usize)> {
        let writer = self.writer.as_mut().unwrap();

        let bytes_before = writer.get_ref().total_bytes_written;
        writer.write(batch)?;
        let bytes_after = writer.get_ref().total_bytes_written;
        self.num_batches += 1;
        let delta_num_rows = batch.num_rows();
        self.num_rows += delta_num_rows;
        let delta_num_bytes = bytes_after - bytes_before;
        self.num_bytes += delta_num_bytes;
        Ok((delta_num_rows, delta_num_bytes))
    }

    pub fn flush(&mut self) -> Result<()> {
        use std::io::Write;
        if let Some(writer) = &mut self.writer {
            writer.get_mut().flush()?;
        }
        Ok(())
    }

    /// Finish the writer.
    ///
    /// Returns the number of trailing bytes written during the finish operation
    /// (e.g., IPC metadata and footers).
    pub fn finish(&mut self) -> Result<usize> {
        let mut writer = self.writer.take().unwrap();

        let bytes_before = writer.get_ref().total_bytes_written;
        writer.finish()?; // Writes IPC tail

        // Extract the adapter and flush the final bytes
        let adapter = writer.into_inner()?;
        let bytes_after = adapter.total_bytes_written;
        adapter.finish()?;

        Ok(bytes_after - bytes_before)
    }
    /// Returns the total number of bytes written so far
    pub fn bytes_written(&self) -> usize {
        self.writer
            .as_ref()
            .map(|w| w.get_ref().total_bytes_written)
            .unwrap_or(0)
    }
}

// Returns the maximum byte alignment required by any field in the schema (>= 8), derived from Arrow buffer layouts.
fn get_max_alignment_for_schema(schema: &Schema) -> usize {
    let minimum_alignment = 8;
    let mut max_alignment = minimum_alignment;
    for field in schema.fields() {
        let layout = layout(field.data_type());
        let required_alignment = layout
            .buffers
            .iter()
            .map(|buffer_spec| {
                if let BufferSpec::FixedWidth { alignment, .. } = buffer_spec {
                    *alignment
                } else {
                    minimum_alignment
                }
            })
            .max()
            .unwrap_or(minimum_alignment);
        max_alignment = std::cmp::max(max_alignment, required_alignment);
    }
    max_alignment
}

/// Size of a single view structure in StringView/BinaryView arrays (in bytes).
/// Each view is 16 bytes: 4 bytes length + 4 bytes prefix + 8 bytes buffer ID/offset.
const VIEW_SIZE_BYTES: usize = 16;

/// Performs garbage collection on StringView and BinaryView arrays before spilling to reduce memory usage.
///
/// # Why GC is needed
///
/// StringView and BinaryView arrays can accumulate significant memory waste when sliced.
/// When a large array is sliced (e.g., taking first 100 rows of 1000), the view array
/// still references the original data buffers containing all 1000 rows of data.
///
/// For example, in the ClickBench benchmark (issue #19414), repeated slicing of StringView
/// arrays resulted in 820MB of spill files that could be reduced to just 33MB after GC -
/// a 96% reduction in size.
///
/// # How it works
///
/// The GC process:
/// 1. Identifies view arrays (StringView/BinaryView) in the batch
/// 2. Checks if their data buffers exceed a memory threshold
/// 3. If exceeded, calls the Arrow `gc()` method which creates new compact buffers
///    containing only the data referenced by the current views
/// 4. Returns a new batch with GC'd arrays (or original arrays if GC not needed)
///
/// # When GC is triggered
///
/// GC is only performed when data buffers exceed a threshold (currently 10KB).
/// This balances memory savings against the CPU overhead of garbage collection.
/// Small arrays are passed through unchanged since the GC overhead would exceed
/// any memory savings.
///
/// # Performance considerations
///
/// - If no view arrays need compaction, the original batch is cloned cheaply
/// - GC is skipped for small buffers to avoid unnecessary CPU overhead
/// - Nested container types are traversed recursively so view arrays inside
///   `List`, `Map`, `Union`, `Dictionary`, and other child-bearing arrays are compacted too
/// - The Arrow `gc()` method itself is optimized and only copies referenced data
pub(crate) fn gc_view_arrays(batch: &RecordBatch) -> Result<RecordBatch> {
    let mut mutated = false;
    let mut new_columns: Vec<Arc<dyn Array>> = Vec::with_capacity(batch.num_columns());

    for array in batch.columns() {
        let (gc_array, array_mutated) = gc_array(array)?;
        mutated |= array_mutated;
        new_columns.push(gc_array);
    }

    if mutated {
        Ok(RecordBatch::try_new(batch.schema(), new_columns)?)
    } else {
        Ok(batch.clone())
    }
}

/// Garbage collect and deduplicate a string view before writing it out to disk
///
/// This is to avoid inflating disk usage and also to ensure that deduplication reduces memory pressure when reading back.
fn gc_dedup_view<T: ByteViewType>(
    array: &GenericByteViewArray<T>,
) -> GenericByteViewArray<T> {
    let mut builder = GenericByteViewBuilder::<T>::with_capacity(array.len())
        .with_deduplicate_strings();
    for v in array.iter() {
        builder.append_option(v);
    }
    builder.finish()
}

fn gc_array(array: &ArrayRef) -> Result<(ArrayRef, bool)> {
    match array.data_type() {
        DataType::Utf8View => {
            let string_view = array
                .as_any()
                .downcast_ref::<StringViewArray>()
                .expect("Utf8View array should downcast to StringViewArray");
            if should_gc_view_array(string_view) {
                Ok((Arc::new(gc_dedup_view(string_view)) as ArrayRef, true))
            } else {
                Ok((Arc::clone(array), false))
            }
        }
        DataType::BinaryView => {
            let binary_view = array
                .as_any()
                .downcast_ref::<BinaryViewArray>()
                .expect("BinaryView array should downcast to BinaryViewArray");
            if should_gc_view_array(binary_view) {
                Ok((Arc::new(gc_dedup_view(binary_view)) as ArrayRef, true))
            } else {
                Ok((Arc::clone(array), false))
            }
        }
        _ => gc_array_children(array),
    }
}

fn gc_array_children(array: &ArrayRef) -> Result<(ArrayRef, bool)> {
    let data = array.to_data();
    if data.child_data().is_empty() {
        return Ok((Arc::clone(array), false));
    }

    let mut mutated = false;
    let mut child_data = Vec::with_capacity(data.child_data().len());
    for child in data.child_data() {
        let child_array = make_array(child.clone());
        let (gc_child, child_mutated) = gc_array(&child_array)?;
        mutated |= child_mutated;
        child_data.push(gc_child.to_data());
    }

    if !mutated {
        return Ok((Arc::clone(array), false));
    }

    let rebuilt = ArrayDataBuilder::new(data.data_type().clone())
        .len(data.len())
        .offset(data.offset())
        .nulls(data.nulls().cloned())
        .buffers(data.buffers().to_vec())
        .child_data(child_data)
        .build()?;

    Ok((make_array(rebuilt), true))
}

/// Determines whether a view array should be garbage collected before spilling.
///
/// Arrow's `gc()` always allocates new compact buffers (it is never a no-op), so we
/// check here to skip the allocation cost when data buffers are small. We subtract
/// the views buffer (16 bytes × n_rows) from `get_buffer_memory_size()` so the
/// threshold tracks non-inline string data rather than row count.
fn should_gc_view_array<T: ByteViewType>(array: &GenericByteViewArray<T>) -> bool {
    const MIN_BUFFER_SIZE_FOR_GC: usize = 10 * 1024; // 10KB threshold

    if array.data_buffers().is_empty() {
        return false;
    }

    let data_buffer_size = array
        .get_buffer_memory_size()
        .saturating_sub(array.len() * VIEW_SIZE_BYTES);
    data_buffer_size > MIN_BUFFER_SIZE_FOR_GC
}

#[cfg(test)]
fn calculate_string_view_waste_ratio(array: &StringViewArray) -> f64 {
    use arrow_data::MAX_INLINE_VIEW_LEN;
    calculate_view_waste_ratio(array.len(), array.data_buffers(), |i| {
        if !array.is_null(i) {
            let value = array.value(i);
            if value.len() > MAX_INLINE_VIEW_LEN as usize {
                return value.len();
            }
        }
        0
    })
}

#[cfg(test)]
fn calculate_view_waste_ratio<F>(
    len: usize,
    data_buffers: &[Buffer],
    get_value_size: F,
) -> f64
where
    F: Fn(usize) -> usize,
{
    let total_buffer_size: usize = data_buffers.iter().map(|b| b.capacity()).sum();
    if total_buffer_size == 0 {
        return 0.0;
    }

    let mut actual_used_size = (0..len).map(get_value_size).sum::<usize>();
    actual_used_size += len * VIEW_SIZE_BYTES;

    let waste = total_buffer_size.saturating_sub(actual_used_size);
    waste as f64 / total_buffer_size as f64
}

#[cfg(test)]
mod tests {
    use super::in_progress_spill_file::InProgressSpillFile;
    use super::*;
    use crate::common::collect;
    use crate::metrics::ExecutionPlanMetricsSet;
    use crate::metrics::SpillMetrics;
    use crate::spill::spill_manager::SpillManager;
    use crate::test::build_table_i32;
    use arrow::array::{ArrayRef, Int32Array, StringArray};
    use arrow::compute::cast;
    use arrow::datatypes::{DataType, Field};
    use datafusion_execution::runtime_env::RuntimeEnv;
    use futures::StreamExt as _;

    #[tokio::test]
    async fn test_batch_spill_and_read() -> Result<()> {
        let batch1 = build_table_i32(
            ("a2", &vec![0, 1, 2]),
            ("b2", &vec![3, 4, 5]),
            ("c2", &vec![4, 5, 6]),
        );

        let batch2 = build_table_i32(
            ("a2", &vec![10, 11, 12]),
            ("b2", &vec![13, 14, 15]),
            ("c2", &vec![14, 15, 16]),
        );

        let schema = batch1.schema();
        let num_rows = batch1.num_rows() + batch2.num_rows();

        // Construct SpillManager
        let env = Arc::new(RuntimeEnv::default());
        let metrics = SpillMetrics::new(&ExecutionPlanMetricsSet::new(), 0);
        let spill_manager = SpillManager::new(env, metrics, Arc::clone(&schema));

        let spill_file = spill_manager
            .spill_record_batch_and_finish(&[batch1, batch2], "Test")?
            .unwrap();
        assert!(spill_file.path().unwrap().exists());
        let spilled_rows = spill_manager.metrics.spilled_rows.value();
        assert_eq!(spilled_rows, num_rows);

        let stream = spill_manager.read_spill_as_stream(spill_file, None)?;
        assert_eq!(stream.schema(), schema);

        let batches = collect(stream).await?;
        assert_eq!(batches.len(), 2);

        Ok(())
    }

    /// Reading a spill file back must not inflate the batches' memory
    /// footprint: the decoder is zero-copy, so without framing every small
    /// batch would keep a whole read chunk alive, see [`MessageFramer`].
    ///
    /// Regression test for
    /// <https://github.com/apache/datafusion/issues/17340>. Without framing
    /// these 50 batches read back at 131072 bytes each, against a 22992 byte
    /// maximum recorded at spill time, and the whole 345 KB stream is
    /// accounted for as 5.9 MB. Note that this is not a counting bug that a
    /// shared-buffer aware counter could net out: holding only 5 of the 50
    /// batches still pins every read chunk, so the memory really is retained.
    #[tokio::test]
    async fn test_read_back_does_not_inflate_batch_memory() -> Result<()> {
        use arrow::array::{ListArray, StringViewArray};
        use arrow::buffer::OffsetBuffer;

        let schema = Arc::new(Schema::new(vec![
            Field::new("i", DataType::Int32, false),
            Field::new("s", DataType::Utf8, false),
            Field::new("v", DataType::Utf8View, true),
            Field::new(
                "l",
                DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
                false,
            ),
        ]));

        // Small batches: many fit in one 128 KB read chunk.
        let batches: Vec<RecordBatch> = (0..50)
            .map(|b| {
                let n = 100;
                let ints = Int32Array::from_iter_values((0..n).map(|i| b * n + i));
                let strs = StringArray::from_iter_values(
                    (0..n).map(|i| format!("string value number {i}")),
                );
                let views = StringViewArray::from_iter((0..n).map(|i| {
                    (i % 3 != 0).then(|| format!("a longer view value {b}/{i}"))
                }));
                let values = Int32Array::from_iter_values(0..n * 2);
                let offsets =
                    OffsetBuffer::from_lengths(std::iter::repeat_n(2, n as usize));
                let list = ListArray::new(
                    Arc::new(Field::new("item", DataType::Int32, true)),
                    offsets,
                    Arc::new(values),
                    None,
                );
                RecordBatch::try_new(
                    Arc::clone(&schema),
                    vec![
                        Arc::new(ints),
                        Arc::new(strs),
                        Arc::new(views),
                        Arc::new(list),
                    ],
                )
                .unwrap()
            })
            .collect();

        let max_written = batches
            .iter()
            .map(get_record_batch_memory_size)
            .max()
            .unwrap();

        let env = Arc::new(RuntimeEnv::default());
        let metrics = SpillMetrics::new(&ExecutionPlanMetricsSet::new(), 0);
        let spill_manager = SpillManager::new(env, metrics, Arc::clone(&schema));
        let spill_file = spill_manager
            .spill_record_batch_and_finish(&batches, "Test")?
            .unwrap();

        let stream = spill_manager.read_spill_as_stream(spill_file, None)?;
        let read_back = collect(stream).await?;
        assert_eq!(read_back.len(), batches.len());

        for (written, read) in batches.iter().zip(&read_back) {
            assert_eq!(written, read);
            let size = get_record_batch_memory_size(read);
            assert!(
                size <= max_written + SPILL_BATCH_MEMORY_MARGIN,
                "read-back batch retains {size} bytes, written max was {max_written}"
            );
        }
        Ok(())
    }

    /// The workload from <https://github.com/apache/datafusion/issues/17340>,
    /// reduced to a single spilled run.
    ///
    /// `memory_limit::test_stringview_external_sort` sorts 200 batches of 1000
    /// random 100 byte strings under a 60 MB pool, producing around 10 spilled
    /// runs of about 20 batches each. This spills one such run and reads it
    /// back through the same [`SpillManager`] calls the external sorter and the
    /// multi-level merge use: the spill records the largest batch's memory size,
    /// and the merge reserves its budget from that number, so a batch that comes
    /// back bigger is memory the merge is using without having reserved it.
    ///
    /// Each batch here is around 167 KB, larger than the 128 KB read chunk, so
    /// every IPC message spans chunks. Without framing the decoder gathers such
    /// a message into a `Vec` grown by doubling and the batch keeps the spare
    /// capacity, so these batches read back at about 255 KB each against the
    /// ~167 KB recorded at spill time - half again the budget the merge
    /// reserved for them, which is the discrepancy the issue reports.
    #[tokio::test]
    async fn test_stringview_spill_read_back_memory_accounting() -> Result<()> {
        use arrow::array::StringViewArray;
        use rand::Rng;

        let schema = Arc::new(Schema::new(vec![
            Field::new("strings", DataType::Utf8View, false),
            Field::new("random_numbers", DataType::Int32, false),
        ]));

        // 100 random bytes per string, as in the issue's reproducer: long
        // enough that the views point into a data buffer rather than inlining,
        // and random so nothing dedupes.
        let mut rng = rand::rng();
        let batches: Vec<RecordBatch> = (0..20)
            .map(|_| {
                let strings: Vec<String> = (0..1000)
                    .map(|_| {
                        (0..100)
                            .map(|_| rng.random_range(0..=u8::MAX) as char)
                            .collect()
                    })
                    .collect();
                let numbers: Vec<i32> =
                    (0..1000).map(|_| rng.random_range(0..=1000)).collect();
                RecordBatch::try_new(
                    Arc::clone(&schema),
                    vec![
                        Arc::new(StringViewArray::from(strings)) as ArrayRef,
                        Arc::new(Int32Array::from(numbers)) as ArrayRef,
                    ],
                )
                .unwrap()
            })
            .collect();

        let env = Arc::new(RuntimeEnv::default());
        let metrics = SpillMetrics::new(&ExecutionPlanMetricsSet::new(), 0);
        let spill_manager = SpillManager::new(env, metrics, Arc::clone(&schema));

        // The same call the external sorter spills a sorted run with: the
        // returned size is what the merge budgets against.
        let (spill_file, max_record_batch_memory) = spill_manager
            .spill_record_batch_iter_and_return_max_batch_memory(
                batches
                    .iter()
                    .map(Ok::<_, datafusion_common::DataFusionError>),
                "Test",
            )?
            .unwrap();

        let stream = spill_manager
            .read_spill_as_stream(spill_file, Some(max_record_batch_memory))?;
        let read_back = collect(stream).await?;
        assert_eq!(read_back.len(), batches.len());

        for (written, read) in batches.iter().zip(&read_back) {
            assert_eq!(written, read);
            let size = get_record_batch_memory_size(read);
            assert!(
                size <= max_record_batch_memory + SPILL_BATCH_MEMORY_MARGIN,
                "read-back batch retains {size} bytes, but the merge only \
                 reserved for {max_record_batch_memory} bytes"
            );
        }
        Ok(())
    }

    /// Frames an IPC stream delivered in chunks of `chunk_size` bytes and
    /// decodes it, checking that every batch is intact and that its buffers
    /// are backed by an allocation no larger than its own message body.
    fn frame_and_decode(ipc: &[u8], chunk_size: usize, expected: &[RecordBatch]) {
        let mut framer = MessageFramer::new();
        let mut decoder = StreamDecoder::new();
        let mut decoded = vec![];
        for chunk in ipc.chunks(chunk_size) {
            let mut input = Buffer::from(chunk);
            while !input.is_empty() {
                let Some(buffers) = framer.push(&mut input).unwrap() else {
                    continue;
                };
                let body_size = buffers.last().unwrap().len();
                for mut buffer in buffers {
                    while !buffer.is_empty() {
                        if let Some(batch) = decoder.decode(&mut buffer).unwrap() {
                            let retained = get_record_batch_memory_size(&batch);
                            assert!(
                                retained <= body_size,
                                "batch retains {retained} bytes for a {body_size} byte body"
                            );
                            decoded.push(batch);
                        }
                    }
                }
            }
        }
        decoder.finish().unwrap();
        assert!(matches!(framer.state, FramerState::Finished));
        assert_eq!(decoded, expected);
    }

    #[test]
    fn test_message_framer_across_chunk_boundaries() {
        let batches: Vec<RecordBatch> = (0..5)
            .map(|b| {
                let n = 10 * (b + 1);
                build_table_i32(
                    ("a", &(0..n).collect::<Vec<_>>()),
                    ("b", &(n..2 * n).collect::<Vec<_>>()),
                    ("c", &(2 * n..3 * n).collect::<Vec<_>>()),
                )
            })
            .collect();
        let schema = batches[0].schema();

        let mut ipc = vec![];
        let mut writer = StreamWriter::try_new(&mut ipc, &schema).unwrap();
        for batch in &batches {
            writer.write(batch).unwrap();
        }
        writer.finish().unwrap();

        for chunk_size in [1, 3, 7, 64, 1000, ipc.len()] {
            frame_and_decode(&ipc, chunk_size, &batches);
        }
    }

    /// Decodes `ipc` the way the reader did before framing, returning the
    /// number of batches or `Err` if the stream is not a complete one.
    fn decode_unframed(ipc: &[u8]) -> std::result::Result<usize, ()> {
        let mut decoder = StreamDecoder::new();
        let mut buffer = Buffer::from(ipc);
        let mut batches = 0;
        while !buffer.is_empty() {
            match decoder.decode(&mut buffer) {
                Ok(Some(_)) => batches += 1,
                Ok(None) => {}
                Err(_) => return Err(()),
            }
        }
        decoder.finish().map_err(|_| ())?;
        Ok(batches)
    }

    /// Same as [`decode_unframed`], but through the [`MessageFramer`].
    fn decode_framed(ipc: &[u8]) -> std::result::Result<usize, ()> {
        let mut framer = MessageFramer::new();
        let mut decoder = StreamDecoder::new();
        let mut input = Buffer::from(ipc);
        let mut batches = 0;
        while !input.is_empty() {
            let Some(buffers) = framer.push(&mut input).map_err(|_| ())? else {
                continue;
            };
            for mut buffer in buffers {
                while !buffer.is_empty() {
                    match decoder.decode(&mut buffer) {
                        Ok(Some(_)) => batches += 1,
                        Ok(None) => {}
                        Err(_) => return Err(()),
                    }
                }
            }
        }
        framer.finish().map_err(|_| ())?;
        decoder.finish().map_err(|_| ())?;
        Ok(batches)
    }

    /// Framing must not weaken the reader's detection of a corrupt or cut
    /// short spill file. The partial bytes of an incomplete trailing message
    /// end up in the [`MessageFramer`] instead of the decoder's own scratch
    /// space, so the framer has to report them, see [`MessageFramer::finish`].
    ///
    /// Truncated at every possible offset, framed decoding must accept and
    /// reject exactly what unframed decoding does.
    #[test]
    fn test_message_framer_reports_truncation_like_the_decoder() {
        let batches: Vec<RecordBatch> = (0..3)
            .map(|b| {
                build_table_i32(
                    ("a", &(0..50).map(|i| b * 50 + i).collect::<Vec<_>>()),
                    ("b", &(0..50).collect::<Vec<_>>()),
                    ("c", &(0..50).collect::<Vec<_>>()),
                )
            })
            .collect();

        let mut ipc = vec![];
        let mut writer = StreamWriter::try_new(&mut ipc, &batches[0].schema()).unwrap();
        for batch in &batches {
            writer.write(batch).unwrap();
        }
        writer.finish().unwrap();

        let mut complete = 0;
        for cut in 0..=ipc.len() {
            let truncated = &ipc[..cut];
            let framed = decode_framed(truncated);
            assert_eq!(
                framed,
                decode_unframed(truncated),
                "framed and unframed decoding disagree on a stream cut to {cut} bytes"
            );
            complete += usize::from(framed.is_ok());
        }
        // Cutting anywhere but on a message boundary must be an error, so
        // only the empty stream, the three batches and the end-of-stream
        // marker may decode cleanly.
        assert_eq!(complete, 5);
        assert_eq!(decode_framed(&ipc), Ok(batches.len()));
    }

    /// End-to-end: a spill file cut short mid-message must error rather than
    /// silently read back as a shorter stream.
    #[tokio::test]
    async fn test_truncated_spill_file_errors() -> Result<()> {
        let batch = build_table_i32(
            ("a", &(0..100).collect::<Vec<_>>()),
            ("b", &(100..200).collect::<Vec<_>>()),
            ("c", &(200..300).collect::<Vec<_>>()),
        );
        let schema = batch.schema();
        let batches = vec![batch.clone(), batch];

        let env = Arc::new(RuntimeEnv::default());
        let metrics = SpillMetrics::new(&ExecutionPlanMetricsSet::new(), 0);
        let spill_manager = SpillManager::new(env, metrics, Arc::clone(&schema));
        let spill_file = spill_manager
            .spill_record_batch_and_finish(&batches, "Test")?
            .unwrap();

        // Truncate the file mid-message: drop the 8 byte end-of-stream
        // marker plus part of the last batch's body.
        let path = spill_file.path().unwrap().to_path_buf();
        let len = std::fs::metadata(&path)?.len();
        let f = std::fs::OpenOptions::new().write(true).open(&path)?;
        f.set_len(len - 8 - 13)?;
        drop(f);

        let stream = spill_manager.read_spill_as_stream(spill_file, None)?;
        let result = collect(stream).await;
        assert!(
            result.is_err(),
            "truncated spill file should error, got Ok with {} batches",
            result.as_ref().map(|b| b.len()).unwrap_or(0)
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_batch_spill_and_read_dictionary_arrays() -> Result<()> {
        // See https://github.com/apache/datafusion/issues/4658

        let batch1 = build_table_i32(
            ("a2", &vec![0, 1, 2]),
            ("b2", &vec![3, 4, 5]),
            ("c2", &vec![4, 5, 6]),
        );

        let batch2 = build_table_i32(
            ("a2", &vec![10, 11, 12]),
            ("b2", &vec![13, 14, 15]),
            ("c2", &vec![14, 15, 16]),
        );

        // Dictionary encode the arrays
        let dict_type =
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Int32));
        let dict_schema = Arc::new(Schema::new(vec![
            Field::new("a2", dict_type.clone(), true),
            Field::new("b2", dict_type.clone(), true),
            Field::new("c2", dict_type.clone(), true),
        ]));

        let batch1 = RecordBatch::try_new(
            Arc::clone(&dict_schema),
            batch1
                .columns()
                .iter()
                .map(|array| cast(array, &dict_type))
                .collect::<Result<_, _>>()?,
        )?;

        let batch2 = RecordBatch::try_new(
            Arc::clone(&dict_schema),
            batch2
                .columns()
                .iter()
                .map(|array| cast(array, &dict_type))
                .collect::<Result<_, _>>()?,
        )?;

        // Construct SpillManager
        let env = Arc::new(RuntimeEnv::default());
        let metrics = SpillMetrics::new(&ExecutionPlanMetricsSet::new(), 0);
        let spill_manager = SpillManager::new(env, metrics, Arc::clone(&dict_schema));

        let num_rows = batch1.num_rows() + batch2.num_rows();
        let spill_file = spill_manager
            .spill_record_batch_and_finish(&[batch1, batch2], "Test")?
            .unwrap();
        let spilled_rows = spill_manager.metrics.spilled_rows.value();
        assert_eq!(spilled_rows, num_rows);

        let stream = spill_manager.read_spill_as_stream(spill_file, None)?;
        assert_eq!(stream.schema(), dict_schema);
        let batches = collect(stream).await?;
        assert_eq!(batches.len(), 2);

        Ok(())
    }

    #[tokio::test]
    async fn test_batch_spill_by_size() -> Result<()> {
        let batch1 = build_table_i32(
            ("a2", &vec![0, 1, 2, 3]),
            ("b2", &vec![3, 4, 5, 6]),
            ("c2", &vec![4, 5, 6, 7]),
        );

        let schema = batch1.schema();
        let env = Arc::new(RuntimeEnv::default());
        let metrics = SpillMetrics::new(&ExecutionPlanMetricsSet::new(), 0);
        let spill_manager = SpillManager::new(env, metrics, Arc::clone(&schema));

        let row_batches: Vec<RecordBatch> =
            (0..batch1.num_rows()).map(|i| batch1.slice(i, 1)).collect();
        let (spill_file, max_batch_mem) = spill_manager
            .spill_record_batch_iter_and_return_max_batch_memory(
                row_batches.iter().map(Ok),
                "Test Spill",
            )?
            .unwrap();
        assert!(spill_file.path().unwrap().exists());
        assert!(max_batch_mem > 0);

        let stream = spill_manager.read_spill_as_stream(spill_file, None)?;
        assert_eq!(stream.schema(), schema);

        let batches = collect(stream).await?;
        assert_eq!(batches.len(), 4);

        Ok(())
    }

    fn build_compressible_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Utf8, false),
            Field::new("b", DataType::Int32, false),
            Field::new("c", DataType::Int32, true),
        ]));

        let a: ArrayRef = Arc::new(StringArray::from_iter_values(std::iter::repeat_n(
            "repeated", 100,
        )));
        let b: ArrayRef = Arc::new(Int32Array::from(vec![1; 100]));
        let c: ArrayRef = Arc::new(Int32Array::from(vec![2; 100]));

        RecordBatch::try_new(schema, vec![a, b, c]).unwrap()
    }

    async fn validate(
        spill_manager: &SpillManager,
        spill_file: Arc<dyn SpillFile>,
        num_rows: usize,
        schema: SchemaRef,
        batch_count: usize,
    ) -> Result<()> {
        let spilled_rows = spill_manager.metrics.spilled_rows.value();
        assert_eq!(spilled_rows, num_rows);

        let stream = spill_manager.read_spill_as_stream(spill_file, None)?;
        assert_eq!(stream.schema(), schema);

        let batches = collect(stream).await?;
        assert_eq!(batches.len(), batch_count);

        Ok(())
    }

    #[tokio::test]
    async fn test_spill_compression() -> Result<()> {
        let batch = build_compressible_batch();
        let num_rows = batch.num_rows();
        let schema = batch.schema();
        let batch_count = 1;
        let batches = [batch];

        // Construct SpillManager
        let env = Arc::new(RuntimeEnv::default());
        let uncompressed_metrics = SpillMetrics::new(&ExecutionPlanMetricsSet::new(), 0);
        let lz4_metrics = SpillMetrics::new(&ExecutionPlanMetricsSet::new(), 0);
        let zstd_metrics = SpillMetrics::new(&ExecutionPlanMetricsSet::new(), 0);
        let uncompressed_spill_manager = SpillManager::new(
            Arc::clone(&env),
            uncompressed_metrics,
            Arc::clone(&schema),
        );
        let lz4_spill_manager =
            SpillManager::new(Arc::clone(&env), lz4_metrics, Arc::clone(&schema))
                .with_compression_type(SpillCompression::Lz4Frame);
        let zstd_spill_manager =
            SpillManager::new(env, zstd_metrics, Arc::clone(&schema))
                .with_compression_type(SpillCompression::Zstd);
        let uncompressed_spill_file = uncompressed_spill_manager
            .spill_record_batch_and_finish(&batches, "Test")?
            .unwrap();
        let lz4_spill_file = lz4_spill_manager
            .spill_record_batch_and_finish(&batches, "Lz4_Test")?
            .unwrap();
        let zstd_spill_file = zstd_spill_manager
            .spill_record_batch_and_finish(&batches, "ZSTD_Test")?
            .unwrap();
        assert!(uncompressed_spill_file.path().unwrap().exists());
        assert!(lz4_spill_file.path().unwrap().exists());
        assert!(zstd_spill_file.path().unwrap().exists());

        let lz4_spill_size = std::fs::metadata(lz4_spill_file.path().unwrap())?.len();
        let zstd_spill_size = std::fs::metadata(zstd_spill_file.path().unwrap())?.len();
        let uncompressed_spill_size =
            std::fs::metadata(uncompressed_spill_file.path().unwrap())?.len();

        assert!(uncompressed_spill_size > lz4_spill_size);
        assert!(uncompressed_spill_size > zstd_spill_size);

        validate(
            &lz4_spill_manager,
            lz4_spill_file,
            num_rows,
            Arc::clone(&schema),
            batch_count,
        )
        .await?;
        validate(
            &zstd_spill_manager,
            zstd_spill_file,
            num_rows,
            Arc::clone(&schema),
            batch_count,
        )
        .await?;
        validate(
            &uncompressed_spill_manager,
            uncompressed_spill_file,
            num_rows,
            schema,
            batch_count,
        )
        .await?;
        Ok(())
    }

    // ==== Spill manager tests ====

    #[test]
    fn test_spill_manager_spill_record_batch_and_finish() -> Result<()> {
        let env = Arc::new(RuntimeEnv::default());
        let metrics = SpillMetrics::new(&ExecutionPlanMetricsSet::new(), 0);
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Utf8, false),
        ]));

        let spill_manager = SpillManager::new(env, metrics, Arc::clone(&schema));

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
            ],
        )?;

        let temp_file = spill_manager.spill_record_batch_and_finish(&[batch], "Test")?;
        assert!(temp_file.is_some());
        assert!(temp_file.unwrap().path().unwrap().exists());
        Ok(())
    }

    fn verify_metrics(
        in_progress_file: &InProgressSpillFile,
        expected_spill_file_count: usize,
        expected_spilled_bytes: usize,
        expected_spilled_rows: usize,
    ) -> Result<()> {
        let actual_spill_file_count = in_progress_file
            .spill_writer
            .metrics
            .spill_file_count
            .value();
        let actual_spilled_bytes =
            in_progress_file.spill_writer.metrics.spilled_bytes.value();
        let actual_spilled_rows =
            in_progress_file.spill_writer.metrics.spilled_rows.value();

        assert_eq!(
            actual_spill_file_count, expected_spill_file_count,
            "Spill file count mismatch"
        );
        assert_eq!(
            actual_spilled_bytes, expected_spilled_bytes,
            "Spilled bytes mismatch"
        );
        assert_eq!(
            actual_spilled_rows, expected_spilled_rows,
            "Spilled rows mismatch"
        );

        Ok(())
    }

    #[test]
    fn test_in_progress_spill_file_append_and_finish() -> Result<()> {
        let env = Arc::new(RuntimeEnv::default());
        let metrics = SpillMetrics::new(&ExecutionPlanMetricsSet::new(), 0);
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Utf8, false),
        ]));

        let spill_manager =
            Arc::new(SpillManager::new(env, metrics, Arc::clone(&schema)));
        let mut in_progress_file = spill_manager.create_in_progress_file("Test")?;

        let batch1 = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
            ],
        )?;

        let batch2 = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(vec![4, 5, 6])),
                Arc::new(StringArray::from(vec!["d", "e", "f"])),
            ],
        )?;
        // After appending each batch, spilled_rows and spilled_bytes should increase incrementally,
        // while spill_file_count remains 1 (since we're writing to the same file)
        in_progress_file.append_batch(&batch1)?;
        verify_metrics(&in_progress_file, 1, 440, 3)?;

        in_progress_file.append_batch(&batch2)?;
        verify_metrics(&in_progress_file, 1, 704, 6)?;

        let completed_file = in_progress_file.finish()?;
        assert!(completed_file.is_some());
        assert!(completed_file.unwrap().path().unwrap().exists());
        verify_metrics(&in_progress_file, 1, 712, 6)?;
        // Double finish produce error
        let result = in_progress_file.finish();
        assert!(result.is_err());

        Ok(())
    }

    // Test write no batches
    #[test]
    fn test_in_progress_spill_file_write_no_batches() -> Result<()> {
        let env = Arc::new(RuntimeEnv::default());
        let metrics = SpillMetrics::new(&ExecutionPlanMetricsSet::new(), 0);
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Utf8, false),
        ]));

        let spill_manager =
            Arc::new(SpillManager::new(env, metrics, Arc::clone(&schema)));

        // Test write empty batch with interface `InProgressSpillFile` and `append_batch()`
        let mut in_progress_file = spill_manager.create_in_progress_file("Test")?;
        let completed_file = in_progress_file.finish()?;
        assert!(completed_file.is_none());

        // Test write empty batch with interface `spill_record_batch_and_finish()`
        let completed_file = spill_manager.spill_record_batch_and_finish(&[], "Test")?;
        assert!(completed_file.is_none());

        // Test write empty batch with interface `spill_record_batch_iter_and_return_max_batch_memory()`
        let empty_batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(Vec::<Option<i32>>::new())),
                Arc::new(StringArray::from(Vec::<Option<&str>>::new())),
            ],
        )?;
        let completed_file = spill_manager
            .spill_record_batch_iter_and_return_max_batch_memory(
                std::iter::once(Ok(&empty_batch)),
                "Test",
            )?;
        assert!(completed_file.is_none());

        Ok(())
    }

    #[test]
    fn test_reading_more_spills_than_tokio_blocking_threads() -> Result<()> {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .max_blocking_threads(1)
            .build()
            .unwrap()
            .block_on(async {
                let batch = build_table_i32(
                    ("a2", &vec![0, 1, 2]),
                    ("b2", &vec![3, 4, 5]),
                    ("c2", &vec![4, 5, 6]),
                );

                let schema = batch.schema();

                // Construct SpillManager
                let env = Arc::new(RuntimeEnv::default());
                let metrics = SpillMetrics::new(&ExecutionPlanMetricsSet::new(), 0);
                let spill_manager = SpillManager::new(env, metrics, Arc::clone(&schema));
                let batches: [_; 10] = std::array::from_fn(|_| batch.clone());

                let spill_file_1 = spill_manager
                    .spill_record_batch_and_finish(&batches, "Test1")?
                    .unwrap();
                let spill_file_2 = spill_manager
                    .spill_record_batch_and_finish(&batches, "Test2")?
                    .unwrap();

                let mut stream_1 =
                    spill_manager.read_spill_as_stream(spill_file_1, None)?;
                let mut stream_2 =
                    spill_manager.read_spill_as_stream(spill_file_2, None)?;
                stream_1.next().await;
                stream_2.next().await;

                Ok(())
            })
    }

    #[test]
    fn test_alignment_for_schema() -> Result<()> {
        let schema = Schema::new(vec![Field::new("strings", DataType::Utf8View, false)]);
        let alignment = get_max_alignment_for_schema(&schema);
        assert_eq!(alignment, 16);

        let schema = Schema::new(vec![
            Field::new("int32", DataType::Int32, false),
            Field::new("int64", DataType::Int64, false),
        ]);
        let alignment = get_max_alignment_for_schema(&schema);
        assert_eq!(alignment, 8);
        Ok(())
    }
    #[tokio::test]
    async fn test_real_time_spill_metrics() -> Result<()> {
        let env = Arc::new(RuntimeEnv::default());
        let metrics = SpillMetrics::new(&ExecutionPlanMetricsSet::new(), 0);
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Utf8, false),
        ]));

        let spill_manager = Arc::new(SpillManager::new(
            Arc::clone(&env),
            metrics.clone(),
            Arc::clone(&schema),
        ));
        let mut in_progress_file = spill_manager.create_in_progress_file("Test")?;

        let batch1 = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
            ],
        )?;

        // Before any batch, metrics should be 0
        assert_eq!(metrics.spilled_bytes.value(), 0);
        assert_eq!(metrics.spill_file_count.value(), 0);

        // Append first batch
        in_progress_file.append_batch(&batch1)?;

        // Metrics should be updated immediately (at least schema and first batch)
        let bytes_after_batch1 = metrics.spilled_bytes.value();
        assert_eq!(bytes_after_batch1, 440);
        assert_eq!(metrics.spill_file_count.value(), 1);

        // Check global progress
        let progress = env.spilling_progress();
        assert_eq!(progress.current_bytes, bytes_after_batch1 as u64);
        assert_eq!(progress.active_files_count, 1);

        // Append another batch
        in_progress_file.append_batch(&batch1)?;
        let bytes_after_batch2 = metrics.spilled_bytes.value();
        assert!(bytes_after_batch2 > bytes_after_batch1);

        // Check global progress again
        let progress = env.spilling_progress();
        assert_eq!(progress.current_bytes, bytes_after_batch2 as u64);

        // Finish the file
        let spilled_file = in_progress_file.finish()?;
        let final_bytes = metrics.spilled_bytes.value();
        assert!(final_bytes > bytes_after_batch2);

        // Even after finish, file is still "active" until dropped
        let progress = env.spilling_progress();
        assert!(progress.current_bytes > 0);
        assert_eq!(progress.active_files_count, 1);

        drop(spilled_file);
        assert_eq!(env.spilling_progress().active_files_count, 0);
        assert_eq!(env.spilling_progress().current_bytes, 0);

        Ok(())
    }

    #[test]
    fn test_gc_string_view_before_spill() -> Result<()> {
        use arrow::array::StringViewArray;

        let strings: Vec<String> = (0..200)
            .map(|i| {
                if i % 2 == 0 {
                    "short_string".to_string()
                } else {
                    "this_is_a_much_longer_string_that_will_not_be_inlined".to_string()
                }
            })
            .collect();

        let string_array = StringViewArray::from(strings);
        let schema = Arc::new(Schema::new(vec![Field::new(
            "strings",
            DataType::Utf8View,
            false,
        )]));

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(string_array) as ArrayRef],
        )?;
        let sliced_batch = batch.slice(0, 20);
        let gc_batch = gc_view_arrays(&sliced_batch)?;

        assert_eq!(gc_batch.num_rows(), sliced_batch.num_rows());
        assert_eq!(gc_batch.num_columns(), sliced_batch.num_columns());

        Ok(())
    }

    #[test]
    fn test_gc_binary_view_before_spill() -> Result<()> {
        use arrow::array::BinaryViewArray;

        let binaries: Vec<Vec<u8>> = (0..200)
            .map(|i| {
                if i % 2 == 0 {
                    vec![1, 2, 3, 4]
                } else {
                    vec![1; 50]
                }
            })
            .collect();

        let binary_array =
            BinaryViewArray::from_iter(binaries.iter().map(|b| Some(b.as_slice())));
        let schema = Arc::new(Schema::new(vec![Field::new(
            "binaries",
            DataType::BinaryView,
            false,
        )]));

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(binary_array) as ArrayRef],
        )?;
        let sliced_batch = batch.slice(0, 20);
        let gc_batch = gc_view_arrays(&sliced_batch)?;

        assert_eq!(gc_batch.num_rows(), sliced_batch.num_rows());
        assert_eq!(gc_batch.num_columns(), sliced_batch.num_columns());

        Ok(())
    }

    #[test]
    fn test_gc_skips_small_arrays() -> Result<()> {
        use arrow::array::StringViewArray;

        let strings: Vec<String> = (0..10).map(|i| format!("string_{i}")).collect();

        let string_array = StringViewArray::from(strings);
        let array_ref: ArrayRef = Arc::new(string_array);

        let schema = Arc::new(Schema::new(vec![Field::new(
            "strings",
            DataType::Utf8View,
            false,
        )]));

        let batch = RecordBatch::try_new(Arc::clone(&schema), vec![array_ref])?;

        // GC should return the original batch for small arrays
        let should_gc = should_gc_view_array(
            batch
                .column(0)
                .as_any()
                .downcast_ref::<StringViewArray>()
                .unwrap(),
        );
        let gc_batch = gc_view_arrays(&batch)?;

        assert!(!should_gc);
        assert_eq!(gc_batch.num_rows(), batch.num_rows());
        assert!(Arc::ptr_eq(batch.column(0), gc_batch.column(0)));

        Ok(())
    }

    #[test]
    fn test_gc_with_mixed_columns() -> Result<()> {
        use arrow::array::{Int32Array, StringViewArray};

        let strings: Vec<String> = (0..200)
            .map(|i| format!("long_string_for_gc_testing_{i}"))
            .collect();

        let string_array = StringViewArray::from(strings);
        let int_array = Int32Array::from((0..200).collect::<Vec<i32>>());

        let schema = Arc::new(Schema::new(vec![
            Field::new("strings", DataType::Utf8View, false),
            Field::new("ints", DataType::Int32, false),
        ]));

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(string_array) as ArrayRef,
                Arc::new(int_array) as ArrayRef,
            ],
        )?;

        let sliced_batch = batch.slice(0, 50);
        let gc_batch = gc_view_arrays(&sliced_batch)?;

        assert_eq!(gc_batch.num_columns(), 2);
        assert_eq!(gc_batch.num_rows(), 50);

        Ok(())
    }

    #[test]
    fn test_verify_gc_triggers_for_sliced_arrays() -> Result<()> {
        let strings: Vec<String> = (0..200)
            .map(|i| {
                format!(
                    "http://example.com/very/long/path/that/exceeds/inline/threshold/{i}"
                )
            })
            .collect();

        let string_array = StringViewArray::from(strings);
        let schema = Arc::new(Schema::new(vec![Field::new(
            "url",
            DataType::Utf8View,
            false,
        )]));

        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(string_array.clone()) as ArrayRef],
        )?;

        let sliced = batch.slice(0, 20);

        let sliced_array = sliced
            .column(0)
            .as_any()
            .downcast_ref::<StringViewArray>()
            .unwrap();
        let should_gc = should_gc_view_array(sliced_array);
        let waste_ratio = calculate_string_view_waste_ratio(sliced_array);

        assert!(
            waste_ratio > 0.8,
            "Waste ratio should be > 0.8 for sliced array"
        );
        assert!(
            should_gc,
            "GC should trigger for sliced array with high waste"
        );

        Ok(())
    }

    #[test]
    fn test_reproduce_issue_19414_string_view_spill_without_gc() -> Result<()> {
        use arrow::array::StringViewArray;
        use std::fs;

        let num_rows = 1000;
        let mut strings = Vec::with_capacity(num_rows);

        for i in 0..num_rows {
            let url = match i % 5 {
                0 => format!(
                    "http://irr.ru/index.php?showalbum/login-leniya7777294,938303130/{i}"
                ),
                1 => format!("http://komme%2F27.0.1453.116/very/long/path/{i}"),
                2 => format!("https://produkty%2Fproduct/category/item/{i}"),
                3 => format!(
                    "http://irr.ru/index.php?showalbum/login-kapusta-advert2668/{i}"
                ),
                4 => format!(
                    "http://irr.ru/index.php?showalbum/login-kapustic/product/{i}"
                ),
                _ => unreachable!(),
            };
            strings.push(url);
        }

        let string_array = StringViewArray::from(strings);
        let schema = Arc::new(Schema::new(vec![Field::new(
            "URL",
            DataType::Utf8View,
            false,
        )]));

        let original_batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(string_array.clone()) as ArrayRef],
        )?;

        let total_buffer_size: usize = string_array
            .data_buffers()
            .iter()
            .map(|buffer| buffer.capacity())
            .sum();

        let mut sliced_batches = Vec::new();
        let slice_size = 100;

        for i in (0..num_rows).step_by(slice_size) {
            let len = std::cmp::min(slice_size, num_rows - i);
            let sliced = original_batch.slice(i, len);
            sliced_batches.push(sliced);
        }

        let env = Arc::new(RuntimeEnv::default());
        let metrics = SpillMetrics::new(&ExecutionPlanMetricsSet::new(), 0);
        let spill_manager = SpillManager::new(env, metrics, schema);

        let mut in_progress_file = spill_manager.create_in_progress_file("Test GC")?;

        for batch in &sliced_batches {
            in_progress_file.append_batch(batch)?;
        }

        let spill_file = in_progress_file.finish()?.unwrap();
        let file_size = fs::metadata(spill_file.path().unwrap())?.len() as usize;

        let theoretical_without_gc = total_buffer_size * sliced_batches.len();
        let reduction_percent = ((theoretical_without_gc - file_size) as f64
            / theoretical_without_gc as f64)
            * 100.0;

        assert!(
            reduction_percent > 80.0,
            "GC should reduce spill file size by >80%, got {reduction_percent:.1}%"
        );

        Ok(())
    }

    #[test]
    fn test_spill_with_and_without_gc_comparison() -> Result<()> {
        let num_rows = 400;
        let strings: Vec<String> = (0..num_rows)
            .map(|i| {
                format!(
                    "http://example.com/this/is/a/long/url/path/that/wont/be/inlined/{i}"
                )
            })
            .collect();

        let string_array = StringViewArray::from(strings);
        let schema = Arc::new(Schema::new(vec![Field::new(
            "url",
            DataType::Utf8View,
            false,
        )]));

        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(string_array) as ArrayRef])?;

        let sliced_batch = batch.slice(0, 40);

        let array_without_gc = sliced_batch
            .column(0)
            .as_any()
            .downcast_ref::<StringViewArray>()
            .unwrap();
        let size_without_gc: usize = array_without_gc
            .data_buffers()
            .iter()
            .map(|buffer| buffer.len())
            .sum();

        let gc_batch = gc_view_arrays(&sliced_batch)?;
        let array_with_gc = gc_batch
            .column(0)
            .as_any()
            .downcast_ref::<StringViewArray>()
            .unwrap();
        let size_with_gc: usize = array_with_gc
            .data_buffers()
            .iter()
            .map(|buffer| buffer.len())
            .sum();

        let reduction_percent =
            ((size_without_gc - size_with_gc) as f64 / size_without_gc as f64) * 100.0;

        assert!(
            reduction_percent > 85.0,
            "Expected >85% reduction for 10% slice, got {reduction_percent:.1}%"
        );

        Ok(())
    }

    #[test]
    fn test_gc_recurses_into_nested_view_arrays() -> Result<()> {
        use arrow::array::{DictionaryArray, Int32Array};
        use arrow::buffer::Buffer;

        // A small pool of distinct, non-inlined (> 12 byte) strings.
        let distinct: Vec<String> = (0..8)
            .map(|i| format!("http://example.com/nested/path/that/is/not/inlined/{i}"))
            .collect();

        // Bytes stored once each string is deduplicated (all references collapse
        // to a single copy of every distinct value).
        let distinct_bytes: usize = distinct.iter().map(|s| s.len()).sum();

        let strings: Vec<String> = (0..200)
            .map(|i| distinct[i % distinct.len()].clone())
            .collect();
        let string_values = Arc::new(StringViewArray::from(strings)) as ArrayRef;

        let list_data = ArrayDataBuilder::new(DataType::List(Arc::new(
            Field::new_list_field(DataType::Utf8View, true),
        )))
        .len(20)
        .buffers(vec![Buffer::from_iter((0..=20).map(|i| i * 5_i32))])
        .child_data(vec![string_values.slice(0, 100).to_data()])
        .build()?;
        let list_array = make_array(list_data);

        let keys = Int32Array::from_iter_values(0..20);
        let dictionary = DictionaryArray::new(keys, string_values.slice(0, 20));
        let dictionary_array = Arc::new(dictionary) as ArrayRef;

        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "list_strings",
                DataType::List(Arc::new(Field::new_list_field(DataType::Utf8View, true))),
                false,
            ),
            Field::new(
                "dictionary_strings",
                DataType::Dictionary(
                    Box::new(DataType::Int32),
                    Box::new(DataType::Utf8View),
                ),
                false,
            ),
        ]));
        let batch = RecordBatch::try_new(schema, vec![list_array, dictionary_array])?;
        let gc_batch = gc_view_arrays(&batch)?;

        let gc_list_values = gc_batch.column(0).to_data().child_data()[0].clone();
        let gc_list_values = make_array(gc_list_values);
        let gc_list_values = gc_list_values
            .as_any()
            .downcast_ref::<StringViewArray>()
            .unwrap();
        let list_stored_bytes: usize =
            gc_list_values.data_buffers().iter().map(|b| b.len()).sum();
        assert_eq!(
            list_stored_bytes, distinct_bytes,
            "GC should deduplicate nested List child views (regression: dedup not applied)"
        );

        let gc_dictionary_values = gc_batch.column(1).to_data().child_data()[0].clone();
        let gc_dictionary_values = make_array(gc_dictionary_values);
        let gc_dictionary_values = gc_dictionary_values
            .as_any()
            .downcast_ref::<StringViewArray>()
            .unwrap();
        let dictionary_stored_bytes: usize = gc_dictionary_values
            .data_buffers()
            .iter()
            .map(|b| b.len())
            .sum();
        assert_eq!(
            dictionary_stored_bytes, distinct_bytes,
            "GC should deduplicate nested Dictionary values (regression: dedup not applied)"
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_spill_file_size_gc_verification_string_view() -> Result<()> {
        use arrow::array::StringViewArray;
        use std::fs;

        // 1. Setup bloated data (large buffers)
        let num_rows = 1000;
        let string_array: StringViewArray = (0..num_rows)
            .map(|i| Some(format!("this_is_a_long_string_to_ensure_it_is_not_inlined_and_causes_waste_{i}")))
            .collect();
        let schema = Arc::new(Schema::new(vec![Field::new(
            "s",
            DataType::Utf8View,
            false,
        )]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(string_array.clone()) as ArrayRef],
        )?;

        // 2. Slice it heavily (1% of the data)
        let sliced_batch = batch.slice(0, 10);

        // 3. Spill to disk using SpillManager
        let env = Arc::new(RuntimeEnv::default());
        let metrics = SpillMetrics::new(&ExecutionPlanMetricsSet::new(), 0);
        let spill_manager = SpillManager::new(env, metrics, schema);
        let spill_file = spill_manager
            .spill_record_batch_and_finish(&[sliced_batch], "TestGC")?
            .unwrap();

        // 4. Check file size on disk
        let file_size = fs::metadata(spill_file.path().unwrap())?.len();

        // The original buffer size is around 70KB.
        // Without GC, the spill file would be > 70KB.
        // With GC, it should be much smaller (only 10 rows of ~70 bytes each + metadata).
        assert!(
            file_size < 10 * 1024,
            "Spill file is too large ({file_size} bytes)! GC might not be working."
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_spill_file_size_gc_verification_binary_view() -> Result<()> {
        use arrow::array::BinaryViewArray;
        use std::fs;

        // 1. Setup bloated data (large buffers)
        let num_rows = 1000;
        let binary_array: BinaryViewArray =
            (0..num_rows).map(|i| Some(vec![i as u8; 100])).collect();
        let schema = Arc::new(Schema::new(vec![Field::new(
            "b",
            DataType::BinaryView,
            false,
        )]));
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(binary_array.clone()) as ArrayRef],
        )?;

        // 2. Slice it heavily (1% of the data)
        let sliced_batch = batch.slice(0, 10);

        // 3. Spill to disk using SpillManager
        let env = Arc::new(RuntimeEnv::default());
        let metrics = SpillMetrics::new(&ExecutionPlanMetricsSet::new(), 0);
        let spill_manager = SpillManager::new(env, metrics, schema);
        let spill_file = spill_manager
            .spill_record_batch_and_finish(&[sliced_batch], "TestGCBinary")?
            .unwrap();

        // 4. Check file size on disk
        let file_size = fs::metadata(spill_file.path().unwrap())?.len();

        // Original buffer is 100KB.
        // With GC, it should be much smaller.
        assert!(
            file_size < 10 * 1024,
            "Spill file is too large ({file_size} bytes)! GC might not be working."
        );

        Ok(())
    }
}
