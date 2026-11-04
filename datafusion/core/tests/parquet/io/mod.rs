// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Test infrastructure for tracking storage I/O events during Parquet scans.
//!
//! Provides an instrumented [`ObjectStore`] that records storage requests and
//! correlates each requested byte range with the Parquet file structure it
//! covers, so the resulting trace can be compared against `insta` snapshots.
//!
//! `ListingTable`object store access tests live in
//! [`datafusion/core/tests/datasource/object_store_access.rs`](https://github.com/apache/datafusion/blob/main/datafusion/core/tests/datasource/object_store_access.rs).

use std::ops::Range;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use async_trait::async_trait;
use bytes::Bytes;
use datafusion::error::Result;
use datafusion::prelude::{ParquetReadOptions, SessionConfig, SessionContext};
use futures::StreamExt;
use futures::stream::BoxStream;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{
    GetOptions, GetRange, GetResult, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult,
};
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::file::serialized_reader::ReadOptionsBuilder;
use url::Url;

// Tests are in submodules
mod bloom_filter_io;
mod filter_pushdown_io;
mod page_pruning_io;
mod row_group_pruning_io;
mod topk_io;
mod trace_capture;

use trace_capture::TraceCapture;

#[derive(Debug, Clone)]
struct ByteRangeDescriptor {
    range: Range<u64>,
    description: String,
}

/// Byte ranges of one column chunk:
///
/// 1. (optional) dictionary page
/// 2. data pages (from the offset index, if present)
#[derive(Debug, Clone)]
struct ColumnChunkLayout {
    col_name: String,
    /// Row Group Index
    rg_idx: usize,
    /// The whole column chunk (dictionary page + data pages)
    range: Range<u64>,
    /// Byte range of the dictionary page
    dictionary_page: Option<Range<u64>>,
    /// Byte range of each data page, in page order (empty without an offset index)
    data_pages: Vec<Range<u64>>,
}

impl ColumnChunkLayout {
    /// Describes `range` in terms of the pages of this column chunk it covers
    fn describe(&self, range: &Range<u64>) -> String {
        let overlaps = |r: &Range<u64>| range.start < r.end && range.end > r.start;
        let mut parts = vec![];
        if self.dictionary_page.as_ref().is_some_and(overlaps) {
            parts.push("dictionary page".to_string());
        }
        let pages: Vec<usize> = self
            .data_pages
            .iter()
            .enumerate()
            .filter(|(_, r)| overlaps(r))
            .map(|(idx, _)| idx)
            .collect();
        match pages.as_slice() {
            [] if self.data_pages.is_empty() => parts.push("data pages".to_string()),
            [] => {}
            [page] => parts.push(format!("data page {page}")),
            [first, .., last] if last - first + 1 == pages.len() => {
                parts.push(format!("data pages {first}..={last}"))
            }
            pages => {
                let pages: Vec<String> = pages.iter().map(|p| p.to_string()).collect();
                parts.push(format!("data pages {}", pages.join(", ")))
            }
        }
        format!(
            "{} for column '{}' (rg {})",
            parts.join(" and "),
            self.col_name,
            self.rg_idx
        )
    }
}

#[derive(Debug, Clone, Default)]
struct ParquetFileLayout {
    /// Bloom filter and page index byte ranges
    descriptors: Vec<ByteRangeDescriptor>,
    column_chunks: Vec<ColumnChunkLayout>,
}

impl ParquetFileLayout {
    fn new(bytes: &Bytes) -> Self {
        let cursor = bytes.clone();
        let options = ReadOptionsBuilder::new().with_page_index().build();
        let reader = SerializedFileReader::new_with_options(cursor, options)
            .expect("valid parquet file");
        let metadata = reader.metadata();
        let mut descriptors = Vec::new();
        let mut column_chunks = Vec::new();

        for (rg_idx, rg) in metadata.row_groups().iter().enumerate() {
            let page_index = metadata.page_index_for_row_group(rg_idx);
            for (col_idx, col) in rg.columns().iter().enumerate() {
                let col_name = col.column_descr().name();

                // Bloom filter byte range
                if let (Some(bf_offset), Some(bf_len)) =
                    (col.bloom_filter_offset(), col.bloom_filter_length())
                {
                    let start = bf_offset as u64;
                    let end = start + bf_len as u64;
                    descriptors.push(ByteRangeDescriptor {
                        range: start..end,
                        description: format!(
                            "Bloom filter for column '{col_name}' (rg {rg_idx})"
                        ),
                    });
                }

                // Page index byte ranges
                if let Some(r) = col.column_index_range() {
                    descriptors.push(ByteRangeDescriptor {
                        range: r,
                        description: format!(
                            "ColumnIndex for column '{col_name}' (rg {rg_idx})"
                        ),
                    });
                }
                if let Some(r) = col.offset_index_range() {
                    descriptors.push(ByteRangeDescriptor {
                        range: r,
                        description: format!(
                            "OffsetIndex for column '{col_name}' (rg {rg_idx})"
                        ),
                    });
                }

                // Dictionary page and data pages of the column chunk
                let dictionary_start = col
                    .dictionary_page_offset()
                    .and_then(|o| u64::try_from(o).ok());
                let data_pages: Vec<Range<u64>> = page_index
                    .offset_index(col_idx)
                    .map(|offset_index| {
                        offset_index
                            .page_locations()
                            .iter()
                            .map(|location| {
                                let start = location.offset as u64;
                                start..start + location.compressed_page_size as u64
                            })
                            .collect()
                    })
                    .unwrap_or_default();
                let dictionary_page = dictionary_start.map(|start| {
                    let end = data_pages
                        .first()
                        .map(|page| page.start)
                        .unwrap_or_else(|| col.data_page_offset() as u64);
                    start..end
                });
                let chunk_start =
                    dictionary_start.unwrap_or_else(|| col.data_page_offset() as u64);
                let chunk_end = chunk_start + col.compressed_size() as u64;
                column_chunks.push(ColumnChunkLayout {
                    col_name: col_name.to_string(),
                    rg_idx,
                    range: chunk_start..chunk_end,
                    dictionary_page,
                    data_pages,
                });
            }
        }

        Self {
            descriptors,
            column_chunks,
        }
    }

    fn describe_range(&self, range: &Range<u64>, file_size: u64) -> String {
        if range.end >= file_size && (file_size.saturating_sub(range.start)) <= 8192 {
            return format!("Parquet footer (offsets {}..{})", range.start, range.end);
        }
        for desc in &self.descriptors {
            if range.start < desc.range.end && range.end > desc.range.start {
                return format!(
                    "{} (offsets {}..{})",
                    desc.description, range.start, range.end
                );
            }
        }
        for chunk in &self.column_chunks {
            if range.start < chunk.range.end && range.end > chunk.range.start {
                return format!(
                    "{} (offsets {}..{})",
                    chunk.describe(range),
                    range.start,
                    range.end
                );
            }
        }
        format!("byte range (offsets {}..{})", range.start, range.end)
    }
}

#[derive(Debug)]
struct InstrumentedObjectStore {
    inner: Arc<dyn ObjectStore>,
    file_size: u64,
    /// Number of requests made to the store (reset for each query)
    requests: Arc<AtomicUsize>,
}

impl InstrumentedObjectStore {
    fn new(
        inner: Arc<dyn ObjectStore>,
        file_size: u64,
        requests: Arc<AtomicUsize>,
    ) -> Self {
        Self {
            inner,
            file_size,
            requests,
        }
    }

    /// Emits a span for a `HEAD` request, which reads no bytes.
    fn record_head_request(&self) {
        let request = self.requests.fetch_add(1, Ordering::SeqCst) + 1;
        let span = tracing::debug_span!(
            target: "datafusion::test::object_store",
            "object_store_request",
            request
        );
        span.in_scope(|| {
            tracing::debug!(target: "datafusion::test::object_store", "HEAD object metadata");
        });
    }

    /// Emits a span for one request to the store, with an event per byte
    /// range it reads.
    fn record_request(&self, ranges: impl IntoIterator<Item = Range<u64>>) {
        let request = self.requests.fetch_add(1, Ordering::SeqCst) + 1;
        let span = tracing::debug_span!(
            target: "datafusion::test::object_store",
            "object_store_request",
            request
        );
        span.in_scope(|| {
            for range in ranges {
                tracing::debug!(
                    target: "datafusion::test::object_store",
                    offset_start = range.start,
                    offset_end = range.end,
                    "Read byte range"
                );
            }
        });
    }
}

impl std::fmt::Display for InstrumentedObjectStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "InstrumentedObjectStore")
    }
}

#[async_trait]
impl ObjectStore for InstrumentedObjectStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        if options.head {
            self.record_head_request();
            return self.inner.get_opts(location, options).await;
        }
        let range = match &options.range {
            Some(GetRange::Bounded(r)) => r.clone(),
            Some(GetRange::Offset(offset)) => *offset..self.file_size,
            Some(GetRange::Suffix(suffix)) => {
                self.file_size.saturating_sub(*suffix)..self.file_size
            }
            None => 0..self.file_size,
        };
        self.record_request([range]);
        self.inner.get_opts(location, options).await
    }

    async fn get_ranges(
        &self,
        location: &Path,
        ranges: &[Range<u64>],
    ) -> object_store::Result<Vec<Bytes>> {
        self.record_request(ranges.iter().cloned());
        self.inner.get_ranges(location, ranges).await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(
        &self,
        prefix: Option<&Path>,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&Path>,
    ) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: object_store::CopyOptions,
    ) -> object_store::Result<()> {
        self.inner.copy_opts(from, to, options).await
    }
}

/// Test fixture: a single Parquet file registered as table `t`, read through an
/// [`InstrumentedObjectStore`] that emits a `tracing` span for every storage
/// request.
///
/// Typical use:
///
/// ```text
/// let test = ParquetIoTest::new(file_bytes, SessionConfig::new()).await;
/// let traces = test.capture_traces();
/// test.query("SELECT * FROM t WHERE ...").await.unwrap();
/// assert_snapshot!(traces.render(), @"...");
/// ```
pub(crate) struct ParquetIoTest {
    ctx: SessionContext,
    layout: ParquetFileLayout,
    file_size: u64,
    requests: Arc<AtomicUsize>,
}

impl ParquetIoTest {
    /// Stores `file_bytes` as `mem:///table.parquet` and registers it as table
    /// `t` in a new [`SessionContext`] created with `config`.
    pub(crate) async fn new(file_bytes: Bytes, config: SessionConfig) -> Self {
        let file_size = file_bytes.len() as u64;
        let layout = ParquetFileLayout::new(&file_bytes);

        let memory_store = Arc::new(InMemory::new());
        memory_store
            .put_opts(
                &Path::from("table.parquet"),
                PutPayload::from(file_bytes),
                PutOptions::default(),
            )
            .await
            .unwrap();

        let requests = Arc::new(AtomicUsize::new(0));
        let instrumented_store = Arc::new(InstrumentedObjectStore::new(
            memory_store,
            file_size,
            Arc::clone(&requests),
        ));

        let ctx = SessionContext::new_with_config(config);
        ctx.register_object_store(&Url::parse("mem://").unwrap(), instrumented_store);
        ctx.register_parquet("t", "mem:///table.parquet", ParquetReadOptions::default())
            .await
            .unwrap();

        Self {
            ctx,
            layout,
            file_size,
            requests,
        }
    }

    /// Captures the spans and events that DataFusion emits on this thread
    /// until the returned [`TraceCapture`] is dropped.
    pub(crate) fn capture_traces(&self) -> TraceCapture {
        TraceCapture::new(self.layout.clone(), self.file_size)
    }

    /// Runs `sql` to completion inside a `query` span and returns the number
    /// of output rows. Emits a `Produce batch` event for each output batch.
    pub(crate) async fn query(&self, sql: &str) -> Result<usize> {
        self.requests.store(0, Ordering::SeqCst);
        let span = tracing::debug_span!(
            target: "datafusion::test",
            "query",
            sql,
            rows = tracing::field::Empty
        );
        let _entered = span.enter();

        let df = self.ctx.sql(sql).await?;
        let mut stream = df.execute_stream().await?;
        let mut row_count = 0;
        while let Some(batch) = stream.next().await {
            let rows = batch?.num_rows();
            row_count += rows;
            tracing::debug!(target: "datafusion::test", rows, "Produce batch");
        }
        span.record("rows", row_count);
        Ok(row_count)
    }
}
