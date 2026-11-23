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
use parquet::file::FOOTER_SIZE;
use parquet::file::metadata::FooterTail;
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::file::serialized_reader::ReadOptionsBuilder;
use tracing::Instrument;
use url::Url;

// Tests are in submodules
mod bloom_filter_io;
mod filter_pushdown_io;
mod page_pruning_io;
mod row_group_pruning_io;
mod topk_io;
mod trace_capture;

use trace_capture::TraceCapture;

/// Kind of per column chunk metadata structure stored outside the footer
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MetadataKind {
    BloomFilter,
    ColumnIndex,
    OffsetIndex,
}

impl std::fmt::Display for MetadataKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::BloomFilter => write!(f, "Bloom filter"),
            Self::ColumnIndex => write!(f, "ColumnIndex"),
            Self::OffsetIndex => write!(f, "OffsetIndex"),
        }
    }
}

/// Byte range of a Bloom filter, ColumnIndex, or OffsetIndex of one column chunk
#[derive(Debug, Clone)]
struct ByteRangeDescriptor {
    range: Range<u64>,
    kind: MetadataKind,
    col_name: String,
    rg_idx: usize,
}

/// Describes the Bloom filters / page indexes in `descriptors` as
/// `<kinds> for column(s) '<a>', '<b>' (rg <x>, <y>)`.
///
/// Kinds that cover the same columns and row groups are merged into one
/// clause (for example `ColumnIndex and OffsetIndex for columns 'id', 'val'
/// (rg 0, 1, 2)`).
fn describe_descriptors(descriptors: &[&ByteRangeDescriptor]) -> Vec<String> {
    // (columns and row groups covered) -> kinds, in order of first appearance
    let mut groups: Vec<(String, Vec<MetadataKind>)> = vec![];
    for kind in [
        MetadataKind::BloomFilter,
        MetadataKind::ColumnIndex,
        MetadataKind::OffsetIndex,
    ] {
        let of_kind: Vec<&ByteRangeDescriptor> = descriptors
            .iter()
            .copied()
            .filter(|d| d.kind == kind)
            .collect();
        if of_kind.is_empty() {
            continue;
        }
        // Column names in file order, without repeating them for each row group
        let mut col_names: Vec<&str> = vec![];
        for d in &of_kind {
            if !col_names.contains(&d.col_name.as_str()) {
                col_names.push(&d.col_name);
            }
        }
        let mut rg_idxs: Vec<usize> = of_kind.iter().map(|d| d.rg_idx).collect();
        rg_idxs.sort_unstable();
        rg_idxs.dedup();
        let columns: Vec<String> = col_names.iter().map(|c| format!("'{c}'")).collect();
        let rgs: Vec<String> = rg_idxs.iter().map(|rg| rg.to_string()).collect();
        let covered = format!(
            "for column{} {} (rg {})",
            if columns.len() == 1 { "" } else { "s" },
            columns.join(", "),
            rgs.join(", ")
        );
        match groups.iter_mut().find(|(c, _)| *c == covered) {
            Some((_, kinds)) => kinds.push(kind),
            None => groups.push((covered, vec![kind])),
        }
    }
    groups
        .into_iter()
        .map(|(covered, kinds)| {
            let kinds: Vec<String> = kinds.iter().map(|k| k.to_string()).collect();
            format!("{} {covered}", kinds.join(" and "))
        })
        .collect()
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
                        kind: MetadataKind::BloomFilter,
                        col_name: col_name.to_string(),
                        rg_idx,
                    });
                }

                // Page index byte ranges
                if let Some(r) = col.column_index_range() {
                    descriptors.push(ByteRangeDescriptor {
                        range: r,
                        kind: MetadataKind::ColumnIndex,
                        col_name: col_name.to_string(),
                        rg_idx,
                    });
                }
                if let Some(r) = col.offset_index_range() {
                    descriptors.push(ByteRangeDescriptor {
                        range: r,
                        kind: MetadataKind::OffsetIndex,
                        col_name: col_name.to_string(),
                        rg_idx,
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

    /// Describes `range` in terms of the parts of the Parquet file it covers.
    ///
    /// A single request may span several items (for example, a coalesced read
    /// of adjacent column chunks), so all overlapping items are listed.
    fn describe_range(&self, range: &Range<u64>, file_size: u64) -> String {
        // The footer is read as a suffix of the file (see `footer_size`)
        if range.end >= file_size && (file_size.saturating_sub(range.start)) <= 8192 {
            return "footer".to_string();
        }
        let overlaps = |r: &Range<u64>| range.start < r.end && range.end > r.start;
        // Describe the overlapping Bloom filters / page indexes
        let descriptors: Vec<&ByteRangeDescriptor> = self
            .descriptors
            .iter()
            .filter(|desc| overlaps(&desc.range))
            .collect();
        let mut parts = describe_descriptors(&descriptors);
        parts.extend(
            self.column_chunks
                .iter()
                .filter(|chunk| overlaps(&chunk.range))
                .map(|chunk| chunk.describe(range)),
        );
        if parts.is_empty() {
            parts.push("unknown".to_string());
        }
        parts.join(", ")
    }
}

/// An [`ObjectStore`] that emits a `tracing` span for every operation. Each
/// byte range read gets an event that names the Parquet structure it covers.
#[derive(Debug)]
struct InstrumentedObjectStore {
    inner: Arc<dyn ObjectStore>,
    layout: ParquetFileLayout,
    file_size: u64,
}

/// Target of the spans and events that this test harness emits. The capture
/// layer only records targets that start with `datafusion`.
const TEST_TARGET: &str = "datafusion::test";

macro_rules! store_span {
    ($op:literal, $($fields:tt)*) => {
        tracing::debug_span!(target: TEST_TARGET, $op, $($fields)*)
    };
}

impl InstrumentedObjectStore {
    fn record_ranges(&self, ranges: impl IntoIterator<Item = Range<u64>>) {
        for range in ranges {
            tracing::debug!(
                target: TEST_TARGET,
                range = ?range,
                parquet = %self.layout.describe_range(&range, self.file_size),
                "Read byte range"
            );
        }
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
        let span =
            store_span!("ObjectStore::put", %location, bytes = payload.content_length());
        self.inner
            .put_opts(location, payload, opts)
            .instrument(span)
            .await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        let span = store_span!("ObjectStore::put_multipart", %location);
        self.inner
            .put_multipart_opts(location, opts)
            .instrument(span)
            .await
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        if options.head {
            let span = store_span!("ObjectStore::head", %location);
            return self
                .inner
                .get_opts(location, options)
                .instrument(span)
                .await;
        }
        let range = match &options.range {
            Some(GetRange::Bounded(r)) => r.clone(),
            Some(GetRange::Offset(offset)) => *offset..self.file_size,
            Some(GetRange::Suffix(suffix)) => {
                self.file_size.saturating_sub(*suffix)..self.file_size
            }
            None => 0..self.file_size,
        };
        let span = store_span!("ObjectStore::get", %location);
        span.in_scope(|| self.record_ranges([range]));
        self.inner
            .get_opts(location, options)
            .instrument(span)
            .await
    }

    async fn get_ranges(
        &self,
        location: &Path,
        ranges: &[Range<u64>],
    ) -> object_store::Result<Vec<Bytes>> {
        let span =
            store_span!("ObjectStore::get_ranges", %location, ranges = ranges.len());
        span.in_scope(|| self.record_ranges(ranges.iter().cloned()));
        self.inner
            .get_ranges(location, ranges)
            .instrument(span)
            .await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        tracing::debug!(target: TEST_TARGET, "ObjectStore::delete_stream");
        self.inner.delete_stream(locations)
    }

    fn list(
        &self,
        prefix: Option<&Path>,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        tracing::debug!(target: TEST_TARGET, prefix = ?prefix.map(Path::as_ref), "ObjectStore::list");
        self.inner.list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        tracing::debug!(
            target: TEST_TARGET,
            prefix = ?prefix.map(Path::as_ref),
            %offset,
            "ObjectStore::list_with_offset"
        );
        self.inner.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&Path>,
    ) -> object_store::Result<ListResult> {
        let span = store_span!(
            "ObjectStore::list_with_delimiter",
            prefix = ?prefix.map(Path::as_ref)
        );
        self.inner
            .list_with_delimiter(prefix)
            .instrument(span)
            .await
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        options: object_store::CopyOptions,
    ) -> object_store::Result<()> {
        let span = store_span!("ObjectStore::copy", %from, %to);
        self.inner
            .copy_opts(from, to, options)
            .instrument(span)
            .await
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
}

impl ParquetIoTest {
    /// Stores `file_bytes` as `mem:///table.parquet` and registers it as table
    /// `t` in a new [`SessionContext`] created with `config`.
    ///
    /// Note: sets the metadata size hint to the exact size of the footer so
    /// the first request reads only the footer metadata. Otherwise the default
    /// hint (512 KiB) covers the (small) test files entirely, hiding requests
    /// for the page index and Bloom filters.
    pub(crate) async fn new(file_bytes: Bytes, mut config: SessionConfig) -> Self {
        let file_size = file_bytes.len() as u64;
        let layout = ParquetFileLayout::new(&file_bytes);
        config.options_mut().execution.parquet.metadata_size_hint =
            Some(footer_size(&file_bytes));

        let memory_store = Arc::new(InMemory::new());
        memory_store
            .put_opts(
                &Path::from("table.parquet"),
                PutPayload::from(file_bytes),
                PutOptions::default(),
            )
            .await
            .unwrap();

        let instrumented_store = Arc::new(InstrumentedObjectStore {
            inner: memory_store,
            layout,
            file_size,
        });

        let ctx = SessionContext::new_with_config(config);
        ctx.register_object_store(&Url::parse("mem://").unwrap(), instrumented_store);
        ctx.register_parquet("t", "mem:///table.parquet", ParquetReadOptions::default())
            .await
            .unwrap();

        Self { ctx }
    }

    /// Captures the spans and events that DataFusion emits on this thread
    /// until the returned [`TraceCapture`] is dropped.
    pub(crate) fn capture_traces(&self) -> TraceCapture {
        TraceCapture::new()
    }

    /// Runs `sql` to completion inside a `query` span and returns the number
    /// of output rows. Emits a `Produce batch` event for each output batch.
    ///
    /// The file metadata cache is cleared first so each query shows the I/O
    /// of a cold read, including the footer and page index.
    pub(crate) async fn query(&self, sql: &str) -> Result<usize> {
        self.ctx
            .runtime_env()
            .cache_manager
            .get_file_metadata_cache()
            .clear();
        let span = tracing::debug_span!(
            target: TEST_TARGET,
            "Query",
            sql,
            rows = tracing::field::Empty
        );
        let row_count = async {
            let df = self.ctx.sql(sql).await?;
            let mut stream = df.execute_stream().await?;
            let mut row_count = 0;
            while let Some(batch) = stream.next().await {
                let rows = batch?.num_rows();
                row_count += rows;
                tracing::debug!(target: TEST_TARGET, rows, "Produce batch");
            }
            Ok::<_, datafusion::error::DataFusionError>(row_count)
        }
        .instrument(span.clone())
        .await?;
        span.record("rows", row_count);
        Ok(row_count)
    }
}

/// Returns the size in bytes of the Parquet footer (the serialized file
/// metadata plus the 8 byte footer tail) of `file_bytes`
fn footer_size(file_bytes: &Bytes) -> usize {
    let tail: &[u8; FOOTER_SIZE] = file_bytes[file_bytes.len() - FOOTER_SIZE..]
        .try_into()
        .unwrap();
    FooterTail::try_new(tail).unwrap().metadata_length() + FOOTER_SIZE
}
