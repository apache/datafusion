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

use std::fmt::Write as _;
use std::ops::Range;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use bytes::Bytes;
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
use url::Url;

// Tests are in submodules
mod bloom_filter_io;

#[derive(Debug, Clone)]
struct ByteRangeDescriptor {
    range: Range<u64>,
    description: String,
}

#[derive(Debug, Clone, Default)]
struct ParquetFileLayout {
    descriptors: Vec<ByteRangeDescriptor>,
}

impl ParquetFileLayout {
    fn new(bytes: &Bytes) -> Self {
        let cursor = bytes.clone();
        let reader = SerializedFileReader::new(cursor).expect("valid parquet file");
        let metadata = reader.metadata();
        let mut descriptors = Vec::new();

        for (rg_idx, rg) in metadata.row_groups().iter().enumerate() {
            for col in rg.columns() {
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

                // Data pages / column chunk byte range
                let data_start = col
                    .dictionary_page_offset()
                    .and_then(|o| u64::try_from(o).ok())
                    .unwrap_or_else(|| col.data_page_offset() as u64);
                let data_end = data_start + col.compressed_size() as u64;
                descriptors.push(ByteRangeDescriptor {
                    range: data_start..data_end,
                    description: format!(
                        "data page for column '{col_name}' (rg {rg_idx})"
                    ),
                });
            }
        }

        Self { descriptors }
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
        format!("byte range (offsets {}..{})", range.start, range.end)
    }
}

#[derive(Debug)]
struct InstrumentedObjectStore {
    inner: Arc<dyn ObjectStore>,
    layout: ParquetFileLayout,
    file_size: u64,
    events: Arc<Mutex<Vec<String>>>,
}

impl InstrumentedObjectStore {
    fn new(
        inner: Arc<dyn ObjectStore>,
        layout: ParquetFileLayout,
        file_size: u64,
        events: Arc<Mutex<Vec<String>>>,
    ) -> Self {
        Self {
            inner,
            layout,
            file_size,
            events,
        }
    }

    fn record_range(&self, range: &Range<u64>) {
        let desc = self.layout.describe_range(range, self.file_size);
        self.events.lock().unwrap().push(format!("Read {desc}"));
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
        if let Some(range) = &options.range {
            match range {
                GetRange::Bounded(r) => {
                    self.record_range(&(r.start..r.end));
                }
                GetRange::Offset(offset) => {
                    self.record_range(&(*offset..self.file_size));
                }
                GetRange::Suffix(suffix) => {
                    let start = self.file_size.saturating_sub(*suffix);
                    self.record_range(&(start..self.file_size));
                }
            }
        } else {
            self.record_range(&(0..self.file_size));
        }
        self.inner.get_opts(location, options).await
    }

    async fn get_ranges(
        &self,
        location: &Path,
        ranges: &[Range<u64>],
    ) -> object_store::Result<Vec<Bytes>> {
        for range in ranges {
            self.record_range(range);
        }
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
/// [`InstrumentedObjectStore`] that records every storage request.
///
/// Typical use:
///
/// ```text
/// let test = ParquetIoTest::new(file_bytes, SessionConfig::new()).await;
/// assert_snapshot!(test.query("SELECT * FROM t WHERE ...").await, @"...");
/// ```
pub(crate) struct ParquetIoTest {
    ctx: SessionContext,
    events: Arc<Mutex<Vec<String>>>,
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

        let events = Arc::new(Mutex::new(Vec::new()));
        let instrumented_store = Arc::new(InstrumentedObjectStore::new(
            memory_store,
            layout,
            file_size,
            Arc::clone(&events),
        ));

        let ctx = SessionContext::new_with_config(config);
        ctx.register_object_store(&Url::parse("mem://").unwrap(), instrumented_store);
        ctx.register_parquet("t", "mem:///table.parquet", ParquetReadOptions::default())
            .await
            .unwrap();

        Self { ctx, events }
    }

    /// Runs `sql` and returns the storage requests and output batches it
    /// produced, formatted for snapshot comparison.
    ///
    /// Only events recorded while running this query are included.
    pub(crate) async fn query(&self, sql: &str) -> String {
        self.events.lock().unwrap().clear();

        let df = self.ctx.sql(sql).await.unwrap();
        let mut stream = df.execute_stream().await.unwrap();

        let mut row_count = 0;
        while let Some(batch) = stream.next().await {
            let batch = batch.unwrap();
            row_count += batch.num_rows();
            self.events
                .lock()
                .unwrap()
                .push(format!("Produce batch ({} rows)", batch.num_rows()));
        }

        let recorded = self.events.lock().unwrap().clone();
        let mut output = format!("--- Query: {sql} ({row_count} rows) ---\n");
        for event in recorded {
            let _ = writeln!(output, "{event}");
        }
        output
    }
}
