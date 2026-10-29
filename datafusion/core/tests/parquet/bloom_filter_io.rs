// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Integration tests tracking storage I/O events during Parquet Bloom filter evaluation.
//!
//! Uses an instrumented [`ObjectStore`] to record storage requests (footers, page indexes,
//! bloom filters, data pages) and compares the resulting trace against an `insta` snapshot.

use std::fmt::Write as _;
use std::ops::Range;
use std::sync::{Arc, Mutex};

use arrow::array::{ArrayRef, Int32Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use async_trait::async_trait;
use bytes::Bytes;
use datafusion::prelude::{ParquetReadOptions, SessionConfig, SessionContext};
use futures::StreamExt;
use futures::stream::BoxStream;
use insta::assert_snapshot;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{
    GetOptions, GetRange, GetResult, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult,
};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::{EnabledStatistics, WriterProperties};
use parquet::file::reader::{FileReader, SerializedFileReader};
use url::Url;

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

async fn run_query_and_collect_events(
    ctx: &SessionContext,
    sql: &str,
    events: &Arc<Mutex<Vec<String>>>,
) -> String {
    events.lock().unwrap().clear();

    let df = ctx.sql(sql).await.unwrap();
    let mut stream = df.execute_stream().await.unwrap();

    let mut row_count = 0;
    while let Some(batch) = stream.next().await {
        let batch = batch.unwrap();
        row_count += batch.num_rows();
        events
            .lock()
            .unwrap()
            .push(format!("Produce batch ({} rows)", batch.num_rows()));
    }

    let recorded = events.lock().unwrap().clone();
    let mut output = format!("--- Query: {sql} ({row_count} rows) ---\n");
    for event in recorded {
        let _ = writeln!(output, "{event}");
    }
    output
}

#[tokio::test]
async fn test_parquet_bloom_filter_io_baseline() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int32, false),
        Field::new("b", DataType::Int32, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![10, 20, 30])) as ArrayRef,
            Arc::new(Int32Array::from(vec![100, 200, 300])) as ArrayRef,
        ],
    )
    .unwrap();

    // Disable row group and page statistics so pruning relies entirely on Bloom filters
    let props = WriterProperties::builder()
        .set_max_row_group_row_count(Some(3))
        .set_bloom_filter_enabled(true)
        .set_statistics_enabled(EnabledStatistics::None)
        .build();

    let mut buffer = Vec::new();
    let mut writer =
        ArrowWriter::try_new(&mut buffer, batch.schema(), Some(props)).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();

    let file_bytes = Bytes::from(buffer);
    let file_size = file_bytes.len() as u64;
    let layout = ParquetFileLayout::new(&file_bytes);

    let memory_store = Arc::new(InMemory::new());
    let path = Path::from("table.parquet");
    memory_store
        .put_opts(&path, PutPayload::from(file_bytes), PutOptions::default())
        .await
        .unwrap();

    let events = Arc::new(Mutex::new(Vec::new()));
    let instrumented_store = Arc::new(InstrumentedObjectStore::new(
        memory_store,
        layout,
        file_size,
        events.clone(),
    ));

    let config = SessionConfig::new()
        .with_target_partitions(1)
        .with_parquet_bloom_filter_pruning(true);

    let ctx = SessionContext::new_with_config(config);
    let table_url = Url::parse("mem://").unwrap();
    ctx.register_object_store(&table_url, instrumented_store);

    ctx.register_parquet("t", "mem:///table.parquet", ParquetReadOptions::default())
        .await
        .unwrap();

    // Case 1: Matching row group (a = 10 AND b = 100)
    // Loads Bloom filters, verifies match, then reads data pages and produces batch
    let trace_matching = run_query_and_collect_events(
        &ctx,
        "SELECT * FROM t WHERE a = 10 AND b = 100",
        &events,
    )
    .await;

    // Case 2: Column 'a' does not match (a = 999 AND b = 100)
    // On main, all candidate column Bloom filters ('a' and 'b') are loaded upfront.
    // In #25811, incremental pruning will evaluate 'a' first, short-circuit, and skip loading 'b'.
    let trace_pruned = run_query_and_collect_events(
        &ctx,
        "SELECT * FROM t WHERE a = 999 AND b = 100",
        &events,
    )
    .await;

    let combined_trace = format!("{trace_matching}\n{trace_pruned}");
    assert_snapshot!(combined_trace);
}
