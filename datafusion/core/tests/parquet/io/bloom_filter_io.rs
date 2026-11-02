// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Tests tracking storage I/O events during Parquet Bloom filter evaluation.

use std::sync::{Arc, Mutex};

use arrow::array::{ArrayRef, Int32Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use bytes::Bytes;
use datafusion::prelude::{ParquetReadOptions, SessionConfig, SessionContext};
use insta::assert_snapshot;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{ObjectStore, PutOptions, PutPayload};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::{EnabledStatistics, WriterProperties};
use url::Url;

use super::{InstrumentedObjectStore, ParquetFileLayout, run_query_and_collect_events};

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
    assert_snapshot!(combined_trace, @r"
    --- Query: SELECT * FROM t WHERE a = 10 AND b = 100 (1 rows) ---
    Read Parquet footer (offsets 0..678)
    Read OffsetIndex for column 'a' (rg 0) (offsets 192..213)
    Read Bloom filter for column 'a' (rg 0) (offsets 98..145)
    Read Bloom filter for column 'b' (rg 0) (offsets 145..192)
    Read data page for column 'a' (rg 0) (offsets 4..51)
    Read data page for column 'b' (rg 0) (offsets 51..98)
    Produce batch (1 rows)

    --- Query: SELECT * FROM t WHERE a = 999 AND b = 100 (0 rows) ---
    Read Parquet footer (offsets 0..678)
    Read Bloom filter for column 'a' (rg 0) (offsets 98..145)
    Read Bloom filter for column 'b' (rg 0) (offsets 145..192)
    ");
}
