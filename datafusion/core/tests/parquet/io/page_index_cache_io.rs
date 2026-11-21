// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Tests tracking I/O events when the page index is loaded after the footer
//! and the file metadata cache is reused by later queries.
//!
//! Statistics collection is disabled so that planning does not load (and
//! cache) the page index. Each scan first reads the footer without the page
//! index, and loads the page index only if row group statistics can not prove
//! it is unneeded.
//!
//! Most tests use a file with one row group with three data pages of three
//! rows per column (see [`three_page_file`]), so the page index of `id` can
//! prune pages:
//!
//! ```text
//!  id page min/max:  page 0: 1/3   page 1: 4/6   page 2: 7/9
//!  id = 5  -> page 1 of each column
//!  id > 0  -> the row group is fully matched, the page index is not needed
//! ```

use std::sync::Arc;

use arrow::array::{ArrayRef, Int32Array, RecordBatch};
use bytes::Bytes;
use datafusion::assert_batches_eq;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::prelude::SessionConfig;
use insta::assert_snapshot;
use object_store::path::Path;
use object_store::{PutOptions, PutPayload};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::{EnabledStatistics, WriterProperties};

use super::ParquetIoTest;

const VAL: [i32; 9] = [0, 5, 10, 0, 7, 10, 0, 5, 10];

/// A page index loaded after the footer is cached with the footer, so a
/// later query of the same file reads neither again.
#[tokio::test]
async fn test_page_index_cache_io_repeated_query() {
    let test = page_index_cache_test(three_page_file(VAL, true)).await;
    let sql = "SELECT * FROM t WHERE id = 5";
    assert_snapshot!(test.query(sql).await, @r"
    --- Query: SELECT * FROM t WHERE id = 5 (1 rows) ---
    Request 1
        Head (object size and metadata)
    Request 2
        Read Parquet footer (offsets 323..838)
    Request 3
        Read ColumnIndex and OffsetIndex for columns 'id', 'val' (rg 0) (offsets 178..323)
    Request 4
        Read data page 1 for column 'id' (rg 0) (offsets 33..62)
        Read data page 1 for column 'val' (rg 0) (offsets 120..149)
    Produce batch (1 rows)
    ");
    assert_snapshot!(test.query_with_cached_metadata(sql).await, @r"
    --- Query: SELECT * FROM t WHERE id = 5 (1 rows) ---
    Request 1
        Head (object size and metadata)
    Request 2
        Read data page 1 for column 'id' (rg 0) (offsets 33..62)
        Read data page 1 for column 'val' (rg 0) (offsets 120..149)
    Produce batch (1 rows)
    ");
    let batches = test.ctx.sql(sql).await.unwrap().collect().await.unwrap();
    assert_batches_eq!(
        [
            "+----+-----+",
            "| id | val |",
            "+----+-----+",
            "| 5  | 7   |",
            "+----+-----+",
        ],
        &batches
    );
}

/// Each range of a file split across partitions opens the file separately.
/// Once the page index is cached, the opens of all ranges reuse it.
#[tokio::test]
async fn test_page_index_cache_io_file_ranges() {
    let mut config = SessionConfig::new()
        .with_target_partitions(2)
        .with_collect_statistics(false);
    config.options_mut().optimizer.repartition_file_min_size = 0;
    let test = ParquetIoTest::new(three_row_group_file(), config).await;
    let sql = "SELECT * FROM t WHERE val = 7 ORDER BY id";

    let plan = test.ctx.sql(&format!("EXPLAIN {sql}")).await.unwrap();
    let plan = arrow::util::pretty::pretty_format_batches(&plan.collect().await.unwrap())
        .unwrap()
        .to_string();
    assert_eq!(plan.matches("table.parquet:").count(), 2, "{plan}");

    // Partitions run concurrently, so the order of requests (and whether
    // both ranges read the page index on the first query) varies
    test.query(sql).await;
    let warm = test.query_with_cached_metadata(sql).await;
    assert!(!warm.contains("Parquet footer"), "{warm}");
    assert!(!warm.contains("ColumnIndex"), "{warm}");
    assert!(!warm.contains("OffsetIndex"), "{warm}");
    assert!(warm.contains("(rg 0)") && warm.contains("(rg 2)"), "{warm}");

    let batches = test.ctx.sql(sql).await.unwrap().collect().await.unwrap();
    assert_batches_eq!(
        [
            "+------+-----+",
            "| id   | val |",
            "+------+-----+",
            "| 500  | 7   |",
            "| 1500 | 7   |",
            "| 2500 | 7   |",
            "| 3500 | 7   |",
            "| 4500 | 7   |",
            "| 5500 | 7   |",
            "+------+-----+",
        ],
        &batches
    );
}

/// When row group statistics prove the page index is not needed, it is not
/// read by the first query, nor by a later query that reuses the footer.
#[tokio::test]
async fn test_page_index_cache_io_fully_matched() {
    let test = page_index_cache_test(three_page_file(VAL, true)).await;
    let sql = "SELECT * FROM t WHERE id > 0";
    assert_snapshot!(test.query(sql).await, @r"
    --- Query: SELECT * FROM t WHERE id > 0 (9 rows) ---
    Request 1
        Head (object size and metadata)
    Request 2
        Read Parquet footer (offsets 323..838)
    Request 3
        Read data pages 0..=2 for column 'id' (rg 0) (offsets 4..91)
        Read data pages 0..=2 for column 'val' (rg 0) (offsets 91..178)
    Produce batch (9 rows)
    ");
    assert_snapshot!(test.query_with_cached_metadata(sql).await, @r"
    --- Query: SELECT * FROM t WHERE id > 0 (9 rows) ---
    Request 1
        Head (object size and metadata)
    Request 2
        Read data pages 0..=2 for column 'id' (rg 0) (offsets 4..91)
        Read data pages 0..=2 for column 'val' (rg 0) (offsets 91..178)
    Produce batch (9 rows)
    ");
}

/// Without a column index for `id`, the page index is never read.
#[tokio::test]
async fn test_page_index_cache_io_no_column_index() {
    let test = page_index_cache_test(three_page_file(VAL, false)).await;
    let sql = "SELECT * FROM t WHERE id = 5";
    assert_snapshot!(test.query(sql).await, @r"
    --- Query: SELECT * FROM t WHERE id = 5 (1 rows) ---
    Request 1
        Head (object size and metadata)
    Request 2
        Read Parquet footer (offsets 229..734)
    Request 3
        Read data pages 0..=2 for column 'id' (rg 0) (offsets 4..91)
        Read data pages 0..=2 for column 'val' (rg 0) (offsets 91..178)
    Produce batch (1 rows)
    ");
    assert_snapshot!(test.query_with_cached_metadata(sql).await, @r"
    --- Query: SELECT * FROM t WHERE id = 5 (1 rows) ---
    Request 1
        Head (object size and metadata)
    Request 2
        Read data pages 0..=2 for column 'id' (rg 0) (offsets 4..91)
        Read data pages 0..=2 for column 'val' (rg 0) (offsets 91..178)
    Produce batch (1 rows)
    ");
}

/// A cached footer and page index are not reused after the file changes.
///
/// The new file has the same layout (only the values of `val` differ), so
/// the reported byte ranges still describe it.
#[tokio::test]
async fn test_page_index_cache_io_changed_file() {
    let test = page_index_cache_test(three_page_file(VAL, true)).await;
    let sql = "SELECT * FROM t WHERE id = 5 AND val = 8";
    assert_snapshot!(test.query(sql).await, @r"
    --- Query: SELECT * FROM t WHERE id = 5 AND val = 8 (0 rows) ---
    Request 1
        Head (object size and metadata)
    Request 2
        Read Parquet footer (offsets 323..838)
    Request 3
        Read ColumnIndex and OffsetIndex for columns 'id', 'val' (rg 0) (offsets 178..323)
    Request 4
        Read data page 1 for column 'id' (rg 0) (offsets 33..62)
        Read data page 1 for column 'val' (rg 0) (offsets 120..149)
    ");

    test.ctx
        .runtime_env()
        .object_store(ObjectStoreUrl::parse("mem://").unwrap())
        .unwrap()
        .put_opts(
            &Path::from("table.parquet"),
            PutPayload::from(three_page_file(VAL.map(|v| v + 1), true)),
            PutOptions::default(),
        )
        .await
        .unwrap();
    assert_snapshot!(test.query_with_cached_metadata(sql).await, @r"
    --- Query: SELECT * FROM t WHERE id = 5 AND val = 8 (1 rows) ---
    Request 1
        Head (object size and metadata)
    Request 2
        Read Parquet footer (offsets 323..838)
    Request 3
        Read ColumnIndex and OffsetIndex for columns 'id', 'val' (rg 0) (offsets 178..323)
    Request 4
        Read data page 1 for column 'id' (rg 0) (offsets 33..62)
        Read data page 1 for column 'val' (rg 0) (offsets 120..149)
    Produce batch (1 rows)
    ");
}

/// Returns a [`ParquetIoTest`] for `file` that does not collect statistics
/// during planning
async fn page_index_cache_test(file: Bytes) -> ParquetIoTest {
    let config = SessionConfig::new()
        .with_target_partitions(1)
        .with_collect_statistics(false);
    ParquetIoTest::new(file, config).await
}

/// Returns a Parquet file with three row groups of 2000 rows, `id` `0..6000`
/// and `val`, written as pages of 1000 rows. `val` is 7 when `id % 1000` is
/// 500, and `id % 5` otherwise. The column chunks are much larger than the
/// footer, so splitting the file in two puts row groups 0 and 1 in the first
/// range and row group 2 in the second.
fn three_row_group_file() -> Bytes {
    let id: ArrayRef = Arc::new(Int32Array::from_iter_values(0..6000));
    let val: ArrayRef = Arc::new(Int32Array::from_iter_values(
        (0..6000).map(|i| if i % 1000 == 500 { 7 } else { i % 5 }),
    ));
    let batch = RecordBatch::try_from_iter([("id", id), ("val", val)]).unwrap();
    let props = WriterProperties::builder()
        .set_dictionary_enabled(false)
        .set_max_row_group_row_count(Some(2000))
        .set_data_page_row_count_limit(1000)
        .set_write_batch_size(1000)
        .build();

    let mut buffer = Vec::new();
    let mut writer =
        ArrowWriter::try_new(&mut buffer, batch.schema(), Some(props)).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    Bytes::from(buffer)
}

/// Returns a Parquet file with one row group of nine rows, `id` `1..=9` and
/// `val`, where each column chunk is written as three data pages of three
/// rows each. The column index is written only if `page_statistics` is true.
fn three_page_file(val: [i32; 9], page_statistics: bool) -> Bytes {
    let id: ArrayRef = Arc::new(Int32Array::from_iter_values(1..=9));
    let val: ArrayRef = Arc::new(Int32Array::from(val.to_vec()));
    let batch = RecordBatch::try_from_iter([("id", id), ("val", val)]).unwrap();

    let statistics = if page_statistics {
        EnabledStatistics::Page
    } else {
        EnabledStatistics::Chunk
    };
    let props = WriterProperties::builder()
        .set_dictionary_enabled(false)
        .set_statistics_enabled(statistics)
        .set_data_page_row_count_limit(3)
        .set_write_batch_size(3)
        .build();

    let mut buffer = Vec::new();
    let mut writer =
        ArrowWriter::try_new(&mut buffer, batch.schema(), Some(props)).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    Bytes::from(buffer)
}
