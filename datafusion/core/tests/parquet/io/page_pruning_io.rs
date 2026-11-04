// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Tests tracking I/O events during Parquet page pruning using the page index
//! (`datafusion.execution.parquet.enable_page_index`, see
//! [`ParquetOptions::enable_page_index`]).
//!
//! The file has a single row group, so row group statistics can not prune
//! anything. Each column chunk is split into three data pages of three rows,
//! and `id` is sorted, so the page index (per page min/max of `id`) can
//! prune pages within the row group:
//!
//! ```text
//!                        row group 0
//!             column 'id'            column 'val'
//!           ┌──────────────┐       ┌──────────────┐
//!  page 0   │   1,  2,  3  │       │   0,  5, 10  │   rows 0..3
//!           ├──────────────┤       ├──────────────┤
//!  page 1   │   4,  5,  6  │       │   0,  7, 10  │   rows 3..6
//!           ├──────────────┤       ├──────────────┤
//!  page 2   │   7,  8,  9  │       │   0,  5, 10  │   rows 6..9
//!           └──────────────┘       └──────────────┘
//!  id page min/max:  page 0: 1/3   page 1: 4/6   page 2: 7/9
//!
//!  id = 5  -> rows 3..6 -> page 1 of each column
//!  id > 3  -> rows 3..9 -> pages 1 and 2 of each column
//! ```
//!
//! [`ParquetOptions::enable_page_index`]: datafusion::config::ParquetOptions::enable_page_index

use std::sync::Arc;

use arrow::array::{ArrayRef, Int32Array, RecordBatch};
use bytes::Bytes;
use datafusion::prelude::SessionConfig;
use insta::assert_snapshot;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;

use super::ParquetIoTest;

/// No predicate, so every page of every column is read as one column chunk.
#[tokio::test]
async fn test_page_pruning_io_no_predicate() {
    let test = page_pruning_test(true).await;
    assert_snapshot!(test.query("SELECT * FROM t").await, @r"
    --- Query: SELECT * FROM t (9 rows) ---
    Read Parquet footer (offsets 0..838)
    Read data pages 0..=2 for column 'id' (rg 0) (offsets 4..91)
    Read data pages 0..=2 for column 'val' (rg 0) (offsets 91..178)
    Produce batch (9 rows)
    ");
}

/// Only page 1 of `id` (`4..=6`) can contain `id = 5`, so only page 1 of
/// each column is fetched.
#[tokio::test]
async fn test_page_pruning_io_one_page() {
    let test = page_pruning_test(true).await;
    assert_snapshot!(test.query("SELECT * FROM t WHERE id = 5").await, @r"
    --- Query: SELECT * FROM t WHERE id = 5 (1 rows) ---
    Read Parquet footer (offsets 0..838)
    Read data page 1 for column 'id' (rg 0) (offsets 33..62)
    Read data page 1 for column 'val' (rg 0) (offsets 120..149)
    Produce batch (1 rows)
    ");
}

/// Page 0 of `id` (`1..=3`) cannot contain `id > 3`, so pages 1 and 2 of
/// each column are fetched.
#[tokio::test]
async fn test_page_pruning_io_two_pages() {
    let test = page_pruning_test(true).await;
    assert_snapshot!(test.query("SELECT * FROM t WHERE id > 3").await, @r"
    --- Query: SELECT * FROM t WHERE id > 3 (6 rows) ---
    Read Parquet footer (offsets 0..838)
    Read data page 1 for column 'id' (rg 0) (offsets 33..62)
    Read data page 2 for column 'id' (rg 0) (offsets 62..91)
    Read data page 1 for column 'val' (rg 0) (offsets 120..149)
    Read data page 2 for column 'val' (rg 0) (offsets 149..178)
    Produce batch (6 rows)
    ");
}

/// With page index pruning disabled, the whole column chunk of every column
/// is read even though only page 1 can match.
#[tokio::test]
async fn test_page_pruning_io_disabled() {
    let test = page_pruning_test(false).await;
    assert_snapshot!(test.query("SELECT * FROM t WHERE id = 5").await, @r"
    --- Query: SELECT * FROM t WHERE id = 5 (1 rows) ---
    Read Parquet footer (offsets 0..838)
    Read data pages 0..=2 for column 'id' (rg 0) (offsets 4..91)
    Read data pages 0..=2 for column 'val' (rg 0) (offsets 91..178)
    Produce batch (1 rows)
    ");
}

/// Returns a [`ParquetIoTest`] for the file described in the module docs
async fn page_pruning_test(page_index_pruning: bool) -> ParquetIoTest {
    let config = SessionConfig::new()
        .with_target_partitions(1)
        .with_parquet_page_index_pruning(page_index_pruning);
    ParquetIoTest::new(three_page_file(), config).await
}

/// Returns a Parquet file with one row group of nine rows, where each column
/// chunk is written as three data pages of three rows each (see the module
/// docs). Dictionary encoding is disabled so each column chunk contains only
/// data pages.
fn three_page_file() -> Bytes {
    let id: ArrayRef = Arc::new(Int32Array::from_iter_values(1..=9));
    let val: ArrayRef = Arc::new(Int32Array::from(vec![0, 5, 10, 0, 7, 10, 0, 5, 10]));
    let batch = RecordBatch::try_from_iter([("id", id), ("val", val)]).unwrap();

    let props = WriterProperties::builder()
        .set_dictionary_enabled(false)
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
