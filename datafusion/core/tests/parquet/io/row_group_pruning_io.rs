// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Tests tracking I/O events during Parquet row group pruning
//!
//! All tests filter on `id`, whose values are sorted across row groups so the
//! row group statistics are disjoint and can prune:
//!
//! ```text
//!   row group 0        row group 1        row group 2
//! ┌──────┬───────┐   ┌──────┬───────┐   ┌──────┬───────┐
//! │  id  │  val  │   │  id  │  val  │   │  id  │  val  │
//! ├──────┼───────┤   ├──────┼───────┤   ├──────┼───────┤
//! │   1  │   0   │   │   4  │   0   │   │   7  │   0   │
//! │   2  │   5   │   │   5  │   7   │   │   8  │   5   │
//! │   3  │  10   │   │   6  │  10   │   │   9  │  10   │
//! └──────┴───────┘   └──────┴───────┘   └──────┴───────┘
//!  id: min 1 max 3    id: min 4 max 6    id: min 7 max 9
//!
//!  id = 5    -> row group 1 only
//!  id > 5    -> row groups 1 and 2
//!  id > 100  -> no row groups
//! ```

use std::sync::Arc;

use arrow::array::{ArrayRef, Int32Array, RecordBatch};
use bytes::Bytes;
use datafusion::prelude::SessionConfig;
use insta::assert_snapshot;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;

use super::ParquetIoTest;

/// No predicate, so every row group is read.
#[tokio::test]
async fn test_row_group_pruning_io_no_predicate() {
    let test = row_group_pruning_test().await;
    assert_snapshot!(test.query("SELECT * FROM t").await, @r"
    --- Query: SELECT * FROM t (9 rows) ---
    Request 1
        Read Parquet footer (offsets 0..1402)
    Request 2
        Read dictionary page and data page 0 for column 'id' (rg 0) (offsets 4..51)
        Read dictionary page and data page 0 for column 'val' (rg 0) (offsets 51..98)
    Produce batch (3 rows)
    Request 3
        Read dictionary page and data page 0 for column 'id' (rg 1) (offsets 98..145)
        Read dictionary page and data page 0 for column 'val' (rg 1) (offsets 145..192)
    Produce batch (3 rows)
    Request 4
        Read dictionary page and data page 0 for column 'id' (rg 2) (offsets 192..239)
        Read dictionary page and data page 0 for column 'val' (rg 2) (offsets 239..286)
    Produce batch (3 rows)
    ");
}

/// Only row group 1 (`id` in `4..=6`) can contain `id = 5`, so the data pages
/// for row groups 0 and 2 are never fetched.
#[tokio::test]
async fn test_row_group_pruning_io_one_row_group() {
    let test = row_group_pruning_test().await;
    assert_snapshot!(test.query("SELECT * FROM t WHERE id = 5").await, @r"
    --- Query: SELECT * FROM t WHERE id = 5 (1 rows) ---
    Request 1
        Read Parquet footer (offsets 0..1402)
    Request 2
        Read dictionary page and data page 0 for column 'id' (rg 1) (offsets 98..145)
        Read dictionary page and data page 0 for column 'val' (rg 1) (offsets 145..192)
    Produce batch (1 rows)
    ");
}

/// Row group 0 (`id` in `1..=3`) cannot contain `id > 5`, so only row groups 1
/// and 2 are fetched.
#[tokio::test]
async fn test_row_group_pruning_io_two_row_groups() {
    let test = row_group_pruning_test().await;
    assert_snapshot!(test.query("SELECT * FROM t WHERE id > 5").await, @r"
    --- Query: SELECT * FROM t WHERE id > 5 (4 rows) ---
    Request 1
        Read Parquet footer (offsets 0..1402)
    Request 2
        Read dictionary page and data page 0 for column 'id' (rg 1) (offsets 98..145)
        Read dictionary page and data page 0 for column 'val' (rg 1) (offsets 145..192)
    Request 3
        Read dictionary page and data page 0 for column 'id' (rg 2) (offsets 192..239)
        Read dictionary page and data page 0 for column 'val' (rg 2) (offsets 239..286)
    Produce batch (4 rows)
    ");
}

/// No row group can contain `id > 100`, so only the footer is read.
#[tokio::test]
async fn test_row_group_pruning_io_all_pruned() {
    let test = row_group_pruning_test().await;
    assert_snapshot!(test.query("SELECT * FROM t WHERE id > 100").await, @r"
    --- Query: SELECT * FROM t WHERE id > 100 (0 rows) ---
    Request 1
        Read Parquet footer (offsets 0..1402)
    ");
}

async fn row_group_pruning_test() -> ParquetIoTest {
    let config = SessionConfig::new()
        .with_target_partitions(1)
        .with_parquet_pruning(true);
    ParquetIoTest::new(three_row_group_file(), config).await
}

/// Returns the Parquet file described in the module docs: three row groups of
/// three rows each, with `id` sorted across row groups.
fn three_row_group_file() -> Bytes {
    let id: ArrayRef = Arc::new(Int32Array::from_iter_values(1..=9));
    let val: ArrayRef = Arc::new(Int32Array::from(vec![0, 5, 10, 0, 7, 10, 0, 5, 10]));
    let batch = RecordBatch::try_from_iter([("id", id), ("val", val)]).unwrap();

    let props = WriterProperties::builder()
        .set_max_row_group_row_count(Some(3))
        .build();

    let mut buffer = Vec::new();
    let mut writer =
        ArrowWriter::try_new(&mut buffer, batch.schema(), Some(props)).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    Bytes::from(buffer)
}
