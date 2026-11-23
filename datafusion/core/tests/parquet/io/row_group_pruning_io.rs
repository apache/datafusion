// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Tests tracking I/O events during Parquet row group pruning.
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
    // Note: in practice, the default 512 KiB metadata size hint would fetch
    // the page index of a small file like this along with the footer.
    let traces = test.capture_traces();
    test.query("SELECT * FROM t").await.unwrap();
    assert_snapshot!(traces.render(), @"
    Query (sql=SELECT * FROM t, rows=9)
        ObjectStore::head (location=table.parquet)
        Metadata cache miss (cache_enabled=true)
        Prefetch Parquet footer (range=489..1402)
        ObjectStore::get_ranges (location=table.parquet, ranges=1)
            Read byte range (range=489..1402, parquet=footer)
        Read page index (range=286..489)
        ObjectStore::get_ranges (location=table.parquet, ranges=1)
            Read byte range (range=286..489, parquet=ColumnIndex and OffsetIndex for columns 'id', 'val' (rg 0, 1, 2))
        Open Parquet file (file=table.parquet, size=1402)
        Load Parquet metadata
            Metadata cache hit (has_page_index=true)
        Prune row groups
            Row groups remaining (row_groups=3, row_groups_remaining=3, pruning_predicate=false)
        Load Bloom filters
        Prune row groups with Bloom filters
        Fetch column chunk data (ranges=2)
            ObjectStore::get_ranges (location=table.parquet, ranges=2)
                Read byte range (range=4..51, parquet=dictionary page and data page 0 for column 'id' (rg 0))
                Read byte range (range=51..98, parquet=dictionary page and data page 0 for column 'val' (rg 0))
        Produce batch (rows=3)
        Fetch column chunk data (ranges=2)
            ObjectStore::get_ranges (location=table.parquet, ranges=2)
                Read byte range (range=98..145, parquet=dictionary page and data page 0 for column 'id' (rg 1))
                Read byte range (range=145..192, parquet=dictionary page and data page 0 for column 'val' (rg 1))
        Produce batch (rows=3)
        Fetch column chunk data (ranges=2)
            ObjectStore::get_ranges (location=table.parquet, ranges=2)
                Read byte range (range=192..239, parquet=dictionary page and data page 0 for column 'id' (rg 2))
                Read byte range (range=239..286, parquet=dictionary page and data page 0 for column 'val' (rg 2))
        Produce batch (rows=3)
    ");
}

/// Only row group 1 (`id` in `4..=6`) can contain `id = 5`, so the data pages
/// for row groups 0 and 2 are never fetched.
#[tokio::test]
async fn test_row_group_pruning_io_one_row_group() {
    let test = row_group_pruning_test().await;
    let traces = test.capture_traces();
    test.query("SELECT * FROM t WHERE id = 5").await.unwrap();
    assert_snapshot!(traces.render(), @"
    Query (sql=SELECT * FROM t WHERE id = 5, rows=1)
        ObjectStore::head (location=table.parquet)
        Metadata cache miss (cache_enabled=true)
        Prefetch Parquet footer (range=489..1402)
        ObjectStore::get_ranges (location=table.parquet, ranges=1)
            Read byte range (range=489..1402, parquet=footer)
        Read page index (range=286..489)
        ObjectStore::get_ranges (location=table.parquet, ranges=1)
            Read byte range (range=286..489, parquet=ColumnIndex and OffsetIndex for columns 'id', 'val' (rg 0, 1, 2))
        Open Parquet file (file=table.parquet, size=1402)
        Load Parquet metadata
            Metadata cache hit (has_page_index=true)
        Prune row groups
            Row groups remaining (row_groups=3, row_groups_remaining=1, pruning_predicate=true)
        Load page index
        Load Bloom filters
        Prune row groups with Bloom filters
            Row groups remaining (row_groups_remaining=1)
        Prune pages with page index
            Row groups remaining (row_groups_remaining=1)
        Fetch column chunk data (ranges=2)
            ObjectStore::get_ranges (location=table.parquet, ranges=2)
                Read byte range (range=98..145, parquet=dictionary page and data page 0 for column 'id' (rg 1))
                Read byte range (range=145..192, parquet=dictionary page and data page 0 for column 'val' (rg 1))
        Produce batch (rows=1)
    ");
}

/// Row group 0 (`id` in `1..=3`) cannot contain `id > 5`, so only row groups 1
/// and 2 are fetched.
#[tokio::test]
async fn test_row_group_pruning_io_two_row_groups() {
    let test = row_group_pruning_test().await;
    let traces = test.capture_traces();
    test.query("SELECT * FROM t WHERE id > 5").await.unwrap();
    assert_snapshot!(traces.render(), @"
    Query (sql=SELECT * FROM t WHERE id > 5, rows=4)
        ObjectStore::head (location=table.parquet)
        Metadata cache miss (cache_enabled=true)
        Prefetch Parquet footer (range=489..1402)
        ObjectStore::get_ranges (location=table.parquet, ranges=1)
            Read byte range (range=489..1402, parquet=footer)
        Read page index (range=286..489)
        ObjectStore::get_ranges (location=table.parquet, ranges=1)
            Read byte range (range=286..489, parquet=ColumnIndex and OffsetIndex for columns 'id', 'val' (rg 0, 1, 2))
        Open Parquet file (file=table.parquet, size=1402)
        Load Parquet metadata
            Metadata cache hit (has_page_index=true)
        Prune row groups
            Row groups remaining (row_groups=3, row_groups_remaining=2, pruning_predicate=true)
        Load page index
        Load Bloom filters
        Prune row groups with Bloom filters
            Row groups remaining (row_groups_remaining=2)
        Prune pages with page index
            Row groups remaining (row_groups_remaining=2)
        Fetch column chunk data (ranges=2)
            ObjectStore::get_ranges (location=table.parquet, ranges=2)
                Read byte range (range=98..145, parquet=dictionary page and data page 0 for column 'id' (rg 1))
                Read byte range (range=145..192, parquet=dictionary page and data page 0 for column 'val' (rg 1))
        Fetch column chunk data (ranges=2)
            ObjectStore::get_ranges (location=table.parquet, ranges=2)
                Read byte range (range=192..239, parquet=dictionary page and data page 0 for column 'id' (rg 2))
                Read byte range (range=239..286, parquet=dictionary page and data page 0 for column 'val' (rg 2))
        Produce batch (rows=4)
    ");
}

/// No row group can contain `id > 100`, so only the footer and page index
/// are read.
#[tokio::test]
async fn test_row_group_pruning_io_all_pruned() {
    let test = row_group_pruning_test().await;
    let traces = test.capture_traces();
    test.query("SELECT * FROM t WHERE id > 100").await.unwrap();
    assert_snapshot!(traces.render(), @"
    Query (sql=SELECT * FROM t WHERE id > 100, rows=0)
        ObjectStore::head (location=table.parquet)
        Metadata cache miss (cache_enabled=true)
        Prefetch Parquet footer (range=489..1402)
        ObjectStore::get_ranges (location=table.parquet, ranges=1)
            Read byte range (range=489..1402, parquet=footer)
        Read page index (range=286..489)
        ObjectStore::get_ranges (location=table.parquet, ranges=1)
            Read byte range (range=286..489, parquet=ColumnIndex and OffsetIndex for columns 'id', 'val' (rg 0, 1, 2))
        Open Parquet file (file=table.parquet, size=1402)
    ");
}

/// Returns a [`ParquetIoTest`] for the file described in the module docs
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
