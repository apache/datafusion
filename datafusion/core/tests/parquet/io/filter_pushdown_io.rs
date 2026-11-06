// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Tests tracking I/O events with and without Parquet filter pushdown
//! (`datafusion.execution.parquet.pushdown_filters`, see
//! [`ParquetOptions::pushdown_filters`]).
//!
//! Both tests run `val = 7`, which statistics cannot prune, but which only
//! matches a row in row group 1:
//!
//! ```text
//!   row group 0        row group 1        row group 2
//! ┌──────┬───────┐   ┌──────┬───────┐   ┌──────┬───────┐
//! │  id  │  val  │   │  id  │  val  │   │  id  │  val  │
//! ├──────┼───────┤   ├──────┼───────┤   ├──────┼───────┤
//! │   1  │   0   │   │   4  │   0   │   │   7  │   0   │
//! │   2  │   5   │   │   5  │   7 * │   │   8  │   5   │
//! │   3  │  10   │   │   6  │  10   │   │   9  │  10   │
//! └──────┴───────┘   └──────┴───────┘   └──────┴───────┘
//!  val: min 0 max 10  val: min 0 max 10  val: min 0 max 10
//!
//!  * the only row matching `val = 7`
//! ```
//!
//! [`ParquetOptions::pushdown_filters`]: datafusion::config::ParquetOptions::pushdown_filters

use std::sync::Arc;

use arrow::array::{ArrayRef, Int32Array, RecordBatch};
use bytes::Bytes;
use datafusion::prelude::SessionConfig;
use insta::assert_snapshot;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;

use super::ParquetIoTest;

/// Without filter pushdown, the scan fetches every column of every row group.
#[tokio::test]
async fn test_filter_pushdown_io_disabled() {
    let test = filter_pushdown_test(false).await;
    // Note: in practice, the default 512 KiB metadata size hint often covers
    // the OffsetIndex and Bloom filters for small files, so they wouldn't
    // need separate requests.
    let traces = test.capture_traces();
    test.query("SELECT * FROM t WHERE val = 7").await.unwrap();
    assert_snapshot!(traces.render(), @"
    Query (sql=SELECT * FROM t WHERE val = 7, rows=1)
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
            Row groups remaining (row_groups=3, row_groups_remaining=3, pruning_predicate=true)
        Load page index
        Load Bloom filters
        Prune row groups with Bloom filters
            Row groups remaining (row_groups_remaining=3)
        Prune pages with page index
            Row groups remaining (row_groups_remaining=3)
        Fetch column chunk data (ranges=2)
            ObjectStore::get_ranges (location=table.parquet, ranges=2)
                Read byte range (range=4..51, parquet=dictionary page and data page 0 for column 'id' (rg 0))
                Read byte range (range=51..98, parquet=dictionary page and data page 0 for column 'val' (rg 0))
        Fetch column chunk data (ranges=2)
            ObjectStore::get_ranges (location=table.parquet, ranges=2)
                Read byte range (range=98..145, parquet=dictionary page and data page 0 for column 'id' (rg 1))
                Read byte range (range=145..192, parquet=dictionary page and data page 0 for column 'val' (rg 1))
        Fetch column chunk data (ranges=2)
            ObjectStore::get_ranges (location=table.parquet, ranges=2)
                Read byte range (range=192..239, parquet=dictionary page and data page 0 for column 'id' (rg 2))
                Read byte range (range=239..286, parquet=dictionary page and data page 0 for column 'val' (rg 2))
        Produce batch (rows=1)
    ");
}

/// **With** filter pushdown, the scan first fetches and evaluates the filter
/// column (`val`) for each row group, and then fetches the remaining columns
/// (`id`) only for the row groups with matching rows (row group 1).
#[tokio::test]
async fn test_filter_pushdown_io_enabled() {
    let test = filter_pushdown_test(true).await;
    let traces = test.capture_traces();
    test.query("SELECT * FROM t WHERE val = 7").await.unwrap();
    assert_snapshot!(traces.render(), @"
    Query (sql=SELECT * FROM t WHERE val = 7, rows=1)
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
            Row groups remaining (row_groups=3, row_groups_remaining=3, pruning_predicate=true)
        Load page index
        Load Bloom filters
        Prune row groups with Bloom filters
            Row groups remaining (row_groups_remaining=3)
        Prune pages with page index
            Row groups remaining (row_groups_remaining=3)
        Fetch column chunk data (ranges=1)
            ObjectStore::get_ranges (location=table.parquet, ranges=1)
                Read byte range (range=51..98, parquet=dictionary page and data page 0 for column 'val' (rg 0))
        Evaluate row filter (predicate=val@0 = 7, rows=3, rows_matched=0)
        Fetch column chunk data (ranges=1)
            ObjectStore::get_ranges (location=table.parquet, ranges=1)
                Read byte range (range=145..192, parquet=dictionary page and data page 0 for column 'val' (rg 1))
        Evaluate row filter (predicate=val@0 = 7, rows=3, rows_matched=1)
        Fetch column chunk data (ranges=2)
            ObjectStore::get_ranges (location=table.parquet, ranges=2)
                Read byte range (range=98..124, parquet=dictionary page for column 'id' (rg 1))
                Read byte range (range=124..145, parquet=data page 0 for column 'id' (rg 1))
        Produce batch (rows=1)
        Fetch column chunk data (ranges=1)
            ObjectStore::get_ranges (location=table.parquet, ranges=1)
                Read byte range (range=239..286, parquet=dictionary page and data page 0 for column 'val' (rg 2))
        Evaluate row filter (predicate=val@0 = 7, rows=3, rows_matched=0)
    ");
}

/// Returns a [`ParquetIoTest`] for the file described in the module docs
async fn filter_pushdown_test(pushdown_filters: bool) -> ParquetIoTest {
    let mut config = SessionConfig::new().with_target_partitions(1);
    config.options_mut().execution.parquet.pushdown_filters = pushdown_filters;
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
