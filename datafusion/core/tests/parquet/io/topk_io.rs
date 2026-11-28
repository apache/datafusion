// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Tests tracking I/O events for TopK (`ORDER BY .. LIMIT`) queries.
//!
//! Both tests run `ORDER BY id DESC LIMIT 2`. `id` is sorted across row
//! groups, so sort pushdown reads the row groups in reverse order (largest
//! `id` first) and the two matching rows are both in row group 2:
//!
//! ```text
//!   row group 0        row group 1        row group 2
//! ┌──────┬───────┐   ┌──────┬───────┐   ┌──────┬───────┐
//! │  id  │  val  │   │  id  │  val  │   │  id  │  val  │
//! ├──────┼───────┤   ├──────┼───────┤   ├──────┼───────┤
//! │   1  │   0   │   │   4  │   0   │   │   7  │   0   │
//! │   2  │   5   │   │   5  │   7   │   │   8  │   5 * │
//! │   3  │  10   │   │   6  │  10   │   │   9  │  10 * │
//! └──────┴───────┘   └──────┴───────┘   └──────┴───────┘
//!  id: min 1 max 3    id: min 4 max 6    id: min 7 max 9
//!
//!  read order: 3rd    read order: 2nd    read order: 1st
//!
//!  * the two rows returned by `ORDER BY id DESC LIMIT 2`
//! ```

use std::sync::Arc;

use arrow::array::{ArrayRef, Int32Array, RecordBatch};
use bytes::Bytes;
use datafusion::prelude::SessionConfig;
use insta::assert_snapshot;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;

use super::ParquetIoTest;

/// TopK dynamic filter pushdown prunes row groups during the scan.
///
/// Sort pushdown reorders the row groups using their statistics so row group 2
/// (largest `id` values) is read first. After it is read, the TopK filter is
/// `id > 8` which proves that row groups 1 and 0 cannot contain better rows,
/// so they are pruned without being fetched.
#[tokio::test]
async fn test_topk_io_dynamic_filter() {
    let test = topk_test(true).await;
    // Note: in practice, the default 512 KiB metadata size hint would fetch
    // the page index of a small file like this along with the footer.
    assert_snapshot!(test.query("SELECT * FROM t ORDER BY id DESC LIMIT 2").await, @r"
    --- Query: SELECT * FROM t ORDER BY id DESC LIMIT 2 (2 rows) ---
    Request 1
        Head (object size and metadata)
    Request 2
        Read Parquet footer (offsets 489..1396)
    Request 3
        Read ColumnIndex and OffsetIndex for columns 'id', 'val' (rg 0, 1, 2) (offsets 286..489)
    Request 4
        Read dictionary page and data page 0 for column 'id' (rg 2) (offsets 192..239)
        Read dictionary page and data page 0 for column 'val' (rg 2) (offsets 239..286)
    Produce batch (2 rows)
    ");
}

/// Without TopK dynamic filter pushdown, the row groups are still read in the
/// optimal order, but all of them are fetched.
#[tokio::test]
async fn test_topk_io_no_dynamic_filter() {
    let test = topk_test(false).await;
    assert_snapshot!(test.query("SELECT * FROM t ORDER BY id DESC LIMIT 2").await, @r"
    --- Query: SELECT * FROM t ORDER BY id DESC LIMIT 2 (2 rows) ---
    Request 1
        Head (object size and metadata)
    Request 2
        Read Parquet footer (offsets 489..1396)
    Request 3
        Read ColumnIndex and OffsetIndex for columns 'id', 'val' (rg 0, 1, 2) (offsets 286..489)
    Request 4
        Read dictionary page and data page 0 for column 'id' (rg 2) (offsets 192..239)
        Read dictionary page and data page 0 for column 'val' (rg 2) (offsets 239..286)
    Request 5
        Read dictionary page and data page 0 for column 'id' (rg 1) (offsets 98..145)
        Read dictionary page and data page 0 for column 'val' (rg 1) (offsets 145..192)
    Request 6
        Read dictionary page and data page 0 for column 'id' (rg 0) (offsets 4..51)
        Read dictionary page and data page 0 for column 'val' (rg 0) (offsets 51..98)
    Produce batch (2 rows)
    ");
}

/// Returns a [`ParquetIoTest`] for the file described in the module docs
async fn topk_test(dynamic_filter_pushdown: bool) -> ParquetIoTest {
    let mut config = SessionConfig::new().with_target_partitions(1);
    config
        .options_mut()
        .optimizer
        .enable_topk_dynamic_filter_pushdown = dynamic_filter_pushdown;
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

/// TopK dynamic filter pushdown when the page index has also pruned pages
/// within the row groups.
///
/// `w = 1` only matches page 1 of each row group, so the page index creates a
/// row selection for every row group:
///
/// ```text
///        row group 0        row group 1        row group 2
///      ┌──────┬───────┐   ┌──────┬───────┐   ┌──────┬───────┐
///      │  id  │   w   │   │  id  │   w   │   │  id  │   w   │
///      ├──────┼───────┤   ├──────┼───────┤   ├──────┼───────┤
/// page │ 1..3 │   0   │   │ 7..9 │   0   │   │13..15│   0   │
///  0   ├──────┼───────┤   ├──────┼───────┤   ├──────┼───────┤
/// page │ 4..6 │   1   │   │10..12│   1   │   │16..18│   1   │
///  1   └──────┴───────┘   └──────┴───────┘   └──────┴───────┘
/// ```
///
/// Row group 2 is read first (sort pushdown). The TopK filter is then
/// `id > 17`, which proves row groups 1 and 0 cannot contain better rows.
/// However, runtime row group pruning is disabled when a row selection is
/// present, so they are fetched anyway. See
/// <https://github.com/apache/datafusion/issues/24358>.
#[tokio::test]
async fn test_topk_io_dynamic_filter_with_page_pruning() {
    let mut config = SessionConfig::new().with_target_partitions(1);
    config.options_mut().execution.parquet.pushdown_filters = true;
    let test = ParquetIoTest::new(three_row_group_two_page_file(), config).await;
    assert_snapshot!(test.query("SELECT id FROM t WHERE w = 1 ORDER BY id DESC LIMIT 2").await, @r"
    --- Query: SELECT id FROM t WHERE w = 1 ORDER BY id DESC LIMIT 2 (2 rows) ---
    Request 1
        Head (object size and metadata)
    Request 2
        Read Parquet footer (offsets 673..1507)
    Request 3
        Read ColumnIndex and OffsetIndex for columns 'id', 'w' (rg 0, 1, 2) (offsets 352..673)
    Request 4
        Read data page 1 for column 'w' (rg 2) (offsets 323..352)
    Request 5
        Read data page 0 for column 'id' (rg 2) (offsets 236..265)
        Read data page 1 for column 'id' (rg 2) (offsets 265..294)
    Produce batch (2 rows)
    ");
}

/// Returns a Parquet file with three row groups of six rows, where each
/// column chunk is written as two data pages of three rows each.
fn three_row_group_two_page_file() -> Bytes {
    let id: ArrayRef = Arc::new(Int32Array::from_iter_values(1..=18));
    let w: ArrayRef = Arc::new(Int32Array::from_iter_values(
        [0, 0, 0, 1, 1, 1].into_iter().cycle().take(18),
    ));
    let batch = RecordBatch::try_from_iter([("id", id), ("w", w)]).unwrap();

    let props = WriterProperties::builder()
        .set_dictionary_enabled(false)
        .set_max_row_group_row_count(Some(6))
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
