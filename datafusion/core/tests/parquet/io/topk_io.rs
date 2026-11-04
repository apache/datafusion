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

use datafusion::prelude::SessionConfig;
use insta::assert_snapshot;

use super::{ParquetIoTest, three_row_group_file};

/// TopK dynamic filter pushdown prunes row groups during the scan.
///
/// Sort pushdown reorders the row groups using their statistics so row group 2
/// (largest `id` values) is read first. After it is read, the TopK filter  is
/// `id > 8` which  proves that row groups 1 and 0 cannot contain better rows,
/// so they are pruned without being fetched.
#[tokio::test]
async fn test_topk_io_dynamic_filter() {
    let test = topk_test(true).await;
    assert_snapshot!(test.query("SELECT * FROM t ORDER BY id DESC LIMIT 2").await, @r"
    --- Query: SELECT * FROM t ORDER BY id DESC LIMIT 2 (2 rows) ---
    Read Parquet footer (offsets 0..1402)
    Read data page for column 'id' (rg 2) (offsets 192..239)
    Read data page for column 'val' (rg 2) (offsets 239..286)
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
    Read Parquet footer (offsets 0..1402)
    Read data page for column 'id' (rg 2) (offsets 192..239)
    Read data page for column 'val' (rg 2) (offsets 239..286)
    Read data page for column 'id' (rg 1) (offsets 98..145)
    Read data page for column 'val' (rg 1) (offsets 145..192)
    Read data page for column 'id' (rg 0) (offsets 4..51)
    Read data page for column 'val' (rg 0) (offsets 51..98)
    Produce batch (2 rows)
    ");
}

async fn topk_test(dynamic_filter_pushdown: bool) -> ParquetIoTest {
    let mut config = SessionConfig::new().with_target_partitions(1);
    config
        .options_mut()
        .optimizer
        .enable_topk_dynamic_filter_pushdown = dynamic_filter_pushdown;
    ParquetIoTest::new(three_row_group_file(), config).await
}
