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

use datafusion::prelude::SessionConfig;
use insta::assert_snapshot;

use super::{ParquetIoTest, three_row_group_file};

/// Without filter pushdown, the scan fetches every column of every row group
#[tokio::test]
async fn test_filter_pushdown_io_disabled() {
    let test = filter_pushdown_test(false).await;
    assert_snapshot!(test.query("SELECT * FROM t WHERE val = 7").await, @r"
    --- Query: SELECT * FROM t WHERE val = 7 (1 rows) ---
    Read Parquet footer (offsets 0..1402)
    Read data page for column 'id' (rg 0) (offsets 4..51)
    Read data page for column 'val' (rg 0) (offsets 51..98)
    Read data page for column 'id' (rg 1) (offsets 98..145)
    Read data page for column 'val' (rg 1) (offsets 145..192)
    Read data page for column 'id' (rg 2) (offsets 192..239)
    Read data page for column 'val' (rg 2) (offsets 239..286)
    Produce batch (1 rows)
    ");
}

/// **With** filter pushdown, the scan first fetches and evaluates the filter
/// column (`val`) for each row group, and then fetches the remaining columns
/// (`id`) only for the row groups with matching rows (row group 1).
#[tokio::test]
async fn test_filter_pushdown_io_enabled() {
    let test = filter_pushdown_test(true).await;
    // Note: `id` for row group 1 is fetched as two ranges, its dictionary
    // page and its data page, rather than as a single column chunk read.
    assert_snapshot!(test.query("SELECT * FROM t WHERE val = 7").await, @r"
    --- Query: SELECT * FROM t WHERE val = 7 (1 rows) ---
    Read Parquet footer (offsets 0..1402)
    Read data page for column 'val' (rg 0) (offsets 51..98)
    Read data page for column 'val' (rg 1) (offsets 145..192)
    Read data page for column 'id' (rg 1) (offsets 98..124)
    Read data page for column 'id' (rg 1) (offsets 124..145)
    Produce batch (1 rows)
    Read data page for column 'val' (rg 2) (offsets 239..286)
    ");
}

async fn filter_pushdown_test(pushdown_filters: bool) -> ParquetIoTest {
    let mut config = SessionConfig::new().with_target_partitions(1);
    config.options_mut().execution.parquet.pushdown_filters = pushdown_filters;
    ParquetIoTest::new(three_row_group_file(), config).await
}
