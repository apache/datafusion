// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Tests tracking storage I/O events with and without Parquet filter pushdown
//! (`datafusion.execution.parquet.pushdown_filters`).
//!
//! Both tests run `val = 7`, which statistics cannot prune (every row group
//! has `val` in `0..=10`), but which only matches a row in row group 1.

use datafusion::prelude::SessionConfig;
use insta::assert_snapshot;

use super::{ParquetIoTest, three_row_group_file};

/// Without filter pushdown, the scan fetches every column of every row group
/// and the filter is applied after decoding.
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

/// With filter pushdown, the scan first fetches and evaluates the filter
/// column (`val`) for each row group, and then fetches the remaining columns
/// (`id`) only for the row groups with matching rows (row group 1).
///
/// Note that `id` for row group 1 is fetched as two ranges, its dictionary
/// page and its data page, rather than as a single column chunk read.
#[tokio::test]
async fn test_filter_pushdown_io_enabled() {
    let test = filter_pushdown_test(true).await;
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
