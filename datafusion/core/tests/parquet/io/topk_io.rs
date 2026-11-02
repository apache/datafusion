// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Tests tracking storage I/O events for TopK (`ORDER BY .. LIMIT`) queries.

use datafusion::prelude::SessionConfig;
use insta::assert_snapshot;

use super::{ParquetIoTest, three_row_group_file};

/// TopK dynamic filter pushdown prunes row groups during the scan.
///
/// Sort pushdown reorders the row groups using their statistics so row group
/// 2 (largest `id` values) is read first. After it is read, the TopK heap holds
/// `id` 9 and 8, and its dynamic filter (`id > 8`) proves that row groups 1
/// and 0 cannot contain better rows, so they are pruned without being fetched.
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
