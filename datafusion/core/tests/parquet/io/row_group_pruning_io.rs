// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Tests tracking storage I/O events during Parquet row group pruning based
//! on row group statistics (min/max values).

use datafusion::prelude::SessionConfig;
use insta::assert_snapshot;

use super::{ParquetIoTest, three_row_group_file};

/// No predicate, so every row group is read.
#[tokio::test]
async fn test_row_group_pruning_io_no_predicate() {
    let test = row_group_pruning_test().await;
    assert_snapshot!(test.query("SELECT * FROM t").await, @r"
    --- Query: SELECT * FROM t (9 rows) ---
    Read Parquet footer (offsets 0..1402)
    Read data page for column 'id' (rg 0) (offsets 4..51)
    Read data page for column 'val' (rg 0) (offsets 51..98)
    Produce batch (3 rows)
    Read data page for column 'id' (rg 1) (offsets 98..145)
    Read data page for column 'val' (rg 1) (offsets 145..192)
    Produce batch (3 rows)
    Read data page for column 'id' (rg 2) (offsets 192..239)
    Read data page for column 'val' (rg 2) (offsets 239..286)
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
    Read Parquet footer (offsets 0..1402)
    Read data page for column 'id' (rg 1) (offsets 98..145)
    Read data page for column 'val' (rg 1) (offsets 145..192)
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
    Read Parquet footer (offsets 0..1402)
    Read data page for column 'id' (rg 1) (offsets 98..145)
    Read data page for column 'val' (rg 1) (offsets 145..192)
    Read data page for column 'id' (rg 2) (offsets 192..239)
    Read data page for column 'val' (rg 2) (offsets 239..286)
    Produce batch (4 rows)
    ");
}

/// No row group can contain `id > 100`, so only the footer is read.
#[tokio::test]
async fn test_row_group_pruning_io_all_pruned() {
    let test = row_group_pruning_test().await;
    assert_snapshot!(test.query("SELECT * FROM t WHERE id > 100").await, @r"
    --- Query: SELECT * FROM t WHERE id > 100 (0 rows) ---
    Read Parquet footer (offsets 0..1402)
    ");
}

async fn row_group_pruning_test() -> ParquetIoTest {
    let config = SessionConfig::new()
        .with_target_partitions(1)
        .with_parquet_pruning(true);
    ParquetIoTest::new(three_row_group_file(), config).await
}
