// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Tests tracking storage I/O events during Parquet Bloom filter evaluation.

use std::sync::Arc;

use arrow::array::{ArrayRef, Int32Array, RecordBatch};
use bytes::Bytes;
use datafusion::prelude::SessionConfig;
use insta::assert_snapshot;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::{EnabledStatistics, WriterProperties};

use super::ParquetIoTest;

/// Matching row group
#[tokio::test]
async fn test_parquet_bloom_filter_io_matching() {
    let test = bloom_filter_test().await;
    // Loads the Bloom filters, finds a match, then reads the data pages and
    // produces a batch.
    assert_snapshot!(test.query("SELECT * FROM t WHERE a = 10 AND b = 100").await, @r"
    --- Query: SELECT * FROM t WHERE a = 10 AND b = 100 (1 rows) ---
    Read Parquet footer (offsets 0..678)
    Read OffsetIndex for column 'a' (rg 0) (offsets 192..213)
    Read Bloom filter for column 'a' (rg 0) (offsets 98..145)
    Read Bloom filter for column 'b' (rg 0) (offsets 145..192)
    Read data page for column 'a' (rg 0) (offsets 4..51)
    Read data page for column 'b' (rg 0) (offsets 51..98)
    Produce batch (1 rows)
    ");
}

/// Column `a` does not match, so the row group is pruned.
#[tokio::test]
async fn test_parquet_bloom_filter_io_pruned() {
    let test = bloom_filter_test().await;
    // Currently the Bloom filters for all candidate columns (`a` and `b`) are
    // loaded upfront. With incremental pruning
    // (https://github.com/apache/datafusion/issues/25811), `a` would be
    // evaluated first and the Bloom filter for `b` would not be loaded.
    assert_snapshot!(test.query("SELECT * FROM t WHERE a = 999 AND b = 100").await, @r"
    --- Query: SELECT * FROM t WHERE a = 999 AND b = 100 (0 rows) ---
    Read Parquet footer (offsets 0..678)
    Read OffsetIndex for column 'a' (rg 0) (offsets 192..213)
    Read Bloom filter for column 'a' (rg 0) (offsets 98..145)
    Read Bloom filter for column 'b' (rg 0) (offsets 145..192)
    ");
}

/// Returns a [`ParquetIoTest`] for a file with one row group of 3 rows:
///
/// | a  | b   |
/// |----|-----|
/// | 10 | 100 |
/// | 20 | 200 |
/// | 30 | 300 |
///
/// Both columns have Bloom filters. Statistics are disabled so that row group
/// pruning relies entirely on the Bloom filters.
async fn bloom_filter_test() -> ParquetIoTest {
    let a: ArrayRef = Arc::new(Int32Array::from(vec![10, 20, 30]));
    let b: ArrayRef = Arc::new(Int32Array::from(vec![100, 200, 300]));
    let batch = RecordBatch::try_from_iter([("a", a), ("b", b)]).unwrap();

    let props = WriterProperties::builder()
        .set_max_row_group_row_count(Some(3))
        .set_bloom_filter_enabled(true)
        .set_statistics_enabled(EnabledStatistics::None)
        .build();

    let mut buffer = Vec::new();
    let mut writer =
        ArrowWriter::try_new(&mut buffer, batch.schema(), Some(props)).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();

    let config = SessionConfig::new()
        .with_target_partitions(1)
        .with_parquet_bloom_filter_pruning(true);

    ParquetIoTest::new(Bytes::from(buffer), config).await
}
