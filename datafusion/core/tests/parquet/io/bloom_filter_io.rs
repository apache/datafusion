// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Tests tracking storage I/O events during Parquet Bloom filter evaluation.
//!
//! The file has a single row group with Bloom filters on both columns and no
//! statistics, so only the Bloom filters are used to prune.
//!
//! ```text
//!   row group 0
//! ┌──────┬───────┐
//! │  a   │  b    │
//! ├──────┼───────┤
//! │  10  │  100  │
//! │  20  │  200  │
//! │  30  │  300  │
//! └──────┴───────┘
//!  Bloom filter (a)   Bloom filter (b)   no min/max statistics
//!
//!  a = 10  AND b = 100  -> Bloom filters match, row group read
//!  a = 999 AND b = 100  -> Bloom filter on `a` misses, row group pruned
//! ```

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
    //
    // Note: in practice, the default 512 KiB metadata size hint often covers
    // the OffsetIndex and Bloom filters for small files, so they wouldn't
    // need separate requests.
    assert_snapshot!(test.query("SELECT * FROM t WHERE a = 10 AND b = 100").await, @r"
    --- Query: SELECT * FROM t WHERE a = 10 AND b = 100 (1 rows) ---
    Request 1
        Head (object size and metadata)
    Request 2
        Read Parquet footer (offsets 213..678)
    Request 3
        Read OffsetIndex for columns 'a', 'b' (rg 0) (offsets 192..213)
    Request 4
        Read Bloom filter for column 'a' (rg 0) (offsets 98..145)
    Request 5
        Read Bloom filter for column 'b' (rg 0) (offsets 145..192)
    Request 6
        Read dictionary page and data page 0 for column 'a' (rg 0) (offsets 4..51)
        Read dictionary page and data page 0 for column 'b' (rg 0) (offsets 51..98)
    Produce batch (1 rows)
    ");
}

/// Column `a` does not match, so the row group is pruned.
#[tokio::test]
async fn test_parquet_bloom_filter_io_pruned() {
    let test = bloom_filter_test().await;
    // Column `a` does not match, so the row group is pruned immediately
    // and the Bloom filter for column `b` is not loaded.
    assert_snapshot!(test.query("SELECT * FROM t WHERE a = 999 AND b = 100").await, @r"
    --- Query: SELECT * FROM t WHERE a = 999 AND b = 100 (0 rows) ---
    Request 1
        Head (object size and metadata)
    Request 2
        Read Parquet footer (offsets 213..678)
    Request 3
        Read OffsetIndex for columns 'a', 'b' (rg 0) (offsets 192..213)
    Request 4
        Read Bloom filter for column 'a' (rg 0) (offsets 98..145)
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
