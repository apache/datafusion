// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! End-to-end tests for `datafusion.execution.target_batch_size_bytes`:
//! byte-aware re-chunking of data source output batches

use std::sync::Arc;

use arrow::array::{ArrayRef, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::datasource::MemTable;
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_common::Result;
use datafusion_common::utils::memory::get_record_batch_memory_size;

fn wide_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new("s", DataType::Utf8, false)]))
}

/// Batch with `rows` rows of `str_len`-byte strings
fn wide_batch(rows: usize, str_len: usize) -> RecordBatch {
    let s = StringArray::from_iter_values((0..rows).map(|i| format!("{i:0>str_len$}")));
    RecordBatch::try_new(wide_schema(), vec![Arc::new(s) as ArrayRef]).unwrap()
}

async fn run_scan(
    config: SessionConfig,
    batches: Vec<RecordBatch>,
) -> Result<Vec<RecordBatch>> {
    let ctx = SessionContext::new_with_config(config.with_target_partitions(1));
    let table = MemTable::try_new(wide_schema(), vec![batches])?;
    ctx.register_table("t", Arc::new(table))?;
    ctx.sql("SELECT * FROM t").await?.collect().await
}

#[tokio::test]
async fn target_batch_size_bytes_splits_wide_batches() -> Result<()> {
    // One ~1MB batch of 100 wide rows; a 64KiB byte target must split it
    let input = wide_batch(100, 10 * 1024);
    let target_bytes = 64 * 1024;

    let mut config = SessionConfig::new();
    config.options_mut().execution.target_batch_size_bytes = Some(target_bytes);

    let results = run_scan(config, vec![input.clone()]).await?;
    assert!(
        results.len() > 1,
        "expected wide batch to be split, got {} batch(es)",
        results.len()
    );
    assert_eq!(results.iter().map(|b| b.num_rows()).sum::<usize>(), 100);
    for batch in &results {
        // each chunk must be a compact copy of roughly the target size, not
        // a zero-copy slice pinning the ~1MB parent
        assert!(
            get_record_batch_memory_size(batch) <= 2 * target_bytes,
            "output batch retains {} bytes, expected <= {}",
            get_record_batch_memory_size(batch),
            2 * target_bytes
        );
    }
    Ok(())
}

#[tokio::test]
async fn wide_batches_not_split_by_default() -> Result<()> {
    // Without a byte target, splitting is row-based only: a single wide
    // batch within `batch_size` rows passes through whole
    let input = wide_batch(100, 10 * 1024);
    let results = run_scan(SessionConfig::new(), vec![input]).await?;
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].num_rows(), 100);
    Ok(())
}

#[tokio::test]
async fn target_batch_size_bytes_coalesces_small_batches() -> Result<()> {
    // 50 batches of 10 tiny rows each: the normalizer coalesces them
    let batches: Vec<_> = (0..50).map(|_| wide_batch(10, 8)).collect();

    let mut config = SessionConfig::new();
    config.options_mut().execution.target_batch_size_bytes = Some(1024 * 1024);

    let results = run_scan(config, batches).await?;
    assert_eq!(results.iter().map(|b| b.num_rows()).sum::<usize>(), 500);
    assert_eq!(
        results.len(),
        1,
        "expected small batches to be coalesced, got {} batches",
        results.len()
    );
    Ok(())
}
