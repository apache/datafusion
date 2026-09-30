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

//! An external sort over wide rows must finish under a memory limit that holds a few rows,
//! even though `batch_size` rows do not fit.
//!
//! The input arrives in small batches, so every input batch fits. Before the fix, the sort
//! wrote its spill files in batches of up to `batch_size` rows. With wide rows, one spilled
//! batch was larger than the memory pool, and the final merge of the spill files, which
//! reserves memory for the largest batch of each file, failed with `ResourcesExhausted`.

use std::sync::Arc;

use arrow::array::{
    ArrayRef, Int64Array, ListBuilder, RecordBatch, StringArray, StringBuilder,
    StringViewArray,
};
use arrow_schema::{DataType, Field, Schema};
use datafusion::datasource::MemTable;
use datafusion::execution::memory_pool::FairSpillPool;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};

/// 64 KiB per row.
const ROW_BYTES: usize = 64 * 1024;
/// 32 MiB of rows in total.
const ROWS: usize = 512;
/// Rows per input batch: 256 KiB, which fits in the pool.
const INPUT_BATCH_ROWS: usize = 4;
/// Holds about 190 rows, less than half of `ROWS`, so the sort must spill.
const POOL_BYTES: usize = 12 * 1024 * 1024;

fn payload(i: usize) -> String {
    format!("{i:08}{}", "x".repeat(ROW_BYTES))
}

async fn sort_wide_rows(data_type: DataType) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("payload", data_type.clone(), false),
    ]));
    let batches: Vec<RecordBatch> = (0..ROWS)
        .step_by(INPUT_BATCH_ROWS)
        .map(|start| {
            let rows = start..start + INPUT_BATCH_ROWS;
            let ids = Int64Array::from_iter_values(rows.clone().map(|i| i as i64));
            let payload: ArrayRef = match data_type {
                DataType::Utf8 => {
                    Arc::new(StringArray::from_iter_values(rows.map(payload)))
                }
                DataType::Utf8View => {
                    Arc::new(StringViewArray::from_iter_values(rows.map(payload)))
                }
                DataType::List(_) => {
                    // Four elements of a quarter row each.
                    let mut builder = ListBuilder::new(StringBuilder::new());
                    for i in rows {
                        let quarter = format!("{i:08}{}", "x".repeat(ROW_BYTES / 4));
                        for _ in 0..4 {
                            builder.values().append_value(&quarter);
                        }
                        builder.append(true);
                    }
                    Arc::new(builder.finish())
                }
                _ => unreachable!(),
            };
            RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(ids), payload])
                .unwrap()
        })
        .collect();

    let table = MemTable::try_new(Arc::clone(&schema), vec![batches]).unwrap();
    let runtime = RuntimeEnvBuilder::new()
        .with_memory_pool(Arc::new(FairSpillPool::new(POOL_BYTES)))
        .build_arc()
        .unwrap();
    let config = SessionConfig::new()
        .with_target_partitions(1)
        // 512 rows of 64 KiB is 32 MiB, more than the pool.
        .with_batch_size(ROWS)
        .with_sort_spill_reservation_bytes(1024 * 1024);
    let ctx = SessionContext::new_with_config_rt(config, runtime);
    ctx.register_table("t", Arc::new(table)).unwrap();

    let result = ctx
        .sql("SELECT id, payload FROM t ORDER BY id DESC")
        .await
        .unwrap()
        .collect()
        .await
        .expect("the sort must spill and finish");

    let ids: Vec<i64> = result
        .iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
                .to_vec()
        })
        .collect();
    let expected: Vec<i64> = (0..ROWS as i64).rev().collect();
    assert_eq!(ids, expected);
}

#[tokio::test]
async fn sort_wide_utf8_rows_spills_and_finishes() {
    sort_wide_rows(DataType::Utf8).await;
}

#[tokio::test]
async fn sort_wide_utf8_view_rows_spills_and_finishes() {
    sort_wide_rows(DataType::Utf8View).await;
}

/// A list column keeps its whole child array alive when it is sliced, so the spill writer
/// must measure and copy only the child range that each piece uses.
#[tokio::test]
async fn sort_wide_list_rows_spills_and_finishes() {
    sort_wide_rows(DataType::new_list(DataType::Utf8, true)).await;
}
