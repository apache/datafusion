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

//! End-to-end regression tests for spill file compaction of view arrays.
//!
//! `StringViewArray` / `BinaryViewArray` batches produced by `take` or
//! `slice` keep referencing the parent's data buffers. If such batches were
//! written to spill files as-is, the IPC writer would duplicate every
//! referenced buffer for every batch, inflating spill files by an order of
//! magnitude (issue #19414 measured 820 MB of spill files for 33 MB of
//! data). To prevent that, `InProgressSpillFile::append_batch` compacts view
//! arrays with `gc_view_arrays` before writing.
//!
//! The unit tests in `datafusion-physical-plan`'s `spill` module verify that
//! compaction at the `SpillManager` level. The tests here additionally pin
//! down that the operators with the heaviest view-array spill traffic (hash
//! aggregation and sort) actually route their spill writes through that
//! compaction: each test runs a spilling query over `Utf8View` / `BinaryView`
//! data and asserts the total `spilled_bytes` stays proportional to the
//! logical data size. If an operator's spill path stops compacting (or a new
//! write path bypasses `append_batch`), the multiplied buffer duplication
//! trips the upper bound.

use std::sync::Arc;

use arrow::array::cast::AsArray;
use arrow::array::{
    Array, ArrayRef, BinaryViewArray, GenericByteViewArray, Int64Array, RecordBatch,
    StringViewArray,
};
use arrow::datatypes::{ByteViewType, DataType, Field, Schema, SchemaRef};
use datafusion::datasource::MemTable;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::physical_plan::{ExecutionPlan, collect};
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_common::Result;
use datafusion_execution::memory_pool::FairSpillPool;

use crate::helper::plan_metrics::{plan_spill_count, plan_spilled_bytes};

const NUM_BATCHES: usize = 20;
const ROWS_PER_BATCH: usize = 2000;

/// Deterministic table with one column of each view type plus an integer
/// column. All string/binary values are longer than 12 bytes so none of them
/// are inlined into the views: every value lives in the data buffers that
/// spill compaction is responsible for deduplicating.
fn view_batches() -> Vec<RecordBatch> {
    let schema = view_schema();
    (0..NUM_BATCHES)
        .map(|batch_idx| {
            let row_offset = batch_idx * ROWS_PER_BATCH;
            // Group keys repeat every other row for a 50% group/row ratio:
            // high enough cardinality to build up aggregation state, while
            // still exercising actual grouping.
            let strings: Vec<String> = (0..ROWS_PER_BATCH)
                .map(|i| format!("group_key_{:029}", usize::midpoint(row_offset, i)))
                .collect();
            let binaries: Vec<Vec<u8>> = (0..ROWS_PER_BATCH)
                .map(|i| format!("payload_{:023}", row_offset + i).into_bytes())
                .collect();
            let values: Vec<i64> = (0..ROWS_PER_BATCH)
                .map(|i| (row_offset + i) as i64)
                .collect();
            RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(StringViewArray::from_iter_values(strings)) as ArrayRef,
                    Arc::new(BinaryViewArray::from_iter_values(binaries)) as ArrayRef,
                    Arc::new(Int64Array::from(values)) as ArrayRef,
                ],
            )
            .unwrap()
        })
        .collect()
}

fn view_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("group_key", DataType::Utf8View, false),
        Field::new("payload", DataType::BinaryView, false),
        Field::new("value", DataType::Int64, false),
    ]))
}

/// Bytes needed to materialize the batches into fresh, fully compact
/// buffers: 16 bytes per view plus the out-of-line data, plus the integer
/// column. This is what a compacted spill of the full table costs before
/// IPC framing overhead.
fn logical_size(batches: &[RecordBatch]) -> usize {
    fn view_bytes<T: ByteViewType>(array: &GenericByteViewArray<T>) -> usize {
        // 16 bytes per view struct plus the out-of-line value data
        array.len() * 16
            + array
                .iter()
                .flatten()
                .map(|v| AsRef::<[u8]>::as_ref(v).len())
                .sum::<usize>()
    }

    batches
        .iter()
        .flat_map(|batch| batch.columns())
        .map(|array| match array.data_type() {
            DataType::Utf8View => view_bytes(array.as_string_view()),
            DataType::BinaryView => view_bytes(array.as_binary_view()),
            _ => array.len() * 8,
        })
        .sum()
}

/// Run `sql` over the view table with a memory pool small enough to force
/// spilling, and return the executed plan for metric inspection.
async fn run_spilling_query(
    sql: &str,
    pool_bytes: usize,
) -> Result<Arc<dyn ExecutionPlan>> {
    let batches = view_batches();
    let table = MemTable::try_new(view_schema(), vec![batches])?;

    let runtime = RuntimeEnvBuilder::new()
        .with_memory_pool(Arc::new(FairSpillPool::new(pool_bytes)))
        .build_arc()?;
    // A single partition keeps the whole workload on one operator instance,
    // making the spill volume deterministic. The small batch size makes the
    // emitted batches get sliced into many chunks, so a missing compaction
    // shows up as a large (roughly chunk-count times) amplification.
    let config = SessionConfig::new()
        .with_target_partitions(1)
        .with_batch_size(1024)
        .with_sort_spill_reservation_bytes(256 * 1024);
    let ctx = SessionContext::new_with_config_rt(config, runtime);
    ctx.register_table("t", Arc::new(table))?;

    let plan = ctx.sql(sql).await?.create_physical_plan().await?;
    collect(Arc::clone(&plan), ctx.task_ctx()).await?;
    Ok(plan)
}

/// Assert the query spilled, and that the spilled bytes stayed proportional
/// to the logical data size instead of being amplified by duplicated view
/// buffers.
fn assert_compact_spill(plan: &dyn ExecutionPlan, context: &str) {
    let logical_bytes = logical_size(&view_batches());
    let spill_count = plan_spill_count(plan);
    let spilled_bytes = plan_spilled_bytes(plan);
    println!(
        "{context}: spill_count={spill_count}, spilled_bytes={spilled_bytes}, \
         logical_bytes={logical_bytes}"
    );

    assert!(spill_count > 0, "{context}: expected the query to spill");
    // Sanity check that a meaningful share of the data went through the
    // spill files, so the compaction bound below actually tests something.
    assert!(
        spilled_bytes > logical_bytes / 4,
        "{context}: expected a meaningful share of the data to spill, got \
         {spilled_bytes} of {logical_bytes} logical bytes"
    );
    // The regression bound: compacted spill files hold each spilled row once,
    // so total spilled bytes stay within a small factor of the logical size
    // (IPC framing and re-spilling during multi-pass merging contribute the
    // slack; measured ratios are ~1.01x for sort and ~0.5x for aggregation,
    // which spills only part of its input). Without `gc_view_arrays` in the
    // spill path, the shared view buffers are re-written for every sliced
    // batch: at looser memory limits that inflates the spill files, and at
    // the limits used here the inflated buffer sizes flow into the merge
    // memory estimates and both queries fail with ResourcesExhausted before
    // reaching this assertion. Either way a compaction regression fails the
    // test.
    assert!(
        spilled_bytes < logical_bytes * 3,
        "{context}: spilled {spilled_bytes} bytes for {logical_bytes} logical \
         bytes; view array buffers are being duplicated into the spill files \
         instead of compacted"
    );
}

/// Hash aggregation spill path: emitted batches are sliced into batch-sized
/// chunks (each sharing the emitted batch's view buffers) before being
/// written to the spill file.
#[tokio::test]
async fn aggregate_spill_is_compacted_for_view_arrays() -> Result<()> {
    let plan = run_spilling_query(
        "SELECT group_key, max(payload) AS payload, sum(value) AS total \
         FROM t GROUP BY group_key",
        2 * 1024 * 1024,
    )
    .await?;
    assert_compact_spill(plan.as_ref(), "aggregate");
    Ok(())
}

/// Sort spill path: sorted runs are produced with `take` over the buffered
/// input, so every spilled batch shares the concatenated input's view
/// buffers.
#[tokio::test]
async fn sort_spill_is_compacted_for_view_arrays() -> Result<()> {
    let plan = run_spilling_query(
        "SELECT group_key, payload, value FROM t ORDER BY group_key",
        4 * 1024 * 1024,
    )
    .await?;
    assert_compact_spill(plan.as_ref(), "sort");
    Ok(())
}
