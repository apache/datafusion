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
//! data and checks `spilled_bytes` against the logical size of the rows
//! actually written, including rows rewritten in intermediate merge passes. If an operator's spill path stops compacting (or a new
//! write path bypasses `append_batch`), the duplicated buffers either trip
//! that bound (sort) or blow up the merge memory estimates and fail the query
//! outright (aggregation); see `assert_compact_spill` for the measurements
//! behind the bounds.

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
///
/// `batch_size` is a sensitivity knob rather than a realistic setting: the
/// amplification a missing compaction produces is proportional to the number
/// of sliced batches sharing one parent buffer, so smaller batches make the
/// regression easier to assert on. The following historical measurements
/// predate recursive construction accounting and its two-input merge limit.
/// They compare cumulative spill bytes to one logical copy, not bytes per
/// written row; they do not describe the current candidate. With GC disabled,
/// at an 8 MB pool:
///
/// | batch_size | 256   | 512   | 1024  | 4096  | 8192  |
/// |------------|-------|-------|-------|-------|-------|
/// | compacted  | 1.02x | 1.01x | 1.01x | 1.00x | 1.00x |
/// | regressed  | 9.34x | 2.56x | 1.68x | 1.08x | 1.04x |
async fn run_spilling_query(
    sql: &str,
    pool_bytes: usize,
    batch_size: usize,
) -> Result<Arc<dyn ExecutionPlan>> {
    let batches = view_batches();
    let table = MemTable::try_new(view_schema(), vec![batches])?;

    let runtime = RuntimeEnvBuilder::new()
        .with_memory_pool(Arc::new(FairSpillPool::new(pool_bytes)))
        .build_arc()?;
    // A single partition keeps the whole workload on one operator instance,
    // making the spill volume deterministic: every configuration below
    // reproduces its `spilled_bytes` exactly, byte for byte, across runs.
    let config = SessionConfig::new()
        .with_target_partitions(1)
        .with_batch_size(batch_size)
        .with_sort_spill_reservation_bytes(256 * 1024);
    let ctx = SessionContext::new_with_config_rt(config, runtime);
    ctx.register_table("t", Arc::new(table))?;

    let plan = ctx.sql(sql).await?.create_physical_plan().await?;
    collect(Arc::clone(&plan), ctx.task_ctx()).await?;
    Ok(plan)
}

/// Assert each written row remains compact, independently of the number of
/// intermediate merge passes. This fixture has equal logical bytes per row.
fn assert_compact_spill(plan: &dyn ExecutionPlan, context: &str) {
    let logical_bytes = logical_size(&view_batches());
    let spill_count = plan_spill_count(plan);
    let spilled_bytes = plan_spilled_bytes(plan);
    fn spilled_rows(plan: &dyn ExecutionPlan) -> usize {
        plan.metrics().and_then(|m| m.spilled_rows()).unwrap_or(0)
            + plan
                .children()
                .into_iter()
                .map(|child| spilled_rows(child.as_ref()))
                .sum::<usize>()
    }
    let written_rows = spilled_rows(plan);
    let input_rows = NUM_BATCHES * ROWS_PER_BATCH;
    let written_logical_bytes = logical_bytes
        .checked_mul(written_rows)
        .expect("fixture accounting fits usize")
        / input_rows;
    println!(
        "{context}: spill_count={spill_count}, spilled_bytes={spilled_bytes}, \
         logical_bytes={logical_bytes}, spilled_rows={written_rows}, \
         written_logical_bytes={written_logical_bytes}"
    );

    assert!(spill_count > 0, "{context}: expected the query to spill");
    // Vacuity guard, not a regression bound: it keeps the upper bound from
    // passing trivially if a future change makes these queries stop pushing a
    // meaningful share of the data through the spill files. Measured ratios
    // are 0.50x for aggregation (which spills part of its input) and 1.02x
    // for sort, so 0.25x leaves 2x of headroom under the tighter of the two.
    assert!(
        spilled_bytes > logical_bytes / 4,
        "{context}: expected a meaningful share of the data to spill, got \
         {spilled_bytes} of {logical_bytes} logical bytes"
    );
    // Each row in this fixed-length fixture has the same logical size.
    // Normalize by actual spilled rows so additional intermediate merge passes
    // do not look like duplicated backing buffers. A missing GC must still fail
    // the per-written-row bound; increasing the number of passes cannot hide it.
    // At this fixed sort configuration (8 MiB, 256 rows), the two-input policy
    // writes 148,000 rows: compacted 16,522,256 / 16,280,000 = 1.015x;
    // disabling GC gives 63,768,560 / 16,280,000 = 3.917x. This is a calibration
    // for these parameters, not a claim about every pool/batch-size combination.
    // The aggregation mutation instead fails with ResourcesExhausted, so this
    // byte bound is only a consistency check for that path.
    assert!(written_rows > 0, "{context}: expected spilled rows");
    assert!(
        spilled_bytes < written_logical_bytes * 2,
        "{context}: spilled {spilled_bytes} bytes for {written_rows} written rows \
         ({written_logical_bytes} logical bytes); view backing buffers are not compacted"
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
        1024,
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
        8 * 1024 * 1024,
        256,
    )
    .await?;
    assert_compact_spill(plan.as_ref(), "sort");
    Ok(())
}
