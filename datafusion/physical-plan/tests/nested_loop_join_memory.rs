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

//! Peak-allocation check for the NestedLoopJoin build side under a memory limit.
//!
//! The memory pool only sees what the operator reserves, so a copy of the build side that is
//! never reserved (the `concat_batches` this operator used to make, or coalescing a pass while
//! its inputs are still live) is invisible to pool-based assertions. This binary counts live
//! bytes at the allocator instead, and must stay the only test in it so nothing else runs
//! alongside the measurement.

use std::alloc::{GlobalAlloc, Layout, System};
use std::any::Any;
use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use arrow::array::{Int32Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use datafusion_common::{JoinSide, JoinType, Result};
use datafusion_execution::TaskContext;
use datafusion_execution::runtime_env::RuntimeEnvBuilder;
use datafusion_expr::Operator;
use datafusion_physical_expr::expressions::{BinaryExpr, Column};
use datafusion_physical_plan::common::collect;
use datafusion_physical_plan::joins::NestedLoopJoinExec;
use datafusion_physical_plan::joins::utils::{ColumnIndex, JoinFilter};
use datafusion_physical_plan::memory::{LazyBatchGenerator, LazyMemoryExec};
use datafusion_physical_plan::test::TestMemoryExec;
use datafusion_physical_plan::{ExecutionPlan, PhysicalExpr};
use parking_lot::RwLock;

struct PeakTrackingAllocator;

static LIVE_BYTES: AtomicUsize = AtomicUsize::new(0);
static PEAK_BYTES: AtomicUsize = AtomicUsize::new(0);

fn record_alloc(bytes: usize) {
    let live = LIVE_BYTES.fetch_add(bytes, Ordering::Relaxed) + bytes;
    PEAK_BYTES.fetch_max(live, Ordering::Relaxed);
}

unsafe impl GlobalAlloc for PeakTrackingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc(layout) };
        if !ptr.is_null() {
            record_alloc(layout.size());
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) };
        LIVE_BYTES.fetch_sub(layout.size(), Ordering::Relaxed);
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let new_ptr = unsafe { System.realloc(ptr, layout, new_size) };
        if !new_ptr.is_null() {
            LIVE_BYTES.fetch_sub(layout.size(), Ordering::Relaxed);
            record_alloc(new_size);
        }
        new_ptr
    }
}

#[global_allocator]
static GLOBAL: PeakTrackingAllocator = PeakTrackingAllocator;

const ROW_BYTES: usize = 64 * 1024;
const BUILD_ROWS: usize = 512;
const POOL_BYTES: usize = 8 * 1024 * 1024;

/// One row per batch, each with its own 64 KiB string allocation, so a target-row coalescer
/// would merge a whole pass into a single chunk and copy every byte of it. The count is shared
/// across `reset_state` so the test can read it after the run.
#[derive(Debug)]
struct WideRows {
    schema: SchemaRef,
    emitted: Arc<AtomicUsize>,
}

impl fmt::Display for WideRows {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "WideRows(emitted={})",
            self.emitted.load(Ordering::Relaxed)
        )
    }
}

impl LazyBatchGenerator for WideRows {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn generate_next_batch(&mut self) -> Result<Option<RecordBatch>> {
        if self.emitted.load(Ordering::Relaxed) == BUILD_ROWS {
            return Ok(None);
        }
        self.emitted.fetch_add(1, Ordering::Relaxed);
        let batch = RecordBatch::try_new(
            Arc::clone(&self.schema),
            vec![
                Arc::new(Int32Array::from(vec![0])),
                Arc::new(StringArray::from(vec!["x".repeat(ROW_BYTES)])),
            ],
        )?;
        Ok(Some(batch))
    }

    fn reset_state(&self) -> Arc<RwLock<dyn LazyBatchGenerator>> {
        Arc::new(RwLock::new(WideRows {
            schema: Arc::clone(&self.schema),
            emitted: Arc::clone(&self.emitted),
        }))
    }
}

/// `left.k > right.k` with every left `k` at 0 and every right `k` at 1, so the join keeps its
/// full probe shape while producing no rows to hold on to.
fn never_matching_filter() -> JoinFilter {
    let expression: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
        Arc::new(Column::new("k", 0)),
        Operator::Gt,
        Arc::new(Column::new("k", 1)),
    ));
    let column_indices = vec![
        ColumnIndex {
            index: 0,
            side: JoinSide::Left,
        },
        ColumnIndex {
            index: 0,
            side: JoinSide::Right,
        },
    ];
    let schema = Schema::new(vec![
        Field::new("k", DataType::Int32, false),
        Field::new("k", DataType::Int32, false),
    ]);
    JoinFilter::new(expression, column_indices, Arc::new(schema))
}

#[tokio::test]
async fn build_side_spill_and_replay_stay_within_the_pool() -> Result<()> {
    let left_schema = Arc::new(Schema::new(vec![
        Field::new("k", DataType::Int32, false),
        Field::new("s", DataType::Utf8, false),
    ]));
    let emitted = Arc::new(AtomicUsize::new(0));
    let left: Arc<dyn ExecutionPlan> = Arc::new(LazyMemoryExec::try_new(
        Arc::clone(&left_schema),
        vec![Arc::new(RwLock::new(WideRows {
            schema: left_schema,
            emitted: Arc::clone(&emitted),
        }))],
    )?);

    let right_schema =
        Arc::new(Schema::new(vec![Field::new("k", DataType::Int32, false)]));
    let right_batch = RecordBatch::try_new(
        Arc::clone(&right_schema),
        vec![Arc::new(Int32Array::from(vec![1, 1, 1, 1]))],
    )?;
    let right: Arc<dyn ExecutionPlan> =
        TestMemoryExec::try_new_exec(&[vec![right_batch]], right_schema, None)?;

    let join = NestedLoopJoinExec::try_new(
        left,
        right,
        Some(never_matching_filter()),
        &JoinType::Inner,
        None,
    )?;
    let runtime = RuntimeEnvBuilder::new()
        .with_memory_limit(POOL_BYTES, 1.0)
        .build_arc()?;
    let ctx = Arc::new(TaskContext::default().with_runtime(runtime));

    let baseline = LIVE_BYTES.load(Ordering::Relaxed);
    PEAK_BYTES.store(baseline, Ordering::Relaxed);
    let output = collect(join.execute(0, ctx)?).await?;
    let peak = PEAK_BYTES.load(Ordering::Relaxed) - baseline;

    let output_rows: usize = output.iter().map(|b| b.num_rows()).sum();
    assert_eq!(output_rows, 0);
    let metrics = join.metrics().expect("metrics");
    assert!(
        metrics.spill_count().unwrap_or(0) > 0,
        "the build side must spill"
    );
    assert_eq!(
        emitted.load(Ordering::Relaxed),
        BUILD_ROWS,
        "every build row must be consumed"
    );

    // The build side is four times the pool, so it spills and is replayed pass by pass, each pass
    // filling the pool. Any unreserved copy of a pass (a coalesced spill chunk at the spill
    // transition, or a coalesced replay pass) doubles that peak; ordinary bookkeeping (the
    // carried-over batch, spill reader buffers, the probe side) stays far below half a pool.
    let limit = POOL_BYTES + POOL_BYTES / 2;
    assert!(
        peak < limit,
        "peak live allocation {:.2} MiB exceeds {:.2} MiB with an {:.0} MiB pool",
        peak as f64 / (1024.0 * 1024.0),
        limit as f64 / (1024.0 * 1024.0),
        POOL_BYTES as f64 / (1024.0 * 1024.0),
    );
    Ok(())
}
