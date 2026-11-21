// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Checks that a grouped aggregate reserves memory before its group values
//! grow, so that the memory pool sees the peak heap usage.
//!
//! When the group hash table grows, it holds the old and the new table at the
//! same time. A stream that reserves only after the batch never shows that
//! peak to the pool.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use arrow::array::{Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use datafusion_common::Result;
use datafusion_execution::TaskContext;
use datafusion_execution::config::SessionConfig;
use datafusion_execution::memory_pool::{
    MemoryConsumer, MemoryPool, MemoryReservation, UnboundedMemoryPool,
};
use datafusion_execution::runtime_env::RuntimeEnvBuilder;
use datafusion_physical_expr::expressions::col;
use datafusion_physical_plan::ExecutionPlan;
use datafusion_physical_plan::aggregates::{
    AggregateExec, AggregateMode, PhysicalGroupBy,
};
use datafusion_physical_plan::common::collect;
use datafusion_physical_plan::test::TestMemoryExec;

/// Counts the heap bytes that the current thread allocates, and their peak.
/// The counts are per thread so that tests running in parallel do not mix.
/// Memory freed on another thread makes the count drift, so the counter
/// saturates instead of underflowing.
struct ThreadCountingAlloc;

thread_local! {
    static LIVE: Cell<usize> = const { Cell::new(0) };
    static PEAK: Cell<usize> = const { Cell::new(0) };
}

fn add(bytes: usize) {
    let _ = LIVE.try_with(|live| {
        live.set(live.get().saturating_add(bytes));
        let _ = PEAK.try_with(|peak| peak.set(peak.get().max(live.get())));
    });
}

fn sub(bytes: usize) {
    let _ = LIVE.try_with(|live| live.set(live.get().saturating_sub(bytes)));
}

unsafe impl GlobalAlloc for ThreadCountingAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        add(layout.size());
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        sub(layout.size());
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        // Count a realloc as a copy: the new block is live before the old
        // block is freed.
        add(new_size);
        let new = unsafe { System.realloc(ptr, layout, new_size) };
        sub(if new.is_null() {
            new_size
        } else {
            layout.size()
        });
        new
    }
}

#[global_allocator]
static ALLOC: ThreadCountingAlloc = ThreadCountingAlloc;

/// An [`UnboundedMemoryPool`] that records the peak of its reserved bytes.
#[derive(Debug, Default)]
struct PeakPool {
    inner: UnboundedMemoryPool,
    peak: AtomicUsize,
}

impl std::fmt::Display for PeakPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "PeakPool")
    }
}

impl MemoryPool for PeakPool {
    fn name(&self) -> &str {
        "PeakPool"
    }

    fn grow(&self, reservation: &MemoryReservation, additional: usize) {
        self.inner.grow(reservation, additional);
        self.peak
            .fetch_max(self.inner.reserved(), Ordering::Relaxed);
    }

    fn shrink(&self, reservation: &MemoryReservation, shrink: usize) {
        self.inner.shrink(reservation, shrink);
    }

    fn try_grow(&self, reservation: &MemoryReservation, additional: usize) -> Result<()> {
        self.inner.try_grow(reservation, additional)?;
        self.peak
            .fetch_max(self.inner.reserved(), Ordering::Relaxed);
        Ok(())
    }

    fn reserved(&self) -> usize {
        self.inner.reserved()
    }

    fn register(&self, consumer: &MemoryConsumer) {
        self.inner.register(consumer);
    }
}

/// Bytes that the aggregate allocates outside its reservation: plan and
/// stream structs, metrics, and the output `Vec<RecordBatch>`. They do not
/// grow with the number of groups.
const UNTRACKED_BYTES: usize = 64 * 1024;

/// `GROUP BY a` over distinct `Int64` keys, with no aggregate functions, so
/// that the group values are the only state that grows. The 4 batches of
/// 8192 rows take the table past 28672 groups, the capacity of 32768
/// buckets. So the last batch doubles the table to 65536 buckets.
#[tokio::test]
async fn reservation_covers_group_table_growth() -> Result<()> {
    const BATCH_ROWS: i64 = 8192;
    const BATCHES: i64 = 4;

    for mode in [AggregateMode::Single, AggregateMode::Partial] {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));
        let batches = (0..BATCHES)
            .map(|i| {
                let keys =
                    Int64Array::from_iter_values(i * BATCH_ROWS..(i + 1) * BATCH_ROWS);
                RecordBatch::try_new(Arc::clone(&schema), vec![Arc::new(keys)])
            })
            .collect::<std::result::Result<Vec<_>, _>>()?;
        let input = TestMemoryExec::try_new_exec(&[batches], Arc::clone(&schema), None)?;
        let group_by =
            PhysicalGroupBy::new_single(vec![(col("a", &schema)?, "a".to_string())]);
        let aggregate = Arc::new(AggregateExec::try_new(
            mode,
            group_by,
            vec![],
            vec![],
            input,
            Arc::clone(&schema),
        )?);

        let pool = Arc::new(PeakPool::default());
        let runtime = RuntimeEnvBuilder::new()
            .with_memory_pool(Arc::clone(&pool) as _)
            .build_arc()?;
        let context = Arc::new(
            TaskContext::default()
                .with_session_config(
                    SessionConfig::new().with_batch_size(BATCH_ROWS as usize),
                )
                .with_runtime(runtime),
        );

        let start = LIVE.with(Cell::get);
        PEAK.with(|peak| peak.set(start));
        let output = collect(aggregate.execute(0, context)?).await?;
        let heap_peak = PEAK.with(Cell::get) - start;
        let reserved_peak = pool.peak.load(Ordering::Relaxed);

        let rows: usize = output.iter().map(RecordBatch::num_rows).sum();
        assert_eq!(rows, (BATCH_ROWS * BATCHES) as usize);
        // The doubling alone holds about 1.6 MB (old and new table), so a
        // reservation made after the batch misses it by far more than the
        // untracked bytes.
        assert!(
            heap_peak <= reserved_peak + UNTRACKED_BYTES,
            "{mode:?}: heap peak {heap_peak} exceeds reserved peak {reserved_peak}"
        );
    }
    Ok(())
}
