// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Checks that a grouped aggregate reserves memory before its group values
//! grow, so that the memory pool sees the peak heap usage.
//!
//! When the group hash table grows, it holds the old and the new table at the
//! same time. A stream that reserves only after the batch never shows that
//! peak to the pool.

use std::sync::Arc;

use arrow::array::{Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use datafusion_common::Result;
use datafusion_execution::TaskContext;
use datafusion_execution::config::SessionConfig;
use datafusion_execution::memory_pool::counting_allocator::{
    CountingAllocator, ThreadPeak,
};
use datafusion_execution::memory_pool::{PeakRecordingPool, UnboundedMemoryPool};
use datafusion_execution::runtime_env::RuntimeEnvBuilder;
use datafusion_physical_expr::expressions::col;
use datafusion_physical_plan::ExecutionPlan;
use datafusion_physical_plan::aggregates::{
    AggregateExec, AggregateMode, PhysicalGroupBy,
};
use datafusion_physical_plan::common::collect;
use datafusion_physical_plan::test::TestMemoryExec;

#[global_allocator]
static ALLOC: CountingAllocator = CountingAllocator;

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

        let pool = Arc::new(PeakRecordingPool::new(Arc::new(
            UnboundedMemoryPool::default(),
        )));
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

        let heap = ThreadPeak::start();
        let output = collect(aggregate.execute(0, context)?).await?;
        let heap_peak = heap.peak_bytes();
        let reserved_peak = pool.max_reserved();

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
