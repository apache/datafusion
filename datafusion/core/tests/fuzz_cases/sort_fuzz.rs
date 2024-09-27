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

//! Fuzz Test for various corner cases sorting RecordBatches exceeds available memory and should spill

use arrow::{
    array::{ArrayRef, Int32Array},
    compute::SortOptions,
    record_batch::RecordBatch,
};
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::physical_plan::expressions::PhysicalSortExpr;
use datafusion::physical_plan::memory::MemoryExec;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::{collect, ExecutionPlan};
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_execution::memory_pool::GreedyMemoryPool;
use datafusion_physical_expr::expressions::col;
use rand::Rng;
use std::sync::Arc;
use test_utils::{batches_to_vec, partitions_to_sorted_vec};

const KB: usize = 1 << 10;
#[tokio::test]
#[cfg_attr(tarpaulin, ignore)]
async fn test_sort_1k_mem() {
    for (batch_size, should_spill) in [(5, false), (20000, true), (1000000, true)] {
        SortTest::new()
            .with_int32_batches(batch_size)
            .with_pool_size(10 * KB)
            .with_should_spill(should_spill)
            .run()
            .await;
    }
}

#[tokio::test]
#[cfg_attr(tarpaulin, ignore)]
async fn test_sort_100k_mem() {
    for (batch_size, should_spill) in [(5, false), (20000, false), (1000000, true)] {
        SortTest::new()
            .with_int32_batches(batch_size)
            .with_pool_size(100 * KB)
            .with_should_spill(should_spill)
            .run()
            .await;
    }
}

#[tokio::test]
async fn test_sort_unlimited_mem() {
    for (batch_size, should_spill) in [(5, false), (20000, false), (1000000, false)] {
        SortTest::new()
            .with_int32_batches(batch_size)
            .with_pool_size(usize::MAX)
            .with_should_spill(should_spill)
            .run()
            .await;
    }
}
#[derive(Debug, Default)]
struct SortTest {
    input: Vec<Vec<RecordBatch>>,
    /// GreedyMemoryPool size, if specified
    pool_size: Option<usize>,
    /// If true, expect the sort to spill
    should_spill: bool,
}

impl SortTest {
    fn new() -> Self {
        Default::default()
    }

    /// Create batches of int32 values of rows
    fn with_int32_batches(mut self, rows: usize) -> Self {
        self.input = vec![make_staggered_i32_batches(rows)];
        self
    }

    /// specify that this test should use a memory pool of the specifeid size
    fn with_pool_size(mut self, pool_size: usize) -> Self {
        self.pool_size = Some(pool_size);
        self
    }

    fn with_should_spill(mut self, should_spill: bool) -> Self {
        self.should_spill = should_spill;
        self
    }

    /// Sort the input using SortExec and ensure the results are
    /// correct according to `Vec::sort` both with and without spilling
    async fn run(&self) {
        let input = self.input.clone();
        let first_batch = input
            .iter()
            .flat_map(|p| p.iter())
            .next()
            .expect("at least one batch");
        let schema = first_batch.schema();

        let sort = vec![PhysicalSortExpr {
            expr: col("x", &schema).unwrap(),
            options: SortOptions {
                descending: false,
                nulls_first: true,
            },
        }];

        let exec = MemoryExec::try_new(&input, schema, None).unwrap();
        let sort = Arc::new(SortExec::new(sort, Arc::new(exec)));

        let session_config = SessionConfig::new();
        let session_ctx = if let Some(pool_size) = self.pool_size {
            // Make sure there is enough space for the initial spill
            // reservation
            let pool_size = pool_size.saturating_add(
                session_config
                    .options()
                    .execution
                    .sort_spill_reservation_bytes,
            );

            let runtime = RuntimeEnvBuilder::new()
                .with_memory_pool(Arc::new(GreedyMemoryPool::new(pool_size)))
                .build_arc()
                .unwrap();
            SessionContext::new_with_config_rt(session_config, runtime)
        } else {
            SessionContext::new_with_config(session_config)
        };

        let task_ctx = session_ctx.task_ctx();
        let collected = collect(sort.clone(), task_ctx).await.unwrap();

        let expected = partitions_to_sorted_vec(&input);
        let actual = batches_to_vec(&collected);

        if self.should_spill {
            assert_ne!(
                sort.metrics().unwrap().spill_count().unwrap(),
                0,
                "Expected spill, but did not: {self:?}"
            );
        } else {
            assert_eq!(
                sort.metrics().unwrap().spill_count().unwrap(),
                0,
                "Expected no spill, but did: {self:?}"
            );
        }

        assert_eq!(
            session_ctx.runtime_env().memory_pool.reserved(),
            0,
            "The sort should have returned all memory used back to the memory pool"
        );
        assert_eq!(expected, actual, "failure in @ pool_size {self:?}");
    }
}

/// Return randomly sized record batches in a field named 'x' of type `Int32`
/// with randomized i32 content
fn make_staggered_i32_batches(len: usize) -> Vec<RecordBatch> {
    let mut rng = rand::thread_rng();
    let max_batch = 1024;

    let mut batches = vec![];
    let mut remaining = len;
    while remaining != 0 {
        let to_read = rng.gen_range(0..=remaining.min(max_batch));
        remaining -= to_read;

        batches.push(
            RecordBatch::try_from_iter(vec![(
                "x",
                Arc::new(Int32Array::from_iter_values(
                    (0..to_read).map(|_| rng.gen()),
                )) as ArrayRef,
            )])
            .unwrap(),
        )
    }
    batches
}
