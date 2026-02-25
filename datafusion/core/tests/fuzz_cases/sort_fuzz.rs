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

use std::sync::Arc;

use arrow::{
    array::{ArrayRef, Int32Array, StringArray, as_string_array},
    compute::SortOptions,
    record_batch::RecordBatch,
};
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::physical_plan::expressions::PhysicalSortExpr;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::{ExecutionPlan, collect};
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_common::cast::as_int32_array;
use datafusion_execution::memory_pool::GreedyMemoryPool;
use datafusion_physical_expr::expressions::col;
use datafusion_physical_expr_common::sort_expr::LexOrdering;

use rand::Rng;
use test_utils::{batches_to_vec, partitions_to_sorted_vec};

const KB: usize = 1 << 10;
#[tokio::test]
#[cfg_attr(tarpaulin, ignore)]
async fn test_sort_10k_mem() {
    for (batch_size, should_spill) in [(5, false), (20000, true), (500000, true)] {
        let (input, collected) = SortTest::new()
            .with_int32_batches(batch_size)
            .with_sort_columns(vec!["x"])
            .with_pool_size(10 * KB)
            .with_should_spill(should_spill)
            .run()
            .await;

        let expected = partitions_to_sorted_vec(&input);
        let actual = batches_to_vec(&collected);
        assert_eq!(expected, actual, "failure in @ batch_size {batch_size:?}");
    }
}

#[tokio::test]
#[cfg_attr(tarpaulin, ignore)]
async fn test_sort_100k_mem() {
    for (batch_size, should_spill) in
        [(5, false), (10000, false), (20000, true), (1000000, true)]
    {
        let (input, collected) = SortTest::new()
            .with_int32_batches(batch_size)
            .with_sort_columns(vec!["x"])
            .with_pool_size(100 * KB)
            .with_should_spill(should_spill)
            .run()
            .await;

        let expected = partitions_to_sorted_vec(&input);
        let actual = batches_to_vec(&collected);
        assert_eq!(expected, actual, "failure in @ batch_size {batch_size:?}");
    }
}

#[tokio::test]
#[cfg_attr(tarpaulin, ignore)]
async fn test_sort_strings_100k_mem() {
    for (batch_size, should_spill) in
        [(5, false), (1000, false), (10000, true), (20000, true)]
    {
        let (input, collected) = SortTest::new()
            .with_utf8_batches(batch_size)
            .with_sort_columns(vec!["x"])
            .with_pool_size(100 * KB)
            .with_should_spill(should_spill)
            .run()
            .await;

        let mut input = input
            .iter()
            .flat_map(|p| p.iter())
            .flat_map(|b| {
                let array = b.column(0);
                as_string_array(array)
                    .iter()
                    .map(|s| s.unwrap().to_string())
            })
            .collect::<Vec<String>>();
        input.sort_unstable();
        let actual = collected
            .iter()
            .flat_map(|b| {
                let array = b.column(0);
                as_string_array(array)
                    .iter()
                    .map(|s| s.unwrap().to_string())
            })
            .collect::<Vec<String>>();
        assert_eq!(input, actual);
    }
}

#[tokio::test]
#[cfg_attr(tarpaulin, ignore)]
async fn test_sort_multi_columns_100k_mem() {
    for (batch_size, should_spill) in
        [(5, false), (1000, false), (10000, true), (20000, true)]
    {
        let (input, collected) = SortTest::new()
            .with_int32_utf8_batches(batch_size)
            .with_sort_columns(vec!["x", "y"])
            .with_pool_size(100 * KB)
            .with_should_spill(should_spill)
            .run()
            .await;

        fn record_batch_to_vec(b: &RecordBatch) -> Vec<(i32, String)> {
            let mut rows: Vec<_> = Vec::new();
            let i32_array = as_int32_array(b.column(0)).unwrap();
            let string_array = as_string_array(b.column(1));
            for i in 0..b.num_rows() {
                let str = string_array.value(i).to_string();
                let i32 = i32_array.value(i);
                rows.push((i32, str));
            }
            rows
        }
        let mut input = input
            .iter()
            .flat_map(|p| p.iter())
            .flat_map(record_batch_to_vec)
            .collect::<Vec<(i32, String)>>();
        input.sort_unstable();
        let actual = collected
            .iter()
            .flat_map(record_batch_to_vec)
            .collect::<Vec<(i32, String)>>();
        assert_eq!(input, actual);
    }
}

#[tokio::test]
async fn test_sort_unlimited_mem() {
    for (batch_size, should_spill) in [(5, false), (20000, false), (1000000, false)] {
        let (input, collected) = SortTest::new()
            .with_int32_batches(batch_size)
            .with_sort_columns(vec!["x"])
            .with_pool_size(usize::MAX)
            .with_should_spill(should_spill)
            .run()
            .await;

        let expected = partitions_to_sorted_vec(&input);
        let actual = batches_to_vec(&collected);
        assert_eq!(expected, actual, "failure in @ batch_size {batch_size:?}");
    }
}

#[derive(Debug, Default)]
struct SortTest {
    input: Vec<Vec<RecordBatch>>,
    /// The names of the columns to sort by
    sort_columns: Vec<String>,
    /// GreedyMemoryPool size, if specified
    pool_size: Option<usize>,
    /// If true, expect the sort to spill
    should_spill: bool,
}

impl SortTest {
    fn new() -> Self {
        Default::default()
    }

    fn with_sort_columns(mut self, sort_columns: Vec<&str>) -> Self {
        self.sort_columns = sort_columns.iter().map(|s| (*s).to_string()).collect();
        self
    }

    /// Create batches of int32 values of rows
    fn with_int32_batches(mut self, rows: usize) -> Self {
        self.input = vec![make_staggered_i32_batches(rows)];
        self
    }

    /// Create batches of utf8 values of rows
    fn with_utf8_batches(mut self, rows: usize) -> Self {
        self.input = vec![make_staggered_utf8_batches(rows)];
        self
    }

    /// Create batches of int32 and utf8 values of rows
    fn with_int32_utf8_batches(mut self, rows: usize) -> Self {
        self.input = vec![make_staggered_i32_utf8_batches(rows)];
        self
    }

    /// specify that this test should use a memory pool of the specified size
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
    async fn run(&self) -> (Vec<Vec<RecordBatch>>, Vec<RecordBatch>) {
        let input = self.input.clone();
        let first_batch = input
            .iter()
            .flat_map(|p| p.iter())
            .next()
            .expect("at least one batch");
        let schema = first_batch.schema();

        let sort_ordering =
            LexOrdering::new(self.sort_columns.iter().map(|c| PhysicalSortExpr {
                expr: col(c, &schema).unwrap(),
                options: SortOptions {
                    descending: false,
                    nulls_first: true,
                },
            }))
            .unwrap();

        let exec = MemorySourceConfig::try_new_exec(&input, schema, None).unwrap();
        let sort = Arc::new(SortExec::new(sort_ordering, exec));

        let session_config = SessionConfig::new().with_repartition_file_scans(false);
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

        (input, collected)
    }
}

/// Return randomly sized record batches in a field named 'x' of type `Int32`
/// with randomized i32 content
fn make_staggered_i32_batches(len: usize) -> Vec<RecordBatch> {
    let mut rng = rand::rng();
    let max_batch = 1024;

    let mut batches = vec![];
    let mut remaining = len;
    while remaining != 0 {
        let to_read = rng.random_range(0..=remaining.min(max_batch));
        remaining -= to_read;

        batches.push(
            RecordBatch::try_from_iter(vec![(
                "x",
                Arc::new(Int32Array::from_iter_values(
                    (0..to_read).map(|_| rng.random()),
                )) as ArrayRef,
            )])
            .unwrap(),
        )
    }
    batches
}

/// Return randomly sized record batches in a field named 'x' of type `Utf8`
/// with randomized content
fn make_staggered_utf8_batches(len: usize) -> Vec<RecordBatch> {
    let mut rng = rand::rng();
    let max_batch = 1024;

    let mut batches = vec![];
    let mut remaining = len;
    while remaining != 0 {
        let to_read = rng.random_range(0..=remaining.min(max_batch));
        remaining -= to_read;

        batches.push(
            RecordBatch::try_from_iter(vec![(
                "x",
                Arc::new(StringArray::from_iter_values(
                    (0..to_read).map(|_| format!("test_string_{}", rng.random::<u32>())),
                )) as ArrayRef,
            )])
            .unwrap(),
        )
    }
    batches
}

/// Return randomly sized record batches in a field named 'x' of type `Int32`
/// with randomized i32 content and a field named 'y' of type `Utf8`
/// with randomized content
fn make_staggered_i32_utf8_batches(len: usize) -> Vec<RecordBatch> {
    let mut rng = rand::rng();
    let max_batch = 1024;

    let mut batches = vec![];
    let mut remaining = len;
    while remaining != 0 {
        let to_read = rng.random_range(0..=remaining.min(max_batch));
        remaining -= to_read;

        batches.push(
            RecordBatch::try_from_iter(vec![
                (
                    "x",
                    Arc::new(Int32Array::from_iter_values(
                        (0..to_read).map(|_| rng.random()),
                    )) as ArrayRef,
                ),
                (
                    "y",
                    Arc::new(StringArray::from_iter_values(
                        (0..to_read)
                            .map(|_| format!("test_string_{}", rng.random::<u32>())),
                    )) as ArrayRef,
                ),
            ])
            .unwrap(),
        )
    }

    batches
}
