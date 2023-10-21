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
use arrow_array::{Float64Array, StringArray};
use datafusion::physical_plan::expressions::PhysicalSortExpr;
use datafusion::physical_plan::memory::MemoryExec;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::{collect, ExecutionPlan};
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion::{
    datasource::MemTable,
    execution::runtime_env::{RuntimeConfig, RuntimeEnv},
};
use datafusion_common::{
    cast::{as_float64_array, as_string_array},
    TableReference,
};
use datafusion_execution::memory_pool::GreedyMemoryPool;
use datafusion_physical_expr::expressions::col;
use rand::{rngs::StdRng, Rng, SeedableRng};
use std::sync::Arc;
use test_utils::{batches_to_vec, partitions_to_sorted_vec, stagger_batch};

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

#[tokio::test]
async fn test_sort_topk() {
    for size in [10, 100, 1000, 10000, 1000000] {
        let mut topk_scenario = TopKScenario::new()
            .with_limit(10)
            .with_table_name("t")
            .with_col_name("x");

        // test topk with i32
        let collected_i32 = SortTest::new()
            .with_input(topk_scenario.batches(size, ColType::I32))
            .run_with_limit(&topk_scenario)
            .await;
        let actual = batches_to_vec(&collected_i32);
        let excepted_i32 = topk_scenario.excepted_i32();
        assert_eq!(actual, excepted_i32);

        // test topk with f64
        let collected_f64 = SortTest::new()
            .with_input(topk_scenario.batches(size, ColType::F64))
            .run_with_limit(&topk_scenario)
            .await;
        let actual: Vec<Option<f64>> = batches_to_f64_vec(&collected_f64);
        let excepted_f64 = topk_scenario.excepted_f64();
        assert_eq!(actual, excepted_f64);

        // test topk with str
        let collected_str = SortTest::new()
            .with_input(topk_scenario.batches(size, ColType::Str))
            .run_with_limit(&topk_scenario)
            .await;
        let actual: Vec<Option<&str>> = batches_to_str_vec(&collected_str);
        let excepted_str = topk_scenario.excepted_str();
        assert_eq!(actual, excepted_str);
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

    fn with_input(mut self, batches: Vec<Vec<RecordBatch>>) -> Self {
        self.input = batches.clone();
        self
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

    async fn run_with_limit<'a>(
        &self,
        topk_scenario: &TopKScenario<'a>,
    ) -> Vec<RecordBatch> {
        let input = self.input.clone();
        let schema = input
            .iter()
            .flat_map(|p| p.iter())
            .next()
            .expect("at least one batch")
            .schema();

        let table = MemTable::try_new(schema, input.clone()).unwrap();

        let ctx = SessionContext::new();

        ctx.register_table(
            TableReference::Bare {
                table: topk_scenario.table_name.into(),
            },
            Arc::new(table),
        )
        .unwrap();

        let df = ctx
            .table(topk_scenario.table_name)
            .await
            .unwrap()
            .sort(vec![
                datafusion_expr::col(topk_scenario.col_name).sort(true, true)
            ])
            .unwrap()
            .limit(0, Some(topk_scenario.limit))
            .unwrap();

        df.collect().await.unwrap()
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

            let runtime_config = RuntimeConfig::new()
                .with_memory_pool(Arc::new(GreedyMemoryPool::new(pool_size)));
            let runtime = Arc::new(RuntimeEnv::new(runtime_config).unwrap());
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

enum ColType {
    I32,
    F64,
    Str,
}

struct TopKScenario<'a> {
    limit: usize,
    batches: Vec<Vec<RecordBatch>>,
    table_name: &'a str,
    col_name: &'a str,
}

impl<'a> TopKScenario<'a> {
    fn new() -> Self {
        TopKScenario {
            limit: 0,
            batches: vec![],
            table_name: "",
            col_name: "",
        }
    }

    fn with_limit(mut self, limit: usize) -> Self {
        self.limit = limit;
        self
    }

    fn with_table_name(mut self, table_name: &'a str) -> Self {
        self.table_name = table_name;
        self
    }

    fn with_col_name(mut self, col_name: &'a str) -> Self {
        self.col_name = col_name;
        self
    }

    fn batches(&mut self, len: usize, t: ColType) -> Vec<Vec<RecordBatch>> {
        let batches = match t {
            ColType::I32 => make_staggered_i32_batches(len),
            ColType::F64 => make_staggered_f64_batches(len),
            ColType::Str => make_staggered_str_batches(len),
        };
        self.batches = vec![batches];
        self.batches.clone()
    }

    fn excepted_i32(&self) -> Vec<Option<i32>> {
        let excepted = partitions_to_sorted_vec(&self.batches);
        excepted[0..self.limit].into()
    }

    fn excepted_f64(&self) -> Vec<Option<f64>> {
        let mut excepted: Vec<Option<f64>> = self
            .batches
            .iter()
            .flat_map(|batches| batches_to_f64_vec(batches).into_iter())
            .collect();
        excepted.sort_by(|a, b| a.partial_cmp(b).unwrap());
        excepted[0..self.limit].into()
    }

    fn excepted_str(&self) -> Vec<Option<&str>> {
        let mut excepted: Vec<Option<&str>> = self
            .batches
            .iter()
            .flat_map(|batches| batches_to_str_vec(batches).into_iter())
            .collect();
        excepted.sort_unstable();
        excepted[0..self.limit].into()
    }
}

impl Default for TopKScenario<'_> {
    fn default() -> Self {
        Self::new()
    }
}

fn make_staggered_f64_batches(len: usize) -> Vec<RecordBatch> {
    let mut rng = StdRng::seed_from_u64(100);
    let remainder = RecordBatch::try_from_iter(vec![(
        "x",
        Arc::new(Float64Array::from_iter_values(
            (0..len).map(|_| rng.gen_range(0.0..1000.7)),
        )) as ArrayRef,
    )])
    .unwrap();
    stagger_batch(remainder)
}

fn make_staggered_str_batches(len: usize) -> Vec<RecordBatch> {
    let remainder = RecordBatch::try_from_iter(vec![(
        "x",
        Arc::new(StringArray::from_iter_values(
            (0..len).map(|_| get_random_string(6)),
        )) as ArrayRef,
    )])
    .unwrap();
    stagger_batch(remainder)
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

/// Return random ASCII String with len
fn get_random_string(len: usize) -> String {
    rand::thread_rng()
        .sample_iter(rand::distributions::Alphanumeric)
        .take(len)
        .map(char::from)
        .collect()
}

fn batches_to_f64_vec(batches: &[RecordBatch]) -> Vec<Option<f64>> {
    batches
        .iter()
        .flat_map(|batch| as_float64_array(batch.column(0)).unwrap().iter())
        .collect()
}

fn batches_to_str_vec(batches: &[RecordBatch]) -> Vec<Option<&str>> {
    batches
        .iter()
        .flat_map(|batch| as_string_array(batch.column(0)).unwrap().iter())
        .collect()
}
