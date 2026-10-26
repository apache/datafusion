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

//! Benchmarks for an external sort that spills under a memory limit.
//!
//! Each case sorts one partition of `NUM_BATCHES` batches of `BATCH_SIZE` rows with a
//! memory pool smaller than the input, so the sort writes spill files and merges them.

use std::sync::Arc;

use arrow::array::{ArrayRef, DictionaryArray, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Int32Type, Schema};
use arrow::record_batch::RecordBatch;
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use datafusion_execution::TaskContext;
use datafusion_execution::config::SessionConfig;
use datafusion_execution::memory_pool::FairSpillPool;
use datafusion_execution::runtime_env::RuntimeEnvBuilder;
use datafusion_physical_expr::{LexOrdering, PhysicalSortExpr, expressions::col};
use datafusion_physical_plan::sorts::sort::SortExec;
use datafusion_physical_plan::test::TestMemoryExec;
use datafusion_physical_plan::{ExecutionPlan, collect};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use tokio::runtime::Runtime;

const NUM_BATCHES: usize = 16;
const BATCH_SIZE: usize = 8192;

/// A sort key and one payload column, in random key order.
fn make_batches(payload: impl Fn(&mut StdRng, usize) -> ArrayRef) -> Vec<RecordBatch> {
    let mut rng = StdRng::seed_from_u64(42);
    (0..NUM_BATCHES)
        .map(|_| {
            let keys: ArrayRef = Arc::new(Int64Array::from_iter_values(
                (0..BATCH_SIZE).map(|_| rng.random::<i64>()),
            ));
            let payload = payload(&mut rng, BATCH_SIZE);
            let schema = Arc::new(Schema::new(vec![
                Field::new("key", DataType::Int64, false),
                Field::new("payload", payload.data_type().clone(), false),
            ]));
            RecordBatch::try_new(schema, vec![keys, payload]).unwrap()
        })
        .collect()
}

/// Dictionary with many distinct values, shared by all input batches.
fn dictionary_batches() -> Vec<RecordBatch> {
    const DISTINCT: usize = 100_000;
    let values: ArrayRef = Arc::new(StringArray::from_iter_values(
        (0..DISTINCT).map(|i| format!("value number {i:08}")),
    ));
    make_batches(|rng, rows| {
        let keys = (0..rows).map(|_| rng.random_range(0..DISTINCT as i32));
        Arc::new(
            DictionaryArray::<Int32Type>::try_new(keys.collect(), Arc::clone(&values))
                .unwrap(),
        )
    })
}

/// Short strings, so no spilled batch is split.
fn narrow_utf8_batches() -> Vec<RecordBatch> {
    make_batches(|rng, rows| {
        Arc::new(StringArray::from_iter_values(
            (0..rows).map(|_| format!("string {:016}", rng.random::<u32>())),
        ))
    })
}

/// 1 KiB strings: 8 MiB per batch, larger than the spill batch bound.
fn wide_utf8_batches() -> Vec<RecordBatch> {
    make_batches(|rng, rows| {
        Arc::new(StringArray::from_iter_values((0..rows).map(|_| {
            format!("{:016}{}", rng.random::<u32>(), "x".repeat(1008))
        })))
    })
}

fn sort_spilled(rt: &Runtime, batches: &[RecordBatch], pool_bytes: usize) {
    let schema = batches[0].schema();
    let input =
        TestMemoryExec::try_new_exec(&[batches.to_vec()], Arc::clone(&schema), None)
            .unwrap();
    let ordering =
        LexOrdering::new([PhysicalSortExpr::new_default(col("key", &schema).unwrap())])
            .unwrap();
    let sort = Arc::new(SortExec::new(ordering, input));
    let runtime = RuntimeEnvBuilder::new()
        .with_memory_pool(Arc::new(FairSpillPool::new(pool_bytes)))
        .build_arc()
        .unwrap();
    let task_ctx = Arc::new(
        TaskContext::default()
            .with_session_config(SessionConfig::new().with_batch_size(BATCH_SIZE))
            .with_runtime(runtime),
    );
    let output = rt
        .block_on(collect(
            Arc::clone(&sort) as Arc<dyn ExecutionPlan>,
            task_ctx,
        ))
        .unwrap();
    assert_eq!(
        output.iter().map(|b| b.num_rows()).sum::<usize>(),
        NUM_BATCHES * BATCH_SIZE
    );
    let spills = sort.metrics().and_then(|m| m.spill_count()).unwrap_or(0);
    assert!(spills > 0, "the sort must spill");
}

fn criterion_benchmark(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let cases: [(&str, Vec<RecordBatch>, usize); 3] = [
        ("dictionary", dictionary_batches(), 32 * 1024 * 1024),
        ("narrow_utf8", narrow_utf8_batches(), 16 * 1024 * 1024),
        ("wide_utf8", wide_utf8_batches(), 64 * 1024 * 1024),
    ];
    let mut group = c.benchmark_group("sort_spill");
    group.sample_size(10);
    for (name, batches, pool_bytes) in &cases {
        group.bench_with_input(
            BenchmarkId::from_parameter(name),
            batches,
            |b, batches| b.iter(|| sort_spilled(&rt, batches, *pool_bytes)),
        );
    }
    group.finish();
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
