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

use arrow::array::{
    Date32Builder, Decimal128Builder, Int32Builder, RecordBatch, StringBuilder,
};
use arrow::datatypes::{DataType, Field, Schema};
use criterion::{criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion};
use datafusion_execution::runtime_env::RuntimeEnv;
use datafusion_physical_plan::common::collect;
use datafusion_physical_plan::metrics::{ExecutionPlanMetricsSet, SpillMetrics};
use datafusion_physical_plan::SpillManager;
use std::sync::Arc;
use tokio::runtime::Runtime;

pub fn create_batch(num_rows: usize, allow_nulls: bool) -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("c0", DataType::Int32, true),
        Field::new("c1", DataType::Utf8, true),
        Field::new("c2", DataType::Date32, true),
        Field::new("c3", DataType::Decimal128(11, 2), true),
    ]));

    let mut a = Int32Builder::new();
    let mut b = StringBuilder::new();
    let mut c = Date32Builder::new();
    let mut d = Decimal128Builder::new()
        .with_precision_and_scale(11, 2)
        .unwrap();

    for i in 0..num_rows {
        a.append_value(i as i32);
        c.append_value(i as i32);
        d.append_value((i * 1000000) as i128);
        if allow_nulls && i % 10 == 0 {
            b.append_null();
        } else {
            b.append_value(format!("this is string number {i}"));
        }
    }

    let a = a.finish();
    let b = b.finish();
    let c = c.finish();
    let d = d.finish();

    RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(a), Arc::new(b), Arc::new(c), Arc::new(d)],
    )
    .unwrap()
}

// BENCHMARK: REVALIDATION OVERHEAD COMPARISON
// ---------------------------------------------------------
// To compare performance with/without Arrow IPC validation:
//
// 1. Locate the function `read_spill`
// 2. Modify the `skip_validation` flag:
//    - Set to `false` to enable validation
// 3. Rerun `cargo bench --bench spill_io`
fn bench_spill_io(c: &mut Criterion) {
    let env = Arc::new(RuntimeEnv::default());
    let metrics = SpillMetrics::new(&ExecutionPlanMetricsSet::new(), 0);
    let schema = Arc::new(Schema::new(vec![
        Field::new("c0", DataType::Int32, true),
        Field::new("c1", DataType::Utf8, true),
        Field::new("c2", DataType::Date32, true),
        Field::new("c3", DataType::Decimal128(11, 2), true),
    ]));
    let spill_manager = SpillManager::new(env, metrics, schema);

    let mut group = c.benchmark_group("spill_manager");
    let rt = Runtime::new().unwrap();

    group.bench_with_input(
        BenchmarkId::new("StreamReader/read_100", ""),
        &spill_manager,
        |b, spill_manager| {
            b.iter_batched(
                // Setup phase: Create fresh state for each benchmark iteration.
                // - generate an ipc file.
                // This ensures each iteration starts with clean resources.
                || {
                    let batch = create_batch(8192, true);
                    let spill_file = spill_manager
                        .spill_record_batch_and_finish(&vec![batch; 100], "Test")
                        .unwrap()
                        .unwrap();
                    spill_file
                },
                // Benchmark phase:
                // - Execute the read operation via SpillManager
                // - Wait for the consumer to finish processing
                |spill_file| {
                    rt.block_on(async {
                        let stream =
                            spill_manager.read_spill_as_stream(spill_file).unwrap();
                        let _ = collect(stream).await.unwrap();
                    })
                },
                BatchSize::LargeInput,
            )
        },
    );
    group.finish();
}

criterion_group!(benches, bench_spill_io);
criterion_main!(benches);
