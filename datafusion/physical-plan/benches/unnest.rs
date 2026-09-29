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

use std::hint::black_box;
use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array, StringArray, StructArray};
use arrow::buffer::NullBuffer;
use arrow::datatypes::{Field, Schema};
use arrow::record_batch::RecordBatch;
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use datafusion_common::UnnestOptions;
use datafusion_execution::{TaskContext, config::SessionConfig};
use datafusion_physical_plan::test::TestMemoryExec;
use datafusion_physical_plan::unnest::UnnestExec;
use datafusion_physical_plan::{ExecutionPlan, collect};
use rand::SeedableRng;
use rand::rngs::StdRng;
use rand::seq::SliceRandom;

fn unnest_benchmark(c: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let mut group = c.benchmark_group("unnest_struct");
    for num_rows in [1024, 8192, 65536] {
        let integers = Arc::new(Int64Array::from_iter(
            (0..num_rows).map(|i| (i % 10 != 0).then_some(i as i64)),
        )) as ArrayRef;
        let strings = Arc::new(StringArray::from_iter(
            (0..num_rows).map(|i| (i % 10 != 0).then(|| format!("value-{i:0122}"))),
        )) as ArrayRef;
        let context = Arc::new(
            TaskContext::default()
                .with_session_config(SessionConfig::new().with_batch_size(num_rows)),
        );
        group.throughput(Throughput::Elements(num_rows as u64));
        for (name, child) in [("int64", integers), ("utf8_128_bytes", strings)] {
            for null_percent in [0, 1, 50, 100] {
                let nulls = (null_percent != 0).then(|| {
                    let mut validity = vec![true; num_rows];
                    validity[..num_rows * null_percent / 100].fill(false);
                    validity.shuffle(&mut StdRng::seed_from_u64(42));
                    NullBuffer::from(validity)
                });
                let fields = vec![Field::new("value", child.data_type().clone(), true)];
                let parent = StructArray::new(
                    fields.clone().into(),
                    vec![Arc::clone(&child)],
                    nulls,
                );
                let batch =
                    RecordBatch::try_from_iter(vec![("s", Arc::new(parent) as ArrayRef)])
                        .unwrap();
                let source = TestMemoryExec::try_new_exec(
                    &[vec![batch.clone()]],
                    batch.schema(),
                    None,
                )
                .unwrap();
                let plan: Arc<dyn ExecutionPlan> = Arc::new(
                    UnnestExec::new(
                        source,
                        vec![],
                        vec![0],
                        Arc::new(Schema::new(fields)),
                        UnnestOptions::default(),
                    )
                    .unwrap(),
                );
                group.bench_function(
                    BenchmarkId::new(format!("{name}_nulls_{null_percent}"), num_rows),
                    |b| {
                        b.iter(|| {
                            black_box(
                                runtime
                                    .block_on(collect(
                                        Arc::clone(&plan),
                                        Arc::clone(&context),
                                    ))
                                    .unwrap(),
                            )
                        });
                    },
                );
            }
        }
    }
    group.finish();
}

criterion_group!(benches, unnest_benchmark);
criterion_main!(benches);
