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

use arrow::array::{ArrayRef, Float64Array};
use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
use datafusion_expr::Accumulator;
use datafusion_functions_aggregate::variance::VarianceAccumulator;
use datafusion_functions_aggregate_common::stats::StatsType;

const BATCH_SIZE: usize = 8192;

fn batch_array(null_stride: Option<usize>) -> ArrayRef {
    let values = (0..BATCH_SIZE)
        .map(|idx| {
            if null_stride.is_some_and(|stride| idx % stride == 0) {
                None
            } else {
                Some(idx as f64)
            }
        })
        .collect::<Vec<_>>();
    Arc::new(Float64Array::from(values)) as ArrayRef
}

fn update_bench(c: &mut Criterion, name: &str, batch: &ArrayRef) {
    c.bench_function(name, |b| {
        b.iter(|| {
            let mut acc = VarianceAccumulator::try_new(StatsType::Sample).unwrap();
            acc.update_batch(std::slice::from_ref(batch)).unwrap();
            black_box(acc.evaluate().unwrap())
        })
    });
}

fn retract_bench(c: &mut Criterion, name: &str, batch: &ArrayRef) {
    c.bench_function(name, |b| {
        b.iter_batched(
            || {
                let mut acc = VarianceAccumulator::try_new(StatsType::Sample).unwrap();
                // Accumulate two batches so that retracting one leaves the
                // accumulator with rows remaining, as in a sliding window.
                acc.update_batch(std::slice::from_ref(batch)).unwrap();
                acc.update_batch(std::slice::from_ref(batch)).unwrap();
                acc
            },
            |mut acc| {
                acc.retract_batch(std::slice::from_ref(batch)).unwrap();
                black_box(acc.evaluate().unwrap())
            },
            BatchSize::SmallInput,
        )
    });
}

fn variance_benchmark(c: &mut Criterion) {
    let no_nulls = batch_array(None);
    let with_nulls = batch_array(Some(10));

    update_bench(c, "variance update_batch f64 no_nulls", &no_nulls);
    update_bench(c, "variance update_batch f64 with_nulls", &with_nulls);
    retract_bench(c, "variance retract_batch f64 no_nulls", &no_nulls);
    retract_bench(c, "variance retract_batch f64 with_nulls", &with_nulls);
}

criterion_group!(benches, variance_benchmark);
criterion_main!(benches);
