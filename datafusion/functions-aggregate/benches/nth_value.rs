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

use arrow::array::{ArrayRef, Int64Array};
use arrow::compute::SortOptions;
use arrow::datatypes::DataType;
use criterion::{BatchSize, BenchmarkId, Criterion, criterion_group, criterion_main};
use datafusion_expr::Accumulator;
use datafusion_functions_aggregate::nth_value::{
    NthValueAccumulator, TrivialNthValueAccumulator,
};
use datafusion_physical_expr::expressions::Column;
use datafusion_physical_expr::{LexOrdering, PhysicalSortExpr};

const N_VALUES: [i64; 6] = [-100, -10, -1, 1, 10, 100];
const BATCH_LEN: usize = 8192;
const UPDATES_PER_ITER: usize = 10;

fn int64_array(values: impl IntoIterator<Item = i64>) -> ArrayRef {
    Arc::new(Int64Array::from_iter_values(values))
}

fn trivial_accumulator(n: i64) -> TrivialNthValueAccumulator {
    TrivialNthValueAccumulator::try_new(n, &DataType::Int64).unwrap()
}

fn ordered_accumulator(n: i64) -> NthValueAccumulator {
    let ordering = LexOrdering::new(vec![PhysicalSortExpr::new(
        Arc::new(Column::new("ordering", 0)),
        SortOptions::default(),
    )])
    .unwrap();
    NthValueAccumulator::try_new(n, &DataType::Int64, &[DataType::Int64], ordering)
        .unwrap()
}

fn nth_value_benchmark(c: &mut Criterion) {
    let values = int64_array(0..BATCH_LEN as i64);
    let mut group = c.benchmark_group("nth_value");

    for n in N_VALUES {
        let n_values = int64_array(std::iter::repeat_n(n, BATCH_LEN));
        let trivial_batch = vec![Arc::clone(&values), Arc::clone(&n_values)];
        group.bench_function(BenchmarkId::new("trivial", format!("n={n}")), |b| {
            b.iter_batched(
                || trivial_accumulator(n),
                |mut accumulator| {
                    for _ in 0..UPDATES_PER_ITER {
                        accumulator.update_batch(&trivial_batch).unwrap();
                    }
                    black_box(accumulator.evaluate().unwrap());
                },
                BatchSize::SmallInput,
            )
        });

        let ordered_batches = (0..UPDATES_PER_ITER)
            .map(|batch_idx| {
                let start = (batch_idx * BATCH_LEN) as i64;
                vec![
                    Arc::clone(&values),
                    Arc::clone(&n_values),
                    int64_array(start..start + BATCH_LEN as i64),
                ]
            })
            .collect::<Vec<_>>();
        group.bench_function(BenchmarkId::new("ordered", format!("n={n}")), |b| {
            b.iter_batched(
                || ordered_accumulator(n),
                |mut accumulator| {
                    for batch in &ordered_batches {
                        accumulator.update_batch(batch).unwrap();
                    }
                    black_box(accumulator.evaluate().unwrap());
                },
                BatchSize::SmallInput,
            )
        });
    }

    group.finish();
}

criterion_group!(benches, nth_value_benchmark);
criterion_main!(benches);
