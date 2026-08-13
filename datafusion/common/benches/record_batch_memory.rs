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
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use datafusion_common::utils::memory::get_record_batch_memory_size;

fn make_batch(num_columns: usize) -> RecordBatch {
    let fields = (0..num_columns)
        .map(|index| Field::new(format!("col_{index}"), DataType::Int64, false))
        .collect::<Vec<_>>();
    let columns = (0..num_columns)
        .map(|index| {
            Arc::new(Int64Array::from_iter_values(
                (0..8192).map(|value| value + index as i64),
            )) as ArrayRef
        })
        .collect::<Vec<_>>();

    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap()
}

fn benchmark_record_batch_memory_size(c: &mut Criterion) {
    let mut group = c.benchmark_group("record_batch_memory_size");

    for num_columns in [1, 4, 16, 64] {
        let batch = make_batch(num_columns);
        group.bench_with_input(
            BenchmarkId::from_parameter(num_columns),
            &batch,
            |bencher, batch| {
                bencher.iter(|| get_record_batch_memory_size(black_box(batch)));
            },
        );
    }

    group.finish();
}

criterion_group!(benches, benchmark_record_batch_memory_size);
criterion_main!(benches);
