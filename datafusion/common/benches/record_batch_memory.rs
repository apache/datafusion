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

use arrow::array::{ArrayRef, Int64Array, ListArray, StructArray};
use arrow::datatypes::{DataType, Field, Int64Type, Schema};
use arrow::record_batch::RecordBatch;
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use datafusion_common::utils::memory::get_record_batch_memory_size;

fn make_batch(columns: Vec<ArrayRef>) -> RecordBatch {
    let fields = columns
        .iter()
        .enumerate()
        .map(|(index, column)| {
            Field::new(format!("col_{index}"), column.data_type().clone(), false)
        })
        .collect::<Vec<_>>();

    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap()
}

fn make_primitive_batch(num_rows: usize, num_columns: usize) -> RecordBatch {
    let columns = (0..num_columns)
        .map(|index| {
            Arc::new(Int64Array::from_iter_values(
                (0..num_rows).map(|value| value as i64 + index as i64),
            )) as ArrayRef
        })
        .collect::<Vec<_>>();

    make_batch(columns)
}

fn make_list_batch(num_rows: usize, num_columns: usize) -> RecordBatch {
    let columns = (0..num_columns)
        .map(|column| {
            Arc::new(ListArray::from_iter_primitive::<Int64Type, _, _>(
                (0..num_rows).map(|row| {
                    let value = row as i64 + column as i64;
                    Some(vec![Some(value), Some(value + 1)])
                }),
            )) as ArrayRef
        })
        .collect::<Vec<_>>();

    make_batch(columns)
}

fn make_struct_batch(num_rows: usize, num_columns: usize) -> RecordBatch {
    let columns = (0..num_columns)
        .map(|column| {
            let left = Arc::new(Int64Array::from_iter_values(
                (0..num_rows).map(|row| row as i64 + column as i64),
            )) as ArrayRef;
            let right = Arc::new(Int64Array::from_iter_values(
                (0..num_rows).map(|row| row as i64 - column as i64),
            )) as ArrayRef;

            Arc::new(StructArray::from(vec![
                (Arc::new(Field::new("left", DataType::Int64, false)), left),
                (Arc::new(Field::new("right", DataType::Int64, false)), right),
            ])) as ArrayRef
        })
        .collect::<Vec<_>>();

    make_batch(columns)
}

fn benchmark_column_count(c: &mut Criterion) {
    let mut group = c.benchmark_group("record_batch_memory_size/column_count");

    for num_columns in [1, 4, 16, 64] {
        let batch = make_primitive_batch(8192, num_columns);
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

fn benchmark_row_count(c: &mut Criterion) {
    let mut group = c.benchmark_group("record_batch_memory_size/row_count");

    for num_rows in [1, 128, 8192, 65_536] {
        let batch = make_primitive_batch(num_rows, 4);
        group.bench_with_input(
            BenchmarkId::from_parameter(num_rows),
            &batch,
            |bencher, batch| {
                bencher.iter(|| get_record_batch_memory_size(black_box(batch)));
            },
        );
    }

    group.finish();
}

fn benchmark_array_layout(c: &mut Criterion) {
    let mut group = c.benchmark_group("record_batch_memory_size/array_layout");

    for (name, batch) in [
        ("primitive", make_primitive_batch(8192, 4)),
        ("list", make_list_batch(8192, 4)),
        ("struct", make_struct_batch(8192, 4)),
    ] {
        group.bench_with_input(
            BenchmarkId::from_parameter(name),
            &batch,
            |bencher, batch| {
                bencher.iter(|| get_record_batch_memory_size(black_box(batch)));
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    benchmark_column_count,
    benchmark_row_count,
    benchmark_array_layout
);
criterion_main!(benches);
