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

use arrow::array::{ArrayRef, BooleanBufferBuilder, Int32Array, ListArray, StringArray};
use arrow::buffer::{NullBuffer, OffsetBuffer};
use arrow::datatypes::{DataType, Field};
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use datafusion_common::config::ConfigOptions;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl};
use datafusion_functions_nested::sort::ArraySort;
use rand::SeedableRng;
use rand::rngs::StdRng;
use rand::seq::SliceRandom;

const SEED: u64 = 42;
const NUM_ROWS: usize = 8192;

fn create_int32_list_array(
    num_rows: usize,
    elements_per_row: usize,
    with_nulls: bool,
) -> ArrayRef {
    let mut rng = StdRng::seed_from_u64(SEED);
    let total_values = num_rows * elements_per_row;

    let mut values: Vec<i32> = (0..total_values as i32).collect();
    values.shuffle(&mut rng);

    let values = Arc::new(Int32Array::from(values));
    let offsets: Vec<i32> = (0..=num_rows)
        .map(|i| (i * elements_per_row) as i32)
        .collect();

    let nulls = if with_nulls {
        // Every 10th row is null
        Some(NullBuffer::from(
            (0..num_rows).map(|i| i % 10 != 0).collect::<Vec<bool>>(),
        ))
    } else {
        None
    };

    Arc::new(ListArray::new(
        Arc::new(Field::new("item", DataType::Int32, true)),
        OffsetBuffer::new(offsets.into()),
        values,
        nulls,
    ))
}

/// Creates a ListArray where ~10% of elements within each row are null.
fn create_int32_list_array_with_null_elements(
    num_rows: usize,
    elements_per_row: usize,
) -> ArrayRef {
    let mut rng = StdRng::seed_from_u64(SEED);
    let total_values = num_rows * elements_per_row;

    let mut values: Vec<i32> = (0..total_values as i32).collect();
    values.shuffle(&mut rng);

    // ~10% of elements are null
    let mut validity = BooleanBufferBuilder::new(total_values);
    for i in 0..total_values {
        validity.append(i % 10 != 0);
    }
    let null_buffer = NullBuffer::from(validity.finish());

    let values = Arc::new(Int32Array::new(values.into(), Some(null_buffer)));
    let offsets: Vec<i32> = (0..=num_rows)
        .map(|i| (i * elements_per_row) as i32)
        .collect();

    Arc::new(ListArray::new(
        Arc::new(Field::new("item", DataType::Int32, true)),
        OffsetBuffer::new(offsets.into()),
        values,
        None,
    ))
}

fn create_string_list_array(num_rows: usize, elements_per_row: usize) -> ArrayRef {
    let mut rng = StdRng::seed_from_u64(SEED);
    let total_values = num_rows * elements_per_row;

    let mut indices: Vec<usize> = (0..total_values).collect();
    indices.shuffle(&mut rng);
    let string_values: Vec<String> =
        indices.iter().map(|i| format!("value_{i:06}")).collect();
    let values = Arc::new(StringArray::from(string_values));

    let offsets: Vec<i32> = (0..=num_rows)
        .map(|i| (i * elements_per_row) as i32)
        .collect();

    Arc::new(ListArray::new(
        Arc::new(Field::new("item", DataType::Utf8, true)),
        OffsetBuffer::new(offsets.into()),
        values,
        None,
    ))
}

fn invoke_array_sort(udf: &ArraySort, array: &ArrayRef) -> ColumnarValue {
    udf.invoke_with_args(ScalarFunctionArgs {
        args: vec![ColumnarValue::Array(Arc::clone(array))],
        arg_fields: vec![Field::new("arr", array.data_type().clone(), true).into()],
        number_rows: array.len(),
        return_field: Field::new("result", array.data_type().clone(), true).into(),
        config_options: Arc::new(ConfigOptions::default()),
    })
    .unwrap()
}

/// Vary elements_per_row over [5, 20, 100, 1000]: for small arrays, per-row
/// overhead dominates, whereas for larger arrays the sort kernel dominates.
fn bench_array_sort(c: &mut Criterion) {
    let mut group = c.benchmark_group("array_sort");
    let udf = ArraySort::new();

    // Int32 arrays
    for &elements_per_row in &[5, 20, 100, 1000] {
        let array = create_int32_list_array(NUM_ROWS, elements_per_row, false);
        group.bench_with_input(
            BenchmarkId::new("int32", elements_per_row),
            &elements_per_row,
            |b, _| {
                b.iter(|| {
                    black_box(invoke_array_sort(&udf, &array));
                });
            },
        );
    }

    // Int32 with nulls in the outer list (10% null rows), single size
    {
        let array = create_int32_list_array(NUM_ROWS, 50, true);
        group.bench_function("int32_with_nulls", |b| {
            b.iter(|| {
                black_box(invoke_array_sort(&udf, &array));
            });
        });
    }

    // Int32 with null elements (~10% of elements within rows are null)
    for &elements_per_row in &[5, 20, 100, 1000] {
        let array =
            create_int32_list_array_with_null_elements(NUM_ROWS, elements_per_row);
        group.bench_with_input(
            BenchmarkId::new("int32_null_elements", elements_per_row),
            &elements_per_row,
            |b, _| {
                b.iter(|| {
                    black_box(invoke_array_sort(&udf, &array));
                });
            },
        );
    }

    // String arrays
    for &elements_per_row in &[5, 20, 100, 1000] {
        let array = create_string_list_array(NUM_ROWS, elements_per_row);
        group.bench_with_input(
            BenchmarkId::new("string", elements_per_row),
            &elements_per_row,
            |b, _| {
                b.iter(|| {
                    black_box(invoke_array_sort(&udf, &array));
                });
            },
        );
    }

    group.finish();
}

criterion_group!(benches, bench_array_sort);
criterion_main!(benches);
