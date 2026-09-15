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

use arrow::array::{ArrayRef, Float64Array, Int64Array, ListArray};
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::{DataType, Field};
use criterion::{
    criterion_group, criterion_main, {BenchmarkId, Criterion},
};
use datafusion_common::config::ConfigOptions;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl};
use datafusion_functions_nested::except::ArrayExcept;
use datafusion_functions_nested::set_ops::{ArrayDistinct, ArrayIntersect, ArrayUnion};
use rand::SeedableRng;
use rand::prelude::SliceRandom;
use rand::rngs::StdRng;
use std::collections::HashSet;
use std::hint::black_box;
use std::sync::Arc;

const NUM_ROWS: usize = 1000;
const ARRAY_SIZES: &[usize] = &[10, 50, 100];
const SEED: u64 = 42;
/// Extra rows on each side when building sliced arrays, so the underlying
/// values buffer is much larger than the visible portion.
const SLICE_PADDING: usize = 5000;
/// Keep the visible slice at two values while varying the backing array from
/// 8 KiB to 8 MiB.
const SLICED_FLOAT_BACKING_VALUES: &[usize] = &[1024, 1024 * 1024];
/// Keep the backing array at 8 MiB while varying the visible slice.
const SLICED_FLOAT_VISIBLE_VALUES: &[usize] = &[2, 2048];

fn criterion_benchmark(c: &mut Criterion) {
    bench_array_union(c);
    bench_array_intersect(c);
    bench_array_except(c);
    bench_array_distinct(c);
    bench_array_union_sliced(c);
    bench_array_intersect_sliced(c);
    bench_array_distinct_sliced(c);
    bench_array_distinct_sliced_float(c);
    bench_array_except_sliced(c);
}

fn invoke_udf(udf: &impl ScalarUDFImpl, array1: &ArrayRef, array2: &ArrayRef) {
    black_box(
        udf.invoke_with_args(ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(array1.clone()),
                ColumnarValue::Array(array2.clone()),
            ],
            arg_fields: vec![
                Field::new("arr1", array1.data_type().clone(), false).into(),
                Field::new("arr2", array2.data_type().clone(), false).into(),
            ],
            number_rows: NUM_ROWS,
            return_field: Field::new("result", array1.data_type().clone(), false).into(),
            config_options: Arc::new(ConfigOptions::default()),
        })
        .unwrap(),
    );
}

fn invoke_unary_udf(
    udf: &impl ScalarUDFImpl,
    array: &ArrayRef,
    number_rows: usize,
) -> ColumnarValue {
    black_box(
        udf.invoke_with_args(ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(array.clone())],
            arg_fields: vec![Field::new("arr", array.data_type().clone(), false).into()],
            number_rows,
            return_field: Field::new("result", array.data_type().clone(), false).into(),
            config_options: Arc::new(ConfigOptions::default()),
        })
        .unwrap(),
    )
}

fn bench_array_union(c: &mut Criterion) {
    let mut group = c.benchmark_group("array_union");
    let udf = ArrayUnion::new();

    for (overlap_label, overlap_ratio) in &[("high_overlap", 0.8), ("low_overlap", 0.2)] {
        for &array_size in ARRAY_SIZES {
            let (array1, array2) =
                create_arrays_with_overlap(NUM_ROWS, array_size, *overlap_ratio);
            group.bench_with_input(
                BenchmarkId::new(*overlap_label, array_size),
                &array_size,
                |b, _| b.iter(|| invoke_udf(&udf, &array1, &array2)),
            );
        }
    }

    group.finish();
}

fn bench_array_intersect(c: &mut Criterion) {
    let mut group = c.benchmark_group("array_intersect");
    let udf = ArrayIntersect::new();

    for (overlap_label, overlap_ratio) in &[("high_overlap", 0.8), ("low_overlap", 0.2)] {
        for &array_size in ARRAY_SIZES {
            let (array1, array2) =
                create_arrays_with_overlap(NUM_ROWS, array_size, *overlap_ratio);
            group.bench_with_input(
                BenchmarkId::new(*overlap_label, array_size),
                &array_size,
                |b, _| b.iter(|| invoke_udf(&udf, &array1, &array2)),
            );
        }
    }

    group.finish();
}

fn bench_array_except(c: &mut Criterion) {
    let mut group = c.benchmark_group("array_except");
    let udf = ArrayExcept::new();

    for (overlap_label, overlap_ratio) in &[("high_overlap", 0.8), ("low_overlap", 0.2)] {
        for &array_size in ARRAY_SIZES {
            let (array1, array2) =
                create_arrays_with_overlap(NUM_ROWS, array_size, *overlap_ratio);
            group.bench_with_input(
                BenchmarkId::new(*overlap_label, array_size),
                &array_size,
                |b, _| b.iter(|| invoke_udf(&udf, &array1, &array2)),
            );
        }
    }

    group.finish();
}

fn bench_array_distinct(c: &mut Criterion) {
    let mut group = c.benchmark_group("array_distinct");
    let udf = ArrayDistinct::new();

    for (duplicate_label, duplicate_ratio) in
        &[("high_duplicate", 0.8), ("low_duplicate", 0.2)]
    {
        for &array_size in ARRAY_SIZES {
            let array =
                create_array_with_duplicates(NUM_ROWS, array_size, *duplicate_ratio);
            group.bench_with_input(
                BenchmarkId::new(*duplicate_label, array_size),
                &array_size,
                |b, _| b.iter(|| invoke_unary_udf(&udf, &array, NUM_ROWS)),
            );
        }
    }

    group.finish();
}

fn create_arrays_with_overlap(
    num_rows: usize,
    array_size: usize,
    overlap_ratio: f64,
) -> (ArrayRef, ArrayRef) {
    assert!((0.0..=1.0).contains(&overlap_ratio));
    let overlap_count = ((array_size as f64) * overlap_ratio).round() as usize;

    let mut rng = StdRng::seed_from_u64(SEED);

    let mut values1 = Vec::with_capacity(num_rows * array_size);
    let mut values2 = Vec::with_capacity(num_rows * array_size);

    for row in 0..num_rows {
        let base = (row as i64) * (array_size as i64) * 2;

        for i in 0..array_size {
            values1.push(base + i as i64);
        }

        let mut positions: Vec<usize> = (0..array_size).collect();
        positions.shuffle(&mut rng);

        let overlap_positions: HashSet<_> =
            positions[..overlap_count].iter().copied().collect();

        for i in 0..array_size {
            if overlap_positions.contains(&i) {
                values2.push(base + i as i64);
            } else {
                values2.push(base + array_size as i64 + i as i64);
            }
        }
    }

    let values1 = Int64Array::from(values1);
    let values2 = Int64Array::from(values2);

    let field = Arc::new(Field::new("item", DataType::Int64, true));

    let offsets = (0..=num_rows)
        .map(|i| (i * array_size) as i32)
        .collect::<Vec<i32>>();

    let array1 = Arc::new(
        ListArray::try_new(
            field.clone(),
            OffsetBuffer::new(offsets.clone().into()),
            Arc::new(values1),
            None,
        )
        .unwrap(),
    );

    let array2 = Arc::new(
        ListArray::try_new(
            field,
            OffsetBuffer::new(offsets.into()),
            Arc::new(values2),
            None,
        )
        .unwrap(),
    );

    (array1, array2)
}

fn create_array_with_duplicates(
    num_rows: usize,
    array_size: usize,
    duplicate_ratio: f64,
) -> ArrayRef {
    assert!((0.0..=1.0).contains(&duplicate_ratio));
    let unique_count = ((array_size as f64) * (1.0 - duplicate_ratio)).round() as usize;
    let duplicate_count = array_size - unique_count;

    let mut rng = StdRng::seed_from_u64(SEED);
    let mut values = Vec::with_capacity(num_rows * array_size);

    for row in 0..num_rows {
        let base = (row as i64) * (array_size as i64) * 2;

        // Add unique values first
        for i in 0..unique_count {
            values.push(base + i as i64);
        }

        // Fill the rest with duplicates randomly picked from the unique values
        let mut unique_indices: Vec<i64> =
            (0..unique_count).map(|i| base + i as i64).collect();
        unique_indices.shuffle(&mut rng);

        for i in 0..duplicate_count {
            values.push(unique_indices[i % unique_count]);
        }
    }

    let values = Int64Array::from(values);
    let field = Arc::new(Field::new("item", DataType::Int64, true));

    let offsets = (0..=num_rows)
        .map(|i| (i * array_size) as i32)
        .collect::<Vec<i32>>();

    Arc::new(
        ListArray::try_new(
            field,
            OffsetBuffer::new(offsets.into()),
            Arc::new(values),
            None,
        )
        .unwrap(),
    )
}

/// Slice a pair of arrays to the middle `NUM_ROWS` rows from a larger array.
fn slice_pair(arrays: &(ArrayRef, ArrayRef)) -> (ArrayRef, ArrayRef) {
    let a1 = arrays.0.slice(SLICE_PADDING, NUM_ROWS);
    let a2 = arrays.1.slice(SLICE_PADDING, NUM_ROWS);
    (a1, a2)
}

fn bench_array_union_sliced(c: &mut Criterion) {
    let mut group = c.benchmark_group("array_union_sliced");
    let udf = ArrayUnion::new();

    for &array_size in ARRAY_SIZES {
        let (a1, a2) = slice_pair(&create_arrays_with_overlap(
            NUM_ROWS + 2 * SLICE_PADDING,
            array_size,
            0.5,
        ));
        group.bench_with_input(
            BenchmarkId::from_parameter(array_size),
            &array_size,
            |b, _| b.iter(|| invoke_udf(&udf, &a1, &a2)),
        );
    }
    group.finish();
}

fn bench_array_intersect_sliced(c: &mut Criterion) {
    let mut group = c.benchmark_group("array_intersect_sliced");
    let udf = ArrayIntersect::new();

    for &array_size in ARRAY_SIZES {
        let (a1, a2) = slice_pair(&create_arrays_with_overlap(
            NUM_ROWS + 2 * SLICE_PADDING,
            array_size,
            0.5,
        ));
        group.bench_with_input(
            BenchmarkId::from_parameter(array_size),
            &array_size,
            |b, _| b.iter(|| invoke_udf(&udf, &a1, &a2)),
        );
    }
    group.finish();
}

fn bench_array_except_sliced(c: &mut Criterion) {
    let mut group = c.benchmark_group("array_except_sliced");
    let udf = ArrayExcept::new();

    for &array_size in ARRAY_SIZES {
        let (a1, a2) = slice_pair(&create_arrays_with_overlap(
            NUM_ROWS + 2 * SLICE_PADDING,
            array_size,
            0.5,
        ));
        group.bench_with_input(
            BenchmarkId::from_parameter(array_size),
            &array_size,
            |b, _| b.iter(|| invoke_udf(&udf, &a1, &a2)),
        );
    }
    group.finish();
}

fn bench_array_distinct_sliced(c: &mut Criterion) {
    let mut group = c.benchmark_group("array_distinct_sliced");
    let udf = ArrayDistinct::new();

    for &array_size in ARRAY_SIZES {
        let array =
            create_array_with_duplicates(NUM_ROWS + 2 * SLICE_PADDING, array_size, 0.5)
                .slice(SLICE_PADDING, NUM_ROWS);
        group.bench_with_input(
            BenchmarkId::from_parameter(array_size),
            &array_size,
            |b, _| b.iter(|| invoke_unary_udf(&udf, &array, NUM_ROWS)),
        );
    }
    group.finish();
}

fn create_sliced_float_array(backing_values: usize, visible_values: usize) -> ArrayRef {
    assert!(visible_values > 0 && visible_values <= backing_values);

    let values = Float64Array::from(
        (0..backing_values)
            .map(|i| if i.is_multiple_of(2) { -0.0 } else { 0.0 })
            .collect::<Vec<_>>(),
    );
    let left_padding = (backing_values - visible_values) / 2;
    let offsets = vec![
        0,
        left_padding as i32,
        (left_padding + visible_values) as i32,
        backing_values as i32,
    ];
    let array = ListArray::try_new(
        Arc::new(Field::new("item", DataType::Float64, true)),
        OffsetBuffer::new(offsets.into()),
        Arc::new(values),
        None,
    )
    .unwrap();

    Arc::new(array.slice(1, 1))
}

fn create_unsliced_float_array(value_count: usize) -> ArrayRef {
    let values = Float64Array::from(
        (0..value_count)
            .map(|i| if i.is_multiple_of(2) { -0.0 } else { 0.0 })
            .collect::<Vec<_>>(),
    );
    Arc::new(ListArray::new(
        Arc::new(Field::new("item", DataType::Float64, true)),
        OffsetBuffer::new(vec![0, value_count as i32].into()),
        Arc::new(values),
        None,
    ))
}

/// Keep the visible list fixed at one `-0.0` and one `0.0` while increasing
/// the backing values buffer. This isolates work outside the logical slice.
fn bench_array_distinct_sliced_float(c: &mut Criterion) {
    let mut group = c.benchmark_group("array_distinct_sliced_float");
    let udf = ArrayDistinct::new();

    for &backing_values in SLICED_FLOAT_BACKING_VALUES {
        let array = create_sliced_float_array(backing_values, 2);
        group.bench_with_input(
            BenchmarkId::new("backing_values", backing_values),
            &backing_values,
            |b, _| b.iter(|| invoke_unary_udf(&udf, &array, 1)),
        );
    }

    for &visible_values in SLICED_FLOAT_VISIBLE_VALUES {
        let array = create_sliced_float_array(1024 * 1024, visible_values);
        group.bench_with_input(
            BenchmarkId::new("visible_values", visible_values),
            &visible_values,
            |b, _| b.iter(|| invoke_unary_udf(&udf, &array, 1)),
        );
    }

    let array = create_unsliced_float_array(1024);
    group.bench_function("unsliced_values_1024", |b| {
        b.iter(|| invoke_unary_udf(&udf, &array, 1))
    });
    group.finish();
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
