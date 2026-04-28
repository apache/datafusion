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
    Array, ArrayBuilder, ArrayRef, BooleanBuilder, FixedSizeBinaryArray, Int64Builder,
    ListArray, ListBuilder, StringBuilder,
};
use arrow::buffer::{NullBuffer, OffsetBuffer};
use arrow::datatypes::{DataType, Field};
use criterion::{
    criterion_group, criterion_main, {BenchmarkId, Criterion},
};
use datafusion_common::ScalarValue;
use datafusion_common::config::ConfigOptions;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl};
use datafusion_functions_nested::remove::{ArrayRemove, ArrayRemoveAll, ArrayRemoveN};
use rand::Rng;
use rand::SeedableRng;
use rand::rngs::StdRng;
use rand::seq::IndexedRandom;
use std::hint::black_box;
use std::sync::Arc;

// (num_rows, list_size)
// Settings tuned so benchmarks finish in approx 5 seconds
const SIZES: &[(usize, usize)] = &[(4_000, 10), (10_000, 100), (10_000, 500)];
const NESTED_SIZES: &[(usize, usize)] = &[(4_000, 10), (3_000, 100), (1_500, 300)];
const SEED: u64 = 42;
const HAYSTACK_NULL_DENSITY: f64 = 0.1;
const NEEDLE_DENSITY: f64 = 0.1;

fn criterion_benchmark(c: &mut Criterion) {
    bench_array_remove_int64(c);
    bench_array_remove_n_int64(c);
    bench_array_remove_all_int64(c);

    bench_array_remove_int64_nested(c);
    bench_array_remove_n_int64_nested(c);
    bench_array_remove_all_int64_nested(c);

    bench_array_remove_strings(c);
    bench_array_remove_boolean(c);
    bench_array_remove_fixed_size_binary(c);
}

fn bench_array_remove_int64(c: &mut Criterion) {
    let mut group = c.benchmark_group("array_remove_int64");

    let filler_values = [None, Some(1), Some(2), Some(3), Some(4), Some(5)];
    let needle = 0;
    for &(num_rows, list_size) in SIZES {
        let list_array = create_list_array::<Int64Builder, _>(
            num_rows,
            list_size,
            needle,
            &filler_values,
        );
        group.bench_with_input(
            BenchmarkId::new(
                "remove",
                format!("list size: {list_size}, num_rows: {num_rows}"),
            ),
            &(list_size, num_rows),
            |b, _| {
                let udf = ArrayRemove::new();
                b.iter(|| {
                    let args = create_args(list_array.clone(), ScalarValue::from(needle));
                    black_box(udf.invoke_with_args(args).unwrap())
                })
            },
        );
    }

    group.finish();
}

fn bench_array_remove_n_int64(c: &mut Criterion) {
    let mut group = c.benchmark_group("array_remove_n_int64");

    let filler_values = [None, Some(1), Some(2), Some(3), Some(4), Some(5)];
    let needle = 0;
    for &(num_rows, list_size) in SIZES {
        let list_array = create_list_array::<Int64Builder, _>(
            num_rows,
            list_size,
            needle,
            &filler_values,
        );
        let n = (NEEDLE_DENSITY / 2.0 * list_size as f64) as i64;
        let n = 2.max(n);

        group.bench_with_input(
            BenchmarkId::new(
                "remove",
                format!("list size: {list_size}, num_rows: {num_rows}"),
            ),
            &(list_size, num_rows),
            |b, _| {
                let udf = ArrayRemoveN::new();
                b.iter(|| {
                    let args = create_args_n(
                        list_array.clone(),
                        ScalarValue::from(needle),
                        ScalarValue::from(n),
                    );
                    black_box(udf.invoke_with_args(args).unwrap())
                })
            },
        );
    }

    group.finish();
}

fn bench_array_remove_all_int64(c: &mut Criterion) {
    let mut group = c.benchmark_group("array_remove_all_int64");

    let filler_values = [None, Some(1), Some(2), Some(3), Some(4), Some(5)];
    let needle = 0;
    for &(num_rows, list_size) in SIZES {
        let list_array = create_list_array::<Int64Builder, _>(
            num_rows,
            list_size,
            needle,
            &filler_values,
        );
        group.bench_with_input(
            BenchmarkId::new(
                "remove",
                format!("list size: {list_size}, num_rows: {num_rows}"),
            ),
            &(list_size, num_rows),
            |b, _| {
                let udf = ArrayRemoveAll::new();
                b.iter(|| {
                    let args = create_args(list_array.clone(), ScalarValue::from(needle));
                    black_box(udf.invoke_with_args(args).unwrap())
                })
            },
        );
    }

    group.finish();
}

fn bench_array_remove_int64_nested(c: &mut Criterion) {
    let mut group = c.benchmark_group("array_remove_int64_nested");

    let filler_values = [
        None,
        Some(vec![Some(1), Some(0), Some(2), Some(0)]),
        Some(vec![Some(1)]),
        Some(vec![]),
        Some(vec![Some(1), Some(0), Some(2), Some(4), None]),
        Some(vec![None]),
    ];
    let needle = vec![Some(1), Some(0), Some(2), Some(4)];
    let needle_scalar = needle
        .iter()
        .copied()
        .map(ScalarValue::from)
        .collect::<Vec<_>>();
    let needle_scalar = ScalarValue::List(ScalarValue::new_list_nullable(
        &needle_scalar,
        &DataType::Int64,
    ));
    for &(num_rows, list_size) in NESTED_SIZES {
        let list_array =
            create_nested_i64_list_array(num_rows, list_size, &needle, &filler_values);
        group.bench_with_input(
            BenchmarkId::new(
                "remove",
                format!("list size: {list_size}, num_rows: {num_rows}"),
            ),
            &(list_size, num_rows),
            |b, _| {
                let udf = ArrayRemove::new();
                b.iter(|| {
                    let args = create_args(list_array.clone(), needle_scalar.clone());
                    black_box(udf.invoke_with_args(args).unwrap())
                })
            },
        );
    }

    group.finish();
}

fn bench_array_remove_n_int64_nested(c: &mut Criterion) {
    let mut group = c.benchmark_group("array_remove_n_int64_nested");

    let filler_values = [
        None,
        Some(vec![Some(1), Some(0), Some(2), Some(0)]),
        Some(vec![Some(1)]),
        Some(vec![]),
        Some(vec![Some(1), Some(0), Some(2), Some(4), None]),
        Some(vec![None]),
    ];
    let needle = vec![Some(1), Some(0), Some(2), Some(4)];
    let needle_scalar = needle
        .iter()
        .copied()
        .map(ScalarValue::from)
        .collect::<Vec<_>>();
    let needle_scalar = ScalarValue::List(ScalarValue::new_list_nullable(
        &needle_scalar,
        &DataType::Int64,
    ));
    for &(num_rows, list_size) in NESTED_SIZES {
        let list_array =
            create_nested_i64_list_array(num_rows, list_size, &needle, &filler_values);
        let n = (NEEDLE_DENSITY / 2.0 * list_size as f64) as i64;
        let n = 2.max(n);
        group.bench_with_input(
            BenchmarkId::new(
                "remove",
                format!("list size: {list_size}, num_rows: {num_rows}"),
            ),
            &(list_size, num_rows),
            |b, _| {
                let udf = ArrayRemoveN::new();
                b.iter(|| {
                    let args = create_args_n(
                        list_array.clone(),
                        needle_scalar.clone(),
                        ScalarValue::from(n),
                    );
                    black_box(udf.invoke_with_args(args).unwrap())
                })
            },
        );
    }

    group.finish();
}

fn bench_array_remove_all_int64_nested(c: &mut Criterion) {
    let mut group = c.benchmark_group("array_remove_all_int64_nested");

    let filler_values = [
        None,
        Some(vec![Some(1), Some(0), Some(2), Some(0)]),
        Some(vec![Some(1)]),
        Some(vec![]),
        Some(vec![Some(1), Some(0), Some(2), Some(4), None]),
        Some(vec![None]),
    ];
    let needle = vec![Some(1), Some(0), Some(2), Some(4)];
    let needle_scalar = needle
        .iter()
        .copied()
        .map(ScalarValue::from)
        .collect::<Vec<_>>();
    let needle_scalar = ScalarValue::List(ScalarValue::new_list_nullable(
        &needle_scalar,
        &DataType::Int64,
    ));
    for &(num_rows, list_size) in NESTED_SIZES {
        let list_array =
            create_nested_i64_list_array(num_rows, list_size, &needle, &filler_values);
        group.bench_with_input(
            BenchmarkId::new(
                "remove",
                format!("list size: {list_size}, num_rows: {num_rows}"),
            ),
            &(list_size, num_rows),
            |b, _| {
                let udf = ArrayRemoveAll::new();
                b.iter(|| {
                    let args = create_args(list_array.clone(), needle_scalar.clone());
                    black_box(udf.invoke_with_args(args).unwrap())
                })
            },
        );
    }

    group.finish();
}

fn bench_array_remove_strings(c: &mut Criterion) {
    let mut group = c.benchmark_group("array_remove_strings");

    let filler_values = [
        None,
        Some("neenee"),
        Some("notthis"),
        Some("value1"),
        Some("abc"),
        Some("hello"),
    ];
    let needle = "needle";
    for &(num_rows, list_size) in SIZES {
        let list_array = create_list_array::<StringBuilder, _>(
            num_rows,
            list_size,
            needle,
            &filler_values,
        );
        group.bench_with_input(
            BenchmarkId::new(
                "remove",
                format!("list size: {list_size}, num_rows: {num_rows}"),
            ),
            &(list_size, num_rows),
            |b, _| {
                let udf = ArrayRemove::new();
                b.iter(|| {
                    let args = create_args(list_array.clone(), ScalarValue::from(needle));
                    black_box(udf.invoke_with_args(args).unwrap())
                })
            },
        );
    }

    group.finish();
}

fn bench_array_remove_boolean(c: &mut Criterion) {
    let mut group = c.benchmark_group("array_remove_boolean");

    let filler_values = [None, Some(false)];
    let needle = true;
    for &(num_rows, list_size) in SIZES {
        let list_array = create_list_array::<BooleanBuilder, _>(
            num_rows,
            list_size,
            needle,
            &filler_values,
        );
        group.bench_with_input(
            BenchmarkId::new(
                "remove",
                format!("list size: {list_size}, num_rows: {num_rows}"),
            ),
            &(list_size, num_rows),
            |b, _| {
                let udf = ArrayRemove::new();
                b.iter(|| {
                    let args = create_args(list_array.clone(), ScalarValue::from(needle));
                    black_box(udf.invoke_with_args(args).unwrap())
                })
            },
        );
    }

    group.finish();
}

fn bench_array_remove_fixed_size_binary(c: &mut Criterion) {
    let mut group = c.benchmark_group("array_remove_fixed_size_binary");

    const SIZE: usize = 16;
    let filler_values = [
        None,
        Some([2_u8; SIZE]),
        Some([3_u8; SIZE]),
        Some([4_u8; SIZE]),
        Some([5_u8; SIZE]),
        Some([6_u8; SIZE]),
    ];
    let needle = [1_u8; SIZE];
    for &(num_rows, list_size) in SIZES {
        let list_array = create_fixed_size_binary_list_array::<SIZE>(
            num_rows,
            list_size,
            needle,
            &filler_values,
        );
        group.bench_with_input(
            BenchmarkId::new(
                "remove",
                format!("list size: {list_size}, num_rows: {num_rows}"),
            ),
            &(list_size, num_rows),
            |b, _| {
                let udf = ArrayRemove::new();
                b.iter(|| {
                    let args = create_args(
                        list_array.clone(),
                        ScalarValue::FixedSizeBinary(SIZE as i32, Some(needle.to_vec())),
                    );
                    black_box(udf.invoke_with_args(args).unwrap())
                })
            },
        );
    }

    group.finish();
}

#[inline]
fn create_args(haystack: ArrayRef, needle: ScalarValue) -> ScalarFunctionArgs {
    let number_rows = haystack.len();
    let haystack_type = haystack.data_type().clone();
    let needle_type = needle.data_type().clone();
    ScalarFunctionArgs {
        args: vec![
            ColumnarValue::Array(haystack),
            ColumnarValue::Scalar(needle),
        ],
        arg_fields: vec![
            Field::new("haystack", haystack_type.clone(), true).into(),
            Field::new("needle", needle_type, true).into(),
        ],
        number_rows,
        return_field: Field::new("result", haystack_type, true).into(),
        config_options: Arc::new(ConfigOptions::default()),
    }
}

#[inline]
fn create_args_n(
    haystack: ArrayRef,
    needle: ScalarValue,
    n: ScalarValue,
) -> ScalarFunctionArgs {
    let number_rows = haystack.len();
    let haystack_type = haystack.data_type().clone();
    let needle_type = needle.data_type().clone();
    let n_type = n.data_type().clone();
    ScalarFunctionArgs {
        args: vec![
            ColumnarValue::Array(haystack),
            ColumnarValue::Scalar(needle),
            ColumnarValue::Scalar(n),
        ],
        arg_fields: vec![
            Field::new("haystack", haystack_type.clone(), true).into(),
            Field::new("needle", needle_type, true).into(),
            Field::new("n", n_type, true).into(),
        ],
        number_rows,
        return_field: Field::new("result", haystack_type, true).into(),
        config_options: Arc::new(ConfigOptions::default()),
    }
}

fn create_list_array<Builder, Item>(
    num_rows: usize,
    list_size: usize,
    needle_value: Item,
    filler_values: &[Option<Item>],
) -> ArrayRef
where
    Builder: ArrayBuilder + Default + Extend<Option<Item>>,
    Item: Copy,
{
    let mut rng = StdRng::seed_from_u64(SEED);
    let values = (0..num_rows)
        .map(|_| {
            if rng.random_bool(HAYSTACK_NULL_DENSITY) {
                None
            } else {
                let list = (0..list_size)
                    .map(|_| {
                        if rng.random_bool(NEEDLE_DENSITY) {
                            Some(needle_value)
                        } else {
                            *filler_values.choose(&mut rng).unwrap()
                        }
                    })
                    .collect::<Vec<_>>();
                Some(list)
            }
        })
        .collect::<Vec<_>>();
    Arc::new(ListArray::from_nested_iter::<Builder, _, _, _>(values))
}

fn create_fixed_size_binary_list_array<const SIZE: usize>(
    num_rows: usize,
    list_size: usize,
    needle_value: [u8; SIZE],
    filler_values: &[Option<[u8; SIZE]>],
) -> ArrayRef {
    let mut rng = StdRng::seed_from_u64(SEED);
    let mut buffer = Vec::with_capacity(num_rows * list_size);
    for _ in 0..num_rows {
        for _ in 0..list_size {
            if rng.random_bool(NEEDLE_DENSITY) {
                buffer.push(Some(needle_value));
            } else {
                buffer.push(*filler_values.choose(&mut rng).unwrap());
            }
        }
    }
    let values = FixedSizeBinaryArray::try_from_sparse_iter_with_size(
        buffer.into_iter(),
        SIZE as i32,
    )
    .unwrap();

    let null_buffer = NullBuffer::from_iter(
        (0..num_rows).map(|_| rng.random_bool(1.0 - HAYSTACK_NULL_DENSITY)),
    );

    Arc::new(ListArray::new(
        Field::new("item", DataType::FixedSizeBinary(SIZE as i32), true).into(),
        OffsetBuffer::from_repeated_length(list_size, num_rows),
        Arc::new(values),
        Some(null_buffer),
    ))
}

fn create_nested_i64_list_array(
    num_rows: usize,
    list_size: usize,
    needle_value: &[Option<i64>],
    filler_values: &[Option<Vec<Option<i64>>>],
) -> ArrayRef {
    let mut rng = StdRng::seed_from_u64(SEED);

    let value_builder = Int64Builder::new();
    let inner_builder = ListBuilder::new(value_builder);
    let mut outer_builder = ListBuilder::new(inner_builder);

    for _ in 0..num_rows {
        if rng.random_bool(HAYSTACK_NULL_DENSITY) {
            outer_builder.append(false);
            continue;
        }

        for _ in 0..list_size {
            let inner = outer_builder.values();
            if rng.random_bool(NEEDLE_DENSITY) {
                inner.append_value(needle_value.to_vec());
            } else {
                inner.append_option(filler_values.choose(&mut rng).unwrap().clone());
            }
        }
        outer_builder.append(true);
    }

    Arc::new(outer_builder.finish())
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
