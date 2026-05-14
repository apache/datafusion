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

//! Benchmarks for range and generate_series functions.

use arrow::array::{ArrayRef, Int64Array};
use arrow::datatypes::{DataType, Field};
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use datafusion_common::{ScalarValue, config::ConfigOptions};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl};
use datafusion_functions_nested::range::{Range, gen_series_udf};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::hint::black_box;
use std::sync::Arc;

const NUM_ROWS: &[usize] = &[100, 1000, 10000];
const INT_STEPS: &[i64] = &[1, 5, 50];
const INT_DIRECTIONS: &[(&str, bool)] = &[("increasing", true), ("decreasing", false)];
/// Each row produces at most RANGE_SIZE elements in its Int64 list
const RANGE_SIZE: i64 = 200;
const SEED: u64 = 42;

fn criterion_benchmark(c: &mut Criterion) {
    bench_range_int64(c);
    bench_generate_series_int64(c);
}

// ---------------------------------------------------------------------------
// Int64 – range(start, stop, step)
// ---------------------------------------------------------------------------
fn bench_range_int64(c: &mut Criterion) {
    let mut group = c.benchmark_group("range_int64");

    for &num_rows in NUM_ROWS {
        for &(direction, increasing) in INT_DIRECTIONS {
            let (start_array, stop_array) =
                make_int64_start_stop_arrays(num_rows, RANGE_SIZE, increasing);

            for &step in INT_STEPS {
                let step = if increasing { step } else { -step };
                let args = vec![
                    ColumnarValue::Array(start_array.clone()),
                    ColumnarValue::Array(stop_array.clone()),
                    ColumnarValue::Scalar(ScalarValue::Int64(Some(step))),
                ];

                group.bench_with_input(
                    BenchmarkId::new(format!("{direction}/step_{step}"), num_rows),
                    &num_rows,
                    |b, _| {
                        let udf = Range::new();
                        b.iter(|| {
                            black_box(
                                udf.invoke_with_args(ScalarFunctionArgs {
                                    args: args.clone(),
                                    arg_fields: vec![
                                        Arc::new(Field::new(
                                            "start",
                                            DataType::Int64,
                                            true,
                                        )),
                                        Arc::new(Field::new(
                                            "stop",
                                            DataType::Int64,
                                            true,
                                        )),
                                        Arc::new(Field::new(
                                            "step",
                                            DataType::Int64,
                                            true,
                                        )),
                                    ],
                                    number_rows: num_rows,
                                    return_field: Arc::new(Field::new(
                                        "result",
                                        DataType::List(Arc::new(Field::new_list_field(
                                            DataType::Int64,
                                            true,
                                        ))),
                                        true,
                                    )),
                                    config_options: Arc::new(ConfigOptions::default()),
                                })
                                .unwrap(),
                            )
                        })
                    },
                );
            }
        }
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// Int64 – generate_series(start, stop, step)
// ---------------------------------------------------------------------------
fn bench_generate_series_int64(c: &mut Criterion) {
    let mut group = c.benchmark_group("generate_series_int64");

    for &num_rows in NUM_ROWS {
        for &(direction, increasing) in INT_DIRECTIONS {
            let (start_array, stop_array) =
                make_int64_start_stop_arrays(num_rows, RANGE_SIZE, increasing);

            for &step in INT_STEPS {
                let step = if increasing { step } else { -step };
                let args = vec![
                    ColumnarValue::Array(start_array.clone()),
                    ColumnarValue::Array(stop_array.clone()),
                    ColumnarValue::Scalar(ScalarValue::Int64(Some(step))),
                ];

                group.bench_with_input(
                    BenchmarkId::new(format!("{direction}/step_{step}"), num_rows),
                    &num_rows,
                    |b, _| {
                        let udf = gen_series_udf();
                        b.iter(|| {
                            black_box(
                                udf.invoke_with_args(ScalarFunctionArgs {
                                    args: args.clone(),
                                    arg_fields: vec![
                                        Arc::new(Field::new(
                                            "start",
                                            DataType::Int64,
                                            true,
                                        )),
                                        Arc::new(Field::new(
                                            "stop",
                                            DataType::Int64,
                                            true,
                                        )),
                                        Arc::new(Field::new(
                                            "step",
                                            DataType::Int64,
                                            true,
                                        )),
                                    ],
                                    number_rows: num_rows,
                                    return_field: Arc::new(Field::new(
                                        "result",
                                        DataType::List(Arc::new(Field::new_list_field(
                                            DataType::Int64,
                                            true,
                                        ))),
                                        true,
                                    )),
                                    config_options: Arc::new(ConfigOptions::default()),
                                })
                                .unwrap(),
                            )
                        })
                    },
                );
            }
        }
    }

    group.finish();
}

/// Build (start, stop) Int64Arrays where each stop = start + offset,
/// with offset in [1, max_range]. This ensures every row produces a
/// bounded-size list, avoiding OOM from unbounded i64 ranges.
fn make_int64_start_stop_arrays(
    num_rows: usize,
    max_range: i64,
    increasing: bool,
) -> (ArrayRef, ArrayRef) {
    let mut rng = StdRng::seed_from_u64(SEED);
    let mut starts: Vec<i64> = Vec::with_capacity(num_rows);
    let mut stops: Vec<i64> = Vec::with_capacity(num_rows);
    for _ in 0..num_rows {
        let s = rng.random_range(0..max_range);
        let offset = rng.random_range(1..=max_range);
        if increasing {
            starts.push(s);
            stops.push(s + offset);
        } else {
            starts.push(s + offset);
            stops.push(s);
        }
    }
    (
        Arc::new(Int64Array::from(starts)),
        Arc::new(Int64Array::from(stops)),
    )
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
