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

//! Microbenchmark for `round(float_array, scalar_decimal_places)` over a
//! Float column with no NULLs — the dense elementwise-rounding path.

use arrow::array::ArrayRef;
use arrow::datatypes::{DataType, Field, Float32Type, Float64Type};
use arrow::util::bench_util::create_primitive_array;
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use datafusion_common::ScalarValue;
use datafusion_common::config::ConfigOptions;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl};
use datafusion_functions::math::round::RoundFunc;
use std::hint::black_box;
use std::sync::Arc;

fn criterion_benchmark(c: &mut Criterion) {
    let round_fn = RoundFunc::new();
    let config_options = Arc::new(ConfigOptions::default());

    for size in [1024usize, 4096, 8192] {
        // Float64, no nulls.
        let f64_array: ArrayRef =
            Arc::new(create_primitive_array::<Float64Type>(size, 0.0));
        let f64_args = vec![
            ColumnarValue::Array(Arc::clone(&f64_array)),
            ColumnarValue::Scalar(ScalarValue::Int32(Some(2))),
        ];
        c.bench_with_input(BenchmarkId::new("round_dense_f64", size), &size, |b, _| {
            b.iter(|| {
                black_box(
                    round_fn
                        .invoke_with_args(ScalarFunctionArgs {
                            args: f64_args.clone(),
                            arg_fields: vec![
                                Field::new("a", DataType::Float64, false).into(),
                                Field::new("b", DataType::Int32, false).into(),
                            ],
                            number_rows: size,
                            return_field: Field::new("f", DataType::Float64, false)
                                .into(),
                            config_options: Arc::clone(&config_options),
                        })
                        .unwrap(),
                )
            })
        });

        // Float32, no nulls.
        let f32_array: ArrayRef =
            Arc::new(create_primitive_array::<Float32Type>(size, 0.0));
        let f32_args = vec![
            ColumnarValue::Array(Arc::clone(&f32_array)),
            ColumnarValue::Scalar(ScalarValue::Int32(Some(2))),
        ];
        c.bench_with_input(BenchmarkId::new("round_dense_f32", size), &size, |b, _| {
            b.iter(|| {
                black_box(
                    round_fn
                        .invoke_with_args(ScalarFunctionArgs {
                            args: f32_args.clone(),
                            arg_fields: vec![
                                Field::new("a", DataType::Float32, false).into(),
                                Field::new("b", DataType::Int32, false).into(),
                            ],
                            number_rows: size,
                            return_field: Field::new("f", DataType::Float32, false)
                                .into(),
                            config_options: Arc::clone(&config_options),
                        })
                        .unwrap(),
                )
            })
        });
    }
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
