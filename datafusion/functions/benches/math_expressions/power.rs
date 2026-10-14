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

//! Microbenchmark for `power`.
//!
//! `power` coerces both arguments to Float64, so the inputs are Float64.
//! Covers a different exponent for each row, as in `power(x, y)`, and a
//! constant exponent, as in `power(x, 2)`.

use std::hint::black_box;
use std::ops::Range;
use std::sync::Arc;

use arrow::array::Float64Array;
use arrow::datatypes::{DataType, Field, FieldRef};
use criterion::{Criterion, criterion_group};
use datafusion_common::ScalarValue;
use datafusion_common::config::ConfigOptions;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDF};
use datafusion_functions::math::power;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};

const NULL_DENSITY: f64 = 0.1;

fn make_f64_array(rng: &mut StdRng, size: usize, range: Range<f64>) -> Float64Array {
    (0..size)
        .map(|_| {
            if rng.random_bool(NULL_DENSITY) {
                None
            } else {
                Some(rng.random_range(range.clone()))
            }
        })
        .collect()
}

fn run_power(
    power_fn: &ScalarUDF,
    args: &[ColumnarValue],
    arg_fields: &[FieldRef],
    return_field: &FieldRef,
    config_options: &Arc<ConfigOptions>,
    num_rows: usize,
) {
    black_box(
        power_fn
            .invoke_with_args(ScalarFunctionArgs {
                args: args.to_vec(),
                arg_fields: arg_fields.to_vec(),
                number_rows: num_rows,
                return_field: Arc::clone(return_field),
                config_options: Arc::clone(config_options),
            })
            .unwrap(),
    );
}

fn criterion_benchmark(c: &mut Criterion) {
    let power_fn = power();
    let config_options = Arc::new(ConfigOptions::default());
    let base_field: FieldRef = Field::new("base", DataType::Float64, true).into();
    let exp_field: FieldRef = Field::new("exp", DataType::Float64, true).into();
    let return_field: FieldRef = Field::new("r", DataType::Float64, true).into();
    let arg_fields = vec![base_field, exp_field];

    for size in [1024usize, 8192] {
        let mut rng = StdRng::seed_from_u64(42);
        // Bases are positive: a zero base with a negative exponent is an error.
        let base_arr = Arc::new(make_f64_array(&mut rng, size, 0.1..100.0));
        let exp_arr = Arc::new(make_f64_array(&mut rng, size, -4.0..4.0));

        let array_args = vec![
            ColumnarValue::Array(Arc::clone(&base_arr) as _),
            ColumnarValue::Array(exp_arr),
        ];
        c.bench_function(&format!("power f64 array x f64 array, n={size}"), |b| {
            b.iter(|| {
                run_power(
                    &power_fn,
                    &array_args,
                    &arg_fields,
                    &return_field,
                    &config_options,
                    size,
                )
            })
        });

        for exp in [2.0, 0.5] {
            let scalar_args = vec![
                ColumnarValue::Array(Arc::clone(&base_arr) as _),
                ColumnarValue::Scalar(ScalarValue::Float64(Some(exp))),
            ];
            c.bench_function(
                &format!("power f64 array x f64 scalar, exp={exp}, n={size}"),
                |b| {
                    b.iter(|| {
                        run_power(
                            &power_fn,
                            &scalar_args,
                            &arg_fields,
                            &return_field,
                            &config_options,
                            size,
                        )
                    })
                },
            );
        }
    }
}

criterion_group!(benches, criterion_benchmark);
