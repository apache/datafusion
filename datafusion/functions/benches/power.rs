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

//! Microbenchmark for `power(decimal_array, int_*)`.
//!
//! Covers both array- and scalar-shaped integer exponents on a Decimal
//! base. Both shapes are dispatched to the native per-row decimal kernel;
//! the bench guards against any future change that routes either shape
//! through a Float64 round-trip, which is measurably slower than the
//! decimal kernel for the cases the kernel can handle.

extern crate criterion;

use arrow::array::{Decimal128Array, Int64Array};
use arrow::datatypes::{DataType, Field, FieldRef};
use criterion::{Criterion, criterion_group, criterion_main};
use datafusion_common::ScalarValue;
use datafusion_common::config::ConfigOptions;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDF};
use datafusion_functions::math::power;
use std::hint::black_box;
use std::sync::Arc;

fn make_decimal_array(size: usize, precision: u8, scale: i8) -> Decimal128Array {
    // Use a fixed unscaled value (250) so the bench is independent of `scale`.
    // The four-arm dispatch in `power` only cares about the Decimal variant
    // and the exponent's shape, not the numeric value.
    let arr = Decimal128Array::from(vec![250i128; size]);
    arr.with_precision_and_scale(precision, scale).unwrap()
}

fn make_int_array(size: usize, value: i64) -> Int64Array {
    Int64Array::from(vec![value; size])
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
    let precision: u8 = 20;
    let scale: i8 = 2;
    let decimal_ty = DataType::Decimal128(precision, scale);

    // Exponents are bounded by what the native decimal kernel can handle
    // without overflowing the i128 intermediate; see
    // <https://github.com/apache/datafusion/issues/22480>
    let exponents = [2i64, 4, 8];

    for size in [1024usize, 8192] {
        let base_arr = Arc::new(make_decimal_array(size, precision, scale));
        let base_field: FieldRef = Field::new("base", decimal_ty.clone(), true).into();
        let exp_field: FieldRef = Field::new("exp", DataType::Int64, true).into();
        let return_field: FieldRef = Field::new("r", decimal_ty.clone(), true).into();
        let arg_fields = vec![base_field, exp_field];

        for &exp in &exponents {
            let exp_arr = Arc::new(make_int_array(size, exp));
            let array_args = vec![
                ColumnarValue::Array(base_arr.clone()),
                ColumnarValue::Array(exp_arr),
            ];
            c.bench_function(
                &format!(
                    "power decimal({precision},{scale}) array x int array, exp={exp}, n={size}"
                ),
                |b| {
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
                },
            );

            let scalar_args = vec![
                ColumnarValue::Array(base_arr.clone()),
                ColumnarValue::Scalar(ScalarValue::Int64(Some(exp))),
            ];
            c.bench_function(
                &format!(
                    "power decimal({precision},{scale}) array x int scalar, exp={exp}, n={size}"
                ),
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
criterion_main!(benches);
