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

use arrow::datatypes::Field;
use arrow::{
    array::{ArrayRef, Decimal128Array, Int64Array},
    datatypes::DataType,
};
use criterion::{Criterion, criterion_group};
use datafusion_common::ScalarValue;
use datafusion_common::config::ConfigOptions;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs};
use datafusion_functions::math::gcd;
use rand::prelude::*;
use std::hint::black_box;
use std::sync::Arc;

fn generate_i64_array(rng: &mut StdRng, n_rows: usize) -> ArrayRef {
    let values = (0..n_rows)
        .map(|_| rng.random_range(0..1000))
        .collect::<Vec<_>>();
    Arc::new(Int64Array::from(values)) as ArrayRef
}

fn criterion_benchmark(c: &mut Criterion) {
    let n_rows = 100000;
    let mut rng = StdRng::seed_from_u64(0);
    let array_a = ColumnarValue::Array(generate_i64_array(&mut rng, n_rows));
    let array_b = ColumnarValue::Array(generate_i64_array(&mut rng, n_rows));
    let udf = gcd();
    let config_options = Arc::new(ConfigOptions::default());

    c.bench_function("gcd both array", |b| {
        b.iter(|| {
            black_box(
                udf.invoke_with_args(ScalarFunctionArgs {
                    args: vec![array_a.clone(), array_b.clone()],
                    arg_fields: vec![
                        Field::new("a", array_a.data_type(), true).into(),
                        Field::new("b", array_b.data_type(), true).into(),
                    ],
                    number_rows: 0,
                    return_field: Field::new("f", DataType::Int64, true).into(),
                    config_options: Arc::clone(&config_options),
                })
                .expect("date_bin should work on valid values"),
            )
        })
    });

    // 10! = 3628800
    let scalar_b = ColumnarValue::Scalar(ScalarValue::Int64(Some(3628800)));

    c.bench_function("gcd array and scalar", |b| {
        b.iter(|| {
            black_box(
                udf.invoke_with_args(ScalarFunctionArgs {
                    args: vec![array_a.clone(), scalar_b.clone()],
                    arg_fields: vec![
                        Field::new("a", array_a.data_type(), true).into(),
                        Field::new("b", scalar_b.data_type(), true).into(),
                    ],
                    number_rows: 0,
                    return_field: Field::new("f", DataType::Int64, true).into(),
                    config_options: Arc::clone(&config_options),
                })
                .expect("date_bin should work on valid values"),
            )
        })
    });

    // scalar and scalar
    let scalar_a = ColumnarValue::Scalar(ScalarValue::Int64(Some(3628800)));

    c.bench_function("gcd both scalar", |b| {
        b.iter(|| {
            black_box(
                udf.invoke_with_args(ScalarFunctionArgs {
                    args: vec![scalar_a.clone(), scalar_b.clone()],
                    arg_fields: vec![
                        Field::new("a", scalar_a.data_type(), true).into(),
                        Field::new("b", scalar_b.data_type(), true).into(),
                    ],
                    number_rows: 0,
                    return_field: Field::new("f", DataType::Int64, true).into(),
                    config_options: Arc::clone(&config_options),
                })
                .expect("date_bin should work on valid values"),
            )
        })
    });
}

fn generate_decimal128_array(
    rng: &mut StdRng,
    n_rows: usize,
    first_values: [i128; 3],
) -> ColumnarValue {
    let mut values = (0..n_rows)
        .map(|_| rng.random_range(0..1000))
        .collect::<Vec<i128>>();
    values[..first_values.len()].copy_from_slice(&first_values);
    ColumnarValue::Array(Arc::new(
        Decimal128Array::from(values)
            .with_precision_and_scale(20, 2)
            .expect("valid Decimal128 precision and scale"),
    ))
}

fn criterion_benchmark_decimal128(c: &mut Criterion) {
    let n_rows = 100000;
    // Share the RNG so the two arrays do not contain identical samples.
    let mut rng = StdRng::seed_from_u64(0);
    let array_a = generate_decimal128_array(&mut rng, n_rows, [0, 1200, -1800]);
    let array_b = generate_decimal128_array(&mut rng, n_rows, [600, 1800, 1200]);
    // With scale 2, 1800 and 1200 represent 18.00 and 12.00 (GCD 6.00).
    let scalar_a = ColumnarValue::Scalar(ScalarValue::Decimal128(Some(1800), 20, 2));
    let scalar_b = ColumnarValue::Scalar(ScalarValue::Decimal128(Some(1200), 20, 2));
    let udf = gcd();
    let config_options = Arc::new(ConfigOptions::default());

    let cases = [
        (
            "gcd decimal128 both array",
            array_a.clone(),
            array_b,
            n_rows,
            vec![600, 600, 600],
        ),
        (
            "gcd decimal128 array and scalar",
            array_a,
            scalar_b.clone(),
            n_rows,
            vec![1200, 1200, 600],
        ),
        (
            "gcd decimal128 both scalar",
            scalar_a,
            scalar_b,
            1,
            vec![600],
        ),
    ];

    for (name, a, b, number_rows, expected_prefix) in cases {
        let invoke = || {
            udf.invoke_with_args(ScalarFunctionArgs {
                args: vec![a.clone(), b.clone()],
                arg_fields: vec![
                    Field::new("a", a.data_type(), true).into(),
                    Field::new("b", b.data_type(), true).into(),
                ],
                number_rows,
                return_field: Field::new("f", DataType::Decimal128(20, 2), true).into(),
                config_options: Arc::clone(&config_options),
            })
            .expect("gcd should work on valid Decimal128 values")
        };

        // Check the measured inputs outside the timed loop. The Decimal path
        // returns an array even for scalar inputs and uses number_rows to size it.
        let ColumnarValue::Array(result) = invoke() else {
            panic!("Decimal128 gcd should return an array");
        };
        assert_eq!(result.data_type(), &DataType::Decimal128(20, 2));
        assert_eq!(result.len(), number_rows);
        assert_eq!(result.null_count(), 0);
        let result = result
            .as_any()
            .downcast_ref::<Decimal128Array>()
            .expect("Decimal128 gcd should return a Decimal128Array");
        assert_eq!(
            &result.values()[..expected_prefix.len()],
            expected_prefix.as_slice()
        );

        c.bench_function(name, |b| b.iter(|| black_box(invoke())));
    }
}

criterion_group!(benches, criterion_benchmark, criterion_benchmark_decimal128);
