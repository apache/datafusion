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

use arrow::array::*;
use arrow::datatypes::*;
use criterion::{Criterion, criterion_group, criterion_main};
use datafusion_common::config::ConfigOptions;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl};
use datafusion_spark::function::math::floor::SparkFloor;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::hint::black_box;
use std::sync::Arc;

fn seedable_rng() -> StdRng {
    StdRng::seed_from_u64(42)
}

fn generate_float64_data(size: usize, null_density: f32) -> Float64Array {
    let mut rng = seedable_rng();
    (0..size)
        .map(|_| {
            if rng.random::<f32>() < null_density {
                None
            } else {
                Some(rng.random_range::<f64, _>(-1_000_000.0..1_000_000.0))
            }
        })
        .collect()
}

fn generate_decimal128_data(size: usize, null_density: f32) -> Decimal128Array {
    let mut rng = seedable_rng();
    let array: Decimal128Array = (0..size)
        .map(|_| {
            if rng.random::<f32>() < null_density {
                None
            } else {
                Some(rng.random_range::<i128, _>(-999_999_999..999_999_999))
            }
        })
        .collect();
    array.with_precision_and_scale(18, 2).unwrap()
}

fn run_benchmark(
    c: &mut Criterion,
    name: &str,
    size: usize,
    array: Arc<dyn Array>,
    return_type: &DataType,
) {
    let floor_func = SparkFloor::new();
    let args = vec![ColumnarValue::Array(array)];
    let arg_fields: Vec<_> = args
        .iter()
        .enumerate()
        .map(|(idx, arg)| Field::new(format!("arg_{idx}"), arg.data_type(), true).into())
        .collect();
    let config_options = Arc::new(ConfigOptions::default());

    c.bench_function(&format!("{name}/size={size}"), |b| {
        b.iter(|| {
            black_box(
                floor_func
                    .invoke_with_args(ScalarFunctionArgs {
                        args: args.clone(),
                        arg_fields: arg_fields.clone(),
                        number_rows: size,
                        return_field: Arc::new(Field::new(
                            "f",
                            return_type.clone(),
                            true,
                        )),
                        config_options: Arc::clone(&config_options),
                    })
                    .unwrap(),
            )
        })
    });
}

fn criterion_benchmark(c: &mut Criterion) {
    let sizes = vec![1024, 4096, 8192];
    let null_density = 0.1;

    for &size in &sizes {
        let data = generate_float64_data(size, null_density);
        run_benchmark(c, "floor_float64", size, Arc::new(data), &DataType::Int64);
    }

    for &size in &sizes {
        let data = generate_decimal128_data(size, null_density);
        run_benchmark(
            c,
            "floor_decimal128",
            size,
            Arc::new(data),
            &DataType::Decimal128(17, 0),
        );
    }
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
