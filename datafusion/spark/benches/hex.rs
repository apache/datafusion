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
use datafusion_spark::function::math::hex::SparkHex;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::hint::black_box;
use std::sync::Arc;

fn seedable_rng() -> StdRng {
    StdRng::seed_from_u64(42)
}

fn generate_int64_data(size: usize, null_density: f32) -> PrimitiveArray<Int64Type> {
    let mut rng = seedable_rng();
    (0..size)
        .map(|_| {
            if rng.random::<f32>() < null_density {
                None
            } else {
                Some(rng.random_range::<i64, _>(-999_999_999_999..999_999_999_999))
            }
        })
        .collect()
}

fn generate_utf8_data(size: usize, null_density: f32) -> StringArray {
    let mut rng = seedable_rng();
    let mut builder = StringBuilder::new();
    for _ in 0..size {
        if rng.random::<f32>() < null_density {
            builder.append_null();
        } else {
            let len = rng.random_range::<usize, _>(1..=100);
            let s: String =
                std::iter::repeat_with(|| rng.random_range(b'a'..=b'z') as char)
                    .take(len)
                    .collect();
            builder.append_value(&s);
        }
    }
    builder.finish()
}

fn generate_binary_data(size: usize, null_density: f32) -> BinaryArray {
    let mut rng = seedable_rng();
    let mut builder = BinaryBuilder::new();
    for _ in 0..size {
        if rng.random::<f32>() < null_density {
            builder.append_null();
        } else {
            let len = rng.random_range::<usize, _>(1..=100);
            let bytes: Vec<u8> = (0..len).map(|_| rng.random()).collect();
            builder.append_value(&bytes);
        }
    }
    builder.finish()
}

fn generate_int64_dict_data(
    size: usize,
    null_density: f32,
) -> DictionaryArray<Int32Type> {
    let mut rng = seedable_rng();
    let mut builder = PrimitiveDictionaryBuilder::<Int32Type, Int64Type>::new();
    for _ in 0..size {
        if rng.random::<f32>() < null_density {
            builder.append_null();
        } else {
            builder.append_value(
                rng.random_range::<i64, _>(-999_999_999_999..999_999_999_999),
            );
        }
    }
    builder.finish()
}

fn run_benchmark(c: &mut Criterion, name: &str, size: usize, array: Arc<dyn Array>) {
    let hex_func = SparkHex::new();
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
                hex_func
                    .invoke_with_args(ScalarFunctionArgs {
                        args: args.clone(),
                        arg_fields: arg_fields.clone(),
                        number_rows: size,
                        return_field: Arc::new(Field::new("f", DataType::Utf8, true)),
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
        let data = generate_int64_data(size, null_density);
        run_benchmark(c, "hex_int64", size, Arc::new(data));
    }

    for &size in &sizes {
        let data = generate_utf8_data(size, null_density);
        run_benchmark(c, "hex_utf8", size, Arc::new(data));
    }

    for &size in &sizes {
        let data = generate_binary_data(size, null_density);
        run_benchmark(c, "hex_binary", size, Arc::new(data));
    }

    for &size in &sizes {
        let data = generate_int64_dict_data(size, null_density);
        run_benchmark(c, "hex_int64_dict", size, Arc::new(data));
    }
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
