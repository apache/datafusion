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

extern crate criterion;

use arrow::array::{
    Array, LargeStringArray, LargeStringBuilder, StringArray, StringBuilder,
    StringViewArray, StringViewBuilder,
};
use arrow::datatypes::{DataType, Field};
use criterion::{Criterion, criterion_group, criterion_main};
use datafusion_common::config::ConfigOptions;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl};
use datafusion_spark::function::math::unhex::SparkUnhex;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::hint::black_box;
use std::sync::Arc;

fn generate_hex_string_data(size: usize, null_density: f32) -> StringArray {
    let mut rng = StdRng::seed_from_u64(42);
    let mut builder = StringBuilder::with_capacity(size, 0);
    let hex_chars = b"0123456789abcdefABCDEF";

    for _ in 0..size {
        if rng.random::<f32>() < null_density {
            builder.append_null();
        } else {
            let len = rng.random_range::<usize, _>(2..=100);
            let s: String = std::iter::repeat_with(|| {
                hex_chars[rng.random_range(0..hex_chars.len())] as char
            })
            .take(len)
            .collect();
            builder.append_value(&s);
        }
    }
    builder.finish()
}

fn generate_hex_large_string_data(size: usize, null_density: f32) -> LargeStringArray {
    let mut rng = StdRng::seed_from_u64(42);
    let mut builder = LargeStringBuilder::with_capacity(size, 0);
    let hex_chars = b"0123456789abcdefABCDEF";

    for _ in 0..size {
        if rng.random::<f32>() < null_density {
            builder.append_null();
        } else {
            let len = rng.random_range::<usize, _>(2..=100);
            let s: String = std::iter::repeat_with(|| {
                hex_chars[rng.random_range(0..hex_chars.len())] as char
            })
            .take(len)
            .collect();
            builder.append_value(&s);
        }
    }
    builder.finish()
}

fn generate_hex_utf8view_data(size: usize, null_density: f32) -> StringViewArray {
    let mut rng = StdRng::seed_from_u64(42);
    let mut builder = StringViewBuilder::with_capacity(size);
    let hex_chars = b"0123456789abcdefABCDEF";

    for _ in 0..size {
        if rng.random::<f32>() < null_density {
            builder.append_null();
        } else {
            let len = rng.random_range::<usize, _>(2..=100);
            let s: String = std::iter::repeat_with(|| {
                hex_chars[rng.random_range(0..hex_chars.len())] as char
            })
            .take(len)
            .collect();
            builder.append_value(&s);
        }
    }
    builder.finish()
}

fn run_benchmark(c: &mut Criterion, name: &str, size: usize, array: Arc<dyn Array>) {
    let unhex_func = SparkUnhex::new();
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
                unhex_func
                    .invoke_with_args(ScalarFunctionArgs {
                        args: args.clone(),
                        arg_fields: arg_fields.clone(),
                        number_rows: size,
                        return_field: Arc::new(Field::new("f", DataType::Binary, true)),
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

    // Benchmark with hex string
    for &size in &sizes {
        let data = generate_hex_string_data(size, null_density);
        run_benchmark(c, "unhex_utf8", size, Arc::new(data));
    }

    // Benchmark with hex large string
    for &size in &sizes {
        let data = generate_hex_large_string_data(size, null_density);
        run_benchmark(c, "unhex_large_utf8", size, Arc::new(data));
    }

    // Benchmark with hex Utf8View
    for &size in &sizes {
        let data = generate_hex_utf8view_data(size, null_density);
        run_benchmark(c, "unhex_utf8view", size, Arc::new(data));
    }
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
