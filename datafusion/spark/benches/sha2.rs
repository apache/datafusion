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

use arrow::array::*;
use arrow::datatypes::*;
use criterion::{Criterion, criterion_group, criterion_main};
use datafusion_common::ScalarValue;
use datafusion_common::config::ConfigOptions;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl};
use datafusion_spark::function::hash::sha2::SparkSha2;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::hint::black_box;
use std::sync::Arc;

fn seedable_rng() -> StdRng {
    StdRng::seed_from_u64(42)
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

fn run_benchmark(c: &mut Criterion, name: &str, size: usize, args: &[ColumnarValue]) {
    let sha2_func = SparkSha2::new();
    let arg_fields: Vec<_> = args
        .iter()
        .enumerate()
        .map(|(idx, arg)| Field::new(format!("arg_{idx}"), arg.data_type(), true).into())
        .collect();
    let config_options = Arc::new(ConfigOptions::default());

    c.bench_function(&format!("{name}/size={size}"), |b| {
        b.iter(|| {
            black_box(
                sha2_func
                    .invoke_with_args(ScalarFunctionArgs {
                        args: args.to_vec(),
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
    // Scalar benchmark (avoid array expansion)
    let scalar_args = vec![
        ColumnarValue::Scalar(ScalarValue::Binary(Some(b"Spark".to_vec()))),
        ColumnarValue::Scalar(ScalarValue::Int32(Some(256))),
    ];
    run_benchmark(c, "sha2/scalar", 1, &scalar_args);

    let sizes = vec![1024, 4096, 8192];
    let null_density = 0.1;

    for &size in &sizes {
        let values = generate_binary_data(size, null_density);
        let bit_lengths = Int32Array::from(vec![256; size]);
        let array_args = vec![
            ColumnarValue::Array(Arc::new(values)),
            ColumnarValue::Array(Arc::new(bit_lengths)),
        ];
        run_benchmark(c, "sha2/array_binary_256", size, &array_args);
    }
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
