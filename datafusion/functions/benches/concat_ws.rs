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

use arrow::array::ArrayRef;
use arrow::datatypes::{DataType, Field};
use arrow::util::bench_util::create_string_array_with_len;
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use datafusion_common::ScalarValue;
use datafusion_common::config::ConfigOptions;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs};
use datafusion_functions::string::concat_ws;
use rand::Rng;
use rand::distr::Alphanumeric;
use std::hint::black_box;
use std::sync::Arc;

fn create_array_args(size: usize, str_len: usize) -> Vec<ColumnarValue> {
    let array = Arc::new(create_string_array_with_len::<i32>(size, 0.2, str_len));
    let scalar = ScalarValue::Utf8(Some(", ".to_string()));
    vec![
        ColumnarValue::Scalar(scalar),
        ColumnarValue::Array(Arc::clone(&array) as ArrayRef),
        ColumnarValue::Array(array),
    ]
}

fn generate_random_string(str_len: usize) -> String {
    rand::rng()
        .sample_iter(&Alphanumeric)
        .take(str_len)
        .map(char::from)
        .collect()
}

fn create_scalar_args(count: usize, str_len: usize) -> Vec<ColumnarValue> {
    let mut args = Vec::with_capacity(count + 1);

    args.push(ColumnarValue::Scalar(ScalarValue::Utf8(Some(
        ",".to_string(),
    ))));

    for _ in 0..count {
        let s = generate_random_string(str_len);
        args.push(ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))));
    }
    args
}

fn criterion_benchmark(c: &mut Criterion) {
    // Benchmark for array concat_ws
    for size in [1024, 4096, 8192] {
        let args = create_array_args(size, 32);
        let arg_fields = args
            .iter()
            .enumerate()
            .map(|(idx, arg)| {
                Field::new(format!("arg_{idx}"), arg.data_type(), true).into()
            })
            .collect::<Vec<_>>();
        let config_options = Arc::new(ConfigOptions::default());

        let mut group = c.benchmark_group("concat_ws function");
        group.bench_function(BenchmarkId::new("concat_ws", size), |b| {
            b.iter(|| {
                let args_cloned = args.clone();
                black_box(
                    concat_ws()
                        .invoke_with_args(ScalarFunctionArgs {
                            args: args_cloned,
                            arg_fields: arg_fields.clone(),
                            number_rows: size,
                            return_field: Field::new("f", DataType::Utf8, true).into(),
                            config_options: Arc::clone(&config_options),
                        })
                        .unwrap(),
                )
            })
        });
        group.finish();
    }

    // Benchmark for scalar concat_ws
    let scalar_args = create_scalar_args(10, 100);
    let scalar_arg_fields = scalar_args
        .iter()
        .enumerate()
        .map(|(idx, arg)| Field::new(format!("arg_{idx}"), arg.data_type(), true).into())
        .collect::<Vec<_>>();
    let mut group = c.benchmark_group("concat_ws function");
    group.bench_function(BenchmarkId::new("concat_ws", "scalar"), |b| {
        b.iter(|| {
            let args_cloned = scalar_args.clone();
            black_box(
                concat_ws()
                    .invoke_with_args(ScalarFunctionArgs {
                        args: args_cloned,
                        arg_fields: scalar_arg_fields.clone(),
                        number_rows: 1,
                        return_field: Field::new("f", DataType::Utf8, true).into(),
                        config_options: Arc::new(ConfigOptions::default()),
                    })
                    .unwrap(),
            )
        })
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
