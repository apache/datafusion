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

use arrow::datatypes::{DataType, Field};
use arrow::util::bench_util::create_string_array_with_len;
use criterion::{Criterion, criterion_group, criterion_main};
use datafusion_common::config::ConfigOptions;
use datafusion_expr::ScalarFunctionArgs;
use datafusion_expr_common::columnar_value::ColumnarValue;
use datafusion_functions::crypto;
use std::hint::black_box;
use std::sync::Arc;

fn criterion_benchmark(c: &mut Criterion) {
    let crypto = vec![
        crypto::md5(),
        crypto::sha224(),
        crypto::sha256(),
        crypto::sha384(),
        crypto::sha512(),
    ];
    let config_options = Arc::new(ConfigOptions::default());

    for func in crypto {
        let size = 1024;
        let arr_args = vec![ColumnarValue::Array(Arc::new(
            create_string_array_with_len::<i32>(size, 0.2, 32),
        ))];
        c.bench_function(&format!("{}_array", func.name()), |b| {
            b.iter(|| {
                let args_cloned = arr_args.clone();
                black_box(func.invoke_with_args(ScalarFunctionArgs {
                    args: args_cloned,
                    arg_fields: vec![Field::new("a", DataType::Utf8, true).into()],
                    number_rows: size,
                    return_field: Field::new("f", DataType::Utf8, true).into(),
                    config_options: Arc::clone(&config_options),
                }))
            })
        });

        let scalar_args = vec![ColumnarValue::Scalar("test_string".into())];
        c.bench_function(&format!("{}_scalar", func.name()), |b| {
            b.iter(|| {
                let args_cloned = scalar_args.clone();
                black_box(func.invoke_with_args(ScalarFunctionArgs {
                    args: args_cloned,
                    arg_fields: vec![Field::new("a", DataType::Utf8, true).into()],
                    number_rows: 1,
                    return_field: Field::new("f", DataType::Utf8, true).into(),
                    config_options: Arc::clone(&config_options),
                }))
            })
        });
    }
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
