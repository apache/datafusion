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

use arrow::array::Array;
use arrow::datatypes::{DataType, Field};
use arrow::util::bench_util::create_string_array_with_len;
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use datafusion_common::config::ConfigOptions;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs};
use datafusion_functions::encoding;
use std::sync::Arc;

fn criterion_benchmark(c: &mut Criterion) {
    let decode = encoding::decode();
    let config_options = Arc::new(ConfigOptions::default());

    for size in [1024, 4096, 8192] {
        let str_array = Arc::new(create_string_array_with_len::<i32>(size, 0.2, 32));
        c.bench_function(&format!("base64_decode/{size}"), |b| {
            let method = ColumnarValue::Scalar("base64".into());
            let encoded = encoding::encode()
                .invoke_with_args(ScalarFunctionArgs {
                    args: vec![ColumnarValue::Array(str_array.clone()), method.clone()],
                    arg_fields: vec![
                        Field::new("a", str_array.data_type().to_owned(), true).into(),
                        Field::new("b", method.data_type().to_owned(), true).into(),
                    ],
                    number_rows: size,
                    return_field: Field::new("f", DataType::Utf8, true).into(),
                    config_options: Arc::clone(&config_options),
                })
                .unwrap();

            let arg_fields = vec![
                Field::new("a", encoded.data_type().to_owned(), true).into(),
                Field::new("b", method.data_type().to_owned(), true).into(),
            ];
            let args = vec![encoded, method];

            b.iter(|| {
                black_box(
                    decode
                        .invoke_with_args(ScalarFunctionArgs {
                            args: args.clone(),
                            arg_fields: arg_fields.clone(),
                            number_rows: size,
                            return_field: Field::new("f", DataType::Utf8, true).into(),
                            config_options: Arc::clone(&config_options),
                        })
                        .unwrap(),
                )
            })
        });

        c.bench_function(&format!("hex_decode/{size}"), |b| {
            let method = ColumnarValue::Scalar("hex".into());
            let arg_fields = vec![
                Field::new("a", str_array.data_type().to_owned(), true).into(),
                Field::new("b", method.data_type().to_owned(), true).into(),
            ];
            let encoded = encoding::encode()
                .invoke_with_args(ScalarFunctionArgs {
                    args: vec![ColumnarValue::Array(str_array.clone()), method.clone()],
                    arg_fields,
                    number_rows: size,
                    return_field: Field::new("f", DataType::Utf8, true).into(),
                    config_options: Arc::clone(&config_options),
                })
                .unwrap();

            let arg_fields = vec![
                Field::new("a", encoded.data_type().to_owned(), true).into(),
                Field::new("b", method.data_type().to_owned(), true).into(),
            ];
            let return_field = Field::new("f", DataType::Utf8, true).into();
            let args = vec![encoded, method];

            b.iter(|| {
                black_box(
                    decode
                        .invoke_with_args(ScalarFunctionArgs {
                            args: args.clone(),
                            arg_fields: arg_fields.clone(),
                            number_rows: size,
                            return_field: Arc::clone(&return_field),
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
