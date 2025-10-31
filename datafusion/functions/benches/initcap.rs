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

use arrow::array::OffsetSizeTrait;
use arrow::datatypes::{DataType, Field};
use arrow::util::bench_util::{
    create_string_array_with_len, create_string_view_array_with_len,
};
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use datafusion_common::config::ConfigOptions;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs};
use datafusion_functions::unicode;
use std::sync::Arc;

fn create_args<O: OffsetSizeTrait>(
    size: usize,
    str_len: usize,
    force_view_types: bool,
) -> Vec<ColumnarValue> {
    if force_view_types {
        let string_array =
            Arc::new(create_string_view_array_with_len(size, 0.2, str_len, false));

        vec![ColumnarValue::Array(string_array)]
    } else {
        let string_array =
            Arc::new(create_string_array_with_len::<O>(size, 0.2, str_len));

        vec![ColumnarValue::Array(string_array)]
    }
}

fn criterion_benchmark(c: &mut Criterion) {
    let initcap = unicode::initcap();
    for size in [1024, 4096] {
        let args = create_args::<i32>(size, 8, true);
        let arg_fields = args
            .iter()
            .enumerate()
            .map(|(idx, arg)| {
                Field::new(format!("arg_{idx}"), arg.data_type(), true).into()
            })
            .collect::<Vec<_>>();
        let config_options = Arc::new(ConfigOptions::default());

        c.bench_function(
            format!("initcap string view shorter than 12 [size={size}]").as_str(),
            |b| {
                b.iter(|| {
                    black_box(initcap.invoke_with_args(ScalarFunctionArgs {
                        args: args.clone(),
                        arg_fields: arg_fields.clone(),
                        number_rows: size,
                        return_field: Field::new("f", DataType::Utf8View, true).into(),
                        config_options: Arc::clone(&config_options),
                    }))
                })
            },
        );

        let args = create_args::<i32>(size, 16, true);
        c.bench_function(
            format!("initcap string view longer than 12 [size={size}]").as_str(),
            |b| {
                b.iter(|| {
                    black_box(initcap.invoke_with_args(ScalarFunctionArgs {
                        args: args.clone(),
                        arg_fields: arg_fields.clone(),
                        number_rows: size,
                        return_field: Field::new("f", DataType::Utf8View, true).into(),
                        config_options: Arc::clone(&config_options),
                    }))
                })
            },
        );

        let args = create_args::<i32>(size, 16, false);
        c.bench_function(format!("initcap string [size={size}]").as_str(), |b| {
            b.iter(|| {
                black_box(initcap.invoke_with_args(ScalarFunctionArgs {
                    args: args.clone(),
                    arg_fields: arg_fields.clone(),
                    number_rows: size,
                    return_field: Field::new("f", DataType::Utf8, true).into(),
                    config_options: Arc::clone(&config_options),
                }))
            })
        });
    }
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
