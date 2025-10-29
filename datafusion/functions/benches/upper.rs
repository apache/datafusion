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
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use datafusion_common::config::ConfigOptions;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs};
use datafusion_functions::string;
use std::sync::Arc;

/// Create an array of args containing a StringArray, where all the values in the
/// StringArray are ASCII.
/// * `size` - the length of the StringArray, and
/// * `str_len` - the length of the strings within the StringArray.
fn create_args(size: usize, str_len: usize) -> Vec<ColumnarValue> {
    let array = Arc::new(create_string_array_with_len::<i32>(size, 0.2, str_len));
    vec![ColumnarValue::Array(array)]
}

fn criterion_benchmark(c: &mut Criterion) {
    let upper = string::upper();
    let config_options = Arc::new(ConfigOptions::default());

    for size in [1024, 4096, 8192] {
        let args = create_args(size, 32);
        c.bench_function("upper_all_values_are_ascii", |b| {
            b.iter(|| {
                let args_cloned = args.clone();
                black_box(upper.invoke_with_args(ScalarFunctionArgs {
                    args: args_cloned,
                    arg_fields: vec![Field::new("a", DataType::Utf8, true).into()],
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
