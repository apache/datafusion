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

use std::hint::black_box;
use std::ops::Range;
use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array};
use arrow::datatypes::{DataType, Field};
use arrow::util::bench_util::{
    create_string_array_with_len, create_string_view_array_with_len,
};
use criterion::{Criterion, criterion_group, criterion_main};
use datafusion_common::config::ConfigOptions;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs};
use datafusion_functions::unicode::{left, right};

const BATCH_SIZE: usize = 8192;

fn create_args(
    str_len: usize,
    n_range: Range<i64>,
    is_string_view: bool,
) -> Vec<ColumnarValue> {
    let string_arg = if is_string_view {
        ColumnarValue::Array(Arc::new(create_string_view_array_with_len(
            BATCH_SIZE, 0.1, str_len, true,
        )))
    } else {
        ColumnarValue::Array(Arc::new(create_string_array_with_len::<i32>(
            BATCH_SIZE, 0.1, str_len,
        )))
    };

    let n_span = (n_range.end - n_range.start) as usize;
    let n_values: Vec<i64> = (0..BATCH_SIZE)
        .map(|i| n_range.start + (i % n_span) as i64)
        .collect();
    let n_array = Arc::new(Int64Array::from(n_values));

    vec![
        string_arg,
        ColumnarValue::Array(Arc::clone(&n_array) as ArrayRef),
    ]
}

fn criterion_benchmark(c: &mut Criterion) {
    // Short results (1-10 chars) produce inline StringView entries (≤12 bytes).
    // Long results (20-29 chars) produce out-of-line entries.
    let cases = [
        ("short_result", 32, 1..11_i64),
        ("long_result", 32, 20..30_i64),
    ];

    for function in [left(), right()] {
        let mut group = c.benchmark_group(function.name().to_string());

        for is_string_view in [false, true] {
            let array_type = if is_string_view {
                "string_view"
            } else {
                "string"
            };

            for (case_name, str_len, n_range) in &cases {
                let bench_name = format!("{array_type} {case_name}");
                let args = create_args(*str_len, n_range.clone(), is_string_view);
                let arg_fields: Vec<_> = args
                    .iter()
                    .enumerate()
                    .map(|(idx, arg)| {
                        Field::new(format!("arg_{idx}"), arg.data_type(), true).into()
                    })
                    .collect();
                let config_options = Arc::new(ConfigOptions::default());
                let return_field = Field::new("f", DataType::Utf8View, true).into();

                group.bench_function(&bench_name, |b| {
                    b.iter(|| {
                        black_box(
                            function
                                .invoke_with_args(ScalarFunctionArgs {
                                    args: args.clone(),
                                    arg_fields: arg_fields.clone(),
                                    number_rows: BATCH_SIZE,
                                    return_field: Arc::clone(&return_field),
                                    config_options: Arc::clone(&config_options),
                                })
                                .expect("should work"),
                        )
                    })
                });
            }
        }

        group.finish();
    }
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
