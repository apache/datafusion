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

use arrow::array::{StringArray, StringViewArray};
use arrow::datatypes::{DataType, Field};
use criterion::{Criterion, criterion_group, criterion_main};
use datafusion_common::ScalarValue;
use datafusion_common::config::ConfigOptions;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs};
use rand::distr::Alphanumeric;
use rand::prelude::StdRng;
use rand::{Rng, SeedableRng};
use std::hint::black_box;
use std::sync::Arc;

/// Generate a StringArray/StringViewArray with random ASCII strings
fn gen_string_array(
    n_rows: usize,
    str_len: usize,
    is_string_view: bool,
) -> ColumnarValue {
    let mut rng = StdRng::seed_from_u64(42);
    let strings: Vec<Option<String>> = (0..n_rows)
        .map(|_| {
            let s: String = (&mut rng)
                .sample_iter(&Alphanumeric)
                .take(str_len)
                .map(char::from)
                .collect();
            Some(s)
        })
        .collect();

    if is_string_view {
        ColumnarValue::Array(Arc::new(StringViewArray::from(strings)))
    } else {
        ColumnarValue::Array(Arc::new(StringArray::from(strings)))
    }
}

/// Generate a scalar prefix string
fn gen_scalar_prefix(prefix_str: &str, is_string_view: bool) -> ColumnarValue {
    if is_string_view {
        ColumnarValue::Scalar(ScalarValue::Utf8View(Some(prefix_str.to_string())))
    } else {
        ColumnarValue::Scalar(ScalarValue::Utf8(Some(prefix_str.to_string())))
    }
}

/// Generate an array of prefix strings (same string repeated)
fn gen_array_prefix(
    prefix_str: &str,
    n_rows: usize,
    is_string_view: bool,
) -> ColumnarValue {
    let strings: Vec<Option<String>> =
        (0..n_rows).map(|_| Some(prefix_str.to_string())).collect();

    if is_string_view {
        ColumnarValue::Array(Arc::new(StringViewArray::from(strings)))
    } else {
        ColumnarValue::Array(Arc::new(StringArray::from(strings)))
    }
}

fn criterion_benchmark(c: &mut Criterion) {
    let starts_with = datafusion_functions::string::starts_with();
    let n_rows = 8192;
    let str_len = 128;
    let prefix_str = "xyz"; // A pattern that likely won't match

    // Benchmark: StringArray with scalar prefix (the optimized path)
    let str_array = gen_string_array(n_rows, str_len, false);
    let scalar_prefix = gen_scalar_prefix(prefix_str, false);
    let arg_fields = vec![
        Field::new("a", DataType::Utf8, true).into(),
        Field::new("b", DataType::Utf8, true).into(),
    ];
    let return_field = Field::new("f", DataType::Boolean, true).into();
    let config_options = Arc::new(ConfigOptions::default());

    c.bench_function("starts_with_StringArray_scalar_prefix", |b| {
        b.iter(|| {
            black_box(starts_with.invoke_with_args(ScalarFunctionArgs {
                args: vec![str_array.clone(), scalar_prefix.clone()],
                arg_fields: arg_fields.clone(),
                number_rows: n_rows,
                return_field: Arc::clone(&return_field),
                config_options: Arc::clone(&config_options),
            }))
        })
    });

    // Benchmark: StringArray with array prefix (for comparison)
    let array_prefix = gen_array_prefix(prefix_str, n_rows, false);
    c.bench_function("starts_with_StringArray_array_prefix", |b| {
        b.iter(|| {
            black_box(starts_with.invoke_with_args(ScalarFunctionArgs {
                args: vec![str_array.clone(), array_prefix.clone()],
                arg_fields: arg_fields.clone(),
                number_rows: n_rows,
                return_field: Arc::clone(&return_field),
                config_options: Arc::clone(&config_options),
            }))
        })
    });

    // Benchmark: StringViewArray with scalar prefix (the optimized path)
    let str_view_array = gen_string_array(n_rows, str_len, true);
    let scalar_prefix_view = gen_scalar_prefix(prefix_str, true);
    let arg_fields_view = vec![
        Field::new("a", DataType::Utf8View, true).into(),
        Field::new("b", DataType::Utf8View, true).into(),
    ];

    c.bench_function("starts_with_StringViewArray_scalar_prefix", |b| {
        b.iter(|| {
            black_box(starts_with.invoke_with_args(ScalarFunctionArgs {
                args: vec![str_view_array.clone(), scalar_prefix_view.clone()],
                arg_fields: arg_fields_view.clone(),
                number_rows: n_rows,
                return_field: Arc::clone(&return_field),
                config_options: Arc::clone(&config_options),
            }))
        })
    });

    // Benchmark: StringViewArray with array prefix (for comparison)
    let array_prefix_view = gen_array_prefix(prefix_str, n_rows, true);
    c.bench_function("starts_with_StringViewArray_array_prefix", |b| {
        b.iter(|| {
            black_box(starts_with.invoke_with_args(ScalarFunctionArgs {
                args: vec![str_view_array.clone(), array_prefix_view.clone()],
                arg_fields: arg_fields_view.clone(),
                number_rows: n_rows,
                return_field: Arc::clone(&return_field),
                config_options: Arc::clone(&config_options),
            }))
        })
    });

    // Benchmark different string lengths with scalar prefix
    for str_len in [8, 32, 128, 512] {
        let str_array = gen_string_array(n_rows, str_len, true);
        let scalar_prefix = gen_scalar_prefix(prefix_str, true);
        let arg_fields = vec![
            Field::new("a", DataType::Utf8View, true).into(),
            Field::new("b", DataType::Utf8View, true).into(),
        ];

        c.bench_function(
            &format!("starts_with_StringViewArray_scalar_strlen_{str_len}"),
            |b| {
                b.iter(|| {
                    black_box(starts_with.invoke_with_args(ScalarFunctionArgs {
                        args: vec![str_array.clone(), scalar_prefix.clone()],
                        arg_fields: arg_fields.clone(),
                        number_rows: n_rows,
                        return_field: Arc::clone(&return_field),
                        config_options: Arc::clone(&config_options),
                    }))
                })
            },
        );
    }
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
