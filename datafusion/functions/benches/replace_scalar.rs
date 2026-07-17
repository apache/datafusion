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

//! Benchmarks the common `replace(column, 'lit', 'lit')` shape where the
//! `from`/`to` arguments are scalars, exercising the scalar-argument fast path.

use arrow::array::ArrayRef;
use arrow::datatypes::{DataType, Field};
use arrow::util::bench_util::create_string_array_with_len;
use criterion::{Criterion, criterion_group, criterion_main};
use datafusion_common::ScalarValue;
use datafusion_common::config::ConfigOptions;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs};
use datafusion_functions::string;
use std::hint::black_box;
use std::sync::Arc;

fn run(c: &mut Criterion, size: usize, str_len: usize, from: &str, to: &str) {
    let haystack: ArrayRef =
        Arc::new(create_string_array_with_len::<i32>(size, 0.1, str_len));
    let args = vec![
        ColumnarValue::Array(Arc::clone(&haystack)),
        ColumnarValue::Scalar(ScalarValue::Utf8(Some(from.to_string()))),
        ColumnarValue::Scalar(ScalarValue::Utf8(Some(to.to_string()))),
    ];
    let arg_fields = args
        .iter()
        .enumerate()
        .map(|(i, a)| Field::new(format!("arg_{i}"), a.data_type(), true).into())
        .collect::<Vec<_>>();
    let config_options = Arc::new(ConfigOptions::default());
    let func = string::replace();

    c.bench_function(
        &format!("replace_scalar from={from:?} [size={size}, str_len={str_len}]"),
        |b| {
            b.iter(|| {
                black_box(
                    func.invoke_with_args(ScalarFunctionArgs {
                        args: args.clone(),
                        arg_fields: arg_fields.clone(),
                        number_rows: size,
                        return_field: Field::new("f", DataType::Utf8, true).into(),
                        config_options: Arc::clone(&config_options),
                    })
                    .unwrap(),
                )
            })
        },
    );
}

fn criterion_benchmark(c: &mut Criterion) {
    let size = 8192;
    for str_len in [16_usize, 32, 64] {
        // Multi-character patterns exercise the substring-finder path, where
        // hoisting the finder out of the per-row loop matters most.
        run(c, size, str_len, "ab", "XYZ");
        run(c, size, str_len, "the", "a");
    }
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
