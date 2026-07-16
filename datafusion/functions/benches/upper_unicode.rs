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

//! Benchmarks `upper` on non-ASCII input, which exercises the
//! character-streaming case-conversion path (not the ASCII fast path).

use std::hint::black_box;
use std::sync::Arc;

use arrow::array::{ArrayRef, LargeStringArray, StringArray};
use arrow::datatypes::{DataType, Field};
use criterion::{Criterion, criterion_group, criterion_main};
use datafusion_common::config::ConfigOptions;
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDF};
use datafusion_functions::string;

// A pool of non-ASCII words so `is_ascii()` is false and the Unicode path runs.
const WORDS: [&str; 8] = [
    "café",
    "straße",
    "αλφα",
    "こんにちは",
    "münchen",
    "naïve",
    " órdenes",
    "tschüß",
];

fn build_values(size: usize) -> Vec<Option<String>> {
    (0..size)
        .map(|i| {
            if i % 10 == 0 {
                None
            } else {
                // Concatenate a few words for a longer, mixed value.
                let a = WORDS[i % WORDS.len()];
                let b = WORDS[(i * 7 + 3) % WORDS.len()];
                Some(format!("{a} {b} {a}"))
            }
        })
        .collect()
}

fn invoke(func: &ScalarUDF, array: ArrayRef, dt: DataType) {
    let len = array.len();
    let config_options = Arc::new(ConfigOptions::default());
    black_box(
        func.invoke_with_args(ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(array)],
            arg_fields: vec![Field::new("a", dt.clone(), true).into()],
            number_rows: len,
            return_field: Field::new("f", dt, true).into(),
            config_options,
        })
        .unwrap(),
    );
}

fn criterion_benchmark(c: &mut Criterion) {
    let upper = string::upper();
    let size = 4096;
    let values = build_values(size);

    let utf8: ArrayRef = Arc::new(StringArray::from(values.clone()));
    let large: ArrayRef = Arc::new(LargeStringArray::from(values));

    c.bench_function("upper_unicode_utf8", |b| {
        b.iter(|| invoke(&upper, Arc::clone(&utf8), DataType::Utf8))
    });
    c.bench_function("upper_unicode_large_utf8", |b| {
        b.iter(|| invoke(&upper, Arc::clone(&large), DataType::LargeUtf8))
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
