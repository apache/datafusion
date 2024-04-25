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

use arrow::array::{ArrayRef, StringArray};
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use datafusion_common::ScalarValue;
use datafusion_expr::ColumnarValue;
use datafusion_functions::string;
use std::sync::Arc;

fn create_args(size: usize, characters: &str) -> Vec<ColumnarValue> {
    let iter =
        std::iter::repeat(format!("{}datafusion{}", characters, characters)).take(size);
    let array = Arc::new(StringArray::from_iter_values(iter)) as ArrayRef;
    vec![
        ColumnarValue::Array(array),
        ColumnarValue::Scalar(ScalarValue::Utf8(Some(characters.to_string()))),
    ]
}

fn criterion_benchmark(c: &mut Criterion) {
    let ltrim = string::ltrim();
    for char in ["\"", "Header:"] {
        for size in [1024, 4096, 8192] {
            let args = create_args(size, char);
            c.bench_function(&format!("ltrim {}: {}", char, size), |b| {
                b.iter(|| black_box(ltrim.invoke(&args)))
            });
        }
    }
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
