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

use arrow::util::bench_util::create_string_array_with_len;
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use datafusion_common::ScalarValue;
use datafusion_expr::ColumnarValue;
use datafusion_functions::core::nullif;
use std::sync::Arc;

fn criterion_benchmark(c: &mut Criterion) {
    let nullif = nullif();
    for size in [1024, 4096, 8192] {
        let array = Arc::new(create_string_array_with_len::<i32>(size, 0.2, 32));
        let args = vec![
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("abcd".to_string()))),
            ColumnarValue::Array(array),
        ];
        c.bench_function(&format!("nullif scalar array: {}", size), |b| {
            b.iter(|| black_box(nullif.invoke(&args).unwrap()))
        });
    }
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
