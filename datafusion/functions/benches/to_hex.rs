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

use arrow::{
    array::Int64Array,
    datatypes::{Float32Type, Float64Type, Int32Type, Int64Type},
    util::bench_util::create_primitive_array,
};
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use datafusion_expr::ColumnarValue;
use datafusion_functions::string;
use std::sync::Arc;

fn criterion_benchmark(c: &mut Criterion) {
    let hex = string::to_hex();
    let size = 1024;
    let i32_array = Arc::new(create_primitive_array::<Int32Type>(size, 0.2));
    let batch_len = i32_array.len();
    let i32_args = vec![ColumnarValue::Array(i32_array)];
    c.bench_function(&format!("to_hex i32 array: {}", size), |b| {
        b.iter(|| black_box(hex.invoke_batch(&i32_args, batch_len).unwrap()))
    });
    let i64_array = Arc::new(create_primitive_array::<Int64Type>(size, 0.2));
    let batch_len = i64_array.len();
    let i64_args = vec![ColumnarValue::Array(i64_array)];
    c.bench_function(&format!("to_hex i64 array: {}", size), |b| {
        b.iter(|| black_box(hex.invoke_batch(&i64_args, batch_len).unwrap()))
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
