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
use arrow::util::bench_util::create_string_array_with_len;
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use datafusion_expr::ColumnarValue;
use datafusion_functions::string;
use std::sync::Arc;

/// Create an array of args containing a StringArray, where all the values in the
/// StringArray are ASCII.
/// * `size` - the length of the StringArray, and
/// * `str_len` - the length of the strings within the StringArray.
fn create_args1(size: usize, str_len: usize) -> Vec<ColumnarValue> {
    let array = Arc::new(create_string_array_with_len::<i32>(size, 0.2, str_len));
    vec![ColumnarValue::Array(array)]
}

/// Create an array of args containing a StringArray, where the first value in the
/// StringArray is non-ASCII.
/// * `size` - the length of the StringArray, and
/// * `str_len` - the length of the strings within the StringArray.
fn create_args2(size: usize) -> Vec<ColumnarValue> {
    let mut items = Vec::with_capacity(size);
    items.push("农历新年".to_string());
    for i in 1..size {
        items.push(format!("DATAFUSION {}", i));
    }
    let array = Arc::new(StringArray::from(items)) as ArrayRef;
    vec![ColumnarValue::Array(array)]
}

/// Create an array of args containing a StringArray, where the middle value of the
/// StringArray is non-ASCII.
/// * `size` - the length of the StringArray, and
/// * `str_len` - the length of the strings within the StringArray.
fn create_args3(size: usize) -> Vec<ColumnarValue> {
    let mut items = Vec::with_capacity(size);
    let half = size / 2;
    for i in 0..half {
        items.push(format!("DATAFUSION {}", i));
    }
    items.push("Ⱦ".to_string());
    for i in half + 1..size {
        items.push(format!("DATAFUSION {}", i));
    }
    let array = Arc::new(StringArray::from(items)) as ArrayRef;
    vec![ColumnarValue::Array(array)]
}

fn criterion_benchmark(c: &mut Criterion) {
    let lower = string::lower();
    for size in [1024, 4096, 8192] {
        let args = create_args1(size, 32);
        c.bench_function(&format!("lower_all_values_are_ascii: {}", size), |b| {
            b.iter(|| black_box(lower.invoke(&args)))
        });

        let args = create_args2(size);
        c.bench_function(
            &format!("lower_the_first_value_is_nonascii: {}", size),
            |b| b.iter(|| black_box(lower.invoke(&args))),
        );

        let args = create_args3(size);
        c.bench_function(
            &format!("lower_the_middle_value_is_nonascii: {}", size),
            |b| b.iter(|| black_box(lower.invoke(&args))),
        );
    }
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
