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

#[macro_use]
extern crate criterion;
extern crate arrow;

use std::sync::Arc;

use crate::criterion::Criterion;
use arrow::{
    array::{FixedSizeListArray, Int32Array, ListArray, ListViewArray},
    buffer::{OffsetBuffer, ScalarBuffer},
    datatypes::{DataType, Field},
};
use datafusion_functions_nested::reverse::array_reverse_inner;

fn criterion_benchmark(c: &mut Criterion) {
    // Construct large arrays for benchmarking
    let array_len = 100000;
    let step_size: usize = 1000;
    let offsets: Vec<i32> = (0..array_len as i32).step_by(step_size).collect();
    let offsets = ScalarBuffer::from(offsets);
    let sizes: Vec<i32> = vec![step_size as i32; array_len / step_size];
    let values = (0..array_len as i32).collect::<Vec<i32>>();
    let list_array = ListArray::new(
        Arc::new(Field::new("a", DataType::Int32, false)),
        OffsetBuffer::new(offsets.clone()),
        Arc::new(Int32Array::from(values.clone())),
        None,
    );
    let fixed_size_list_array = FixedSizeListArray::new(
        Arc::new(Field::new("a", DataType::Int32, false)),
        step_size as i32,
        Arc::new(Int32Array::from(values.clone())),
        None,
    );
    let list_view_array = ListViewArray::new(
        Arc::new(Field::new("a", DataType::Int32, false)),
        offsets,
        ScalarBuffer::from(sizes),
        Arc::new(Int32Array::from(values)),
        None,
    );

    c.bench_function("array_reverse_list", |b| {
        b.iter(|| array_reverse_inner(&[Arc::new(list_array.clone())]))
    });

    c.bench_function("array_reverse_fixed_size_list", |b| {
        b.iter(|| array_reverse_inner(&[Arc::new(fixed_size_list_array.clone())]))
    });

    c.bench_function("array_reverse_list_view", |b| {
        b.iter(|| array_reverse_inner(&[Arc::new(list_view_array.clone())]))
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
