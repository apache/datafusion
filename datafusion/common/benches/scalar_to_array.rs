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

//! Benchmarks for `ScalarValue::to_array_of_size`, focusing on List
//! scalars.

use arrow::array::{Array, ArrayRef, AsArray, StringViewBuilder};
use arrow::datatypes::{DataType, Field};
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use datafusion_common::ScalarValue;
use datafusion_common::utils::SingleRowListArrayBuilder;
use std::sync::Arc;

/// Build a `ScalarValue::List` of `num_elements` Utf8View strings whose
/// inner StringViewArray has `num_buffers` data buffers.
fn make_list_scalar(num_elements: usize, num_buffers: usize) -> ScalarValue {
    let elements_per_buffer = num_elements.div_ceil(num_buffers);

    let mut small_arrays: Vec<ArrayRef> = Vec::new();
    let mut remaining = num_elements;
    for buf_idx in 0..num_buffers {
        let count = remaining.min(elements_per_buffer);
        if count == 0 {
            break;
        }
        let start = buf_idx * elements_per_buffer;
        let mut builder = StringViewBuilder::with_capacity(count);
        for i in start..start + count {
            builder.append_value(format!("{i:024x}"));
        }
        small_arrays.push(Arc::new(builder.finish()) as ArrayRef);
        remaining -= count;
    }

    let refs: Vec<&dyn Array> = small_arrays.iter().map(|a| a.as_ref()).collect();
    let concated = arrow::compute::concat(&refs).unwrap();

    let list_array = SingleRowListArrayBuilder::new(concated)
        .with_field(&Field::new_list_field(DataType::Utf8View, true))
        .build_list_array();
    ScalarValue::List(Arc::new(list_array))
}

/// We want to measure the cost of doing the conversion and then also accessing
/// the results, to model what would happen during query evaluation.
fn consume_list_array(arr: &ArrayRef) {
    let list_arr = arr.as_list::<i32>();
    let mut total_len: usize = 0;
    for i in 0..list_arr.len() {
        let inner = list_arr.value(i);
        let sv = inner.as_string_view();
        for j in 0..sv.len() {
            total_len += sv.value(j).len();
        }
    }
    std::hint::black_box(total_len);
}

fn bench_list_to_array_of_size(c: &mut Criterion) {
    let mut group = c.benchmark_group("list_to_array_of_size");

    let num_elements = 1245;
    let scalar_1buf = make_list_scalar(num_elements, 1);
    let scalar_50buf = make_list_scalar(num_elements, 50);

    for batch_size in [256, 1024] {
        group.bench_with_input(
            BenchmarkId::new("1_buffer", batch_size),
            &batch_size,
            |b, &sz| {
                b.iter(|| {
                    let arr = scalar_1buf.to_array_of_size(sz).unwrap();
                    consume_list_array(&arr);
                });
            },
        );
        group.bench_with_input(
            BenchmarkId::new("50_buffers", batch_size),
            &batch_size,
            |b, &sz| {
                b.iter(|| {
                    let arr = scalar_50buf.to_array_of_size(sz).unwrap();
                    consume_list_array(&arr);
                });
            },
        );
    }

    group.finish();
}

criterion_group!(benches, bench_list_to_array_of_size);
criterion_main!(benches);
