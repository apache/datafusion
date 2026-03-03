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

use std::{hint::black_box, sync::Arc};

use arrow::{
    array::{ArrayRef, FixedSizeListArray, Int32Array, ListArray, ListViewArray},
    buffer::{NullBuffer, OffsetBuffer, ScalarBuffer},
    datatypes::{DataType, Field},
};
use criterion::{Criterion, criterion_group, criterion_main};
use datafusion_functions_nested::reverse::array_reverse_inner;

fn array_reverse(array: &ArrayRef) -> ArrayRef {
    black_box(array_reverse_inner(std::slice::from_ref(array)).unwrap())
}

fn criterion_benchmark(c: &mut Criterion) {
    // Create array sizes with step size of 100, starting from 100.
    let number_of_arrays = 1000;
    let sizes = (0..number_of_arrays)
        .map(|i| 100 + i * 100)
        .collect::<Vec<i32>>();

    // Calculate the total number of values
    let total_values = sizes.iter().sum::<i32>();

    // Calculate sizes and offsets from array lengths
    let offsets = sizes
        .iter()
        .scan(0, |acc, &x| {
            let offset = *acc;
            *acc += x;
            Some(offset)
        })
        .collect::<Vec<i32>>();
    let offsets = ScalarBuffer::from(offsets);
    // Set every 10th array to null
    let nulls = (0..number_of_arrays)
        .map(|i| i % 10 != 0)
        .collect::<Vec<bool>>();

    let values = (0..total_values).collect::<Vec<i32>>();
    let values = Arc::new(Int32Array::from(values));

    // Create ListArray and ListViewArray
    let nulls_list_array = Some(NullBuffer::from(
        nulls[..((number_of_arrays as usize) - 1)].to_vec(),
    ));
    let list_array: ArrayRef = Arc::new(ListArray::new(
        Arc::new(Field::new("a", DataType::Int32, false)),
        OffsetBuffer::new(offsets.clone()),
        values.clone(),
        nulls_list_array,
    ));
    let nulls_list_view_array = Some(NullBuffer::from(
        nulls[..(number_of_arrays as usize)].to_vec(),
    ));
    let list_view_array: ArrayRef = Arc::new(ListViewArray::new(
        Arc::new(Field::new("a", DataType::Int32, false)),
        offsets,
        ScalarBuffer::from(sizes),
        values.clone(),
        nulls_list_view_array,
    ));

    c.bench_function("array_reverse_list", |b| {
        b.iter(|| array_reverse(&list_array))
    });

    c.bench_function("array_reverse_list_view", |b| {
        b.iter(|| array_reverse(&list_view_array))
    });

    // Create FixedSizeListArray
    let array_len = 1000;
    let num_arrays = 5000;
    let total_values = num_arrays * array_len;
    let values = (0..total_values).collect::<Vec<i32>>();
    let values = Arc::new(Int32Array::from(values));
    // Set every 10th array to null
    let nulls = (0..num_arrays).map(|i| i % 10 != 0).collect::<Vec<bool>>();
    let nulls = Some(NullBuffer::from(nulls));
    let fixed_size_list_array: ArrayRef = Arc::new(FixedSizeListArray::new(
        Arc::new(Field::new("a", DataType::Int32, false)),
        array_len,
        values.clone(),
        nulls.clone(),
    ));
    c.bench_function("array_reverse_fixed_size_list", |b| {
        b.iter(|| array_reverse(&fixed_size_list_array))
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
