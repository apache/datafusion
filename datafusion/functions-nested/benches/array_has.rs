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

use std::sync::Arc;

use arrow::datatypes::Int32Type;
use arrow_array::{ArrayRef, BooleanArray, Int32Array, ListArray};
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use datafusion_functions_nested::{array_has::ComparisonType, array_has_internal, general_array_has_dispatch};

fn criterion_benchmark(c: &mut Criterion) {
    let data = vec![
        Some(std::iter::repeat(Some(100)).take(100000).collect::<Vec<Option<i32>>>()),
        ];
    let array = Arc::new(ListArray::from_iter_primitive::<Int32Type, _, _>(data)) as ArrayRef;
    let sub_array = Arc::new(Int32Array::from(vec![Some(100)])) as ArrayRef;

    let expected = Arc::new(BooleanArray::from(vec![true])) as ArrayRef;

    c.bench_function("array_has new", |b| {
        b.iter(|| {
            let is_contained = black_box(array_has_internal::<i32>(&array, &sub_array, ComparisonType::Single).unwrap());
            assert_eq!(&is_contained, &expected);
        });
    });

    c.bench_function("array_has old", |b| {
        b.iter(|| {
            let is_contained = black_box(general_array_has_dispatch::<i32>(&array, &sub_array, ComparisonType::Single).unwrap());
            assert_eq!(&is_contained, &expected);
        })
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
