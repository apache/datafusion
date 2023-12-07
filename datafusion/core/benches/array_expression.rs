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
extern crate datafusion;

mod data_utils;
use crate::criterion::Criterion;
use arrow_array::cast::AsArray;
use arrow_array::types::Int64Type;
use arrow_array::{ArrayRef, Int64Array, ListArray};
use datafusion_physical_expr::array_expressions;
use std::sync::Arc;

fn criterion_benchmark(c: &mut Criterion) {
    // Construct large arrays for benchmarking

    let array_len = 100000000;

    let array = (0..array_len).map(|_| Some(2_i64)).collect::<Vec<_>>();
    let list_array = ListArray::from_iter_primitive::<Int64Type, _, _>(vec![
        Some(array.clone()),
        Some(array.clone()),
        Some(array),
    ]);
    let from_array = Int64Array::from_value(2, 3);
    let to_array = Int64Array::from_value(-2, 3);

    let args = vec![
        Arc::new(list_array) as ArrayRef,
        Arc::new(from_array) as ArrayRef,
        Arc::new(to_array) as ArrayRef,
    ];

    let array = (0..array_len).map(|_| Some(-2_i64)).collect::<Vec<_>>();
    let expected_array = ListArray::from_iter_primitive::<Int64Type, _, _>(vec![
        Some(array.clone()),
        Some(array.clone()),
        Some(array),
    ]);

    // Benchmark array functions

    c.bench_function("array_replace", |b| {
        b.iter(|| {
            assert_eq!(
                array_expressions::array_replace_all(args.as_slice())
                    .unwrap()
                    .as_list::<i32>(),
                criterion::black_box(&expected_array)
            )
        })
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
