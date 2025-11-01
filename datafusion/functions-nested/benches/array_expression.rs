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
    array::{Int32Array, ListViewArray},
    buffer::ScalarBuffer,
    datatypes::{DataType, Field},
};
use datafusion_expr::lit;
use datafusion_functions_nested::{
    expr_fn::{array_replace_all, make_array},
    reverse::array_reverse_inner,
};

fn criterion_benchmark(c: &mut Criterion) {
    // Construct large arrays for benchmarking
    let array_len = 100000;
    let array = (0..array_len).map(|_| lit(2_i64)).collect::<Vec<_>>();
    let list_array = make_array(vec![make_array(array); 3]);
    let from_array = make_array(vec![lit(2_i64); 3]);
    let to_array = make_array(vec![lit(-2_i64); 3]);

    let expected_array = list_array.clone();

    // Benchmark array functions
    c.bench_function("array_replace", |b| {
        b.iter(|| {
            assert_eq!(
                array_replace_all(
                    list_array.clone(),
                    from_array.clone(),
                    to_array.clone()
                ),
                *criterion::black_box(&expected_array)
            )
        })
    });

    // Construct a large list view array
    let step_size: usize = 1000;
    let offsets: Vec<i32> = (0..array_len as i32)
        .step_by(step_size.try_into().unwrap())
        .collect();
    let sizes: Vec<i32> = vec![step_size as i32; array_len / step_size as usize];
    let values = (0..array_len as i32).collect::<Vec<i32>>();
    let list_view_array = ListViewArray::new(
        Arc::new(Field::new("a", DataType::Int32, false)),
        ScalarBuffer::from(offsets),
        ScalarBuffer::from(sizes),
        Arc::new(Int32Array::from(values)),
        None,
    );
    c.bench_function("array_reverse", |b| {
        b.iter(|| array_reverse_inner(&[Arc::new(list_view_array.clone())]))
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
