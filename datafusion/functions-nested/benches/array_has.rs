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

use arrow_array::{ArrayRef, BooleanArray, StringArray};
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use datafusion_common::utils::array_into_list_array;
use datafusion_functions_nested::{
    array_has::ComparisonType, array_has_dispatch, general_array_has_dispatch,
};
use rand::Rng;

fn generate_random_strings(n: usize, size: usize) -> Vec<String> {
    let mut rng = rand::thread_rng();
    let mut strings = Vec::with_capacity(n);

    // Define the characters to use in the random strings
    let charset: &[u8] =
        b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";

    for _ in 0..n {
        // Generate a random string of the specified size or length 4
        let random_string: String = if rng.gen_bool(0.5) {
            (0..4)
                .map(|_| {
                    let idx = rng.gen_range(0..charset.len());
                    charset[idx] as char
                })
                .collect()
        } else {
            (0..size)
                .map(|_| {
                    let idx = rng.gen_range(0..charset.len());
                    charset[idx] as char
                })
                .collect()
        };

        strings.push(random_string);
    }

    strings
}

fn criterion_benchmark(c: &mut Criterion) {
    let expected = Arc::new(BooleanArray::from(vec![false])) as ArrayRef;
    let data = generate_random_strings(100000, 100);
    let array = Arc::new(StringArray::from(data)) as ArrayRef;
    let array = Arc::new(array_into_list_array(array, true)) as ArrayRef;

    let sub_array = Arc::new(StringArray::from(vec!["abcd"])) as ArrayRef;

    c.bench_function("array_has specialized approach", |b| {
        b.iter(|| {
            let is_contained =
                black_box(array_has_dispatch::<i32>(&array, &sub_array).unwrap());
            assert_eq!(&is_contained, &expected);
        });
    });

    c.bench_function("array_has general approach", |b| {
        b.iter(|| {
            let is_contained = black_box(
                general_array_has_dispatch::<i32>(
                    &array,
                    &sub_array,
                    ComparisonType::Single,
                )
                .unwrap(),
            );
            assert_eq!(&is_contained, &expected);
        })
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
