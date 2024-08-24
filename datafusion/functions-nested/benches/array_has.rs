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
use criterion::{criterion_group, criterion_main, Criterion};
use datafusion_common::utils::array_into_list_array;
use datafusion_expr::ColumnarValue;
use datafusion_functions_nested::{
    invoke_eq_kernel, invoke_general_kernel, invoke_general_scalar, invoke_iter, invoke_new}
;
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

    let a1 = ColumnarValue::Array(array);
    // let a2 = ColumnarValue::Scalar(ScalarValue::Utf8(Some(String::from("abcd"))));
    let a2 = ColumnarValue::Array(sub_array);

    c.bench_function("array_has new", |b| {
        b.iter(|| {
            invoke_new(&[a1.clone(), a2.clone()])
            // let is_contained = black_box(
            //     // array_has_dispatch_eq_kernel::<i32>(&array, &sub_array).unwrap(),
            // );
            // assert_eq!(&is_contained, &expected);
        });
    });

    c.bench_function("array_has iter", |b| {
        b.iter(|| {
            invoke_iter(&[a1.clone(), a2.clone()])
            // let is_contained =
            //     black_box(array_has_dispatch::<i32>(&array, &sub_array).unwrap());
            // assert_eq!(&is_contained, &expected);
        })
    });

    c.bench_function("array_has eq", |b| {
        b.iter(|| {
            invoke_eq_kernel(&[a1.clone(), a2.clone()])
            // let is_contained =
            //     black_box(array_has_dispatch::<i32>(&array, &sub_array).unwrap());
            // assert_eq!(&is_contained, &expected);
        })
    });

    c.bench_function("array_has general kerenl", |b| {
        b.iter(|| {
            invoke_general_kernel(&[a1.clone(), a2.clone()])
            // let is_contained =
            //     black_box(array_has_dispatch::<i32>(&array, &sub_array).unwrap());
            // assert_eq!(&is_contained, &expected);
        })
    });

    c.bench_function("array_has general scalar", |b| {
        b.iter(|| {
            invoke_general_scalar(&[a1.clone(), a2.clone()])
            // let is_contained =
            //     black_box(array_has_dispatch::<i32>(&array, &sub_array).unwrap());
            // assert_eq!(&is_contained, &expected);
        })
    });

    // c.bench_function("array_has row approach", |b| {
    //     b.iter(|| {
    //         let haystack = as_generic_list_array::<i32>(&array).unwrap();
    //         let is_contained =
    //             black_box(general_array_has::<i32>(haystack, &sub_array).unwrap());
    //         assert_eq!(&is_contained, &expected);
    //     })
    // });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
