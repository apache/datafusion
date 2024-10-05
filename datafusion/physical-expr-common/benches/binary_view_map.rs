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

use arrow::array::ArrayRef;
use arrow::util::bench_util::create_string_view_array_with_len;
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use datafusion_physical_expr_common::{
    binary_map::OutputType, binary_view_map::ArrowBytesViewMap,
};

fn benchmark_arrow_bytes_view_map(c: &mut Criterion) {
    let sizes = [100_000, 1_000_000];
    let null_densities = [0.1, 0.5];
    let string_lengths = [20, 50];

    for &num_items in &sizes {
        for &null_density in &null_densities {
            for &str_len in &string_lengths {
                let array: ArrayRef = Arc::new(create_string_view_array_with_len(
                    num_items,
                    null_density,
                    str_len,
                    false,
                ));

                c.bench_function(
                    &format!(
                        "ArrowBytesViewMap insert_if_new - items: {}, null_density: {:.1}, str_len: {}",
                        num_items, null_density, str_len
                    ),
                    |b| {
                        b.iter(|| {
                            let mut map = ArrowBytesViewMap::<()>::new(OutputType::Utf8View);
                            map.insert_if_new(black_box(&array), |_| {}, |_| {}, |_| {});
                            black_box(&map);
                        });
                    },
                );

                let mut map = ArrowBytesViewMap::<i32>::new(OutputType::Utf8View);
                map.insert_if_new(&array, |_| 1i32, |_| {}, |_| {});

                c.bench_function(
                    &format!(
                        "ArrowBytesViewMap get_payloads - items: {}, null_density: {:.1}, str_len: {}",
                        num_items, null_density, str_len
                    ),
                    |b| {
                        b.iter(|| {
                            let payloads = map.take().get_payloads(black_box(&array));
                            black_box(payloads);
                        });
                    },
                );
            }
        }
    }
}

criterion_group!(benches, benchmark_arrow_bytes_view_map);
criterion_main!(benches);
