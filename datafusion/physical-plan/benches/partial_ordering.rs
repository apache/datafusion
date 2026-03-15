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

use arrow::array::{ArrayRef, Int32Array};
use datafusion_physical_plan::aggregates::order::GroupOrderingPartial;

use criterion::{Criterion, criterion_group, criterion_main};

const BATCH_SIZE: usize = 8192;

fn create_test_arrays(num_columns: usize) -> Vec<ArrayRef> {
    (0..num_columns)
        .map(|i| {
            Arc::new(Int32Array::from_iter_values(
                (0..BATCH_SIZE as i32).map(|x| x * (i + 1) as i32),
            )) as ArrayRef
        })
        .collect()
}
fn bench_new_groups(c: &mut Criterion) {
    let mut group = c.benchmark_group("group_ordering_partial");

    // Test with 1, 2, 4, and 8 order indices
    for num_columns in [1, 2, 4, 8] {
        let order_indices: Vec<usize> = (0..num_columns).collect();

        group.bench_function(format!("order_indices_{num_columns}"), |b| {
            let batch_group_values = create_test_arrays(num_columns);
            let group_indices: Vec<usize> = (0..BATCH_SIZE).collect();

            b.iter(|| {
                let mut ordering =
                    GroupOrderingPartial::try_new(order_indices.clone()).unwrap();
                ordering
                    .new_groups(&batch_group_values, &group_indices, BATCH_SIZE)
                    .unwrap();
            });
        });
    }
    group.finish();
}

criterion_group!(benches, bench_new_groups);
criterion_main!(benches);
