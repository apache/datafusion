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

extern crate criterion;

use std::sync::Arc;

use arrow::array::{ArrayRef, BooleanArray, Int64Array};
use criterion::{Criterion, criterion_group, criterion_main};
use datafusion_functions_aggregate_common::aggregate::groups_accumulator::accumulate::accumulate_indices;

fn generate_group_indices(len: usize) -> Vec<usize> {
    (0..len).collect()
}

fn generate_values(len: usize, has_null: bool) -> ArrayRef {
    if has_null {
        let values = (0..len)
            .map(|i| if i % 7 == 0 { None } else { Some(i as i64) })
            .collect::<Vec<_>>();
        Arc::new(Int64Array::from(values))
    } else {
        let values = (0..len).map(|i| Some(i as i64)).collect::<Vec<_>>();
        Arc::new(Int64Array::from(values))
    }
}

fn generate_filter(len: usize) -> Option<BooleanArray> {
    let values = (0..len)
        .map(|i| {
            if i % 7 == 0 {
                None
            } else if i % 5 == 0 {
                Some(false)
            } else {
                Some(true)
            }
        })
        .collect::<Vec<_>>();
    Some(BooleanArray::from(values))
}

fn criterion_benchmark(c: &mut Criterion) {
    let len = 500_000;
    let group_indices = generate_group_indices(len);
    let rows_count = group_indices.len();
    let values = generate_values(len, true);
    let opt_filter = generate_filter(len);
    let mut counts: Vec<i64> = vec![0; rows_count];
    accumulate_indices(
        &group_indices,
        values.logical_nulls().as_ref(),
        opt_filter.as_ref(),
        |group_index| {
            counts[group_index] += 1;
        },
    );

    c.bench_function("Handle both nulls and filter", |b| {
        b.iter(|| {
            accumulate_indices(
                &group_indices,
                values.logical_nulls().as_ref(),
                opt_filter.as_ref(),
                |group_index| {
                    counts[group_index] += 1;
                },
            );
        })
    });

    c.bench_function("Handle nulls only", |b| {
        b.iter(|| {
            accumulate_indices(
                &group_indices,
                values.logical_nulls().as_ref(),
                None,
                |group_index| {
                    counts[group_index] += 1;
                },
            );
        })
    });

    let values = generate_values(len, false);
    c.bench_function("Handle filter only", |b| {
        b.iter(|| {
            accumulate_indices(
                &group_indices,
                values.logical_nulls().as_ref(),
                opt_filter.as_ref(),
                |group_index| {
                    counts[group_index] += 1;
                },
            );
        })
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
