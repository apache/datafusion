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

use arrow::array::{ArrayRef, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use datafusion_expr::{function::AccumulatorArgs, AggregateUDFImpl, GroupsAccumulator};
use datafusion_functions_aggregate::min_max::Min;
use datafusion_physical_expr::expressions::col;

const BATCH_SIZE: usize = 512;
const SPARSE_GROUPS: usize = 16;
const LARGE_TOTAL_GROUPS: usize = 10_000;
const MONOTONIC_BATCHES: usize = 32;
const MONOTONIC_TOTAL_GROUPS: usize = MONOTONIC_BATCHES * BATCH_SIZE;
const LARGE_DENSE_GROUPS: usize = MONOTONIC_TOTAL_GROUPS;

fn prepare_min_accumulator(data_type: &DataType) -> Box<dyn GroupsAccumulator> {
    let field = Field::new("f", data_type.clone(), true).into();
    let schema = Arc::new(Schema::new(vec![Arc::clone(&field)]));
    let accumulator_args = AccumulatorArgs {
        return_field: field,
        schema: &schema,
        ignore_nulls: false,
        order_bys: &[],
        is_reversed: false,
        name: "MIN(f)",
        is_distinct: false,
        exprs: &[col("f", &schema).unwrap()],
    };

    Min::new()
        .create_groups_accumulator(accumulator_args)
        .expect("create min accumulator")
}

fn min_bytes_sparse_groups(c: &mut Criterion) {
    let values: ArrayRef = Arc::new(StringArray::from_iter_values(
        (0..BATCH_SIZE).map(|i| format!("value_{:04}", i % 1024)),
    ));
    let group_indices: Vec<usize> = (0..BATCH_SIZE).map(|i| i % SPARSE_GROUPS).collect();

    c.bench_function("min bytes sparse groups", |b| {
        b.iter(|| {
            let mut accumulator = prepare_min_accumulator(&DataType::Utf8);
            black_box(
                accumulator
                    .update_batch(
                        std::slice::from_ref(&values),
                        &group_indices,
                        None,
                        LARGE_TOTAL_GROUPS,
                    )
                    .expect("update batch"),
            );
        })
    });
}

fn min_bytes_dense_groups(c: &mut Criterion) {
    let values: ArrayRef = Arc::new(StringArray::from_iter_values(
        (0..BATCH_SIZE).map(|i| format!("value_{:04}", i)),
    ));
    let group_indices: Vec<usize> = (0..BATCH_SIZE).collect();

    c.bench_function("min bytes dense groups", |b| {
        b.iter(|| {
            let mut accumulator = prepare_min_accumulator(&DataType::Utf8);
            black_box(
                accumulator
                    .update_batch(
                        std::slice::from_ref(&values),
                        &group_indices,
                        None,
                        BATCH_SIZE,
                    )
                    .expect("update batch"),
            );
        })
    });
}

fn min_bytes_monotonic_group_ids(c: &mut Criterion) {
    let values: ArrayRef = Arc::new(StringArray::from_iter_values(
        (0..BATCH_SIZE).map(|i| format!("value_{:04}", i % 1024)),
    ));
    let group_batches: Vec<Vec<usize>> = (0..MONOTONIC_BATCHES)
        .map(|batch| {
            let start = batch * BATCH_SIZE;
            (0..BATCH_SIZE).map(|i| start + i).collect()
        })
        .collect();

    c.bench_function("min bytes monotonic group ids", |b| {
        b.iter(|| {
            let mut accumulator = prepare_min_accumulator(&DataType::Utf8);
            for group_indices in &group_batches {
                black_box(
                    accumulator
                        .update_batch(
                            std::slice::from_ref(&values),
                            group_indices,
                            None,
                            MONOTONIC_TOTAL_GROUPS,
                        )
                        .expect("update batch"),
                );
            }
        })
    });
}

fn min_bytes_large_dense_groups(c: &mut Criterion) {
    let values: ArrayRef = Arc::new(StringArray::from_iter_values(
        (0..LARGE_DENSE_GROUPS).map(|i| format!("value_{:04}", i)),
    ));
    let group_indices: Vec<usize> = (0..LARGE_DENSE_GROUPS).collect();

    c.bench_function("min bytes large dense groups", |b| {
        b.iter(|| {
            let mut accumulator = prepare_min_accumulator(&DataType::Utf8);
            black_box(
                accumulator
                    .update_batch(
                        std::slice::from_ref(&values),
                        &group_indices,
                        None,
                        LARGE_DENSE_GROUPS,
                    )
                    .expect("update batch"),
            );
        })
    });
}

criterion_group!(
    benches,
    min_bytes_dense_groups,
    min_bytes_sparse_groups,
    min_bytes_monotonic_group_ids,
    min_bytes_large_dense_groups
);
criterion_main!(benches);
