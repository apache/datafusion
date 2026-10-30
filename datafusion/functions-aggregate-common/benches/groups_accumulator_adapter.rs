// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Benchmarks the uninstrumented `GroupsAccumulatorAdapter` update path.

use std::hint::black_box;
use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array};
use arrow::datatypes::DataType;
use criterion::{BatchSize, Criterion, criterion_group, criterion_main};
use datafusion_expr_common::accumulator::Accumulator;
use datafusion_expr_common::groups_accumulator::GroupsAccumulator;
use datafusion_functions_aggregate_common::aggregate::groups_accumulator::GroupsAccumulatorAdapter;
use datafusion_functions_aggregate_common::min_max::MaxAccumulator;

const NUM_GROUPS: usize = 8_192;

fn groups_accumulator_adapter(c: &mut Criterion) {
    let values: ArrayRef = Arc::new(Int64Array::from_iter_values(0..NUM_GROUPS as i64));
    let group_indices: Vec<_> = (0..NUM_GROUPS).collect();

    c.bench_function("groups_accumulator_adapter/update_batch/8192_groups", |b| {
        b.iter_batched(
            || {
                GroupsAccumulatorAdapter::new(|| {
                    Ok(Box::new(MaxAccumulator::try_new(&DataType::Int64)?)
                        as Box<dyn Accumulator>)
                })
            },
            |mut accumulator| {
                accumulator
                    .update_batch(
                        &[Arc::clone(&values)],
                        &group_indices,
                        None,
                        NUM_GROUPS,
                    )
                    .unwrap();
                black_box(accumulator);
            },
            BatchSize::SmallInput,
        );
    });
}

criterion_group!(benches, groups_accumulator_adapter);
criterion_main!(benches);
