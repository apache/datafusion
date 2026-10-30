// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

// A minimal benchmark of the min_max accumulator for byte-like data types.
//
// The benchmark simulates the insertion of NUM_BATCHES batches into an aggregation,
// where every row belongs to a distinct group. The data generated beforehand to
// ensure that (mostly) the cost of the update_batch method is measured.
//
// The throughput value describes the rows per second that are ingested.

use std::sync::Arc;

use arrow::{
    array::{ArrayRef, StringArray},
    datatypes::{DataType, Field, Schema},
};
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use datafusion_expr::{GroupsAccumulator, function::AccumulatorArgs};
use datafusion_functions_aggregate::min_max;
use datafusion_physical_expr::expressions::col;

const BATCH_SIZE: usize = 8192;

fn create_max_bytes_accumulator() -> Box<dyn GroupsAccumulator> {
    let input_schema =
        Arc::new(Schema::new(vec![Field::new("value", DataType::Utf8, true)]));

    let max = min_max::max_udaf();
    max.create_groups_accumulator(AccumulatorArgs {
        return_field: Arc::new(Field::new("value", DataType::Utf8, true)),
        schema: &input_schema,
        expr_fields: &[Field::new("value", DataType::Utf8, true).into()],
        ignore_nulls: true,
        order_bys: &[],
        is_reversed: false,
        name: "max_utf8",
        is_distinct: true,
        exprs: &[col("value", &input_schema).unwrap()],
    })
    .unwrap()
}

fn bench_min_max_bytes(c: &mut Criterion) {
    let mut group = c.benchmark_group("min_max_bytes");

    for num_batches in [10, 20, 50, 100, 150, 200, 300, 400, 500] {
        let id = BenchmarkId::from_parameter(num_batches);
        group.throughput(Throughput::Elements((num_batches * BATCH_SIZE) as u64));
        group.bench_with_input(id, &num_batches, |bencher, num_batches| {
            bencher.iter_with_large_drop(|| {
                let mut accumulator = create_max_bytes_accumulator();
                let mut group_indices = Vec::with_capacity(BATCH_SIZE);
                let strings: ArrayRef = Arc::new(StringArray::from_iter_values(
                    (0..BATCH_SIZE).map(|i| i.to_string()),
                ));

                for batch_idx in 0..*num_batches {
                    group_indices.clear();
                    group_indices
                        .extend((batch_idx * BATCH_SIZE)..(batch_idx + 1) * BATCH_SIZE);
                    let total_num_groups = (batch_idx + 1) * BATCH_SIZE;

                    accumulator
                        .update_batch(
                            &[Arc::clone(&strings)],
                            &group_indices,
                            None,
                            total_num_groups,
                        )
                        .unwrap()
                }
            });
        });
    }
}

criterion_group!(benches, bench_min_max_bytes);
criterion_main!(benches);
