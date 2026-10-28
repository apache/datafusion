// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use std::sync::Arc;

use arrow::array::{ArrayRef, Int32Array};
use datafusion_physical_plan::aggregates::order::GroupClusteringPartial;

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
    let mut group = c.benchmark_group("group_clustering_partial");

    // Test with 1, 2, 4, and 8 grouping indices
    for num_columns in [1, 2, 4, 8] {
        let grouping_indices: Vec<usize> = (0..num_columns).collect();

        group.bench_function(format!("grouping_indices_{num_columns}"), |b| {
            let batch_group_values = create_test_arrays(num_columns);
            let group_indices: Vec<usize> = (0..BATCH_SIZE).collect();

            b.iter(|| {
                let mut clustering =
                    GroupClusteringPartial::try_new(grouping_indices.clone()).unwrap();
                clustering
                    .new_groups(&batch_group_values, &group_indices, BATCH_SIZE)
                    .unwrap();
            });
        });
    }
    group.finish();
}

criterion_group!(benches, bench_new_groups);
criterion_main!(benches);
