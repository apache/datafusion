// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Benchmark for `Statistics::try_merge_iter`.

use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema};
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use datafusion_common::stats::Precision;
use datafusion_common::{ColumnStatistics, ScalarValue, Statistics};

/// Build a vector of `n` with `num_cols` columns
fn make_stats(n: usize, num_cols: usize) -> Vec<Statistics> {
    (0..n)
        .map(|i| {
            let mut stats = Statistics::default()
                .with_num_rows(Precision::Exact(100 + i))
                .with_total_byte_size(Precision::Exact(8000 + i * 80));
            for c in 0..num_cols {
                let base = (i * num_cols + c) as i64;
                stats = stats.add_column_statistics(
                    ColumnStatistics::new_unknown()
                        .with_null_count(Precision::Exact(i))
                        .with_min_value(Precision::Exact(ScalarValue::Int64(Some(base))))
                        .with_max_value(Precision::Exact(ScalarValue::Int64(Some(
                            base + 1000,
                        ))))
                        .with_sum_value(Precision::Exact(ScalarValue::Int64(Some(
                            base * 100,
                        )))),
                );
            }
            stats
        })
        .collect()
}

fn bench_stats_merge(c: &mut Criterion) {
    let mut group = c.benchmark_group("stats_merge");

    for &num_partitions in &[10, 100, 500] {
        for &num_cols in &[1, 5, 20] {
            let items = make_stats(num_partitions, num_cols);
            let schema = Arc::new(Schema::new(
                (0..num_cols)
                    .map(|i| Field::new(format!("col{i}"), DataType::Int64, true))
                    .collect::<Vec<_>>(),
            ));

            let param = format!("{num_partitions}parts_{num_cols}cols");

            group.bench_with_input(
                BenchmarkId::new("try_merge_iter", &param),
                &(&items, &schema),
                |b, (items, schema)| {
                    b.iter(|| {
                        std::hint::black_box(
                            Statistics::try_merge_iter(*items, schema).unwrap(),
                        );
                    });
                },
            );
        }
    }

    group.finish();
}

criterion_group!(benches, bench_stats_merge);
criterion_main!(benches);
