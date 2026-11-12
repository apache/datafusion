// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use std::hint::black_box;
use std::sync::Arc;
use std::time::Duration;

use arrow::array::{ArrayRef, AsArray, Float64Array, RecordBatch, StringArray};
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use datafusion::physical_expr::expressions::Column;
use datafusion_physical_expr_common::physical_expr::PhysicalExpr;
use datafusion_spark::function::predicate::at_least_n_non_nulls::SparkAtLeastNNonNullsExpr;
use rand::{Rng, SeedableRng, rngs::StdRng};

fn benchmark(c: &mut Criterion) {
    let mut group = c.benchmark_group("at_least_n_non_nulls");
    group.sample_size(30);
    group.warm_up_time(Duration::from_millis(200));
    group.measurement_time(Duration::from_secs(1));
    for (rows, kind, width, nulls, nans) in [
        (8192, "f64", 4, 0, 0),
        (8192, "f64", 4, 50, 0),
        (8192, "f64", 4, 5, 5),
        (8192, "f64", 32, 0, 0),
        (8192, "f64", 32, 50, 0),
        (8192, "f64", 32, 5, 5),
        (8192, "utf8", 4, 0, 0),
        (8192, "utf8", 4, 50, 0),
        (8192, "utf8", 32, 0, 0),
        (8192, "utf8", 32, 50, 0),
        (65, "f64", 32, 5, 5),
    ] {
        let mut rng = StdRng::seed_from_u64(42);
        let mut counts = vec![0; rows];
        let arrays = (0..width)
            .map(|column| {
                let values = counts
                    .iter_mut()
                    .enumerate()
                    .map(|(row, count)| {
                        let bucket = rng.random_range(0..100);
                        *count += usize::from(bucket >= nulls + nans);
                        if bucket < nulls {
                            None
                        } else if bucket < nulls + nans {
                            Some(f64::NAN)
                        } else {
                            Some((row + column) as f64)
                        }
                    })
                    .collect::<Vec<_>>();
                let array: ArrayRef = match kind {
                    "f64" => Arc::new(Float64Array::from(values)),
                    _ => Arc::new(StringArray::from_iter(
                        values.into_iter().map(|v| v.map(|_| "value")),
                    )),
                };
                (format!("c{column}"), array)
            })
            .collect::<Vec<_>>();
        let batch = RecordBatch::try_from_iter(arrays).unwrap();
        for n in [1, width / 2, width] {
            if rows == 65 && n != width / 2 {
                continue;
            }
            let children: Vec<Arc<dyn PhysicalExpr>> = (0..width)
                .map(|i| Arc::new(Column::new(&format!("c{i}"), i)) as _)
                .collect();
            let expr = SparkAtLeastNNonNullsExpr::new(n as i32, children);
            let expected = counts.iter().map(|&count| count >= n).collect::<Vec<_>>();
            let result = expr
                .evaluate(&batch)
                .unwrap()
                .into_array(batch.num_rows())
                .unwrap();
            assert_eq!(result.null_count(), 0);
            assert_eq!(
                result.as_boolean().values().iter().collect::<Vec<_>>(),
                expected
            );
            group.bench_with_input(
                BenchmarkId::new(
                    format!("{rows}/{kind}_{width}_null{nulls}_nan{nans}"),
                    n,
                ),
                &batch,
                |b, batch| b.iter(|| black_box(expr.evaluate(black_box(batch)).unwrap())),
            );
        }
    }
    group.finish();
}

criterion_group!(benches, benchmark);
criterion_main!(benches);
