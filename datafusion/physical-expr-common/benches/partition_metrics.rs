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

//! Container costs; run alongside the physical-plan shared-tree benchmark.

use std::hint::black_box;
use std::sync::Arc;
use std::time::Duration;

use criterion::{BatchSize, BenchmarkId, Criterion, criterion_group, criterion_main};
use datafusion_physical_expr_common::metrics::{
    Count, ExecutionPlanMetricsSet, Metric, MetricValue, MetricsSet,
};

fn fixture(partitions: usize) -> MetricsSet {
    (0..partitions)
        .flat_map(|partition| {
            (0..8).map(move |_| {
                Arc::new(Metric::new(
                    MetricValue::OutputRows(Count::new()),
                    Some(partition),
                ))
            })
        })
        .collect()
}

fn indexed_metrics(metrics: &ExecutionPlanMetricsSet, partition: usize) -> MetricsSet {
    metrics.clone_partition(partition)
}

fn benchmarks(c: &mut Criterion) {
    let mut group = c.benchmark_group("metrics_container");
    group
        .sample_size(30)
        .warm_up_time(Duration::from_secs(1))
        .measurement_time(Duration::from_secs(3));
    for partitions in [1, 64, 1024, 8192, 32768] {
        let input = fixture(partitions);
        let metrics = ExecutionPlanMetricsSet::from(input.clone());
        group.bench_with_input(
            BenchmarkId::new("indexed", partitions),
            &partitions,
            |b, _| {
                b.iter(|| black_box(indexed_metrics(&metrics, 0)));
            },
        );
        group.bench_with_input(
            BenchmarkId::new("full", partitions),
            &partitions,
            |b, _| {
                b.iter(|| black_box(metrics.clone_inner()));
            },
        );
        group.bench_with_input(
            BenchmarkId::new("legacy", partitions),
            &partitions,
            |b, _| {
                b.iter(|| {
                    black_box(
                        metrics
                            .clone_inner()
                            .into_iter()
                            .filter(|m| m.partition() == Some(0))
                            .collect::<MetricsSet>(),
                    )
                });
            },
        );
        group.bench_with_input(
            BenchmarkId::new("register", partitions),
            &partitions,
            |b, _| {
                b.iter_batched(
                    ExecutionPlanMetricsSet::new,
                    |registered| {
                        for metric in &input {
                            registered.register(Arc::clone(metric));
                        }
                        black_box(registered)
                    },
                    BatchSize::PerIteration,
                );
            },
        );
        group.bench_with_input(
            BenchmarkId::new("from", partitions),
            &partitions,
            |b, _| {
                b.iter_batched(
                    || input.clone(),
                    |input| black_box(ExecutionPlanMetricsSet::from(input)),
                    BatchSize::PerIteration,
                );
            },
        );
    }
    group.finish();
}

criterion_group!(benches, benchmarks);
criterion_main!(benches);
