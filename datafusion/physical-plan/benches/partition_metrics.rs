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

//! Run with `cargo bench -p datafusion-physical-plan --bench partition_metrics`.
//! SQL cannot express per-task metrics reporting while retaining a shared plan.

#[path = "metrics/plan.rs"]
mod plan;

use std::hint::black_box;
use std::sync::Arc;
use std::time::Duration;

use criterion::{BatchSize, BenchmarkId, Criterion, criterion_group, criterion_main};
use datafusion_execution::TaskContext;
use datafusion_physical_plan::ExecutionPlan;
use datafusion_physical_plan::metrics::MetricsSet;
use futures::{FutureExt, StreamExt, TryStreamExt};

fn legacy_metrics(plan: &dyn ExecutionPlan, partition: usize) -> Option<MetricsSet> {
    plan.metrics().map(|metrics| {
        metrics
            .into_iter()
            .filter(|metric| metric.partition() == Some(partition))
            .collect()
    })
}

fn indexed_metrics(plan: &dyn ExecutionPlan, partition: usize) -> Option<MetricsSet> {
    plan.metrics_for_partition(partition)
}

fn benchmarks(c: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let context = Arc::new(TaskContext::default());
    let mut retrieval = c.benchmark_group("partition_metrics/retrieval");
    retrieval
        .sample_size(30)
        .warm_up_time(Duration::from_secs(1))
        .measurement_time(Duration::from_secs(3));
    let mut target_metric_counts = None;
    for partitions in [1, 64, 1024, 8192] {
        let (nodes, gate) = plan::shared_plan(partitions).unwrap();
        let mut long_stream = runtime.block_on(async {
            let mut stream = nodes[0].execute(0, Arc::clone(&context)).unwrap();
            assert_eq!(stream.next().await.unwrap().unwrap().num_rows(), 16);
            assert!(stream.next().now_or_never().is_none());
            for partition in 1..partitions {
                let batches: Vec<_> = nodes[0]
                    .execute(partition, Arc::clone(&context))
                    .unwrap()
                    .try_collect()
                    .await
                    .unwrap();
                assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 32);
            }
            stream
        });
        let counts: Vec<_> = nodes
            .iter()
            .map(|node| {
                let selected = indexed_metrics(node.as_ref(), 0).unwrap();
                selected.iter().count()
            })
            .collect();
        if let Some(expected) = &target_metric_counts {
            assert_eq!(&counts, expected);
        } else {
            target_metric_counts = Some(counts);
        }
        for (label, report) in [
            (
                "legacy",
                legacy_metrics as fn(&dyn ExecutionPlan, usize) -> Option<MetricsSet>,
            ),
            ("indexed", indexed_metrics),
        ] {
            retrieval.bench_with_input(
                BenchmarkId::new(label, partitions),
                &partitions,
                |b, _| {
                    b.iter(|| {
                        for node in &nodes {
                            black_box(report(node.as_ref(), 0));
                        }
                    })
                },
            );
        }
        retrieval.bench_with_input(
            BenchmarkId::new("full", partitions),
            &partitions,
            |b, _| {
                b.iter(|| {
                    for node in &nodes {
                        black_box(node.metrics());
                    }
                })
            },
        );
        gate.notify_one();
        runtime.block_on(async {
            while let Some(batch) = long_stream.next().await {
                batch.unwrap();
            }
        });
    }
    retrieval.finish();

    let mut execution = c.benchmark_group("partition_metrics/shared_tree");
    execution
        .sample_size(10)
        .warm_up_time(Duration::from_secs(1))
        .measurement_time(Duration::from_secs(3));
    for partitions in [64, 1024, 8192] {
        for (label, report) in [
            (
                "legacy",
                legacy_metrics as fn(&dyn ExecutionPlan, usize) -> Option<MetricsSet>,
            ),
            ("indexed", indexed_metrics),
        ] {
            execution.bench_with_input(
                BenchmarkId::new(label, partitions),
                &partitions,
                |b, &partitions| {
                    b.iter_batched(
                        || plan::shared_plan(partitions).unwrap(),
                        |(nodes, gate)| {
                            runtime.block_on(async {
                                let mut long_stream =
                                    nodes[0].execute(0, Arc::clone(&context)).unwrap();
                                assert_eq!(
                                    long_stream.next().await.unwrap().unwrap().num_rows(),
                                    16
                                );
                                assert!(long_stream.next().now_or_never().is_none());
                                for partition in 1..partitions {
                                    let batches: Vec<_> = nodes[0]
                                        .execute(partition, Arc::clone(&context))
                                        .unwrap()
                                        .try_collect()
                                        .await
                                        .unwrap();
                                    assert_eq!(
                                        batches
                                            .iter()
                                            .map(|b| b.num_rows())
                                            .sum::<usize>(),
                                        32
                                    );
                                    for node in &nodes {
                                        black_box(report(node.as_ref(), partition));
                                    }
                                }
                                gate.notify_one();
                                while let Some(batch) = long_stream.next().await {
                                    batch.unwrap();
                                }
                                for node in &nodes {
                                    black_box(report(node.as_ref(), 0));
                                }
                            })
                        },
                        BatchSize::PerIteration,
                    );
                },
            );
        }
    }
    execution.finish();
}

criterion_group!(benches, benchmarks);
criterion_main!(benches);
