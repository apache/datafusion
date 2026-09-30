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

//! Execute a SQL query over a partitioned table, then report operator metrics.
//! Run with `cargo bench -p datafusion --bench partition_metrics`.

use std::hint::black_box;
use std::sync::Arc;
use std::time::Duration;

use arrow::array::Int32Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use criterion::{BatchSize, BenchmarkId, Criterion, criterion_group, criterion_main};
use datafusion::datasource::MemTable;
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_common::Result;
use datafusion_physical_plan::ExecutionPlan;
use datafusion_physical_plan::metrics::MetricsSet;
use futures::TryStreamExt;

async fn query(partitions: usize) -> Result<(SessionContext, Arc<dyn ExecutionPlan>)> {
    let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
    let batch = RecordBatch::try_new(
        Arc::clone(&schema),
        vec![Arc::new(Int32Array::from_iter_values(0..32))],
    )?;
    let table = MemTable::try_new(schema, vec![vec![batch]; partitions])?;
    let ctx = SessionContext::new_with_config(
        SessionConfig::new()
            .with_target_partitions(partitions)
            .with_batch_size(16),
    );
    ctx.register_table("t", Arc::new(table))?;
    let plan = ctx
        .sql("SELECT a + 1 AS b FROM t WHERE a < 16")
        .await?
        .create_physical_plan()
        .await?;
    assert_eq!(plan.properties().partitioning.partition_count(), partitions);
    Ok((ctx, plan))
}

fn nodes(plan: &Arc<dyn ExecutionPlan>) -> Vec<Arc<dyn ExecutionPlan>> {
    let mut result = vec![Arc::clone(plan)];
    for child in plan.children() {
        result.extend(nodes(child));
    }
    result
}

fn select(metrics: &MetricsSet, partition: usize, indexed: bool) -> usize {
    if indexed {
        metrics
            .for_partition(partition)
            .iter()
            .map(|m| {
                black_box(m.value().as_usize());
                1
            })
            .sum()
    } else {
        metrics
            .iter()
            .filter(|m| m.partition() == Some(partition))
            .map(|m| {
                black_box(m.value().as_usize());
                1
            })
            .sum()
    }
}

async fn execute(ctx: &SessionContext, plan: &Arc<dyn ExecutionPlan>, partition: usize) {
    let batches: Vec<_> = plan
        .execute(partition, ctx.task_ctx())
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 16);
}

fn benchmarks(c: &mut Criterion) {
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let mut retrieval = c.benchmark_group("partition_metrics/sql_retrieval");
    retrieval
        .sample_size(20)
        .warm_up_time(Duration::from_secs(1))
        .measurement_time(Duration::from_secs(3));
    let mut expected_count = None;
    for partitions in [1, 64, 1024, 8192] {
        let (ctx, plan) = runtime.block_on(query(partitions)).unwrap();
        runtime.block_on(async {
            for p in 0..partitions {
                execute(&ctx, &plan, p).await;
            }
        });
        let nodes = nodes(&plan);
        let count: usize = nodes
            .iter()
            .filter_map(|n| n.metrics())
            .map(|m| m.for_partition(0).iter().count())
            .sum();
        assert!(count > 0);
        assert_eq!(*expected_count.get_or_insert(count), count);
        for (label, indexed) in [("full_then_filter", false), ("partition", true)] {
            retrieval.bench_with_input(
                BenchmarkId::new(label, partitions),
                &indexed,
                |b, &indexed| {
                    b.iter(|| {
                        for node in &nodes {
                            if let Some(metrics) = node.metrics() {
                                black_box(select(&metrics, 0, indexed));
                            }
                        }
                    });
                },
            );
        }
    }
    retrieval.finish();

    let mut execution = c.benchmark_group("partition_metrics/sql_execution");
    execution
        .sample_size(10)
        .warm_up_time(Duration::from_secs(1))
        .measurement_time(Duration::from_secs(3));
    for partitions in [64, 1024] {
        for (label, report) in [
            ("execute_only", None),
            ("full_then_filter", Some(false)),
            ("partition", Some(true)),
        ] {
            execution.bench_with_input(
                BenchmarkId::new(label, partitions),
                &report,
                |b, &report| {
                    b.iter_batched(
                        || runtime.block_on(query(partitions)).unwrap(),
                        |(ctx, plan)| {
                            let nodes = nodes(&plan);
                            runtime.block_on(async {
                                let mut previous_snapshots = Vec::new();
                                for partition in 0..partitions {
                                    // Keep the previous snapshots alive while executing and
                                    // registering the next partition's metrics.
                                    execute(&ctx, &plan, partition).await;
                                    if let Some(indexed) = report {
                                        let snapshots: Vec<_> = nodes
                                            .iter()
                                            .filter_map(|n| n.metrics())
                                            .collect();
                                        for metrics in &snapshots {
                                            black_box(select(
                                                metrics, partition, indexed,
                                            ));
                                        }
                                        black_box(&previous_snapshots);
                                        previous_snapshots = snapshots;
                                    }
                                }
                            });
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
