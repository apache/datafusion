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

#[macro_use]
extern crate criterion;
extern crate arrow;
extern crate datafusion;

mod data_utils;
use crate::criterion::Criterion;
use data_utils::{create_table_provider, make_data};
use datafusion::execution::context::SessionContext;
use datafusion::physical_plan::{collect, ExecutionPlan};
use datafusion::{datasource::MemTable, error::Result};
use datafusion_execution::config::SessionConfig;
use datafusion_execution::TaskContext;

use parking_lot::Mutex;
use std::{sync::Arc, time::Duration};
use tokio::runtime::Runtime;

fn query(ctx: Arc<Mutex<SessionContext>>, sql: &str) {
    let rt = Runtime::new().unwrap();
    let df = rt.block_on(ctx.lock().sql(sql)).unwrap();
    criterion::black_box(rt.block_on(df.collect()).unwrap());
}

fn create_context(
    partitions_len: usize,
    array_len: usize,
    batch_size: usize,
) -> Result<Arc<Mutex<SessionContext>>> {
    let ctx = SessionContext::new();
    let provider = create_table_provider(partitions_len, array_len, batch_size)?;
    ctx.register_table("t", provider)?;
    Ok(Arc::new(Mutex::new(ctx)))
}

fn criterion_benchmark_limited_distinct(c: &mut Criterion) {
    let partitions_len = 10;
    let array_len = 1 << 26; // 64 M
    let batch_size = 32768;
    let ctx = create_context(partitions_len, array_len, batch_size).unwrap();

    let mut group = c.benchmark_group("custom-measurement-time");
    group.measurement_time(Duration::from_secs(40));

    group.bench_function("distinct_group_by_u64_narrow_limit_10", |b| {
        b.iter(|| {
            query(
                ctx.clone(),
                "SELECT DISTINCT u64_narrow FROM t GROUP BY u64_narrow LIMIT 10",
            )
        })
    });

    group.bench_function("distinct_group_by_u64_narrow_limit_100", |b| {
        b.iter(|| {
            query(
                ctx.clone(),
                "SELECT DISTINCT u64_narrow FROM t GROUP BY u64_narrow LIMIT 100",
            )
        })
    });

    group.bench_function("distinct_group_by_u64_narrow_limit_1000", |b| {
        b.iter(|| {
            query(
                ctx.clone(),
                "SELECT DISTINCT u64_narrow FROM t GROUP BY u64_narrow LIMIT 1000",
            )
        })
    });

    group.bench_function("distinct_group_by_u64_narrow_limit_10000", |b| {
        b.iter(|| {
            query(
                ctx.clone(),
                "SELECT DISTINCT u64_narrow FROM t GROUP BY u64_narrow LIMIT 10000",
            )
        })
    });

    group.bench_function("group_by_multiple_columns_limit_10", |b| {
        b.iter(|| {
            query(
                ctx.clone(),
                "SELECT u64_narrow, u64_wide, utf8, f64 FROM t GROUP BY 1, 2, 3, 4 LIMIT 10",
            )
        })
    });
    group.finish();
}

async fn distinct_with_limit(
    plan: Arc<dyn ExecutionPlan>,
    ctx: Arc<TaskContext>,
) -> Result<()> {
    let batches = collect(plan, ctx).await?;
    assert_eq!(batches.len(), 1);
    let batch = batches.first().unwrap();
    assert_eq!(batch.num_rows(), 10);

    Ok(())
}

fn run(plan: Arc<dyn ExecutionPlan>, ctx: Arc<TaskContext>) {
    let rt = Runtime::new().unwrap();
    criterion::black_box(
        rt.block_on(async { distinct_with_limit(plan.clone(), ctx.clone()).await }),
    )
    .unwrap();
}

pub async fn create_context_sampled_data(
    sql: &str,
    partition_cnt: i32,
    sample_cnt: i32,
) -> Result<(Arc<dyn ExecutionPlan>, Arc<TaskContext>)> {
    let (schema, parts) = make_data(partition_cnt, sample_cnt, false /* asc */).unwrap();
    let mem_table = Arc::new(MemTable::try_new(schema, parts).unwrap());

    // Create the DataFrame
    let cfg = SessionConfig::new();
    let ctx = SessionContext::new_with_config(cfg);
    let _ = ctx.register_table("traces", mem_table)?;
    let df = ctx.sql(sql).await?;
    let physical_plan = df.create_physical_plan().await?;
    Ok((physical_plan, ctx.task_ctx()))
}

fn criterion_benchmark_limited_distinct_sampled(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();

    let limit = 10;
    let partitions = 100;
    let samples = 100_000;
    let sql =
        format!("select DISTINCT trace_id from traces group by trace_id limit {limit};");

    let distinct_trace_id_100_partitions_100_000_samples_limit_100 = rt.block_on(async {
        create_context_sampled_data(sql.as_str(), partitions, samples)
            .await
            .unwrap()
    });

    c.bench_function(
        format!("distinct query with {} partitions and {} samples per partition with limit {}", partitions, samples, limit).as_str(),
        |b| b.iter(|| run(distinct_trace_id_100_partitions_100_000_samples_limit_100.0.clone(),
                                   distinct_trace_id_100_partitions_100_000_samples_limit_100.1.clone())),
    );

    let partitions = 10;
    let samples = 1_000_000;
    let sql =
        format!("select DISTINCT trace_id from traces group by trace_id limit {limit};");

    let distinct_trace_id_10_partitions_1_000_000_samples_limit_10 = rt.block_on(async {
        create_context_sampled_data(sql.as_str(), partitions, samples)
            .await
            .unwrap()
    });

    c.bench_function(
        format!("distinct query with {} partitions and {} samples per partition with limit {}", partitions, samples, limit).as_str(),
        |b| b.iter(|| run(distinct_trace_id_10_partitions_1_000_000_samples_limit_10.0.clone(),
                                   distinct_trace_id_10_partitions_1_000_000_samples_limit_10.1.clone())),
    );

    let partitions = 1;
    let samples = 10_000_000;
    let sql =
        format!("select DISTINCT trace_id from traces group by trace_id limit {limit};");

    let rt = Runtime::new().unwrap();
    let distinct_trace_id_1_partition_10_000_000_samples_limit_10 = rt.block_on(async {
        create_context_sampled_data(sql.as_str(), partitions, samples)
            .await
            .unwrap()
    });

    c.bench_function(
        format!("distinct query with {} partitions and {} samples per partition with limit {}", partitions, samples, limit).as_str(),
        |b| b.iter(|| run(distinct_trace_id_1_partition_10_000_000_samples_limit_10.0.clone(),
                                   distinct_trace_id_1_partition_10_000_000_samples_limit_10.1.clone())),
    );
}

criterion_group!(
    benches,
    criterion_benchmark_limited_distinct,
    criterion_benchmark_limited_distinct_sampled
);
criterion_main!(benches);
