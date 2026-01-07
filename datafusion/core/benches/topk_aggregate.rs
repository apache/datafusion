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

mod data_utils;

use arrow::util::pretty::pretty_format_batches;
use criterion::{Criterion, criterion_group, criterion_main};
use data_utils::make_data;
use datafusion::physical_plan::{collect, displayable};
use datafusion::prelude::SessionContext;
use datafusion::{datasource::MemTable, error::Result};
use datafusion_execution::config::SessionConfig;
use std::hint::black_box;
use std::sync::Arc;
use tokio::runtime::Runtime;

const LIMIT: usize = 10;

async fn create_context(
    partition_cnt: i32,
    sample_cnt: i32,
    asc: bool,
    use_topk: bool,
    use_view: bool,
) -> Result<SessionContext> {
    let (schema, parts) = make_data(partition_cnt, sample_cnt, asc, use_view).unwrap();
    let mem_table = Arc::new(MemTable::try_new(schema, parts).unwrap());

    // Create the DataFrame
    let mut cfg = SessionConfig::new();
    let opts = cfg.options_mut();
    opts.optimizer.enable_topk_aggregation = use_topk;
    let ctx = SessionContext::new_with_config(cfg);
    let _ = ctx.register_table("traces", mem_table)?;

    Ok(ctx)
}

fn run(rt: &Runtime, ctx: SessionContext, limit: usize, use_topk: bool, asc: bool) {
    black_box(rt.block_on(async { aggregate(ctx, limit, use_topk, asc).await })).unwrap();
}

fn run_string(rt: &Runtime, ctx: SessionContext, limit: usize, use_topk: bool) {
    black_box(rt.block_on(async { aggregate_string(ctx, limit, use_topk).await }))
        .unwrap();
}

async fn aggregate(
    ctx: SessionContext,
    limit: usize,
    use_topk: bool,
    asc: bool,
) -> Result<()> {
    let sql = format!(
        "select max(timestamp_ms) from traces group by trace_id order by max(timestamp_ms) desc limit {limit};"
    );
    let df = ctx.sql(sql.as_str()).await?;
    let plan = df.create_physical_plan().await?;
    let actual_phys_plan = displayable(plan.as_ref()).indent(true).to_string();
    assert_eq!(
        actual_phys_plan.contains(&format!("lim=[{limit}]")),
        use_topk
    );

    let batches = collect(plan, ctx.task_ctx()).await?;
    assert_eq!(batches.len(), 1);
    let batch = batches.first().unwrap();
    assert_eq!(batch.num_rows(), LIMIT);

    let actual = format!("{}", pretty_format_batches(&batches)?).to_lowercase();
    let expected_asc = r#"
+--------------------------+
| max(traces.timestamp_ms) |
+--------------------------+
| 16909009999999           |
| 16909009999998           |
| 16909009999997           |
| 16909009999996           |
| 16909009999995           |
| 16909009999994           |
| 16909009999993           |
| 16909009999992           |
| 16909009999991           |
| 16909009999990           |
+--------------------------+
        "#
    .trim();
    if asc {
        assert_eq!(actual.trim(), expected_asc);
    }

    Ok(())
}

/// Benchmark for string aggregate functions with topk optimization.
/// This tests grouping by a numeric column (timestamp_ms) and aggregating
/// a string column (trace_id) with Utf8 or Utf8View data types.
async fn aggregate_string(
    ctx: SessionContext,
    limit: usize,
    use_topk: bool,
) -> Result<()> {
    let sql = format!(
        "select max(trace_id) from traces group by timestamp_ms order by max(trace_id) desc limit {limit};"
    );
    let df = ctx.sql(sql.as_str()).await?;
    let plan = df.create_physical_plan().await?;
    let actual_phys_plan = displayable(plan.as_ref()).indent(true).to_string();
    assert_eq!(
        actual_phys_plan.contains(&format!("lim=[{limit}]")),
        use_topk
    );

    let batches = collect(plan, ctx.task_ctx()).await?;
    assert_eq!(batches.len(), 1);
    let batch = batches.first().unwrap();
    assert_eq!(batch.num_rows(), LIMIT);

    Ok(())
}

fn criterion_benchmark(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let limit = LIMIT;
    let partitions = 10;
    let samples = 1_000_000;

    let ctx = rt
        .block_on(create_context(partitions, samples, false, false, false))
        .unwrap();
    c.bench_function(
        format!("aggregate {} time-series rows", partitions * samples).as_str(),
        |b| b.iter(|| run(&rt, ctx.clone(), limit, false, false)),
    );

    let ctx = rt
        .block_on(create_context(partitions, samples, true, false, false))
        .unwrap();
    c.bench_function(
        format!("aggregate {} worst-case rows", partitions * samples).as_str(),
        |b| b.iter(|| run(&rt, ctx.clone(), limit, false, true)),
    );

    let ctx = rt
        .block_on(create_context(partitions, samples, false, true, false))
        .unwrap();
    c.bench_function(
        format!(
            "top k={limit} aggregate {} time-series rows",
            partitions * samples
        )
        .as_str(),
        |b| b.iter(|| run(&rt, ctx.clone(), limit, true, false)),
    );

    let ctx = rt
        .block_on(create_context(partitions, samples, true, true, false))
        .unwrap();
    c.bench_function(
        format!(
            "top k={limit} aggregate {} worst-case rows",
            partitions * samples
        )
        .as_str(),
        |b| b.iter(|| run(&rt, ctx.clone(), limit, true, true)),
    );

    // Utf8View schema，time-series rows
    let ctx = rt
        .block_on(create_context(partitions, samples, false, true, true))
        .unwrap();
    c.bench_function(
        format!(
            "top k={limit} aggregate {} time-series rows [Utf8View]",
            partitions * samples
        )
        .as_str(),
        |b| b.iter(|| run(&rt, ctx.clone(), limit, true, false)),
    );

    // Utf8View schema，worst-case rows
    let ctx = rt
        .block_on(create_context(partitions, samples, true, true, true))
        .unwrap();
    c.bench_function(
        format!(
            "top k={limit} aggregate {} worst-case rows [Utf8View]",
            partitions * samples
        )
        .as_str(),
        |b| b.iter(|| run(&rt, ctx.clone(), limit, true, true)),
    );

    // String aggregate benchmarks - grouping by timestamp, aggregating string column
    let ctx = rt
        .block_on(create_context(partitions, samples, false, true, false))
        .unwrap();
    c.bench_function(
        format!(
            "top k={limit} string aggregate {} time-series rows [Utf8]",
            partitions * samples
        )
        .as_str(),
        |b| b.iter(|| run_string(&rt, ctx.clone(), limit, true)),
    );

    let ctx = rt
        .block_on(create_context(partitions, samples, true, true, false))
        .unwrap();
    c.bench_function(
        format!(
            "top k={limit} string aggregate {} worst-case rows [Utf8]",
            partitions * samples
        )
        .as_str(),
        |b| b.iter(|| run_string(&rt, ctx.clone(), limit, true)),
    );

    let ctx = rt
        .block_on(create_context(partitions, samples, false, true, true))
        .unwrap();
    c.bench_function(
        format!(
            "top k={limit} string aggregate {} time-series rows [Utf8View]",
            partitions * samples
        )
        .as_str(),
        |b| b.iter(|| run_string(&rt, ctx.clone(), limit, true)),
    );

    let ctx = rt
        .block_on(create_context(partitions, samples, true, true, true))
        .unwrap();
    c.bench_function(
        format!(
            "top k={limit} string aggregate {} worst-case rows [Utf8View]",
            partitions * samples
        )
        .as_str(),
        |b| b.iter(|| run_string(&rt, ctx.clone(), limit, true)),
    );
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
