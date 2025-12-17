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
use datafusion::physical_plan::{ExecutionPlan, collect, displayable};
use datafusion::prelude::SessionContext;
use datafusion::{datasource::MemTable, error::Result};
use datafusion_execution::TaskContext;
use datafusion_execution::config::SessionConfig;
use std::hint::black_box;
use std::sync::Arc;
use tokio::runtime::Runtime;

async fn create_context(
    limit: usize,
    partition_cnt: i32,
    sample_cnt: i32,
    asc: bool,
    use_topk: bool,
    use_view: bool,
) -> Result<(Arc<dyn ExecutionPlan>, Arc<TaskContext>)> {
    let (schema, parts) = make_data(partition_cnt, sample_cnt, asc, use_view).unwrap();
    let mem_table = Arc::new(MemTable::try_new(schema, parts).unwrap());

    // Create the DataFrame
    let mut cfg = SessionConfig::new();
    let opts = cfg.options_mut();
    opts.optimizer.enable_topk_aggregation = use_topk;
    let ctx = SessionContext::new_with_config(cfg);
    let _ = ctx.register_table("traces", mem_table)?;
    let sql = format!(
        "select max(timestamp_ms) from traces group by trace_id order by max(timestamp_ms) desc limit {limit};"
    );
    let df = ctx.sql(sql.as_str()).await?;
    let physical_plan = df.create_physical_plan().await?;
    let actual_phys_plan = displayable(physical_plan.as_ref()).indent(true).to_string();
    assert_eq!(
        actual_phys_plan.contains(&format!("lim=[{limit}]")),
        use_topk
    );

    Ok((physical_plan, ctx.task_ctx()))
}

#[expect(clippy::needless_pass_by_value)]
fn run(rt: &Runtime, plan: Arc<dyn ExecutionPlan>, ctx: Arc<TaskContext>, asc: bool) {
    black_box(rt.block_on(async { aggregate(plan.clone(), ctx.clone(), asc).await }))
        .unwrap();
}

async fn aggregate(
    plan: Arc<dyn ExecutionPlan>,
    ctx: Arc<TaskContext>,
    asc: bool,
) -> Result<()> {
    let batches = collect(plan, ctx).await?;
    assert_eq!(batches.len(), 1);
    let batch = batches.first().unwrap();
    assert_eq!(batch.num_rows(), 10);

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

fn criterion_benchmark(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let limit = 10;
    let partitions = 10;
    let samples = 1_000_000;

    c.bench_function(
        format!("aggregate {} time-series rows", partitions * samples).as_str(),
        |b| {
            b.iter(|| {
                let real = rt.block_on(async {
                    create_context(limit, partitions, samples, false, false, false)
                        .await
                        .unwrap()
                });
                run(&rt, real.0.clone(), real.1.clone(), false)
            })
        },
    );

    c.bench_function(
        format!("aggregate {} worst-case rows", partitions * samples).as_str(),
        |b| {
            b.iter(|| {
                let asc = rt.block_on(async {
                    create_context(limit, partitions, samples, true, false, false)
                        .await
                        .unwrap()
                });
                run(&rt, asc.0.clone(), asc.1.clone(), true)
            })
        },
    );

    c.bench_function(
        format!(
            "top k={limit} aggregate {} time-series rows",
            partitions * samples
        )
        .as_str(),
        |b| {
            b.iter(|| {
                let topk_real = rt.block_on(async {
                    create_context(limit, partitions, samples, false, true, false)
                        .await
                        .unwrap()
                });
                run(&rt, topk_real.0.clone(), topk_real.1.clone(), false)
            })
        },
    );

    c.bench_function(
        format!(
            "top k={limit} aggregate {} worst-case rows",
            partitions * samples
        )
        .as_str(),
        |b| {
            b.iter(|| {
                let topk_asc = rt.block_on(async {
                    create_context(limit, partitions, samples, true, true, false)
                        .await
                        .unwrap()
                });
                run(&rt, topk_asc.0.clone(), topk_asc.1.clone(), true)
            })
        },
    );

    // Utf8View schema，time-series rows
    c.bench_function(
        format!(
            "top k={limit} aggregate {} time-series rows [Utf8View]",
            partitions * samples
        )
        .as_str(),
        |b| {
            b.iter(|| {
                let topk_real = rt.block_on(async {
                    create_context(limit, partitions, samples, false, true, true)
                        .await
                        .unwrap()
                });
                run(&rt, topk_real.0.clone(), topk_real.1.clone(), false)
            })
        },
    );

    // Utf8View schema，worst-case rows
    c.bench_function(
        format!(
            "top k={limit} aggregate {} worst-case rows [Utf8View]",
            partitions * samples
        )
        .as_str(),
        |b| {
            b.iter(|| {
                let topk_asc = rt.block_on(async {
                    create_context(limit, partitions, samples, true, true, true)
                        .await
                        .unwrap()
                });
                run(&rt, topk_asc.0.clone(), topk_asc.1.clone(), true)
            })
        },
    );
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
