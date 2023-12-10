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
use criterion::{criterion_group, criterion_main, Criterion};
use data_utils::make_data;
use datafusion::physical_plan::{collect, displayable, ExecutionPlan};
use datafusion::prelude::SessionContext;
use datafusion::{datasource::MemTable, error::Result};
use datafusion_execution::config::SessionConfig;
use datafusion_execution::TaskContext;
use std::sync::Arc;
use tokio::runtime::Runtime;

async fn create_context(
    limit: usize,
    partition_cnt: i32,
    sample_cnt: i32,
    asc: bool,
    use_topk: bool,
) -> Result<(Arc<dyn ExecutionPlan>, Arc<TaskContext>)> {
    let (schema, parts) = make_data(partition_cnt, sample_cnt, asc).unwrap();
    let mem_table = Arc::new(MemTable::try_new(schema, parts).unwrap());

    // Create the DataFrame
    let mut cfg = SessionConfig::new();
    let opts = cfg.options_mut();
    opts.optimizer.enable_topk_aggregation = use_topk;
    let ctx = SessionContext::new_with_config(cfg);
    let _ = ctx.register_table("traces", mem_table)?;
    let sql = format!("select trace_id, max(timestamp_ms) from traces group by trace_id order by max(timestamp_ms) desc limit {limit};");
    let df = ctx.sql(sql.as_str()).await?;
    let physical_plan = df.create_physical_plan().await?;
    let actual_phys_plan = displayable(physical_plan.as_ref()).indent(true).to_string();
    assert_eq!(
        actual_phys_plan.contains(&format!("lim=[{limit}]")),
        use_topk
    );

    Ok((physical_plan, ctx.task_ctx()))
}

fn run(plan: Arc<dyn ExecutionPlan>, ctx: Arc<TaskContext>, asc: bool) {
    let rt = Runtime::new().unwrap();
    criterion::black_box(
        rt.block_on(async { aggregate(plan.clone(), ctx.clone(), asc).await }),
    )
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
+----------------------------------+--------------------------+
| trace_id                         | max(traces.timestamp_ms) |
+----------------------------------+--------------------------+
| 5868861a23ed31355efc5200eb80fe74 | 16909009999999           |
| 4040e64656804c3d77320d7a0e7eb1f0 | 16909009999998           |
| 02801bbe533190a9f8713d75222f445d | 16909009999997           |
| 9e31b3b5a620de32b68fefa5aeea57f1 | 16909009999996           |
| 2d88a860e9bd1cfaa632d8e7caeaa934 | 16909009999995           |
| a47edcef8364ab6f191dd9103e51c171 | 16909009999994           |
| 36a3fa2ccfbf8e00337f0b1254384db6 | 16909009999993           |
| 0756be84f57369012e10de18b57d8a2f | 16909009999992           |
| d4d6bf9845fa5897710e3a8db81d5907 | 16909009999991           |
| 3c2cc1abe728a66b61e14880b53482a0 | 16909009999990           |
+----------------------------------+--------------------------+
        "#
    .trim();
    if asc {
        assert_eq!(actual.trim(), expected_asc);
    }

    Ok(())
}

fn criterion_benchmark(c: &mut Criterion) {
    let limit = 10;
    let partitions = 10;
    let samples = 1_000_000;

    let rt = Runtime::new().unwrap();
    let topk_real = rt.block_on(async {
        create_context(limit, partitions, samples, false, true)
            .await
            .unwrap()
    });
    let topk_asc = rt.block_on(async {
        create_context(limit, partitions, samples, true, true)
            .await
            .unwrap()
    });
    let real = rt.block_on(async {
        create_context(limit, partitions, samples, false, false)
            .await
            .unwrap()
    });
    let asc = rt.block_on(async {
        create_context(limit, partitions, samples, true, false)
            .await
            .unwrap()
    });

    c.bench_function(
        format!("aggregate {} time-series rows", partitions * samples).as_str(),
        |b| b.iter(|| run(real.0.clone(), real.1.clone(), false)),
    );

    c.bench_function(
        format!("aggregate {} worst-case rows", partitions * samples).as_str(),
        |b| b.iter(|| run(asc.0.clone(), asc.1.clone(), true)),
    );

    c.bench_function(
        format!(
            "top k={limit} aggregate {} time-series rows",
            partitions * samples
        )
        .as_str(),
        |b| b.iter(|| run(topk_real.0.clone(), topk_real.1.clone(), false)),
    );

    c.bench_function(
        format!(
            "top k={limit} aggregate {} worst-case rows",
            partitions * samples
        )
        .as_str(),
        |b| b.iter(|| run(topk_asc.0.clone(), topk_asc.1.clone(), true)),
    );
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
