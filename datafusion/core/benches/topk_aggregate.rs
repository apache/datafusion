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

use arrow::util::pretty::pretty_format_batches;
use arrow::{datatypes::Schema, record_batch::RecordBatch};
use arrow_array::builder::{Int64Builder, StringBuilder};
use arrow_schema::{DataType, Field, SchemaRef};
use criterion::{criterion_group, criterion_main, Criterion};
use datafusion::physical_plan::{collect, displayable, ExecutionPlan};
use datafusion::prelude::SessionContext;
use datafusion::{datasource::MemTable, error::Result};
use datafusion_common::DataFusionError;
use datafusion_execution::config::SessionConfig;
use datafusion_execution::TaskContext;
use rand_distr::Distribution;
use rand_distr::{Normal, Pareto};
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
    let ctx = SessionContext::with_config(cfg);
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

    let actual = format!("{}", pretty_format_batches(&batches)?);
    let expected_asc = r#"
+----------------------------------+--------------------------+
| trace_id                         | MAX(traces.timestamp_ms) |
+----------------------------------+--------------------------+
| 233dda9b90a120947515b7d62f46683b | 16909000999999           |
| dd5dee780518eac66c2a4bef4bb64536 | 16909000999998           |
| 67b7d474236429b889b53259dd23c9c6 | 16909000999997           |
| e93942f02f96759e4f0c17700dbe944e | 16909000999996           |
| 2b0b92a840de408268dc45534ff14b66 | 16909000999995           |
| 1557b5073068dc4ea1e65b22af727733 | 16909000999994           |
| 01b8ea6ba186370da76c1634992326dc | 16909000999993           |
| 0c5c9fb232710c7f618d8d5719cd9acd | 16909000999992           |
| b36157951cd7d886514cc49c9ff00987 | 16909000999991           |
| 2a377b56d35464771442a7fb147108ee | 16909000999990           |
+----------------------------------+--------------------------+
        "#
    .trim();
    if asc {
        assert_eq!(actual.trim(), expected_asc);
    }

    Ok(())
}

fn make_data(
    partition_cnt: i32,
    sample_cnt: i32,
    asc: bool,
) -> Result<(Arc<Schema>, Vec<Vec<RecordBatch>>), DataFusionError> {
    use rand::Rng;
    use rand::SeedableRng;

    // constants observed from trace data
    let simultaneous_group_cnt = 2000;
    let fitted_shape = 12f64;
    let fitted_scale = 5f64;
    let mean = 0.1;
    let stddev = 1.1;
    let pareto = Pareto::new(fitted_scale, fitted_shape).unwrap();
    let normal = Normal::new(mean, stddev).unwrap();
    let mut rng = rand::rngs::SmallRng::from_seed([0; 32]);

    // populate data
    let schema = test_schema();
    let mut partitions = vec![];
    let mut cur_time = 16909000000000i64;
    for _ in 0..partition_cnt {
        let mut id_builder = StringBuilder::new();
        let mut ts_builder = Int64Builder::new();
        let gen_id = |rng: &mut rand::rngs::SmallRng| {
            rng.gen::<[u8; 16]>()
                .iter()
                .map(|b| format!("{:02x}", b))
                .collect::<String>()
        };
        let gen_sample_cnt =
            |mut rng: &mut rand::rngs::SmallRng| pareto.sample(&mut rng).ceil() as u32;
        let mut group_ids = (0..simultaneous_group_cnt)
            .map(|_| gen_id(&mut rng))
            .collect::<Vec<_>>();
        let mut group_sample_cnts = (0..simultaneous_group_cnt)
            .map(|_| gen_sample_cnt(&mut rng))
            .collect::<Vec<_>>();
        for _ in 0..sample_cnt {
            let random_index = rng.gen_range(0..simultaneous_group_cnt);
            let trace_id = &mut group_ids[random_index];
            let sample_cnt = &mut group_sample_cnts[random_index];
            *sample_cnt -= 1;
            if *sample_cnt == 0 {
                *trace_id = gen_id(&mut rng);
                *sample_cnt = gen_sample_cnt(&mut rng);
            }

            id_builder.append_value(trace_id);
            ts_builder.append_value(cur_time);

            if asc {
                cur_time += 1;
            } else {
                let samp: f64 = normal.sample(&mut rng);
                let samp = samp.round();
                cur_time += samp as i64;
            }
        }

        // convert to MemTable
        let id_col = Arc::new(id_builder.finish());
        let ts_col = Arc::new(ts_builder.finish());
        let batch = RecordBatch::try_new(schema.clone(), vec![id_col, ts_col])?;
        partitions.push(vec![batch]);
    }
    Ok((schema, partitions))
}

fn test_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("trace_id", DataType::Utf8, false),
        Field::new("timestamp_ms", DataType::Int64, false),
    ]))
}

fn criterion_benchmark(c: &mut Criterion) {
    let limit = 10;
    let partitions = 10;
    let samples = 100_000;

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
