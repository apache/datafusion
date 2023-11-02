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
use std::fmt::Write;
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
                .fold(String::new(), |mut output, b| {
                    let _ = write!(output, "{b:02X}");
                    output
                })
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
