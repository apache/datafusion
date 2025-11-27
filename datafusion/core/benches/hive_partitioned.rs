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

use arrow::array::{ArrayRef, Int32Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use arrow::util::pretty::pretty_format_batches;
use criterion::{criterion_group, criterion_main, Criterion};
use datafusion::prelude::{ParquetReadOptions, SessionConfig, SessionContext};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use std::fs::{self, File};
use std::io::Write;
use std::path::Path;
use std::sync::Arc;
use tempfile::TempDir;
use tokio::runtime::Runtime;

/// Generate partitioned data for benchmarking
/// Creates `num_partitions` directories, each with one parquet file containing `rows_per_partition`.
fn generate_partitioned_data(
    base_dir: &Path,
    num_partitions: usize,
    rows_per_partition: usize,
) {
    let schema = Arc::new(Schema::new(vec![Field::new("val", DataType::Int32, false)]));

    let props = WriterProperties::builder().build();

    for i in 0..num_partitions {
        let part_dir = base_dir.join(format!("part_col={i}"));
        fs::create_dir_all(&part_dir).unwrap();
        let file_path = part_dir.join("data.parquet");
        let file = File::create(file_path).unwrap();

        let mut writer =
            ArrowWriter::try_new(file, schema.clone(), Some(props.clone())).unwrap();

        // Generate data: just a simple sequence
        let vals: Vec<i32> = (0..rows_per_partition).map(|x| x as i32).collect();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vals)) as ArrayRef],
        )
        .unwrap();

        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }
}

/// Save execution plans to file
async fn save_plans(table_path: &str, output_file: &Path) {
    let query = "SELECT part_col, count(*), sum(val), avg(val) FROM t GROUP BY part_col";
    let mut file = File::create(output_file).unwrap();

    writeln!(file, "KeyPartitioned Aggregation Benchmark Plans\n").unwrap();
    writeln!(file, "Query: {query}\n").unwrap();

    // Optimized plan
    let config_opt = SessionConfig::new().with_target_partitions(20).set_bool(
        "datafusion.execution.listing_table_preserve_partition_values",
        true,
    );
    let ctx_opt = SessionContext::new_with_config(config_opt);
    let options = ParquetReadOptions {
        table_partition_cols: vec![("part_col".to_string(), DataType::Int32)],
        ..Default::default()
    };
    ctx_opt
        .register_parquet("t", table_path, options.clone())
        .await
        .unwrap();

    let df_opt = ctx_opt.sql(query).await.unwrap();
    let plan_opt = df_opt
        .explain(false, false)
        .unwrap()
        .collect()
        .await
        .unwrap();
    writeln!(file, "=== WITH KeyPartitioned Optimization ===").unwrap();
    writeln!(file, "{}\n", pretty_format_batches(&plan_opt).unwrap()).unwrap();

    // Unoptimized plan
    let config_unopt = SessionConfig::new().with_target_partitions(20).set_bool(
        "datafusion.execution.listing_table_preserve_partition_values",
        false,
    );
    let ctx_unopt = SessionContext::new_with_config(config_unopt);
    ctx_unopt
        .register_parquet("t", table_path, options)
        .await
        .unwrap();

    let df_unopt = ctx_unopt.sql(query).await.unwrap();
    let plan_unopt = df_unopt
        .explain(false, false)
        .unwrap()
        .collect()
        .await
        .unwrap();
    writeln!(file, "=== WITHOUT KeyPartitioned Optimization ===").unwrap();
    writeln!(file, "{}", pretty_format_batches(&plan_unopt).unwrap()).unwrap();
}

fn run_benchmark(c: &mut Criterion) {
    // Benchmark KeyPartitioned optimization for aggregations on Hive-partitioned tables
    // 20 partitions × 500K rows = 10M total rows
    let partitions = 20;
    let rows_per_partition = 500_000;
    let tmp_dir = TempDir::new().unwrap();

    generate_partitioned_data(tmp_dir.path(), partitions, rows_per_partition);
    let table_path = tmp_dir.path().to_str().unwrap();

    let rt = Runtime::new().unwrap();

    // Save execution plans if SAVE_PLANS env var is set
    if std::env::var("SAVE_PLANS").is_ok() {
        let output_path = Path::new("hive_partitioned_plans.txt");
        rt.block_on(save_plans(table_path, output_path));
        println!("Execution plans saved to {}", output_path.display());
    }

    let query = "SELECT part_col, count(*), sum(val), avg(val) FROM t GROUP BY part_col";
    let mut group = c.benchmark_group("hive_partitioned_agg");

    group.bench_function("with_key_partitioned", |b| {
        b.to_async(&rt).iter(|| async {
            let config = SessionConfig::new().with_target_partitions(20).set_bool(
                "datafusion.execution.listing_table_preserve_partition_values",
                true,
            );
            let ctx = SessionContext::new_with_config(config);

            let options = ParquetReadOptions {
                table_partition_cols: vec![("part_col".to_string(), DataType::Int32)],
                ..Default::default()
            };

            ctx.register_parquet("t", table_path, options)
                .await
                .unwrap();

            let df = ctx.sql(query).await.unwrap();
            df.collect().await.unwrap();
        })
    });

    group.bench_function("without_key_partitioned", |b| {
        b.to_async(&rt).iter(|| async {
            let config = SessionConfig::new().with_target_partitions(20).set_bool(
                "datafusion.execution.listing_table_preserve_partition_values",
                false,
            );
            let ctx = SessionContext::new_with_config(config);

            let options = ParquetReadOptions {
                table_partition_cols: vec![("part_col".to_string(), DataType::Int32)],
                ..Default::default()
            };

            ctx.register_parquet("t", table_path, options)
                .await
                .unwrap();

            let df = ctx.sql(query).await.unwrap();
            df.collect().await.unwrap();
        })
    });

    group.finish();
}

criterion_group!(benches, run_benchmark);
criterion_main!(benches);
