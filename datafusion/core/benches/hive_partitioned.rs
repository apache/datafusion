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
use datafusion::dataframe::DataFrame;
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
        let part_dir = base_dir.join(format!("part_col={}", i));
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

async fn maybe_write_plan(
    df: &DataFrame,
    query_name: &str,
    query_sql: &str,
    file: &mut Option<File>,
    save_plan: bool,
) {
    if !save_plan {
        return;
    }

    if let Some(f) = file {
        let plan = df
            .clone()
            .explain(false, false)
            .unwrap()
            .collect()
            .await
            .unwrap();
        let formatted_plan = pretty_format_batches(&plan).unwrap().to_string();

        writeln!(f, "=== Query: {} [{}] ===", query_name, query_sql).unwrap();
        writeln!(f, "--- Plan ---").unwrap();
        writeln!(f, "{}", formatted_plan).unwrap();
        writeln!(f).unwrap();
    }
}

fn run_benchmark(c: &mut Criterion) {
    // Sweet spot: enough partitions to show benefit, but not so many that task overhead dominates
    // - More rows per partition = more data to shuffle (this is the key!)
    // - Moderate partition count = avoids excessive task scheduling overhead
    let partitions = 100;  // Keep moderate - too many creates task overhead
    let rows_per_partition = 500_000;  // Increased from 10k to 500k (50x more data per partition!)
    let tmp_dir = TempDir::new().unwrap();

    println!("Generating test data in {}", tmp_dir.path().display());
    generate_partitioned_data(tmp_dir.path(), partitions, rows_per_partition);
    let table_path = tmp_dir.path().to_str().unwrap();

    // Check for environment variables to control behavior
    let save_plans = std::env::var("SAVE_PLANS").is_ok();
    let output_filename = "hive_partitioned_benchmark_output.txt";

    let rt = Runtime::new().unwrap();

    // --- Validation Phase ---
    rt.block_on(async {
        let queries = vec![
            (
                "Group By Partition Col",
                "SELECT part_col, count(*) FROM t GROUP BY part_col",
            ),
            (
                "Group By Partition+Val",
                "SELECT part_col, val, count(*) FROM t GROUP BY part_col, val",
            ),
            (
                "Group By Non-Partition Col",
                "SELECT val, count(*) FROM t GROUP BY val",
            ),
        ];

        let mut output_file = if save_plans {
            Some(File::create(output_filename).unwrap())
        } else {
            None
        };

        println!("Running validation checks...");
        for (name, query) in queries.iter() {
            // Optimized
            let config_opt = SessionConfig::new().with_target_partitions(64).set_bool(
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

            // Capture plan + results for comparison
            let df_opt = ctx_opt.sql(query).await.unwrap();
            maybe_write_plan(
                &df_opt,
                &format!("{} (Optimized)", name),
                query,
                &mut output_file,
                save_plans,
            )
            .await;
            let results_opt = df_opt.collect().await.unwrap();

            // Unoptimized
            let config_unopt = SessionConfig::new().with_target_partitions(64).set_bool(
                "datafusion.execution.listing_table_preserve_partition_values",
                false,
            );
            let ctx_unopt = SessionContext::new_with_config(config_unopt);
            ctx_unopt
                .register_parquet("t", table_path, options)
                .await
                .unwrap();

            let df_unopt = ctx_unopt.sql(query).await.unwrap();
            maybe_write_plan(
                &df_unopt,
                &format!("{} (Unoptimized)", name),
                query,
                &mut output_file,
                save_plans,
            )
            .await;
            let results_unopt = df_unopt.collect().await.unwrap();

            // Verify Results Match
            let formatted_opt = pretty_format_batches(&results_opt).unwrap().to_string();
            let formatted_unopt =
                pretty_format_batches(&results_unopt).unwrap().to_string();

            // Since row order is not guaranteed without an ORDER BY, we should ideally sort before comparing.
            // However, for this benchmark, we can just assert that the number of rows is the same
            // or add an ORDER BY to the query if strict equality is needed.
            // Given we are using `pretty_format_batches`, string comparison is sensitive to order.
            // Let's just compare row counts for now to avoid flakiness, or we can sort the output lines.
            assert_eq!(
                results_opt.iter().map(|b| b.num_rows()).sum::<usize>(),
                results_unopt.iter().map(|b| b.num_rows()).sum::<usize>(),
                "Row count mismatch for query: {}",
                name
            );

            // A simple check: if the query has an ORDER BY, we can compare strings.
            // The queries in this benchmark do NOT have ORDER BY.
            // We can verify the content is the same by sorting the output string lines (hacky but works for simple tables).
            let mut lines_opt: Vec<&str> = formatted_opt.lines().collect();
            let mut lines_unopt: Vec<&str> = formatted_unopt.lines().collect();
            lines_opt.sort();
            lines_unopt.sort();
            if lines_opt != lines_unopt {
                eprintln!("Query '{}': Results MISMATCH", name);
                panic!("Content mismatch (sorted lines) for query: {}", name);
            }

            println!("Query '{}': Results MATCH", name);
        }
        if save_plans {
            println!("Validation detailed output written to {}", output_filename);
        }
    });

    // --- Benchmark Phase ---
    // Keep aggregation simple - more data makes repartition overhead more significant
    let query = "SELECT part_col, count(*) FROM t GROUP BY part_col";
    let mut group = c.benchmark_group("hive_partitioned_agg");

    // 1. Benchmark with Optimization ENABLED
    group.bench_function("preserve_partition_values=true", |b| {
        b.to_async(&rt).iter(|| async {
            let config = SessionConfig::new()
                .with_target_partitions(64) // Increased from 16 - more repartition overhead
                .set_bool(
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

    // 2. Benchmark with Optimization DISABLED
    group.bench_function("preserve_partition_values=false", |b| {
        b.to_async(&rt).iter(|| async {
            let config = SessionConfig::new().with_target_partitions(64).set_bool(
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
