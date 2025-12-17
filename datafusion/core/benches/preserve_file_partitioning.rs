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

//! Benchmark for `preserve_file_partitions` optimization.
//!
//! When enabled, this optimization declares Hive-partitioned tables as
//! `Hash([partition_col])` partitioned, allowing the query optimizer to
//! skip unnecessary repartitioning and sorting operations.
//!
//! When This Optimization Helps
//! - Window functions: PARTITION BY on partition column eliminates RepartitionExec and SortExec
//! - Aggregates with ORDER BY: GROUP BY partition column and ORDER BY eliminates post aggregate sort
//!
//! When This Optimization Does NOT Help
//! - GROUP BY non-partition columns: Required Hash distribution doesn't match declared partitioning
//! - When the number of distinct file partitioning groups < the number of CPUs available: Reduces
//!   parallelization, thus may outweigh the pros of reduced shuffles
//!
//! Usage
//! - BENCH_SIZE=small|medium|large cargo bench -p datafusion --bench preserve_file_partitions
//! - SAVE_PLANS=1 cargo bench ...  # Save query plans to files

use arrow::array::{ArrayRef, Float64Array, StringArray, TimestampMillisecondArray};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::record_batch::RecordBatch;
use arrow::util::pretty::pretty_format_batches;
use criterion::{Criterion, criterion_group, criterion_main};
use datafusion::prelude::{ParquetReadOptions, SessionConfig, SessionContext, col};
use datafusion_expr::SortExpr;
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use std::fs::{self, File};
use std::io::Write;
use std::path::Path;
use std::sync::Arc;
use tempfile::TempDir;
use tokio::runtime::Runtime;

#[derive(Debug, Clone, Copy)]
struct BenchConfig {
    fact_partitions: usize,
    rows_per_partition: usize,
    target_partitions: usize,
    measurement_time_secs: u64,
}

impl BenchConfig {
    fn small() -> Self {
        Self {
            fact_partitions: 10,
            rows_per_partition: 1_000_000,
            target_partitions: 10,
            measurement_time_secs: 15,
        }
    }

    fn medium() -> Self {
        Self {
            fact_partitions: 30,
            rows_per_partition: 3_000_000,
            target_partitions: 30,
            measurement_time_secs: 30,
        }
    }

    fn large() -> Self {
        Self {
            fact_partitions: 50,
            rows_per_partition: 6_000_000,
            target_partitions: 50,
            measurement_time_secs: 90,
        }
    }

    fn from_env() -> Self {
        match std::env::var("BENCH_SIZE").as_deref() {
            Ok("small") | Ok("SMALL") => Self::small(),
            Ok("medium") | Ok("MEDIUM") => Self::medium(),
            Ok("large") | Ok("LARGE") => Self::large(),
            _ => {
                println!("Using SMALL dataset (set BENCH_SIZE=small|medium|large)");
                Self::small()
            }
        }
    }

    fn total_rows(&self) -> usize {
        self.fact_partitions * self.rows_per_partition
    }

    fn high_cardinality(base: &Self) -> Self {
        Self {
            fact_partitions: (base.fact_partitions as f64 * 2.5) as usize,
            rows_per_partition: base.rows_per_partition / 2,
            target_partitions: base.target_partitions,
            measurement_time_secs: base.measurement_time_secs,
        }
    }
}

fn dkey_names(count: usize) -> Vec<String> {
    (0..count)
        .map(|i| {
            if i < 26 {
                ((b'A' + i as u8) as char).to_string()
            } else {
                format!(
                    "{}{}",
                    (b'A' + ((i / 26) - 1) as u8) as char,
                    (b'A' + (i % 26) as u8) as char
                )
            }
        })
        .collect()
}

/// Hive-partitioned fact table, sorted by timestamp within each partition.
fn generate_fact_table(
    base_dir: &Path,
    num_partitions: usize,
    rows_per_partition: usize,
) {
    let fact_dir = base_dir.join("fact");

    let schema = Arc::new(Schema::new(vec![
        Field::new(
            "timestamp",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
        Field::new("value", DataType::Float64, false),
    ]));

    let props = WriterProperties::builder()
        .set_compression(parquet::basic::Compression::SNAPPY)
        .build();

    let dkeys = dkey_names(num_partitions);

    for dkey in &dkeys {
        let part_dir = fact_dir.join(format!("f_dkey={dkey}"));
        fs::create_dir_all(&part_dir).unwrap();
        let file_path = part_dir.join("data.parquet");
        let file = File::create(file_path).unwrap();

        let mut writer =
            ArrowWriter::try_new(file, schema.clone(), Some(props.clone())).unwrap();

        let base_ts = 1672567200000i64; // 2023-01-01T09:00:00
        let timestamps: Vec<i64> = (0..rows_per_partition)
            .map(|i| base_ts + (i as i64 * 10000))
            .collect();

        let values: Vec<f64> = (0..rows_per_partition)
            .map(|i| 50.0 + (i % 100) as f64 + ((i % 7) as f64 * 10.0))
            .collect();

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(TimestampMillisecondArray::from(timestamps)) as ArrayRef,
                Arc::new(Float64Array::from(values)),
            ],
        )
        .unwrap();

        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }
}

/// Single-file dimension table for CollectLeft joins.
fn generate_dimension_table(base_dir: &Path, num_partitions: usize) {
    let dim_dir = base_dir.join("dimension");
    fs::create_dir_all(&dim_dir).unwrap();

    let schema = Arc::new(Schema::new(vec![
        Field::new("d_dkey", DataType::Utf8, false),
        Field::new("env", DataType::Utf8, false),
        Field::new("service", DataType::Utf8, false),
        Field::new("host", DataType::Utf8, false),
    ]));

    let props = WriterProperties::builder()
        .set_compression(parquet::basic::Compression::SNAPPY)
        .build();

    let file_path = dim_dir.join("data.parquet");
    let file = File::create(file_path).unwrap();
    let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props)).unwrap();

    let dkeys = dkey_names(num_partitions);
    let envs = ["dev", "prod", "staging", "test"];
    let services = ["log", "trace", "metric"];
    let hosts = ["ma", "vim", "nano", "emacs"];

    let d_dkey_vals: Vec<String> = dkeys.clone();
    let env_vals: Vec<String> = dkeys
        .iter()
        .enumerate()
        .map(|(i, _)| envs[i % envs.len()].to_string())
        .collect();
    let service_vals: Vec<String> = dkeys
        .iter()
        .enumerate()
        .map(|(i, _)| services[i % services.len()].to_string())
        .collect();
    let host_vals: Vec<String> = dkeys
        .iter()
        .enumerate()
        .map(|(i, _)| hosts[i % hosts.len()].to_string())
        .collect();

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(d_dkey_vals)) as ArrayRef,
            Arc::new(StringArray::from(env_vals)),
            Arc::new(StringArray::from(service_vals)),
            Arc::new(StringArray::from(host_vals)),
        ],
    )
    .unwrap();

    writer.write(&batch).unwrap();
    writer.close().unwrap();
}

struct BenchVariant {
    name: &'static str,
    preserve_file_partitions: usize,
    prefer_existing_sort: bool,
}

const BENCH_VARIANTS: [BenchVariant; 3] = [
    BenchVariant {
        name: "with_optimization",
        preserve_file_partitions: 1,
        prefer_existing_sort: false,
    },
    BenchVariant {
        name: "prefer_existing_sort",
        preserve_file_partitions: 0,
        prefer_existing_sort: true,
    },
    BenchVariant {
        name: "without_optimization",
        preserve_file_partitions: 0,
        prefer_existing_sort: false,
    },
];

async fn save_plans(
    output_file: &Path,
    fact_path: &str,
    dim_path: Option<&str>,
    target_partitions: usize,
    query: &str,
    file_sort_order: Option<Vec<Vec<SortExpr>>>,
) {
    let mut file = File::create(output_file).unwrap();
    writeln!(file, "Query: {query}\n").unwrap();

    for variant in &BENCH_VARIANTS {
        let session_config = SessionConfig::new()
            .with_target_partitions(target_partitions)
            .set_usize(
                "datafusion.optimizer.preserve_file_partitions",
                variant.preserve_file_partitions,
            )
            .set_bool(
                "datafusion.optimizer.prefer_existing_sort",
                variant.prefer_existing_sort,
            );
        let ctx = SessionContext::new_with_config(session_config);

        let mut fact_options = ParquetReadOptions {
            table_partition_cols: vec![("f_dkey".to_string(), DataType::Utf8)],
            ..Default::default()
        };
        if let Some(ref order) = file_sort_order {
            fact_options.file_sort_order = order.clone();
        }
        ctx.register_parquet("fact", fact_path, fact_options)
            .await
            .unwrap();

        if let Some(dim) = dim_path {
            let dim_schema = Arc::new(Schema::new(vec![
                Field::new("d_dkey", DataType::Utf8, false),
                Field::new("env", DataType::Utf8, false),
                Field::new("service", DataType::Utf8, false),
                Field::new("host", DataType::Utf8, false),
            ]));
            let dim_options = ParquetReadOptions {
                schema: Some(&dim_schema),
                ..Default::default()
            };
            ctx.register_parquet("dimension", dim, dim_options)
                .await
                .unwrap();
        }

        let df = ctx.sql(query).await.unwrap();
        let plan = df.explain(false, false).unwrap().collect().await.unwrap();
        writeln!(file, "=== {} ===", variant.name).unwrap();
        writeln!(file, "{}\n", pretty_format_batches(&plan).unwrap()).unwrap();
    }
}

#[allow(clippy::too_many_arguments)]
fn run_benchmark(
    c: &mut Criterion,
    rt: &Runtime,
    name: &str,
    fact_path: &str,
    dim_path: Option<&str>,
    target_partitions: usize,
    query: &str,
    file_sort_order: &Option<Vec<Vec<SortExpr>>>,
) {
    if std::env::var("SAVE_PLANS").is_ok() {
        let output_path = format!("{name}_plans.txt");
        rt.block_on(save_plans(
            Path::new(&output_path),
            fact_path,
            dim_path,
            target_partitions,
            query,
            file_sort_order.clone(),
        ));
        println!("Plans saved to {output_path}");
    }

    let mut group = c.benchmark_group(name);

    for variant in &BENCH_VARIANTS {
        let fact_path_owned = fact_path.to_string();
        let dim_path_owned = dim_path.map(|s| s.to_string());
        let sort_order = file_sort_order.clone();
        let query_owned = query.to_string();
        let preserve_file_partitions = variant.preserve_file_partitions;
        let prefer_existing_sort = variant.prefer_existing_sort;

        group.bench_function(variant.name, |b| {
            b.to_async(rt).iter(|| {
                let fact_path = fact_path_owned.clone();
                let dim_path = dim_path_owned.clone();
                let sort_order = sort_order.clone();
                let query = query_owned.clone();
                async move {
                    let session_config = SessionConfig::new()
                        .with_target_partitions(target_partitions)
                        .set_usize(
                            "datafusion.optimizer.preserve_file_partitions",
                            preserve_file_partitions,
                        )
                        .set_bool(
                            "datafusion.optimizer.prefer_existing_sort",
                            prefer_existing_sort,
                        );
                    let ctx = SessionContext::new_with_config(session_config);

                    let mut fact_options = ParquetReadOptions {
                        table_partition_cols: vec![(
                            "f_dkey".to_string(),
                            DataType::Utf8,
                        )],
                        ..Default::default()
                    };
                    if let Some(ref order) = sort_order {
                        fact_options.file_sort_order = order.clone();
                    }
                    ctx.register_parquet("fact", &fact_path, fact_options)
                        .await
                        .unwrap();

                    if let Some(ref dim) = dim_path {
                        let dim_schema = Arc::new(Schema::new(vec![
                            Field::new("d_dkey", DataType::Utf8, false),
                            Field::new("env", DataType::Utf8, false),
                            Field::new("service", DataType::Utf8, false),
                            Field::new("host", DataType::Utf8, false),
                        ]));
                        let dim_options = ParquetReadOptions {
                            schema: Some(&dim_schema),
                            ..Default::default()
                        };
                        ctx.register_parquet("dimension", dim, dim_options)
                            .await
                            .unwrap();
                    }

                    let df = ctx.sql(&query).await.unwrap();
                    df.collect().await.unwrap()
                }
            })
        });
    }

    group.finish();
}

/// Aggregate on high-cardinality partitions which eliminates repartition and sort.
///
/// Query: SELECT f_dkey, COUNT(*), SUM(value) FROM fact GROUP BY f_dkey ORDER BY f_dkey
///
/// ┌─────────────────────────────────────────────────────────────────────────────────────────────────────────┐
/// │                                          with_optimization                                              │
/// │                                   (preserve_file_partitions=enabled)                                    │
/// │                                                                                                         │
/// │   ┌───────────────────────────┐                                                                         │
/// │   │  SortPreservingMergeExec  │ Sort Preserved                                                          │
/// │   └─────────────┬─────────────┘                                                                         │
/// │                 │                                                                                       │
/// │   ┌─────────────▼─────────────┐                                                                         │
/// │   │     AggregateExec         │ No repartitioning needed                                                │
/// │   │   (SinglePartitioned)     │                                                                         │
/// │   └─────────────┬─────────────┘                                                                         │
/// │                 │                                                                                       │
/// │   ┌─────────────▼─────────────┐                                                                         │
/// │   │     DataSourceExec        │ partitioning=Hash([f_dkey])                                             │
/// │   │   file_groups={N groups}  │                                                                         │
/// │   └───────────────────────────┘                                                                         │
/// └─────────────────────────────────────────────────────────────────────────────────────────────────────────┘
///
/// ┌─────────────────────────────────────────────────────────────────────────────────────────────────────────┐
/// │                                        prefer_existing_sort                                             │
/// │                         (preserve_file_partitions=disabled, prefer_existing_sort=true)                  │
/// │                                                                                                         │
/// │   ┌───────────────────────────┐                                                                         │
/// │   │  SortPreservingMergeExec  │ Sort Preserved                                                          │
/// │   └─────────────┬─────────────┘                                                                         │
/// │                 │                                                                                       │
/// │   ┌─────────────▼─────────────┐                                                                         │
/// │   │      AggregateExec        │                                                                         │
/// │   │    (FinalPartitioned)     │                                                                         │
/// │   └─────────────┬─────────────┘                                                                         │
/// │                 │                                                                                       │
/// │   ┌─────────────▼─────────────┐                                                                         │
/// │   │     RepartitionExec       │ Hash shuffle with order preservation                                    │
/// │   │  Hash([f_dkey], N)        │ Uses k-way merge to maintain sort, has overhead                         │
/// │   │  preserve_order=true      │                                                                         │
/// │   └─────────────┬─────────────┘                                                                         │
/// │                 │                                                                                       │
/// │   ┌─────────────▼─────────────┐                                                                         │
/// │   │      AggregateExec        │                                                                         │
/// │   │        (Partial)          │                                                                         │
/// │   └─────────────┬─────────────┘                                                                         │
/// │                 │                                                                                       │
/// │   ┌─────────────▼─────────────┐                                                                         │
/// │   │     DataSourceExec        │ partitioning=UnknownPartitioning                                        │
/// │   └───────────────────────────┘                                                                         │
/// └─────────────────────────────────────────────────────────────────────────────────────────────────────────┘
///
/// ┌─────────────────────────────────────────────────────────────────────────────────────────────────────────┐
/// │                                       without_optimization                                              │
/// │                        (preserve_file_partitions=disabled, prefer_existing_sort=false)                  │
/// │                                                                                                         │
/// │   ┌───────────────────────────┐                                                                         │
/// │   │  SortPreservingMergeExec  │                                                                         │
/// │   └─────────────┬─────────────┘                                                                         │
/// │                 │                                                                                       │
/// │   ┌─────────────▼─────────────┐                                                                         │
/// │   │      AggregateExec        │ FinalPartitioned                                                        │
/// │   │    (FinalPartitioned)     │                                                                         │
/// │   └─────────────┬─────────────┘                                                                         │
/// │                 │                                                                                       │
/// │   ┌─────────────▼─────────────┐                                                                         │
/// │   │        SortExec           │ Must sort after shuffle                                                 │
/// │   │    [f_dkey ASC]           │                                                                         │
/// │   └─────────────┬─────────────┘                                                                         │
/// │                 │                                                                                       │
/// │   ┌─────────────▼─────────────┐                                                                         │
/// │   │     RepartitionExec       │ Hash shuffle destroys ordering                                          │
/// │   │     Hash([f_dkey], N)     │                                                                         │
/// │   └─────────────┬─────────────┘                                                                         │
/// │                 │                                                                                       │
/// │   ┌─────────────▼─────────────┐                                                                         │
/// │   │      AggregateExec        │                                                                         │
/// │   │        (Partial)          │                                                                         │
/// │   └─────────────┬─────────────┘                                                                         │
/// │                 │                                                                                       │
/// │   ┌─────────────▼─────────────┐                                                                         │
/// │   │     DataSourceExec        │ partitioning=UnknownPartitioning                                                     │
/// │   └───────────────────────────┘                                                                         │
/// └─────────────────────────────────────────────────────────────────────────────────────────────────────────┘
fn preserve_order_bench(
    c: &mut Criterion,
    rt: &Runtime,
    hc_fact_path: &str,
    target_partitions: usize,
) {
    let query = "SELECT f_dkey, COUNT(*) as cnt, SUM(value) as total \
                 FROM fact \
                 GROUP BY f_dkey \
                 ORDER BY f_dkey";

    let file_sort_order = vec![vec![col("f_dkey").sort(true, false)]];

    run_benchmark(
        c,
        rt,
        "preserve_order",
        hc_fact_path,
        None,
        target_partitions,
        query,
        &Some(file_sort_order),
    );
}

/// Join and aggregate on partition column which demonstrates propagation through join.
///
/// Query: SELECT f.f_dkey, MAX(d.env), ... FROM fact f JOIN dimension d ON f.f_dkey = d.d_dkey
///        WHERE d.service = 'log' GROUP BY f.f_dkey ORDER BY f.f_dkey
///
/// ┌─────────────────────────────────────────────────────────────────────────────────────────────────────────┐
/// │                                          with_optimization                                              │
/// │                                   (preserve_file_partitions=enabled)                                    │
/// │                                                                                                         │
/// │   ┌───────────────────────────┐                                                                         │
/// │   │  SortPreservingMergeExec  │                                                                         │
/// │   └─────────────┬─────────────┘                                                                         │
/// │                 │                                                                                       │
/// │   ┌─────────────▼─────────────┐                                                                         │
/// │   │      AggregateExec        │ Hash partitioning propagates through join                               │
/// │   │    (SinglePartitioned)    │                                                                         │
/// │   └─────────────┬─────────────┘                                                                         │
/// │                 │                                                                                       │
/// │   ┌─────────────▼─────────────┐                                                                         │
/// │   │      HashJoinExec         │ Hash partitioning preserved on probe side                               │
/// │   │     (CollectLeft)         │                                                                         │
/// │   └──────────┬────────────────┘                                                                         │
/// │              │                                                                                          │
/// │       ┌──────┴──────┐                                                                                   │
/// │       │             │                                                                                   │
/// │   ┌───▼───┐    ┌────▼────────────────┐                                                                  │
/// │   │ Dim   │    │   DataSourceExec    │  partitioning=Hash([f_dkey]), output_ordering=[f_dkey]           │
/// │   │ Table │    │  (fact, N groups)   │                                                                  │
/// │   └───────┘    └─────────────────────┘                                                                  │
/// └─────────────────────────────────────────────────────────────────────────────────────────────────────────┘
///
/// ┌─────────────────────────────────────────────────────────────────────────────────────────────────────────┐
/// │                                        prefer_existing_sort                                             │
/// │                         (preserve_file_partitions=disabled, prefer_existing_sort=true)                  │
/// │                                                                                                         │
/// │   ┌───────────────────────────┐                                                                         │
/// │   │  SortPreservingMergeExec  │                                                                         │
/// │   └─────────────┬─────────────┘                                                                         │
/// │                 │                                                                                       │
/// │   ┌─────────────▼─────────────┐                                                                         │
/// │   │      AggregateExec        │                                                                         │
/// │   │    (FinalPartitioned)     │                                                                         │
/// │   └─────────────┬─────────────┘                                                                         │
/// │                 │                                                                                       │
/// │   ┌─────────────▼─────────────┐                                                                         │
/// │   │     RepartitionExec       │  Hash shuffle with order preservation                                   │
/// │   │     preserve_order=true   │  Uses k-way merge to maintain sort, has overhead                        │
/// │   └─────────────┬─────────────┘                                                                         │
/// │                 │                                                                                       │
/// │   ┌─────────────▼─────────────┐                                                                         │
/// │   │      AggregateExec        │                                                                         │
/// │   │        (Partial)          │                                                                         │
/// │   └─────────────┬─────────────┘                                                                         │
/// │                 │                                                                                       │
/// │   ┌─────────────▼─────────────┐                                                                         │
/// │   │      HashJoinExec         │                                                                         │
/// │   │     (CollectLeft)         │                                                                         │
/// │   └──────────┬────────────────┘                                                                         │
/// │              │                                                                                          │
/// │       ┌──────┴──────┐                                                                                   │
/// │       │             │                                                                                   │
/// │   ┌───▼───┐    ┌────▼────────────────┐                                                                  │
/// │   │ Dim   │    │   DataSourceExec    │ partitioning=UnknownPartitioning, output_ordering=[f_dkey]       │
/// │   │ Table │    │      (fact)         │                                                                  │
/// │   └───────┘    └─────────────────────┘                                                                  │
/// └─────────────────────────────────────────────────────────────────────────────────────────────────────────┘
///
/// ┌─────────────────────────────────────────────────────────────────────────────────────────────────────────┐
/// │                                       without_optimization                                              │
/// │                        (preserve_file_partitions=disabled, prefer_existing_sort=false)                  │
/// │                                                                                                         │
/// │   ┌───────────────────────────┐                                                                         │
/// │   │  SortPreservingMergeExec  │                                                                         │
/// │   └─────────────┬─────────────┘                                                                         │
/// │                 │                                                                                       │
/// │   ┌─────────────▼─────────────┐                                                                         │
/// │   │      AggregateExec        │                                                                         │
/// │   │    (FinalPartitioned)     │                                                                         │
/// │   └─────────────┬─────────────┘                                                                         │
/// │                 │                                                                                       │
/// │   ┌─────────────▼─────────────┐                                                                         │
/// │   │        SortExec           │ Must sort after shuffle                                                 │
/// │   └─────────────┬─────────────┘                                                                         │
/// │                 │                                                                                       │
/// │   ┌─────────────▼─────────────┐                                                                         │
/// │   │     RepartitionExec       │ Hash shuffle destroys ordering                                          │
/// │   └─────────────┬─────────────┘                                                                         │
/// │                 │                                                                                       │
/// │   ┌─────────────▼─────────────┐                                                                         │
/// │   │      AggregateExec        │                                                                         │
/// │   │        (Partial)          │                                                                         │
/// │   └─────────────┬─────────────┘                                                                         │
/// │                 │                                                                                       │
/// │   ┌─────────────▼─────────────┐                                                                         │
/// │   │      HashJoinExec         │                                                                         │
/// │   │     (CollectLeft)         │                                                                         │
/// │   └──────────┬────────────────┘                                                                         │
/// │              │                                                                                          │
/// │       ┌──────┴──────┐                                                                                   │
/// │       │             │                                                                                   │
/// │   ┌───▼───┐    ┌────▼────────────────┐                                                                  │
/// │   │ Dim   │    │   DataSourceExec    │ partitioning=UnknownPartitioning, output_ordering=[f_dkey]       │
/// │   │ Table │    │      (fact)         │                                                                  │
/// │   └───────┘    └─────────────────────┘                                                                  │
/// └─────────────────────────────────────────────────────────────────────────────────────────────────────────┘
fn preserve_order_join_bench(
    c: &mut Criterion,
    rt: &Runtime,
    hc_fact_path: &str,
    dim_path: &str,
    target_partitions: usize,
) {
    let query = "SELECT f.f_dkey, MAX(d.env), MAX(d.service), COUNT(*), SUM(f.value) \
                 FROM fact f \
                 INNER JOIN dimension d ON f.f_dkey = d.d_dkey \
                 WHERE d.service = 'log' \
                 GROUP BY f.f_dkey \
                 ORDER BY f.f_dkey";

    let file_sort_order = vec![vec![col("f_dkey").sort(true, false)]];

    run_benchmark(
        c,
        rt,
        "preserve_order_join",
        hc_fact_path,
        Some(dim_path),
        target_partitions,
        query,
        &Some(file_sort_order),
    );
}

/// Window function with LIMIT which demonstrates partition and sort elimination.
///
/// Query: SELECT f_dkey, timestamp, value,
///               ROW_NUMBER() OVER (PARTITION BY f_dkey ORDER BY timestamp) as rn
///        FROM fact LIMIT 1000
///
/// ┌─────────────────────────────────────────────────────────────────────────────────────────────────────────┐
/// │                                          with_optimization                                              │
/// │                                   (preserve_file_partitions=enabled)                                    │
/// │                                                                                                         │
/// │   ┌───────────────────────────┐                                                                         │
/// │   │       GlobalLimitExec     │                                                                         │
/// │   │        (LIMIT 1000)       │                                                                         │
/// │   └─────────────┬─────────────┘                                                                         │
/// │                 │                                                                                       │
/// │   ┌─────────────▼─────────────┐                                                                         │
/// │   │   BoundedWindowAggExec    │ No repaartition needed                                                  │
/// │   │  PARTITION BY f_dkey      │                                                                         │
/// │   │  ORDER BY timestamp       │                                                                         │
/// │   └─────────────┬─────────────┘                                                                         │
/// │                 │                                                                                       │
/// │   ┌─────────────▼─────────────┐                                                                         │
/// │   │     DataSourceExec        │ partitioning=Hash([f_dkey]), output_ordering=[f_dkey, timestamp]        │
/// │   │   file_groups={N groups}  │                                                                         │
/// │   └───────────────────────────┘                                                                         │
/// └─────────────────────────────────────────────────────────────────────────────────────────────────────────┘
///
/// ┌─────────────────────────────────────────────────────────────────────────────────────────────────────────┐
/// │                                        prefer_existing_sort                                             │
/// │                         (preserve_file_partitions=disabled, prefer_existing_sort=true)                  │
/// │                                                                                                         │
/// │   ┌───────────────────────────┐                                                                         │
/// │   │       GlobalLimitExec     │                                                                         │
/// │   └─────────────┬─────────────┘                                                                         │
/// │                 │                                                                                       │
/// │   ┌─────────────▼─────────────┐                                                                         │
/// │   │   BoundedWindowAggExec    │                                                                         │
/// │   └─────────────┬─────────────┘                                                                         │
/// │                 │                                                                                       │
/// │   ┌─────────────▼─────────────┐                                                                         │
/// │   │     RepartitionExec       │ Hash shuffle with order preservation                                    │
/// │   │  Hash([f_dkey], N)        │ Uses k-way merge to maintain sort, has overhead                         │
/// │   │  preserve_order=true      │                                                                         │
/// │   └─────────────┬─────────────┘                                                                         │
/// │                 │                                                                                       │
/// │   ┌─────────────▼─────────────┐                                                                         │
/// │   │     DataSourceExec        │ partitioning=UnknownPartitioning, output_ordering=[f_dkey, timestamp]   │
/// │   └───────────────────────────┘                                                                         │
/// └─────────────────────────────────────────────────────────────────────────────────────────────────────────┘
///
/// ┌─────────────────────────────────────────────────────────────────────────────────────────────────────────┐
/// │                                       without_optimization                                              │
/// │                        (preserve_file_partitions=disabled, prefer_existing_sort=false)                  │
/// │                                                                                                         │
/// │   ┌───────────────────────────┐                                                                         │
/// │   │       GlobalLimitExec     │                                                                         │
/// │   └─────────────┬─────────────┘                                                                         │
/// │                 │                                                                                       │
/// │   ┌─────────────▼─────────────┐                                                                         │
/// │   │   BoundedWindowAggExec    │                                                                         │
/// │   └─────────────┬─────────────┘                                                                         │
/// │                 │                                                                                       │
/// │   ┌─────────────▼─────────────┐                                                                         │
/// │   │        SortExec           │ Must sort after shuffle                                                 │
/// │   │  [f_dkey, timestamp]      │                                                                         │
/// │   └─────────────┬─────────────┘                                                                         │
/// │                 │                                                                                       │
/// │   ┌─────────────▼─────────────┐                                                                         │
/// │   │     RepartitionExec       │ Hash shuffle destroys ordering                                          │
/// │   │     Hash([f_dkey], N)     │                                                                         │
/// │   └─────────────┬─────────────┘                                                                         │
/// │                 │                                                                                       │
/// │   ┌─────────────▼─────────────┐                                                                         │
/// │   │     DataSourceExec        │ partitioning=UnknownPartitioning, output_ordering=[f_dkey, timestamp]   │
/// │   └───────────────────────────┘                                                                         │
/// └─────────────────────────────────────────────────────────────────────────────────────────────────────────┘
fn preserve_order_window_bench(
    c: &mut Criterion,
    rt: &Runtime,
    fact_path: &str,
    target_partitions: usize,
) {
    let query = "SELECT f_dkey, timestamp, value, \
                        ROW_NUMBER() OVER (PARTITION BY f_dkey ORDER BY timestamp) as rn \
                 FROM fact \
                 LIMIT 1000";

    let file_sort_order = vec![vec![
        col("f_dkey").sort(true, false),
        col("timestamp").sort(true, false),
    ]];

    run_benchmark(
        c,
        rt,
        "preserve_order_window",
        fact_path,
        None,
        target_partitions,
        query,
        &Some(file_sort_order),
    );
}

fn benchmark_main(c: &mut Criterion) {
    let config = BenchConfig::from_env();
    let hc_config = BenchConfig::high_cardinality(&config);

    println!("\n=== Preserve File Partitioning Benchmark ===");
    println!(
        "Normal config: {} partitions × {} rows = {} total rows",
        config.fact_partitions,
        config.rows_per_partition,
        config.total_rows()
    );
    println!(
        "High-cardinality config: {} partitions × {} rows = {} total rows",
        hc_config.fact_partitions,
        hc_config.rows_per_partition,
        hc_config.total_rows()
    );
    println!("Target partitions: {}\n", config.target_partitions);

    let tmp_dir = TempDir::new().unwrap();
    println!("Generating data...");

    // High-cardinality fact table
    generate_fact_table(
        tmp_dir.path(),
        hc_config.fact_partitions,
        hc_config.rows_per_partition,
    );
    let hc_fact_dir = tmp_dir.path().join("fact_hc");
    fs::rename(tmp_dir.path().join("fact"), &hc_fact_dir).unwrap();
    let hc_fact_path = hc_fact_dir.to_str().unwrap().to_string();

    // Normal fact table
    generate_fact_table(
        tmp_dir.path(),
        config.fact_partitions,
        config.rows_per_partition,
    );
    let fact_path = tmp_dir.path().join("fact").to_str().unwrap().to_string();

    // Dimension table (for join)
    generate_dimension_table(tmp_dir.path(), hc_config.fact_partitions);
    let dim_path = tmp_dir
        .path()
        .join("dimension")
        .to_str()
        .unwrap()
        .to_string();

    println!("Done.\n");

    let rt = Runtime::new().unwrap();

    preserve_order_bench(c, &rt, &hc_fact_path, hc_config.target_partitions);
    preserve_order_join_bench(
        c,
        &rt,
        &hc_fact_path,
        &dim_path,
        hc_config.target_partitions,
    );
    preserve_order_window_bench(c, &rt, &fact_path, config.target_partitions);
}

criterion_group! {
    name = benches;
    config = {
        let config = BenchConfig::from_env();
        Criterion::default()
            .measurement_time(std::time::Duration::from_secs(config.measurement_time_secs))
            .sample_size(10)
    };
    targets = benchmark_main
}
criterion_main!(benches);
