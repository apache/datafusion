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

//! Benchmark for tables with very large row values (multi-KiB strings).
//!
//! With such data, a batch that respects the row-based `batch_size` limit
//! (8192 rows) can still be hundreds of MiB in memory, which stresses
//! byte-blind operators: sorts reserve ~2x the batch size before buffering,
//! aggregations materialize huge transient state, and joins move huge
//! batches through their probe side. Existing suites (TPC-H, ClickBench)
//! have ~1MiB batches and never hit this regime.
//!
//! The dataset is generated on first use: `--gen-rows` rows of
//! (id BIGINT, key BIGINT, val VARCHAR) split over `--gen-files` files,
//! where `val` is a random alphanumeric string of `--gen-value-size` bytes
//! (incompressible, so decoded batches are as large as they look).
//!
//! Intended usage is an A/B of `datafusion.execution.target_batch_size_bytes`
//! (e.g. via the `DATAFUSION_EXECUTION_TARGET_BATCH_SIZE_BYTES` environment
//! variable), optionally with `--memory-limit` to test whether queries that
//! exhaust memory on oversized batches can complete when batches are
//! normalized.

use std::fs;
use std::fs::File;
use std::path::PathBuf;
use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use clap::Args;
use futures::StreamExt;
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;
use rand::SeedableRng;
use rand::distr::{Alphanumeric, SampleString};
use rand::rngs::StdRng;

use datafusion::error::Result;
use datafusion::execution::SessionStateBuilder;
use datafusion::physical_plan::display::DisplayableExecutionPlan;
use datafusion::physical_plan::{displayable, execute_stream};
use datafusion::prelude::*;
use datafusion_common::instant::Instant;

use crate::util::{BenchmarkRun, CommonOpt, QueryResult, print_memory_stats};

/// Key cardinality for the `key` column (`id % KEY_CARDINALITY`)
const KEY_CARDINALITY: i64 = 1000;

/// Rows generated per append to keep generation memory bounded
const GEN_CHUNK_ROWS: usize = 512;

#[derive(Debug, Args)]
pub struct RunOpt {
    /// Common options
    #[command(flatten)]
    common: CommonOpt,

    /// Query number. If not specified, runs all queries
    #[arg(short, long)]
    pub query: Option<usize>,

    /// Directory for the generated dataset
    #[arg(required = true, short = 'p', long = "path")]
    path: PathBuf,

    /// Path to JSON benchmark result to be compared using `compare.py`
    #[arg(short = 'o', long = "output")]
    output_path: Option<PathBuf>,

    /// Total number of rows to generate
    #[arg(long = "gen-rows", default_value = "65536")]
    gen_rows: usize,

    /// Number of parquet files to split the dataset into
    #[arg(long = "gen-files", default_value = "8")]
    gen_files: usize,

    /// Size in bytes of each `val` string value
    #[arg(long = "gen-value-size", default_value = "16384")]
    gen_value_size: usize,

    /// Regenerate the dataset even if it already exists
    #[arg(long = "regenerate")]
    regenerate: bool,
}

pub const LARGE_VALUES_QUERY_START_ID: usize = 1;
pub const LARGE_VALUES_QUERY_END_ID: usize = 5;

/// Queries chosen to stress one byte-blind operator each. `count(*)` /
/// drained-stream shapes keep result sets small so the benchmark measures
/// the operators, not result materialization.
const LARGE_VALUES_QUERIES: [&str; 5] = [
    // Q1: scan + filter on the large values (baseline for the cost of
    // re-chunking itself: scan-bound, no stateful operator)
    r#"
    SELECT count(*)
    FROM large_values
    WHERE val LIKE '%qq%'
    "#,
    // Q2: full sort, small key, large payload rows
    r#"
    SELECT key, val
    FROM large_values
    ORDER BY key
    "#,
    // Q3: full sort keyed on the large values themselves
    r#"
    SELECT val
    FROM large_values
    ORDER BY val
    "#,
    // Q4: aggregation with large-value accumulator state
    r#"
    SELECT key, min(val), max(val)
    FROM large_values
    GROUP BY key
    "#,
    // Q5: hash join with a modest build side; huge batches flow through
    // the probe side and the join output
    r#"
    SELECT count(*)
    FROM large_values a
    JOIN (SELECT id, val FROM large_values WHERE key < 20) b
      ON a.id = b.id
    WHERE a.val != b.val
    "#,
];

impl RunOpt {
    pub async fn run(&self) -> Result<()> {
        self.generate_data_if_needed()?;

        let query_range = match self.query {
            Some(query_id) => query_id..=query_id,
            None => LARGE_VALUES_QUERY_START_ID..=LARGE_VALUES_QUERY_END_ID,
        };

        let mut benchmark_run = BenchmarkRun::new();
        for query_id in query_range {
            benchmark_run.start_new_case(&format!("{query_id}"));

            match self.benchmark_query(query_id).await {
                Ok(query_results) => {
                    for iter in query_results {
                        benchmark_run.write_iter(iter.elapsed, iter.row_count);
                    }
                }
                Err(e) => {
                    benchmark_run.mark_failed();
                    eprintln!("Query {query_id} failed: {e}");
                }
            }
        }

        benchmark_run.maybe_write_json(self.output_path.as_ref())?;
        benchmark_run.maybe_print_failures();
        Ok(())
    }

    async fn benchmark_query(&self, query_id: usize) -> Result<Vec<QueryResult>> {
        let config = self.common.config()?;
        let rt = self.common.build_runtime()?;
        let state = SessionStateBuilder::new()
            .with_config(config)
            .with_runtime_env(rt)
            .with_default_features()
            .build();
        let ctx = SessionContext::from(state);

        let path = self.path.to_str().unwrap();
        ctx.register_parquet("large_values", path, ParquetReadOptions::default())
            .await?;

        let mut millis = vec![];
        let mut query_results = vec![];
        for i in 0..self.common.iterations {
            let start = Instant::now();
            let sql = LARGE_VALUES_QUERIES[query_id - 1];
            let row_count = self.execute_query(&ctx, sql).await?;
            let elapsed = start.elapsed();
            let ms = elapsed.as_secs_f64() * 1000.0;
            millis.push(ms);

            println!(
                "Query {query_id} iteration {i} took {ms:.1} ms and returned {row_count} rows"
            );
            query_results.push(QueryResult { elapsed, row_count });
        }

        let avg = millis.iter().sum::<f64>() / millis.len() as f64;
        println!("Query {query_id} avg time: {avg:.2} ms");
        print_memory_stats();

        Ok(query_results)
    }

    async fn execute_query(&self, ctx: &SessionContext, sql: &str) -> Result<usize> {
        let debug = self.common.debug;
        let plan = ctx.sql(sql).await?;
        let (state, plan) = plan.into_parts();
        let plan = state.optimize(&plan)?;
        let physical_plan = state.create_physical_plan(&plan).await?;
        if debug {
            println!(
                "=== Physical plan ===\n{}\n",
                displayable(physical_plan.as_ref()).indent(true)
            );
        }

        // Drain the stream instead of collecting: sorted output of this
        // dataset is ~1GiB and buffering it would dominate the measurement
        let mut row_count = 0;
        let mut stream = execute_stream(physical_plan.clone(), state.task_ctx())?;
        while let Some(batch) = stream.next().await {
            row_count += batch?.num_rows();
        }

        if debug {
            println!(
                "=== Physical plan with metrics ===\n{}\n",
                DisplayableExecutionPlan::with_metrics(physical_plan.as_ref())
                    .indent(true)
            );
        }
        Ok(row_count)
    }

    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("key", DataType::Int64, false),
            Field::new("val", DataType::Utf8, false),
        ]))
    }

    fn generate_data_if_needed(&self) -> Result<()> {
        let marker = self.path.join(format!(
            "large_values_r{}_f{}_v{}.ok",
            self.gen_rows, self.gen_files, self.gen_value_size
        ));
        if marker.exists() && !self.regenerate {
            return Ok(());
        }
        if self.path.exists() {
            fs::remove_dir_all(&self.path)?;
        }
        fs::create_dir_all(&self.path)?;

        println!(
            "Generating {} rows x {} byte values over {} files in {}...",
            self.gen_rows,
            self.gen_value_size,
            self.gen_files,
            self.path.display()
        );
        let start = Instant::now();
        let schema = Self::schema();
        let rows_per_file = self.gen_rows.div_ceil(self.gen_files);
        let props = WriterProperties::builder()
            .set_compression(Compression::ZSTD(ZstdLevel::try_new(1)?))
            .build();

        for file_idx in 0..self.gen_files {
            let file_path = self.path.join(format!("part-{file_idx}.parquet"));
            let file = File::create(&file_path)?;
            let mut writer =
                ArrowWriter::try_new(file, Arc::clone(&schema), Some(props.clone()))?;
            // deterministic per-file seed for reproducibility
            let mut rng = StdRng::seed_from_u64(42 + file_idx as u64);

            let file_start = file_idx * rows_per_file;
            let file_rows = rows_per_file.min(self.gen_rows.saturating_sub(file_start));
            let mut written = 0;
            while written < file_rows {
                let n = GEN_CHUNK_ROWS.min(file_rows - written);
                let base = (file_start + written) as i64;
                let ids = Int64Array::from_iter_values(base..base + n as i64);
                let keys = Int64Array::from_iter_values(
                    (base..base + n as i64).map(|id| id % KEY_CARDINALITY),
                );
                let vals =
                    StringArray::from_iter_values((0..n).map(|_| {
                        Alphanumeric.sample_string(&mut rng, self.gen_value_size)
                    }));
                let batch = RecordBatch::try_new(
                    Arc::clone(&schema),
                    vec![
                        Arc::new(ids) as ArrayRef,
                        Arc::new(keys) as ArrayRef,
                        Arc::new(vals) as ArrayRef,
                    ],
                )?;
                writer.write(&batch)?;
                written += n;
            }
            writer.close()?;
        }

        fs::write(&marker, b"")?;
        println!("Generated in {:.1}s", start.elapsed().as_secs_f64());
        Ok(())
    }
}
