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

//! CLI driver for `dict_encoding_sweep`. One invocation handles one
//! `(group, rows, width, nulls, batch_size)` combination and sweeps
//! across the given `--ndvs`. All timed iterations run in a single process.

use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::PathBuf;

use clap::Parser;
use datafusion::error::Result;
use datafusion_benchmarks::dict_encoding_sweep::{
    Encoding, Query, SourceEncoding, TableSpec, build_batches, build_dim_batches,
    chunk_info, csv_header, distinct_values_for, groups, make_parquet_session,
    make_session_parallel, register_parquet_table, register_table,
    register_table_partitioned, run_cell, run_cell_replan, split_batches_round_robin,
    stats, write_csv_row, write_parquet_source,
};
use datafusion_common::instant::Instant;

#[cfg(feature = "snmalloc")]
#[global_allocator]
static ALLOC: snmalloc_rs::SnMalloc = snmalloc_rs::SnMalloc;

#[cfg(all(not(feature = "snmalloc"), feature = "mimalloc"))]
#[global_allocator]
static ALLOC: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[derive(Debug, Parser)]
#[command(about = "Utf8View vs Dictionary<Int32, Utf8> sweep driver")]
struct Cli {
    /// Operator group. Use parquet_scan for the physical-source encoding matrix.
    #[arg(long)]
    group: String,

    /// Restrict to a single query id within the group.
    #[arg(long)]
    query: Option<String>,

    /// Rows in the generated table.
    #[arg(long)]
    rows: usize,

    /// Comma-separated NDVs to sweep.
    #[arg(long, value_delimiter = ',')]
    ndvs: Vec<usize>,

    /// Fixed width per value in bytes.
    #[arg(long, default_value_t = 16)]
    width: usize,

    /// Null fraction in column `k` (0.0 .. 1.0).
    #[arg(long, default_value_t = 0.0)]
    nulls: f64,

    /// Session + data batch size.
    #[arg(long, default_value_t = 8192)]
    batch_size: usize,

    /// Target partitions for the SessionContext (default 1). Set >1 to let
    /// the planner insert `RepartitionExec` nodes.
    #[arg(long, default_value_t = 1)]
    target_partitions: usize,

    /// Rows per shared dictionary values-array chunk; Parquet row-group size.
    #[arg(long, default_value_t = 500_000)]
    dict_chunk: usize,

    /// Timed iterations per cell per encoding.
    #[arg(long, default_value_t = 15)]
    iters: usize,

    /// Warmup iterations per cell per encoding (not timed).
    #[arg(long, default_value_t = 3)]
    warmups: usize,

    /// Process id tag written in CSV (for multi-process repeat).
    #[arg(long, default_value_t = 1)]
    process_id: u32,

    /// Output CSV path (appended; header written if new).
    #[arg(long)]
    output: PathBuf,

    /// Git sha stamp. Resolved via `git rev-parse` if empty.
    #[arg(long, default_value = "")]
    git_sha: String,

    /// Base RNG seed for data generation.
    #[arg(long, default_value_t = 42)]
    seed: u64,
}

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    assert!(
        !cli.ndvs.is_empty(),
        "--ndvs must list at least one cardinality"
    );

    let queries = groups::get(&cli.group).unwrap_or_else(|| {
        panic!(
            "unknown group '{}', try one of: {:?}",
            cli.group,
            groups::names()
        )
    });
    let selected: Vec<_> = match &cli.query {
        Some(q) => queries.iter().filter(|x| x.id == q).copied().collect(),
        None => queries.to_vec(),
    };
    assert!(
        !selected.is_empty(),
        "no queries matched group={} query={:?}",
        cli.group,
        cli.query
    );

    let git_sha = if cli.git_sha.is_empty() {
        std::process::Command::new("git")
            .args(["rev-parse", "--short=12", "HEAD"])
            .output()
            .ok()
            .and_then(|o| String::from_utf8(o.stdout).ok())
            .map(|s| s.trim().to_string())
            .unwrap_or_default()
    } else {
        cli.git_sha.clone()
    };

    if let Some(parent) = cli.output.parent() {
        std::fs::create_dir_all(parent).ok();
    }
    let needs_header = !cli.output.exists();
    let file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&cli.output)
        .expect("open csv output");
    let mut writer = BufWriter::new(file);
    if needs_header {
        writeln!(writer, "{}", csv_header()).expect("header");
    }

    if cli.group == "parquet_scan" {
        run_parquet_scan(&cli, &selected, &git_sha, &mut writer).await?;
        return Ok(());
    }

    for query in &selected {
        for &ndv in &cli.ndvs {
            let spec = TableSpec {
                rows: cli.rows,
                ndv,
                width_bytes: cli.width,
                null_rate: cli.nulls,
                batch_size: cli.batch_size,
                dict_chunk_rows: cli.dict_chunk,
                seed: cli.seed,
            };

            eprintln!(
                "[cell] group={} q={} rows={} ndv={} width={} nulls={} batch={} chunk={}",
                cli.group,
                query.id,
                spec.rows,
                spec.ndv,
                spec.width_bytes,
                spec.null_rate,
                spec.batch_size,
                spec.dict_chunk_rows,
            );

            let gen_start = Instant::now();
            let distinct_values = distinct_values_for(&spec);
            let view_batches = build_batches(&spec, Encoding::Utf8View, &distinct_values);
            let dict_batches =
                build_batches(&spec, Encoding::Dictionary, &distinct_values);
            let info = chunk_info(&spec);
            eprintln!(
                "  built: view_batches={} dict_batches={} chunks={} values/chunk={} \
                 mean_distinct/batch~{} (gen {:.2}s)",
                view_batches.len(),
                dict_batches.len(),
                info.num_chunks,
                info.values_per_chunk,
                info.mean_distinct_per_batch,
                gen_start.elapsed().as_secs_f64(),
            );

            let ctx_view = make_session_parallel(cli.batch_size, cli.target_partitions);
            let ctx_dict = make_session_parallel(cli.batch_size, cli.target_partitions);
            if cli.target_partitions > 1 {
                let vp = split_batches_round_robin(view_batches, cli.target_partitions);
                let dp = split_batches_round_robin(dict_batches, cli.target_partitions);
                register_table_partitioned(&ctx_view, "t", vp)?;
                register_table_partitioned(&ctx_dict, "t", dp)?;
            } else {
                register_table(&ctx_view, "t", view_batches)?;
                register_table(&ctx_dict, "t", dict_batches)?;
            }

            if cli.group == "join" || cli.group == "repartition" {
                let dim_view = build_dim_batches(
                    Encoding::Utf8View,
                    &distinct_values,
                    distinct_values.len(),
                    cli.batch_size,
                );
                let dim_dict = build_dim_batches(
                    Encoding::Dictionary,
                    &distinct_values,
                    distinct_values.len(),
                    cli.batch_size,
                );
                register_table(&ctx_view, "d", dim_view)?;
                register_table(&ctx_dict, "d", dim_dict)?;

                let half = distinct_values.len().div_ceil(2);
                let dim_half_view = build_dim_batches(
                    Encoding::Utf8View,
                    &distinct_values,
                    half,
                    cli.batch_size,
                );
                let dim_half_dict = build_dim_batches(
                    Encoding::Dictionary,
                    &distinct_values,
                    half,
                    cli.batch_size,
                );
                register_table(&ctx_view, "d_half", dim_half_view)?;
                register_table(&ctx_dict, "d_half", dim_half_dict)?;
            }

            let sql = query.sql.resolve(&distinct_values);
            eprintln!("  sql: {sql}");

            // Plans that contain RepartitionExec (anything run with
            // target_partitions > 1, or the explicit `repartition` group)
            // consume per-plan state on first execution and would panic on
            // reuse. Re-plan each iteration for those cells.
            let needs_replan = cli.target_partitions > 1 || cli.group == "repartition";
            let (vt, dt) = if needs_replan {
                run_cell_replan(&sql, &ctx_view, &ctx_dict, cli.warmups, cli.iters)
                    .await?
            } else {
                run_cell(&sql, &ctx_view, &ctx_dict, cli.warmups, cli.iters).await?
            };

            let (v_med, v_min, v_p25, v_p75) = stats(vt);
            let (d_med, d_min, d_p25, d_p75) = stats(dt);

            write_csv_row(
                &mut writer,
                &git_sha,
                &cli.group,
                query.id,
                SourceEncoding::Memory,
                Encoding::Utf8View,
                &spec,
                cli.process_id,
                v_med,
                v_min,
                v_p25,
                v_p75,
                cli.iters,
                "",
            )
            .expect("csv row");
            write_csv_row(
                &mut writer,
                &git_sha,
                &cli.group,
                query.id,
                SourceEncoding::Memory,
                Encoding::Dictionary,
                &spec,
                cli.process_id,
                d_med,
                d_min,
                d_p25,
                d_p75,
                cli.iters,
                "",
            )
            .expect("csv row");
            writer.flush().expect("flush");

            let ms = |ns: u128| ns as f64 / 1.0e6;
            eprintln!(
                "  view:   median {:.3} ms  min {:.3}  p25 {:.3}  p75 {:.3}",
                ms(v_med),
                ms(v_min),
                ms(v_p25),
                ms(v_p75)
            );
            eprintln!(
                "  dict:   median {:.3} ms  min {:.3}  p25 {:.3}  p75 {:.3}",
                ms(d_med),
                ms(d_min),
                ms(d_p25),
                ms(d_p75)
            );
            eprintln!("  ratio dict/view = {:.3}", d_med as f64 / v_med as f64);
        }
    }

    Ok(())
}

async fn run_parquet_scan(
    cli: &Cli,
    queries: &[Query],
    git_sha: &str,
    writer: &mut BufWriter<File>,
) -> Result<()> {
    let temp_dir = tempfile::tempdir()?;

    for &ndv in &cli.ndvs {
        let spec = TableSpec {
            rows: cli.rows,
            ndv,
            width_bytes: cli.width,
            null_rate: cli.nulls,
            batch_size: cli.batch_size,
            dict_chunk_rows: cli.dict_chunk,
            seed: cli.seed,
        };
        let distinct_values = distinct_values_for(&spec);

        let gen_start = Instant::now();
        let source_batches = build_batches(&spec, Encoding::Utf8View, &distinct_values);
        eprintln!(
            "[parquet-data] rows={} ndv={} width={} nulls={} batches={} (gen {:.2}s)",
            spec.rows,
            spec.ndv,
            spec.width_bytes,
            spec.null_rate,
            source_batches.len(),
            gen_start.elapsed().as_secs_f64(),
        );

        for source_encoding in [
            SourceEncoding::ParquetPlain,
            SourceEncoding::ParquetRleDictionary,
        ] {
            let path = temp_dir.path().join(format!(
                "ndv_{}_{}.parquet",
                ndv,
                source_encoding.as_str()
            ));
            write_parquet_source(
                &path,
                &source_batches,
                source_encoding,
                cli.dict_chunk,
                spec.ndv,
                spec.width_bytes,
            )?;
            let file_size = std::fs::metadata(&path)?.len();
            let note = format!("file_bytes={file_size}");
            eprintln!(
                "  source={} file_bytes={file_size}",
                source_encoding.as_str()
            );

            let ctx_view = make_parquet_session(
                cli.batch_size,
                cli.target_partitions,
                Encoding::Utf8View,
            );
            let ctx_dict = make_parquet_session(
                cli.batch_size,
                cli.target_partitions,
                Encoding::Dictionary,
            );
            register_parquet_table(&ctx_view, "t", &path, Encoding::Utf8View).await?;
            register_parquet_table(&ctx_dict, "t", &path, Encoding::Dictionary).await?;

            for query in queries {
                let sql = query.sql.resolve(&distinct_values);
                eprintln!(
                    "[cell] group=parquet_scan q={} source={} rows={} ndv={} batch={} row_group={}",
                    query.id,
                    source_encoding.as_str(),
                    spec.rows,
                    spec.ndv,
                    spec.batch_size,
                    spec.dict_chunk_rows,
                );
                eprintln!("  sql: {sql}");

                // Parquet scan plans own one-shot file stream state. Re-plan
                // each drain so warmups cannot exhaust the timed scans.
                let (view_times, dict_times) =
                    run_cell_replan(&sql, &ctx_view, &ctx_dict, cli.warmups, cli.iters)
                        .await?;

                let (view_median, view_min, view_p25, view_p75) = stats(view_times);
                let (dict_median, dict_min, dict_p25, dict_p75) = stats(dict_times);
                write_csv_row(
                    writer,
                    git_sha,
                    "parquet_scan",
                    query.id,
                    source_encoding,
                    Encoding::Utf8View,
                    &spec,
                    cli.process_id,
                    view_median,
                    view_min,
                    view_p25,
                    view_p75,
                    cli.iters,
                    &note,
                )?;
                write_csv_row(
                    writer,
                    git_sha,
                    "parquet_scan",
                    query.id,
                    source_encoding,
                    Encoding::Dictionary,
                    &spec,
                    cli.process_id,
                    dict_median,
                    dict_min,
                    dict_p25,
                    dict_p75,
                    cli.iters,
                    &note,
                )?;
                writer.flush()?;

                let milliseconds = |nanoseconds: u128| nanoseconds as f64 / 1.0e6;
                eprintln!(
                    "  view: median {:.3} ms; dict: median {:.3} ms; dict/view {:.3}",
                    milliseconds(view_median),
                    milliseconds(dict_median),
                    dict_median as f64 / view_median as f64,
                );
            }
        }
    }

    Ok(())
}
