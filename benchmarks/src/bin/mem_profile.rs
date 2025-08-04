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

//! mem_profile binary entrypoint
use datafusion::error::Result;
use std::{
    env,
    io::{BufRead, BufReader},
    path::Path,
    process::{Command, Stdio},
};
use structopt::StructOpt;

use datafusion_benchmarks::{
    clickbench,
    h2o::{self, AllQueries},
    imdb, sort_tpch, tpch,
};

#[derive(Debug, StructOpt)]
#[structopt(name = "Memory Profiling Utility")]
struct MemProfileOpt {
    /// Cargo profile to use in dfbench (e.g. release, release-nonlto)
    #[structopt(long, default_value = "release")]
    bench_profile: String,

    #[structopt(subcommand)]
    command: Options,
}

#[derive(Debug, StructOpt)]
#[structopt(about = "Benchmark command")]
enum Options {
    Clickbench(clickbench::RunOpt),
    H2o(h2o::RunOpt),
    Imdb(imdb::RunOpt),
    SortTpch(sort_tpch::RunOpt),
    Tpch(tpch::RunOpt),
}

#[tokio::main]
pub async fn main() -> Result<()> {
    // 1. Parse args and check which benchmarks should be run
    let mem_profile_opt = MemProfileOpt::from_args();
    let profile = mem_profile_opt.bench_profile;
    let query_range = match mem_profile_opt.command {
        Options::Clickbench(opt) => {
            let entries = std::fs::read_dir(&opt.queries_path)?
                .filter_map(Result::ok)
                .filter(|e| {
                    let path = e.path();
                    path.extension().map(|ext| ext == "sql").unwrap_or(false)
                })
                .collect::<Vec<_>>();

            let max_query_id = entries.len().saturating_sub(1);
            match opt.query {
                Some(query_id) => query_id..=query_id,
                None => 0..=max_query_id,
            }
        }
        Options::H2o(opt) => {
            let queries = AllQueries::try_new(&opt.queries_path)?;
            match opt.query {
                Some(query_id) => query_id..=query_id,
                None => queries.min_query_id()..=queries.max_query_id(),
            }
        }
        Options::Imdb(opt) => match opt.query {
            Some(query_id) => query_id..=query_id,
            None => imdb::IMDB_QUERY_START_ID..=imdb::IMDB_QUERY_END_ID,
        },
        Options::SortTpch(opt) => match opt.query {
            Some(query_id) => query_id..=query_id,
            None => {
                sort_tpch::SORT_TPCH_QUERY_START_ID..=sort_tpch::SORT_TPCH_QUERY_END_ID
            }
        },
        Options::Tpch(opt) => match opt.query {
            Some(query_id) => query_id..=query_id,
            None => tpch::TPCH_QUERY_START_ID..=tpch::TPCH_QUERY_END_ID,
        },
    };

    // 2. Prebuild dfbench binary so that memory does not blow up due to build process
    println!("Pre-building benchmark binary...");
    let status = Command::new("cargo")
        .args([
            "build",
            "--profile",
            &profile,
            "--features",
            "mimalloc_extended",
            "--bin",
            "dfbench",
        ])
        .status()
        .expect("Failed to build dfbench");
    assert!(status.success());
    println!("Benchmark binary built successfully.");

    // 3. Create a new process per each benchmark query and print summary
    // Find position of subcommand to collect args for dfbench
    let args: Vec<_> = env::args().collect();
    let subcommands = ["tpch", "clickbench", "h2o", "imdb", "sort-tpch"];
    let sub_pos = args
        .iter()
        .position(|s| subcommands.iter().any(|&cmd| s == cmd))
        .expect("No benchmark subcommand found");

    // Args starting from subcommand become dfbench args
    let mut dfbench_args: Vec<String> =
        args[sub_pos..].iter().map(|s| s.to_string()).collect();

    run_benchmark_as_child_process(&profile, query_range, &mut dfbench_args)?;

    Ok(())
}

fn run_benchmark_as_child_process(
    profile: &str,
    query_range: std::ops::RangeInclusive<usize>,
    args: &mut Vec<String>,
) -> Result<()> {
    let mut query_strings: Vec<String> = Vec::new();
    for i in query_range {
        query_strings.push(i.to_string());
    }

    let target_dir =
        env::var("CARGO_TARGET_DIR").unwrap_or_else(|_| "target".to_string());
    let command = format!("{target_dir}/{profile}/dfbench");
    // Check whether benchmark binary exists
    if !Path::new(&command).exists() {
        panic!(
            "Benchmark binary not found: `{command}`\nRun this command from the top-level `datafusion/` directory so `target/{profile}/dfbench` can be found.",
        );
    }
    args.insert(0, command);
    let mut results = vec![];

    // Run Single Query (args already contain --query num)
    if args.contains(&"--query".to_string()) {
        let _ = run_query(args, &mut results);
        print_summary_table(&results);
        return Ok(());
    }

    // Run All Queries
    args.push("--query".to_string());
    for query_str in query_strings {
        args.push(query_str);
        let _ = run_query(args, &mut results);
        args.pop();
    }

    print_summary_table(&results);
    Ok(())
}

fn run_query(args: &[String], results: &mut Vec<QueryResult>) -> Result<()> {
    let exec_path = &args[0];
    let exec_args = &args[1..];

    let mut child = Command::new(exec_path)
        .args(exec_args)
        .stdout(Stdio::piped())
        .spawn()
        .expect("Failed to start benchmark");

    let stdout = child.stdout.take().unwrap();
    let reader = BufReader::new(stdout);

    // Buffer child's stdout
    let lines: Result<Vec<String>, std::io::Error> =
        reader.lines().collect::<Result<_, _>>();

    child
        .wait()
        .expect("Benchmark process exited with an error");

    // Parse after child process terminates
    let lines = lines?;
    let mut iter = lines.iter().peekable();

    // Look for lines that contain execution time / memory stats
    while let Some(line) = iter.next() {
        if let Some((query, duration_ms)) = parse_query_time(line) {
            if let Some(next_line) = iter.peek() {
                if let Some((peak_rss, peak_commit, page_faults)) =
                    parse_vm_line(next_line)
                {
                    results.push(QueryResult {
                        query,
                        duration_ms,
                        peak_rss,
                        peak_commit,
                        page_faults,
                    });
                    break;
                }
            }
        }
    }

    Ok(())
}

#[derive(Debug)]
struct QueryResult {
    query: usize,
    duration_ms: f64,
    peak_rss: String,
    peak_commit: String,
    page_faults: String,
}

fn parse_query_time(line: &str) -> Option<(usize, f64)> {
    let re = regex::Regex::new(r"Query (\d+) avg time: ([\d.]+) ms").unwrap();
    if let Some(caps) = re.captures(line) {
        let query_id = caps[1].parse::<usize>().ok()?;
        let avg_time = caps[2].parse::<f64>().ok()?;
        Some((query_id, avg_time))
    } else {
        None
    }
}

fn parse_vm_line(line: &str) -> Option<(String, String, String)> {
    let re = regex::Regex::new(
        r"Peak RSS:\s*([\d.]+\s*[A-Z]+),\s*Peak Commit:\s*([\d.]+\s*[A-Z]+),\s*Page Faults:\s*([\d.]+)"
    ).ok()?;
    let caps = re.captures(line)?;
    let peak_rss = caps.get(1)?.as_str().to_string();
    let peak_commit = caps.get(2)?.as_str().to_string();
    let page_faults = caps.get(3)?.as_str().to_string();
    Some((peak_rss, peak_commit, page_faults))
}

// Print as simple aligned table
fn print_summary_table(results: &[QueryResult]) {
    println!(
        "\n{:<8} {:>10} {:>12} {:>12} {:>18}",
        "Query", "Time (ms)", "Peak RSS", "Peak Commit", "Major Page Faults"
    );
    println!("{}", "-".repeat(64));

    for r in results {
        println!(
            "{:<8} {:>10.2} {:>12} {:>12} {:>18}",
            r.query, r.duration_ms, r.peak_rss, r.peak_commit, r.page_faults
        );
    }
}

#[cfg(test)]
// Only run with "ci" mode when we have the data
#[cfg(feature = "ci")]
mod tests {
    use datafusion::common::exec_err;
    use datafusion::error::Result;
    use std::path::{Path, PathBuf};
    use std::process::Command;

    fn get_tpch_data_path() -> Result<String> {
        let path =
            std::env::var("TPCH_DATA").unwrap_or_else(|_| "benchmarks/data".to_string());
        if !Path::new(&path).exists() {
            return exec_err!(
                "Benchmark data not found (set TPCH_DATA env var to override): {}",
                path
            );
        }
        Ok(path)
    }

    // Try to find target/ dir upward
    fn find_target_dir(start: &Path) -> Option<PathBuf> {
        let mut dir = start;

        while let Some(current) = Some(dir) {
            if current.join("target").is_dir() {
                return Some(current.join("target"));
            }

            dir = match current.parent() {
                Some(parent) => parent,
                None => break,
            };
        }

        None
    }

    #[test]
    // This test checks whether `mem_profile` runs successfully and produces expected output
    // using TPC-H query 6 (which runs quickly).
    fn mem_profile_e2e_tpch_q6() -> Result<()> {
        let profile = "ci";
        let tpch_data = get_tpch_data_path()?;

        // The current working directory may not be the top-level datafusion/ directory,
        // so we manually walkdir upward, locate the target directory
        // and set it explicitly via CARGO_TARGET_DIR for the mem_profile command.
        let target_dir = find_target_dir(&std::env::current_dir()?);
        let output = Command::new("cargo")
            .env("CARGO_TARGET_DIR", target_dir.unwrap())
            .args([
                "run",
                "--profile",
                profile,
                "--bin",
                "mem_profile",
                "--",
                "--bench-profile",
                profile,
                "tpch",
                "--query",
                "6",
                "--path",
                &tpch_data,
                "--format",
                "tbl",
            ])
            .output()
            .expect("Failed to run mem_profile");

        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);

        if !output.status.success() {
            panic!(
                "mem_profile failed\nstdout:\n{stdout}\nstderr:\n{stderr}---------------------",
            );
        }

        assert!(
            stdout.contains("Peak RSS")
                && stdout.contains("Query")
                && stdout.contains("Time"),
            "Unexpected output:\n{stdout}",
        );

        Ok(())
    }
}
