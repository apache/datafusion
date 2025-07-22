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
    io::{BufRead, BufReader},
    process::{Command, Stdio},
};
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
#[structopt(about = "memory profile command")]
struct MemProfileOpt {
    #[structopt(subcommand)]
    command: BenchmarkCommand,
}

#[derive(Debug, StructOpt)]
enum BenchmarkCommand {
    Tpch(TpchOpt),
    // TODO Add other benchmark commands here
}

#[derive(Debug, StructOpt)]
struct TpchOpt {
    #[structopt(long, required = true)]
    path: String,

    /// Query number. If not specified, runs all queries
    #[structopt(short, long)]
    query: Option<usize>,
}

#[tokio::main]
pub async fn main() -> Result<()> {
    // 1. parse args and check which benchmarks should be run
    let opt = MemProfileOpt::from_args();

    // 2. prebuild test binary so that memory does not blow up due to build process
    // check binary file location
    println!("Pre-building benchmark binary...");
    let status = Command::new("cargo")
        .args([
            "build",
            "--profile",
            "release-nonlto",
            "--features",
            "mimalloc_extended",
            "--bin",
            "dfbench",
        ])
        .status()
        .expect("Failed to build dfbench");

    if !status.success() {
        panic!("Failed to build dfbench");
    }
    println!("Benchmark binary built successfully.");

    // 3. create a subprocess, run each benchmark with args (1) (2)
    match opt.command {
        BenchmarkCommand::Tpch(tpch_opt) => {
            run_tpch_benchmark(tpch_opt).await?;
        }
    }

    // (maybe we cannot support result file.. and just have to print..)
    Ok(())
}

async fn run_tpch_benchmark(opt: TpchOpt) -> Result<()> {
    let mut args: Vec<String> = vec![
        "./target/release-nonlto/dfbench".to_string(),
        "tpch".to_string(),
        "--iterations".to_string(),
        "1".to_string(),
        "--path".to_string(),
        opt.path.clone(),
        "--format".to_string(),
        "parquet".to_string(),
        "--partitions".to_string(),
        "4".to_string(),
        "--query".to_string(),
    ];

    let mut query_strings: Vec<String> = Vec::new();
    if let Some(query_id) = opt.query {
        query_strings.push(query_id.to_string());
    } else {
        // run all queries.
        for i in 1..=22 {
            query_strings.push(i.to_string());
        }
    }

    let mut results = vec![];
    for query_str in query_strings {
        args.push(query_str);
        let _ = run_query(&args, &mut results);
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

    // buffer stdout
    let lines: Result<Vec<String>, std::io::Error> =
        reader.lines().collect::<Result<_, _>>();

    child
        .wait()
        .expect("Benchmark process exited with an error");

    // parse after child process terminates
    let lines = lines?;
    let mut iter = lines.iter().peekable();

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
        "\n{:<8} {:>10} {:>12} {:>12} {:>12}",
        "Query", "Time (ms)", "Peak RSS", "Peak Commit", "Page Faults"
    );
    println!("{}", "-".repeat(68));

    for r in results {
        println!(
            "{:<8} {:>10.2} {:>12} {:>12} {:>12}",
            r.query, r.duration_ms, r.peak_rss, r.peak_commit, r.page_faults
        );
    }
}
