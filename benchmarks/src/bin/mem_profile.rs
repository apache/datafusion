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
    process::{Command, Stdio},
};
use structopt::StructOpt;

use datafusion_benchmarks::{
    clickbench,
    h2o::{self, AllQueries},
    imdb, sort_tpch, tpch,
};

#[derive(Debug, StructOpt)]
#[structopt(about = "benchmark command")]
#[allow(dead_code)]
enum Options {
    Clickbench(clickbench::RunOpt),
    H2o(h2o::RunOpt),
    Imdb(imdb::RunOpt),
    SortTpch(sort_tpch::RunOpt),
    Tpch(tpch::RunOpt),
}

#[tokio::main]
pub async fn main() -> Result<()> {
    // 1. parse args and check which benchmarks should be run
    // let opt = MemProfileOpt::from_args();
    let profile = env::var("PROFILE").unwrap_or_else(|_| "release".to_string());

    let args = env::args().skip(1);
    // let opt = Options::from_iter(args);
    let query_range = match Options::from_args() {
        // TODO clickbench
        // TODO run for specific query id
        Options::Clickbench(_) => 0..=42,
        Options::H2o(opt) => {
            let queries = AllQueries::try_new(&opt.queries_path)?;
            match opt.query {
                Some(query_id) => query_id..=query_id,
                None => queries.min_query_id()..=queries.max_query_id(),
            }
        }
        Options::Imdb(_) => imdb::IMDB_QUERY_START_ID..=imdb::IMDB_QUERY_END_ID,
        Options::SortTpch(_) => {
            sort_tpch::SORT_TPCH_QUERY_START_ID..=sort_tpch::SORT_TPCH_QUERY_END_ID
        }
        Options::Tpch(_) => tpch::TPCH_QUERY_START_ID..=tpch::TPCH_QUERY_END_ID,
    };

    // 2. prebuild test binary so that memory does not blow up due to build process
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

    // 3. spawn a new process per each benchmark query and print summary
    let mut dfbench_args: Vec<String> = args.collect();
    println!("{dfbench_args:?}");
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

    let command = format!("target/{profile}/dfbench");
    args.insert(0, command);
    args.push("--query".to_string());

    let mut results = vec![];
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
