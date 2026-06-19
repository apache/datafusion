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

//! DataFusion SQL benchmark runner.

use datafusion_benchmarks::sql_benchmark_runner;

#[cfg(feature = "snmalloc")]
#[global_allocator]
static ALLOC: snmalloc_rs::SnMalloc = snmalloc_rs::SnMalloc;

// `cargo clippy --all-features` enables both allocator features, so prefer
// `snmalloc` in that case and fall back to `mimalloc` otherwise.
#[cfg(all(not(feature = "snmalloc"), feature = "mimalloc"))]
#[global_allocator]
static ALLOC: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[tokio::main]
async fn main() {
    env_logger::init();
    if let Err(error) = sql_benchmark_runner::run_cli().await {
        eprintln!("Error: {error}");
        std::process::exit(1);
    }
}

#[test]
fn benchmark_runner_prints_errors_without_debug_escaping() {
    use std::process::Command;

    let output = Command::new("cargo")
        .args([
            "run",
            "--bin",
            "benchmark_runner",
            "--",
            "predicate_eval",
            "--query",
            "999",
            "--iterations",
            "1",
        ])
        .env("PRED_ROWS", "1000000")
        .output()
        .expect("run benchmark_runner");

    assert!(!output.status.success());

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("no SQL benchmark query matched benchmark 'predicate_eval'"),
        "{stderr}"
    );
    assert!(
        stderr.contains("Available predicate_eval queries:"),
        "{stderr}"
    );
    assert!(!stderr.contains("Execution(\""), "{stderr}");
    assert!(!stderr.contains("\\n"), "{stderr}");
}
