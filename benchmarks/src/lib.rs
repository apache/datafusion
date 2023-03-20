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

use datafusion::DATAFUSION_VERSION;
use serde::Serialize;
use serde_json::Value;
use std::{collections::HashMap, time::SystemTime};

pub mod tpch;

#[derive(Debug, Serialize)]
pub struct RunContext {
    /// Benchmark crate version
    pub benchmark_version: String,
    /// DataFusion crate version
    pub datafusion_version: String,
    /// Number of CPU cores
    pub num_cpus: usize,
    /// Start time
    pub start_time: u64,
    /// CLI arguments
    pub arguments: Vec<String>,
}

impl Default for RunContext {
    fn default() -> Self {
        Self::new()
    }
}

impl RunContext {
    pub fn new() -> Self {
        Self {
            benchmark_version: env!("CARGO_PKG_VERSION").to_owned(),
            datafusion_version: DATAFUSION_VERSION.to_owned(),
            num_cpus: num_cpus::get(),
            start_time: SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .expect("current time is later than UNIX_EPOCH")
                .as_secs(),
            arguments: std::env::args().skip(1).collect::<Vec<String>>(),
        }
    }
}

/// A single iteration of a benchmark query
#[derive(Debug, Serialize)]
struct QueryIter {
    elapsed: f64,
    row_count: usize,
}
/// A single benchmark case
#[derive(Debug, Serialize)]
pub struct BenchQuery {
    query: String,
    iterations: Vec<QueryIter>,
    start_time: u64,
}

/// collects benchmark run data and then serializes it at the end
pub struct BenchmarkRun {
    context: RunContext,
    queries: Vec<BenchQuery>,
    current_case: Option<usize>,
}

impl Default for BenchmarkRun {
    fn default() -> Self {
        Self::new()
    }
}

impl BenchmarkRun {
    // create new
    pub fn new() -> Self {
        Self {
            context: RunContext::new(),
            queries: vec![],
            current_case: None,
        }
    }
    /// begin a new case. iterations added after this will be included in the new case
    pub fn start_new_case(&mut self, id: &str) {
        self.queries.push(BenchQuery {
            query: id.to_owned(),
            iterations: vec![],
            start_time: SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .expect("current time is later than UNIX_EPOCH")
                .as_secs(),
        });
        if let Some(c) = self.current_case.as_mut() {
            *c += 1;
        } else {
            self.current_case = Some(0);
        }
    }
    /// Write a new iteration to the current case
    pub fn write_iter(&mut self, elapsed: f64, row_count: usize) {
        if let Some(idx) = self.current_case {
            self.queries[idx]
                .iterations
                .push(QueryIter { elapsed, row_count })
        } else {
            panic!("no cases existed yet");
        }
    }

    /// Stringify data into formatted json
    pub fn to_json(&self) -> String {
        let mut output = HashMap::<&str, Value>::new();
        output.insert("context", serde_json::to_value(&self.context).unwrap());
        output.insert("queries", serde_json::to_value(&self.queries).unwrap());
        serde_json::to_string_pretty(&output).unwrap()
    }
}
