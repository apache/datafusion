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

//! Memory usage metrics for query execution.

use std::collections::BTreeMap;

use super::{human_readable_size, ConsumerMemoryMetrics};

/// Print summary of memory usage metrics.
///
/// Displays peak usage, cumulative allocations, and totals per operator
/// category.
pub fn print_metrics(metrics: &[ConsumerMemoryMetrics]) {
    if metrics.is_empty() {
        println!("No memory usage recorded");
        return;
    }

    let peak = metrics.iter().map(|m| m.peak).max().unwrap_or(0);
    let cumulative: usize = metrics.iter().map(|m| m.cumulative).sum();

    println!("Peak memory usage: {}", human_readable_size(peak));
    println!(
        "Cumulative allocations: {}",
        human_readable_size(cumulative)
    );

    let mut by_op: BTreeMap<&str, usize> = BTreeMap::new();
    for m in metrics {
        let category = operator_category(&m.name);
        *by_op.entry(category).or_default() += m.cumulative;
    }

    println!("Memory usage by operator:");
    for (op, bytes) in by_op {
        println!("{op}: {}", human_readable_size(bytes));
    }
}

/// Categorize operator names into high-level groups for reporting.
pub fn operator_category(name: &str) -> &'static str {
    let name = name.to_lowercase();
    if name.contains("scan") {
        "Data Input"
    } else if name.contains("filter") {
        "Filtering"
    } else if name.contains("join") {
        "Join Operation"
    } else if name.contains("aggregate") {
        "Aggregation"
    } else if name.contains("sort") {
        "Sorting"
    } else if name.contains("project") {
        "Projection"
    } else if name.contains("union") {
        "Set Operation"
    } else if name.contains("window") {
        "Window Function"
    } else if name.contains("limit") {
        "Limit/TopK"
    } else if name.contains("spill") {
        "Memory Management"
    } else {
        "Other"
    }
}
