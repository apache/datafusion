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

//! Utilities for formatting memory usage metrics.

use datafusion::execution::memory_pool::{
    human_readable_size, operator_category, ConsumerMemoryMetrics,
};
use std::{collections::BTreeMap, fmt::Write};

/// Format summary of memory usage metrics.
///
/// Returns a string with peak usage, cumulative allocations, and totals per
/// operator category. The caller is responsible for printing the returned
/// string if desired.
pub fn format_metrics(metrics: &[ConsumerMemoryMetrics]) -> String {
    if metrics.is_empty() {
        return "no memory metrics recorded".to_string();
    }

    let peak = metrics.iter().map(|m| m.peak).max().unwrap_or(0);
    let cumulative: usize = metrics.iter().map(|m| m.cumulative).sum();

    let mut s = String::new();
    let _ = writeln!(s, "Peak memory usage: {}", human_readable_size(peak));
    let _ = writeln!(
        s,
        "Cumulative allocations: {}",
        human_readable_size(cumulative)
    );

    let mut by_op: BTreeMap<&str, usize> = BTreeMap::new();
    for m in metrics {
        let category = operator_category(&m.name);
        *by_op.entry(category).or_default() += m.cumulative;
    }

    let _ = writeln!(s, "Memory usage by operator:");
    for (op, bytes) in by_op {
        let _ = writeln!(s, "{op}: {}", human_readable_size(bytes));
    }
    s
}
