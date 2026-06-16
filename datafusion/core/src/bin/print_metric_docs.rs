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

//! Print the operator metrics documentation collected via
//! `SessionStateDefaults::default_metric_docs` to stdout.
//!
//! Called from `dev/update_metric_docs.sh` to regenerate
//! `docs/source/user-guide/metrics.md`.

use std::fmt::Write as _;

use datafusion::execution::SessionStateDefaults;
use datafusion_common::Result;
use datafusion_expr::{MetricPosition, MetricsDocumentation};

fn main() -> Result<()> {
    let docs = SessionStateDefaults::default_metric_docs();

    let (common, operator): (Vec<_>, Vec<_>) = docs
        .into_iter()
        .partition(|d| d.position == MetricPosition::Common);

    let mut out = String::new();

    if !common.is_empty() {
        out.push_str("## Common Metrics\n\n");
        for doc in common {
            render(&mut out, doc, 3);
        }
    }

    if !operator.is_empty() {
        out.push_str("## Operator-specific Metrics\n\n");
        for doc in operator {
            render(&mut out, doc, 3);
        }
    }

    print!("{out}");
    Ok(())
}

fn render(out: &mut String, doc: &MetricsDocumentation, level: usize) {
    let _ = writeln!(out, "{} {}", "#".repeat(level), doc.name);
    out.push('\n');

    if !doc.description.is_empty() {
        out.push_str(&doc.description);
        out.push_str("\n\n");
    }

    if doc.metrics.is_empty() {
        return;
    }

    out.push_str("| Metric | Description |\n");
    out.push_str("| --- | --- |\n");
    for field in &doc.metrics {
        let _ = writeln!(
            out,
            "| {} | {} |",
            field.name,
            collapse_whitespace(&field.description),
        );
    }
    out.push('\n');
}

fn collapse_whitespace(s: &str) -> String {
    s.split_whitespace().collect::<Vec<_>>().join(" ")
}
