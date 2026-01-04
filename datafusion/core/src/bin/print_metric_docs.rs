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

//! Print metrics documentation collected via `DocumentedMetrics`/`DocumentedExec`.
//! Called from doc generation scripts to refresh `docs/source/user-guide/metrics.md`.

use datafusion_expr::metric_doc_sections::{
    ExecDoc, MetricDoc, MetricDocPosition, exec_docs, metric_docs,
};

fn main() -> std::io::Result<()> {
    datafusion::doc::link_metrics();

    let mut content = String::new();
    let mut metrics: Vec<&MetricDoc> = metric_docs().collect();
    metrics.sort_by(|a, b| a.name.cmp(b.name));

    let mut execs: Vec<&ExecDoc> = exec_docs().collect();
    execs.sort_by(|a, b| a.name.cmp(b.name));

    let common: Vec<&MetricDoc> = metrics
        .iter()
        .copied()
        .filter(|m| m.position == MetricDocPosition::Common)
        .collect();

    if !common.is_empty() {
        content.push_str("## Common Metrics\n\n");
        for metric in common {
            render_metric_doc(&mut content, metric, 3);
        }
    }

    if !execs.is_empty() {
        content.push_str("## Operator-specific Metrics\n\n");
        for exec in execs {
            render_exec_doc(&mut content, exec);
        }
    }

    println!("{content}");
    Ok(())
}

fn render_exec_doc(out: &mut String, exec: &ExecDoc) {
    out.push_str(&heading(3, exec.name));
    out.push_str("\n\n");

    if let Some(doc) = summarize(exec.doc)
        && !doc.is_empty()
    {
        out.push_str(&sanitize(doc));
        out.push_str("\n\n");
    }

    // Filter to operator-specific metrics only (common metrics are documented separately)
    let mut metrics: Vec<&MetricDoc> = exec
        .metrics
        .iter()
        .copied()
        .filter(|metric| metric.position != MetricDocPosition::Common)
        .collect();
    metrics.sort_by(|a, b| a.name.cmp(b.name));

    if metrics.is_empty() {
        out.push_str(
            "_No operator-specific metrics documented (see Common Metrics)._\n\n",
        );
    } else {
        for metric in metrics {
            render_metric_doc(out, metric, 4);
        }
    }
}

fn render_metric_doc(out: &mut String, metric: &MetricDoc, heading_level: usize) {
    out.push_str(&heading(heading_level, metric.name));
    out.push_str("\n\n");

    if let Some(doc) = summarize(metric.doc)
        && !doc.is_empty()
    {
        out.push_str(&sanitize(doc));
        out.push_str("\n\n");
    }

    out.push_str("| Metric | Description |\n");
    out.push_str("| --- | --- |\n");
    for field in metric.fields {
        out.push_str(&format!("| {} | {} |\n", field.name, sanitize(field.doc)));
    }
    out.push('\n');
}

fn heading(level: usize, title: &str) -> String {
    format!("{} {}", "#".repeat(level), title)
}

fn summarize(doc: &str) -> Option<&str> {
    let trimmed = doc.trim();
    if trimmed.is_empty() {
        return None;
    }
    trimmed.split("\n\n").next().map(str::trim)
}

fn sanitize(doc: &str) -> String {
    doc.split_whitespace().collect::<Vec<_>>().join(" ")
}
