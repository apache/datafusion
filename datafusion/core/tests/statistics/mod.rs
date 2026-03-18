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

use datafusion::common::Result;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_common::Statistics;
use datafusion_common::stats::Precision;
use datafusion_physical_expr_common::metrics::MetricValue;
use std::fmt::{Debug, Display, Formatter};
use std::sync::Arc;

mod tpcds;

#[derive(Debug, Default, Copy, Clone)]
struct StatsVsMetricsDisplayOptions {
    display_output_rows: bool,
    display_output_bytes: bool,
}

/// Represents a node in a plan for which some statistics where estimated, and some metrics where
/// collected at runtime.
struct StatsCheckerNode {
    /// The name of the original [ExecutionPlan].
    name: String,
    /// The stats attached to the original [ExecutionPlan].
    stats: Arc<Statistics>,
    /// How many rows actually flowed through the [ExecutionPlan] at runtime.
    output_rows: Option<usize>,
    /// Now many bytes actually flowed through the [ExecutionPlan] at runtime.
    output_bytes: Option<usize>,
    /// The children of the [ExecutionPlan], represented as other [StatsCheckerNode].
    children: Vec<StatsCheckerNode>,
    /// Visualization options for this node.j
    opts: StatsVsMetricsDisplayOptions,
}

impl Debug for StatsCheckerNode {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        fn fmt(
            f: &mut Formatter<'_>,
            node: &StatsCheckerNode,
            depth: usize,
        ) -> std::fmt::Result {
            for _ in 0..depth {
                write!(f, "  ")?;
            }
            write!(f, "{}:", node.name)?;
            if node.opts.display_output_rows {
                write_metric(f, "output_rows", node.stats.num_rows, node.output_rows)?;
            }
            if node.opts.display_output_bytes {
                write_metric(
                    f,
                    "output_bytes",
                    node.stats.total_byte_size,
                    node.output_bytes,
                )?;
            }
            writeln!(f)?;
            for c in &node.children {
                fmt(f, c, depth + 1)?;
            }
            Ok(())
        }

        fmt(f, self, 0)
    }
}

fn write_metric(
    f: &mut Formatter<'_>,
    label: &str,
    estimated: Precision<usize>,
    actual: Option<usize>,
) -> std::fmt::Result {
    let rounded_estimated = estimated.map(round);
    let rounded_actual = actual.map(round);
    write!(
        f,
        " {label}={} vs {} ({}%)",
        rounded_estimated,
        display_opt(rounded_actual),
        display_opt(accuracy_percent(rounded_estimated, rounded_actual))
    )
}

fn display_opt<T: Display>(opt: Option<T>) -> impl Display {
    match opt {
        None => "?".to_string(),
        Some(v) => v.to_string(),
    }
}

/// Rounds a value to reduce platform-dependent variation while
/// preserving approximate precision:
/// - Values <= 100: kept exact
/// - Values <= 10000: rounded to 2 significant figures (~5% tolerance)
/// - Values > 10000: rounded to 1 significant figure (~50% tolerance)
fn round(n: usize) -> usize {
    if n <= 100 {
        return n;
    }
    let sig_figs = if n > 10000 { 1 } else { 2 };
    let digits = (n as f64).log10().floor() as u32 + 1;
    if digits <= sig_figs {
        return n;
    }
    let divisor = 10usize.pow(digits - sig_figs);
    ((n + divisor / 2) / divisor) * divisor
}

impl StatsCheckerNode {
    /// Given an already executed [ExecutionPlan], builds a [StatsCheckerNode] taking into account:
    /// - its planning time statistics
    /// - its runtime metrics
    ///
    /// The plan passed in this constructor should have been fully executed.
    fn from_plan(
        plan: &Arc<dyn ExecutionPlan>,
        opts: StatsVsMetricsDisplayOptions,
    ) -> Result<Self> {
        let mut children = vec![];
        for child in plan.children() {
            children.push(StatsCheckerNode::from_plan(child, opts)?);
        }

        let mut node = StatsCheckerNode {
            name: plan.name().to_string(),
            stats: plan.partition_statistics(None)?,
            output_rows: None,
            output_bytes: None,
            children,
            opts,
        };
        if let Some(metrics) = plan.metrics() {
            node.output_rows = metrics.output_rows();
            node.output_bytes = metrics
                .sum(|v| matches!(v.value(), MetricValue::OutputBytes(_)))
                .map(|v| v.as_usize());
        }

        Ok(node)
    }
}

fn accuracy_percent(estimated: Precision<usize>, actual: Option<usize>) -> Option<usize> {
    match (estimated.get_value(), actual) {
        (Some(estimated), Some(actual)) => {
            let err = (100 * estimated.abs_diff(actual)) / actual.max(*estimated).max(1);
            Some(100 - err)
        }
        (Some(_estimated), None) => None,
        (None, Some(_actual)) => Some(0),
        (None, None) => None,
    }
}
