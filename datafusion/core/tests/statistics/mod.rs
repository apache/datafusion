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
            fn display_opt<T: Display>(opt: Option<T>) -> impl Display {
                match opt {
                    None => "?".to_string(),
                    Some(v) => v.to_string(),
                }
            }
            if node.opts.display_output_rows {
                write!(
                    f,
                    " output_rows={} vs {} ({}%)",
                    node.stats.num_rows,
                    display_opt(node.output_rows),
                    display_opt(accuracy_percent(node.stats.num_rows, node.output_rows))
                )?;
            }
            if node.opts.display_output_bytes {
                write!(
                    f,
                    " output_bytes={} vs {} ({}%)",
                    node.stats.total_byte_size,
                    display_opt(node.output_bytes),
                    display_opt(accuracy_percent(
                        node.stats.total_byte_size,
                        node.output_bytes
                    ))
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
