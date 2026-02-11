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

struct Node {
    name: String,
    stats: Statistics,
    output_rows: Option<usize>,
    output_bytes: Option<usize>,
    children: Vec<Node>,
    opts: StatsVsMetricsDisplayOptions,
}

impl Debug for Node {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        fn fmt(f: &mut Formatter<'_>, node: &Node, depth: usize) -> std::fmt::Result {
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

impl Node {
    fn from_plan(
        plan: &Arc<dyn ExecutionPlan>,
        opts: StatsVsMetricsDisplayOptions,
    ) -> Result<Self> {
        let mut children = vec![];
        for child in plan.children() {
            children.push(Node::from_plan(child, opts)?);
        }

        let mut node = Node {
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

    fn avg_row_accuracy(&self) -> usize {
        fn collect_accuracy(node: &Node) -> Vec<usize> {
            let mut results = vec![];
            for child in &node.children {
                results.extend(collect_accuracy(child));
            }
            if let Some(accuracy) =
                accuracy_percent(node.stats.num_rows, node.output_rows)
            {
                results.push(accuracy);
            }
            results
        }
        let accuracy = collect_accuracy(self);
        accuracy.iter().sum::<usize>() / accuracy.len()
    }

    fn avg_byte_accuracy(&self) -> usize {
        fn collect_accuracy(node: &Node) -> Vec<usize> {
            let mut results = vec![];
            for child in &node.children {
                results.extend(collect_accuracy(child));
            }
            if let Some(accuracy) =
                accuracy_percent(node.stats.total_byte_size, node.output_bytes)
            {
                results.push(accuracy);
            }
            results
        }
        let accuracy = collect_accuracy(self);
        accuracy.iter().sum::<usize>() / accuracy.len()
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
