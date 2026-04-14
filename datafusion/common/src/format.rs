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

use std::fmt::{self, Display};
use std::str::FromStr;

use arrow::compute::CastOptions;
use arrow::util::display::{DurationFormat, FormatOptions};

use crate::config::{ConfigField, Visit};
use crate::error::{DataFusionError, Result};

/// The default [`FormatOptions`] to use within DataFusion
/// Also see [`crate::config::FormatOptions`]
pub const DEFAULT_FORMAT_OPTIONS: FormatOptions<'static> =
    FormatOptions::new().with_duration_format(DurationFormat::Pretty);

/// The default [`CastOptions`] to use within DataFusion
pub const DEFAULT_CAST_OPTIONS: CastOptions<'static> = CastOptions {
    safe: false,
    format_options: DEFAULT_FORMAT_OPTIONS,
};

/// Output formats for controlling for Explain plans
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ExplainFormat {
    /// Indent mode
    ///
    /// Example:
    /// ```text
    /// > explain format indent select x from values (1) t(x);
    /// +---------------+-----------------------------------------------------+
    /// | plan_type     | plan                                                |
    /// +---------------+-----------------------------------------------------+
    /// | logical_plan  | SubqueryAlias: t                                    |
    /// |               |   Projection: column1 AS x                          |
    /// |               |     Values: (Int64(1))                              |
    /// | physical_plan | ProjectionExec: expr=[column1@0 as x]               |
    /// |               |   DataSourceExec: partitions=1, partition_sizes=[1] |
    /// |               |                                                     |
    /// +---------------+-----------------------------------------------------+
    /// ```
    Indent,
    /// Tree mode
    ///
    /// Example:
    /// ```text
    /// > explain format tree select x from values (1) t(x);
    /// +---------------+-------------------------------+
    /// | plan_type     | plan                          |
    /// +---------------+-------------------------------+
    /// | physical_plan | ┌───────────────────────────┐ |
    /// |               | │       ProjectionExec      │ |
    /// |               | │    --------------------   │ |
    /// |               | │        x: column1@0       │ |
    /// |               | └─────────────┬─────────────┘ |
    /// |               | ┌─────────────┴─────────────┐ |
    /// |               | │       DataSourceExec      │ |
    /// |               | │    --------------------   │ |
    /// |               | │         bytes: 128        │ |
    /// |               | │       format: memory      │ |
    /// |               | │          rows: 1          │ |
    /// |               | └───────────────────────────┘ |
    /// |               |                               |
    /// +---------------+-------------------------------+
    /// ```
    Tree,
    /// Postgres Json mode
    ///
    /// A displayable structure that produces plan in postgresql JSON format.
    ///
    /// Users can use this format to visualize the plan in existing plan
    /// visualization tools, for example [dalibo](https://explain.dalibo.com/)
    ///
    /// Example:
    /// ```text
    /// > explain format pgjson select x from values (1) t(x);
    /// +--------------+--------------------------------------+
    /// | plan_type    | plan                                 |
    /// +--------------+--------------------------------------+
    /// | logical_plan | [                                    |
    /// |              |   {                                  |
    /// |              |     "Plan": {                        |
    /// |              |       "Alias": "t",                  |
    /// |              |       "Node Type": "Subquery",       |
    /// |              |       "Output": [                    |
    /// |              |         "x"                          |
    /// |              |       ],                             |
    /// |              |       "Plans": [                     |
    /// |              |         {                            |
    /// |              |           "Expressions": [           |
    /// |              |             "column1 AS x"           |
    /// |              |           ],                         |
    /// |              |           "Node Type": "Projection", |
    /// |              |           "Output": [                |
    /// |              |             "x"                      |
    /// |              |           ],                         |
    /// |              |           "Plans": [                 |
    /// |              |             {                        |
    /// |              |               "Node Type": "Values", |
    /// |              |               "Output": [            |
    /// |              |                 "column1"            |
    /// |              |               ],                     |
    /// |              |               "Plans": [],           |
    /// |              |               "Values": "(Int64(1))" |
    /// |              |             }                        |
    /// |              |           ]                          |
    /// |              |         }                            |
    /// |              |       ]                              |
    /// |              |     }                                |
    /// |              |   }                                  |
    /// |              | ]                                    |
    /// +--------------+--------------------------------------+
    /// ```
    PostgresJSON,
    /// Graphviz mode
    ///
    /// Example:
    /// ```text
    /// > explain format graphviz select x from values (1) t(x);
    /// +--------------+------------------------------------------------------------------------+
    /// | plan_type    | plan                                                                   |
    /// +--------------+------------------------------------------------------------------------+
    /// | logical_plan |                                                                        |
    /// |              | // Begin DataFusion GraphViz Plan,                                     |
    /// |              | // display it online here: https://dreampuf.github.io/GraphvizOnline   |
    /// |              |                                                                        |
    /// |              | digraph {                                                              |
    /// |              |   subgraph cluster_1                                                   |
    /// |              |   {                                                                    |
    /// |              |     graph[label="LogicalPlan"]                                         |
    /// |              |     2[shape=box label="SubqueryAlias: t"]                              |
    /// |              |     3[shape=box label="Projection: column1 AS x"]                      |
    /// |              |     2 -> 3 [arrowhead=none, arrowtail=normal, dir=back]                |
    /// |              |     4[shape=box label="Values: (Int64(1))"]                            |
    /// |              |     3 -> 4 [arrowhead=none, arrowtail=normal, dir=back]                |
    /// |              |   }                                                                    |
    /// |              |   subgraph cluster_5                                                   |
    /// |              |   {                                                                    |
    /// |              |     graph[label="Detailed LogicalPlan"]                                |
    /// |              |     6[shape=box label="SubqueryAlias: t\nSchema: [x:Int64;N]"]         |
    /// |              |     7[shape=box label="Projection: column1 AS x\nSchema: [x:Int64;N]"] |
    /// |              |     6 -> 7 [arrowhead=none, arrowtail=normal, dir=back]                |
    /// |              |     8[shape=box label="Values: (Int64(1))\nSchema: [column1:Int64;N]"] |
    /// |              |     7 -> 8 [arrowhead=none, arrowtail=normal, dir=back]                |
    /// |              |   }                                                                    |
    /// |              | }                                                                      |
    /// |              | // End DataFusion GraphViz Plan                                        |
    /// |              |                                                                        |
    /// +--------------+------------------------------------------------------------------------+
    /// ```
    Graphviz,
}

/// Implement  parsing strings to `ExplainFormat`
impl FromStr for ExplainFormat {
    type Err = DataFusionError;

    fn from_str(format: &str) -> Result<Self, Self::Err> {
        match format.to_lowercase().as_str() {
            "indent" => Ok(ExplainFormat::Indent),
            "tree" => Ok(ExplainFormat::Tree),
            "pgjson" => Ok(ExplainFormat::PostgresJSON),
            "graphviz" => Ok(ExplainFormat::Graphviz),
            _ => Err(DataFusionError::Configuration(format!(
                "Invalid explain format. Expected 'indent', 'tree', 'pgjson' or 'graphviz'. Got '{format}'"
            ))),
        }
    }
}

impl Display for ExplainFormat {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            ExplainFormat::Indent => "indent",
            ExplainFormat::Tree => "tree",
            ExplainFormat::PostgresJSON => "pgjson",
            ExplainFormat::Graphviz => "graphviz",
        };
        write!(f, "{s}")
    }
}

impl ConfigField for ExplainFormat {
    fn visit<V: Visit>(&self, v: &mut V, key: &str, description: &'static str) {
        v.some(key, self, description)
    }

    fn set(&mut self, _: &str, value: &str) -> Result<()> {
        *self = ExplainFormat::from_str(value)?;
        Ok(())
    }
}

/// Categorizes metrics so the display layer can choose the desired verbosity.
///
/// The `datafusion.explain.analyze_level` configuration controls which
/// type is shown:
/// - `"dev"` (the default): all metrics are shown.
/// - `"summary"`: only metrics tagged as `Summary` are shown.
///
/// This is orthogonal to [`MetricCategory`], which filters by *what kind*
/// of value a metric represents (rows / bytes / timing).
///
/// # Difference from `EXPLAIN ANALYZE VERBOSE`
///
/// The `VERBOSE` keyword controls whether per-partition metrics are shown
/// (when specified) or aggregated metrics are displayed (when omitted).
/// In contrast, `MetricType` determines which *levels* of metrics are
/// displayed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MetricType {
    /// Common metrics for high-level insights (answering which operator is slow)
    Summary,
    /// For deep operator-level introspection for developers
    Dev,
}

impl MetricType {
    /// Returns the set of metric types that should be shown for this level.
    ///
    /// `Dev` is a superset of `Summary`: when the user selects
    /// `analyze_level = 'dev'`, both `Summary` and `Dev` metrics are shown.
    pub fn included_types(self) -> Vec<MetricType> {
        match self {
            MetricType::Summary => vec![MetricType::Summary],
            MetricType::Dev => vec![MetricType::Summary, MetricType::Dev],
        }
    }
}

impl FromStr for MetricType {
    type Err = DataFusionError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.trim().to_lowercase().as_str() {
            "summary" => Ok(Self::Summary),
            "dev" => Ok(Self::Dev),
            other => Err(DataFusionError::Configuration(format!(
                "Invalid explain analyze level. Expected 'summary' or 'dev'. Got '{other}'"
            ))),
        }
    }
}

impl Display for MetricType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Summary => write!(f, "summary"),
            Self::Dev => write!(f, "dev"),
        }
    }
}

impl ConfigField for MetricType {
    fn visit<V: Visit>(&self, v: &mut V, key: &str, description: &'static str) {
        v.some(key, self, description)
    }

    fn set(&mut self, _: &str, value: &str) -> Result<()> {
        *self = MetricType::from_str(value)?;
        Ok(())
    }
}

/// Classifies a metric by what it measures.
///
/// This is orthogonal to [`MetricType`] (Summary / Dev), which controls
/// *verbosity*. `MetricCategory` controls *what kind of value* is shown,
/// so that `EXPLAIN ANALYZE` output can be narrowed to only the categories
/// that are useful in a given context.
///
/// In particular this is useful for testing since metrics differ in their stability across runs:
/// - [`Rows`](Self::Rows) and [`Bytes`](Self::Bytes) depend only on the plan
///   and the data, so they are mostly deterministic across runs (given the same
///   input). Variations can existing e.g. because of non-deterministic ordering
///   of evaluation between threads.
///   Running with a single target partition often makes these metrics stable enough to assert on in tests.
/// - [`Timing`](Self::Timing) depends on hardware, system load, scheduling,
///   etc., so it varies from run to run even on the same machine.
///
/// [`MetricCategory`] is especially useful in sqllogictest (`.slt`) files:
/// setting `datafusion.explain.analyze_categories = 'rows'` lets a test
/// assert on row-count metrics without sprinkling `<slt:ignore>` over every
/// timing value.
///
/// Metrics that do not declare a category (the default for custom
/// `Count` / `Gauge` metrics) are treated as
/// [`Uncategorized`](Self::Uncategorized) for filtering purposes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MetricCategory {
    /// Row counts and related dimensionless counters: `output_rows`,
    /// `spilled_rows`, `output_batches`, pruning metrics, ratios, etc.
    ///
    /// Mostly deterministic given the same plan and data.
    Rows,
    /// Byte measurements: `output_bytes`, `spilled_bytes`,
    /// `current_memory_usage`, `bytes_scanned`, etc.
    ///
    /// Mostly deterministic given the same plan and data.
    Bytes,
    /// Wall-clock durations and timestamps: `elapsed_compute`,
    /// operator-defined `Time` metrics, `start_timestamp` /
    /// `end_timestamp`, etc.
    ///
    /// **Non-deterministic** — varies across runs even on the same hardware.
    Timing,
    /// Catch-all for metrics that do not fit into [`Rows`](Self::Rows),
    /// [`Bytes`](Self::Bytes), or [`Timing`](Self::Timing).
    ///
    /// Custom `Count` / `Gauge` metrics that are not explicitly assigned
    /// a category are treated as `Uncategorized` for filtering purposes.
    ///
    /// This variant lets users explicitly include or exclude these
    /// metrics, e.g.:
    /// ```sql
    /// SET datafusion.explain.analyze_categories = 'rows, bytes, uncategorized';
    /// ```
    Uncategorized,
}

impl FromStr for MetricCategory {
    type Err = DataFusionError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.trim().to_lowercase().as_str() {
            "rows" => Ok(Self::Rows),
            "bytes" => Ok(Self::Bytes),
            "timing" => Ok(Self::Timing),
            "uncategorized" => Ok(Self::Uncategorized),
            other => Err(DataFusionError::Configuration(format!(
                "Invalid metric category '{other}'. \
                 Expected 'rows', 'bytes', 'timing', or 'uncategorized'."
            ))),
        }
    }
}

impl Display for MetricCategory {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Rows => write!(f, "rows"),
            Self::Bytes => write!(f, "bytes"),
            Self::Timing => write!(f, "timing"),
            Self::Uncategorized => write!(f, "uncategorized"),
        }
    }
}

/// Controls which [`MetricCategory`] values are shown in `EXPLAIN ANALYZE`.
///
/// Set via `SET datafusion.explain.analyze_categories = '...'`.
///
/// See [`MetricCategory`] for the determinism properties that motivate
/// this filter.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Default)]
pub enum ExplainAnalyzeCategories {
    /// Show all metrics regardless of category (the default).
    #[default]
    All,
    /// Show only metrics whose category is in the list.
    /// Metrics with no declared category are treated as
    /// [`Uncategorized`](MetricCategory::Uncategorized) for filtering.
    ///
    /// An **empty** vec means "plan only" — suppress all metrics.
    Only(Vec<MetricCategory>),
}

impl FromStr for ExplainAnalyzeCategories {
    type Err = DataFusionError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let s = s.trim().to_lowercase();
        match s.as_str() {
            "all" => Ok(Self::All),
            "none" => Ok(Self::Only(vec![])),
            other => {
                let mut cats = Vec::new();
                for part in other.split(',') {
                    cats.push(part.trim().parse::<MetricCategory>()?);
                }
                cats.dedup();
                Ok(Self::Only(cats))
            }
        }
    }
}

impl Display for ExplainAnalyzeCategories {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::All => write!(f, "all"),
            Self::Only(cats) if cats.is_empty() => write!(f, "none"),
            Self::Only(cats) => {
                let mut first = true;
                for cat in cats {
                    if !first {
                        write!(f, ",")?;
                    }
                    first = false;
                    write!(f, "{cat}")?;
                }
                Ok(())
            }
        }
    }
}

impl ConfigField for ExplainAnalyzeCategories {
    fn visit<V: Visit>(&self, v: &mut V, key: &str, description: &'static str) {
        v.some(key, self, description)
    }

    fn set(&mut self, _: &str, value: &str) -> Result<()> {
        *self = ExplainAnalyzeCategories::from_str(value)?;
        Ok(())
    }
}
