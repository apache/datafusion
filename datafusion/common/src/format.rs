// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use std::fmt::{self, Display};
use std::str::FromStr;

use arrow::compute::CastOptions;
use arrow::util::display::{DurationFormat, FormatOptions};

use crate::config::{ConfigField, Visit};
use crate::error::{DataFusionError, Result};
#[cfg(feature = "sql")]
use sqlparser::ast::ExplainOption;

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

/// Normalized options for a single `EXPLAIN` statement.
///
/// This collects the knobs that can be set per-statement from either the
/// legacy keyword form (`EXPLAIN ANALYZE VERBOSE FORMAT tree ...`) or the
/// Postgres-style `EXPLAIN (option [arg], ...) ...` form supported on
/// dialects whose
/// [`Dialect::supports_explain_with_utility_options`](https://docs.rs/sqlparser/latest/sqlparser/dialect/trait.Dialect.html#method.supports_explain_with_utility_options)
/// returns `true`.
///
/// Fields that are `None` / `false` mean "not set at the statement level" —
/// the physical planner falls back to the corresponding session config
/// value.
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash)]
pub struct ExplainStatementOptions {
    /// Whether to actually execute the plan and gather metrics.
    ///
    /// Corresponds to the `ANALYZE` keyword or the `ANALYZE` option.
    pub analyze: bool,
    /// Whether to include extra detail in the output.
    ///
    /// Corresponds to the `VERBOSE` keyword or the `VERBOSE` option.
    pub verbose: bool,
    /// Output format for the plan. When `None`, the session-config
    /// default (`datafusion.explain.format`) is used.
    pub format: Option<ExplainFormat>,
    /// Override for [`MetricType`] (summary / dev) when running
    /// `EXPLAIN ANALYZE`.
    pub analyze_level: Option<MetricType>,
    /// Override for [`ExplainAnalyzeCategories`] (rows / bytes / timing
    /// / uncategorized) when running `EXPLAIN ANALYZE`.
    pub analyze_categories: Option<ExplainAnalyzeCategories>,
    /// Override for `datafusion.explain.show_statistics`.
    pub show_statistics: Option<bool>,
}

#[cfg(feature = "sql")]
impl ExplainStatementOptions {
    /// Parse a list of [`ExplainOption`] values, from `EXPLAIN (...)`, into a
    /// normalized [`ExplainStatementOptions`].
    ///
    /// Argument grammar accepted:
    /// - `OPTION` — bare, implies `TRUE` for boolean options.
    /// - `OPTION TRUE` / `OPTION FALSE`
    /// - `OPTION ON` / `OPTION OFF`
    /// - `OPTION 1` / `OPTION 0`
    /// - `OPTION <ident>` or `OPTION '<string>'` for format / level / metrics.
    ///
    /// Options recognized by DataFusion are: `ANALYZE`, `VERBOSE`, `FORMAT`,
    /// `METRICS`, `LEVEL`, `TIMING`, `SUMMARY`, `COSTS`.
    ///
    /// Postgres-only options (`BUFFERS`, `WAL`, `SETTINGS`, `GENERIC_PLAN`,
    /// `MEMORY`) return a helpful "not supported" error. Any other option
    /// name produces an `unknown EXPLAIN option` error.
    pub fn from_utility_options(opts: &[ExplainOption<'_>]) -> Result<Self> {
        let mut out = ExplainStatementOptions::default();
        // Track whether METRICS was explicitly set so TIMING can merge
        // into it rather than overwrite.
        let mut metrics_explicit = false;

        for opt in opts {
            let name = opt.name.value().to_ascii_lowercase();
            let arg = opt.value.as_deref();
            match name.as_str() {
                "analyze" => {
                    out.analyze = parse_bool_arg(arg, &name)?;
                }
                "verbose" => {
                    out.verbose = parse_bool_arg(arg, &name)?;
                }
                "format" => {
                    let s = parse_ident_or_string_arg(arg, &name)?;
                    out.format = Some(ExplainFormat::from_str(&s)?);
                }
                "metrics" => {
                    let s = parse_ident_or_string_arg(arg, &name)?;
                    out.analyze_categories =
                        Some(ExplainAnalyzeCategories::from_str(&s)?);
                    metrics_explicit = true;
                }
                "level" => {
                    let s = parse_ident_or_string_arg(arg, &name)?;
                    out.analyze_level = Some(MetricType::from_str(&s)?);
                }
                "timing" => {
                    let enable = parse_bool_arg(arg, &name)?;
                    out.analyze_categories = Some(adjust_timing(
                        out.analyze_categories.take(),
                        enable,
                        metrics_explicit,
                    ));
                }
                "summary" => {
                    let summary = parse_bool_arg(arg, &name)?;
                    out.analyze_level = Some(if summary {
                        MetricType::Summary
                    } else {
                        MetricType::Dev
                    });
                }
                "costs" => {
                    out.show_statistics = Some(parse_bool_arg(arg, &name)?);
                }
                // Postgres options DataFusion does not model. Give a helpful
                // pointer rather than silently accepting them.
                "buffers" | "wal" | "settings" | "generic_plan" | "memory" => {
                    let upper = name.to_ascii_uppercase();
                    return Err(DataFusionError::NotImplemented(format!(
                        "EXPLAIN option {upper} is not supported by DataFusion; \
                         see METRICS for category filtering"
                    )));
                }
                _ => {
                    return Err(DataFusionError::Plan(format!(
                        "unknown EXPLAIN option: {}",
                        opt.name.value()
                    )));
                }
            }
        }

        Ok(out)
    }
}

/// The text of an EXPLAIN option's argument, without the quotes of a string.
#[cfg(feature = "sql")]
fn unquote(text: &str) -> &str {
    for quote in ['\'', '"'] {
        if let Some(inner) = text
            .strip_prefix(quote)
            .and_then(|rest| rest.strip_suffix(quote))
        {
            return inner;
        }
    }
    text
}

/// Parse a boolean argument for an EXPLAIN option.
///
/// `None` (bare option, e.g. `ANALYZE`) is treated as `true`. Accepts
/// `TRUE`/`FALSE`/`ON`/`OFF` (case-insensitive, quoted or not) and the
/// numeric literals `0` / `1`.
#[cfg(feature = "sql")]
fn parse_bool_arg(arg: Option<&str>, name: &str) -> Result<bool> {
    let Some(text) = arg else {
        return Ok(true);
    };
    match unquote(text).to_ascii_lowercase().as_str() {
        "true" | "on" | "1" => Ok(true),
        "false" | "off" | "0" => Ok(false),
        _ => Err(DataFusionError::Plan(format!(
            "expected boolean for EXPLAIN option {name}, got '{text}'"
        ))),
    }
}

/// Parse an identifier-or-string argument (used for `FORMAT`, `METRICS`,
/// `LEVEL`).
#[cfg(feature = "sql")]
fn parse_ident_or_string_arg(arg: Option<&str>, name: &str) -> Result<String> {
    let text = arg.ok_or_else(|| {
        DataFusionError::Plan(format!(
            "EXPLAIN option {} requires an argument",
            name.to_ascii_uppercase()
        ))
    })?;
    Ok(unquote(text).to_string())
}

/// Merge a `TIMING on/off` option into an existing `METRICS` selection.
///
/// If METRICS was already specified, we only add/remove the Timing category
/// within that selection. If METRICS was not specified, TIMING effectively
/// means "Only(Timing)" when on, or "show everything except timing" when off.
#[cfg(feature = "sql")]
fn adjust_timing(
    current: Option<ExplainAnalyzeCategories>,
    enable: bool,
    metrics_explicit: bool,
) -> ExplainAnalyzeCategories {
    // METRICS was not specified — TIMING alone shapes the selection.
    if !metrics_explicit {
        return if enable {
            ExplainAnalyzeCategories::All
        } else {
            ExplainAnalyzeCategories::Only(vec![
                MetricCategory::Rows,
                MetricCategory::Bytes,
                MetricCategory::Uncategorized,
            ])
        };
    }

    // METRICS was specified explicitly earlier — merge into its list. When
    // METRICS was explicit, `current` is always `Some(_)`; fall back to All
    // to be safe.
    match current.unwrap_or(ExplainAnalyzeCategories::All) {
        ExplainAnalyzeCategories::All if enable => ExplainAnalyzeCategories::All,
        ExplainAnalyzeCategories::All => {
            // Everything except timing: rows, bytes, uncategorized.
            ExplainAnalyzeCategories::Only(vec![
                MetricCategory::Rows,
                MetricCategory::Bytes,
                MetricCategory::Uncategorized,
            ])
        }
        ExplainAnalyzeCategories::Only(mut cats) if enable => {
            if !cats.contains(&MetricCategory::Timing) {
                cats.push(MetricCategory::Timing);
            }
            ExplainAnalyzeCategories::Only(cats)
        }
        ExplainAnalyzeCategories::Only(cats) => ExplainAnalyzeCategories::Only(
            cats.into_iter()
                .filter(|c| *c != MetricCategory::Timing)
                .collect(),
        ),
    }
}

#[cfg(all(test, feature = "sql"))]
mod explain_options_tests {
    use super::*;
    use sqlparser::ast::{Ident, Span};

    fn option(name: &'static str, value: Option<String>) -> ExplainOption<'static> {
        ExplainOption {
            name: Ident {
                text: name.into(),
                quote: None,
                span: Span::default(),
            },
            sign: "".into(),
            value: value.map(Into::into),
        }
    }

    fn bare(name: &'static str) -> ExplainOption<'static> {
        option(name, None)
    }

    fn with_ident_arg(name: &'static str, arg: &str) -> ExplainOption<'static> {
        option(name, Some(arg.to_string()))
    }

    fn with_string_arg(name: &'static str, arg: &str) -> ExplainOption<'static> {
        option(name, Some(format!("'{arg}'")))
    }

    fn with_bool_arg(name: &'static str, b: bool) -> ExplainOption<'static> {
        option(name, Some(if b { "TRUE" } else { "FALSE" }.to_string()))
    }

    fn with_number_arg(name: &'static str, n: &str) -> ExplainOption<'static> {
        option(name, Some(n.to_string()))
    }

    #[test]
    fn bare_analyze_and_verbose() {
        let opts = ExplainStatementOptions::from_utility_options(&[
            bare("ANALYZE"),
            bare("VERBOSE"),
        ])
        .unwrap();
        assert!(opts.analyze);
        assert!(opts.verbose);
        assert!(opts.format.is_none());
    }

    #[test]
    fn format_from_ident_and_string() {
        let opts = ExplainStatementOptions::from_utility_options(&[with_ident_arg(
            "FORMAT", "tree",
        )])
        .unwrap();
        assert_eq!(opts.format, Some(ExplainFormat::Tree));

        let opts = ExplainStatementOptions::from_utility_options(&[with_string_arg(
            "FORMAT", "pgjson",
        )])
        .unwrap();
        assert_eq!(opts.format, Some(ExplainFormat::PostgresJSON));
    }

    #[test]
    fn metrics_and_level() {
        let opts = ExplainStatementOptions::from_utility_options(&[
            with_string_arg("METRICS", "rows,bytes"),
            with_ident_arg("LEVEL", "dev"),
        ])
        .unwrap();
        assert_eq!(
            opts.analyze_categories,
            Some(ExplainAnalyzeCategories::Only(vec![
                MetricCategory::Rows,
                MetricCategory::Bytes,
            ]))
        );
        assert_eq!(opts.analyze_level, Some(MetricType::Dev));
    }

    #[test]
    fn on_off_numeric_bool() {
        let opts = ExplainStatementOptions::from_utility_options(&[
            with_ident_arg("ANALYZE", "ON"),
            with_ident_arg("VERBOSE", "off"),
            with_bool_arg("COSTS", true),
        ])
        .unwrap();
        assert!(opts.analyze);
        assert!(!opts.verbose);
        assert_eq!(opts.show_statistics, Some(true));

        let opts = ExplainStatementOptions::from_utility_options(&[
            with_number_arg("ANALYZE", "1"),
            with_number_arg("VERBOSE", "0"),
        ])
        .unwrap();
        assert!(opts.analyze);
        assert!(!opts.verbose);
    }

    #[test]
    fn summary_sugar_sets_level() {
        let opts = ExplainStatementOptions::from_utility_options(&[with_ident_arg(
            "SUMMARY", "ON",
        )])
        .unwrap();
        assert_eq!(opts.analyze_level, Some(MetricType::Summary));

        let opts = ExplainStatementOptions::from_utility_options(&[with_bool_arg(
            "SUMMARY", false,
        )])
        .unwrap();
        assert_eq!(opts.analyze_level, Some(MetricType::Dev));
    }

    #[test]
    fn timing_merges_with_metrics() {
        // METRICS then TIMING off → timing is removed from the list
        let opts = ExplainStatementOptions::from_utility_options(&[
            with_string_arg("METRICS", "rows,timing"),
            with_bool_arg("TIMING", false),
        ])
        .unwrap();
        assert_eq!(
            opts.analyze_categories,
            Some(ExplainAnalyzeCategories::Only(vec![MetricCategory::Rows]))
        );

        // METRICS 'rows' then TIMING on → timing is appended
        let opts = ExplainStatementOptions::from_utility_options(&[
            with_string_arg("METRICS", "rows"),
            with_bool_arg("TIMING", true),
        ])
        .unwrap();
        assert_eq!(
            opts.analyze_categories,
            Some(ExplainAnalyzeCategories::Only(vec![
                MetricCategory::Rows,
                MetricCategory::Timing,
            ]))
        );
    }

    #[test]
    fn timing_alone() {
        let opts = ExplainStatementOptions::from_utility_options(&[with_bool_arg(
            "TIMING", false,
        )])
        .unwrap();
        assert_eq!(
            opts.analyze_categories,
            Some(ExplainAnalyzeCategories::Only(vec![
                MetricCategory::Rows,
                MetricCategory::Bytes,
                MetricCategory::Uncategorized,
            ]))
        );
    }

    #[test]
    fn unknown_option_rejected() {
        let err =
            ExplainStatementOptions::from_utility_options(&[bare("FOO")]).unwrap_err();
        assert!(
            err.to_string().contains("unknown EXPLAIN option: FOO"),
            "got: {err}"
        );
    }

    #[test]
    fn postgres_only_options_rejected() {
        for pg_only in ["BUFFERS", "WAL", "SETTINGS", "GENERIC_PLAN", "MEMORY"] {
            let err = ExplainStatementOptions::from_utility_options(&[bare(pg_only)])
                .unwrap_err();
            let msg = err.to_string();
            assert!(
                msg.contains(pg_only),
                "msg did not include {pg_only}: {msg}"
            );
            assert!(msg.contains("not supported"), "msg: {msg}");
        }
    }
}
