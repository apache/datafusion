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

/// Owned version of Arrow's `FormatOptions` with all `String` values instead of `&str`.
///
/// Arrow's `FormatOptions<'static>` requires `&'static str` references, which makes it
/// difficult to work with dynamic format options. This struct uses `String` values,
/// allowing format options to be created and owned at runtime without lifetime constraints.
///
/// # Conversion to Arrow Types
///
/// Use the `as_arrow_options()` method to temporarily convert to `FormatOptions<'a>`
/// with borrowed references for passing to Arrow compute kernels:
///
/// ```ignore
/// let owned_options = OwnedFormatOptions::default();
/// let arrow_options = owned_options.as_arrow_options(); // borrows owned strings
/// ```
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct OwnedFormatOptions {
    /// String representation of null values
    pub null: String,
    /// Date format string
    pub date_format: Option<String>,
    /// Datetime format string
    pub datetime_format: Option<String>,
    /// Timestamp format string
    pub timestamp_format: Option<String>,
    /// Timestamp with timezone format string
    pub timestamp_tz_format: Option<String>,
    /// Time format string
    pub time_format: Option<String>,
    /// Duration format (owned, since DurationFormat is a simple enum)
    pub duration_format: DurationFormat,
    /// Include type information in formatted output
    pub types_info: bool,
}

impl OwnedFormatOptions {
    /// Create a new `OwnedFormatOptions` with default values.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the null string.
    pub fn with_null(mut self, null: String) -> Self {
        self.null = null;
        self
    }

    /// Set the date format.
    pub fn with_date_format(mut self, date_format: Option<String>) -> Self {
        self.date_format = date_format;
        self
    }

    /// Set the datetime format.
    pub fn with_datetime_format(mut self, datetime_format: Option<String>) -> Self {
        self.datetime_format = datetime_format;
        self
    }

    /// Set the timestamp format.
    pub fn with_timestamp_format(mut self, timestamp_format: Option<String>) -> Self {
        self.timestamp_format = timestamp_format;
        self
    }

    /// Set the timestamp with timezone format.
    pub fn with_timestamp_tz_format(
        mut self,
        timestamp_tz_format: Option<String>,
    ) -> Self {
        self.timestamp_tz_format = timestamp_tz_format;
        self
    }

    /// Set the time format.
    pub fn with_time_format(mut self, time_format: Option<String>) -> Self {
        self.time_format = time_format;
        self
    }

    /// Set the duration format.
    pub fn with_duration_format(mut self, duration_format: DurationFormat) -> Self {
        self.duration_format = duration_format;
        self
    }

    /// Set whether to include type information in formatted output.
    pub fn with_types_info(mut self, types_info: bool) -> Self {
        self.types_info = types_info;
        self
    }

    /// Convert to Arrow's `FormatOptions<'a>` with borrowed references.
    ///
    /// This creates a temporary `FormatOptions` with borrowed `&str` references
    /// to the owned strings. The returned options can be passed to Arrow compute
    /// kernels. The borrowed references are valid only as long as `self` is alive.
    pub fn as_arrow_options<'a>(&'a self) -> FormatOptions<'a> {
        FormatOptions::new()
            .with_null(self.null.as_str())
            .with_date_format(self.date_format.as_deref())
            .with_datetime_format(self.datetime_format.as_deref())
            .with_timestamp_format(self.timestamp_format.as_deref())
            .with_timestamp_tz_format(self.timestamp_tz_format.as_deref())
            .with_time_format(self.time_format.as_deref())
            .with_duration_format(self.duration_format)
            .with_display_error(false)
            .with_types_info(self.types_info)
    }
}

impl Default for OwnedFormatOptions {
    fn default() -> Self {
        Self {
            null: "NULL".to_string(),
            date_format: None,
            datetime_format: None,
            timestamp_format: None,
            timestamp_tz_format: None,
            time_format: None,
            duration_format: DurationFormat::Pretty,
            types_info: false,
        }
    }
}

/// Owned version of Arrow's `CastOptions` with `OwnedFormatOptions` instead of `FormatOptions<'static>`.
///
/// Arrow's `CastOptions<'static>` requires `FormatOptions<'static>`, which mandates
/// `&'static str` references. This struct uses `OwnedFormatOptions` with `String` values,
/// allowing dynamic cast options to be created without memory leaks.
///
/// # Conversion to Arrow Types
///
/// Use the `as_arrow_options()` method to temporarily convert to `CastOptions<'a>`
/// with borrowed references for passing to Arrow compute kernels:
///
/// ```ignore
/// let owned_options = OwnedCastOptions { ... };
/// let arrow_options = owned_options.as_arrow_options(); // borrows owned strings
/// arrow::compute::cast(&array, &data_type, Some(&arrow_options))?;
/// ```
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct OwnedCastOptions {
    /// Whether to use safe casting (return errors instead of overflowing)
    pub safe: bool,
    /// Format options for string output
    pub format_options: OwnedFormatOptions,
}

impl OwnedCastOptions {
    /// Create a new `OwnedCastOptions` with default values.
    pub fn new(safe: bool) -> Self {
        Self {
            safe,
            format_options: OwnedFormatOptions::default(),
        }
    }

    /// Create a new `OwnedCastOptions` from an Arrow `CastOptions`.
    pub fn from_arrow_options(options: &CastOptions<'_>) -> Self {
        Self {
            safe: options.safe,
            format_options: OwnedFormatOptions {
                null: options.format_options.null().to_string(),
                date_format: options.format_options.date_format().map(|s| s.to_string()),
                datetime_format: options
                    .format_options
                    .datetime_format()
                    .map(|s| s.to_string()),
                timestamp_format: options
                    .format_options
                    .timestamp_format()
                    .map(|s| s.to_string()),
                timestamp_tz_format: options
                    .format_options
                    .timestamp_tz_format()
                    .map(|s| s.to_string()),
                time_format: options.format_options.time_format().map(|s| s.to_string()),
                duration_format: options.format_options.duration_format(),
                types_info: options.format_options.types_info(),
            },
        }
    }

    /// Convert to Arrow's `CastOptions<'a>` with borrowed references.
    ///
    /// This creates a temporary `CastOptions` with borrowed `&str` references
    /// to the owned strings. The returned options can be passed to Arrow compute
    /// kernels. The borrowed references are valid only as long as `self` is alive.
    pub fn as_arrow_options<'a>(&'a self) -> CastOptions<'a> {
        CastOptions {
            safe: self.safe,
            format_options: self.format_options.as_arrow_options(),
        }
    }
}

impl Default for OwnedCastOptions {
    fn default() -> Self {
        Self {
            safe: false,
            format_options: OwnedFormatOptions::default(),
        }
    }
}

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

/// Verbosity levels controlling how `EXPLAIN ANALYZE` renders metrics
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ExplainAnalyzeLevel {
    /// Show a compact view containing high-level metrics
    Summary,
    /// Show a developer-focused view with per-operator details
    Dev,
    // When adding new enum, update the error message in `from_str()` accordingly.
}

impl FromStr for ExplainAnalyzeLevel {
    type Err = DataFusionError;

    fn from_str(level: &str) -> Result<Self, Self::Err> {
        match level.to_lowercase().as_str() {
            "summary" => Ok(ExplainAnalyzeLevel::Summary),
            "dev" => Ok(ExplainAnalyzeLevel::Dev),
            other => Err(DataFusionError::Configuration(format!(
                "Invalid explain analyze level. Expected 'summary' or 'dev'. Got '{other}'"
            ))),
        }
    }
}

impl Display for ExplainAnalyzeLevel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            ExplainAnalyzeLevel::Summary => "summary",
            ExplainAnalyzeLevel::Dev => "dev",
        };
        write!(f, "{s}")
    }
}

impl ConfigField for ExplainAnalyzeLevel {
    fn visit<V: Visit>(&self, v: &mut V, key: &str, description: &'static str) {
        v.some(key, self, description)
    }

    fn set(&mut self, _: &str, value: &str) -> Result<()> {
        *self = ExplainAnalyzeLevel::from_str(value)?;
        Ok(())
    }
}
