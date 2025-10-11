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
use std::hash::{Hash, Hasher};
use std::str::FromStr;

use crate::config::{ConfigField, Visit};
use crate::error::{DataFusionError, Result};
use arrow::compute::CastOptions;
use arrow::util::display::{DurationFormat, FormatOptions};
use parquet::basic::{Compression, Encoding, ZstdLevel};
use parquet::file::properties::EnabledStatistics;

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
            _ => {
                Err(DataFusionError::Configuration(format!("Invalid explain format. Expected 'indent', 'tree', 'pgjson' or 'graphviz'. Got '{format}'")))
            }
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

#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct DFEnabledStatistics(pub EnabledStatistics);

impl FromStr for DFEnabledStatistics {
    type Err = DataFusionError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match EnabledStatistics::from_str(s) {
            Ok(enabled_stats) => Ok(DFEnabledStatistics(enabled_stats)),
            Err(_) => Err(DataFusionError::Configuration(format!(
                "Invalid enabled statistics format. Expected 'chunk' and others. Got '{s}'"
            ))),
        }
    }
}

impl Display for DFEnabledStatistics {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self.0 {
            EnabledStatistics::None => "none",
            EnabledStatistics::Chunk => "chunk",
            EnabledStatistics::Page => "page",
        };
        f.write_str(s)
    }
}

impl ConfigField for DFEnabledStatistics {
    fn visit<V: Visit>(&self, v: &mut V, key: &str, description: &'static str) {
        v.some(key, self, description)
    }

    fn set(&mut self, _: &str, value: &str) -> Result<()> {
        *self = DFEnabledStatistics::from_str(value)?;
        Ok(())
    }
}

// ------- ------- ------- ------- ------- ------- -------
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DFEncoding(pub Encoding);

impl Default for DFEncoding {
    fn default() -> Self {
        Self(Encoding::PLAIN)
    }
}

impl FromStr for DFEncoding {
    type Err = DataFusionError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match Encoding::from_str(s) {
            Ok(encoding) => Ok(DFEncoding(encoding)),
            Err(_) => Err(DataFusionError::Configuration(format!(
                "Invalid encoding format. Expected 'plain' and others. Got '{s}'"
            ))),
        }
    }
}

impl Display for DFEncoding {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0.to_string())
    }
}

impl ConfigField for DFEncoding {
    fn visit<V: Visit>(&self, v: &mut V, key: &str, description: &'static str) {
        v.some(key, self, description)
    }

    fn set(&mut self, _: &str, value: &str) -> Result<()> {
        *self = DFEncoding::from_str(value)?;
        Ok(())
    }
}

// -------- -------- -------- -------- --------
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DFCompression(pub Compression);

impl Default for DFCompression {
    fn default() -> Self {
        Self(Compression::ZSTD(ZstdLevel::try_new(3_i32).unwrap()))
    }
}

impl Hash for DFCompression {
    fn hash<H: Hasher>(&self, state: &mut H) {
        std::mem::discriminant(&self.0).hash(state);
    }
}

// TODO use these
impl From<Compression> for DFCompression {
    fn from(c: Compression) -> Self {
        Self(c)
    }
}
impl From<DFCompression> for Compression {
    fn from(w: DFCompression) -> Self {
        w.0
    }
}

impl FromStr for DFCompression {
    type Err = DataFusionError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match Compression::from_str(s) {
            Ok(c) => Ok(DFCompression(c)),
            Err(_) => Err(DataFusionError::Configuration(format!(
                "Invalid compression format. Expected 'lz4' and others. Got '{s}'"
            ))),
        }
    }
}

impl Display for DFCompression {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0.to_string())
    }
}

impl ConfigField for DFCompression {
    fn visit<V: Visit>(&self, v: &mut V, key: &str, description: &'static str) {
        v.some(key, self, description)
    }

    fn set(&mut self, _: &str, value: &str) -> Result<()> {
        *self = DFCompression::from_str(value)?;
        Ok(())
    }
}

// -------- -------- -------- -------- --------
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DFDurationFormat(pub DurationFormat);

impl FromStr for DFDurationFormat {
    type Err = DataFusionError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "iso8601" => Ok(DFDurationFormat(DurationFormat::ISO8601)),
            "pretty" => Ok(DFDurationFormat(DurationFormat::Pretty)),
            _ => Err(DataFusionError::Configuration(format!(
                "Invalid duration format. Expected 'pretty', or 'iso8601'. Got '{s}'"
            ))),
        }
    }
}

impl Display for DFDurationFormat {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.0 {
            DurationFormat::ISO8601 => write!(f, "iso8601"),
            DurationFormat::Pretty => write!(f, "pretty"),
            _ => Err(fmt::Error),
        }
    }
}

impl ConfigField for DFDurationFormat {
    fn visit<V: Visit>(&self, v: &mut V, key: &str, description: &'static str) {
        v.some(key, self, description)
    }

    fn set(&mut self, _: &str, value: &str) -> Result<()> {
        *self = DFDurationFormat::from_str(value)?;
        Ok(())
    }
}
// TODO should be moved from here
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SQLDialect {
    /// Configure the SQL dialect used by DataFusion's parser; supported values include
    #[default]
    Generic,
    MySQL,
    PostgreSQL,
    Hive,
    SQLite,
    Snowflake,
    Redshift,
    MsSQL,
    ClickHouse,
    BigQuery,
    Ansi,
    DuckDB,
    Databricks,
}

impl FromStr for SQLDialect {
    type Err = DataFusionError;

    fn from_str(format: &str) -> Result<Self, Self::Err> {
        match format.to_lowercase().as_str() {
            "" | "generic" => Ok(SQLDialect::Generic), // TODO default behaviour
            "mysql" => Ok(SQLDialect::MySQL),
            "postgresql" | "postgres" => Ok(SQLDialect::PostgreSQL),
            "hive" => Ok(SQLDialect::Hive),
            "sqlite" => Ok(SQLDialect::SQLite),
            "snowflake" => Ok(SQLDialect::Snowflake),
            "redshift" => Ok(SQLDialect::Redshift),
            "mssql" => Ok(SQLDialect::MsSQL),
            "clickhouse" => Ok(SQLDialect::ClickHouse),
            "bigquery" => Ok(SQLDialect::BigQuery),
            "ansi" => Ok(SQLDialect::Ansi),
            "duckdb" => Ok(SQLDialect::DuckDB),
            "databricks" => Ok(SQLDialect::Databricks),
            _ => {
                Err(DataFusionError::Configuration(format!("Invalid sql dialect. Expected 'mysql', 'postgresql', 'hive', 'sqlite', 'snowflake', 'redshift', 'mssql', 'clickhouse', 'bigquery', 'ansi', 'duckdb' and 'databricks'. Got '{format}'")))
            }
        }
    }
}

impl Display for SQLDialect {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            SQLDialect::Generic => "generic",
            SQLDialect::MySQL => "mysql",
            SQLDialect::PostgreSQL => "postgresql",
            SQLDialect::Hive => "hive",
            SQLDialect::SQLite => "sqlite",
            SQLDialect::Snowflake => "snowflake",
            SQLDialect::Redshift => "redshift",
            SQLDialect::MsSQL => "mssql",
            SQLDialect::ClickHouse => "clickhouse",
            SQLDialect::BigQuery => "bigquery",
            SQLDialect::Ansi => "ansi",
            SQLDialect::DuckDB => "duckdb",
            SQLDialect::Databricks => "databricks",
        };
        write!(f, "{s}")
    }
}

impl ConfigField for SQLDialect {
    fn visit<V: Visit>(&self, v: &mut V, key: &str, description: &'static str) {
        v.some(key, self, description)
    }

    fn set(&mut self, _: &str, value: &str) -> Result<()> {
        *self = SQLDialect::from_str(value)?;
        Ok(())
    }
}

/// ---------------------------------------------------------------------------
// TODO should be moved from here
/// Represents the null ordering for sorting expressions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum NullOrdering {
    /// Nulls appear last in ascending order.
    NullsMax,
    /// Nulls appear first in descending order.
    NullsMin,
    /// Nulls appear first.
    NullsFirst,
    /// Nulls appear last.
    NullsLast,
}

impl NullOrdering {
    /// Evaluates the null ordering based on the given ascending flag.
    ///
    /// # Returns
    /// * `true` if nulls should appear first.
    /// * `false` if nulls should appear last.
    pub fn nulls_first(&self, asc: bool) -> bool {
        match self {
            Self::NullsMax => !asc,
            Self::NullsMin => asc,
            Self::NullsFirst => true,
            Self::NullsLast => false,
        }
    }
}

impl From<&str> for NullOrdering {
    fn from(s: &str) -> Self {
        Self::from_str(s).unwrap_or(Self::NullsMax)
    }
}

impl FromStr for NullOrdering {
    type Err = DataFusionError;

    fn from_str(format: &str) -> Result<Self, Self::Err> {
        match format.to_lowercase().as_str() {
            "" | "nulls_max" => Ok(NullOrdering::NullsMax), // TODO default behaviour
            "nulls_min" => Ok(NullOrdering::NullsMin),
            "nulls_first" => Ok(NullOrdering::NullsFirst),
            "nulls_last" => Ok(NullOrdering::NullsLast),
            _ => {
                Err(DataFusionError::Configuration(format!("Invalid null ordering. Expected 'nulls_max', 'nulls_min', 'nulls_first' or 'nulls_last'. Got '{format}'")))
            }
        }
    }
}

impl Display for NullOrdering {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            NullOrdering::NullsMax => "nulls_max",
            NullOrdering::NullsMin => "nulls_min",
            NullOrdering::NullsFirst => "nulls_first",
            NullOrdering::NullsLast => "nulls_last",
        };
        write!(f, "{s}")
    }
}

impl ConfigField for NullOrdering {
    fn visit<V: Visit>(&self, v: &mut V, key: &str, description: &'static str) {
        v.some(key, self, description)
    }

    fn set(&mut self, _: &str, value: &str) -> Result<()> {
        *self = NullOrdering::from_str(value)?;
        Ok(())
    }
}
