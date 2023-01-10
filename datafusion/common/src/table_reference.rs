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

/// A resolved path to a table of the form "catalog.schema.table"
#[derive(Debug, Clone, Copy)]
pub struct ResolvedTableReference<'a> {
    /// The catalog (aka database) containing the table
    pub catalog: &'a str,
    /// The schema containing the table
    pub schema: &'a str,
    /// The table name
    pub table: &'a str,
}

impl<'a> std::fmt::Display for ResolvedTableReference<'a> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}.{}", self.catalog, self.schema, self.table)
    }
}

/// Represents a path to a table that may require further resolution
#[derive(Debug, Clone, Copy)]
pub enum TableReference<'a> {
    /// An unqualified table reference, e.g. "table"
    Bare {
        /// The table name
        table: &'a str,
    },
    /// A partially resolved table reference, e.g. "schema.table"
    Partial {
        /// The schema containing the table
        schema: &'a str,
        /// The table name
        table: &'a str,
    },
    /// A fully resolved table reference, e.g. "catalog.schema.table"
    Full {
        /// The catalog (aka database) containing the table
        catalog: &'a str,
        /// The schema containing the table
        schema: &'a str,
        /// The table name
        table: &'a str,
    },
}

/// Represents a path to a table that may require further resolution
/// that owns the underlying names
#[derive(Debug, Clone)]
pub enum OwnedTableReference {
    /// An unqualified table reference, e.g. "table"
    Bare {
        /// The table name
        table: String,
    },
    /// A partially resolved table reference, e.g. "schema.table"
    Partial {
        /// The schema containing the table
        schema: String,
        /// The table name
        table: String,
    },
    /// A fully resolved table reference, e.g. "catalog.schema.table"
    Full {
        /// The catalog (aka database) containing the table
        catalog: String,
        /// The schema containing the table
        schema: String,
        /// The table name
        table: String,
    },
}

impl OwnedTableReference {
    /// Return a `TableReference` view of this `OwnedTableReference`
    pub fn as_table_reference(&self) -> TableReference<'_> {
        match self {
            Self::Bare { table } => TableReference::Bare { table },
            Self::Partial { schema, table } => TableReference::Partial { schema, table },
            Self::Full {
                catalog,
                schema,
                table,
            } => TableReference::Full {
                catalog,
                schema,
                table,
            },
        }
    }
}

impl std::fmt::Display for OwnedTableReference {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OwnedTableReference::Bare { table } => write!(f, "{table}"),
            OwnedTableReference::Partial { schema, table } => {
                write!(f, "{schema}.{table}")
            }
            OwnedTableReference::Full {
                catalog,
                schema,
                table,
            } => write!(f, "{catalog}.{schema}.{table}"),
        }
    }
}

/// Convert `OwnedTableReference` into a `TableReference`. Somewhat
/// akward to use but 'idiomatic': `(&table_ref).into()`
impl<'a> From<&'a OwnedTableReference> for TableReference<'a> {
    fn from(r: &'a OwnedTableReference) -> Self {
        r.as_table_reference()
    }
}

impl<'a> TableReference<'a> {
    /// Retrieve the actual table name, regardless of qualification
    pub fn table(&self) -> &str {
        match self {
            Self::Full { table, .. }
            | Self::Partial { table, .. }
            | Self::Bare { table } => table,
        }
    }

    /// Given a default catalog and schema, ensure this table reference is fully resolved
    pub fn resolve(
        self,
        default_catalog: &'a str,
        default_schema: &'a str,
    ) -> ResolvedTableReference<'a> {
        match self {
            Self::Full {
                catalog,
                schema,
                table,
            } => ResolvedTableReference {
                catalog,
                schema,
                table,
            },
            Self::Partial { schema, table } => ResolvedTableReference {
                catalog: default_catalog,
                schema,
                table,
            },
            Self::Bare { table } => ResolvedTableReference {
                catalog: default_catalog,
                schema: default_schema,
                table,
            },
        }
    }

    /// Forms a [`TableReference`] by splitting `s` on periods `.`.
    ///
    /// Note that this function does NOT handle periods or name
    /// normalization correctly (e.g. `"foo.bar"` will be parsed as
    /// `"foo`.`bar"`. and `Foo` will be parsed as `Foo` (not `foo`).
    ///
    /// If you need to handle such identifiers correctly, you should
    /// use a SQL parser or form the [`OwnedTableReference`] directly.
    ///
    /// See more detail in <https://github.com/apache/arrow-datafusion/issues/4532>
    pub fn parse_str(s: &'a str) -> Self {
        let parts: Vec<&str> = s.split('.').collect();

        match parts.len() {
            1 => Self::Bare { table: s },
            2 => Self::Partial {
                schema: parts[0],
                table: parts[1],
            },
            3 => Self::Full {
                catalog: parts[0],
                schema: parts[1],
                table: parts[2],
            },
            _ => Self::Bare { table: s },
        }
    }
}

/// Parse a string into a TableReference, by splittig on `.`
///
/// See caveats on [`TableReference::parse_str`]
impl<'a> From<&'a str> for TableReference<'a> {
    fn from(s: &'a str) -> Self {
        Self::parse_str(s)
    }
}

impl<'a> From<ResolvedTableReference<'a>> for TableReference<'a> {
    fn from(resolved: ResolvedTableReference<'a>) -> Self {
        Self::Full {
            catalog: resolved.catalog,
            schema: resolved.schema,
            table: resolved.table,
        }
    }
}
