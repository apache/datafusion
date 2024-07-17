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

use regex::Regex;
use sqlparser::{ast, keywords::ALL_KEYWORDS};

/// `Dialect` to use for Unparsing
///
/// The default dialect tries to avoid quoting identifiers unless necessary (e.g. `a` instead of `"a"`)
/// but this behavior can be overridden as needed
///
/// **Note**: This trait will eventually be replaced by the Dialect in the SQLparser package
///
/// See <https://github.com/sqlparser-rs/sqlparser-rs/pull/1170>
/// See also the discussion in <https://github.com/apache/datafusion/pull/10625>
pub trait Dialect {
    /// Return the character used to quote identifiers.
    fn identifier_quote_style(&self, _identifier: &str) -> Option<char>;

    /// Does the dialect support specifying `NULLS FIRST/LAST` in `ORDER BY` clauses?
    fn supports_nulls_first_in_sort(&self) -> bool {
        true
    }

    // Does the dialect use TIMESTAMP to represent Date64 rather than DATETIME?
    // E.g. Trino, Athena and Dremio does not have DATETIME data type
    fn use_timestamp_for_date64(&self) -> bool {
        false
    }

    fn interval_style(&self) -> IntervalStyle {
        IntervalStyle::PostgresVerbose
    }

    // The SQL type to use for Arrow Utf8 unparsing
    // Most dialects use VARCHAR, but some, like MySQL, require CHAR
    fn utf8_cast_dtype(&self) -> ast::DataType {
        ast::DataType::Varchar(None)
    }
    // The SQL type to use for Arrow LargeUtf8 unparsing
    // Most dialects use TEXT, but some, like MySQL, require CHAR
    fn large_utf8_cast_dtype(&self) -> ast::DataType {
        ast::DataType::Text
    }
}

/// `IntervalStyle` to use for unparsing
///
/// <https://www.postgresql.org/docs/current/datatype-datetime.html#DATATYPE-INTERVAL-INPUT>
/// different DBMS follows different standards, popular ones are:
/// postgres_verbose: '2 years 15 months 100 weeks 99 hours 123456789 milliseconds' which is
/// compatible with arrow display format, as well as duckdb
/// sql standard format is '1-2' for year-month, or '1 10:10:10.123456' for day-time
/// <https://www.contrib.andrew.cmu.edu/~shadow/sql/sql1992.txt>
#[derive(Clone, Copy)]
pub enum IntervalStyle {
    PostgresVerbose,
    SQLStandard,
    MySQL,
}

pub struct DefaultDialect {}

impl Dialect for DefaultDialect {
    fn identifier_quote_style(&self, identifier: &str) -> Option<char> {
        let identifier_regex = Regex::new(r"^[a-zA-Z_][a-zA-Z0-9_]*$").unwrap();
        if ALL_KEYWORDS.contains(&identifier.to_uppercase().as_str())
            || !identifier_regex.is_match(identifier)
        {
            Some('"')
        } else {
            None
        }
    }
}

pub struct PostgreSqlDialect {}

impl Dialect for PostgreSqlDialect {
    fn identifier_quote_style(&self, _: &str) -> Option<char> {
        Some('"')
    }

    fn interval_style(&self) -> IntervalStyle {
        IntervalStyle::PostgresVerbose
    }
}

pub struct MySqlDialect {}

impl Dialect for MySqlDialect {
    fn identifier_quote_style(&self, _: &str) -> Option<char> {
        Some('`')
    }

    fn supports_nulls_first_in_sort(&self) -> bool {
        false
    }

    fn interval_style(&self) -> IntervalStyle {
        IntervalStyle::MySQL
    }

    fn utf8_cast_dtype(&self) -> ast::DataType {
        ast::DataType::Char(None)
    }

    fn large_utf8_cast_dtype(&self) -> ast::DataType {
        ast::DataType::Char(None)
    }
}

pub struct SqliteDialect {}

impl Dialect for SqliteDialect {
    fn identifier_quote_style(&self, _: &str) -> Option<char> {
        Some('`')
    }
}

pub struct CustomDialect {
    identifier_quote_style: Option<char>,
    supports_nulls_first_in_sort: bool,
    use_timestamp_for_date64: bool,
    interval_style: IntervalStyle,
    utf8_cast_dtype: ast::DataType,
    large_utf8_cast_dtype: ast::DataType,
}

impl Default for CustomDialect {
    fn default() -> Self {
        Self {
            identifier_quote_style: None,
            supports_nulls_first_in_sort: true,
            use_timestamp_for_date64: false,
            interval_style: IntervalStyle::SQLStandard,
            utf8_cast_dtype: ast::DataType::Varchar(None),
            large_utf8_cast_dtype: ast::DataType::Text,
        }
    }
}

impl CustomDialect {
    // create a CustomDialect
    #[deprecated(note = "please use `CustomDialectBuilder` instead")]
    pub fn new(identifier_quote_style: Option<char>) -> Self {
        Self {
            identifier_quote_style,
            ..Default::default()
        }
    }
}

impl Dialect for CustomDialect {
    fn identifier_quote_style(&self, _: &str) -> Option<char> {
        self.identifier_quote_style
    }

    fn supports_nulls_first_in_sort(&self) -> bool {
        self.supports_nulls_first_in_sort
    }

    fn use_timestamp_for_date64(&self) -> bool {
        self.use_timestamp_for_date64
    }

    fn interval_style(&self) -> IntervalStyle {
        self.interval_style
    }

    fn utf8_cast_dtype(&self) -> ast::DataType {
        self.utf8_cast_dtype.clone()
    }

    fn large_utf8_cast_dtype(&self) -> ast::DataType {
        self.large_utf8_cast_dtype.clone()
    }
}

/// `CustomDialectBuilder` to build `CustomDialect` using builder pattern
///
///
/// # Examples
///
/// Building a custom dialect with all default options set in CustomDialectBuilder::new()
/// but with `use_timestamp_for_date64` overridden to `true`
///
/// ```
/// use datafusion_sql::unparser::dialect::CustomDialectBuilder;
/// let dialect = CustomDialectBuilder::new()
///     .with_use_timestamp_for_date64(true)
///     .build();
/// ```
pub struct CustomDialectBuilder {
    identifier_quote_style: Option<char>,
    supports_nulls_first_in_sort: bool,
    use_timestamp_for_date64: bool,
    interval_style: IntervalStyle,
    utf8_cast_dtype: ast::DataType,
    large_utf8_cast_dtype: ast::DataType,
}

impl Default for CustomDialectBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl CustomDialectBuilder {
    pub fn new() -> Self {
        Self {
            identifier_quote_style: None,
            supports_nulls_first_in_sort: true,
            use_timestamp_for_date64: false,
            interval_style: IntervalStyle::PostgresVerbose,
            utf8_cast_dtype: ast::DataType::Varchar(None),
            large_utf8_cast_dtype: ast::DataType::Text,
        }
    }

    pub fn build(self) -> CustomDialect {
        CustomDialect {
            identifier_quote_style: self.identifier_quote_style,
            supports_nulls_first_in_sort: self.supports_nulls_first_in_sort,
            use_timestamp_for_date64: self.use_timestamp_for_date64,
            interval_style: self.interval_style,
            utf8_cast_dtype: self.utf8_cast_dtype,
            large_utf8_cast_dtype: self.large_utf8_cast_dtype,
        }
    }

    /// Customize the dialect with a specific identifier quote style, e.g. '`', '"'
    pub fn with_identifier_quote_style(mut self, identifier_quote_style: char) -> Self {
        self.identifier_quote_style = Some(identifier_quote_style);
        self
    }

    /// Customize the dialect to supports `NULLS FIRST` in `ORDER BY` clauses
    pub fn with_supports_nulls_first_in_sort(
        mut self,
        supports_nulls_first_in_sort: bool,
    ) -> Self {
        self.supports_nulls_first_in_sort = supports_nulls_first_in_sort;
        self
    }

    /// Customize the dialect to uses TIMESTAMP when casting Date64 rather than DATETIME
    pub fn with_use_timestamp_for_date64(
        mut self,
        use_timestamp_for_date64: bool,
    ) -> Self {
        self.use_timestamp_for_date64 = use_timestamp_for_date64;
        self
    }

    /// Customize the dialect with a specific interval style listed in `IntervalStyle`
    pub fn with_interval_style(mut self, interval_style: IntervalStyle) -> Self {
        self.interval_style = interval_style;
        self
    }

    pub fn with_utf8_cast_dtype(mut self, utf8_cast_dtype: ast::DataType) -> Self {
        self.utf8_cast_dtype = utf8_cast_dtype;
        self
    }

    pub fn with_large_utf8_cast_dtype(
        mut self,
        large_utf8_cast_dtype: ast::DataType,
    ) -> Self {
        self.large_utf8_cast_dtype = large_utf8_cast_dtype;
        self
    }
}
