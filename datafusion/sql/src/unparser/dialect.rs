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

use std::sync::Arc;

use arrow_schema::TimeUnit;
use datafusion_expr::Expr;
use regex::Regex;
use sqlparser::{
    ast::{self, Function, Ident, ObjectName, TimezoneInfo},
    keywords::ALL_KEYWORDS,
};

use datafusion_common::Result;

use super::{utils::date_part_to_sql, Unparser};

/// `Dialect` to use for Unparsing
///
/// The default dialect tries to avoid quoting identifiers unless necessary (e.g. `a` instead of `"a"`)
/// but this behavior can be overridden as needed
///
/// **Note**: This trait will eventually be replaced by the Dialect in the SQLparser package
///
/// See <https://github.com/sqlparser-rs/sqlparser-rs/pull/1170>
/// See also the discussion in <https://github.com/apache/datafusion/pull/10625>
pub trait Dialect: Send + Sync {
    /// Return the character used to quote identifiers.
    fn identifier_quote_style(&self, _identifier: &str) -> Option<char>;

    /// Does the dialect support specifying `NULLS FIRST/LAST` in `ORDER BY` clauses?
    fn supports_nulls_first_in_sort(&self) -> bool {
        true
    }

    /// Does the dialect use TIMESTAMP to represent Date64 rather than DATETIME?
    /// E.g. Trino, Athena and Dremio does not have DATETIME data type
    fn use_timestamp_for_date64(&self) -> bool {
        false
    }

    fn interval_style(&self) -> IntervalStyle {
        IntervalStyle::PostgresVerbose
    }

    /// Does the dialect use DOUBLE PRECISION to represent Float64 rather than DOUBLE?
    /// E.g. Postgres uses DOUBLE PRECISION instead of DOUBLE
    fn float64_ast_dtype(&self) -> sqlparser::ast::DataType {
        sqlparser::ast::DataType::Double
    }

    /// The SQL type to use for Arrow Utf8 unparsing
    /// Most dialects use VARCHAR, but some, like MySQL, require CHAR
    fn utf8_cast_dtype(&self) -> ast::DataType {
        ast::DataType::Varchar(None)
    }

    /// The SQL type to use for Arrow LargeUtf8 unparsing
    /// Most dialects use TEXT, but some, like MySQL, require CHAR
    fn large_utf8_cast_dtype(&self) -> ast::DataType {
        ast::DataType::Text
    }

    /// The date field extract style to use: `DateFieldExtractStyle`
    fn date_field_extract_style(&self) -> DateFieldExtractStyle {
        DateFieldExtractStyle::DatePart
    }

    /// The SQL type to use for Arrow Int64 unparsing
    /// Most dialects use BigInt, but some, like MySQL, require SIGNED
    fn int64_cast_dtype(&self) -> ast::DataType {
        ast::DataType::BigInt(None)
    }

    /// The SQL type to use for Arrow Int32 unparsing
    /// Most dialects use Integer, but some, like MySQL, require SIGNED
    fn int32_cast_dtype(&self) -> ast::DataType {
        ast::DataType::Integer(None)
    }

    /// The SQL type to use for Timestamp unparsing
    /// Most dialects use Timestamp, but some, like MySQL, require Datetime
    /// Some dialects like Dremio does not support WithTimeZone and requires always Timestamp
    fn timestamp_cast_dtype(
        &self,
        _time_unit: &TimeUnit,
        tz: &Option<Arc<str>>,
    ) -> ast::DataType {
        let tz_info = match tz {
            Some(_) => TimezoneInfo::WithTimeZone,
            None => TimezoneInfo::None,
        };

        ast::DataType::Timestamp(None, tz_info)
    }

    /// The SQL type to use for Arrow Date32 unparsing
    /// Most dialects use Date, but some, like SQLite require TEXT
    fn date32_cast_dtype(&self) -> sqlparser::ast::DataType {
        sqlparser::ast::DataType::Date
    }

    /// Does the dialect support specifying column aliases as part of alias table definition?
    /// (SELECT col1, col2 from my_table) AS my_table_alias(col1_alias, col2_alias)
    fn supports_column_alias_in_table_alias(&self) -> bool {
        true
    }

    /// Whether the dialect requires a table alias for any subquery in the FROM clause
    /// This affects behavior when deriving logical plans for Sort, Limit, etc.
    fn requires_derived_table_alias(&self) -> bool {
        false
    }

    fn scalar_function_to_sql_overrides(
        &self,
        _unparser: &Unparser,
        _func_name: &str,
        _args: &[Expr],
    ) -> Result<Option<ast::Expr>> {
        Ok(None)
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

/// Datetime subfield extraction style for unparsing
///
/// `<https://www.postgresql.org/docs/current/functions-datetime.html#FUNCTIONS-DATETIME-EXTRACT>`
/// Different DBMSs follow different standards; popular ones are:
/// date_part('YEAR', date '2001-02-16')
/// EXTRACT(YEAR from date '2001-02-16')
/// Some DBMSs, like Postgres, support both, whereas others like MySQL require EXTRACT.
#[derive(Clone, Copy, PartialEq)]
pub enum DateFieldExtractStyle {
    DatePart,
    Extract,
    Strftime,
}

pub struct DefaultDialect {}

impl Dialect for DefaultDialect {
    fn identifier_quote_style(&self, identifier: &str) -> Option<char> {
        let identifier_regex = Regex::new(r"^[a-zA-Z_][a-zA-Z0-9_]*$").unwrap();
        let id_upper = identifier.to_uppercase();
        // special case ignore "ID", see https://github.com/sqlparser-rs/sqlparser-rs/issues/1382
        // ID is a keyword in ClickHouse, but we don't want to quote it when unparsing SQL here
        if (id_upper != "ID" && ALL_KEYWORDS.contains(&id_upper.as_str()))
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

    fn float64_ast_dtype(&self) -> sqlparser::ast::DataType {
        sqlparser::ast::DataType::DoublePrecision
    }

    fn scalar_function_to_sql_overrides(
        &self,
        unparser: &Unparser,
        func_name: &str,
        args: &[Expr],
    ) -> Result<Option<ast::Expr>> {
        if func_name == "round" {
            return Ok(Some(
                self.round_to_sql_enforce_numeric(unparser, func_name, args)?,
            ));
        }

        Ok(None)
    }
}

impl PostgreSqlDialect {
    fn round_to_sql_enforce_numeric(
        &self,
        unparser: &Unparser,
        func_name: &str,
        args: &[Expr],
    ) -> Result<ast::Expr> {
        let mut args = unparser.function_args_to_sql(args)?;

        // Enforce the first argument to be Numeric
        if let Some(ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(expr))) =
            args.first_mut()
        {
            if let ast::Expr::Cast { data_type, .. } = expr {
                // Don't create an additional cast wrapper if we can update the existing one
                *data_type = ast::DataType::Numeric(ast::ExactNumberInfo::None);
            } else {
                // Wrap the expression in a new cast
                *expr = ast::Expr::Cast {
                    kind: ast::CastKind::Cast,
                    expr: Box::new(expr.clone()),
                    data_type: ast::DataType::Numeric(ast::ExactNumberInfo::None),
                    format: None,
                };
            }
        }

        Ok(ast::Expr::Function(Function {
            name: ast::ObjectName(vec![Ident {
                value: func_name.to_string(),
                quote_style: None,
            }]),
            args: ast::FunctionArguments::List(ast::FunctionArgumentList {
                duplicate_treatment: None,
                args,
                clauses: vec![],
            }),
            filter: None,
            null_treatment: None,
            over: None,
            within_group: vec![],
            parameters: ast::FunctionArguments::None,
        }))
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

    fn date_field_extract_style(&self) -> DateFieldExtractStyle {
        DateFieldExtractStyle::Extract
    }

    fn int64_cast_dtype(&self) -> ast::DataType {
        ast::DataType::Custom(ObjectName(vec![Ident::new("SIGNED")]), vec![])
    }

    fn int32_cast_dtype(&self) -> ast::DataType {
        ast::DataType::Custom(ObjectName(vec![Ident::new("SIGNED")]), vec![])
    }

    fn timestamp_cast_dtype(
        &self,
        _time_unit: &TimeUnit,
        _tz: &Option<Arc<str>>,
    ) -> ast::DataType {
        ast::DataType::Datetime(None)
    }

    fn requires_derived_table_alias(&self) -> bool {
        true
    }

    fn scalar_function_to_sql_overrides(
        &self,
        unparser: &Unparser,
        func_name: &str,
        args: &[Expr],
    ) -> Result<Option<ast::Expr>> {
        if func_name == "date_part" {
            return date_part_to_sql(unparser, self.date_field_extract_style(), args);
        }

        Ok(None)
    }
}

pub struct SqliteDialect {}

impl Dialect for SqliteDialect {
    fn identifier_quote_style(&self, _: &str) -> Option<char> {
        Some('`')
    }

    fn date_field_extract_style(&self) -> DateFieldExtractStyle {
        DateFieldExtractStyle::Strftime
    }

    fn date32_cast_dtype(&self) -> sqlparser::ast::DataType {
        sqlparser::ast::DataType::Text
    }

    fn supports_column_alias_in_table_alias(&self) -> bool {
        false
    }

    fn scalar_function_to_sql_overrides(
        &self,
        unparser: &Unparser,
        func_name: &str,
        args: &[Expr],
    ) -> Result<Option<ast::Expr>> {
        if func_name == "date_part" {
            return date_part_to_sql(unparser, self.date_field_extract_style(), args);
        }

        Ok(None)
    }
}

pub struct CustomDialect {
    identifier_quote_style: Option<char>,
    supports_nulls_first_in_sort: bool,
    use_timestamp_for_date64: bool,
    interval_style: IntervalStyle,
    float64_ast_dtype: sqlparser::ast::DataType,
    utf8_cast_dtype: ast::DataType,
    large_utf8_cast_dtype: ast::DataType,
    date_field_extract_style: DateFieldExtractStyle,
    int64_cast_dtype: ast::DataType,
    int32_cast_dtype: ast::DataType,
    timestamp_cast_dtype: ast::DataType,
    timestamp_tz_cast_dtype: ast::DataType,
    date32_cast_dtype: sqlparser::ast::DataType,
    supports_column_alias_in_table_alias: bool,
    requires_derived_table_alias: bool,
}

impl Default for CustomDialect {
    fn default() -> Self {
        Self {
            identifier_quote_style: None,
            supports_nulls_first_in_sort: true,
            use_timestamp_for_date64: false,
            interval_style: IntervalStyle::SQLStandard,
            float64_ast_dtype: sqlparser::ast::DataType::Double,
            utf8_cast_dtype: ast::DataType::Varchar(None),
            large_utf8_cast_dtype: ast::DataType::Text,
            date_field_extract_style: DateFieldExtractStyle::DatePart,
            int64_cast_dtype: ast::DataType::BigInt(None),
            int32_cast_dtype: ast::DataType::Integer(None),
            timestamp_cast_dtype: ast::DataType::Timestamp(None, TimezoneInfo::None),
            timestamp_tz_cast_dtype: ast::DataType::Timestamp(
                None,
                TimezoneInfo::WithTimeZone,
            ),
            date32_cast_dtype: sqlparser::ast::DataType::Date,
            supports_column_alias_in_table_alias: true,
            requires_derived_table_alias: false,
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

    fn float64_ast_dtype(&self) -> sqlparser::ast::DataType {
        self.float64_ast_dtype.clone()
    }

    fn utf8_cast_dtype(&self) -> ast::DataType {
        self.utf8_cast_dtype.clone()
    }

    fn large_utf8_cast_dtype(&self) -> ast::DataType {
        self.large_utf8_cast_dtype.clone()
    }

    fn date_field_extract_style(&self) -> DateFieldExtractStyle {
        self.date_field_extract_style
    }

    fn int64_cast_dtype(&self) -> ast::DataType {
        self.int64_cast_dtype.clone()
    }

    fn int32_cast_dtype(&self) -> ast::DataType {
        self.int32_cast_dtype.clone()
    }

    fn timestamp_cast_dtype(
        &self,
        _time_unit: &TimeUnit,
        tz: &Option<Arc<str>>,
    ) -> ast::DataType {
        if tz.is_some() {
            self.timestamp_tz_cast_dtype.clone()
        } else {
            self.timestamp_cast_dtype.clone()
        }
    }

    fn date32_cast_dtype(&self) -> sqlparser::ast::DataType {
        self.date32_cast_dtype.clone()
    }

    fn supports_column_alias_in_table_alias(&self) -> bool {
        self.supports_column_alias_in_table_alias
    }

    fn scalar_function_to_sql_overrides(
        &self,
        unparser: &Unparser,
        func_name: &str,
        args: &[Expr],
    ) -> Result<Option<ast::Expr>> {
        if func_name == "date_part" {
            return date_part_to_sql(unparser, self.date_field_extract_style(), args);
        }

        Ok(None)
    }

    fn requires_derived_table_alias(&self) -> bool {
        self.requires_derived_table_alias
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
    float64_ast_dtype: sqlparser::ast::DataType,
    utf8_cast_dtype: ast::DataType,
    large_utf8_cast_dtype: ast::DataType,
    date_field_extract_style: DateFieldExtractStyle,
    int64_cast_dtype: ast::DataType,
    int32_cast_dtype: ast::DataType,
    timestamp_cast_dtype: ast::DataType,
    timestamp_tz_cast_dtype: ast::DataType,
    date32_cast_dtype: ast::DataType,
    supports_column_alias_in_table_alias: bool,
    requires_derived_table_alias: bool,
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
            float64_ast_dtype: sqlparser::ast::DataType::Double,
            utf8_cast_dtype: ast::DataType::Varchar(None),
            large_utf8_cast_dtype: ast::DataType::Text,
            date_field_extract_style: DateFieldExtractStyle::DatePart,
            int64_cast_dtype: ast::DataType::BigInt(None),
            int32_cast_dtype: ast::DataType::Integer(None),
            timestamp_cast_dtype: ast::DataType::Timestamp(None, TimezoneInfo::None),
            timestamp_tz_cast_dtype: ast::DataType::Timestamp(
                None,
                TimezoneInfo::WithTimeZone,
            ),
            date32_cast_dtype: sqlparser::ast::DataType::Date,
            supports_column_alias_in_table_alias: true,
            requires_derived_table_alias: false,
        }
    }

    pub fn build(self) -> CustomDialect {
        CustomDialect {
            identifier_quote_style: self.identifier_quote_style,
            supports_nulls_first_in_sort: self.supports_nulls_first_in_sort,
            use_timestamp_for_date64: self.use_timestamp_for_date64,
            interval_style: self.interval_style,
            float64_ast_dtype: self.float64_ast_dtype,
            utf8_cast_dtype: self.utf8_cast_dtype,
            large_utf8_cast_dtype: self.large_utf8_cast_dtype,
            date_field_extract_style: self.date_field_extract_style,
            int64_cast_dtype: self.int64_cast_dtype,
            int32_cast_dtype: self.int32_cast_dtype,
            timestamp_cast_dtype: self.timestamp_cast_dtype,
            timestamp_tz_cast_dtype: self.timestamp_tz_cast_dtype,
            date32_cast_dtype: self.date32_cast_dtype,
            supports_column_alias_in_table_alias: self
                .supports_column_alias_in_table_alias,
            requires_derived_table_alias: self.requires_derived_table_alias,
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

    /// Customize the dialect with a specific SQL type for Float64 casting: DOUBLE, DOUBLE PRECISION, etc.
    pub fn with_float64_ast_dtype(
        mut self,
        float64_ast_dtype: sqlparser::ast::DataType,
    ) -> Self {
        self.float64_ast_dtype = float64_ast_dtype;
        self
    }

    /// Customize the dialect with a specific SQL type for Utf8 casting: VARCHAR, CHAR, etc.
    pub fn with_utf8_cast_dtype(mut self, utf8_cast_dtype: ast::DataType) -> Self {
        self.utf8_cast_dtype = utf8_cast_dtype;
        self
    }

    /// Customize the dialect with a specific SQL type for LargeUtf8 casting: TEXT, CHAR, etc.
    pub fn with_large_utf8_cast_dtype(
        mut self,
        large_utf8_cast_dtype: ast::DataType,
    ) -> Self {
        self.large_utf8_cast_dtype = large_utf8_cast_dtype;
        self
    }

    /// Customize the dialect with a specific date field extract style listed in `DateFieldExtractStyle`
    pub fn with_date_field_extract_style(
        mut self,
        date_field_extract_style: DateFieldExtractStyle,
    ) -> Self {
        self.date_field_extract_style = date_field_extract_style;
        self
    }

    /// Customize the dialect with a specific SQL type for Int64 casting: BigInt, SIGNED, etc.
    pub fn with_int64_cast_dtype(mut self, int64_cast_dtype: ast::DataType) -> Self {
        self.int64_cast_dtype = int64_cast_dtype;
        self
    }

    /// Customize the dialect with a specific SQL type for Int32 casting: BigInt, SIGNED, etc.
    pub fn with_int32_cast_dtype(mut self, int32_cast_dtype: ast::DataType) -> Self {
        self.int32_cast_dtype = int32_cast_dtype;
        self
    }

    /// Customize the dialect with a specific SQL type for Timestamp casting: Timestamp, Datetime, etc.
    pub fn with_timestamp_cast_dtype(
        mut self,
        timestamp_cast_dtype: ast::DataType,
        timestamp_tz_cast_dtype: ast::DataType,
    ) -> Self {
        self.timestamp_cast_dtype = timestamp_cast_dtype;
        self.timestamp_tz_cast_dtype = timestamp_tz_cast_dtype;
        self
    }

    pub fn with_date32_cast_dtype(mut self, date32_cast_dtype: ast::DataType) -> Self {
        self.date32_cast_dtype = date32_cast_dtype;
        self
    }

    /// Customize the dialect to supports column aliases as part of alias table definition
    pub fn with_supports_column_alias_in_table_alias(
        mut self,
        supports_column_alias_in_table_alias: bool,
    ) -> Self {
        self.supports_column_alias_in_table_alias = supports_column_alias_in_table_alias;
        self
    }

    pub fn with_requires_derived_table_alias(
        mut self,
        requires_derived_table_alias: bool,
    ) -> Self {
        self.requires_derived_table_alias = requires_derived_table_alias;
        self
    }
}
