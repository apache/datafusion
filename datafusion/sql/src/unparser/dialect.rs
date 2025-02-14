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

use std::{collections::HashMap, sync::Arc};

use super::{utils::character_length_to_sql, utils::date_part_to_sql, Unparser};
use arrow::datatypes::TimeUnit;
use datafusion_common::Result;
use datafusion_expr::Expr;
use regex::Regex;
use sqlparser::tokenizer::Span;
use sqlparser::{
    ast::{
        self, BinaryOperator, Function, Ident, ObjectName, TimezoneInfo, WindowFrameBound,
    },
    keywords::ALL_KEYWORDS,
};

pub type ScalarFnToSqlHandler =
    Box<dyn Fn(&Unparser, &[Expr]) -> Result<Option<ast::Expr>> + Send + Sync>;

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
    fn float64_ast_dtype(&self) -> ast::DataType {
        ast::DataType::Double(ast::ExactNumberInfo::None)
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

    /// The character length extraction style to use: `CharacterLengthStyle`
    fn character_length_style(&self) -> CharacterLengthStyle {
        CharacterLengthStyle::CharacterLength
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
    fn date32_cast_dtype(&self) -> ast::DataType {
        ast::DataType::Date
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

    /// The division operator for the dialect
    /// Most dialect uses ` BinaryOperator::Divide` (/)
    /// But DuckDB dialect uses `BinaryOperator::DuckIntegerDivide` (//)
    fn division_operator(&self) -> BinaryOperator {
        BinaryOperator::Divide
    }

    /// Allows the dialect to override scalar function unparsing if the dialect has specific rules.
    /// Returns None if the default unparsing should be used, or Some(ast::Expr) if there is
    /// a custom implementation for the function.
    fn scalar_function_to_sql_overrides(
        &self,
        _unparser: &Unparser,
        _func_name: &str,
        _args: &[Expr],
    ) -> Result<Option<ast::Expr>> {
        Ok(None)
    }

    /// Allows the dialect to choose to omit window frame in unparsing
    /// based on function name and window frame bound
    /// Returns false if specific function name / window frame bound indicates no window frame is needed in unparsing
    fn window_func_support_window_frame(
        &self,
        _func_name: &str,
        _start_bound: &WindowFrameBound,
        _end_bound: &WindowFrameBound,
    ) -> bool {
        true
    }

    /// Extends the dialect's default rules for unparsing scalar functions.
    /// This is useful for supporting application-specific UDFs or custom engine extensions.
    fn with_custom_scalar_overrides(
        self,
        _handlers: Vec<(&str, ScalarFnToSqlHandler)>,
    ) -> Self
    where
        Self: Sized,
    {
        unimplemented!("Custom scalar overrides are not supported by this dialect yet");
    }

    /// Allow to unparse a qualified column with a full qualified name
    /// (e.g. catalog_name.schema_name.table_name.column_name)
    /// Otherwise, the column will be unparsed with only the table name and column name
    /// (e.g. table_name.column_name)
    fn full_qualified_col(&self) -> bool {
        false
    }

    /// Allow to unparse the unnest plan as [ast::TableFactor::UNNEST].
    ///
    /// Some dialects like BigQuery require UNNEST to be used in the FROM clause but
    /// the LogicalPlan planner always puts UNNEST in the SELECT clause. This flag allows
    /// to unparse the UNNEST plan as [ast::TableFactor::UNNEST] instead of a subquery.
    fn unnest_as_table_factor(&self) -> bool {
        false
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

/// `CharacterLengthStyle` to use for unparsing
///
/// Different DBMSs uses different names for function calculating the number of characters in the string
/// `Length` style uses length(x)
/// `SQLStandard` style uses character_length(x)
#[derive(Clone, Copy, PartialEq)]
pub enum CharacterLengthStyle {
    Length,
    CharacterLength,
}

pub struct DefaultDialect {}

impl Dialect for DefaultDialect {
    fn identifier_quote_style(&self, identifier: &str) -> Option<char> {
        let identifier_regex = Regex::new(r"^[a-zA-Z_][a-zA-Z0-9_]*$").unwrap();
        let id_upper = identifier.to_uppercase();
        // Special case ignore "ID", see https://github.com/sqlparser-rs/sqlparser-rs/issues/1382
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

    fn float64_ast_dtype(&self) -> ast::DataType {
        ast::DataType::DoublePrecision
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
            name: ObjectName(vec![Ident {
                value: func_name.to_string(),
                quote_style: None,
                span: Span::empty(),
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
            uses_odbc_syntax: false,
        }))
    }
}

#[derive(Default)]
pub struct DuckDBDialect {
    custom_scalar_fn_overrides: HashMap<String, ScalarFnToSqlHandler>,
}

impl DuckDBDialect {
    #[must_use]
    pub fn new() -> Self {
        Self {
            custom_scalar_fn_overrides: HashMap::new(),
        }
    }
}

impl Dialect for DuckDBDialect {
    fn identifier_quote_style(&self, _: &str) -> Option<char> {
        Some('"')
    }

    fn character_length_style(&self) -> CharacterLengthStyle {
        CharacterLengthStyle::Length
    }

    fn division_operator(&self) -> BinaryOperator {
        BinaryOperator::DuckIntegerDivide
    }

    fn with_custom_scalar_overrides(
        mut self,
        handlers: Vec<(&str, ScalarFnToSqlHandler)>,
    ) -> Self {
        for (func_name, handler) in handlers {
            self.custom_scalar_fn_overrides
                .insert(func_name.to_string(), handler);
        }
        self
    }

    fn scalar_function_to_sql_overrides(
        &self,
        unparser: &Unparser,
        func_name: &str,
        args: &[Expr],
    ) -> Result<Option<ast::Expr>> {
        if let Some(handler) = self.custom_scalar_fn_overrides.get(func_name) {
            return handler(unparser, args);
        }

        if func_name == "character_length" {
            return character_length_to_sql(
                unparser,
                self.character_length_style(),
                args,
            );
        }

        Ok(None)
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

    fn date32_cast_dtype(&self) -> ast::DataType {
        ast::DataType::Text
    }

    fn character_length_style(&self) -> CharacterLengthStyle {
        CharacterLengthStyle::Length
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
        match func_name {
            "date_part" => {
                date_part_to_sql(unparser, self.date_field_extract_style(), args)
            }
            "character_length" => {
                character_length_to_sql(unparser, self.character_length_style(), args)
            }
            _ => Ok(None),
        }
    }
}

pub struct CustomDialect {
    identifier_quote_style: Option<char>,
    supports_nulls_first_in_sort: bool,
    use_timestamp_for_date64: bool,
    interval_style: IntervalStyle,
    float64_ast_dtype: ast::DataType,
    utf8_cast_dtype: ast::DataType,
    large_utf8_cast_dtype: ast::DataType,
    date_field_extract_style: DateFieldExtractStyle,
    character_length_style: CharacterLengthStyle,
    int64_cast_dtype: ast::DataType,
    int32_cast_dtype: ast::DataType,
    timestamp_cast_dtype: ast::DataType,
    timestamp_tz_cast_dtype: ast::DataType,
    date32_cast_dtype: ast::DataType,
    supports_column_alias_in_table_alias: bool,
    requires_derived_table_alias: bool,
    division_operator: BinaryOperator,
    window_func_support_window_frame: bool,
    full_qualified_col: bool,
    unnest_as_table_factor: bool,
}

impl Default for CustomDialect {
    fn default() -> Self {
        Self {
            identifier_quote_style: None,
            supports_nulls_first_in_sort: true,
            use_timestamp_for_date64: false,
            interval_style: IntervalStyle::SQLStandard,
            float64_ast_dtype: ast::DataType::Double(ast::ExactNumberInfo::None),
            utf8_cast_dtype: ast::DataType::Varchar(None),
            large_utf8_cast_dtype: ast::DataType::Text,
            date_field_extract_style: DateFieldExtractStyle::DatePart,
            character_length_style: CharacterLengthStyle::CharacterLength,
            int64_cast_dtype: ast::DataType::BigInt(None),
            int32_cast_dtype: ast::DataType::Integer(None),
            timestamp_cast_dtype: ast::DataType::Timestamp(None, TimezoneInfo::None),
            timestamp_tz_cast_dtype: ast::DataType::Timestamp(
                None,
                TimezoneInfo::WithTimeZone,
            ),
            date32_cast_dtype: ast::DataType::Date,
            supports_column_alias_in_table_alias: true,
            requires_derived_table_alias: false,
            division_operator: BinaryOperator::Divide,
            window_func_support_window_frame: true,
            full_qualified_col: false,
            unnest_as_table_factor: false,
        }
    }
}

impl CustomDialect {
    // Create a CustomDialect
    #[deprecated(since = "41.0.0", note = "please use `CustomDialectBuilder` instead")]
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

    fn float64_ast_dtype(&self) -> ast::DataType {
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

    fn character_length_style(&self) -> CharacterLengthStyle {
        self.character_length_style
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

    fn date32_cast_dtype(&self) -> ast::DataType {
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
        match func_name {
            "date_part" => {
                date_part_to_sql(unparser, self.date_field_extract_style(), args)
            }
            "character_length" => {
                character_length_to_sql(unparser, self.character_length_style(), args)
            }
            _ => Ok(None),
        }
    }

    fn requires_derived_table_alias(&self) -> bool {
        self.requires_derived_table_alias
    }

    fn division_operator(&self) -> BinaryOperator {
        self.division_operator.clone()
    }

    fn window_func_support_window_frame(
        &self,
        _func_name: &str,
        _start_bound: &WindowFrameBound,
        _end_bound: &WindowFrameBound,
    ) -> bool {
        self.window_func_support_window_frame
    }

    fn full_qualified_col(&self) -> bool {
        self.full_qualified_col
    }

    fn unnest_as_table_factor(&self) -> bool {
        self.unnest_as_table_factor
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
    float64_ast_dtype: ast::DataType,
    utf8_cast_dtype: ast::DataType,
    large_utf8_cast_dtype: ast::DataType,
    date_field_extract_style: DateFieldExtractStyle,
    character_length_style: CharacterLengthStyle,
    int64_cast_dtype: ast::DataType,
    int32_cast_dtype: ast::DataType,
    timestamp_cast_dtype: ast::DataType,
    timestamp_tz_cast_dtype: ast::DataType,
    date32_cast_dtype: ast::DataType,
    supports_column_alias_in_table_alias: bool,
    requires_derived_table_alias: bool,
    division_operator: BinaryOperator,
    window_func_support_window_frame: bool,
    full_qualified_col: bool,
    unnest_as_table_factor: bool,
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
            float64_ast_dtype: ast::DataType::Double(ast::ExactNumberInfo::None),
            utf8_cast_dtype: ast::DataType::Varchar(None),
            large_utf8_cast_dtype: ast::DataType::Text,
            date_field_extract_style: DateFieldExtractStyle::DatePart,
            character_length_style: CharacterLengthStyle::CharacterLength,
            int64_cast_dtype: ast::DataType::BigInt(None),
            int32_cast_dtype: ast::DataType::Integer(None),
            timestamp_cast_dtype: ast::DataType::Timestamp(None, TimezoneInfo::None),
            timestamp_tz_cast_dtype: ast::DataType::Timestamp(
                None,
                TimezoneInfo::WithTimeZone,
            ),
            date32_cast_dtype: ast::DataType::Date,
            supports_column_alias_in_table_alias: true,
            requires_derived_table_alias: false,
            division_operator: BinaryOperator::Divide,
            window_func_support_window_frame: true,
            full_qualified_col: false,
            unnest_as_table_factor: false,
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
            character_length_style: self.character_length_style,
            int64_cast_dtype: self.int64_cast_dtype,
            int32_cast_dtype: self.int32_cast_dtype,
            timestamp_cast_dtype: self.timestamp_cast_dtype,
            timestamp_tz_cast_dtype: self.timestamp_tz_cast_dtype,
            date32_cast_dtype: self.date32_cast_dtype,
            supports_column_alias_in_table_alias: self
                .supports_column_alias_in_table_alias,
            requires_derived_table_alias: self.requires_derived_table_alias,
            division_operator: self.division_operator,
            window_func_support_window_frame: self.window_func_support_window_frame,
            full_qualified_col: self.full_qualified_col,
            unnest_as_table_factor: self.unnest_as_table_factor,
        }
    }

    /// Customize the dialect with a specific identifier quote style, e.g. '`', '"'
    pub fn with_identifier_quote_style(mut self, identifier_quote_style: char) -> Self {
        self.identifier_quote_style = Some(identifier_quote_style);
        self
    }

    /// Customize the dialect to support `NULLS FIRST` in `ORDER BY` clauses
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

    /// Customize the dialect with a specific character_length_style listed in `CharacterLengthStyle`
    pub fn with_character_length_style(
        mut self,
        character_length_style: CharacterLengthStyle,
    ) -> Self {
        self.character_length_style = character_length_style;
        self
    }

    /// Customize the dialect with a specific SQL type for Float64 casting: DOUBLE, DOUBLE PRECISION, etc.
    pub fn with_float64_ast_dtype(mut self, float64_ast_dtype: ast::DataType) -> Self {
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

    /// Customize the dialect with a specific SQL type for Int32 casting: Integer, SIGNED, etc.
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

    /// Customize the dialect to support column aliases as part of alias table definition
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

    pub fn with_division_operator(mut self, division_operator: BinaryOperator) -> Self {
        self.division_operator = division_operator;
        self
    }

    pub fn with_window_func_support_window_frame(
        mut self,
        window_func_support_window_frame: bool,
    ) -> Self {
        self.window_func_support_window_frame = window_func_support_window_frame;
        self
    }

    /// Customize the dialect to allow full qualified column names
    pub fn with_full_qualified_col(mut self, full_qualified_col: bool) -> Self {
        self.full_qualified_col = full_qualified_col;
        self
    }

    pub fn with_unnest_as_table_factor(mut self, _unnest_as_table_factor: bool) -> Self {
        self.unnest_as_table_factor = _unnest_as_table_factor;
        self
    }
}
