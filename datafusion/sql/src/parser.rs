// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! [`DFParser`]: DataFusion SQL Parser based on [`sqlparser`]
//!
//! This parser implements DataFusion specific statements such as
//! `CREATE EXTERNAL TABLE`

use std::borrow::Cow;
use std::collections::VecDeque;
use std::fmt;
use std::str::FromStr;

use datafusion_common::DataFusionError;
use datafusion_common::config::{ConfigNonZeroUsize, SqlParserOptions};
use datafusion_common::format::{ExplainFormat, ExplainStatementOptions};
use datafusion_common::{Diagnostic, Span};
use sqlparser::ParseError;
use sqlparser::ast::{DropBehavior, DropObject, ObjectType, StatementKind};
use sqlparser::dialect::{
    BigQuery, ClickHouse, Core, Databricks, Dialect, DuckDb, Hive, MsSql, MySql, Oracle,
    Postgres, Redshift, Snowflake, Spark, Sqlite, Teradata,
};
use sqlparser::grammar;
use sqlparser::lexer::TokenKind;
use sqlparser::parser::Parser;

use crate::ast::{
    Builtin, ColumnDef, ExprWithAlias, Ident, ObjectName, OrderByItem, Query,
    Statement as SQLStatement, TableConstraint,
};

pub use sqlparser::dialect::DialectKind;

/// A SQL dialect DataFusion can parse with.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SqlDialect {
    /// DataFusion's own dialect, [`crate::dialect::DataFusion`].
    DataFusion,
    /// One of `sqlparser`'s built-in dialects.
    Builtin(DialectKind),
}

impl From<DialectKind> for SqlDialect {
    fn from(kind: DialectKind) -> Self {
        Self::Builtin(kind)
    }
}

/// The dialect DataFusion parses with unless another is chosen.
pub const DEFAULT_DIALECT: SqlDialect = SqlDialect::DataFusion;

/// Finds a dialect by the name DataFusion's configuration uses for it, ignoring
/// case. `generic`, `ansi` and `datafusion` all name DataFusion's own dialect.
pub fn dialect_from_str(name: &str) -> Option<SqlDialect> {
    match name.to_ascii_lowercase().as_str() {
        "generic" | "ansi" | "datafusion" => Some(SqlDialect::DataFusion),
        other => DialectKind::from_name(other).map(SqlDialect::Builtin),
    }
}

/// Calls a function that is generic over the dialect with the dialect that
/// `dialect` names.
macro_rules! dispatch {
    ($dialect:expr, $function:ident($($arg:expr),*)) => {
        match $dialect {
            SqlDialect::DataFusion => $function::<crate::dialect::DataFusion>($($arg),*),
            SqlDialect::Builtin(kind) => dispatch_builtin!(kind, $function($($arg),*)),
        }
    };
}

/// Like [`dispatch`], for a built-in dialect.
macro_rules! dispatch_builtin {
    ($kind:expr, $function:ident($($arg:expr),*)) => {
        match $kind {
            DialectKind::Core => $function::<Core>($($arg),*),
            DialectKind::MySql => $function::<MySql>($($arg),*),
            DialectKind::Postgres => $function::<Postgres>($($arg),*),
            DialectKind::Redshift => $function::<Redshift>($($arg),*),
            DialectKind::Snowflake => $function::<Snowflake>($($arg),*),
            DialectKind::Spark => $function::<Spark>($($arg),*),
            DialectKind::Databricks => $function::<Databricks>($($arg),*),
            DialectKind::Hive => $function::<Hive>($($arg),*),
            DialectKind::BigQuery => $function::<BigQuery>($($arg),*),
            DialectKind::ClickHouse => $function::<ClickHouse>($($arg),*),
            DialectKind::DuckDb => $function::<DuckDb>($($arg),*),
            DialectKind::MsSql => $function::<MsSql>($($arg),*),
            DialectKind::Oracle => $function::<Oracle>($($arg),*),
            DialectKind::Sqlite => $function::<Sqlite>($($arg),*),
            DialectKind::Teradata => $function::<Teradata>($($arg),*),
        }
    };
}

/// A parse error with no position.
fn parse_error(message: impl Into<String>) -> ParseError {
    ParseError {
        message: message.into(),
        span: sqlparser::ast::Span::default(),
    }
}

// Use `Parser::expected` instead, if possible
macro_rules! parser_err {
    ($MSG:expr $(; diagnostic = $DIAG:expr)?) => {{
        let err = DataFusionError::from(parse_error($MSG.to_string()));
        $(
            let err = err.with_diagnostic($DIAG);
        )?
        Err(err)
    }};
}

fn parse_file_type(s: &str) -> Result<String, DataFusionError> {
    Ok(s.to_uppercase())
}

/// DataFusion specific `EXPLAIN`
///
/// Supports both the legacy keyword form and, on dialects that have it
/// (PostgreSQL, DuckDB, etc.), the Postgres-style parenthesized option list:
///
/// ```sql
/// -- Legacy keyword form (any dialect)
/// EXPLAIN <ANALYZE> <VERBOSE> [FORMAT format] statement
///
/// -- Postgres-style option form (dialect-gated)
/// EXPLAIN (option [arg] [, ...]) statement
/// ```
///
/// See [`ExplainStatementOptions`] for the list of supported options in the
/// parenthesized form.
#[derive(Debug, Clone, PartialEq)]
pub struct ExplainStatement<'a> {
    /// Normalized options parsed from either the legacy keyword form or the
    /// parenthesized option list.
    pub options: ExplainStatementOptions,
    /// The statement to analyze. Note this is a DataFusion [`Statement`] (not a
    /// [`sqlparser::ast::Statement`] so that we can use `EXPLAIN`, `COPY`, and other
    /// DataFusion specific statements
    pub statement: Box<Statement<'a>>,
}

impl fmt::Display for ExplainStatement<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Self { options, statement } = self;

        // If only the legacy-era fields are set, print the legacy keyword
        // form so existing round-trip tests continue to pass.
        let uses_parenthesized = options.analyze_level.is_some()
            || options.analyze_categories.is_some()
            || options.show_statistics.is_some();

        write!(f, "EXPLAIN ")?;
        if uses_parenthesized {
            // Emit a parenthesized option list.
            let mut parts: Vec<String> = Vec::new();
            if options.analyze {
                parts.push("ANALYZE".to_string());
            }
            if options.verbose {
                parts.push("VERBOSE".to_string());
            }
            if let Some(format) = &options.format {
                parts.push(format!("FORMAT {format}"));
            }
            if let Some(level) = options.analyze_level {
                parts.push(format!("LEVEL {level}"));
            }
            if let Some(cats) = &options.analyze_categories {
                parts.push(format!("METRICS '{cats}'"));
            }
            if let Some(stats) = options.show_statistics {
                parts.push(format!("COSTS {}", if stats { "ON" } else { "OFF" }));
            }
            write!(f, "({}) ", parts.join(", "))?;
        } else {
            if options.analyze {
                write!(f, "ANALYZE ")?;
            }
            if options.verbose {
                write!(f, "VERBOSE ")?;
            }
            if let Some(format) = &options.format {
                write!(f, "FORMAT {format} ")?;
            }
        }

        write!(f, "{statement}")
    }
}

/// The value of a `key value` option, in `COPY`, `CREATE EXTERNAL TABLE` and
/// `CREATE EXTERNAL CATALOG`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OptionValue {
    /// A string, or a word such as `snappy` that may be a keyword.
    String(String),
    /// A double-quoted string.
    DoubleQuotedString(String),
    /// A number, as written.
    Number(String),
}

impl OptionValue {
    /// The value as text. A double-quoted string has none.
    pub fn as_str(&self) -> Option<&str> {
        match self {
            OptionValue::String(s) | OptionValue::Number(s) => Some(s),
            OptionValue::DoubleQuotedString(_) => None,
        }
    }
}

impl fmt::Display for OptionValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            OptionValue::String(s) => write!(f, "'{}'", s.replace('\'', "''")),
            OptionValue::DoubleQuotedString(s) => {
                write!(f, "\"{}\"", s.replace('"', "\"\""))
            }
            OptionValue::Number(n) => f.write_str(n),
        }
    }
}

/// DataFusion extension DDL for `COPY`
///
/// # Syntax:
///
/// ```text
/// COPY <table_name | (<query>)>
/// TO
/// <destination_url>
/// (key_value_list)
/// ```
///
/// # Examples
///
/// ```sql
/// COPY lineitem  TO 'lineitem'
/// STORED AS PARQUET (
///   partitions 16,
///   row_group_limit_rows 100000,
///   row_group_limit_bytes 200000
/// )
///
/// COPY (SELECT l_orderkey from lineitem) to 'lineitem.parquet';
/// ```
#[derive(Debug, Clone, PartialEq)]
pub struct CopyToStatement<'a> {
    /// From where the data comes from
    pub source: CopyToSource<'a>,
    /// The URL to where the data is heading
    pub target: String,
    /// Partition keys
    pub partitioned_by: Vec<String>,
    /// File type (Parquet, NDJSON, CSV etc.)
    pub stored_as: Option<String>,
    /// Target specific options
    pub options: Vec<(String, OptionValue)>,
}

impl fmt::Display for CopyToStatement<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Self {
            source,
            target,
            partitioned_by,
            stored_as,
            options,
            ..
        } = self;

        write!(f, "COPY {source} TO {target}")?;
        if let Some(file_type) = stored_as {
            write!(f, " STORED AS {file_type}")?;
        }
        if !partitioned_by.is_empty() {
            write!(f, " PARTITIONED BY ({})", partitioned_by.join(", "))?;
        }

        if !options.is_empty() {
            let opts: Vec<_> =
                options.iter().map(|(k, v)| format!("'{k}' {v}")).collect();
            write!(f, " OPTIONS ({})", opts.join(", "))?;
        }

        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum CopyToSource<'a> {
    /// `COPY <table> TO ...`
    Relation(ObjectName<'a>),
    /// COPY (...query...) TO ...
    Query(Box<Query<'a>>),
}

impl fmt::Display for CopyToSource<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CopyToSource::Relation(r) => write!(f, "{r}"),
            CopyToSource::Query(q) => write!(f, "({q})"),
        }
    }
}

/// This type defines a lexicographical ordering.
pub(crate) type LexOrdering<'a> = Vec<OrderByItem<'a>>;

/// DataFusion extension DDL for `CREATE EXTERNAL TABLE`
///
/// Syntax:
///
/// ```text
/// CREATE
/// [ OR REPLACE ]
/// EXTERNAL TABLE
/// [ IF NOT EXISTS ]
/// <TABLE_NAME>[ (<column_definition>) ]
/// STORED AS <file_type>
/// [ PARTITIONED BY (<column_definition list> | <column list>) ]
/// [ WITH ORDER (<ordered column list>)
/// [ OPTIONS (<key_value_list>) ]
/// LOCATION <literal> | LOCATION (<literal>[, ...])
///
/// <column_definition> := (<column_name> <data_type>, ...)
///
/// <column_list> := (<column_name>, ...)
///
/// <ordered_column_list> := (<column_name> <sort_clause>, ...)
///
/// <key_value_list> := (<literal> <literal, <literal> <literal>, ...)
/// ```
#[derive(Debug, Clone, PartialEq)]
pub struct CreateExternalTable<'a> {
    /// Table name
    pub name: ObjectName<'a>,
    /// Optional schema
    pub columns: Vec<ColumnDef<'a>>,
    /// File type (Parquet, NDJSON, CSV, etc)
    pub file_type: String,
    /// Paths to files
    pub locations: Vec<String>,
    /// Partition Columns
    pub table_partition_cols: Vec<String>,
    /// Ordered expressions
    pub order_exprs: Vec<LexOrdering<'a>>,
    /// Option to not error if table already exists
    pub if_not_exists: bool,
    /// Option to replace table content if table already exists
    pub or_replace: bool,
    /// Whether the table is a temporary table
    pub temporary: bool,
    /// Infinite streams?
    pub unbounded: bool,
    /// Table(provider) specific options
    pub options: Vec<(String, OptionValue)>,
    /// A table-level constraint
    pub constraints: Vec<TableConstraint<'a>>,
}

/// A string literal for `text`.
fn quoted(text: &str) -> String {
    format!("'{}'", text.replace('\'', "''"))
}

impl fmt::Display for CreateExternalTable<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CREATE ")?;
        if self.or_replace {
            write!(f, "OR REPLACE ")?;
        }
        write!(f, "EXTERNAL TABLE ")?;
        if self.if_not_exists {
            write!(f, "IF NOT EXISTS ")?;
        }
        write!(f, "{} ", self.name)?;
        write!(f, "STORED AS {} ", self.file_type)?;
        if !self.order_exprs.is_empty() {
            write!(f, "WITH ORDER (")?;
            let mut first = true;
            for expr in self.order_exprs.iter().flatten() {
                if !first {
                    write!(f, ", ")?;
                }
                write!(f, "{expr}")?;
                first = false;
            }
            write!(f, ") ")?;
        }
        match self.locations.as_slice() {
            [location] => write!(f, "LOCATION {}", quoted(location)),
            locations => {
                write!(f, "LOCATION (")?;
                for (idx, location) in locations.iter().enumerate() {
                    if idx > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{}", quoted(location))?;
                }
                write!(f, ")")
            }
        }
    }
}

/// DataFusion extension `CREATE EXTERNAL CATALOG` statement.
///
/// ```sql
/// CREATE [OR REPLACE] EXTERNAL CATALOG [IF NOT EXISTS] <catalog_name>
/// STORED AS <catalog_type>
/// [ LOCATION <literal> ]
/// [ OPTIONS (<key_value_list>) ]
///
/// <key_value_list> := (<literal> <literal>, <literal> <literal>, ...)
/// ```
#[derive(Debug, Clone, PartialEq)]
pub struct CreateExternalCatalog<'a> {
    /// Catalog name
    pub catalog_name: ObjectName<'a>,
    /// The key used to look up the registered `CatalogProviderFactory`
    pub catalog_type: String,
    /// The physical location of the catalog, if applicable
    pub location: Option<String>,
    /// Option to not error if catalog already exists
    pub if_not_exists: bool,
    /// Option to replace the catalog if it already exists
    pub or_replace: bool,
    /// Catalog(provider) specific options
    pub options: Vec<(String, OptionValue)>,
}

impl fmt::Display for CreateExternalCatalog<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CREATE EXTERNAL CATALOG ")?;
        if self.or_replace {
            write!(f, "OR REPLACE ")?;
        }
        write!(f, "EXTERNAL CATALOG ")?;
        if self.if_not_exists {
            write!(f, "IF NOT EXISTS ")?;
        }
        write!(f, "{} ", self.catalog_name)?;
        write!(f, "STORED AS {}", self.catalog_type)?;
        if let Some(location) = &self.location {
            write!(f, " LOCATION {}", quoted(location))?;
        }
        Ok(())
    }
}

/// DataFusion extension `DROP CATALOG` statement.
#[derive(Debug, Clone, PartialEq)]
pub struct DropCatalog<'a> {
    /// Catalog name
    pub name: ObjectName<'a>,
    /// Option to not error if the catalog does not exist
    pub if_exists: bool,
    /// Whether drop should cascade
    pub cascade: bool,
}

impl fmt::Display for DropCatalog<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "DROP CATALOG ")?;
        if self.if_exists {
            write!(f, "IF EXISTS ")?;
        }
        write!(f, "{}", self.name)
    }
}

/// DataFusion extension for `RESET`
#[derive(Debug, Clone, PartialEq)]
pub enum ResetStatement<'a> {
    /// Reset a single configuration variable (stored as provided)
    Variable(ObjectName<'a>),
}

impl fmt::Display for ResetStatement<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ResetStatement::Variable(name) => write!(f, "RESET {name}"),
        }
    }
}

/// DataFusion SQL Statement.
///
/// This can either be a [`Statement`] from [`sqlparser`] from a
/// standard SQL dialect, or a DataFusion extension such as `CREATE
/// EXTERNAL TABLE`. See [`DFParser`] for more information.
///
/// [`Statement`]: sqlparser::ast::Statement
#[derive(Debug, Clone, PartialEq)]
pub enum Statement<'a> {
    /// ANSI SQL AST node (from sqlparser-rs)
    Statement(Box<SQLStatement<'a>>),
    /// Extension: `CREATE EXTERNAL TABLE`
    CreateExternalTable(CreateExternalTable<'a>),
    /// Extension: `CREATE EXTERNAL CATALOG`
    CreateExternalCatalog(CreateExternalCatalog<'a>),
    /// Extension: `COPY TO`
    CopyTo(CopyToStatement<'a>),
    /// EXPLAIN for extensions
    Explain(ExplainStatement<'a>),
    /// Extension: `RESET`
    Reset(ResetStatement<'a>),
}

impl fmt::Display for Statement<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Statement::Statement(stmt) => write!(f, "{stmt}"),
            Statement::CreateExternalTable(stmt) => write!(f, "{stmt}"),
            Statement::CreateExternalCatalog(stmt) => write!(f, "{stmt}"),
            Statement::CopyTo(stmt) => write!(f, "{stmt}"),
            Statement::Explain(stmt) => write!(f, "{stmt}"),
            Statement::Reset(stmt) => write!(f, "{stmt}"),
        }
    }
}

fn ensure_not_set<T>(field: Option<&T>, name: &str) -> Result<(), DataFusionError> {
    if field.is_some() {
        parser_err!(format!("{name} specified more than once",))?
    }
    Ok(())
}

/// DataFusion SQL Parser based on [`sqlparser`]
///
/// Parses DataFusion's SQL dialect, often delegating to [`sqlparser`]'s
/// [`Parser`].
///
/// DataFusion mostly follows existing SQL dialects via
/// `sqlparser`. However, certain statements such as `COPY` and
/// `CREATE EXTERNAL TABLE` have special syntax in DataFusion. See
/// [`Statement`] for a list of this special syntax
pub struct DFParser<'a> {
    sql: &'a str,
    dialect: SqlDialect,
    options: SqlParserOptions,
}

/// Default recursion limit for SQL parsing.
const DEFAULT_RECURSION_LIMIT: usize = 51;

/// Builder for [`DFParser`]
///
/// # Example: Create and Parse SQL statements
/// ```
/// # use datafusion_sql::parser::DFParserBuilder;
/// # use datafusion_common::Result;
/// # fn test() -> Result<()> {
/// let mut parser = DFParserBuilder::new("SELECT * FROM foo; SELECT 1 + 2").build()?;
/// // parse the SQL into DFStatements
/// let statements = parser.parse_statements()?;
/// assert_eq!(statements.len(), 2);
/// # Ok(())
/// # }
/// ```
pub struct DFParserBuilder<'a> {
    sql: &'a str,
    dialect: SqlDialect,
    recursion_limit: usize,
}

impl<'a> DFParserBuilder<'a> {
    /// Create a new parser builder for the specified SQL using DataFusion's
    /// dialect.
    pub fn new(sql: &'a str) -> Self {
        Self {
            sql,
            dialect: DEFAULT_DIALECT,
            recursion_limit: DEFAULT_RECURSION_LIMIT,
        }
    }

    /// Adjust the parser builder's dialect.
    pub fn with_dialect(mut self, dialect: impl Into<SqlDialect>) -> Self {
        self.dialect = dialect.into();
        self
    }

    /// Adjust the recursion limit of sql parsing. Defaults to 51.
    pub fn with_recursion_limit(mut self, recursion_limit: usize) -> Self {
        self.recursion_limit = recursion_limit;
        self
    }

    /// Build resulting parser
    pub fn build(self) -> Result<DFParser<'a>, DataFusionError> {
        Ok(DFParser {
            sql: self.sql,
            dialect: self.dialect,
            options: SqlParserOptions {
                recursion_limit: ConfigNonZeroUsize::try_new(self.recursion_limit)?,
                ..Default::default()
            },
        })
    }
}

impl<'a> DFParser<'a> {
    /// Parse a sql string into one or [`Statement`]s using DataFusion's
    /// dialect.
    pub fn parse_sql(sql: &'a str) -> Result<VecDeque<Statement<'a>>, DataFusionError> {
        DFParserBuilder::new(sql).build()?.parse_statements()
    }

    /// Parse a SQL string and produce one or more [`Statement`]s with
    /// with the specified dialect.
    pub fn parse_sql_with_dialect(
        sql: &'a str,
        dialect: SqlDialect,
    ) -> Result<VecDeque<Statement<'a>>, DataFusionError> {
        DFParserBuilder::new(sql)
            .with_dialect(dialect)
            .build()?
            .parse_statements()
    }

    pub fn parse_sql_into_expr(
        sql: &'a str,
    ) -> Result<ExprWithAlias<'a>, DataFusionError> {
        DFParserBuilder::new(sql).build()?.parse_into_expr()
    }

    pub fn parse_sql_into_expr_with_dialect(
        sql: &'a str,
        dialect: SqlDialect,
    ) -> Result<ExprWithAlias<'a>, DataFusionError> {
        DFParserBuilder::new(sql)
            .with_dialect(dialect)
            .build()?
            .parse_into_expr()
    }

    /// Parse a sql string into one or [`Statement`]s
    pub fn parse_statements(
        &mut self,
    ) -> Result<VecDeque<Statement<'a>>, DataFusionError> {
        dispatch!(self.dialect, parse_statements_as(self.sql))
            .map_err(|e| locate(e, self.sql))
    }

    /// Parse the SQL as exactly one statement.
    pub fn parse_statement(&mut self) -> Result<Statement<'a>, DataFusionError> {
        let mut statements = self.parse_statements()?;
        match (statements.pop_front(), statements.is_empty()) {
            (Some(statement), true) => Ok(statement),
            (None, _) => parser_err!("Expected a statement"),
            (Some(_), false) => parser_err!("Expected exactly one statement"),
        }
    }

    /// Parse the SQL as an expression, optionally followed by `AS alias`.
    pub fn parse_expr(&mut self) -> Result<ExprWithAlias<'a>, DataFusionError> {
        dispatch!(self.dialect, parse_expr_as(self.sql, false))
            .map_err(|e| locate(e, self.sql))
    }

    /// Parses the entire SQL string into an expression.
    ///
    /// In contrast to [`DFParser::parse_expr`], this function will report an error if the input
    /// contains any trailing, unparsed tokens.
    pub fn parse_into_expr(&mut self) -> Result<ExprWithAlias<'a>, DataFusionError> {
        dispatch!(self.dialect, parse_expr_as(self.sql, true))
            .map_err(|e| locate(e, self.sql))
    }

    /// The options this parser was built with.
    pub fn options(&self) -> &SqlParserOptions {
        &self.options
    }
}

/// Adds the line and column of a parse error in `sql` to its message.
fn locate(error: DataFusionError, sql: &str) -> DataFusionError {
    match error {
        DataFusionError::SQL(mut parse, backtrace) => {
            if parse.span != sqlparser::ast::Span::default() {
                let location = parse.span.start_location(sql);
                parse.message = format!(
                    "{} at Line: {}, Column: {}",
                    parse.message, location.line, location.column
                );
                parse.span = sqlparser::ast::Span::default();
            }
            DataFusionError::SQL(parse, backtrace)
        }
        DataFusionError::Diagnostic(diagnostic, inner) => {
            DataFusionError::Diagnostic(diagnostic, Box::new(locate(*inner, sql)))
        }
        other => other,
    }
}

/// Report an unexpected token
fn expected<T, D: Dialect>(p: &Parser<'_, D>, what: &str) -> Result<T, DataFusionError> {
    let error = p.expected(what);
    let span = Span::from_sqlparser_span(error.span, p.source());
    let diagnostic = Diagnostic::new_error(error.message.clone(), span);
    Err(DataFusionError::from(error).with_diagnostic(diagnostic))
}

fn parse_statements_as<'a, D: Dialect<Ext = Builtin>>(
    sql: &'a str,
) -> Result<VecDeque<Statement<'a>>, DataFusionError> {
    let mut p = Parser::<D>::new(sql)?;
    let mut statements = VecDeque::new();
    let mut expecting_statement_delimiter = false;
    loop {
        // ignore empty statements (between successive statement delimiters)
        while p.eat_op(";") {
            expecting_statement_delimiter = false;
        }
        if p.at_eof() {
            break;
        }
        if expecting_statement_delimiter {
            return expected(&p, "end of statement");
        }
        statements.push_back(parse_df_statement(&mut p)?);
        expecting_statement_delimiter = true;
    }
    Ok(statements)
}

fn parse_expr_as<'a, D: Dialect<Ext = Builtin>>(
    sql: &'a str,
    whole: bool,
) -> Result<ExprWithAlias<'a>, DataFusionError> {
    let mut p = Parser::<D>::new(sql)?;
    if p.at_keyword("CREATE") || p.at_keyword("COPY") || p.at_keyword("EXPLAIN") {
        return parser_err!("Unsupported command in expression")?;
    }
    let expr = p.parse_expr()?;
    let alias = if p.eat_keyword("AS") {
        Some(p.parse_ident()?)
    } else {
        None
    };
    if whole && !p.at_eof() {
        return expected(&p, "end of expression");
    }
    Ok(ExprWithAlias { expr, alias })
}

/// Parses a statement, including DataFusion's own.
fn parse_df_statement<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> Result<Statement<'a>, DataFusionError> {
    if p.at_keyword("CREATE") {
        let or_replace = p.peek_nth(1).is_word("OR") && p.peek_nth(2).is_word("REPLACE");
        let mut n = if or_replace { 3 } else { 1 };
        let unbounded = p.peek_nth(n).is_word("UNBOUNDED");
        if unbounded {
            n += 1;
        }
        if p.peek_nth(n).is_word("EXTERNAL") {
            for _ in 0..=n {
                p.advance();
            }
            return parse_create_external(p, unbounded, or_replace);
        }
        if !or_replace && p.peek_nth(1).is_word("CATALOG") {
            p.advance();
            p.advance();
            let if_not_exists = grammar::parse_if_not_exists(p)?;
            let name = grammar::parse_object_name(p)?;
            let create = sqlparser::ast::CreateDatabase {
                if_not_exists,
                name,
            };
            return Ok(Statement::Statement(Box::new(
                StatementKind::CreateDatabase(Box::new(create)).into(),
            )));
        }
    }
    if p.at_keyword("COPY") && !p.peek_nth(1).is_word("INTO") {
        p.advance();
        return parse_copy(p);
    }
    if p.eat_keyword("EXPLAIN") {
        return parse_explain(p);
    }
    if p.eat_keyword("RESET") {
        return parse_reset(p);
    }
    if p.at_keyword("DROP") && p.peek_nth(1).is_word("CATALOG") {
        p.advance();
        p.advance();
        return parse_drop_catalog(p);
    }
    Ok(Statement::Statement(Box::new(p.parse_statement()?)))
}

/// True at the end of a statement.
fn at_statement_end<D: Dialect>(p: &Parser<'_, D>) -> bool {
    p.at_eof() || p.at_op(";")
}

/// Parses a string literal's value.
fn parse_literal_string<D: Dialect>(
    p: &mut Parser<'_, D>,
) -> Result<String, DataFusionError> {
    Ok(grammar::parse_string(p)?.value().into_owned())
}

/// Parse a SQL `COPY TO` statement
fn parse_copy<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> Result<Statement<'a>, DataFusionError> {
    // parse as a query
    let source = if p.eat_op("(") {
        let query = D::parse_query(p)?;
        p.expect_op(")")?;
        CopyToSource::Query(Box::new(query))
    } else {
        // parse as table reference
        CopyToSource::Relation(grammar::parse_object_name(p)?)
    };

    #[derive(Default)]
    struct Builder {
        stored_as: Option<String>,
        target: Option<String>,
        partitioned_by: Option<Vec<String>>,
        options: Option<Vec<(String, OptionValue)>>,
    }

    let mut builder = Builder::default();

    loop {
        if p.eat_keyword("STORED") {
            p.expect_keyword("AS")?;
            ensure_not_set(builder.stored_as.as_ref(), "STORED AS")?;
            builder.stored_as = Some(parse_file_format(p)?);
        } else if p.eat_keyword("TO") {
            ensure_not_set(builder.target.as_ref(), "TO")?;
            builder.target = Some(parse_literal_string(p)?);
        } else if p.eat_keyword("WITH") {
            p.expect_keyword("HEADER")?;
            p.expect_keyword("ROW")?;
            return parser_err!(
                "WITH HEADER ROW clause is no longer in use. Please use the OPTIONS clause with 'format.has_header' set appropriately, e.g., OPTIONS ('format.has_header' 'true')"
            )?;
        } else if p.eat_keyword("PARTITIONED") {
            p.expect_keyword("BY")?;
            ensure_not_set(builder.partitioned_by.as_ref(), "PARTITIONED BY")?;
            builder.partitioned_by = Some(parse_partitions(p)?);
        } else if p.eat_keyword("OPTIONS") {
            ensure_not_set(builder.options.as_ref(), "OPTIONS")?;
            builder.options = Some(parse_value_options(p)?);
        } else if at_statement_end(p) {
            break;
        } else {
            return expected(p, "end of statement or ;");
        }
    }

    let Some(target) = builder.target else {
        return parser_err!("Missing TO clause in COPY statement")?;
    };

    Ok(Statement::CopyTo(CopyToStatement {
        source,
        target,
        partitioned_by: builder.partitioned_by.unwrap_or_default(),
        stored_as: builder.stored_as,
        options: builder.options.unwrap_or_default(),
    }))
}

/// The text of a word, or the value of a string or quoted name.
fn parse_word_or_string<D: Dialect>(
    p: &mut Parser<'_, D>,
    what: &str,
) -> Result<String, DataFusionError> {
    let token = p.peek();
    match token.kind {
        TokenKind::Word => {
            p.advance();
            Ok(token.text.to_string())
        }
        TokenKind::String => Ok(parse_literal_string(p)?),
        TokenKind::QuotedIdent => Ok(p.parse_ident()?.value().into_owned()),
        _ => expected(p, what),
    }
}

/// Parse the next token as a key name for an option list
///
/// This allows keywords, dotted names and strings.
fn parse_option_key<D: Dialect>(
    p: &mut Parser<'_, D>,
) -> Result<String, DataFusionError> {
    if p.peek().kind != TokenKind::Word {
        return parse_word_or_string(p, "key name");
    }
    let mut parts = vec![p.advance().text.to_string()];
    while p.eat_op(".") {
        if p.peek().kind != TokenKind::Word {
            // Unquoted namespaced keys have to conform to the syntax
            // "<WORD>[\.<WORD>]*". If we have a key that breaks this
            // pattern, error out:
            return expected(p, "key name");
        }
        parts.push(p.advance().text.to_string());
    }
    Ok(parts.join("."))
}

/// Parse the next token as a value for an option list
///
/// This allows any word or keyword in this location.
fn parse_option_value<D: Dialect>(
    p: &mut Parser<'_, D>,
) -> Result<OptionValue, DataFusionError> {
    let token = p.peek();
    match token.kind {
        // e.g. things like "snappy" or "gzip" that may be keywords
        TokenKind::Word => {
            p.advance();
            Ok(OptionValue::String(token.text.to_string()))
        }
        TokenKind::String => Ok(OptionValue::String(parse_literal_string(p)?)),
        TokenKind::QuotedIdent => Ok(OptionValue::DoubleQuotedString(
            p.parse_ident()?.value().into_owned(),
        )),
        TokenKind::Number => {
            p.advance();
            Ok(OptionValue::Number(token.text.to_string()))
        }
        _ => expected(p, "string or numeric value"),
    }
}

/// Parse a SQL `EXPLAIN`, after the keyword.
///
/// The parenthesized option list is read where the dialect has one, and a
/// parenthesized query is not mistaken for it.
fn parse_explain<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> Result<Statement<'a>, DataFusionError> {
    let raw = D::parse_explain_options(p)?;
    if !raw.is_empty() {
        let options = ExplainStatementOptions::from_utility_options(&raw)?;
        let statement = parse_df_statement(p)?;
        return Ok(Statement::Explain(ExplainStatement {
            statement: Box::new(statement),
            options,
        }));
    }

    // Legacy keyword form.
    let analyze = p.eat_keyword("ANALYZE");
    let verbose = p.eat_keyword("VERBOSE");
    let format = parse_explain_format(p)?
        .map(|s| ExplainFormat::from_str(&s))
        .transpose()?;

    let statement = parse_df_statement(p)?;

    let options = ExplainStatementOptions {
        analyze,
        verbose,
        format,
        ..Default::default()
    };

    Ok(Statement::Explain(ExplainStatement {
        statement: Box::new(statement),
        options,
    }))
}

/// Parse a SQL `RESET`, after the keyword.
fn parse_reset<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> Result<Statement<'a>, DataFusionError> {
    let mut parts: Vec<Ident<'a>> = Vec::new();
    let mut expecting_segment = true;

    loop {
        let token = p.peek();
        match token.kind {
            TokenKind::Word => {
                p.advance();
                parts.push(Ident {
                    text: Cow::Borrowed(token.text),
                    quote: None,
                    span: token.span,
                });
                expecting_segment = false;
            }
            TokenKind::String | TokenKind::QuotedIdent => {
                let value = parse_word_or_string(p, "configuration parameter")?;
                parts.push(Ident {
                    text: Cow::Owned(value),
                    quote: None,
                    span: token.span,
                });
                expecting_segment = false;
            }
            _ if token.is_op(".") => {
                if expecting_segment || parts.is_empty() {
                    return expected(p, "configuration parameter");
                }
                p.advance();
                expecting_segment = true;
            }
            _ if at_statement_end(p) => break,
            _ => return expected(p, "configuration parameter"),
        }
    }

    if parts.is_empty() || expecting_segment {
        return expected(p, "configuration parameter");
    }

    Ok(Statement::Reset(ResetStatement::Variable(ObjectName {
        parts,
    })))
}

fn parse_explain_format<D: Dialect>(
    p: &mut Parser<'_, D>,
) -> Result<Option<String>, DataFusionError> {
    if !p.eat_keyword("FORMAT") {
        return Ok(None);
    }
    parse_word_or_string(p, "an explain format such as TREE").map(Some)
}

/// Dispatches `CREATE [OR REPLACE] [UNBOUNDED] EXTERNAL ...` (with the
/// leading keywords already consumed) to either `CREATE EXTERNAL TABLE`
/// or `CREATE EXTERNAL CATALOG`, based on the keyword that follows
/// `EXTERNAL`.
fn parse_create_external<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
    unbounded: bool,
    or_replace: bool,
) -> Result<Statement<'a>, DataFusionError> {
    if p.eat_keyword("CATALOG") {
        if unbounded {
            return parser_err!("UNBOUNDED is not supported for CREATE EXTERNAL CATALOG");
        }
        parse_create_external_catalog(p, or_replace)
    } else {
        parse_create_external_table(p, unbounded, or_replace)
    }
}

fn parse_partitions<D: Dialect>(
    p: &mut Parser<'_, D>,
) -> Result<Vec<String>, DataFusionError> {
    let mut partitions: Vec<String> = vec![];
    if !p.eat_op("(") || p.eat_op(")") {
        return Ok(partitions);
    }

    loop {
        if matches!(p.peek().kind, TokenKind::Word | TokenKind::QuotedIdent) {
            let identifier = p.parse_ident()?;
            partitions.push(identifier.to_string());
        } else {
            return expected(p, "partition name");
        }
        let comma = p.eat_op(",");
        if p.eat_op(")") {
            // allow a trailing comma, even though it's not in standard
            break;
        } else if !comma {
            return expected(p, "',' or ')' after partition definition");
        }
    }
    Ok(partitions)
}

/// Parse the ordering clause of a `CREATE EXTERNAL TABLE` SQL statement
fn parse_order_by_exprs<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> Result<Vec<OrderByItem<'a>>, DataFusionError> {
    p.expect_op("(")?;
    let values = p.parse_comma_separated(D::parse_order_by_item)?;
    p.expect_op(")")?;
    Ok(values)
}

#[expect(clippy::type_complexity)]
fn parse_columns<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> Result<(Vec<ColumnDef<'a>>, Vec<TableConstraint<'a>>), DataFusionError> {
    let mut columns = vec![];
    let mut constraints = vec![];
    if !p.eat_op("(") || p.eat_op(")") {
        return Ok((columns, constraints));
    }

    loop {
        if let Some(constraint) = D::parse_table_constraint(p)? {
            constraints.push(constraint);
        } else if matches!(p.peek().kind, TokenKind::Word | TokenKind::QuotedIdent) {
            columns.push(D::parse_column_def(p)?);
        } else {
            return expected(p, "column name or constraint definition");
        }
        let comma = p.eat_op(",");
        if p.eat_op(")") {
            // allow a trailing comma, even though it's not in standard
            break;
        } else if !comma {
            return expected(p, "',' or ')' after column definition");
        }
    }

    Ok((columns, constraints))
}

fn parse_create_external_table<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
    unbounded: bool,
    or_replace: bool,
) -> Result<Statement<'a>, DataFusionError> {
    let temporary = p.eat_keyword("TEMP") || p.eat_keyword("TEMPORARY");

    p.expect_keyword("TABLE")?;
    let if_not_exists = grammar::parse_if_not_exists(p)?;

    if if_not_exists && or_replace {
        return parser_err!("'IF NOT EXISTS' cannot coexist with 'REPLACE'");
    }

    let table_name = grammar::parse_object_name(p)?;
    let (mut columns, constraints) = parse_columns(p)?;

    #[derive(Default)]
    struct Builder<'a> {
        file_type: Option<String>,
        locations: Option<Vec<String>>,
        table_partition_cols: Option<Vec<String>>,
        order_exprs: Vec<LexOrdering<'a>>,
        options: Option<Vec<(String, OptionValue)>>,
    }
    let mut builder = Builder::default();

    loop {
        if p.eat_keyword("STORED") {
            p.expect_keyword("AS")?;
            ensure_not_set(builder.file_type.as_ref(), "STORED AS")?;
            builder.file_type = Some(parse_file_format(p)?);
        } else if p.eat_keyword("LOCATION") {
            ensure_not_set(builder.locations.as_ref(), "LOCATION")?;
            builder.locations = Some(parse_locations(p)?);
        } else if p.eat_keyword("WITH") {
            if p.eat_keyword("ORDER") {
                builder.order_exprs.push(parse_order_by_exprs(p)?);
            } else {
                p.expect_keyword("HEADER")?;
                p.expect_keyword("ROW")?;
                return parser_err!(
                    "WITH HEADER ROW clause is no longer in use. Please use the OPTIONS clause with 'format.has_header' set appropriately, e.g., OPTIONS (format.has_header true)"
                )?;
            }
        } else if p.eat_keyword("DELIMITER") {
            return parser_err!(
                "DELIMITER clause is no longer in use. Please use the OPTIONS clause with 'format.delimiter' set appropriately, e.g., OPTIONS (format.delimiter ',')"
            )?;
        } else if p.eat_keyword("COMPRESSION") {
            p.expect_keyword("TYPE")?;
            return parser_err!(
                "COMPRESSION TYPE clause is no longer in use. Please use the OPTIONS clause with 'format.compression' set appropriately, e.g., OPTIONS (format.compression gzip)"
            )?;
        } else if p.eat_keyword("PARTITIONED") {
            p.expect_keyword("BY")?;
            ensure_not_set(builder.table_partition_cols.as_ref(), "PARTITIONED BY")?;
            // Expects either list of column names (col_name [, col_name]*)
            // or list of column definitions (col_name datatype [, col_name datatype]* )
            // use the token after the name to decide which parsing rule to use
            // Note that mixing both names and definitions is not allowed
            let peeked = p.peek_nth(2);
            if peeked.is_op(",") || peeked.is_op(")") {
                // List of column names
                builder.table_partition_cols = Some(parse_partitions(p)?)
            } else {
                // List of column defs
                let (cols, cons) = parse_columns(p)?;
                builder.table_partition_cols =
                    Some(cols.iter().map(|col| col.name.to_string()).collect());

                columns.extend(cols);

                if !cons.is_empty() {
                    return parser_err!(
                        "Constraints on Partition Columns are not supported"
                    );
                }
            }
        } else if p.eat_keyword("OPTIONS") {
            ensure_not_set(builder.options.as_ref(), "OPTIONS")?;
            builder.options = Some(parse_value_options(p)?);
        } else if at_statement_end(p) {
            break;
        } else {
            return expected(p, "end of statement or ;");
        }
    }

    // Validations: location and file_type are required
    let Some(file_type) = builder.file_type else {
        return parser_err!(
            "Missing STORED AS clause in CREATE EXTERNAL TABLE statement"
        );
    };
    let Some(locations) = builder.locations else {
        return parser_err!("Missing LOCATION clause in CREATE EXTERNAL TABLE statement");
    };
    if locations.is_empty() {
        return parser_err!("LOCATION requires at least one path");
    }

    Ok(Statement::CreateExternalTable(CreateExternalTable {
        name: table_name,
        columns,
        file_type,
        locations,
        table_partition_cols: builder.table_partition_cols.unwrap_or_default(),
        order_exprs: builder.order_exprs,
        if_not_exists,
        or_replace,
        temporary,
        unbounded,
        options: builder.options.unwrap_or_default(),
        constraints,
    }))
}

/// Parses a `CREATE EXTERNAL CATALOG` statement, with `CREATE [OR
/// REPLACE] EXTERNAL CATALOG` already consumed.
fn parse_create_external_catalog<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
    or_replace: bool,
) -> Result<Statement<'a>, DataFusionError> {
    let if_not_exists = grammar::parse_if_not_exists(p)?;

    if if_not_exists && or_replace {
        return parser_err!("'IF NOT EXISTS' cannot coexist with 'REPLACE'");
    }

    let catalog_name = grammar::parse_object_name(p)?;
    let mut catalog_type: Option<String> = None;
    let mut location: Option<String> = None;
    let mut options: Option<Vec<(String, OptionValue)>> = None;

    loop {
        if p.eat_keyword("STORED") {
            p.expect_keyword("AS")?;
            ensure_not_set(catalog_type.as_ref(), "STORED AS")?;
            catalog_type = Some(parse_file_format(p)?);
        } else if p.eat_keyword("LOCATION") {
            ensure_not_set(location.as_ref(), "LOCATION")?;
            location = Some(parse_literal_string(p)?);
        } else if p.eat_keyword("OPTIONS") {
            ensure_not_set(options.as_ref(), "OPTIONS")?;
            options = Some(parse_value_options(p)?);
        } else if at_statement_end(p) {
            break;
        } else {
            return expected(p, "end of statement or ;");
        }
    }

    let Some(catalog_type) = catalog_type else {
        return parser_err!(
            "Missing STORED AS clause in CREATE EXTERNAL CATALOG statement"
        );
    };

    Ok(Statement::CreateExternalCatalog(CreateExternalCatalog {
        catalog_name,
        catalog_type,
        location,
        if_not_exists,
        or_replace,
        options: options.unwrap_or_default(),
    }))
}

/// Parses a `DROP CATALOG` statement, with `DROP CATALOG` already consumed.
fn parse_drop_catalog<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> Result<Statement<'a>, DataFusionError> {
    let if_exists = p.at_keyword("IF") && p.peek_nth(1).is_word("EXISTS");
    if if_exists {
        p.advance();
        p.advance();
    }
    let name = grammar::parse_object_name(p)?;

    let cascade = p.eat_keyword("CASCADE");
    let restrict = p.eat_keyword("RESTRICT");
    if cascade && restrict {
        return parser_err!("Cannot specify both CASCADE and RESTRICT in DROP DATABASE");
    }

    let drop = DropObject {
        temporary: false,
        object_type: ObjectType::Database,
        if_exists,
        names: vec![name],
        on: None,
        behavior: if cascade {
            Some(DropBehavior::Cascade)
        } else if restrict {
            Some(DropBehavior::Restrict)
        } else {
            None
        },
        purge: false,
    };
    Ok(Statement::Statement(Box::new(
        StatementKind::Drop(Box::new(drop)).into(),
    )))
}

/// Parses one or more external table locations.
fn parse_locations<D: Dialect>(
    p: &mut Parser<'_, D>,
) -> Result<Vec<String>, DataFusionError> {
    if !p.eat_op("(") {
        return Ok(vec![parse_literal_string(p)?]);
    }

    let mut locations = vec![];
    loop {
        locations.push(parse_literal_string(p)?);
        let comma = p.eat_op(",");
        if p.eat_op(")") {
            // Allow a trailing comma, even though it's not in standard
            break;
        } else if !comma {
            return expected(p, "',' or ')' after location definition");
        }
    }
    Ok(locations)
}

/// Parses the set of valid formats
fn parse_file_format<D: Dialect>(
    p: &mut Parser<'_, D>,
) -> Result<String, DataFusionError> {
    if p.peek().kind != TokenKind::Word {
        return expected(p, "one of ARROW, PARQUET, NDJSON, or CSV");
    }
    parse_file_type(p.advance().text)
}

/// Parses (key value) style options into a map of String --> [`OptionValue`].
///
/// This method supports keywords as key names as well as multiple
/// value types such as Numbers as well as Strings.
fn parse_value_options<D: Dialect>(
    p: &mut Parser<'_, D>,
) -> Result<Vec<(String, OptionValue)>, DataFusionError> {
    let mut options = vec![];
    p.expect_op("(")?;

    loop {
        let key = parse_option_key(p)?;
        let value = parse_option_value(p)?;
        options.push((key, value));
        let comma = p.eat_op(",");
        if p.eat_op(")") {
            // Allow a trailing comma, even though it's not in standard
            break;
        } else if !comma {
            return expected(p, "',' or ')' after option definition");
        }
    }
    Ok(options)
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion_common::assert_contains;
    use sqlparser::ast::{
        ArrayType, BinaryOp, DataType, DataTypeSuffix, ExprKind, Literal, Span,
    };

    use crate::ast::Expr;

    fn expect_parse_ok(
        sql: &'static str,
        expected: Statement<'static>,
    ) -> Result<(), DataFusionError> {
        let statements = DFParser::parse_sql(sql)?;
        assert_eq!(
            statements.len(),
            1,
            "Expected to parse exactly one statement"
        );
        assert_eq!(statements[0], expected, "actual:\n{:#?}", statements[0]);
        Ok(())
    }

    /// Parses sql and asserts that the expected error message was found
    fn expect_parse_error(sql: &str, expected_error: &str) {
        match DFParser::parse_sql(sql) {
            Ok(statements) => {
                panic!(
                    "Expected parse error for '{sql}', but was successful: {statements:?}"
                );
            }
            Err(e) => {
                let error_message = e.to_string();
                assert!(
                    error_message.contains(expected_error),
                    "Expected error '{expected_error}' not found in actual error '{error_message}'"
                );
            }
        }
    }

    /// An unquoted identifier.
    fn ident(name: &str) -> Ident<'static> {
        Ident {
            text: Cow::Owned(name.to_string()),
            quote: None,
            span: Span::default(),
        }
    }

    /// A column reference.
    fn column(name: &str) -> Expr<'static> {
        ExprKind::Ident(ident(name)).into()
    }

    /// A type with no arguments, such as `int`, as written.
    fn data_type(name: &str) -> DataType<'static> {
        DataType {
            qualifier: vec![],
            name: ident(name),
            angle: None,
            args: vec![],
            suffix: DataTypeSuffix::None,
            array: ArrayType::None,
        }
    }

    fn make_column_def(name: &str, data_type: DataType<'static>) -> ColumnDef<'static> {
        ColumnDef {
            name: ident(name),
            data_type,
            options: vec![],
        }
    }

    fn order_by(
        expr: Expr<'static>,
        asc: Option<bool>,
        nulls_first: Option<bool>,
    ) -> OrderByItem<'static> {
        OrderByItem {
            expr,
            asc,
            nulls_first,
            using: None,
            ext: Default::default(),
        }
    }

    /// `c1 - c2`
    fn c1_minus_c2() -> Expr<'static> {
        ExprKind::Binary {
            left: Box::new(column("c1")),
            op: BinaryOp::Sub,
            right: Box::new(column("c2")),
        }
        .into()
    }

    fn string(value: &str) -> OptionValue {
        OptionValue::String(value.to_string())
    }

    fn make_create_external_table(location: &str) -> CreateExternalTable<'static> {
        make_create_external_table_with_locations(&[location])
    }

    fn make_create_external_table_with_locations(
        locations: &[&str],
    ) -> CreateExternalTable<'static> {
        let locations = locations
            .iter()
            .map(|location| location.to_string())
            .collect::<Vec<_>>();

        CreateExternalTable {
            name: ObjectName {
                parts: vec![ident("t")],
            },
            columns: vec![],
            file_type: "CSV".to_string(),
            locations,
            table_partition_cols: vec![],
            order_exprs: vec![],
            if_not_exists: false,
            or_replace: false,
            temporary: false,
            unbounded: false,
            options: vec![],
            constraints: vec![],
        }
    }

    #[test]
    fn create_external_table() -> Result<(), DataFusionError> {
        // positive case
        let sql = "CREATE EXTERNAL TABLE t(c1 int) STORED AS CSV LOCATION 'foo.csv'";
        let expected = Statement::CreateExternalTable(CreateExternalTable {
            columns: vec![make_column_def("c1", data_type("int"))],
            ..make_create_external_table("foo.csv")
        });
        expect_parse_ok(sql, expected)?;

        // positive case: literal comma remains part of a single path
        let sql = "CREATE EXTERNAL TABLE t(c1 int) STORED AS CSV LOCATION 'foo,bar.csv'";
        let expected = Statement::CreateExternalTable(CreateExternalTable {
            columns: vec![make_column_def("c1", data_type("int"))],
            ..make_create_external_table("foo,bar.csv")
        });
        expect_parse_ok(sql, expected)?;

        // positive case: multiple locations use an explicit list
        let sql = "CREATE EXTERNAL TABLE t(c1 int) STORED AS CSV LOCATION ('foo.csv', 'bar.csv')";
        let expected = Statement::CreateExternalTable(CreateExternalTable {
            columns: vec![make_column_def("c1", data_type("int"))],
            ..make_create_external_table_with_locations(&["foo.csv", "bar.csv"])
        });
        expect_parse_ok(sql, expected)?;

        assert_eq!(
            Statement::CreateExternalTable(make_create_external_table("foo.csv"))
                .to_string(),
            "CREATE EXTERNAL TABLE t STORED AS CSV LOCATION 'foo.csv'"
        );
        assert_eq!(
            Statement::CreateExternalTable(make_create_external_table_with_locations(&[
                "foo.csv", "bar.csv"
            ]))
            .to_string(),
            "CREATE EXTERNAL TABLE t STORED AS CSV LOCATION ('foo.csv', 'bar.csv')"
        );
        assert_eq!(
            Statement::CreateExternalTable(make_create_external_table("foo'bar.csv"))
                .to_string(),
            "CREATE EXTERNAL TABLE t STORED AS CSV LOCATION 'foo''bar.csv'"
        );

        // positive case: leading space
        let sql = "CREATE EXTERNAL TABLE t(c1 int) STORED AS CSV LOCATION 'foo.csv'     ";
        let expected = Statement::CreateExternalTable(CreateExternalTable {
            columns: vec![make_column_def("c1", data_type("int"))],
            ..make_create_external_table("foo.csv")
        });
        expect_parse_ok(sql, expected)?;

        // positive case: leading space + semicolon
        let sql =
            "CREATE EXTERNAL TABLE t(c1 int) STORED AS CSV LOCATION 'foo.csv'      ;";
        let expected = Statement::CreateExternalTable(CreateExternalTable {
            columns: vec![make_column_def("c1", data_type("int"))],
            ..make_create_external_table("foo.csv")
        });
        expect_parse_ok(sql, expected)?;

        // positive case with delimiter
        let sql = "CREATE EXTERNAL TABLE t(c1 int) STORED AS CSV LOCATION 'foo.csv' OPTIONS (format.delimiter '|')";
        let expected = Statement::CreateExternalTable(CreateExternalTable {
            columns: vec![make_column_def("c1", data_type("int"))],
            options: vec![("format.delimiter".into(), string("|"))],
            ..make_create_external_table("foo.csv")
        });
        expect_parse_ok(sql, expected)?;

        // positive case: partitioned by
        let sql = "CREATE EXTERNAL TABLE t(c1 int) STORED AS CSV PARTITIONED BY (p1, p2) LOCATION 'foo.csv'";
        let expected = Statement::CreateExternalTable(CreateExternalTable {
            columns: vec![make_column_def("c1", data_type("int"))],
            table_partition_cols: vec!["p1".to_string(), "p2".to_string()],
            ..make_create_external_table("foo.csv")
        });
        expect_parse_ok(sql, expected)?;

        // positive case: it is ok for sql stmt with `COMPRESSION TYPE GZIP` tokens
        let sqls =
            vec![
             ("CREATE EXTERNAL TABLE t(c1 int) STORED AS CSV LOCATION 'foo.csv' OPTIONS
             ('format.compression' 'GZIP')", "GZIP"),
             ("CREATE EXTERNAL TABLE t(c1 int) STORED AS CSV LOCATION 'foo.csv' OPTIONS
             ('format.compression' 'BZIP2')", "BZIP2"),
             ("CREATE EXTERNAL TABLE t(c1 int) STORED AS CSV LOCATION 'foo.csv' OPTIONS
             ('format.compression' 'XZ')", "XZ"),
             ("CREATE EXTERNAL TABLE t(c1 int) STORED AS CSV LOCATION 'foo.csv' OPTIONS
             ('format.compression' 'ZSTD')", "ZSTD"),
        ];
        for (sql, compression) in sqls {
            let expected = Statement::CreateExternalTable(CreateExternalTable {
                columns: vec![make_column_def("c1", data_type("int"))],
                options: vec![("format.compression".into(), string(compression))],
                ..make_create_external_table("foo.csv")
            });
            expect_parse_ok(sql, expected)?;
        }

        // positive case: it is ok for parquet files not to have columns specified
        let sql = "CREATE EXTERNAL TABLE t STORED AS PARQUET LOCATION 'foo.parquet'";
        let expected = Statement::CreateExternalTable(CreateExternalTable {
            file_type: "PARQUET".to_string(),
            ..make_create_external_table("foo.parquet")
        });
        expect_parse_ok(sql, expected)?;

        // positive case: it is ok for parquet files to be other than upper case
        let sql = "CREATE EXTERNAL TABLE t STORED AS parqueT LOCATION 'foo.parquet'";
        let expected = Statement::CreateExternalTable(CreateExternalTable {
            file_type: "PARQUET".to_string(),
            ..make_create_external_table("foo.parquet")
        });
        expect_parse_ok(sql, expected)?;

        // positive case: it is ok for avro files not to have columns specified
        let sql = "CREATE EXTERNAL TABLE t STORED AS AVRO LOCATION 'foo.avro'";
        let expected = Statement::CreateExternalTable(CreateExternalTable {
            file_type: "AVRO".to_string(),
            ..make_create_external_table("foo.avro")
        });
        expect_parse_ok(sql, expected)?;

        // positive case: it is ok for avro files not to have columns specified
        let sql = "CREATE EXTERNAL TABLE IF NOT EXISTS t STORED AS PARQUET LOCATION 'foo.parquet'";
        let expected = Statement::CreateExternalTable(CreateExternalTable {
            file_type: "PARQUET".to_string(),
            if_not_exists: true,
            ..make_create_external_table("foo.parquet")
        });
        expect_parse_ok(sql, expected)?;

        // positive case: or replace
        let sql =
            "CREATE OR REPLACE EXTERNAL TABLE t STORED AS PARQUET LOCATION 'foo.parquet'";
        let expected = Statement::CreateExternalTable(CreateExternalTable {
            file_type: "PARQUET".to_string(),
            or_replace: true,
            ..make_create_external_table("foo.parquet")
        });
        expect_parse_ok(sql, expected)?;

        // positive case: column definition allowed in 'partition by' clause
        let sql = "CREATE EXTERNAL TABLE t(c1 int) STORED AS CSV PARTITIONED BY (p1 int) LOCATION 'foo.csv'";
        let expected = Statement::CreateExternalTable(CreateExternalTable {
            columns: vec![
                make_column_def("c1", data_type("int")),
                make_column_def("p1", data_type("int")),
            ],
            table_partition_cols: vec!["p1".to_string()],
            ..make_create_external_table("foo.csv")
        });
        expect_parse_ok(sql, expected)?;

        // negative case: mixed column defs and column names in `PARTITIONED BY` clause
        let sql = "CREATE EXTERNAL TABLE t(c1 int) STORED AS CSV PARTITIONED BY (p1 int, c1) LOCATION 'foo.csv'";
        expect_parse_error(
            sql,
            "SQL error: expected identifier, found ) at Line: 1, Column: 73",
        );

        // negative case: mixed column defs and column names in `PARTITIONED BY` clause
        let sql = "CREATE EXTERNAL TABLE t(c1 int) STORED AS CSV PARTITIONED BY (c1, p1 int) LOCATION 'foo.csv'";
        expect_parse_error(
            sql,
            "SQL error: expected ',' or ')' after partition definition, found int at Line: 1, Column: 70",
        );

        // positive case: additional options (one entry) can be specified
        let sql =
            "CREATE EXTERNAL TABLE t STORED AS x OPTIONS ('k1' 'v1') LOCATION 'blahblah'";
        let expected = Statement::CreateExternalTable(CreateExternalTable {
            file_type: "X".to_string(),
            options: vec![("k1".into(), string("v1"))],
            ..make_create_external_table("blahblah")
        });
        expect_parse_ok(sql, expected)?;

        // positive case: additional options (multiple entries) can be specified
        let sql = "CREATE EXTERNAL TABLE t STORED AS x OPTIONS ('k1' 'v1', k2 v2) LOCATION 'blahblah'";
        let expected = Statement::CreateExternalTable(CreateExternalTable {
            file_type: "X".to_string(),
            options: vec![("k1".into(), string("v1")), ("k2".into(), string("v2"))],
            ..make_create_external_table("blahblah")
        });
        expect_parse_ok(sql, expected)?;

        // Ordered Col
        let sqls = [
            "CREATE EXTERNAL TABLE t(c1 int) STORED AS CSV WITH ORDER (c1) LOCATION 'foo.csv'",
            "CREATE EXTERNAL TABLE t(c1 int) STORED AS CSV WITH ORDER (c1 NULLS FIRST) LOCATION 'foo.csv'",
            "CREATE EXTERNAL TABLE t(c1 int) STORED AS CSV WITH ORDER (c1 NULLS LAST) LOCATION 'foo.csv'",
            "CREATE EXTERNAL TABLE t(c1 int) STORED AS CSV WITH ORDER (c1 ASC) LOCATION 'foo.csv'",
            "CREATE EXTERNAL TABLE t(c1 int) STORED AS CSV WITH ORDER (c1 DESC) LOCATION 'foo.csv'",
            "CREATE EXTERNAL TABLE t(c1 int) STORED AS CSV WITH ORDER (c1 DESC NULLS FIRST) LOCATION 'foo.csv'",
            "CREATE EXTERNAL TABLE t(c1 int) STORED AS CSV WITH ORDER (c1 DESC NULLS LAST) LOCATION 'foo.csv'",
            "CREATE EXTERNAL TABLE t(c1 int) STORED AS CSV WITH ORDER (c1 ASC NULLS FIRST) LOCATION 'foo.csv'",
            "CREATE EXTERNAL TABLE t(c1 int) STORED AS CSV WITH ORDER (c1 ASC NULLS LAST) LOCATION 'foo.csv'",
        ];
        let expected = vec![
            (None, None),
            (None, Some(true)),
            (None, Some(false)),
            (Some(true), None),
            (Some(false), None),
            (Some(false), Some(true)),
            (Some(false), Some(false)),
            (Some(true), Some(true)),
            (Some(true), Some(false)),
        ];
        for (sql, (asc, nulls_first)) in sqls.iter().zip(expected) {
            let expected = Statement::CreateExternalTable(CreateExternalTable {
                columns: vec![make_column_def("c1", data_type("int"))],
                order_exprs: vec![vec![order_by(column("c1"), asc, nulls_first)]],
                ..make_create_external_table("foo.csv")
            });
            expect_parse_ok(sql, expected)?;
        }

        // Ordered Col
        let sql = "CREATE EXTERNAL TABLE t(c1 int, c2 int) STORED AS CSV WITH ORDER (c1 ASC, c2 DESC NULLS FIRST) LOCATION 'foo.csv'";
        let expected = Statement::CreateExternalTable(CreateExternalTable {
            columns: vec![
                make_column_def("c1", data_type("int")),
                make_column_def("c2", data_type("int")),
            ],
            order_exprs: vec![vec![
                order_by(column("c1"), Some(true), None),
                order_by(column("c2"), Some(false), Some(true)),
            ]],
            ..make_create_external_table("foo.csv")
        });
        expect_parse_ok(sql, expected)?;

        // Ordered Binary op
        let sql = "CREATE EXTERNAL TABLE t(c1 int, c2 int) STORED AS CSV WITH ORDER (c1 - c2 ASC) LOCATION 'foo.csv'";
        let expected = Statement::CreateExternalTable(CreateExternalTable {
            columns: vec![
                make_column_def("c1", data_type("int")),
                make_column_def("c2", data_type("int")),
            ],
            order_exprs: vec![vec![order_by(c1_minus_c2(), Some(true), None)]],
            ..make_create_external_table("foo.csv")
        });
        expect_parse_ok(sql, expected)?;

        // Most complete CREATE EXTERNAL TABLE statement possible (using IF NOT EXISTS)
        let sql = "
            CREATE UNBOUNDED EXTERNAL TABLE IF NOT EXISTS t (c1 int, c2 float)
            STORED AS PARQUET
            WITH ORDER (c1 - c2 ASC)
            PARTITIONED BY (c1)
            LOCATION 'foo.parquet'
            OPTIONS ('format.compression' 'zstd',
                     'format.delimiter' '*',
                     'ROW_GROUP_SIZE' '1024',
                     'TRUNCATE' 'NO',
                     'format.has_header' 'true')";
        let expected = Statement::CreateExternalTable(CreateExternalTable {
            columns: vec![
                make_column_def("c1", data_type("int")),
                make_column_def("c2", data_type("float")),
            ],
            file_type: "PARQUET".to_string(),
            table_partition_cols: vec!["c1".into()],
            order_exprs: vec![vec![order_by(c1_minus_c2(), Some(true), None)]],
            if_not_exists: true,
            unbounded: true,
            options: vec![
                ("format.compression".into(), string("zstd")),
                ("format.delimiter".into(), string("*")),
                ("ROW_GROUP_SIZE".into(), string("1024")),
                ("TRUNCATE".into(), string("NO")),
                ("format.has_header".into(), string("true")),
            ],
            ..make_create_external_table("foo.parquet")
        });
        expect_parse_ok(sql, expected)?;

        // Most complete CREATE EXTERNAL TABLE statement possible (using OR REPLACE)
        let sql = "
            CREATE OR REPLACE UNBOUNDED EXTERNAL TABLE t (c1 int, c2 float)
            STORED AS PARQUET
            WITH ORDER (c1 - c2 ASC)
            PARTITIONED BY (c1)
            LOCATION 'foo.parquet'
            OPTIONS ('format.compression' 'zstd',
                     'format.delimiter' '*',
                     'ROW_GROUP_SIZE' '1024',
                     'TRUNCATE' 'NO',
                     'format.has_header' 'true')";
        let expected = Statement::CreateExternalTable(CreateExternalTable {
            columns: vec![
                make_column_def("c1", data_type("int")),
                make_column_def("c2", data_type("float")),
            ],
            file_type: "PARQUET".to_string(),
            table_partition_cols: vec!["c1".into()],
            order_exprs: vec![vec![order_by(c1_minus_c2(), Some(true), None)]],
            or_replace: true,
            unbounded: true,
            options: vec![
                ("format.compression".into(), string("zstd")),
                ("format.delimiter".into(), string("*")),
                ("ROW_GROUP_SIZE".into(), string("1024")),
                ("TRUNCATE".into(), string("NO")),
                ("format.has_header".into(), string("true")),
            ],
            ..make_create_external_table("foo.parquet")
        });
        expect_parse_ok(sql, expected)?;

        // For error cases, see: `create_external_table.slt`

        Ok(())
    }

    fn make_create_external_catalog(
        catalog_type: &str,
    ) -> CreateExternalCatalog<'static> {
        CreateExternalCatalog {
            catalog_name: ObjectName {
                parts: vec![ident("c")],
            },
            catalog_type: catalog_type.to_string(),
            location: None,
            if_not_exists: false,
            or_replace: false,
            options: vec![],
        }
    }

    #[test]
    fn create_external_catalog() -> Result<(), DataFusionError> {
        // minimal: just STORED AS
        let sql = "CREATE EXTERNAL CATALOG c STORED AS ICEBERG";
        let expected = Statement::CreateExternalCatalog(CreateExternalCatalog {
            catalog_type: "ICEBERG".to_string(),
            ..make_create_external_catalog("ICEBERG")
        });
        expect_parse_ok(sql, expected)?;

        // with LOCATION
        let sql = "CREATE EXTERNAL CATALOG c STORED AS ICEBERG LOCATION 's3://bucket/warehouse'";
        let expected = Statement::CreateExternalCatalog(CreateExternalCatalog {
            location: Some("s3://bucket/warehouse".to_string()),
            ..make_create_external_catalog("ICEBERG")
        });
        expect_parse_ok(sql, expected)?;

        // with OPTIONS
        let sql = "CREATE EXTERNAL CATALOG c STORED AS ICEBERG OPTIONS ('catalog.uri' 'http://rest:8181', 'warehouse' 'c')";
        let expected = Statement::CreateExternalCatalog(CreateExternalCatalog {
            options: vec![
                ("catalog.uri".into(), string("http://rest:8181")),
                ("warehouse".into(), string("c")),
            ],
            ..make_create_external_catalog("ICEBERG")
        });
        expect_parse_ok(sql, expected)?;

        // IF NOT EXISTS
        let sql = "CREATE EXTERNAL CATALOG IF NOT EXISTS c STORED AS ICEBERG";
        let expected = Statement::CreateExternalCatalog(CreateExternalCatalog {
            if_not_exists: true,
            ..make_create_external_catalog("ICEBERG")
        });
        expect_parse_ok(sql, expected)?;

        // OR REPLACE
        let sql = "CREATE OR REPLACE EXTERNAL CATALOG c STORED AS ICEBERG";
        let expected = Statement::CreateExternalCatalog(CreateExternalCatalog {
            or_replace: true,
            ..make_create_external_catalog("ICEBERG")
        });
        expect_parse_ok(sql, expected)?;

        // IF NOT EXISTS and OR REPLACE cannot coexist
        expect_parse_error(
            "CREATE OR REPLACE EXTERNAL CATALOG IF NOT EXISTS c STORED AS ICEBERG",
            "'IF NOT EXISTS' cannot coexist with 'REPLACE'",
        );

        // missing STORED AS
        expect_parse_error(
            "CREATE EXTERNAL CATALOG c",
            "Missing STORED AS clause in CREATE EXTERNAL CATALOG statement",
        );

        // UNBOUNDED is not applicable to catalogs
        expect_parse_error(
            "CREATE UNBOUNDED EXTERNAL CATALOG c STORED AS ICEBERG",
            "UNBOUNDED is not supported for CREATE EXTERNAL CATALOG",
        );

        Ok(())
    }

    fn drop_catalog_statement(if_exists: bool, cascade: bool) -> Statement<'static> {
        let drop = DropObject {
            temporary: false,
            object_type: ObjectType::Database,
            if_exists,
            names: vec![ObjectName {
                parts: vec![ident("c")],
            }],
            on: None,
            behavior: cascade.then_some(DropBehavior::Cascade),
            purge: false,
        };
        Statement::Statement(Box::new(StatementKind::Drop(Box::new(drop)).into()))
    }

    #[test]
    fn drop_catalog() -> Result<(), DataFusionError> {
        let sql = "DROP CATALOG c";
        let expected = drop_catalog_statement(false, false);
        expect_parse_ok(sql, expected)?;

        let sql = "DROP CATALOG IF EXISTS c";
        let expected = drop_catalog_statement(true, false);
        expect_parse_ok(sql, expected)?;

        let sql = "DROP CATALOG c CASCADE";
        let expected = drop_catalog_statement(false, true);
        expect_parse_ok(sql, expected)?;

        let sql = "DROP CATALOG IF EXISTS c CASCADE";
        let expected = drop_catalog_statement(true, true);
        expect_parse_ok(sql, expected)?;

        Ok(())
    }

    #[test]
    fn copy_to_table_to_table() -> Result<(), DataFusionError> {
        // positive case
        let sql = "COPY foo TO bar STORED AS CSV";
        let expected = Statement::CopyTo(CopyToStatement {
            source: object_name("foo"),
            target: "bar".to_string(),
            partitioned_by: vec![],
            stored_as: Some("CSV".to_owned()),
            options: vec![],
        });

        assert_eq!(verified_stmt(sql), expected);
        Ok(())
    }

    #[test]
    fn skip_copy_into_snowflake() -> Result<(), DataFusionError> {
        let sql = "COPY INTO foo FROM @~/staged FILE_FORMAT = (FORMAT_NAME = 'mycsv');";
        let statements = DFParser::parse_sql_with_dialect(
            sql,
            SqlDialect::Builtin(DialectKind::Snowflake),
        )?;

        assert_eq!(
            statements.len(),
            1,
            "Expected to parse exactly one statement"
        );
        if let Statement::CopyTo(_) = &statements[0] {
            panic!("Expected non COPY TO statement, but was successful: {statements:?}");
        }
        Ok(())
    }

    #[test]
    fn explain_copy_to_table_to_table() -> Result<(), DataFusionError> {
        let cases = vec![
            ("EXPLAIN COPY foo TO bar STORED AS PARQUET", false, false),
            (
                "EXPLAIN ANALYZE COPY foo TO bar STORED AS PARQUET",
                true,
                false,
            ),
            (
                "EXPLAIN VERBOSE COPY foo TO bar STORED AS PARQUET",
                false,
                true,
            ),
            (
                "EXPLAIN ANALYZE VERBOSE COPY foo TO bar STORED AS PARQUET",
                true,
                true,
            ),
        ];
        for (sql, analyze, verbose) in cases {
            println!("sql: {sql}, analyze: {analyze}, verbose: {verbose}");

            let expected_copy = Statement::CopyTo(CopyToStatement {
                source: object_name("foo"),
                target: "bar".to_string(),
                partitioned_by: vec![],
                stored_as: Some("PARQUET".to_owned()),
                options: vec![],
            });
            let expected = Statement::Explain(ExplainStatement {
                options: ExplainStatementOptions {
                    analyze,
                    verbose,
                    format: None,
                    analyze_level: None,
                    analyze_categories: None,
                    show_statistics: None,
                },
                statement: Box::new(expected_copy),
            });
            assert_eq!(verified_stmt(sql), expected);
        }
        Ok(())
    }

    #[test]
    fn copy_to_query_to_table() -> Result<(), DataFusionError> {
        let statement = verified_stmt("SELECT 1");

        // unwrap the various layers
        let statement = if let Statement::Statement(statement) = statement {
            *statement
        } else {
            panic!("Expected statement, got {statement:?}");
        };

        let StatementKind::Query(query) = statement.kind else {
            panic!("Expected query, got {statement:?}");
        };

        let sql =
            "COPY (SELECT 1) TO bar STORED AS CSV OPTIONS ('format.has_header' 'true')";
        let expected = Statement::CopyTo(CopyToStatement {
            source: CopyToSource::Query(query),
            target: "bar".to_string(),
            partitioned_by: vec![],
            stored_as: Some("CSV".to_owned()),
            options: vec![("format.has_header".into(), string("true"))],
        });
        assert_eq!(verified_stmt(sql), expected);
        Ok(())
    }

    #[test]
    fn copy_to_options() -> Result<(), DataFusionError> {
        let sql = "COPY foo TO bar STORED AS CSV OPTIONS ('row_group_size' '55')";
        let expected = Statement::CopyTo(CopyToStatement {
            source: object_name("foo"),
            target: "bar".to_string(),
            partitioned_by: vec![],
            stored_as: Some("CSV".to_owned()),
            options: vec![("row_group_size".to_string(), string("55"))],
        });
        assert_eq!(verified_stmt(sql), expected);
        Ok(())
    }

    #[test]
    fn copy_to_partitioned_by() -> Result<(), DataFusionError> {
        let sql = "COPY foo TO bar STORED AS CSV PARTITIONED BY (a) OPTIONS ('row_group_size' '55')";
        let expected = Statement::CopyTo(CopyToStatement {
            source: object_name("foo"),
            target: "bar".to_string(),
            partitioned_by: vec!["a".to_string()],
            stored_as: Some("CSV".to_owned()),
            options: vec![("row_group_size".to_string(), string("55"))],
        });
        assert_eq!(verified_stmt(sql), expected);
        Ok(())
    }

    #[test]
    fn copy_to_multi_options() -> Result<(), DataFusionError> {
        // order of options is preserved
        let sql = "COPY foo TO bar STORED AS parquet OPTIONS ('format.row_group_size' 55, 'format.compression' snappy, 'execution.keep_partition_by_columns' true)";

        let expected_options = vec![
            (
                "format.row_group_size".to_string(),
                OptionValue::Number("55".to_string()),
            ),
            ("format.compression".to_string(), string("snappy")),
            (
                "execution.keep_partition_by_columns".to_string(),
                string("true"),
            ),
        ];

        let mut statements = DFParser::parse_sql(sql).unwrap();
        assert_eq!(statements.len(), 1);
        let only_statement = statements.pop_front().unwrap();

        let options = if let Statement::CopyTo(copy_to) = only_statement {
            copy_to.options
        } else {
            panic!("Expected copy");
        };

        assert_eq!(options, expected_options);

        Ok(())
    }

    // For error cases, see: `copy.slt`

    fn object_name(name: &str) -> CopyToSource<'static> {
        CopyToSource::Relation(ObjectName {
            parts: vec![ident(name)],
        })
    }

    // Based on  sqlparser-rs
    // https://github.com/sqlparser-rs/sqlparser-rs/blob/ae3b5844c839072c235965fe0d1bddc473dced87/src/test_utils.rs#L104-L116

    /// Ensures that `sql` parses as a single [Statement]
    ///
    /// If `canonical` is non empty,this function additionally asserts
    /// that:
    ///
    /// 1. parsing `sql` results in the same [`Statement`] as parsing
    ///    `canonical`.
    ///
    /// 2. re-serializing the result of parsing `sql` produces the same
    ///    `canonical` sql string
    fn one_statement_parses_to<'a>(sql: &'a str, canonical: &'a str) -> Statement<'a> {
        let mut statements = DFParser::parse_sql(sql).unwrap();
        assert_eq!(statements.len(), 1);

        if sql != canonical {
            assert_eq!(DFParser::parse_sql(canonical).unwrap(), statements);
        }

        let only_statement = statements.pop_front().unwrap();
        assert_eq!(
            canonical.to_uppercase(),
            only_statement.to_string().to_uppercase()
        );
        only_statement
    }

    /// Ensures that `sql` parses as a single [Statement], and that
    /// re-serializing the parse result produces the same `sql`
    /// string (is not modified after a serialization round-trip).
    fn verified_stmt(sql: &str) -> Statement<'_> {
        one_statement_parses_to(sql, sql)
    }

    #[test]
    /// Checks the recursion limit works for sql queries
    /// Recursion can happen easily with binary exprs (i.e, AND or OR)
    fn test_recursion_limit() {
        let sql = "SELECT 1 OR 2";

        // Expect parse to succeed
        DFParserBuilder::new(sql)
            .build()
            .unwrap()
            .parse_statements()
            .unwrap();

        let err = DFParserBuilder::new(sql)
            .with_recursion_limit(1)
            .build()
            .unwrap()
            .parse_statements()
            .unwrap_err();

        assert_contains!(
            err.to_string(),
            "SQL error: RecursionLimitExceeded (current limit: 1)"
        );
    }

    #[test]
    fn test_multistatement() {
        let sql = "COPY foo TO bar STORED AS CSV; \
             CREATE EXTERNAL TABLE t(c1 int) STORED AS CSV LOCATION 'foo.csv'; \
             RESET var;";
        let statements = DFParser::parse_sql(sql).unwrap();
        assert_eq!(
            statements,
            vec![
                Statement::CopyTo(CopyToStatement {
                    source: object_name("foo"),
                    target: "bar".to_string(),
                    partitioned_by: vec![],
                    stored_as: Some("CSV".to_owned()),
                    options: vec![],
                }),
                {
                    Statement::CreateExternalTable(CreateExternalTable {
                        columns: vec![make_column_def("c1", data_type("int"))],
                        ..make_create_external_table("foo.csv")
                    })
                },
                {
                    let name = ObjectName {
                        parts: vec![ident("var")],
                    };
                    Statement::Reset(ResetStatement::Variable(name))
                }
            ]
        );
    }

    fn expect_parse_expr_ok(sql: &'static str, expected: ExprWithAlias<'static>) {
        let expr = DFParser::parse_sql_into_expr(sql).unwrap();
        assert_eq!(expr, expected, "actual:\n{expr:#?}");
    }

    /// Parses sql and asserts that the expected error message was found
    fn expect_parse_expr_error(sql: &str, expected_error: &str) {
        match DFParser::parse_sql_into_expr(sql) {
            Ok(expr) => {
                panic!("Expected parse error for '{sql}', but was successful: {expr:#?}");
            }
            Err(e) => {
                let error_message = e.to_string();
                assert!(
                    error_message.contains(expected_error),
                    "Expected error '{expected_error}' not found in actual error '{error_message}'"
                );
            }
        }
    }

    #[test]
    fn literal() {
        expect_parse_expr_ok(
            "1234",
            ExprWithAlias {
                expr: ExprKind::Literal(Literal::Number("1234".into())).into(),
                alias: None,
            },
        )
    }

    #[test]
    fn literal_with_alias() {
        expect_parse_expr_ok(
            "1234 as foo",
            ExprWithAlias {
                expr: ExprKind::Literal(Literal::Number("1234".into())).into(),
                alias: Some(ident("foo")),
            },
        )
    }

    #[test]
    fn literal_with_alias_and_trailing_tokens() {
        expect_parse_expr_error("1234 as foo.bar", "expected end of expression, found .")
    }

    #[test]
    fn literal_with_alias_and_trailing_whitespace() {
        expect_parse_expr_ok(
            "1234 as foo   ",
            ExprWithAlias {
                expr: ExprKind::Literal(Literal::Number("1234".into())).into(),
                alias: Some(ident("foo")),
            },
        )
    }

    #[test]
    fn literal_with_alias_and_trailing_whitespace_and_token() {
        expect_parse_expr_error(
            "1234 as foo    bar",
            "expected end of expression, found bar",
        )
    }

    // ------------------------------------------------------------------
    // Postgres-style `EXPLAIN (option, ...)` tests
    // ------------------------------------------------------------------

    fn parse_with_pg(sql: &str) -> Result<Statement<'_>, DataFusionError> {
        let mut statements =
            DFParser::parse_sql_with_dialect(sql, DialectKind::Postgres.into())?;
        assert_eq!(statements.len(), 1, "Expected exactly one statement");
        Ok(statements.pop_front().unwrap())
    }

    fn parse_with_generic(sql: &str) -> Result<Statement<'_>, DataFusionError> {
        let mut statements = DFParser::parse_sql(sql)?;
        assert_eq!(statements.len(), 1, "Expected exactly one statement");
        Ok(statements.pop_front().unwrap())
    }

    #[test]
    fn explain_legacy_keyword_form_postgres_dialect() {
        // The legacy keyword form still works under PostgreSQL dialect.
        let stmt = parse_with_pg("EXPLAIN ANALYZE VERBOSE SELECT 1").unwrap();
        let Statement::Explain(ExplainStatement { options, .. }) = stmt else {
            panic!("Expected Statement::Explain");
        };
        assert!(options.analyze);
        assert!(options.verbose);
        assert!(options.format.is_none());
        assert!(options.analyze_level.is_none());
    }

    #[test]
    fn explain_paren_form_on_generic_supports_utility_options() {
        // DataFusion's own dialect, the default, accepts the parenthesized
        // form too.
        let stmt = parse_with_generic("EXPLAIN (FORMAT TREE) SELECT 1").unwrap();
        let Statement::Explain(ExplainStatement { options, .. }) = stmt else {
            panic!("Expected Statement::Explain");
        };
        assert_eq!(options.format, Some(ExplainFormat::Tree));
    }

    #[test]
    fn explain_paren_form_on_non_supporting_dialect_is_parse_error() {
        // Dialects that do NOT declare support for utility options (e.g.
        // Snowflake) must still error on the parenthesized form — proving
        // the dialect gate itself works.
        let res = DFParser::parse_sql_with_dialect(
            "EXPLAIN (FORMAT TREE) SELECT 1",
            DialectKind::Snowflake.into(),
        );
        assert!(
            res.is_err(),
            "expected parse error under non-supporting dialect"
        );
    }

    #[test]
    fn explain_paren_grouping_query_is_not_mistaken_for_options() {
        // Historic DataFusion behavior allows parentheses around the
        // query after EXPLAIN (e.g. `EXPLAIN (SELECT ...)` or
        // `EXPLAIN (q1 EXCEPT q2) UNION ALL (q3 EXCEPT q4)`). The dialect
        // gate for Postgres-style options must not swallow these.
        for sql in [
            "EXPLAIN (SELECT 1)",
            "EXPLAIN (WITH t AS (SELECT 1) SELECT * FROM t)",
            "EXPLAIN (VALUES (1), (2))",
            "EXPLAIN ((SELECT 1))",
        ] {
            let stmt = parse_with_pg(sql).unwrap_or_else(|e| {
                panic!("{sql} failed under PG dialect: {e}");
            });
            let Statement::Explain(ExplainStatement { options, .. }) = stmt else {
                panic!("Expected Statement::Explain for {sql}");
            };
            assert!(!options.analyze, "{sql} should not be ANALYZE");
            assert!(!options.verbose, "{sql} should not be VERBOSE");
            assert!(options.format.is_none(), "{sql} should have no FORMAT");
        }
    }

    #[test]
    fn explain_paren_form_analyze_verbose() {
        let stmt = parse_with_pg("EXPLAIN (ANALYZE, VERBOSE) SELECT 1").unwrap();
        let Statement::Explain(ExplainStatement { options, .. }) = stmt else {
            panic!("Expected Statement::Explain");
        };
        assert!(options.analyze);
        assert!(options.verbose);
    }

    #[test]
    fn explain_paren_form_format_tree() {
        let stmt = parse_with_pg("EXPLAIN (FORMAT tree) SELECT 1").unwrap();
        let Statement::Explain(ExplainStatement { options, .. }) = stmt else {
            panic!("Expected Statement::Explain");
        };
        assert!(!options.analyze);
        assert_eq!(options.format, Some(ExplainFormat::Tree));
    }

    #[test]
    fn explain_paren_form_metrics_level() {
        use datafusion_common::format::{
            ExplainAnalyzeCategories, MetricCategory, MetricType,
        };
        let stmt =
            parse_with_pg("EXPLAIN (ANALYZE, METRICS 'rows,bytes', LEVEL dev) SELECT 1")
                .unwrap();
        let Statement::Explain(ExplainStatement { options, .. }) = stmt else {
            panic!("Expected Statement::Explain");
        };
        assert!(options.analyze);
        assert_eq!(options.analyze_level, Some(MetricType::Dev));
        assert_eq!(
            options.analyze_categories,
            Some(ExplainAnalyzeCategories::Only(vec![
                MetricCategory::Rows,
                MetricCategory::Bytes,
            ]))
        );
    }

    #[test]
    fn explain_paren_form_bool_spellings() {
        let stmt =
            parse_with_pg("EXPLAIN (ANALYZE ON, VERBOSE OFF, COSTS TRUE) SELECT 1")
                .unwrap();
        let Statement::Explain(ExplainStatement { options, .. }) = stmt else {
            panic!("Expected Statement::Explain");
        };
        assert!(options.analyze);
        assert!(!options.verbose);
        assert_eq!(options.show_statistics, Some(true));
    }

    #[test]
    fn explain_paren_form_buffers_rejected() {
        let err = parse_with_pg("EXPLAIN (BUFFERS) SELECT 1").unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("BUFFERS"),
            "error should mention BUFFERS: {msg}"
        );
        assert!(
            msg.contains("not supported"),
            "error should say not supported: {msg}"
        );
    }

    #[test]
    fn explain_paren_form_unknown_option_rejected() {
        let err = parse_with_pg("EXPLAIN (ASDF) SELECT 1").unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("unknown EXPLAIN option"),
            "error should describe unknown option: {msg}"
        );
    }
}
