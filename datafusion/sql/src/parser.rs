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

//! [`DFParser`]: DataFusion SQL Parser based on [`sqlparser`]
//!
//! This parser implements DataFusion specific statements such as
//! `CREATE EXTERNAL TABLE`

use datafusion_common::DataFusionError;
use datafusion_common::config::SqlParserOptions;
use datafusion_common::{Diagnostic, Span, sql_err};
use sqlparser::ast::{ExprWithAlias, Ident, OrderByOptions};
use sqlparser::tokenizer::TokenWithSpan;
use sqlparser::{
    ast::{
        ColumnDef, ColumnOptionDef, ObjectName, OrderByExpr, Query,
        Statement as SQLStatement, TableConstraint, Value,
    },
    dialect::{Dialect, GenericDialect, keywords::Keyword},
    parser::{Parser, ParserError},
    tokenizer::{Token, Tokenizer, Word},
};
use std::collections::VecDeque;
use std::fmt;

// Use `Parser::expected` instead, if possible
macro_rules! parser_err {
    ($MSG:expr $(; diagnostic = $DIAG:expr)?) => {{

        let err = DataFusionError::from(ParserError::ParserError($MSG.to_string()));
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
/// Syntax:
/// ```sql
/// EXPLAIN <ANALYZE> <VERBOSE> [FORMAT format] statement
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExplainStatement {
    /// `EXPLAIN ANALYZE ..`
    pub analyze: bool,
    /// `EXPLAIN .. VERBOSE ..`
    pub verbose: bool,
    /// `EXPLAIN .. FORMAT `
    pub format: Option<String>,
    /// The statement to analyze. Note this is a DataFusion [`Statement`] (not a
    /// [`sqlparser::ast::Statement`] so that we can use `EXPLAIN`, `COPY`, and other
    /// DataFusion specific statements
    pub statement: Box<Statement>,
}

impl fmt::Display for ExplainStatement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let Self {
            analyze,
            verbose,
            format,
            statement,
        } = self;

        write!(f, "EXPLAIN ")?;
        if *analyze {
            write!(f, "ANALYZE ")?;
        }
        if *verbose {
            write!(f, "VERBOSE ")?;
        }
        if let Some(format) = format.as_ref() {
            write!(f, "FORMAT {format} ")?;
        }

        write!(f, "{statement}")
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
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CopyToStatement {
    /// From where the data comes from
    pub source: CopyToSource,
    /// The URL to where the data is heading
    pub target: String,
    /// Partition keys
    pub partitioned_by: Vec<String>,
    /// File type (Parquet, NDJSON, CSV etc.)
    pub stored_as: Option<String>,
    /// Target specific options
    pub options: Vec<(String, Value)>,
}

impl fmt::Display for CopyToStatement {
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CopyToSource {
    /// `COPY <table> TO ...`
    Relation(ObjectName),
    /// COPY (...query...) TO ...
    Query(Box<Query>),
}

impl fmt::Display for CopyToSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CopyToSource::Relation(r) => write!(f, "{r}"),
            CopyToSource::Query(q) => write!(f, "({q})"),
        }
    }
}

/// This type defines a lexicographical ordering.
pub(crate) type LexOrdering = Vec<OrderByExpr>;

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
/// LOCATION <literal>
///
/// <column_definition> := (<column_name> <data_type>, ...)
///
/// <column_list> := (<column_name>, ...)
///
/// <ordered_column_list> := (<column_name> <sort_clause>, ...)
///
/// <key_value_list> := (<literal> <literal, <literal> <literal>, ...)
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateExternalTable {
    /// Table name
    pub name: ObjectName,
    /// Optional schema
    pub columns: Vec<ColumnDef>,
    /// File type (Parquet, NDJSON, CSV, etc)
    pub file_type: String,
    /// Path to file
    pub location: String,
    /// Partition Columns
    pub table_partition_cols: Vec<String>,
    /// Ordered expressions
    pub order_exprs: Vec<LexOrdering>,
    /// Option to not error if table already exists
    pub if_not_exists: bool,
    /// Option to replace table content if table already exists
    pub or_replace: bool,
    /// Whether the table is a temporary table
    pub temporary: bool,
    /// Infinite streams?
    pub unbounded: bool,
    /// Table(provider) specific options
    pub options: Vec<(String, Value)>,
    /// A table-level constraint
    pub constraints: Vec<TableConstraint>,
}

impl fmt::Display for CreateExternalTable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CREATE EXTERNAL TABLE ")?;
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
        write!(f, "LOCATION {}", self.location)
    }
}

/// DataFusion extension for `RESET`
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResetStatement {
    /// Reset a single configuration variable (stored as provided)
    Variable(ObjectName),
}

impl fmt::Display for ResetStatement {
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
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Statement {
    /// ANSI SQL AST node (from sqlparser-rs)
    Statement(Box<SQLStatement>),
    /// Extension: `CREATE EXTERNAL TABLE`
    CreateExternalTable(CreateExternalTable),
    /// Extension: `COPY TO`
    CopyTo(CopyToStatement),
    /// EXPLAIN for extensions
    Explain(ExplainStatement),
    /// Extension: `RESET`
    Reset(ResetStatement),
}

impl fmt::Display for Statement {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Statement::Statement(stmt) => write!(f, "{stmt}"),
            Statement::CreateExternalTable(stmt) => write!(f, "{stmt}"),
            Statement::CopyTo(stmt) => write!(f, "{stmt}"),
            Statement::Explain(stmt) => write!(f, "{stmt}"),
            Statement::Reset(stmt) => write!(f, "{stmt}"),
        }
    }
}

fn ensure_not_set<T>(field: &Option<T>, name: &str) -> Result<(), DataFusionError> {
    if field.is_some() {
        parser_err!(format!("{name} specified more than once",))?
    }
    Ok(())
}

/// DataFusion SQL Parser based on [`sqlparser`]
///
/// Parses DataFusion's SQL dialect, often delegating to [`sqlparser`]'s [`Parser`].
///
/// DataFusion mostly follows existing SQL dialects via
/// `sqlparser`. However, certain statements such as `COPY` and
/// `CREATE EXTERNAL TABLE` have special syntax in DataFusion. See
/// [`Statement`] for a list of this special syntax
pub struct DFParser<'a> {
    pub parser: Parser<'a>,
    options: SqlParserOptions,
}

/// Same as `sqlparser`
const DEFAULT_RECURSION_LIMIT: usize = 50;
const DEFAULT_DIALECT: GenericDialect = GenericDialect {};

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
///
/// # Example: Create and Parse expression with a different dialect
/// ```
/// # use datafusion_sql::parser::DFParserBuilder;
/// # use datafusion_common::Result;
/// # use datafusion_sql::sqlparser::dialect::MySqlDialect;
/// # use datafusion_sql::sqlparser::ast::Expr;
/// # fn test() -> Result<()> {
/// let dialect = MySqlDialect {}; // Parse using MySQL dialect
/// let mut parser = DFParserBuilder::new("1 + 2")
///     .with_dialect(&dialect)
///     .build()?;
/// // parse 1+2 into an sqlparser::ast::Expr
/// let res = parser.parse_expr()?;
/// assert!(matches!(res.expr, Expr::BinaryOp { .. }));
/// # Ok(())
/// # }
/// ```
pub struct DFParserBuilder<'a> {
    /// The SQL string to parse
    sql: &'a str,
    /// The Dialect to use (defaults to [`GenericDialect`]
    dialect: &'a dyn Dialect,
    /// The recursion limit while parsing
    recursion_limit: usize,
}

impl<'a> DFParserBuilder<'a> {
    /// Create a new parser builder for the specified tokens using the
    /// [`GenericDialect`].
    pub fn new(sql: &'a str) -> Self {
        Self {
            sql,
            dialect: &DEFAULT_DIALECT,
            recursion_limit: DEFAULT_RECURSION_LIMIT,
        }
    }

    /// Adjust the parser builder's dialect. Defaults to [`GenericDialect`]
    pub fn with_dialect(mut self, dialect: &'a dyn Dialect) -> Self {
        self.dialect = dialect;
        self
    }

    /// Adjust the recursion limit of sql parsing.  Defaults to 50
    pub fn with_recursion_limit(mut self, recursion_limit: usize) -> Self {
        self.recursion_limit = recursion_limit;
        self
    }

    pub fn build(self) -> Result<DFParser<'a>, DataFusionError> {
        let mut tokenizer = Tokenizer::new(self.dialect, self.sql);
        // Convert TokenizerError -> ParserError
        let tokens = tokenizer
            .tokenize_with_location()
            .map_err(ParserError::from)?;

        Ok(DFParser {
            parser: Parser::new(self.dialect)
                .with_tokens_with_locations(tokens)
                .with_recursion_limit(self.recursion_limit),
            options: SqlParserOptions {
                recursion_limit: self.recursion_limit,
                ..Default::default()
            },
        })
    }
}

impl<'a> DFParser<'a> {
    #[deprecated(since = "46.0.0", note = "DFParserBuilder")]
    pub fn new(sql: &'a str) -> Result<Self, DataFusionError> {
        DFParserBuilder::new(sql).build()
    }

    #[deprecated(since = "46.0.0", note = "DFParserBuilder")]
    pub fn new_with_dialect(
        sql: &'a str,
        dialect: &'a dyn Dialect,
    ) -> Result<Self, DataFusionError> {
        DFParserBuilder::new(sql).with_dialect(dialect).build()
    }

    /// Parse a sql string into one or [`Statement`]s using the
    /// [`GenericDialect`].
    pub fn parse_sql(sql: &'a str) -> Result<VecDeque<Statement>, DataFusionError> {
        let mut parser = DFParserBuilder::new(sql).build()?;

        parser.parse_statements()
    }

    /// Parse a SQL string and produce one or more [`Statement`]s with
    /// with the specified dialect.
    pub fn parse_sql_with_dialect(
        sql: &str,
        dialect: &dyn Dialect,
    ) -> Result<VecDeque<Statement>, DataFusionError> {
        let mut parser = DFParserBuilder::new(sql).with_dialect(dialect).build()?;
        parser.parse_statements()
    }

    pub fn parse_sql_into_expr(sql: &str) -> Result<ExprWithAlias, DataFusionError> {
        DFParserBuilder::new(sql).build()?.parse_into_expr()
    }

    pub fn parse_sql_into_expr_with_dialect(
        sql: &str,
        dialect: &dyn Dialect,
    ) -> Result<ExprWithAlias, DataFusionError> {
        DFParserBuilder::new(sql)
            .with_dialect(dialect)
            .build()?
            .parse_into_expr()
    }

    /// Parse a sql string into one or [`Statement`]s
    pub fn parse_statements(&mut self) -> Result<VecDeque<Statement>, DataFusionError> {
        let mut stmts = VecDeque::new();
        let mut expecting_statement_delimiter = false;
        loop {
            // ignore empty statements (between successive statement delimiters)
            while self.parser.consume_token(&Token::SemiColon) {
                expecting_statement_delimiter = false;
            }

            if self.parser.peek_token() == Token::EOF {
                break;
            }
            if expecting_statement_delimiter {
                return self.expected("end of statement", &self.parser.peek_token());
            }

            let statement = self.parse_statement()?;
            stmts.push_back(statement);
            expecting_statement_delimiter = true;
        }
        Ok(stmts)
    }

    /// Report an unexpected token
    fn expected<T>(
        &self,
        expected: &str,
        found: &TokenWithSpan,
    ) -> Result<T, DataFusionError> {
        let sql_parser_span = found.span;
        let span = Span::try_from_sqlparser_span(sql_parser_span);
        let diagnostic = Diagnostic::new_error(
            format!("Expected: {expected}, found: {found}{}", found.span.start),
            span,
        );
        parser_err!(
            format!("Expected: {expected}, found: {found}{}", found.span.start);
            diagnostic=
            diagnostic
        )
    }

    fn expect_token(
        &mut self,
        expected: &str,
        token: &Token,
    ) -> Result<(), DataFusionError> {
        let next_token = self.parser.peek_token_ref();
        if next_token.token != *token {
            self.expected(expected, next_token)
        } else {
            Ok(())
        }
    }

    /// Parse a new expression
    pub fn parse_statement(&mut self) -> Result<Statement, DataFusionError> {
        match self.parser.peek_token().token {
            Token::Word(w) => {
                match w.keyword {
                    Keyword::CREATE => {
                        self.parser.next_token(); // CREATE
                        self.parse_create()
                    }
                    Keyword::COPY => {
                        if let Token::Word(w) = self.parser.peek_nth_token(1).token {
                            // use native parser for COPY INTO
                            if w.keyword == Keyword::INTO {
                                return self.parse_and_handle_statement();
                            }
                        }
                        self.parser.next_token(); // COPY
                        self.parse_copy()
                    }
                    Keyword::EXPLAIN => {
                        self.parser.next_token(); // EXPLAIN
                        self.parse_explain()
                    }
                    Keyword::RESET => {
                        self.parser.next_token(); // RESET
                        self.parse_reset()
                    }
                    _ => {
                        // use sqlparser-rs parser
                        self.parse_and_handle_statement()
                    }
                }
            }
            _ => {
                // use the native parser
                self.parse_and_handle_statement()
            }
        }
    }

    pub fn parse_expr(&mut self) -> Result<ExprWithAlias, DataFusionError> {
        if let Token::Word(w) = self.parser.peek_token().token {
            match w.keyword {
                Keyword::CREATE | Keyword::COPY | Keyword::EXPLAIN => {
                    return parser_err!("Unsupported command in expression")?;
                }
                _ => {}
            }
        }

        Ok(self.parser.parse_expr_with_alias()?)
    }

    /// Parses the entire SQL string into an expression.
    ///
    /// In contrast to [`DFParser::parse_expr`], this function will report an error if the input
    /// contains any trailing, unparsed tokens.
    pub fn parse_into_expr(&mut self) -> Result<ExprWithAlias, DataFusionError> {
        let expr = self.parse_expr()?;
        self.expect_token("end of expression", &Token::EOF)?;
        Ok(expr)
    }

    /// Helper method to parse a statement and handle errors consistently, especially for recursion limits
    fn parse_and_handle_statement(&mut self) -> Result<Statement, DataFusionError> {
        self.parser
            .parse_statement()
            .map(|stmt| Statement::Statement(Box::from(stmt)))
            .map_err(|e| match e {
                ParserError::RecursionLimitExceeded => DataFusionError::SQL(
                    Box::new(ParserError::RecursionLimitExceeded),
                    Some(format!(
                        " (current limit: {})",
                        self.options.recursion_limit
                    )),
                ),
                other => DataFusionError::SQL(Box::new(other), None),
            })
    }

    /// Parse a SQL `COPY TO` statement
    pub fn parse_copy(&mut self) -> Result<Statement, DataFusionError> {
        // parse as a query
        let source = if self.parser.consume_token(&Token::LParen) {
            let query = self.parser.parse_query()?;
            self.parser.expect_token(&Token::RParen)?;
            CopyToSource::Query(query)
        } else {
            // parse as table reference
            let table_name = self.parser.parse_object_name(true)?;
            CopyToSource::Relation(table_name)
        };

        #[derive(Default)]
        struct Builder {
            stored_as: Option<String>,
            target: Option<String>,
            partitioned_by: Option<Vec<String>>,
            options: Option<Vec<(String, Value)>>,
        }

        let mut builder = Builder::default();

        loop {
            if let Some(keyword) = self.parser.parse_one_of_keywords(&[
                Keyword::STORED,
                Keyword::TO,
                Keyword::PARTITIONED,
                Keyword::OPTIONS,
                Keyword::WITH,
            ]) {
                match keyword {
                    Keyword::STORED => {
                        self.parser.expect_keyword(Keyword::AS)?;
                        ensure_not_set(&builder.stored_as, "STORED AS")?;
                        builder.stored_as = Some(self.parse_file_format()?);
                    }
                    Keyword::TO => {
                        ensure_not_set(&builder.target, "TO")?;
                        builder.target = Some(self.parser.parse_literal_string()?);
                    }
                    Keyword::WITH => {
                        self.parser.expect_keyword(Keyword::HEADER)?;
                        self.parser.expect_keyword(Keyword::ROW)?;
                        return parser_err!(
                            "WITH HEADER ROW clause is no longer in use. Please use the OPTIONS clause with 'format.has_header' set appropriately, e.g., OPTIONS ('format.has_header' 'true')"
                        )?;
                    }
                    Keyword::PARTITIONED => {
                        self.parser.expect_keyword(Keyword::BY)?;
                        ensure_not_set(&builder.partitioned_by, "PARTITIONED BY")?;
                        builder.partitioned_by = Some(self.parse_partitions()?);
                    }
                    Keyword::OPTIONS => {
                        ensure_not_set(&builder.options, "OPTIONS")?;
                        builder.options = Some(self.parse_value_options()?);
                    }
                    _ => {
                        unreachable!()
                    }
                }
            } else {
                let token = self.parser.peek_token();
                if token == Token::EOF || token == Token::SemiColon {
                    break;
                } else {
                    return self.expected("end of statement or ;", &token)?;
                }
            }
        }

        let Some(target) = builder.target else {
            return parser_err!("Missing TO clause in COPY statement")?;
        };

        Ok(Statement::CopyTo(CopyToStatement {
            source,
            target,
            partitioned_by: builder.partitioned_by.unwrap_or(vec![]),
            stored_as: builder.stored_as,
            options: builder.options.unwrap_or(vec![]),
        }))
    }

    /// Parse the next token as a key name for an option list
    ///
    /// Note this is different than [`parse_literal_string`]
    /// because it allows keywords as well as other non words
    ///
    /// [`parse_literal_string`]: sqlparser::parser::Parser::parse_literal_string
    pub fn parse_option_key(&mut self) -> Result<String, DataFusionError> {
        let next_token = self.parser.next_token();
        match next_token.token {
            Token::Word(Word { value, .. }) => {
                let mut parts = vec![value];
                while self.parser.consume_token(&Token::Period) {
                    let next_token = self.parser.next_token();
                    if let Token::Word(Word { value, .. }) = next_token.token {
                        parts.push(value);
                    } else {
                        // Unquoted namespaced keys have to conform to the syntax
                        // "<WORD>[\.<WORD>]*". If we have a key that breaks this
                        // pattern, error out:
                        return self.expected("key name", &next_token);
                    }
                }
                Ok(parts.join("."))
            }
            Token::SingleQuotedString(s) => Ok(s),
            Token::DoubleQuotedString(s) => Ok(s),
            Token::EscapedStringLiteral(s) => Ok(s),
            _ => self.expected("key name", &next_token),
        }
    }

    /// Parse the next token as a value for an option list
    ///
    /// Note this is different than [`parse_value`] as it allows any
    /// word or keyword in this location.
    ///
    /// [`parse_value`]: sqlparser::parser::Parser::parse_value
    pub fn parse_option_value(&mut self) -> Result<Value, DataFusionError> {
        let next_token = self.parser.next_token();
        match next_token.token {
            // e.g. things like "snappy" or "gzip" that may be keywords
            Token::Word(word) => Ok(Value::SingleQuotedString(word.value)),
            Token::SingleQuotedString(s) => Ok(Value::SingleQuotedString(s)),
            Token::DoubleQuotedString(s) => Ok(Value::DoubleQuotedString(s)),
            Token::EscapedStringLiteral(s) => Ok(Value::EscapedStringLiteral(s)),
            Token::Number(n, l) => Ok(Value::Number(n, l)),
            _ => self.expected("string or numeric value", &next_token),
        }
    }

    /// Parse a SQL `EXPLAIN`
    pub fn parse_explain(&mut self) -> Result<Statement, DataFusionError> {
        let analyze = self.parser.parse_keyword(Keyword::ANALYZE);
        let verbose = self.parser.parse_keyword(Keyword::VERBOSE);
        let format = self.parse_explain_format()?;

        let statement = self.parse_statement()?;

        Ok(Statement::Explain(ExplainStatement {
            statement: Box::new(statement),
            analyze,
            verbose,
            format,
        }))
    }

    /// Parse a SQL `RESET`
    pub fn parse_reset(&mut self) -> Result<Statement, DataFusionError> {
        let mut parts: Vec<String> = Vec::new();
        let mut expecting_segment = true;

        loop {
            let next_token = self.parser.peek_token();
            match &next_token.token {
                Token::Word(word) => {
                    self.parser.next_token();
                    parts.push(word.value.clone());
                    expecting_segment = false;
                }
                Token::SingleQuotedString(s)
                | Token::DoubleQuotedString(s)
                | Token::EscapedStringLiteral(s) => {
                    self.parser.next_token();
                    parts.push(s.clone());
                    expecting_segment = false;
                }
                Token::Period => {
                    self.parser.next_token();
                    if expecting_segment || parts.is_empty() {
                        return self.expected("configuration parameter", &next_token);
                    }
                    expecting_segment = true;
                }
                Token::EOF | Token::SemiColon => break,
                _ => return self.expected("configuration parameter", &next_token),
            }
        }

        if parts.is_empty() || expecting_segment {
            return self.expected("configuration parameter", &self.parser.peek_token());
        }

        let idents: Vec<Ident> = parts.into_iter().map(Ident::new).collect();
        let variable = ObjectName::from(idents);
        Ok(Statement::Reset(ResetStatement::Variable(variable)))
    }

    pub fn parse_explain_format(&mut self) -> Result<Option<String>, DataFusionError> {
        if !self.parser.parse_keyword(Keyword::FORMAT) {
            return Ok(None);
        }

        let next_token = self.parser.next_token();
        let format = match next_token.token {
            Token::Word(w) => Ok(w.value),
            Token::SingleQuotedString(w) => Ok(w),
            Token::DoubleQuotedString(w) => Ok(w),
            _ => self.expected("an explain format such as TREE", &next_token),
        }?;
        Ok(Some(format))
    }

    /// Parse a SQL `CREATE` statement handling `CREATE EXTERNAL TABLE`
    pub fn parse_create(&mut self) -> Result<Statement, DataFusionError> {
        // TODO: Change sql parser to take in `or_replace: bool` inside parse_create()
        if self
            .parser
            .parse_keywords(&[Keyword::OR, Keyword::REPLACE, Keyword::EXTERNAL])
        {
            self.parse_create_external_table(false, true)
        } else if self.parser.parse_keywords(&[
            Keyword::OR,
            Keyword::REPLACE,
            Keyword::UNBOUNDED,
            Keyword::EXTERNAL,
        ]) {
            self.parse_create_external_table(true, true)
        } else if self.parser.parse_keyword(Keyword::EXTERNAL) {
            self.parse_create_external_table(false, false)
        } else if self
            .parser
            .parse_keywords(&[Keyword::UNBOUNDED, Keyword::EXTERNAL])
        {
            self.parse_create_external_table(true, false)
        } else {
            Ok(Statement::Statement(Box::from(self.parser.parse_create()?)))
        }
    }

    fn parse_partitions(&mut self) -> Result<Vec<String>, DataFusionError> {
        let mut partitions: Vec<String> = vec![];
        if !self.parser.consume_token(&Token::LParen)
            || self.parser.consume_token(&Token::RParen)
        {
            return Ok(partitions);
        }

        loop {
            if let Token::Word(_) = self.parser.peek_token().token {
                let identifier = self.parser.parse_identifier()?;
                partitions.push(identifier.to_string());
            } else {
                return self.expected("partition name", &self.parser.peek_token());
            }
            let comma = self.parser.consume_token(&Token::Comma);
            if self.parser.consume_token(&Token::RParen) {
                // allow a trailing comma, even though it's not in standard
                break;
            } else if !comma {
                return self.expected(
                    "',' or ')' after partition definition",
                    &self.parser.peek_token(),
                );
            }
        }
        Ok(partitions)
    }

    /// Parse the ordering clause of a `CREATE EXTERNAL TABLE` SQL statement
    pub fn parse_order_by_exprs(&mut self) -> Result<Vec<OrderByExpr>, DataFusionError> {
        let mut values = vec![];
        self.parser.expect_token(&Token::LParen)?;
        loop {
            values.push(self.parse_order_by_expr()?);
            if !self.parser.consume_token(&Token::Comma) {
                self.parser.expect_token(&Token::RParen)?;
                return Ok(values);
            }
        }
    }

    /// Parse an ORDER BY sub-expression optionally followed by ASC or DESC.
    pub fn parse_order_by_expr(&mut self) -> Result<OrderByExpr, DataFusionError> {
        let expr = self.parser.parse_expr()?;

        let asc = if self.parser.parse_keyword(Keyword::ASC) {
            Some(true)
        } else if self.parser.parse_keyword(Keyword::DESC) {
            Some(false)
        } else {
            None
        };

        let nulls_first = if self
            .parser
            .parse_keywords(&[Keyword::NULLS, Keyword::FIRST])
        {
            Some(true)
        } else if self.parser.parse_keywords(&[Keyword::NULLS, Keyword::LAST]) {
            Some(false)
        } else {
            None
        };

        Ok(OrderByExpr {
            expr,
            options: OrderByOptions { asc, nulls_first },
            with_fill: None,
        })
    }

    // This is a copy of the equivalent implementation in sqlparser.
    fn parse_columns(
        &mut self,
    ) -> Result<(Vec<ColumnDef>, Vec<TableConstraint>), DataFusionError> {
        let mut columns = vec![];
        let mut constraints = vec![];
        if !self.parser.consume_token(&Token::LParen)
            || self.parser.consume_token(&Token::RParen)
        {
            return Ok((columns, constraints));
        }

        loop {
            if let Some(constraint) = self.parser.parse_optional_table_constraint()? {
                constraints.push(constraint);
            } else if let Token::Word(_) = self.parser.peek_token().token {
                let column_def = self.parse_column_def()?;
                columns.push(column_def);
            } else {
                return self.expected(
                    "column name or constraint definition",
                    &self.parser.peek_token(),
                );
            }
            let comma = self.parser.consume_token(&Token::Comma);
            if self.parser.consume_token(&Token::RParen) {
                // allow a trailing comma, even though it's not in standard
                break;
            } else if !comma {
                return self.expected(
                    "',' or ')' after column definition",
                    &self.parser.peek_token(),
                );
            }
        }

        Ok((columns, constraints))
    }

    fn parse_column_def(&mut self) -> Result<ColumnDef, DataFusionError> {
        let name = self.parser.parse_identifier()?;
        let data_type = self.parser.parse_data_type()?;
        let mut options = vec![];
        loop {
            if self.parser.parse_keyword(Keyword::CONSTRAINT) {
                let name = Some(self.parser.parse_identifier()?);
                if let Some(option) = self.parser.parse_optional_column_option()? {
                    options.push(ColumnOptionDef { name, option });
                } else {
                    return self.expected(
                        "constraint details after CONSTRAINT <name>",
                        &self.parser.peek_token(),
                    );
                }
            } else if let Some(option) = self.parser.parse_optional_column_option()? {
                options.push(ColumnOptionDef { name: None, option });
            } else {
                break;
            };
        }
        Ok(ColumnDef {
            name,
            data_type,
            options,
        })
    }

    fn parse_create_external_table(
        &mut self,
        unbounded: bool,
        or_replace: bool,
    ) -> Result<Statement, DataFusionError> {
        let temporary = self
            .parser
            .parse_one_of_keywords(&[Keyword::TEMP, Keyword::TEMPORARY])
            .is_some();

        self.parser.expect_keyword(Keyword::TABLE)?;
        let if_not_exists =
            self.parser
                .parse_keywords(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS]);

        if if_not_exists && or_replace {
            return parser_err!("'IF NOT EXISTS' cannot coexist with 'REPLACE'");
        }

        let table_name = self.parser.parse_object_name(true)?;
        let (mut columns, constraints) = self.parse_columns()?;

        #[derive(Default)]
        struct Builder {
            file_type: Option<String>,
            location: Option<String>,
            table_partition_cols: Option<Vec<String>>,
            order_exprs: Vec<LexOrdering>,
            options: Option<Vec<(String, Value)>>,
        }
        let mut builder = Builder::default();

        loop {
            if let Some(keyword) = self.parser.parse_one_of_keywords(&[
                Keyword::STORED,
                Keyword::LOCATION,
                Keyword::WITH,
                Keyword::DELIMITER,
                Keyword::COMPRESSION,
                Keyword::PARTITIONED,
                Keyword::OPTIONS,
            ]) {
                match keyword {
                    Keyword::STORED => {
                        self.parser.expect_keyword(Keyword::AS)?;
                        ensure_not_set(&builder.file_type, "STORED AS")?;
                        builder.file_type = Some(self.parse_file_format()?);
                    }
                    Keyword::LOCATION => {
                        ensure_not_set(&builder.location, "LOCATION")?;
                        builder.location = Some(self.parser.parse_literal_string()?);
                    }
                    Keyword::WITH => {
                        if self.parser.parse_keyword(Keyword::ORDER) {
                            builder.order_exprs.push(self.parse_order_by_exprs()?);
                        } else {
                            self.parser.expect_keyword(Keyword::HEADER)?;
                            self.parser.expect_keyword(Keyword::ROW)?;
                            return parser_err!(
                                "WITH HEADER ROW clause is no longer in use. Please use the OPTIONS clause with 'format.has_header' set appropriately, e.g., OPTIONS (format.has_header true)"
                            )?;
                        }
                    }
                    Keyword::DELIMITER => {
                        return parser_err!(
                            "DELIMITER clause is no longer in use. Please use the OPTIONS clause with 'format.delimiter' set appropriately, e.g., OPTIONS (format.delimiter ',')"
                        )?;
                    }
                    Keyword::COMPRESSION => {
                        self.parser.expect_keyword(Keyword::TYPE)?;
                        return parser_err!(
                            "COMPRESSION TYPE clause is no longer in use. Please use the OPTIONS clause with 'format.compression' set appropriately, e.g., OPTIONS (format.compression gzip)"
                        )?;
                    }
                    Keyword::PARTITIONED => {
                        self.parser.expect_keyword(Keyword::BY)?;
                        ensure_not_set(&builder.table_partition_cols, "PARTITIONED BY")?;
                        // Expects either list of column names (col_name [, col_name]*)
                        // or list of column definitions (col_name datatype [, col_name datatype]* )
                        // use the token after the name to decide which parsing rule to use
                        // Note that mixing both names and definitions is not allowed
                        let peeked = self.parser.peek_nth_token(2);
                        if peeked == Token::Comma || peeked == Token::RParen {
                            // List of column names
                            builder.table_partition_cols = Some(self.parse_partitions()?)
                        } else {
                            // List of column defs
                            let (cols, cons) = self.parse_columns()?;
                            builder.table_partition_cols = Some(
                                cols.iter().map(|col| col.name.to_string()).collect(),
                            );

                            columns.extend(cols);

                            if !cons.is_empty() {
                                return sql_err!(ParserError::ParserError(
                                    "Constraints on Partition Columns are not supported"
                                        .to_string(),
                                ));
                            }
                        }
                    }
                    Keyword::OPTIONS => {
                        ensure_not_set(&builder.options, "OPTIONS")?;
                        builder.options = Some(self.parse_value_options()?);
                    }
                    _ => {
                        unreachable!()
                    }
                }
            } else {
                let token = self.parser.peek_token();
                if token == Token::EOF || token == Token::SemiColon {
                    break;
                } else {
                    return self.expected("end of statement or ;", &token)?;
                }
            }
        }

        // Validations: location and file_type are required
        if builder.file_type.is_none() {
            return sql_err!(ParserError::ParserError(
                "Missing STORED AS clause in CREATE EXTERNAL TABLE statement".into(),
            ));
        }
        if builder.location.is_none() {
            return sql_err!(ParserError::ParserError(
                "Missing LOCATION clause in CREATE EXTERNAL TABLE statement".into(),
            ));
        }

        let create = CreateExternalTable {
            name: table_name,
            columns,
            file_type: builder.file_type.unwrap(),
            location: builder.location.unwrap(),
            table_partition_cols: builder.table_partition_cols.unwrap_or(vec![]),
            order_exprs: builder.order_exprs,
            if_not_exists,
            or_replace,
            temporary,
            unbounded,
            options: builder.options.unwrap_or(Vec::new()),
            constraints,
        };
        Ok(Statement::CreateExternalTable(create))
    }

    /// Parses the set of valid formats
    fn parse_file_format(&mut self) -> Result<String, DataFusionError> {
        let token = self.parser.next_token();
        match &token.token {
            Token::Word(w) => parse_file_type(&w.value),
            _ => self.expected("one of ARROW, PARQUET, NDJSON, or CSV", &token),
        }
    }

    /// Parses (key value) style options into a map of String --> [`Value`].
    ///
    /// This method supports keywords as key names as well as multiple
    /// value types such as Numbers as well as Strings.
    fn parse_value_options(&mut self) -> Result<Vec<(String, Value)>, DataFusionError> {
        let mut options = vec![];
        self.parser.expect_token(&Token::LParen)?;

        loop {
            let key = self.parse_option_key()?;
            let value = self.parse_option_value()?;
            options.push((key, value));
            let comma = self.parser.consume_token(&Token::Comma);
            if self.parser.consume_token(&Token::RParen) {
                // Allow a trailing comma, even though it's not in standard
                break;
            } else if !comma {
                return self.expected(
                    "',' or ')' after option definition",
                    &self.parser.peek_token(),
                );
            }
        }
        Ok(options)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion_common::assert_contains;
    use sqlparser::ast::Expr::Identifier;
    use sqlparser::ast::{
        BinaryOperator, DataType, ExactNumberInfo, Expr, Ident, ValueWithSpan,
    };
    use sqlparser::dialect::SnowflakeDialect;
    use sqlparser::tokenizer::Span;

    fn expect_parse_ok(sql: &str, expected: Statement) -> Result<(), DataFusionError> {
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

    fn make_column_def(name: impl Into<String>, data_type: DataType) -> ColumnDef {
        ColumnDef {
            name: Ident {
                value: name.into(),
                quote_style: None,
                span: Span::empty(),
            },
            data_type,
            options: vec![],
        }
    }

    #[test]
    fn create_external_table() -> Result<(), DataFusionError> {
        // positive case
        let sql = "CREATE EXTERNAL TABLE t(c1 int) STORED AS CSV LOCATION 'foo.csv'";
        let display = None;
        let name = ObjectName::from(vec![Ident::from("t")]);
        let expected = Statement::CreateExternalTable(CreateExternalTable {
            name: name.clone(),
            columns: vec![make_column_def("c1", DataType::Int(display))],
            file_type: "CSV".to_string(),
            location: "foo.csv".into(),
            table_partition_cols: vec![],
            order_exprs: vec![],
            if_not_exists: false,
            or_replace: false,
            temporary: false,
            unbounded: false,
            options: vec![],
            constraints: vec![],
        });
        expect_parse_ok(sql, expected)?;

        // positive case: leading space
        let sql = "CREATE EXTERNAL TABLE t(c1 int) STORED AS CSV LOCATION 'foo.csv'     ";
        let expected = Statement::CreateExternalTable(CreateExternalTable {
            name: name.clone(),
            columns: vec![make_column_def("c1", DataType::Int(None))],
            file_type: "CSV".to_string(),
            location: "foo.csv".into(),
            table_partition_cols: vec![],
            order_exprs: vec![],
            if_not_exists: false,
            or_replace: false,
            temporary: false,
            unbounded: false,
            options: vec![],
            constraints: vec![],
        });
        expect_parse_ok(sql, expected)?;

        // positive case: leading space + semicolon
        let sql =
            "CREATE EXTERNAL TABLE t(c1 int) STORED AS CSV LOCATION 'foo.csv'      ;";
        let expected = Statement::CreateExternalTable(CreateExternalTable {
            name: name.clone(),
            columns: vec![make_column_def("c1", DataType::Int(None))],
            file_type: "CSV".to_string(),
            location: "foo.csv".into(),
            table_partition_cols: vec![],
            order_exprs: vec![],
            if_not_exists: false,
            or_replace: false,
            temporary: false,
            unbounded: false,
            options: vec![],
            constraints: vec![],
        });
        expect_parse_ok(sql, expected)?;

        // positive case with delimiter
        let sql = "CREATE EXTERNAL TABLE t(c1 int) STORED AS CSV LOCATION 'foo.csv' OPTIONS (format.delimiter '|')";
        let display = None;
        let expected = Statement::CreateExternalTable(CreateExternalTable {
            name: name.clone(),
            columns: vec![make_column_def("c1", DataType::Int(display))],
            file_type: "CSV".to_string(),
            location: "foo.csv".into(),
            table_partition_cols: vec![],
            order_exprs: vec![],
            if_not_exists: false,
            or_replace: false,
            temporary: false,
            unbounded: false,
            options: vec![(
                "format.delimiter".into(),
                Value::SingleQuotedString("|".into()),
            )],
            constraints: vec![],
        });
        expect_parse_ok(sql, expected)?;

        // positive case: partitioned by
        let sql = "CREATE EXTERNAL TABLE t(c1 int) STORED AS CSV PARTITIONED BY (p1, p2) LOCATION 'foo.csv'";
        let display = None;
        let expected = Statement::CreateExternalTable(CreateExternalTable {
            name: name.clone(),
            columns: vec![make_column_def("c1", DataType::Int(display))],
            file_type: "CSV".to_string(),
            location: "foo.csv".into(),
            table_partition_cols: vec!["p1".to_string(), "p2".to_string()],
            order_exprs: vec![],
            if_not_exists: false,
            or_replace: false,
            temporary: false,
            unbounded: false,
            options: vec![],
            constraints: vec![],
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
                name: name.clone(),
                columns: vec![make_column_def("c1", DataType::Int(display))],
                file_type: "CSV".to_string(),
                location: "foo.csv".into(),
                table_partition_cols: vec![],
                order_exprs: vec![],
                if_not_exists: false,
                or_replace: false,
                temporary: false,
                unbounded: false,
                options: vec![(
                    "format.compression".into(),
                    Value::SingleQuotedString(compression.into()),
                )],
                constraints: vec![],
            });
            expect_parse_ok(sql, expected)?;
        }

        // positive case: it is ok for parquet files not to have columns specified
        let sql = "CREATE EXTERNAL TABLE t STORED AS PARQUET LOCATION 'foo.parquet'";
        let expected = Statement::CreateExternalTable(CreateExternalTable {
            name: name.clone(),
            columns: vec![],
            file_type: "PARQUET".to_string(),
            location: "foo.parquet".into(),
            table_partition_cols: vec![],
            order_exprs: vec![],
            if_not_exists: false,
            or_replace: false,
            temporary: false,
            unbounded: false,
            options: vec![],
            constraints: vec![],
        });
        expect_parse_ok(sql, expected)?;

        // positive case: it is ok for parquet files to be other than upper case
        let sql = "CREATE EXTERNAL TABLE t STORED AS parqueT LOCATION 'foo.parquet'";
        let expected = Statement::CreateExternalTable(CreateExternalTable {
            name: name.clone(),
            columns: vec![],
            file_type: "PARQUET".to_string(),
            location: "foo.parquet".into(),
            table_partition_cols: vec![],
            order_exprs: vec![],
            if_not_exists: false,
            or_replace: false,
            temporary: false,
            unbounded: false,
            options: vec![],
            constraints: vec![],
        });
        expect_parse_ok(sql, expected)?;

        // positive case: it is ok for avro files not to have columns specified
        let sql = "CREATE EXTERNAL TABLE t STORED AS AVRO LOCATION 'foo.avro'";
        let expected = Statement::CreateExternalTable(CreateExternalTable {
            name: name.clone(),
            columns: vec![],
            file_type: "AVRO".to_string(),
            location: "foo.avro".into(),
            table_partition_cols: vec![],
            order_exprs: vec![],
            if_not_exists: false,
            or_replace: false,
            temporary: false,
            unbounded: false,
            options: vec![],
            constraints: vec![],
        });
        expect_parse_ok(sql, expected)?;

        // positive case: it is ok for avro files not to have columns specified
        let sql = "CREATE EXTERNAL TABLE IF NOT EXISTS t STORED AS PARQUET LOCATION 'foo.parquet'";
        let expected = Statement::CreateExternalTable(CreateExternalTable {
            name: name.clone(),
            columns: vec![],
            file_type: "PARQUET".to_string(),
            location: "foo.parquet".into(),
            table_partition_cols: vec![],
            order_exprs: vec![],
            if_not_exists: true,
            or_replace: false,
            temporary: false,
            unbounded: false,
            options: vec![],
            constraints: vec![],
        });
        expect_parse_ok(sql, expected)?;

        // positive case: or replace
        let sql =
            "CREATE OR REPLACE EXTERNAL TABLE t STORED AS PARQUET LOCATION 'foo.parquet'";
        let expected = Statement::CreateExternalTable(CreateExternalTable {
            name: name.clone(),
            columns: vec![],
            file_type: "PARQUET".to_string(),
            location: "foo.parquet".into(),
            table_partition_cols: vec![],
            order_exprs: vec![],
            if_not_exists: false,
            or_replace: true,
            temporary: false,
            unbounded: false,
            options: vec![],
            constraints: vec![],
        });
        expect_parse_ok(sql, expected)?;

        // positive case: column definition allowed in 'partition by' clause
        let sql = "CREATE EXTERNAL TABLE t(c1 int) STORED AS CSV PARTITIONED BY (p1 int) LOCATION 'foo.csv'";
        let expected = Statement::CreateExternalTable(CreateExternalTable {
            name: name.clone(),
            columns: vec![
                make_column_def("c1", DataType::Int(None)),
                make_column_def("p1", DataType::Int(None)),
            ],
            file_type: "CSV".to_string(),
            location: "foo.csv".into(),
            table_partition_cols: vec!["p1".to_string()],
            order_exprs: vec![],
            if_not_exists: false,
            or_replace: false,
            temporary: false,
            unbounded: false,
            options: vec![],
            constraints: vec![],
        });
        expect_parse_ok(sql, expected)?;

        // negative case: mixed column defs and column names in `PARTITIONED BY` clause
        let sql = "CREATE EXTERNAL TABLE t(c1 int) STORED AS CSV PARTITIONED BY (p1 int, c1) LOCATION 'foo.csv'";
        expect_parse_error(
            sql,
            "SQL error: ParserError(\"Expected: a data type name, found: ) at Line: 1, Column: 73\")",
        );

        // negative case: mixed column defs and column names in `PARTITIONED BY` clause
        let sql = "CREATE EXTERNAL TABLE t(c1 int) STORED AS CSV PARTITIONED BY (c1, p1 int) LOCATION 'foo.csv'";
        expect_parse_error(
            sql,
            "SQL error: ParserError(\"Expected: ',' or ')' after partition definition, found: int at Line: 1, Column: 70\")",
        );

        // positive case: additional options (one entry) can be specified
        let sql =
            "CREATE EXTERNAL TABLE t STORED AS x OPTIONS ('k1' 'v1') LOCATION 'blahblah'";
        let expected = Statement::CreateExternalTable(CreateExternalTable {
            name: name.clone(),
            columns: vec![],
            file_type: "X".to_string(),
            location: "blahblah".into(),
            table_partition_cols: vec![],
            order_exprs: vec![],
            if_not_exists: false,
            or_replace: false,
            temporary: false,
            unbounded: false,
            options: vec![("k1".into(), Value::SingleQuotedString("v1".into()))],
            constraints: vec![],
        });
        expect_parse_ok(sql, expected)?;

        // positive case: additional options (multiple entries) can be specified
        let sql = "CREATE EXTERNAL TABLE t STORED AS x OPTIONS ('k1' 'v1', k2 v2) LOCATION 'blahblah'";
        let expected = Statement::CreateExternalTable(CreateExternalTable {
            name: name.clone(),
            columns: vec![],
            file_type: "X".to_string(),
            location: "blahblah".into(),
            table_partition_cols: vec![],
            order_exprs: vec![],
            if_not_exists: false,
            or_replace: false,
            temporary: false,
            unbounded: false,
            options: vec![
                ("k1".into(), Value::SingleQuotedString("v1".into())),
                ("k2".into(), Value::SingleQuotedString("v2".into())),
            ],
            constraints: vec![],
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
        for (sql, (asc, nulls_first)) in sqls.iter().zip(expected.into_iter()) {
            let expected = Statement::CreateExternalTable(CreateExternalTable {
                name: name.clone(),
                columns: vec![make_column_def("c1", DataType::Int(None))],
                file_type: "CSV".to_string(),
                location: "foo.csv".into(),
                table_partition_cols: vec![],
                order_exprs: vec![vec![OrderByExpr {
                    expr: Identifier(Ident {
                        value: "c1".to_owned(),
                        quote_style: None,
                        span: Span::empty(),
                    }),
                    options: OrderByOptions { asc, nulls_first },
                    with_fill: None,
                }]],
                if_not_exists: false,
                or_replace: false,
                temporary: false,
                unbounded: false,
                options: vec![],
                constraints: vec![],
            });
            expect_parse_ok(sql, expected)?;
        }

        // Ordered Col
        let sql = "CREATE EXTERNAL TABLE t(c1 int, c2 int) STORED AS CSV WITH ORDER (c1 ASC, c2 DESC NULLS FIRST) LOCATION 'foo.csv'";
        let display = None;
        let expected = Statement::CreateExternalTable(CreateExternalTable {
            name: name.clone(),
            columns: vec![
                make_column_def("c1", DataType::Int(display)),
                make_column_def("c2", DataType::Int(display)),
            ],
            file_type: "CSV".to_string(),
            location: "foo.csv".into(),
            table_partition_cols: vec![],
            order_exprs: vec![vec![
                OrderByExpr {
                    expr: Identifier(Ident {
                        value: "c1".to_owned(),
                        quote_style: None,
                        span: Span::empty(),
                    }),
                    options: OrderByOptions {
                        asc: Some(true),
                        nulls_first: None,
                    },
                    with_fill: None,
                },
                OrderByExpr {
                    expr: Identifier(Ident {
                        value: "c2".to_owned(),
                        quote_style: None,
                        span: Span::empty(),
                    }),
                    options: OrderByOptions {
                        asc: Some(false),
                        nulls_first: Some(true),
                    },
                    with_fill: None,
                },
            ]],
            if_not_exists: false,
            or_replace: false,
            temporary: false,
            unbounded: false,
            options: vec![],
            constraints: vec![],
        });
        expect_parse_ok(sql, expected)?;

        // Ordered Binary op
        let sql = "CREATE EXTERNAL TABLE t(c1 int, c2 int) STORED AS CSV WITH ORDER (c1 - c2 ASC) LOCATION 'foo.csv'";
        let display = None;
        let expected = Statement::CreateExternalTable(CreateExternalTable {
            name: name.clone(),
            columns: vec![
                make_column_def("c1", DataType::Int(display)),
                make_column_def("c2", DataType::Int(display)),
            ],
            file_type: "CSV".to_string(),
            location: "foo.csv".into(),
            table_partition_cols: vec![],
            order_exprs: vec![vec![OrderByExpr {
                expr: Expr::BinaryOp {
                    left: Box::new(Identifier(Ident {
                        value: "c1".to_owned(),
                        quote_style: None,
                        span: Span::empty(),
                    })),
                    op: BinaryOperator::Minus,
                    right: Box::new(Identifier(Ident {
                        value: "c2".to_owned(),
                        quote_style: None,
                        span: Span::empty(),
                    })),
                },
                options: OrderByOptions {
                    asc: Some(true),
                    nulls_first: None,
                },
                with_fill: None,
            }]],
            if_not_exists: false,
            or_replace: false,
            temporary: false,
            unbounded: false,
            options: vec![],
            constraints: vec![],
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
            name: name.clone(),
            columns: vec![
                make_column_def("c1", DataType::Int(None)),
                make_column_def("c2", DataType::Float(ExactNumberInfo::None)),
            ],
            file_type: "PARQUET".to_string(),
            location: "foo.parquet".into(),
            table_partition_cols: vec!["c1".into()],
            order_exprs: vec![vec![OrderByExpr {
                expr: Expr::BinaryOp {
                    left: Box::new(Identifier(Ident {
                        value: "c1".to_owned(),
                        quote_style: None,
                        span: Span::empty(),
                    })),
                    op: BinaryOperator::Minus,
                    right: Box::new(Identifier(Ident {
                        value: "c2".to_owned(),
                        quote_style: None,
                        span: Span::empty(),
                    })),
                },
                options: OrderByOptions {
                    asc: Some(true),
                    nulls_first: None,
                },
                with_fill: None,
            }]],
            if_not_exists: true,
            or_replace: false,
            temporary: false,
            unbounded: true,
            options: vec![
                (
                    "format.compression".into(),
                    Value::SingleQuotedString("zstd".into()),
                ),
                (
                    "format.delimiter".into(),
                    Value::SingleQuotedString("*".into()),
                ),
                (
                    "ROW_GROUP_SIZE".into(),
                    Value::SingleQuotedString("1024".into()),
                ),
                ("TRUNCATE".into(), Value::SingleQuotedString("NO".into())),
                (
                    "format.has_header".into(),
                    Value::SingleQuotedString("true".into()),
                ),
            ],
            constraints: vec![],
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
            name: name.clone(),
            columns: vec![
                make_column_def("c1", DataType::Int(None)),
                make_column_def("c2", DataType::Float(ExactNumberInfo::None)),
            ],
            file_type: "PARQUET".to_string(),
            location: "foo.parquet".into(),
            table_partition_cols: vec!["c1".into()],
            order_exprs: vec![vec![OrderByExpr {
                expr: Expr::BinaryOp {
                    left: Box::new(Identifier(Ident {
                        value: "c1".to_owned(),
                        quote_style: None,
                        span: Span::empty(),
                    })),
                    op: BinaryOperator::Minus,
                    right: Box::new(Identifier(Ident {
                        value: "c2".to_owned(),
                        quote_style: None,
                        span: Span::empty(),
                    })),
                },
                options: OrderByOptions {
                    asc: Some(true),
                    nulls_first: None,
                },
                with_fill: None,
            }]],
            if_not_exists: false,
            or_replace: true,
            temporary: false,
            unbounded: true,
            options: vec![
                (
                    "format.compression".into(),
                    Value::SingleQuotedString("zstd".into()),
                ),
                (
                    "format.delimiter".into(),
                    Value::SingleQuotedString("*".into()),
                ),
                (
                    "ROW_GROUP_SIZE".into(),
                    Value::SingleQuotedString("1024".into()),
                ),
                ("TRUNCATE".into(), Value::SingleQuotedString("NO".into())),
                (
                    "format.has_header".into(),
                    Value::SingleQuotedString("true".into()),
                ),
            ],
            constraints: vec![],
        });
        expect_parse_ok(sql, expected)?;

        // For error cases, see: `create_external_table.slt`

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
        let dialect = Box::new(SnowflakeDialect);
        let statements = DFParser::parse_sql_with_dialect(sql, dialect.as_ref())?;

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
                analyze,
                verbose,
                format: None,
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

        let query = if let SQLStatement::Query(query) = statement {
            query
        } else {
            panic!("Expected query, got {statement:?}");
        };

        let sql =
            "COPY (SELECT 1) TO bar STORED AS CSV OPTIONS ('format.has_header' 'true')";
        let expected = Statement::CopyTo(CopyToStatement {
            source: CopyToSource::Query(query),
            target: "bar".to_string(),
            partitioned_by: vec![],
            stored_as: Some("CSV".to_owned()),
            options: vec![(
                "format.has_header".into(),
                Value::SingleQuotedString("true".into()),
            )],
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
            options: vec![(
                "row_group_size".to_string(),
                Value::SingleQuotedString("55".to_string()),
            )],
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
            options: vec![(
                "row_group_size".to_string(),
                Value::SingleQuotedString("55".to_string()),
            )],
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
                Value::Number("55".to_string(), false),
            ),
            (
                "format.compression".to_string(),
                Value::SingleQuotedString("snappy".to_string()),
            ),
            (
                "execution.keep_partition_by_columns".to_string(),
                Value::SingleQuotedString("true".to_string()),
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

    fn object_name(name: &str) -> CopyToSource {
        CopyToSource::Relation(ObjectName::from(vec![Ident::new(name)]))
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
    fn one_statement_parses_to(sql: &str, canonical: &str) -> Statement {
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
    fn verified_stmt(sql: &str) -> Statement {
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
                    let name = ObjectName::from(vec![Ident::from("t")]);
                    let display = None;
                    Statement::CreateExternalTable(CreateExternalTable {
                        name: name.clone(),
                        columns: vec![make_column_def("c1", DataType::Int(display))],
                        file_type: "CSV".to_string(),
                        location: "foo.csv".into(),
                        table_partition_cols: vec![],
                        order_exprs: vec![],
                        if_not_exists: false,
                        or_replace: false,
                        temporary: false,
                        unbounded: false,
                        options: vec![],
                        constraints: vec![],
                    })
                },
                {
                    let name = ObjectName::from(vec![Ident::from("var")]);
                    Statement::Reset(ResetStatement::Variable(name))
                }
            ]
        );
    }

    fn expect_parse_expr_ok(sql: &str, expected: ExprWithAlias) {
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
                expr: Expr::Value(ValueWithSpan::from(Value::Number(
                    "1234".to_string(),
                    false,
                ))),
                alias: None,
            },
        )
    }

    #[test]
    fn literal_with_alias() {
        expect_parse_expr_ok(
            "1234 as foo",
            ExprWithAlias {
                expr: Expr::Value(ValueWithSpan::from(Value::Number(
                    "1234".to_string(),
                    false,
                ))),
                alias: Some(Ident::from("foo")),
            },
        )
    }

    #[test]
    fn literal_with_alias_and_trailing_tokens() {
        expect_parse_expr_error(
            "1234 as foo.bar",
            "Expected: end of expression, found: .",
        )
    }

    #[test]
    fn literal_with_alias_and_trailing_whitespace() {
        expect_parse_expr_ok(
            "1234 as foo   ",
            ExprWithAlias {
                expr: Expr::Value(ValueWithSpan::from(Value::Number(
                    "1234".to_string(),
                    false,
                ))),
                alias: Some(Ident::from("foo")),
            },
        )
    }

    #[test]
    fn literal_with_alias_and_trailing_whitespace_and_token() {
        expect_parse_expr_error(
            "1234 as foo    bar",
            "Expected: end of expression, found: bar",
        )
    }
}
