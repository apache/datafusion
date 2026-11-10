// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! The syntax highlighter.

use std::{
    borrow::Cow::{self, Borrowed},
    fmt::Display,
};

use datafusion::sql::dialect::DataFusion;
use datafusion::sql::parser::{
    DEFAULT_DIALECT, DialectKind, SqlDialect, dialect_from_str,
};
use datafusion::sql::sqlparser::dialect::{
    BigQuery, ClickHouse, Core, Databricks, Dialect, DuckDb, Hive, MsSql, MySql, Oracle,
    Postgres, Redshift, Snowflake, Spark, Sqlite, Teradata,
};
use datafusion::sql::sqlparser::lexer::{TokenKind, tokenize};
use datafusion_common::config;
use rustyline::highlight::{CmdKind, Highlighter};

/// The syntax highlighter.
#[derive(Debug)]
pub struct SyntaxHighlighter {
    dialect: SqlDialect,
}

impl SyntaxHighlighter {
    pub fn new(dialect: &config::Dialect) -> Self {
        let dialect = dialect_from_str(dialect.as_ref()).unwrap_or(DEFAULT_DIALECT);
        Self { dialect }
    }
}

pub struct NoSyntaxHighlighter {}

impl Highlighter for NoSyntaxHighlighter {}

impl Highlighter for SyntaxHighlighter {
    fn highlight<'l>(&self, line: &'l str, _: usize) -> Cow<'l, str> {
        let highlighted = match self.dialect {
            SqlDialect::DataFusion => highlight::<DataFusion>(line),
            SqlDialect::Builtin(kind) => match kind {
                DialectKind::Core => highlight::<Core>(line),
                DialectKind::MySql => highlight::<MySql>(line),
                DialectKind::Postgres => highlight::<Postgres>(line),
                DialectKind::Redshift => highlight::<Redshift>(line),
                DialectKind::Snowflake => highlight::<Snowflake>(line),
                DialectKind::Spark => highlight::<Spark>(line),
                DialectKind::Databricks => highlight::<Databricks>(line),
                DialectKind::Hive => highlight::<Hive>(line),
                DialectKind::BigQuery => highlight::<BigQuery>(line),
                DialectKind::ClickHouse => highlight::<ClickHouse>(line),
                DialectKind::DuckDb => highlight::<DuckDb>(line),
                DialectKind::MsSql => highlight::<MsSql>(line),
                DialectKind::Oracle => highlight::<Oracle>(line),
                DialectKind::Sqlite => highlight::<Sqlite>(line),
                DialectKind::Teradata => highlight::<Teradata>(line),
            },
        };
        highlighted.map_or(Borrowed(line), Cow::Owned)
    }

    fn highlight_char(&self, line: &str, _pos: usize, _cmd: CmdKind) -> bool {
        !line.is_empty()
    }
}

/// Colors the keywords and single-quoted strings of `line`, as `D` lexes it.
/// Returns `None` if `line` does not lex.
fn highlight<D: Dialect>(line: &str) -> Option<String> {
    let tokens = tokenize::<D>(line).ok()?;
    let mut out_line = String::with_capacity(line.len());
    let mut end = 0;
    for token in tokens {
        let start = token.span.start as usize;
        // Copy the whitespace and comments between tokens as they are.
        out_line.push_str(&line[end..start]);
        end = token.span.end as usize;
        match token.kind {
            TokenKind::Word if D::is_reserved(token.text) || is_keyword(token.text) => {
                out_line.push_str(&Color::red(token.text));
            }
            TokenKind::String if token.text.starts_with('\'') => {
                out_line.push_str(&Color::green(token.text));
            }
            _ => out_line.push_str(token.text),
        }
    }
    out_line.push_str(&line[end..]);
    Some(out_line)
}

/// True if `word` is a common SQL keyword. The parser matches keywords by their
/// text and has no list of them, so this list is the highlighter's own.
fn is_keyword(word: &str) -> bool {
    let upper = word.to_ascii_uppercase();
    KEYWORDS.binary_search(&upper.as_str()).is_ok()
}

/// Keywords to highlight beyond each dialect's reserved words, sorted.
#[rustfmt::skip]
const KEYWORDS: &[&str] = &[
    "ADD", "ALL", "ALTER", "ANALYZE", "AND", "ANTI", "ANY", "ARRAY", "AS", "ASC",
    "ASOF", "BEGIN", "BETWEEN", "BIGINT", "BOOLEAN", "BY", "CASCADE", "CASE", "CAST",
    "CATALOG", "CHAR", "COLUMN", "COLUMNS", "COMMIT", "CONSTRAINT", "COPY", "CREATE",
    "CROSS", "CUBE", "CURRENT", "DATABASE", "DATE", "DECIMAL", "DEFAULT", "DELETE",
    "DESC", "DESCRIBE", "DISTINCT", "DOUBLE", "DROP", "ELSE", "END", "ESCAPE", "EXCEPT",
    "EXCLUDE", "EXISTS", "EXPLAIN", "EXTERNAL", "EXTRACT", "FALSE", "FILTER", "FIRST",
    "FLOAT", "FOLLOWING", "FOR", "FORMAT", "FROM", "FULL", "FUNCTION", "FUNCTIONS",
    "GROUP", "GROUPING", "HAVING", "IF", "ILIKE", "IN", "INNER", "INSERT", "INT",
    "INTEGER", "INTERSECT", "INTERVAL", "INTO", "IS", "JOIN", "KEY", "LAST", "LATERAL",
    "LEFT", "LIKE", "LIMIT", "LOCATION", "MAP", "MATCH_CONDITION", "NATURAL", "NOT",
    "NULL", "NULLS", "OFFSET", "ON", "OPTIONS", "OR", "ORDER", "OUTER", "OVER",
    "OVERWRITE", "PARTITION", "PARTITIONED", "PRECEDING", "PREPARE", "PRIMARY",
    "QUALIFY", "RANGE", "RECURSIVE", "REPLACE", "RESET", "RIGHT", "ROLLBACK", "ROLLUP",
    "ROW", "ROWS", "SCHEMA", "SELECT", "SEMI", "SET", "SETS", "SHOW", "SMALLINT",
    "STORED", "STRUCT", "TABLE", "TABLES", "TEMPORARY", "THEN", "TIME", "TIMESTAMP",
    "TINYINT", "TO", "TRANSACTION", "TRUE", "TRUNCATE", "UNBOUNDED", "UNION", "UNIQUE",
    "UNNEST", "UPDATE", "USING", "VALUES", "VARCHAR", "VIEW", "WHEN", "WHERE", "WINDOW",
    "WITH",
];

/// Convenient utility to return strings with [ANSI color](https://gist.github.com/JBlond/2fea43a3049b38287e5e9cefc87b2124).
pub(crate) struct Color {}

impl Color {
    pub(crate) fn green(s: impl Display) -> String {
        format!("\x1b[92m{s}\x1b[0m")
    }

    pub(crate) fn red(s: impl Display) -> String {
        format!("\x1b[91m{s}\x1b[0m")
    }

    pub(crate) fn gray(s: impl Display) -> String {
        format!("\x1b[90m{s}\x1b[0m")
    }
}

#[cfg(test)]
mod tests {
    use super::config::Dialect;
    use super::{KEYWORDS, SyntaxHighlighter};
    use rustyline::highlight::Highlighter;

    #[test]
    fn highlighter_valid() {
        let s = "SElect col_a from tab_1;";
        let highlighter = SyntaxHighlighter::new(&Dialect::Generic);
        let out = highlighter.highlight(s, s.len());
        assert_eq!(
            "\u{1b}[91mSElect\u{1b}[0m col_a \u{1b}[91mfrom\u{1b}[0m tab_1;",
            out
        );
    }

    #[test]
    fn highlighter_valid_with_new_line() {
        let s = "SElect col_a from tab_1\n WHERE col_b = 'なにか';";
        let highlighter = SyntaxHighlighter::new(&Dialect::Generic);
        let out = highlighter.highlight(s, s.len());
        assert_eq!(
            "\u{1b}[91mSElect\u{1b}[0m col_a \u{1b}[91mfrom\u{1b}[0m tab_1\n \u{1b}[91mWHERE\u{1b}[0m col_b = \u{1b}[92m'なにか'\u{1b}[0m;",
            out
        );
    }

    #[test]
    fn highlighter_invalid() {
        let s = "SElect col_a from tab_1 WHERE col_b = ';";
        let highlighter = SyntaxHighlighter::new(&Dialect::Generic);
        let out = highlighter.highlight(s, s.len());
        assert_eq!("SElect col_a from tab_1 WHERE col_b = ';", out);
    }

    #[test]
    fn highlighter_keeps_comments() {
        let s = "select 1 -- one\n /* two */ ;";
        let highlighter = SyntaxHighlighter::new(&Dialect::Generic);
        let out = highlighter.highlight(s, s.len());
        assert_eq!("\u{1b}[91mselect\u{1b}[0m 1 -- one\n /* two */ ;", out);
    }

    #[test]
    fn keywords_are_sorted() {
        assert!(KEYWORDS.windows(2).all(|pair| pair[0] < pair[1]));
    }
}
