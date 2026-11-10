// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! DataFusion's SQL dialect.
//!
//! DataFusion's SQL is DuckDB's, plus:
//!
//! - `FROM VALUES (...)` without parentheses, as in Spark
//! - `FROM 'file path'`, which reads the file as a table
//! - `LEFT SEMI JOIN` and `LEFT ANTI JOIN`, as in Spark, and `RIGHT SEMI JOIN`
//!   and `RIGHT ANTI JOIN`
//! - `ASOF JOIN ... MATCH_CONDITION (...)`, as in Snowflake
//! - `STRUCT<...>`, `ARRAY<...>` and `MAP<...>` types, as in Spark
//! - `UNSIGNED` after an integer type, as in MySQL
//! - `DECIMAL(p, -s)`, with a negative scale
//! - `* EXCEPT (columns)`, as in BigQuery
//! - `STRUCT(value AS name, ...)`, as in BigQuery, where a name can be a string
//! - `|>` pipe operators, as in BigQuery
//! - `IGNORE NULLS` and `RESPECT NULLS` inside a call's parentheses
//! - `LIMIT offset, count`, `VALUES ROW(...)`, `XOR`, `<=>` and backtick-quoted
//!   names, as in MySQL
//! - `SELECT ... INTO table` and `CREATE FUNCTION`, as in PostgreSQL
//! - `DISTRIBUTE BY`, as in Spark
//! - `EXTRACT("field" FROM ...)` with a quoted field
//! - `SHOW name.name...` for a setting, optionally followed by `VERBOSE`
//! - `^` as bitwise XOR, not exponentiation, and `<<` and `>>` ranked with `|`
//! - `value:key.key[index]` paths, as in Snowflake, which can start with an
//!   index: `value:['key']`
//! - `SET TIME ZONE = value`
//! - `CREATE TABLE t` with no columns, which makes an empty table
//! - `DESCRIBE 'file path'`
//! - `FROM (t)`, a table in parentheses with no join
//! - `SELECT FROM t`, with no columns, as in PostgreSQL
//! - `STRUCT<type>`, whose fields need no names, as in BigQuery
//! - a window frame offset written as a string, which is an interval:
//!   `RANGE BETWEEN '1' DAY PRECEDING AND CURRENT ROW`
//!
//! Every rule here either delegates to a built-in dialect's rule or is one of
//! these additions.

use sqlparser::PResult;
use sqlparser::ast::{
    ArrayType, BinaryOp, ColumnOption, CreateFlags, CreateSchema, CreateTable, Cte,
    DataType, DataTypeSuffix, ExplainOption, ExplainTarget, Expr, ExprKind,
    ExprWithAlias, FunctionArg, Ident, InsertSource, Join, JoinKind, Limit,
    NullTreatment, ObjectName, ObjectType, OrderByItem, PipeOperator, Query,
    QueryDistribution, Select, SelectInto, SelectItem, SelectItemKind, SetExpr,
    SetQuantifier, SetStatement, Show, Statement, StatementKind, StringLiteral,
    TableFactor, TableRef, TableWithJoins, WildcardOptions,
};
use sqlparser::dialect::bigquery::{BigQueryExpr, BigQueryStruct};
use sqlparser::dialect::builtin::{BuiltinExpr, BuiltinSelect, BuiltinStatement};
use sqlparser::dialect::snowflake::{PathElement, SnowflakeExpr};
use sqlparser::dialect::{
    Builtin, Dialect, bigquery, duckdb, mysql, postgres, snowflake, spark, sqlite,
};
use sqlparser::grammar::{self, prec};
use sqlparser::lexer;
use sqlparser::lexer::{LexResult, Lexeme, Token, TokenKind};
use sqlparser::parser::{InfixRule, Parser};
use std::borrow::Cow;

/// DataFusion's SQL dialect.
///
/// Each hook delegates to the rule DuckDB uses unless DataFusion adds to it.
/// See the [module documentation](self) for the additions.
#[derive(Debug, Clone, Copy, Default)]
pub struct DataFusion;

impl Dialect for DataFusion {
    type Ext = Builtin;

    fn parse_create_table_query<'a>(
        p: &mut Parser<'a, Self>,
    ) -> PResult<Query<'a, Builtin>> {
        grammar::parse_table_or_query(p)
    }

    fn parse_create_extension<'a>(
        p: &mut Parser<'a, Self>,
        flags: CreateFlags,
    ) -> PResult<Option<Statement<'a, Builtin>>> {
        duckdb::parse_create_type(p, flags)
    }

    fn parse_update_from_after_set<'a>(
        p: &mut Parser<'a, Self>,
    ) -> PResult<Vec<TableWithJoins<'a, Builtin>>> {
        grammar::parse_optional_update_from(p)
    }

    fn parse_delete_using<'a>(
        p: &mut Parser<'a, Self>,
    ) -> PResult<Vec<TableWithJoins<'a, Builtin>>> {
        grammar::parse_optional_delete_using(p)
    }

    fn eat_limit_all(p: &mut Parser<'_, Self>) -> bool {
        p.eat_keyword("ALL")
    }

    fn parse_statement_after_with<'a>(
        p: &mut Parser<'a, Self>,
    ) -> PResult<Option<Statement<'a, Builtin>>> {
        grammar::parse_dml_after_with(p, true, true)
    }

    fn parse_insert_source<'a>(
        p: &mut Parser<'a, Self>,
    ) -> PResult<InsertSource<'a, Builtin>> {
        grammar::parse_insert_source_with_default(p)
    }

    /// `INSERT OVERWRITE [TABLE] t ...`, as in Spark, and `INSERT OR REPLACE`,
    /// as in SQLite. Other inserts keep the core rule and so this dialect's
    /// hooks, such as `DEFAULT VALUES`.
    fn parse_insert_statement<'a>(
        p: &mut Parser<'a, Self>,
    ) -> PResult<Statement<'a, Builtin>> {
        let after_insert = p.peek_nth(1);
        if after_insert.is_word("OVERWRITE") {
            return spark::parse_insert(p);
        }
        if after_insert.is_word("OR") {
            return sqlite::parse_insert(p);
        }
        Self::parse_insert(p).map(|insert| StatementKind::Insert(Box::new(insert)).into())
    }

    fn parse_drop_object_type<'a>(p: &mut Parser<'a, Self>) -> Option<ObjectType<'a>> {
        grammar::eat_drop_object_type(p, &["TYPE"])
    }

    fn parse_group_by_item<'a>(p: &mut Parser<'a, Self>) -> PResult<Expr<'a, Builtin>> {
        grammar::parse_grouping_item_or_empty_set(p)
    }

    fn parse_create_schema<'a>(
        p: &mut Parser<'a, Self>,
        flags: CreateFlags,
    ) -> PResult<CreateSchema<'a>> {
        if flags.temporary {
            return Err(p.expected("TABLE or VIEW"));
        }
        grammar::parse_create_schema_body(p, flags)
    }

    #[inline]
    fn lex_token(rest: &str) -> Option<LexResult> {
        lex_token(rest)
    }

    /// DuckDB's reserved words, except that `LEFT` and `RIGHT` can name a
    /// table or column, as in `FROM left AS t1`. They are still joins after a
    /// table, through [`Self::is_reserved_for_alias`].
    #[inline]
    fn is_reserved(word: &str) -> bool {
        duckdb::is_reserved(word) && !is_join_side(word)
    }

    /// The standard functions written without parentheses, such as
    /// `CURRENT_DATE`, plus `USER` and `CURRENT_CATALOG`.
    #[inline]
    fn is_niladic_function(word: &str) -> bool {
        grammar::is_niladic_function(word)
            || ["USER", "CURRENT_CATALOG"]
                .iter()
                .any(|name| word.eq_ignore_ascii_case(name))
    }

    #[inline]
    fn is_typed_string_type(name: &str) -> bool {
        <sqlparser::dialect::DuckDb as Dialect>::is_typed_string_type(name)
    }

    #[inline]
    fn is_reserved_for_alias(word: &str) -> bool {
        // Spark's `DISTRIBUTE BY`, `CLUSTER BY` and `SORT BY` follow a table,
        // as does `MATCH_CONDITION` in `ASOF JOIN (a JOIN b ON ...) MATCH_CONDITION`.
        duckdb::is_reserved_for_alias(word)
            || is_join_side(word)
            || ["DISTRIBUTE", "CLUSTER", "SORT", "MATCH_CONDITION"]
                .iter()
                .any(|clause| word.eq_ignore_ascii_case(clause))
    }

    fn parse_create_table<'a>(
        p: &mut Parser<'a, Self>,
        flags: CreateFlags,
    ) -> PResult<CreateTable<'a, Builtin>> {
        let if_not_exists = grammar::parse_if_not_exists(p)?;
        let name = grammar::parse_object_name(p)?;
        // `CREATE TABLE t`, with no columns, makes an empty table.
        if p.at_eof() || p.at_op(";") {
            return Ok(CreateTable {
                or_replace: flags.or_replace,
                temporary: flags.temporary,
                name,
                if_not_exists,
                columns: Vec::new(),
                constraints: Vec::new(),
                query: None,
            });
        }
        grammar::parse_create_table_after_name(p, flags, if_not_exists, name)
    }

    fn parse_explain_target<'a>(
        p: &mut Parser<'a, Self>,
    ) -> PResult<ExplainTarget<'a, Builtin>> {
        // `DESCRIBE 'file path'`
        if p.peek().kind == TokenKind::String {
            let name = parse_name_or_string(p)?;
            return Ok(ExplainTarget::Table {
                table_keyword: false,
                name: ObjectName { parts: vec![name] },
            });
        }
        grammar::parse_explain_target(p)
    }

    fn check_nested_join<'a>(
        _p: &Parser<'a, Self>,
        _table: &TableWithJoins<'a, Builtin>,
    ) -> PResult<()> {
        // `FROM (t)`, with no join inside the parentheses.
        Ok(())
    }

    fn at_query_values(p: &Parser<'_, Self>, n: usize) -> bool {
        mysql::at_row_values(p, n) || grammar::at_query_values(p, n)
    }

    /// DuckDB's statements, plus `REPLACE INTO t ...`, as in MySQL and SQLite.
    fn parse_statement<'a>(p: &mut Parser<'a, Self>) -> PResult<Statement<'a, Builtin>> {
        if p.at_keyword("REPLACE") && p.peek_nth(1).is_word("INTO") {
            return sqlite::parse_insert(p);
        }
        duckdb::parse_statement(p)
    }

    fn parse_create<'a>(p: &mut Parser<'a, Self>) -> PResult<Statement<'a, Builtin>> {
        parse_create(p)
    }

    fn parse_prefix<'a>(p: &mut Parser<'a, Self>) -> PResult<Expr<'a, Builtin>> {
        if p.at_keyword("STRUCT") && p.peek_nth(1).is_op("(") {
            return parse_struct(p);
        }
        duckdb::parse_prefix(p)
    }

    fn infix_rule<'a>(p: &Parser<'a, Self>) -> Option<InfixRule<'a, Self>> {
        // Bitwise operators rank as DataFusion has always ranked them: `&`
        // above `^`, which is XOR, above `|`, `<<` and `>>`.
        if p.at_op("^") {
            return Some(InfixRule::binary(prec::BIT_XOR, BinaryOp::BitXor));
        }
        if p.at_op("<<") {
            return Some(InfixRule::binary(prec::BIT_OR, BinaryOp::ShiftLeft));
        }
        if p.at_op(">>") {
            return Some(InfixRule::binary(prec::BIT_OR, BinaryOp::ShiftRight));
        }
        if snowflake::at_path(p) && path_follows(p) {
            return Some(InfixRule::custom(prec::POSTFIX, parse_path));
        }
        if p.at_keyword("XOR") {
            // Between `AND` and `OR`, as in MySQL.
            return Some(InfixRule::binary(15, BinaryOp::Named("XOR".into())));
        }
        if p.at_op("<=>") {
            return Some(InfixRule::binary(
                prec::COMPARE,
                BinaryOp::Named("<=>".into()),
            ));
        }
        duckdb::infix_rule(p)
    }

    fn parse_show<'a>(p: &mut Parser<'a, Self>) -> PResult<Show<'a, Builtin>> {
        parse_show(p)
    }

    fn parse_set<'a>(p: &mut Parser<'a, Self>) -> PResult<SetStatement<'a, Builtin>> {
        // `SET TIME ZONE = value`
        let at_time_zone = p.peek_nth(1).is_word("TIME") && p.peek_nth(2).is_word("ZONE");
        if p.at_keyword("SET") && at_time_zone && p.peek_nth(3).is_op("=") {
            for _ in 0..4 {
                p.advance();
            }
            let value = p.parse_expr()?;
            return Ok(SetStatement::TimeZone { scope: None, value });
        }
        grammar::parse_set(p)
    }

    fn parse_pipe_operators<'a>(
        p: &mut Parser<'a, Self>,
    ) -> PResult<Vec<PipeOperator<'a, Builtin>>> {
        grammar::parse_pipe_operators(p)
    }

    fn parse_select_into<'a>(
        p: &mut Parser<'a, Self>,
    ) -> PResult<Option<Box<SelectInto<'a, Builtin>>>> {
        grammar::parse_select_into_table(p)
    }

    fn parse_limit<'a>(p: &mut Parser<'a, Self>) -> PResult<Limit<'a, Builtin>> {
        mysql::parse_limit(p)
    }

    fn parse_query_values<'a>(p: &mut Parser<'a, Self>) -> PResult<SetExpr<'a, Builtin>> {
        mysql::parse_query_values(p)
    }

    fn parse_function_arg_null_treatment(
        p: &mut Parser<'_, Self>,
    ) -> PResult<Option<NullTreatment>> {
        Ok(grammar::parse_null_treatment(p))
    }

    fn parse_query_distribution<'a>(
        p: &mut Parser<'a, Self>,
    ) -> PResult<Option<Box<QueryDistribution<'a, Builtin>>>> {
        spark::parse_query_distribution(p)
    }

    fn parse_data_type<'a>(p: &mut Parser<'a, Self>) -> PResult<DataType<'a>> {
        parse_data_type(p)
    }

    fn parse_table_factor<'a>(
        p: &mut Parser<'a, Self>,
    ) -> PResult<TableFactor<'a, Builtin>> {
        parse_table_factor(p)
    }

    fn parse_select<'a>(p: &mut Parser<'a, Self>) -> PResult<Select<'a, Builtin>> {
        duckdb::parse_select(p)
    }

    fn at_from_first(p: &Parser<'_, Self>, n: usize) -> bool {
        p.peek_nth(n).is_word("FROM")
    }

    fn parse_select_item<'a>(
        p: &mut Parser<'a, Self>,
    ) -> PResult<SelectItem<'a, Builtin>> {
        parse_select_item(p)
    }

    fn parse_select_item_alias<'a>(
        p: &mut Parser<'a, Self>,
        expr: Expr<'a, Builtin>,
    ) -> PResult<SelectItem<'a, Builtin>> {
        duckdb::parse_select_item_alias(p, expr)
    }

    fn parse_projection<'a>(
        p: &mut Parser<'a, Self>,
    ) -> PResult<Vec<SelectItem<'a, Builtin>>> {
        // `SELECT FROM t`, which selects no columns, as in PostgreSQL.
        if p.at_keyword("FROM") {
            return Ok(Vec::new());
        }
        grammar::parse_list_with_trailing_comma(p, Parser::parse_select_item)
    }

    fn parse_table_ref<'a>(p: &mut Parser<'a, Self>) -> PResult<TableRef<'a, Builtin>> {
        duckdb::parse_table_ref(p)
    }

    fn parse_join<'a>(p: &mut Parser<'a, Self>) -> PResult<Option<Join<'a, Builtin>>> {
        parse_join(p)
    }

    fn parse_cte<'a>(p: &mut Parser<'a, Self>) -> PResult<Cte<'a, Builtin>> {
        duckdb::parse_cte(p)
    }

    fn string_literal(token: Token<'_>) -> StringLiteral<'_> {
        postgres::string_literal(token, false)
    }

    fn parse_set_quantifier(p: &mut Parser<'_, Self>) -> (SetQuantifier, bool) {
        duckdb::parse_set_quantifier(p)
    }

    fn parse_select_modifiers<'a>(
        p: &mut Parser<'a, Self>,
    ) -> PResult<BuiltinSelect<'a>> {
        postgres::parse_distinct_on(p)
    }

    fn parse_query_order_by<'a>(
        p: &mut Parser<'a, Self>,
    ) -> PResult<Vec<OrderByItem<'a, Builtin>>> {
        duckdb::parse_query_order_by(p)
    }

    fn parse_function_args<'a>(
        p: &mut Parser<'a, Self>,
        quantifier: SetQuantifier,
    ) -> PResult<Vec<FunctionArg<'a, Builtin>>> {
        duckdb::parse_function_args(p, quantifier)
    }

    fn parse_function_arg<'a>(
        p: &mut Parser<'a, Self>,
    ) -> PResult<FunctionArg<'a, Builtin>> {
        duckdb::parse_function_arg(p)
    }

    fn parse_frame_offset<'a>(p: &mut Parser<'a, Self>) -> PResult<Expr<'a, Builtin>> {
        // A string is an interval, which can be followed by its unit:
        // `RANGE BETWEEN '1' DAY PRECEDING AND CURRENT ROW`.
        if p.peek().kind == TokenKind::String {
            return grammar::parse_interval(p, false);
        }
        p.parse_expr()
    }

    fn parse_extract_field<'a>(p: &mut Parser<'a, Self>) -> PResult<&'a str> {
        // A quoted field, such as `EXTRACT("year" FROM ts)`.
        let token = p.peek();
        let field = &token.text[1..token.text.len().saturating_sub(1)];
        let is_name = !field.is_empty()
            && field
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'_');
        if token.kind == TokenKind::QuotedIdent && token.text.starts_with('"') && is_name
        {
            p.advance();
            return Ok(field);
        }
        duckdb::parse_extract_field(p)
    }

    fn parse_between_low<'a>(p: &mut Parser<'a, Self>) -> PResult<Expr<'a, Builtin>> {
        let low = grammar::parse_between_low(p)?;
        spark::parse_between_bound(p, low)
    }

    fn parse_qualify<'a>(p: &mut Parser<'a, Self>) -> PResult<Option<Expr<'a, Builtin>>> {
        snowflake::parse_qualify(p)
    }

    fn eat_group_by_all(p: &mut Parser<'_, Self>) -> bool {
        p.eat_keyword("ALL")
    }

    fn parse_returning<'a>(
        p: &mut Parser<'a, Self>,
    ) -> PResult<Vec<SelectItem<'a, Builtin>>> {
        postgres::parse_returning(p)
    }

    fn parse_interval<'a>(p: &mut Parser<'a, Self>) -> PResult<Expr<'a, Builtin>> {
        grammar::parse_interval(p, false)
    }

    fn parse_explain_options<'a>(
        p: &mut Parser<'a, Self>,
    ) -> PResult<Vec<ExplainOption<'a>>> {
        grammar::parse_explain_option_list(p)
    }

    fn parse_add_column_if_not_exists(p: &mut Parser<'_, Self>) -> PResult<bool> {
        grammar::parse_if_not_exists(p)
    }

    fn eat_drop_temporary(p: &mut Parser<'_, Self>) -> bool {
        p.eat_keyword("TEMPORARY")
    }

    fn parse_column_option<'a>(
        p: &mut Parser<'a, Self>,
    ) -> PResult<Option<ColumnOption<'a, Builtin>>> {
        duckdb::parse_column_option(p)
    }
}

/// `LEFT` or `RIGHT`, which start a join after a table.
fn is_join_side(word: &str) -> bool {
    word.eq_ignore_ascii_case("LEFT") || word.eq_ignore_ascii_case("RIGHT")
}

/// A token: DuckDB's, plus `<=>` and backtick-quoted names, as in MySQL, the
/// `|>` of a pipe, and `@name` and `@@name` variables.
pub fn lex_token(rest: &str) -> Option<LexResult> {
    if let Some(variable) = lex_variable(rest) {
        return Some(Ok(variable));
    }
    if rest.starts_with("<=>") || rest.starts_with("|>") {
        let len = if rest.starts_with("|>") { 2 } else { 3 };
        return Some(Ok(Lexeme {
            kind: TokenKind::Op,
            len,
        }));
    }
    if rest.starts_with('`') {
        return Some(lexer::quoted(rest, TokenKind::QuotedIdent));
    }
    duckdb::lex_token(rest)
}

/// Lexes `@name` or `@@name` as a word, which the planner reads as a variable.
/// `@` and `@@` followed by anything else remain operators.
fn lex_variable(rest: &str) -> Option<Lexeme> {
    let bytes = rest.as_bytes();
    let sigils = bytes.iter().take(2).take_while(|&&b| b == b'@').count();
    let starts_name = |b: &u8| b.is_ascii_alphabetic() || *b == b'_' || !b.is_ascii();
    if sigils == 0 || !bytes.get(sigils).is_some_and(starts_name) {
        return None;
    }
    let name = bytes[sigils..]
        .iter()
        .take_while(|&&b| b.is_ascii_alphanumeric() || b == b'_' || !b.is_ascii())
        .count();
    Some(Lexeme {
        kind: TokenKind::Word,
        len: sigils + name,
    })
}

/// `STRUCT(value [AS name], ...)`, in which a name can also be a string.
pub fn parse_struct<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<Expr<'a, Builtin>> {
    p.advance();
    p.expect_op("(")?;
    let values = if p.at_op(")") {
        Vec::new()
    } else {
        p.parse_comma_separated(|p| {
            let expr = p.parse_expr()?;
            let alias = if p.eat_keyword("AS") {
                Some(parse_name_or_string(p)?)
            } else {
                None
            };
            Ok(ExprWithAlias { expr, alias })
        })?
    };
    p.expect_op(")")?;
    let value = BigQueryStruct { ty: None, values };
    Ok(
        ExprKind::Ext(BuiltinExpr::BigQuery(BigQueryExpr::Struct(Box::new(value))))
            .into(),
    )
}

/// A name, or a string used as one.
fn parse_name_or_string<'a, D: Dialect>(p: &mut Parser<'a, D>) -> PResult<Ident<'a>> {
    let token = p.peek();
    if token.kind != TokenKind::String {
        return p.parse_ident();
    }
    p.advance();
    Ok(Ident {
        text: Cow::Borrowed(&token.text[1..token.text.len() - 1]),
        quote: Some('\''),
        span: token.span,
    })
}

/// A select item: DuckDB's, plus `* EXCEPT (columns)` and `t.* EXCEPT (columns)`.
pub fn parse_select_item<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<SelectItem<'a, Builtin>> {
    let item = duckdb::parse_select_item(p)?;
    let qualifier = match &item.kind {
        SelectItemKind::Wildcard => None,
        SelectItemKind::QualifiedWildcard(name) => Some(name.clone()),
        _ => return Ok(item),
    };
    let query_follows = ["SELECT", "WITH", "VALUES"]
        .iter()
        .any(|word| p.peek_nth(2).is_word(word));
    if !(p.at_keyword("EXCEPT") && p.peek_nth(1).is_op("(")) || query_follows {
        return Ok(item);
    }
    p.advance();
    let exclude = grammar::parse_ident_list(p)?;
    let replace = if p.at_keyword("REPLACE") && p.peek_nth(1).is_op("(") {
        grammar::parse_wildcard_replace(p)?
    } else {
        Vec::new()
    };
    let options = WildcardOptions {
        ilike: None,
        exclude,
        exclude_parens: true,
        except_keyword: true,
        replace,
        rename: Vec::new(),
        rename_parens: false,
    };
    Ok(SelectItemKind::WildcardWithOptions {
        qualifier,
        options: Box::new(options),
    }
    .into())
}

/// A table: Spark's, which can be `VALUES (...)` without parentheses, plus a
/// file path written as a string: `FROM 'data.parquet'`.
pub fn parse_table_factor<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<TableFactor<'a, Builtin>> {
    if p.peek().kind == TokenKind::String {
        let name = parse_name_or_string(p)?;
        return Ok(TableFactor::Named(ObjectName { parts: vec![name] }));
    }
    spark::parse_table_factor(p)
}

/// `SHOW`, which can name a setting such as `datafusion.execution.batch_size`,
/// optionally followed by `VERBOSE`. Its words are the shown object.
pub fn parse_show<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<Show<'a, Builtin>> {
    if !(p.at_keyword("SHOW")
        && p.peek_nth(1).kind == TokenKind::Word
        && p.peek_nth(2).is_op("."))
    {
        return grammar::parse_show(p);
    }
    p.advance();
    let mut object = vec![Cow::Borrowed(p.advance().text)];
    while p.eat_op(".") {
        if !matches!(p.peek().kind, TokenKind::Word | TokenKind::QuotedIdent) {
            return Err(p.expected("a setting name"));
        }
        object.push(Cow::Borrowed(p.advance().text));
    }
    if p.at_keyword("VERBOSE") {
        object.push(Cow::Borrowed(p.advance().text));
    }
    Ok(Show {
        modifiers: Vec::new(),
        object,
        name: None,
        clauses: Vec::new(),
    })
}

/// `DECIMAL(precision, -scale)` or `NUMERIC(precision, -scale)`, with a
/// negative scale. Returns `None`, consuming nothing, at anything else.
fn parse_negative_scale_decimal<'a, D: Dialect>(
    p: &mut Parser<'a, D>,
) -> PResult<Option<DataType<'a>>> {
    let shape = (p.at_keyword("DECIMAL") || p.at_keyword("NUMERIC"))
        && p.peek_nth(1).is_op("(")
        && p.peek_nth(2).kind == TokenKind::Number
        && p.peek_nth(3).is_op(",")
        && p.peek_nth(4).is_op("-")
        && p.peek_nth(5).kind == TokenKind::Number
        && p.peek_nth(6).is_op(")");
    if !shape {
        return Ok(None);
    }
    let name = p.parse_ident()?;
    p.advance();
    let precision = p.advance().text;
    p.advance();
    p.advance();
    let scale = p.advance().text;
    p.advance();
    Ok(Some(DataType {
        qualifier: Vec::new(),
        name,
        angle: None,
        args: vec![Cow::Borrowed(precision), Cow::Owned(format!("-{scale}"))],
        suffix: DataTypeSuffix::None,
        array: ArrayType::None,
    }))
}

/// `CREATE`: DuckDB's, except that `CREATE [OR REPLACE] FUNCTION` is
/// PostgreSQL's, with a `RETURNS` type and a body.
pub fn parse_create<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<Statement<'a, Builtin>> {
    let or_replace = p.peek_nth(1).is_word("OR") && p.peek_nth(2).is_word("REPLACE");
    let n = if or_replace { 3 } else { 1 };
    if !p.peek_nth(n).is_word("FUNCTION") {
        return duckdb::parse_create(p);
    }
    for _ in 0..n {
        p.advance();
    }
    match postgres::ddl::parse_create_object(p, or_replace)? {
        Some(parsed) => Ok(StatementKind::Ext(BuiltinStatement::Postgres(parsed)).into()),
        None => Err(p.expected("a function definition")),
    }
}

/// True if what follows a `:` continues a path: a name or a `[`.
fn path_follows<D: Dialect>(p: &Parser<'_, D>) -> bool {
    let next = p.peek_nth(1);
    matches!(next.kind, TokenKind::Word | TokenKind::QuotedIdent) || next.is_op("[")
}

/// `value:key.key[index]`: Snowflake's path into a value, which can also start
/// with an index: `value:['key']`.
pub fn parse_path<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
    value: Expr<'a, Builtin>,
    bp: u8,
) -> PResult<Expr<'a, Builtin>> {
    if !(p.at_op(":") && p.peek_nth(1).is_op("[")) {
        return snowflake::parse_path(p, value, bp);
    }
    p.advance();
    let mut path = Vec::new();
    loop {
        if p.eat_op("[") {
            path.push(PathElement::Index(p.parse_expr()?));
            p.expect_op("]")?;
        } else if p.eat_op(".") {
            path.push(PathElement::Dot(p.parse_name_part()?));
        } else {
            break;
        }
    }
    let path = SnowflakeExpr::Path {
        value: Box::new(value),
        path,
    };
    Ok(ExprKind::Ext(BuiltinExpr::Snowflake(path)).into())
}

/// A join: DuckDB's, plus Snowflake's `ASOF JOIN ... MATCH_CONDITION (...)`,
/// Spark's `LEFT SEMI` and `LEFT ANTI`, and `RIGHT SEMI` and `RIGHT ANTI`.
pub fn parse_join<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<Option<Join<'a, Builtin>>> {
    if p.at_keyword("ASOF") {
        return snowflake::parse_join(p);
    }
    let next = p.peek_nth(1);
    if p.at_keyword("RIGHT") && (next.is_word("SEMI") || next.is_word("ANTI")) {
        let kind = if next.is_word("SEMI") {
            JoinKind::RightSemi
        } else {
            JoinKind::RightAnti
        };
        p.advance();
        p.advance();
        p.expect_keyword("JOIN")?;
        let table = p.parse_table_ref()?;
        let constraint = grammar::parse_join_constraint(p)?;
        return Ok(Some(Join {
            kind,
            spelled_out: false,
            table,
            constraint,
        }));
    }
    spark::parse_join(p)
}

/// A data type: DuckDB's, plus `STRUCT<...>`, `ARRAY<...>` and `MAP<...>` as
/// Spark writes them, and MySQL's `UNSIGNED` after an integer type.
pub fn parse_data_type<'a, D: Dialect<Ext = Builtin>>(
    p: &mut Parser<'a, D>,
) -> PResult<DataType<'a>> {
    if let Some(data_type) = parse_negative_scale_decimal(p)? {
        return Ok(data_type);
    }
    let angle = (p.at_keyword("STRUCT") || p.at_keyword("ARRAY") || p.at_keyword("MAP"))
        && p.peek_nth(1).is_op("<");
    let mut data_type = if angle && p.at_keyword("STRUCT") {
        // BigQuery's, in which a field can have no name: `STRUCT<INT>`.
        bigquery::parse_data_type(p)?
    } else if angle {
        spark::parse_data_type(p)?
    } else {
        duckdb::parse_data_type(p)?
    };
    let integer = [
        "TINYINT", "SMALLINT", "INT", "INTEGER", "BIGINT", "INT2", "INT4", "INT8",
    ]
    .iter()
    .any(|name| data_type.name.text.eq_ignore_ascii_case(name));
    if integer
        && data_type.qualifier.is_empty()
        && data_type.suffix == DataTypeSuffix::None
        && data_type.array == ArrayType::None
        && p.eat_keyword("UNSIGNED")
    {
        data_type.suffix = DataTypeSuffix::Unsigned;
    }
    Ok(data_type)
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlparser::dialect::spark::SparkStatement;
    use sqlparser::dialect::sqlite::SqliteStatement;

    #[test]
    fn parses_what_duckdb_parses() {
        for sql in [
            "SELECT [1, 2], {'a': 1}, MAP {1: 2}",
            "SELECT DISTINCT ON (a) a FROM t ORDER BY ALL",
            "WITH RECURSIVE t (n) AS (SELECT 1) SELECT n FROM t",
            "PREPARE q AS SELECT $1",
        ] {
            let ours = sqlparser::parse_sql::<DataFusion>(sql).unwrap();
            let duckdb = sqlparser::parse_sql::<sqlparser::dialect::DuckDb>(sql).unwrap();
            assert_eq!(ours, duckdb, "{sql}");
        }
    }

    /// The tables and joins in the `FROM` of a single `SELECT`.
    fn from_clause(sql: &str) -> TableWithJoins<'_, Builtin> {
        let mut statements = sqlparser::parse_sql::<DataFusion>(sql).unwrap();
        assert_eq!(statements.len(), 1, "{sql}");
        let StatementKind::Query(query) = statements.remove(0).kind else {
            panic!("not a query: {sql}");
        };
        let SetExpr::Select(select) = query.body else {
            panic!("not a SELECT: {sql}");
        };
        let mut from = select.from;
        assert_eq!(from.len(), 1, "{sql}");
        from.remove(0)
    }

    fn table_name<'t>(table: &'t TableRef<'_, Builtin>) -> &'t str {
        let TableFactor::Named(name) = &table.factor else {
            panic!("not a named table: {:?}", table.factor);
        };
        assert_eq!(name.parts.len(), 1);
        name.parts[0].text.as_ref()
    }

    #[test]
    fn left_and_right_name_tables_but_still_join() {
        let from = from_clause(
            "SELECT * FROM left AS t1 LEFT JOIN right AS t2 ON t1.a = t2.a \
             RIGHT JOIN u ON t1.a = u.a",
        );
        assert_eq!(table_name(&from.relation), "left");
        assert_eq!(from.relation.alias.as_ref().unwrap().name.text, "t1");
        let kinds: Vec<_> = from.joins.iter().map(|join| join.kind.clone()).collect();
        assert_eq!(kinds, [JoinKind::Left, JoinKind::Right]);
        assert_eq!(table_name(&from.joins[0].table), "right");
        assert_eq!(table_name(&from.joins[1].table), "u");
        assert!(from.joins[1].table.alias.is_none());
    }

    #[test]
    fn insert_overwrite_and_replace_use_their_dialects_rules() {
        let mut statements =
            sqlparser::parse_sql::<DataFusion>("INSERT OVERWRITE t1 VALUES (1, null)")
                .unwrap();
        let StatementKind::Ext(BuiltinStatement::Spark(SparkStatement::Insert(insert))) =
            statements.remove(0).kind
        else {
            panic!("not a Spark insert");
        };
        assert!(insert.overwrite);
        assert!(!insert.into);
        assert!(!insert.table_keyword);
        assert_eq!(insert.table.parts[0].text, "t1");

        for sql in [
            "REPLACE INTO t1 VALUES (1, null)",
            "INSERT OR REPLACE INTO t1 VALUES (1, null)",
        ] {
            let mut statements = sqlparser::parse_sql::<DataFusion>(sql).unwrap();
            let StatementKind::Ext(BuiltinStatement::Sqlite(SqliteStatement::Insert(
                insert,
            ))) = statements.remove(0).kind
            else {
                panic!("not a SQLite insert: {sql}");
            };
            assert_eq!(insert.or_action.as_deref(), Some("REPLACE"), "{sql}");
            assert_eq!(insert.table.parts[0].text, "t1");
        }

        // Without `OVERWRITE`, `DEFAULT VALUES` still goes through this dialect.
        let statements =
            sqlparser::parse_sql::<DataFusion>("INSERT INTO t1 DEFAULT VALUES").unwrap();
        assert!(matches!(statements[0].kind, StatementKind::Insert(_)));
    }

    #[test]
    fn match_condition_is_not_an_alias_of_a_nested_join() {
        let from = from_clause(
            "SELECT * FROM t ASOF JOIN (p JOIN q ON p.a = q.a) \
             MATCH_CONDITION (t.ts >= q.ts) ON t.a = p.a",
        );
        assert_eq!(from.joins.len(), 1);
        let asof = &from.joins[0];
        assert_eq!(asof.kind, JoinKind::AsOf);
        assert!(matches!(asof.table.factor, TableFactor::NestedJoin(_)));
        assert!(asof.table.alias.is_none());
    }
}
