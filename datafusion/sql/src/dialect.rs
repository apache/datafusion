// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! DataFusion's SQL dialect.
//!
//! DataFusion's SQL is DuckDB's, plus:
//!
//! - `FROM VALUES (...)` without parentheses, as in Spark
//! - `LEFT SEMI JOIN` and `LEFT ANTI JOIN`, as in Spark, and `RIGHT SEMI JOIN`
//!   and `RIGHT ANTI JOIN`
//! - `ASOF JOIN ... MATCH_CONDITION (...)`, as in Snowflake
//! - `STRUCT<...>`, `ARRAY<...>` and `MAP<...>` types, as in Spark
//! - `UNSIGNED` after an integer type, as in MySQL
//!
//! Every rule here either delegates to a built-in dialect's rule or is one of
//! these additions.

use sqlparser::PResult;
use sqlparser::ast::{
    ArrayType, ColumnOption, CreateFlags, CreateSchema, Cte, DataType, DataTypeSuffix,
    ExplainOption, Expr, FunctionArg, InsertSource, Join, JoinKind, ObjectType,
    OrderByItem, Query, Select, SelectItem, SetQuantifier, Statement, StringLiteral,
    TableFactor, TableRef, TableWithJoins,
};
use sqlparser::dialect::builtin::BuiltinSelect;
use sqlparser::dialect::{Builtin, Dialect, duckdb, postgres, snowflake, spark};
use sqlparser::grammar;
use sqlparser::lexer::{LexResult, Token};
use sqlparser::parser::{InfixRule, Parser};

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

    fn eat_limit_all<'a>(p: &mut Parser<'a, Self>) -> bool {
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
        duckdb::lex_token(rest)
    }

    #[inline]
    fn is_reserved(word: &str) -> bool {
        duckdb::is_reserved(word)
    }

    #[inline]
    fn is_reserved_for_alias(word: &str) -> bool {
        duckdb::is_reserved_for_alias(word)
    }

    fn parse_statement<'a>(p: &mut Parser<'a, Self>) -> PResult<Statement<'a, Builtin>> {
        duckdb::parse_statement(p)
    }

    fn parse_create<'a>(p: &mut Parser<'a, Self>) -> PResult<Statement<'a, Builtin>> {
        duckdb::parse_create(p)
    }

    fn parse_prefix<'a>(p: &mut Parser<'a, Self>) -> PResult<Expr<'a, Builtin>> {
        duckdb::parse_prefix(p)
    }

    fn infix_rule<'a>(p: &Parser<'a, Self>) -> Option<InfixRule<'a, Self>> {
        duckdb::infix_rule(p)
    }

    fn parse_data_type<'a>(p: &mut Parser<'a, Self>) -> PResult<DataType<'a>> {
        parse_data_type(p)
    }

    fn parse_table_factor<'a>(
        p: &mut Parser<'a, Self>,
    ) -> PResult<TableFactor<'a, Builtin>> {
        spark::parse_table_factor(p)
    }

    fn parse_select<'a>(p: &mut Parser<'a, Self>) -> PResult<Select<'a, Builtin>> {
        duckdb::parse_select(p)
    }

    fn at_from_first<'a>(p: &Parser<'a, Self>, n: usize) -> bool {
        p.peek_nth(n).is_word("FROM")
    }

    fn parse_select_item<'a>(
        p: &mut Parser<'a, Self>,
    ) -> PResult<SelectItem<'a, Builtin>> {
        duckdb::parse_select_item(p)
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

    fn parse_set_quantifier<'a>(p: &mut Parser<'a, Self>) -> (SetQuantifier, bool) {
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

    fn parse_extract_field<'a>(p: &mut Parser<'a, Self>) -> PResult<&'a str> {
        duckdb::parse_extract_field(p)
    }

    fn parse_between_low<'a>(p: &mut Parser<'a, Self>) -> PResult<Expr<'a, Builtin>> {
        let low = grammar::parse_between_low(p)?;
        spark::parse_between_bound(p, low)
    }

    fn parse_qualify<'a>(p: &mut Parser<'a, Self>) -> PResult<Option<Expr<'a, Builtin>>> {
        snowflake::parse_qualify(p)
    }

    fn eat_group_by_all<'a>(p: &mut Parser<'a, Self>) -> bool {
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

    fn parse_add_column_if_not_exists<'a>(p: &mut Parser<'a, Self>) -> PResult<bool> {
        grammar::parse_if_not_exists(p)
    }

    fn eat_drop_temporary<'a>(p: &mut Parser<'a, Self>) -> bool {
        p.eat_keyword("TEMPORARY")
    }

    fn parse_column_option<'a>(
        p: &mut Parser<'a, Self>,
    ) -> PResult<Option<ColumnOption<'a, Builtin>>> {
        duckdb::parse_column_option(p)
    }
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
    let angle = (p.at_keyword("STRUCT") || p.at_keyword("ARRAY") || p.at_keyword("MAP"))
        && p.peek_nth(1).is_op("<");
    let mut data_type = if angle {
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
}
