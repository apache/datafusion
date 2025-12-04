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

// This lint violation is acceptable for tests, so suppress for now
// Issue: <https://github.com/apache/datafusion/issues/18503>
#![expect(clippy::needless_pass_by_value)]

use std::any::Any;
use std::hash::Hash;
#[cfg(test)]
use std::sync::Arc;
use std::vec;

use arrow::datatypes::{TimeUnit::Nanosecond, *};
use common::MockContextProvider;
use datafusion_common::{assert_contains, DataFusionError, Result};
use datafusion_expr::{
    col, logical_plan::LogicalPlan, test::function_stub::sum_udaf, ColumnarValue,
    CreateIndex, DdlStatement, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature,
    Volatility,
};
use datafusion_functions::{string, unicode};
use datafusion_sql::{
    parser::DFParser,
    planner::{NullOrdering, ParserOptions, SqlToRel},
};

use crate::common::{CustomExprPlanner, CustomTypePlanner, MockSessionState};
use datafusion_functions::core::planner::CoreFunctionPlanner;
use datafusion_functions_aggregate::{
    approx_median::approx_median_udaf,
    average::avg_udaf,
    count::count_udaf,
    grouping::grouping_udaf,
    min_max::{max_udaf, min_udaf},
};
use datafusion_functions_nested::make_array::make_array_udf;
use datafusion_functions_window::{rank::rank_udwf, row_number::row_number_udwf};
use insta::{allow_duplicates, assert_snapshot};
use rstest::rstest;
use sqlparser::dialect::{Dialect, GenericDialect, HiveDialect, MySqlDialect};

mod cases;
mod common;

#[test]
fn parse_decimals_1() {
    let sql = "SELECT 1";
    let options = parse_decimals_parser_options();
    let plan = logical_plan_with_options(sql, options).unwrap();
    assert_snapshot!(
        plan,
        @r"
    Projection: Int64(1)
      EmptyRelation: rows=1
    "
    );
}

#[test]
fn parse_decimals_2() {
    let sql = "SELECT 001";
    let options = parse_decimals_parser_options();
    let plan = logical_plan_with_options(sql, options).unwrap();
    assert_snapshot!(
        plan,
        @r"
    Projection: Int64(1)
      EmptyRelation: rows=1
    "
    );
}

#[test]
fn parse_decimals_3() {
    let sql = "SELECT 0.1";
    let options = parse_decimals_parser_options();
    let plan = logical_plan_with_options(sql, options).unwrap();
    assert_snapshot!(
        plan,
        @r"
    Projection: Decimal128(Some(1),1,1)
      EmptyRelation: rows=1
    "
    );
}

#[test]
fn parse_decimals_4() {
    let sql = "SELECT 0.01";
    let options = parse_decimals_parser_options();
    let plan = logical_plan_with_options(sql, options).unwrap();
    assert_snapshot!(
        plan,
        @r"
    Projection: Decimal128(Some(1),2,2)
      EmptyRelation: rows=1
    "
    );
}

#[test]
fn parse_decimals_5() {
    let sql = "SELECT 1.0";
    let options = parse_decimals_parser_options();
    let plan = logical_plan_with_options(sql, options).unwrap();
    assert_snapshot!(
        plan,
        @r"
    Projection: Decimal128(Some(10),2,1)
      EmptyRelation: rows=1
    "
    );
}

#[test]
fn parse_decimals_6() {
    let sql = "SELECT 10.01";
    let options = parse_decimals_parser_options();
    let plan = logical_plan_with_options(sql, options).unwrap();
    assert_snapshot!(
        plan,
        @r"
    Projection: Decimal128(Some(1001),4,2)
      EmptyRelation: rows=1
    "
    );
}

#[test]
fn parse_decimals_7() {
    let sql = "SELECT 10000000000000000000.00";
    let options = parse_decimals_parser_options();
    let plan = logical_plan_with_options(sql, options).unwrap();
    assert_snapshot!(
        plan,
        @r"
    Projection: Decimal128(Some(1000000000000000000000),22,2)
      EmptyRelation: rows=1
    "
    );
}

#[test]
fn parse_decimals_8() {
    let sql = "SELECT 18446744073709551615";
    let options = parse_decimals_parser_options();
    let plan = logical_plan_with_options(sql, options).unwrap();
    assert_snapshot!(
        plan,
        @r"
    Projection: UInt64(18446744073709551615)
      EmptyRelation: rows=1
    "
    );
}

#[test]
fn parse_decimals_9() {
    let sql = "SELECT 18446744073709551616";
    let options = parse_decimals_parser_options();
    let plan = logical_plan_with_options(sql, options).unwrap();
    assert_snapshot!(
        plan,
        @r"
    Projection: Decimal128(Some(18446744073709551616),20,0)
      EmptyRelation: rows=1
    "
    );
}

#[test]
fn parse_ident_normalization_1() {
    let sql = "SELECT CHARACTER_LENGTH('str')";
    let parser_option = ident_normalization_parser_options_no_ident_normalization();
    let plan = logical_plan_with_options(sql, parser_option).unwrap();
    assert_snapshot!(
        plan,
        @r#"
    Projection: character_length(Utf8("str"))
      EmptyRelation: rows=1
    "#
    );
}

#[test]
fn parse_ident_normalization_2() {
    let sql = "SELECT CONCAT('Hello', 'World')";
    let parser_option = ident_normalization_parser_options_no_ident_normalization();
    let plan = logical_plan_with_options(sql, parser_option).unwrap();
    assert_snapshot!(
        plan,
        @r#"
    Projection: concat(Utf8("Hello"), Utf8("World"))
      EmptyRelation: rows=1
    "#
    );
}

#[test]
fn parse_ident_normalization_3() {
    let sql = "SELECT age FROM person";
    let parser_option = ident_normalization_parser_options_ident_normalization();
    let plan = logical_plan_with_options(sql, parser_option).unwrap();
    assert_snapshot!(
        plan,
        @r#"
        Projection: person.age
          TableScan: person
        "#
    );
}

#[test]
fn parse_ident_normalization_4() {
    let sql = "SELECT AGE FROM PERSON";
    let parser_option = ident_normalization_parser_options_ident_normalization();
    let plan = logical_plan_with_options(sql, parser_option).unwrap();
    assert_snapshot!(
        plan,
        @r#"
        Projection: person.age
          TableScan: person
        "#
    );
}

#[test]
fn within_group_rejected_for_non_ordered_set_udaf() {
    // MIN is order-sensitive by nature but does not implement the
    // ordered-set `WITHIN GROUP` opt-in. The planner must reject
    // explicit `WITHIN GROUP` syntax for functions that do not
    // advertise `supports_within_group_clause()`.
    let sql = "SELECT min(c1) WITHIN GROUP (ORDER BY c1) FROM person";
    let err = logical_plan(sql)
        .expect_err("expected planning to fail for MIN WITHIN GROUP")
        .to_string();
    assert_contains!(
        err,
        "WITHIN GROUP is only supported for ordered-set aggregate functions"
    );
}

#[test]
fn parse_ident_normalization_5() {
    let sql = "SELECT AGE FROM PERSON";
    let parser_option = ident_normalization_parser_options_no_ident_normalization();
    let plan = logical_plan_with_options(sql, parser_option)
        .unwrap_err()
        .strip_backtrace();
    assert_snapshot!(
        plan,
        @r#"
        Error during planning: No table named: PERSON found
        "#
    );
}

#[test]
fn parse_ident_normalization_6() {
    let sql = "SELECT Id FROM UPPERCASE_test";
    let parser_option = ident_normalization_parser_options_no_ident_normalization();
    let plan = logical_plan_with_options(sql, parser_option).unwrap();
    assert_snapshot!(
        plan,
        @r#"
        Projection: UPPERCASE_test.Id
          TableScan: UPPERCASE_test
        "#
    );
}

#[test]
fn parse_ident_normalization_7() {
    let sql = r#"SELECT "Id", lower FROM "UPPERCASE_test""#;
    let parser_option = ident_normalization_parser_options_ident_normalization();
    let plan = logical_plan_with_options(sql, parser_option).unwrap();
    assert_snapshot!(
        plan,
        @r#"
        Projection: UPPERCASE_test.Id, UPPERCASE_test.lower
          TableScan: UPPERCASE_test
        "#
    );
}

#[test]
fn select_no_relation() {
    let plan = logical_plan("SELECT 1").unwrap();
    assert_snapshot!(
        plan,
        @r"
    Projection: Int64(1)
      EmptyRelation: rows=1
    "
    );
}

#[test]
fn test_real_f32() {
    let plan = logical_plan("SELECT CAST(1.1 AS REAL)").unwrap();
    assert_snapshot!(
        plan,
        @r"
    Projection: CAST(Float64(1.1) AS Float32)
      EmptyRelation: rows=1
    "
    );
}

#[test]
fn test_int_decimal_default() {
    let plan = logical_plan("SELECT CAST(10 AS DECIMAL)").unwrap();
    assert_snapshot!(
        plan,
        @r"
    Projection: CAST(Int64(10) AS Decimal128(38, 10))
      EmptyRelation: rows=1
    "
    );
}

#[test]
fn test_int_decimal_no_scale() {
    let plan = logical_plan("SELECT CAST(10 AS DECIMAL(5))").unwrap();
    assert_snapshot!(
        plan,
        @r"
    Projection: CAST(Int64(10) AS Decimal128(5, 0))
      EmptyRelation: rows=1
    "
    );
}

#[test]
fn test_tinyint() {
    let plan = logical_plan("SELECT CAST(6 AS TINYINT)").unwrap();
    assert_snapshot!(
        plan,
        @r"
    Projection: CAST(Int64(6) AS Int8)
      EmptyRelation: rows=1
    "
    );
}

#[test]
fn cast_from_subquery() {
    let plan = logical_plan("SELECT CAST (a AS FLOAT) FROM (SELECT 1 AS a)").unwrap();
    assert_snapshot!(
        plan,
        @r"
    Projection: CAST(a AS Float32)
      Projection: Int64(1) AS a
        EmptyRelation: rows=1
    "
    );
}

#[test]
fn try_cast_from_aggregation() {
    let plan = logical_plan("SELECT TRY_CAST(sum(age) AS FLOAT) FROM person").unwrap();
    assert_snapshot!(
        plan,
        @r#"
        Projection: TRY_CAST(sum(person.age) AS Float32)
          Aggregate: groupBy=[[]], aggr=[[sum(person.age)]]
            TableScan: person
        "#
    );
}

#[test]
fn cast_to_invalid_decimal_type_precision_0() {
    // precision == 0
    let sql = "SELECT CAST(10 AS DECIMAL(0))";
    let err = logical_plan(sql).expect_err("query should have failed");

    assert_snapshot!(
        err.strip_backtrace(),
        @r"Error during planning: Decimal(precision = 0, scale = 0) should satisfy `0 < precision <= 76`, and `scale <= precision`."
    );
}

#[test]
fn cast_to_invalid_decimal_type_precision_gt_38() {
    // precision > 38
    let sql = "SELECT CAST(10 AS DECIMAL(39))";
    let plan = logical_plan(sql).unwrap();
    assert_snapshot!(
        plan,
        @r"
    Projection: CAST(Int64(10) AS Decimal256(39, 0))
      EmptyRelation: rows=1
    "
    );
}

#[test]
fn cast_to_invalid_decimal_type_precision_gt_76() {
    // precision > 76
    let sql = "SELECT CAST(10 AS DECIMAL(79))";
    let err = logical_plan(sql).expect_err("query should have failed");

    assert_snapshot!(
        err.strip_backtrace(),
        @r"Error during planning: Decimal(precision = 79, scale = 0) should satisfy `0 < precision <= 76`, and `scale <= precision`."
    );
}

#[test]
fn cast_to_invalid_decimal_type_precision_lt_scale() {
    // precision < scale
    let sql = "SELECT CAST(10 AS DECIMAL(5, 10))";
    let err = logical_plan(sql).expect_err("query should have failed");

    assert_snapshot!(
        err.strip_backtrace(),
        @r"Error during planning: Decimal(precision = 5, scale = 10) should satisfy `0 < precision <= 76`, and `scale <= precision`."
    );
}

#[test]
fn plan_create_table_with_pk() {
    let sql = "create table person (id int, name string, primary key(id))";
    let plan = logical_plan(sql).unwrap();
    assert_snapshot!(
        plan,
        @r#"
    CreateMemoryTable: Bare { table: "person" } constraints=[PrimaryKey([0])]
      EmptyRelation: rows=0
    "#
    );

    let sql = "create table person (id int primary key, name string)";
    let plan = logical_plan(sql).unwrap();
    assert_snapshot!(
        plan,
        @r#"
    CreateMemoryTable: Bare { table: "person" } constraints=[PrimaryKey([0])]
      EmptyRelation: rows=0
    "#
    );

    let sql =
        "create table person (id int, name string unique not null, primary key(id))";
    let plan = logical_plan(sql).unwrap();
    assert_snapshot!(
        plan,
        @r#"
    CreateMemoryTable: Bare { table: "person" } constraints=[PrimaryKey([0]), Unique([1])]
      EmptyRelation: rows=0
    "#
    );

    let sql = "create table person (id int, name varchar,  primary key(name,  id));";
    let plan = logical_plan(sql).unwrap();
    assert_snapshot!(
        plan,
        @r#"
    CreateMemoryTable: Bare { table: "person" } constraints=[PrimaryKey([1, 0])]
      EmptyRelation: rows=0
    "#
    );
}

#[test]
fn plan_create_table_with_multi_pk() {
    let sql = "create table person (id int, name string primary key, primary key(id))";
    let plan = logical_plan(sql).unwrap();
    assert_snapshot!(
        plan,
        @r#"
    CreateMemoryTable: Bare { table: "person" } constraints=[PrimaryKey([0]), PrimaryKey([1])]
      EmptyRelation: rows=0
    "#
    );
}

#[test]
fn plan_create_table_with_unique() {
    let sql = "create table person (id int unique, name string)";
    let plan = logical_plan(sql).unwrap();
    assert_snapshot!(
        plan,
        @r#"
    CreateMemoryTable: Bare { table: "person" } constraints=[Unique([0])]
      EmptyRelation: rows=0
    "#
    );
}

#[test]
fn plan_create_table_no_pk() {
    let sql = "create table person (id int, name string)";
    let plan = logical_plan(sql).unwrap();
    assert_snapshot!(
        plan,
        @r#"
    CreateMemoryTable: Bare { table: "person" }
      EmptyRelation: rows=0
    "#
    );
}

#[test]
fn plan_create_table_check_constraint() {
    let sql = "create table person (id int, name string, unique(id))";
    let plan = logical_plan(sql).unwrap();
    assert_snapshot!(
        plan,
        @r#"
    CreateMemoryTable: Bare { table: "person" } constraints=[Unique([0])]
      EmptyRelation: rows=0
    "#
    );
}

#[test]
fn plan_start_transaction() {
    let sql = "start transaction";
    let plan = logical_plan(sql).unwrap();
    assert_snapshot!(
        plan,
        @r#"
        TransactionStart: ReadWrite Serializable
        "#
    );
}

#[test]
fn plan_start_transaction_isolation() {
    let sql = "start transaction isolation level read committed";
    let plan = logical_plan(sql).unwrap();
    assert_snapshot!(
        plan,
        @r#"
        TransactionStart: ReadWrite ReadCommitted
        "#
    );
}

#[test]
fn plan_start_transaction_read_only() {
    let sql = "start transaction read only";
    let plan = logical_plan(sql).unwrap();
    assert_snapshot!(
        plan,
        @r#"
        TransactionStart: ReadOnly Serializable
        "#
    );
}

#[test]
fn plan_start_transaction_fully_qualified() {
    let sql = "start transaction isolation level read committed read only";
    let plan = logical_plan(sql).unwrap();
    assert_snapshot!(
        plan,
        @r#"
        TransactionStart: ReadOnly ReadCommitted
        "#
    );
}

#[test]
fn plan_start_transaction_overly_qualified() {
    let sql = r#"start transaction
isolation level read committed
read only
isolation level repeatable read
"#;
    let plan = logical_plan(sql).unwrap();
    assert_snapshot!(
        plan,
        @r#"
        TransactionStart: ReadOnly RepeatableRead
        "#
    );
}

#[test]
fn plan_commit_transaction() {
    let sql = "commit transaction";
    let plan = logical_plan(sql).unwrap();
    assert_snapshot!(
        plan,
        @r#"
        TransactionEnd: Commit chain:=false
        "#
    );
}

#[test]
fn plan_commit_transaction_chained() {
    let sql = "commit transaction and chain";
    let plan = logical_plan(sql).unwrap();
    assert_snapshot!(
        plan,
        @r#"
        TransactionEnd: Commit chain:=true
        "#
    );
}

#[test]
fn plan_rollback_transaction() {
    let sql = "rollback transaction";
    let plan = logical_plan(sql).unwrap();
    assert_snapshot!(
        plan,
        @r#"
        TransactionEnd: Rollback chain:=false
        "#
    );
}

#[test]
fn plan_rollback_transaction_chained() {
    let sql = "rollback transaction and chain";
    let plan = logical_plan(sql).unwrap();
    assert_snapshot!(
        plan,
        @r#"
        TransactionEnd: Rollback chain:=true
        "#
    );
}

#[test]
fn plan_copy_to() {
    let sql = "COPY test_decimal to 'output.csv' STORED AS CSV";
    let plan = logical_plan(sql).unwrap();
    assert_snapshot!(
        plan,
        @r#"
        CopyTo: format=csv output_url=output.csv options: ()
          TableScan: test_decimal
        "#
    );
}

#[test]
fn plan_explain_copy_to() {
    let sql = "EXPLAIN COPY test_decimal to 'output.csv'";
    let plan = logical_plan(sql).unwrap();
    assert_snapshot!(
        plan,
        @r#"
        Explain
          CopyTo: format=csv output_url=output.csv options: ()
            TableScan: test_decimal
        "#
    );
}

#[test]
fn plan_explain_copy_to_format() {
    let sql = "EXPLAIN COPY test_decimal to 'output.tbl' STORED AS CSV";
    let plan = logical_plan(sql).unwrap();
    assert_snapshot!(
        plan,
        @r#"
        Explain
          CopyTo: format=csv output_url=output.tbl options: ()
            TableScan: test_decimal
        "#
    );
}

#[test]
fn plan_insert() {
    let sql =
        "insert into person (id, first_name, last_name) values (1, 'Alan', 'Turing')";
    let plan = logical_plan(sql).unwrap();
    assert_snapshot!(
        plan,
        @r#"
    Dml: op=[Insert Into] table=[person]
      Projection: column1 AS id, column2 AS first_name, column3 AS last_name, CAST(NULL AS Int32) AS age, CAST(NULL AS Utf8) AS state, CAST(NULL AS Float64) AS salary, CAST(NULL AS Timestamp(ns)) AS birth_date, CAST(NULL AS Int32) AS 😀
        Values: (CAST(Int64(1) AS UInt32), Utf8("Alan"), Utf8("Turing"))
    "#
    );
}

#[test]
fn plan_insert_no_target_columns() {
    let sql = "INSERT INTO test_decimal VALUES (1, 2), (3, 4)";
    let plan = logical_plan(sql).unwrap();
    assert_snapshot!(
        plan,
        @r#"
        Dml: op=[Insert Into] table=[test_decimal]
          Projection: column1 AS id, column2 AS price
            Values: (CAST(Int64(1) AS Int32), CAST(Int64(2) AS Decimal128(10, 2))), (CAST(Int64(3) AS Int32), CAST(Int64(4) AS Decimal128(10, 2)))
        "#
    );
}

#[rstest]
#[case::duplicate_columns(
    "INSERT INTO test_decimal (id, price, price) VALUES (1, 2, 3), (4, 5, 6)",
    "Schema error: Schema contains duplicate unqualified field name price"
)]
#[case::non_existing_column(
    "INSERT INTO test_decimal (nonexistent, price) VALUES (1, 2), (4, 5)",
    "Schema error: No field named nonexistent. \
    Valid fields are id, price."
)]
#[case::target_column_count_mismatch(
    "INSERT INTO person (id, first_name, last_name) VALUES ($1, $2)",
    "Error during planning: Inconsistent data length across values list: got 2 values in row 0 but expected 3"
)]
#[case::source_column_count_mismatch(
    "INSERT INTO person VALUES ($1, $2)",
    "Error during planning: Inconsistent data length across values list: got 2 values in row 0 but expected 8"
)]
#[case::extra_placeholder(
    "INSERT INTO person (id, first_name, last_name) VALUES ($1, $2, $3, $4)",
    "Error during planning: Placeholder $4 refers to a non existent column"
)]
#[case::placeholder_type_unresolved(
    "INSERT INTO person (id, first_name, last_name) VALUES ($id, $first_name, $last_name)",
    "Error during planning: Can't parse placeholder: $id"
)]
#[test]
fn test_insert_schema_errors(#[case] sql: &str, #[case] error: &str) {
    let err = logical_plan(sql).unwrap_err();
    assert_eq!(err.strip_backtrace(), error)
}

#[test]
fn plan_update() {
    let sql = "update person set last_name='Kay' where id=1";
    let plan = logical_plan(sql).unwrap();
    assert_snapshot!(
        plan,
        @r#"
        Dml: op=[Update] table=[person]
          Projection: person.id AS id, person.first_name AS first_name, Utf8("Kay") AS last_name, person.age AS age, person.state AS state, person.salary AS salary, person.birth_date AS birth_date, person.😀 AS 😀
            Filter: person.id = Int64(1)
              TableScan: person
        "#
    );
}

#[rstest]
#[case::missing_assignment_target("UPDATE person SET doesnotexist = true")]
#[case::missing_assignment_expression("UPDATE person SET age = doesnotexist + 42")]
#[case::missing_selection_expression(
    "UPDATE person SET age = 42 WHERE doesnotexist = true"
)]
#[test]
fn update_column_does_not_exist(#[case] sql: &str) {
    let err = logical_plan(sql).expect_err("query should have failed");
    assert_field_not_found(err, "doesnotexist");
}

#[test]
fn plan_delete() {
    let sql = "delete from person where id=1";
    let plan = logical_plan(sql).unwrap();
    assert_snapshot!(
        plan,
        @r#"
        Dml: op=[Delete] table=[person]
          Filter: person.id = Int64(1)
            TableScan: person
        "#
    );
}

#[test]
fn plan_delete_quoted_identifier_case_sensitive() {
    let sql =
        "DELETE FROM \"SomeCatalog\".\"SomeSchema\".\"UPPERCASE_test\" WHERE \"Id\" = 1";
    let plan = logical_plan(sql).unwrap();
    assert_snapshot!(
        plan,
        @r#"
        Dml: op=[Delete] table=[SomeCatalog.SomeSchema.UPPERCASE_test]
          Filter: SomeCatalog.SomeSchema.UPPERCASE_test.Id = Int64(1)
            TableScan: SomeCatalog.SomeSchema.UPPERCASE_test
        "#
    );
}

#[test]
fn select_column_does_not_exist() {
    let sql = "SELECT doesnotexist FROM person";
    let err = logical_plan(sql).expect_err("query should have failed");
    assert_field_not_found(err, "doesnotexist");
}

#[test]
fn select_repeated_column() {
    let sql = "SELECT age, age FROM person";
    let err = logical_plan(sql).expect_err("query should have failed");

    assert_snapshot!(
        err.strip_backtrace(),
        @r#"
        Error during planning: Projections require unique expression names but the expression "person.age" at position 0 and "person.age" at position 1 have the same name. Consider aliasing ("AS") one of them.
        "#
    );
}

#[test]
fn select_scalar_func_with_literal_no_relation() {
    let plan = logical_plan("SELECT sqrt(9)").unwrap();
    assert_snapshot!(
        plan,
        @r"
    Projection: sqrt(Int64(9))
      EmptyRelation: rows=1
    "
    );
}

#[test]
fn select_simple_filter() {
    let sql = "SELECT id, first_name, last_name \
                   FROM person WHERE state = 'CO'";
    let plan = logical_plan(sql).unwrap();
    assert_snapshot!(
        plan,
        @r#"
        Projection: person.id, person.first_name, person.last_name
          Filter: person.state = Utf8("CO")
            TableScan: person
        "#
    );
}

#[test]
fn select_filter_column_does_not_exist() {
    let sql = "SELECT first_name FROM person WHERE doesnotexist = 'A'";
    let err = logical_plan(sql).expect_err("query should have failed");
    assert_field_not_found(err, "doesnotexist");
}

#[test]
fn select_filter_cannot_use_alias() {
    let sql = "SELECT first_name AS x FROM person WHERE x = 'A'";
    let err = logical_plan(sql).expect_err("query should have failed");
    assert_field_not_found(err, "x");
}

#[test]
fn select_neg_filter() {
    let sql = "SELECT id, first_name, last_name \
                   FROM person WHERE NOT state";
    let plan = logical_plan(sql).unwrap();
    assert_snapshot!(
        plan,
        @r#"
        Projection: person.id, person.first_name, person.last_name
          Filter: NOT person.state
            TableScan: person
        "#
    );
}

#[test]
fn select_compound_filter() {
    let sql = "SELECT id, first_name, last_name \
                   FROM person WHERE state = 'CO' AND age >= 21 AND age <= 65";
    let plan = logical_plan(sql).unwrap();
    assert_snapshot!(
        plan,
        @r#"
        Projection: person.id, person.first_name, person.last_name
          Filter: person.state = Utf8("CO") AND person.age >= Int64(21) AND person.age <= Int64(65)
            TableScan: person
        "#
    );
}

#[test]
fn test_timestamp_filter() {
    let sql = "SELECT state FROM person WHERE birth_date < CAST (158412331400600000 as timestamp)";
    let plan = logical_plan(sql).unwrap();
    assert_snapshot!(
        plan,
        @r"
    Projection: person.state
      Filter: person.birth_date < CAST(CAST(Int64(158412331400600000) AS Timestamp(s)) AS Timestamp(ns))
        TableScan: person
    "
    );
}

#[test]
fn test_date_filter() {
    let sql = "SELECT state FROM person WHERE birth_date < CAST ('2020-01-01' as date)";
    let plan = logical_plan(sql).unwrap();
    assert_snapshot!(
        plan,
        @r#"
        Projection: person.state
          Filter: person.birth_date < CAST(Utf8("2020-01-01") AS Date32)
            TableScan: person
        "#
    );
}

#[test]
fn select_all_boolean_operators() {
    let sql = "SELECT age, first_name, last_name \
                   FROM person \
                   WHERE age = 21 \
                   AND age != 21 \
                   AND age > 21 \
                   AND age >= 21 \
                   AND age < 65 \
                   AND age <= 65";
    let plan = logical_plan(sql).unwrap();
    assert_snapshot!(
        plan,
        @r#"
        Projection: person.age, person.first_name, person.last_name
          Filter: person.age = Int64(21) AND person.age != Int64(21) AND person.age > Int64(21) AND person.age >= Int64(21) AND person.age < Int64(65) AND person.age <= Int64(65)
            TableScan: person
        "#
    );
}

#[test]
fn select_between() {
    let sql = "SELECT state FROM person WHERE age BETWEEN 21 AND 65";
    let plan = logical_plan(sql).unwrap();
    assert_snapshot!(
        plan,
        @r#"
        Projection: person.state
          Filter: person.age BETWEEN Int64(21) AND Int64(65)
            TableScan: person
        "#
    );
}

#[test]
fn select_between_negated() {
    let sql = "SELECT state FROM person WHERE age NOT BETWEEN 21 AND 65";
    let plan = logical_plan(sql).unwrap();
    assert_snapshot!(
        plan,
        @r#"
        Projection: person.state
          Filter: person.age NOT BETWEEN Int64(21) AND Int64(65)
            TableScan: person
        "#
    );
}

#[test]
fn select_nested() {
    let sql = "SELECT fn2, last_name
                   FROM (
                     SELECT fn1 as fn2, last_name, birth_date
                     FROM (
                       SELECT first_name AS fn1, last_name, birth_date, age
                       FROM person
                     ) AS a
                   ) AS b";
    let plan = logical_plan(sql).unwrap();
    assert_snapshot!(
        plan,
        @r#"
        Projection: b.fn2, b.last_name
          SubqueryAlias: b
            Projection: a.fn1 AS fn2, a.last_name, a.birth_date
              SubqueryAlias: a
                Projection: person.first_name AS fn1, person.last_name, person.birth_date, person.age
                  TableScan: person
        "#
    );
}

#[test]
fn select_nested_with_filters() {
    let sql = "SELECT fn1, age
                   FROM (
                     SELECT first_name AS fn1, age
                     FROM person
                     WHERE age > 20
                   ) AS a
                   WHERE fn1 = 'X' AND age < 30";
    let plan = logical_plan(sql).unwrap();
    assert_snapshot!(
        plan,
        @r#"
        Projection: a.fn1, a.age
          Filter: a.fn1 = Utf8("X") AND a.age < Int64(30)
            SubqueryAlias: a
              Projection: person.first_name AS fn1, person.age
                Filter: person.age > Int64(20)
                  TableScan: person
        "#
    );
}

#[test]
fn table_with_column_alias() {
    let sql = "SELECT a, b, c
                   FROM lineitem l (a, b, c)";
    let plan = logical_plan(sql).unwrap();
    assert_snapshot!(
        plan,
        @r#"
        Projection: l.a, l.b, l.c
          SubqueryAlias: l
            Projection: lineitem.l_item_id AS a, lineitem.l_description AS b, lineitem.price AS c
              TableScan: lineitem
        "#
    );
}

#[test]
fn table_with_column_alias_number_cols() {
    let sql = "SELECT a, b, c
                   FROM lineitem l (a, b)";
    let err = logical_plan(sql).expect_err("query should have failed");

    assert_snapshot!(
        err.strip_backtrace(),
        @r"Error during planning: Source table contains 3 columns but only 2 names given as column alias"
    );
}

#[test]
fn select_with_ambiguous_column() {
    let sql = "SELECT id FROM person a, person b";
    let err = logical_plan(sql).expect_err("query should have failed");

    assert_snapshot!(
        err.strip_backtrace(),
        @r"Schema error: Ambiguous reference to unqualified field id"
    );
}

#[test]
fn join_with_ambiguous_column() {
    // This is legal.
    let sql = "SELECT id FROM person a join person b using(id)";
    let plan = logical_plan(sql).unwrap();
    assert_snapshot!(
        plan,
        @r#"
        Projection: a.id
          Inner Join: Using a.id = b.id
            SubqueryAlias: a
              TableScan: person
            SubqueryAlias: b
              TableScan: person
        "#
    );
}

#[test]
fn natural_left_join() {
    let sql = "SELECT l_item_id FROM lineitem a NATURAL LEFT JOIN lineitem b";
    let plan = logical_plan(sql).unwrap();
    assert_snapshot!(
        plan,
        @r#"
        Projection: a.l_item_id
          Left Join: Using a.l_item_id = b.l_item_id, a.l_description = b.l_description, a.price = b.price
            SubqueryAlias: a
              TableScan: lineitem
            SubqueryAlias: b
              TableScan: lineitem
        "#
    );
}

#[test]
fn natural_right_join() {
    let sql = "SELECT l_item_id FROM lineitem a NATURAL RIGHT JOIN lineitem b";
    let plan = logical_plan(sql).unwrap();
    assert_snapshot!(
        plan,
        @r#"
        Projection: a.l_item_id
          Right Join: Using a.l_item_id = b.l_item_id, a.l_description = b.l_description, a.price = b.price
            SubqueryAlias: a
              TableScan: lineitem
            SubqueryAlias: b
              TableScan: lineitem
        "#
    );
}

#[test]
fn select_with_having() {
    let sql = "SELECT id, age
                   FROM person
                   HAVING age > 100 AND age < 200";
    let err = logical_plan(sql).expect_err("query should have failed");

    assert_snapshot!(
        err.strip_backtrace(),
        @r"Error during planning: HAVING clause references: person.age > Int64(100) AND person.age < Int64(200) must appear in the GROUP BY clause or be used in an aggregate function"
    );
}

#[test]
fn select_with_having_referencing_column_not_in_select() {
    let sql = "SELECT id, age
                   FROM person
                   HAVING first_name = 'M'";
    let err = logical_plan(sql).expect_err("query should have failed");

    assert_snapshot!(
        err.strip_backtrace(),
        @r#"
        Error during planning: HAVING clause references: person.first_name = Utf8("M") must appear in the GROUP BY clause or be used in an aggregate function
        "#
    );
}

#[test]
fn select_with_having_refers_to_invalid_column() {
    let sql = "SELECT id, MAX(age)
                   FROM person
                   GROUP BY id
                   HAVING first_name = 'M'";
    let err = logical_plan(sql).expect_err("query should have failed");

    assert_snapshot!(
        err.strip_backtrace(),
        @r#"
        Error during planning: Column in HAVING must be in GROUP BY or an aggregate function: While expanding wildcard, column "person.first_name" must appear in the GROUP BY clause or must be part of an aggregate function, currently only "person.id, max(person.age)" appears in the SELECT clause satisfies this requirement
        "#
    );
}

#[test]
fn select_with_having_referencing_column_nested_in_select_expression() {
    let sql = "SELECT id, age + 1
                   FROM person
                   HAVING age > 100";
    let err = logical_plan(sql).expect_err("query should have failed");

    assert_snapshot!(
        err.strip_backtrace(),
        @r#"
        Error during planning: HAVING clause references: person.age > Int64(100) must appear in the GROUP BY clause or be used in an aggregate function
        "#
    );
}

#[test]
fn select_with_having_with_aggregate_not_in_select() {
    let sql = "SELECT first_name
                   FROM person
                   HAVING MAX(age) > 100";
    let err = logical_plan(sql).expect_err("query should have failed");

    assert_snapshot!(
        err.strip_backtrace(),
        @r#"Error during planning: Column in SELECT must be in GROUP BY or an aggregate function: While expanding wildcard, column "person.first_name" must appear in the GROUP BY clause or must be part of an aggregate function, currently only "max(person.age)" appears in the SELECT clause satisfies this requirement"#
    );
}

#[test]
fn select_aggregate_with_having_that_reuses_aggregate() {
    let sql = "SELECT MAX(age)
                   FROM person
                   HAVING MAX(age) < 30";
    let plan = logical_plan(sql).unwrap();
    assert_snapshot!(
        plan,
        @r#"
        Projection: max(person.age)
          Filter: max(person.age) < Int64(30)
            Aggregate: groupBy=[[]], aggr=[[max(person.age)]]
              TableScan: person
        "#
    );
}

#[test]
fn select_aggregate_with_having_with_aggregate_not_in_select() {
    let sql = "SELECT max(age)
                   FROM person
                   HAVING max(first_name) > 'M'";
    let plan = logical_plan(sql).unwrap();
    assert_snapshot!(
        plan,
        @r#"
        Projection: max(person.age)
          Filter: max(person.first_name) > Utf8("M")
            Aggregate: groupBy=[[]], aggr=[[max(person.age), max(person.first_name)]]
              TableScan: person
        "#
    );
}

#[test]
fn select_aggregate_with_having_referencing_column_not_in_select() {
    let sql = "SELECT count(*)
                   FROM person
                   HAVING first_name = 'M'";
    let err = logical_plan(sql).expect_err("query should have failed");

    assert_snapshot!(
        err.strip_backtrace(),
        @r#"
        Error during planning: Column in HAVING must be in GROUP BY or an aggregate function: While expanding wildcard, column "person.first_name" must appear in the GROUP BY clause or must be part of an aggregate function, currently only "count(*)" appears in the SELECT clause satisfies this requirement
        "#
    );
}

#[test]
fn select_aggregate_aliased_with_having_referencing_aggregate_by_its_alias() {
    let sql = "SELECT MAX(age) as max_age
                   FROM person
                   HAVING max_age < 30";
    // FIXME: add test for having in execution
    let plan = logical_plan(sql).unwrap();
    assert_snapshot!(
        plan,
        @r#"
        Projection: max(person.age) AS max_age
          Filter: max(person.age) < Int64(30)
            Aggregate: groupBy=[[]], aggr=[[max(person.age)]]
              TableScan: person
        "#
    );
}

#[test]
fn select_aggregate_aliased_with_having_that_reuses_aggregate_but_not_by_its_alias() {
    let sql = "SELECT max(age) as max_age
                   FROM person
                   HAVING max(age) < 30";
    let plan = logical_plan(sql).unwrap();
    assert_snapshot!(
        plan,
        @r#"
        Projection: max(person.age) AS max_age
          Filter: max(person.age) < Int64(30)
            Aggregate: groupBy=[[]], aggr=[[max(person.age)]]
              TableScan: person
        "#
    );
}

#[test]
fn select_aggregate_with_group_by_with_having() {
    let sql = "SELECT first_name, MAX(age)
                   FROM person
                   GROUP BY first_name
                   HAVING first_name = 'M'";
    let plan = logical_plan(sql).unwrap();
    assert_snapshot!(
        plan,
        @r#"
        Projection: person.first_name, max(person.age)
          Filter: person.first_name = Utf8("M")
            Aggregate: groupBy=[[person.first_name]], aggr=[[max(person.age)]]
              TableScan: person
        "#
    );
}

#[test]
fn select_aggregate_with_group_by_with_having_and_where() {
    let sql = "SELECT first_name, max(age)
                   FROM person
                   WHERE id > 5
                   GROUP BY first_name
                   HAVING MAX(age) < 100";
    let plan = logical_plan(sql).unwrap();
    assert_snapshot!(
        plan,
        @r#"
        Projection: person.first_name, max(person.age)
          Filter: max(person.age) < Int64(100)
            Aggregate: groupBy=[[person.first_name]], aggr=[[max(person.age)]]
              Filter: person.id > Int64(5)
                TableScan: person
        "#
    );
}

#[test]
fn select_aggregate_with_group_by_with_having_and_where_filtering_on_aggregate_column() {
    let sql = "SELECT first_name, MAX(age)
                   FROM person
                   WHERE id > 5 AND age > 18
                   GROUP BY first_name
                   HAVING MAX(age) < 100";
    let plan = logical_plan(sql).unwrap();
    assert_snapshot!(
        plan,
        @r#"
        Projection: person.first_name, max(person.age)
          Filter: max(person.age) < Int64(100)
            Aggregate: groupBy=[[person.first_name]], aggr=[[max(person.age)]]
              Filter: person.id > Int64(5) AND person.age > Int64(18)
                TableScan: person
        "#
    );
}

#[test]
fn select_aggregate_with_group_by_with_having_using_column_by_alias() {
    let sql = "SELECT first_name AS fn, MAX(age)
                   FROM person
                   GROUP BY first_name
                   HAVING MAX(age) > 2 AND fn = 'M'";
    let plan = logical_plan(sql).unwrap();
    assert_snapshot!(
        plan,
        @r#"
        Projection: person.first_name AS fn, max(person.age)
          Filter: max(person.age) > Int64(2) AND person.first_name = Utf8("M")
            Aggregate: groupBy=[[person.first_name]], aggr=[[max(person.age)]]
              TableScan: person
        "#
    );
}

#[test]
fn select_aggregate_with_group_by_with_having_using_columns_with_and_without_their_aliases(
) {
    let sql = "SELECT first_name AS fn, MAX(age) AS max_age
                   FROM person
                   GROUP BY first_name
                   HAVING MAX(age) > 2 AND max_age < 5 AND first_name = 'M' AND fn = 'N'";
    let plan = logical_plan(sql).unwrap();
    assert_snapshot!(
        plan,
        @r#"
        Projection: person.first_name AS fn, max(person.age) AS max_age
          Filter: max(person.age) > Int64(2) AND max(person.age) < Int64(5) AND person.first_name = Utf8("M") AND person.first_name = Utf8("N")
            Aggregate: groupBy=[[person.first_name]], aggr=[[max(person.age)]]
              TableScan: person
        "#
    );
}

#[test]
fn select_aggregate_with_group_by_with_having_that_reuses_aggregate() {
    let sql = "SELECT first_name, MAX(age)
                   FROM person
                   GROUP BY first_name
                   HAVING MAX(age) > 100";
    let plan = logical_plan(sql).unwrap();
    assert_snapshot!(
        plan,
        @r#"
        Projection: person.first_name, max(person.age)
          Filter: max(person.age) > Int64(100)
            Aggregate: groupBy=[[person.first_name]], aggr=[[max(person.age)]]
              TableScan: person
        "#
    );
}

#[test]
fn select_aggregate_with_group_by_with_having_referencing_column_not_in_group_by() {
    let sql = "SELECT first_name, MAX(age)
                   FROM person
                   GROUP BY first_name
                   HAVING MAX(age) > 10 AND last_name = 'M'";
    let err = logical_plan(sql).expect_err("query should have failed");

    assert_snapshot!(
        err.strip_backtrace(),
        @r#"
        Error during planning: Column in HAVING must be in GROUP BY or an aggregate function: While expanding wildcard, column "person.last_name" must appear in the GROUP BY clause or must be part of an aggregate function, currently only "person.first_name, max(person.age)" appears in the SELECT clause satisfies this requirement
        "#
    );
}

#[test]
fn select_aggregate_with_group_by_with_having_that_reuses_aggregate_multiple_times() {
    let sql = "SELECT first_name, MAX(age)
                   FROM person
                   GROUP BY first_name
                   HAVING MAX(age) > 100 AND MAX(age) < 200";
    let plan = logical_plan(sql).unwrap();
    assert_snapshot!(
        plan,
        @r#"
        Projection: person.first_name, max(person.age)
          Filter: max(person.age) > Int64(100) AND max(person.age) < Int64(200)
            Aggregate: groupBy=[[person.first_name]], aggr=[[max(person.age)]]
              TableScan: person
        "#
    );
}

#[test]
fn select_aggregate_with_group_by_with_having_using_aggregate_not_in_select() {
    let sql = "SELECT first_name, MAX(age)
                   FROM person
                   GROUP BY first_name
                   HAVING MAX(age) > 100 AND MIN(id) < 50";
    let plan = logical_plan(sql).unwrap();
    assert_snapshot!(
        plan,
        @r#"
        Projection: person.first_name, max(person.age)
          Filter: max(person.age) > Int64(100) AND min(person.id) < Int64(50)
            Aggregate: groupBy=[[person.first_name]], aggr=[[max(person.age), min(person.id)]]
              TableScan: person
        "#
    );
}

#[test]
fn select_aggregate_aliased_with_group_by_with_having_referencing_aggregate_by_its_alias()
{
    let sql = "SELECT first_name, MAX(age) AS max_age
                   FROM person
                   GROUP BY first_name
                   HAVING max_age > 100";
    let plan = logical_plan(sql).unwrap();
    assert_snapshot!(
        plan,
        @r#"
        Projection: person.first_name, max(person.age) AS max_age
          Filter: max(person.age) > Int64(100)
            Aggregate: groupBy=[[person.first_name]], aggr=[[max(person.age)]]
              TableScan: person
        "#
    );
}

#[test]
fn select_aggregate_compound_aliased_with_group_by_with_having_referencing_compound_aggregate_by_its_alias(
) {
    let sql = "SELECT first_name, MAX(age) + 1 AS max_age_plus_one
                   FROM person
                   GROUP BY first_name
                   HAVING max_age_plus_one > 100";
    let plan = logical_plan(sql).unwrap();
    assert_snapshot!(
        plan,
        @r#"
        Projection: person.first_name, max(person.age) + Int64(1) AS max_age_plus_one
          Filter: max(person.age) + Int64(1) > Int64(100)
            Aggregate: groupBy=[[person.first_name]], aggr=[[max(person.age)]]
              TableScan: person
        "#
    );
}

#[test]
fn select_aggregate_with_group_by_with_having_using_derived_column_aggregate_not_in_select(
) {
    let sql = "SELECT first_name, MAX(age)
                   FROM person
                   GROUP BY first_name
                   HAVING MAX(age) > 100 AND MIN(id - 2) < 50";
    let plan = logical_plan(sql).unwrap();
    assert_snapshot!(
        plan,
        @r#"
        Projection: person.first_name, max(person.age)
          Filter: max(person.age) > Int64(100) AND min(person.id - Int64(2)) < Int64(50)
            Aggregate: groupBy=[[person.first_name]], aggr=[[max(person.age), min(person.id - Int64(2))]]
              TableScan: person
        "#
    );
}

#[test]
fn select_aggregate_with_group_by_with_having_using_count_star_not_in_select() {
    let sql = "SELECT first_name, MAX(age)
                   FROM person
                   GROUP BY first_name
                   HAVING MAX(age) > 100 AND count(*) < 50";
    let plan = logical_plan(sql).unwrap();
    assert_snapshot!(
        plan,
        @r#"
        Projection: person.first_name, max(person.age)
          Filter: max(person.age) > Int64(100) AND count(*) < Int64(50)
            Aggregate: groupBy=[[person.first_name]], aggr=[[max(person.age), count(*)]]
              TableScan: person
        "#
    );
}

#[test]
fn select_binary_expr() {
    let sql = "SELECT age + salary from person";
    let plan = logical_plan(sql).unwrap();
    assert_snapshot!(
        plan,
        @r#"
        Projection: person.age + person.salary
          TableScan: person
        "#
    );
}

#[test]
fn select_binary_expr_nested() {
    let sql = "SELECT (age + salary)/2 from person";
    let plan = logical_plan(sql).unwrap();
    assert_snapshot!(
        plan,
        @r#"
        Projection: (person.age + person.salary) / Int64(2)
          TableScan: person
        "#
    );
}

#[test]
fn select_simple_aggregate() {
    let plan = logical_plan("SELECT MIN(age) FROM person").unwrap();
    assert_snapshot!(
        plan,
        @r#"
        Projection: min(person.age)
          Aggregate: groupBy=[[]], aggr=[[min(person.age)]]
            TableScan: person
        "#
    );
}

#[test]
fn test_sum_aggregate() {
    let plan = logical_plan("SELECT sum(age) from person").unwrap();
    assert_snapshot!(
        plan,
        @r#"
        Projection: sum(person.age)
          Aggregate: groupBy=[[]], aggr=[[sum(person.age)]]
            TableScan: person
        "#
    );
}

#[test]
fn select_simple_aggregate_column_does_not_exist() {
    let sql = "SELECT MIN(doesnotexist) FROM person";
    let err = logical_plan(sql).expect_err("query should have failed");
    assert_field_not_found(err, "doesnotexist");
}

#[test]
fn select_simple_aggregate_repeated_aggregate() {
    let sql = "SELECT MIN(age), MIN(age) FROM person";
    let err = logical_plan(sql).expect_err("query should have failed");

    assert_snapshot!(
        err.strip_backtrace(),
        @r#"
        Error during planning: Projections require unique expression names but the expression "min(person.age)" at position 0 and "min(person.age)" at position 1 have the same name. Consider aliasing ("AS") one of them.
        "#
    );
}

#[test]
fn select_simple_aggregate_repeated_aggregate_with_single_alias() {
    let plan = logical_plan("SELECT MIN(age), MIN(age) AS a FROM person").unwrap();
    assert_snapshot!(
        plan,
        @r#"
        Projection: min(person.age), min(person.age) AS a
          Aggregate: groupBy=[[]], aggr=[[min(person.age)]]
            TableScan: person
        "#
    );
}

#[test]
fn select_simple_aggregate_repeated_aggregate_with_unique_aliases() {
    let plan = logical_plan("SELECT MIN(age) AS a, MIN(age) AS b FROM person").unwrap();
    assert_snapshot!(
        plan,
        @r#"
        Projection: min(person.age) AS a, min(person.age) AS b
          Aggregate: groupBy=[[]], aggr=[[min(person.age)]]
            TableScan: person
        "#
    );
}

#[test]
fn select_from_typed_string_values() {
    let plan = logical_plan(
        "SELECT col1, col2 FROM (VALUES (TIMESTAMP '2021-06-10 17:01:00Z', DATE '2004-04-09')) as t (col1, col2)",
    ).unwrap();
    assert_snapshot!(
        plan,
        @r#"
    Projection: t.col1, t.col2
      SubqueryAlias: t
        Projection: column1 AS col1, column2 AS col2
          Values: (CAST(Utf8("2021-06-10 17:01:00Z") AS Timestamp(ns)), CAST(Utf8("2004-04-09") AS Date32))
    "#
    );
}

#[test]
fn select_simple_aggregate_repeated_aggregate_with_repeated_aliases() {
    let sql = "SELECT MIN(age) AS a, MIN(age) AS a FROM person";
    let err = logical_plan(sql).expect_err("query should have failed");

    assert_snapshot!(
        err.strip_backtrace(),
        @r#"
        Error during planning: Projections require unique expression names but the expression "min(person.age) AS a" at position 0 and "min(person.age) AS a" at position 1 have the same name. Consider aliasing ("AS") one of them.
        "#
    );
}

#[test]
fn select_simple_aggregate_with_groupby() {
    let plan =
        logical_plan("SELECT state, MIN(age), MAX(age) FROM person GROUP BY state")
            .unwrap();
    assert_snapshot!(
        plan,
        @r#"
        Projection: person.state, min(person.age), max(person.age)
          Aggregate: groupBy=[[person.state]], aggr=[[min(person.age), max(person.age)]]
            TableScan: person
        "#
    );
}

#[test]
fn select_simple_aggregate_with_groupby_with_aliases() {
    let plan =
        logical_plan("SELECT state AS a, MIN(age) AS b FROM person GROUP BY state")
            .unwrap();
    assert_snapshot!(
        plan,
        @r#"
        Projection: person.state AS a, min(person.age) AS b
          Aggregate: groupBy=[[person.state]], aggr=[[min(person.age)]]
            TableScan: person
        "#
    );
}

#[test]
fn select_simple_aggregate_with_groupby_with_aliases_repeated() {
    let sql = "SELECT state AS a, MIN(age) AS a FROM person GROUP BY state";
    let err = logical_plan(sql).expect_err("query should have failed");

    assert_snapshot!(
        err.strip_backtrace(),
        @r#"
        Error during planning: Projections require unique expression names but the expression "person.state AS a" at position 0 and "min(person.age) AS a" at position 1 have the same name. Consider aliasing ("AS") one of them.
        "#
    );
}

#[test]
fn select_simple_aggregate_with_groupby_column_unselected() {
    let plan =
        logical_plan("SELECT MIN(age), MAX(age) FROM person GROUP BY state").unwrap();
    assert_snapshot!(
        plan,
        @r#"
        Projection: min(person.age), max(person.age)
          Aggregate: groupBy=[[person.state]], aggr=[[min(person.age), max(person.age)]]
            TableScan: person
        "#
    );
}

#[test]
fn select_simple_aggregate_with_groupby_and_column_in_group_by_does_not_exist() {
    let sql = "SELECT sum(age) FROM person GROUP BY doesnotexist";
    let err = logical_plan(sql).expect_err("query should have failed");

    assert_snapshot!(
        err.strip_backtrace(),
        @r#"
        Schema error: No field named doesnotexist. Valid fields are "sum(person.age)", person.id, person.first_name, person.last_name, person.age, person.state, person.salary, person.birth_date, person."😀".
        "#
    );
}

#[test]
fn select_simple_aggregate_with_groupby_and_column_in_aggregate_does_not_exist() {
    let sql = "SELECT sum(doesnotexist) FROM person GROUP BY first_name";
    let err = logical_plan(sql).expect_err("query should have failed");
    assert_field_not_found(err, "doesnotexist");
}

#[test]
fn select_interval_out_of_range() {
    let sql = "SELECT INTERVAL '100000000000000000 day'";
    let err = logical_plan(sql).expect_err("query should have failed");

    assert_snapshot!(
        err.strip_backtrace(),
        @r#"
        Arrow error: Invalid argument error: Unable to represent 100000000000000000 days in a signed 32-bit integer
        "#
    );
}

#[test]
fn select_simple_aggregate_with_groupby_and_column_is_in_aggregate_and_groupby() {
    let plan =
        logical_plan("SELECT MAX(first_name) FROM person GROUP BY first_name").unwrap();
    assert_snapshot!(
        plan,
        @r#"
        Projection: max(person.first_name)
          Aggregate: groupBy=[[person.first_name]], aggr=[[max(person.first_name)]]
            TableScan: person
        "#
    );
}

#[test]
fn select_simple_aggregate_with_groupby_can_use_positions() {
    let plan = logical_plan("SELECT state, age AS b, count(1) FROM person GROUP BY 1, 2")
        .unwrap();
    assert_snapshot!(
        plan,
        @r#"
        Projection: person.state, person.age AS b, count(Int64(1))
          Aggregate: groupBy=[[person.state, person.age]], aggr=[[count(Int64(1))]]
            TableScan: person
        "#
    );
    let plan = logical_plan("SELECT state, age AS b, count(1) FROM person GROUP BY 2, 1")
        .unwrap();
    assert_snapshot!(
        plan,
        @r#"
        Projection: person.state, person.age AS b, count(Int64(1))
          Aggregate: groupBy=[[person.age, person.state]], aggr=[[count(Int64(1))]]
            TableScan: person
        "#
    );
}

#[test]
fn select_simple_aggregate_with_groupby_position_out_of_range() {
    let sql = "SELECT state, MIN(age) FROM person GROUP BY 0";
    let err = logical_plan(sql).expect_err("query should have failed");

    assert_snapshot!(
        err.strip_backtrace(),
        @r#"
        Error during planning: Cannot find column with position 0 in SELECT clause. Valid columns: 1 to 2
        "#
    );

    let sql2 = "SELECT state, MIN(age) FROM person GROUP BY 5";
    let err2 = logical_plan(sql2).expect_err("query should have failed");

    assert_snapshot!(
        err2.strip_backtrace(),
        @r#"
        Error during planning: Cannot find column with position 5 in SELECT clause. Valid columns: 1 to 2
        "#
    );
}

#[test]
fn select_simple_aggregate_with_groupby_can_use_alias() {
    let plan =
        logical_plan("SELECT state AS a, MIN(age) AS b FROM person GROUP BY a").unwrap();
    assert_snapshot!(
        plan,
        @r#"
        Projection: person.state AS a, min(person.age) AS b
          Aggregate: groupBy=[[person.state]], aggr=[[min(person.age)]]
            TableScan: person
        "#
    );
}

#[test]
fn select_simple_aggregate_with_groupby_aggregate_repeated() {
    let sql = "SELECT state, MIN(age), MIN(age) FROM person GROUP BY state";
    let err = logical_plan(sql).expect_err("query should have failed");

    assert_snapshot!(
        err.strip_backtrace(),
        @r#"
        Error during planning: Projections require unique expression names but the expression "min(person.age)" at position 1 and "min(person.age)" at position 2 have the same name. Consider aliasing ("AS") one of them.
        "#
    );
}

#[test]
fn select_simple_aggregate_with_groupby_aggregate_repeated_and_one_has_alias() {
    let plan =
        logical_plan("SELECT state, MIN(age), MIN(age) AS ma FROM person GROUP BY state")
            .unwrap();
    assert_snapshot!(
        plan,
        @r#"
        Projection: person.state, min(person.age), min(person.age) AS ma
          Aggregate: groupBy=[[person.state]], aggr=[[min(person.age)]]
            TableScan: person
        "#
    );
}

#[test]
fn select_simple_aggregate_with_groupby_non_column_expression_unselected() {
    let plan =
        logical_plan("SELECT MIN(first_name) FROM person GROUP BY age + 1").unwrap();
    assert_snapshot!(
        plan,
        @r#"
        Projection: min(person.first_name)
          Aggregate: groupBy=[[person.age + Int64(1)]], aggr=[[min(person.first_name)]]
            TableScan: person
        "#
    );
}

#[test]
fn select_simple_aggregate_with_groupby_non_column_expression_selected_and_resolvable() {
    let plan =
        logical_plan("SELECT age + 1, MIN(first_name) FROM person GROUP BY age + 1")
            .unwrap();
    assert_snapshot!(
        plan,
        @r#"
        Projection: person.age + Int64(1), min(person.first_name)
          Aggregate: groupBy=[[person.age + Int64(1)]], aggr=[[min(person.first_name)]]
            TableScan: person
        "#
    );
    let plan =
        logical_plan("SELECT MIN(first_name), age + 1 FROM person GROUP BY age + 1")
            .unwrap();
    assert_snapshot!(
        plan,
        @r#"
        Projection: min(person.first_name), person.age + Int64(1)
          Aggregate: groupBy=[[person.age + Int64(1)]], aggr=[[min(person.first_name)]]
            TableScan: person
        "#
    );
}

#[test]
fn select_simple_aggregate_with_groupby_non_column_expression_nested_and_resolvable() {
    let plan = logical_plan(
        "SELECT ((age + 1) / 2) * (age + 1), MIN(first_name) FROM person GROUP BY age + 1"
    ).unwrap();
    assert_snapshot!(
        plan,
        @r#"
        Projection: person.age + Int64(1) / Int64(2) * person.age + Int64(1), min(person.first_name)
          Aggregate: groupBy=[[person.age + Int64(1)]], aggr=[[min(person.first_name)]]
            TableScan: person
        "#
    );
}

#[test]
fn select_simple_aggregate_with_groupby_non_column_expression_nested_and_not_resolvable()
{
    // The query should fail, because age + 9 is not in the group by.
    let sql = "SELECT ((age + 1) / 2) * (age + 9), MIN(first_name) FROM person GROUP BY age + 1";
    let err = logical_plan(sql).expect_err("query should have failed");

    assert_snapshot!(
        err.strip_backtrace(),
        @r#"
        Error during planning: Column in SELECT must be in GROUP BY or an aggregate function: While expanding wildcard, column "person.age" must appear in the GROUP BY clause or must be part of an aggregate function, currently only "person.age + Int64(1), min(person.first_name)" appears in the SELECT clause satisfies this requirement
        "#
    );
}

#[test]
fn select_simple_aggregate_with_groupby_non_column_expression_and_its_column_selected() {
    let sql = "SELECT age, MIN(first_name) FROM person GROUP BY age + 1";
    let err = logical_plan(sql).expect_err("query should have failed");

    assert_snapshot!(
        err.strip_backtrace(),
        @r#"
        Error during planning: Column in SELECT must be in GROUP BY or an aggregate function: While expanding wildcard, column "person.age" must appear in the GROUP BY clause or must be part of an aggregate function, currently only "person.age + Int64(1), min(person.first_name)" appears in the SELECT clause satisfies this requirement
        "#
    );
}

#[test]
fn select_simple_aggregate_nested_in_binary_expr_with_groupby() {
    let plan =
        logical_plan("SELECT state, MIN(age) < 10 FROM person GROUP BY state").unwrap();
    assert_snapshot!(
        plan,
        @r#"
        Projection: person.state, min(person.age) < Int64(10)
          Aggregate: groupBy=[[person.state]], aggr=[[min(person.age)]]
            TableScan: person
        "#
    );
}

#[test]
fn select_simple_aggregate_and_nested_groupby_column() {
    let plan =
        logical_plan("SELECT MAX(first_name), age + 1 FROM person GROUP BY age").unwrap();
    assert_snapshot!(
        plan,
        @r#"
        Projection: max(person.first_name), person.age + Int64(1)
          Aggregate: groupBy=[[person.age]], aggr=[[max(person.first_name)]]
            TableScan: person
        "#
    );
}

#[test]
fn select_aggregate_compounded_with_groupby_column() {
    let plan = logical_plan("SELECT age + MIN(salary) FROM person GROUP BY age").unwrap();
    assert_snapshot!(
        plan,
        @r#"
        Projection: person.age + min(person.salary)
          Aggregate: groupBy=[[person.age]], aggr=[[min(person.salary)]]
            TableScan: person
        "#
    );
}

#[test]
fn select_aggregate_with_non_column_inner_expression_with_groupby() {
    let plan =
        logical_plan("SELECT state, MIN(age + 1) FROM person GROUP BY state").unwrap();
    assert_snapshot!(
        plan,
        @r#"
        Projection: person.state, min(person.age + Int64(1))
          Aggregate: groupBy=[[person.state]], aggr=[[min(person.age + Int64(1))]]
            TableScan: person
        "#
    );
}

#[test]
fn select_count_one() {
    let sql = "SELECT count(1) FROM person";
    let plan = logical_plan(sql).unwrap();
    assert_snapshot!(
        plan,
        @r#"
Projection: count(Int64(1))
  Aggregate: groupBy=[[]], aggr=[[count(Int64(1))]]
    TableScan: person
"#
    );
}

#[test]
fn select_count_column() {
    let sql = "SELECT count(id) FROM person";
    let plan = logical_plan(sql).unwrap();
    assert_snapshot!(
        plan,
        @r#"
Projection: count(person.id)
  Aggregate: groupBy=[[]], aggr=[[count(person.id)]]
    TableScan: person
"#
    );
}

#[test]
fn select_approx_median() {
    let sql = "SELECT approx_median(age) FROM person";
    let plan = logical_plan(sql).unwrap();
    assert_snapshot!(
        plan,
        @r#"
Projection: approx_median(person.age)
  Aggregate: groupBy=[[]], aggr=[[approx_median(person.age)]]
    TableScan: person
"#
    );
}

#[test]
fn select_scalar_func() {
    let sql = "SELECT sqrt(age) FROM person";
    let plan = logical_plan(sql).unwrap();
    assert_snapshot!(
        plan,
        @r#"
Projection: sqrt(person.age)
  TableScan: person
"#
    );
}

#[test]
fn select_aliased_scalar_func() {
    let sql = "SELECT sqrt(person.age) AS square_people FROM person";
    let plan = logical_plan(sql).unwrap();
    assert_snapshot!(
        plan,
        @r#"
Projection: sqrt(person.age) AS square_people
  TableScan: person
"#
    );
}

#[test]
fn select_where_nullif_division() {
    let sql = "SELECT c3/(c4+c5) \
                   FROM aggregate_test_100 WHERE c3/nullif(c4+c5, 0) > 0.1";
    let plan = logical_plan(sql).unwrap();
    assert_snapshot!(
        plan,
        @r#"
Projection: aggregate_test_100.c3 / (aggregate_test_100.c4 + aggregate_test_100.c5)
  Filter: aggregate_test_100.c3 / nullif(aggregate_test_100.c4 + aggregate_test_100.c5, Int64(0)) > Float64(0.1)
    TableScan: aggregate_test_100
"#
    );
}

#[test]
fn select_where_with_negative_operator() {
    let sql = "SELECT c3 FROM aggregate_test_100 WHERE c3 > -0.1 AND -c4 > 0";
    let plan = logical_plan(sql).unwrap();
    assert_snapshot!(
        plan,
        @r#"
Projection: aggregate_test_100.c3
  Filter: aggregate_test_100.c3 > Float64(-0.1) AND (- aggregate_test_100.c4) > Int64(0)
    TableScan: aggregate_test_100
"#
    );
}

#[test]
fn select_where_with_positive_operator() {
    let sql = "SELECT c3 FROM aggregate_test_100 WHERE c3 > +0.1 AND +c4 > 0";
    let plan = logical_plan(sql).unwrap();
    assert_snapshot!(
        plan,
        @r#"
Projection: aggregate_test_100.c3
  Filter: aggregate_test_100.c3 > Float64(0.1) AND aggregate_test_100.c4 > Int64(0)
    TableScan: aggregate_test_100
"#
    );
}

#[test]
fn select_where_compound_identifiers() {
    let sql = "SELECT aggregate_test_100.c3 \
    FROM public.aggregate_test_100 \
    WHERE aggregate_test_100.c3 > 0.1";
    let plan = logical_plan(sql).unwrap();
    assert_snapshot!(
        plan,
        @r#"
Projection: public.aggregate_test_100.c3
  Filter: public.aggregate_test_100.c3 > Float64(0.1)
    TableScan: public.aggregate_test_100
"#
    );
}

#[test]
fn select_order_by_index() {
    let sql = "SELECT id FROM person ORDER BY 1";
    let plan = logical_plan(sql).unwrap();
    assert_snapshot!(
        plan,
        @r#"
Sort: person.id ASC NULLS LAST
  Projection: person.id
    TableScan: person
"#
    );
}

#[test]
fn select_order_by_multiple_index() {
    let sql = "SELECT id, state, age FROM person ORDER BY 1, 3";
    let plan = logical_plan(sql).unwrap();
    assert_snapshot!(
        plan,
        @r#"
Sort: person.id ASC NULLS LAST, person.age ASC NULLS LAST
  Projection: person.id, person.state, person.age
    TableScan: person
"#
    );
}

#[test]
fn select_order_by_index_of_0() {
    let sql = "SELECT id FROM person ORDER BY 0";
    let err = logical_plan(sql)
        .expect_err("query should have failed")
        .strip_backtrace();

    assert_snapshot!(
        err,
        @r#"
        Error during planning: Order by index starts at 1 for column indexes
        "#
    );
}

#[test]
fn select_order_by_index_oob() {
    let sql = "SELECT id FROM person ORDER BY 2";
    let err = logical_plan(sql)
        .expect_err("query should have failed")
        .strip_backtrace();

    assert_snapshot!(
        err,
        @r#"
        Error during planning: Order by column out of bounds, specified: 2, max: 1
        "#
    );
}

#[test]
fn select_with_order_by() {
    let sql = "SELECT id FROM person ORDER BY id";
    let plan = logical_plan(sql).unwrap();
    assert_snapshot!(
        plan,
        @r#"
Sort: person.id ASC NULLS LAST
  Projection: person.id
    TableScan: person
"#
    );
}

#[test]
fn select_order_by_desc() {
    let sql = "SELECT id FROM person ORDER BY id DESC";
    let plan = logical_plan(sql).unwrap();
    assert_snapshot!(
        plan,
        @r#"
Sort: person.id DESC NULLS FIRST
  Projection: person.id
    TableScan: person
"#
    );
}

#[test]
fn select_order_by_nulls_last() {
    let plan = logical_plan("SELECT id FROM person ORDER BY id DESC NULLS LAST").unwrap();
    assert_snapshot!(
        plan,
        @r#"
Sort: person.id DESC NULLS LAST
  Projection: person.id
    TableScan: person
"#
    );

    let plan = logical_plan("SELECT id FROM person ORDER BY id NULLS LAST").unwrap();
    assert_snapshot!(
        plan,
        @r#"
Sort: person.id ASC NULLS LAST
  Projection: person.id
    TableScan: person
"#
    );
}

#[test]
fn select_group_by() {
    let sql = "SELECT state FROM person GROUP BY state";
    let plan = logical_plan(sql).unwrap();
    assert_snapshot!(
        plan,
        @r#"
Projection: person.state
  Aggregate: groupBy=[[person.state]], aggr=[[]]
    TableScan: person
"#
    );
}

#[test]
fn select_group_by_columns_not_in_select() {
    let sql = "SELECT MAX(age) FROM person GROUP BY state";
    let plan = logical_plan(sql).unwrap();
    assert_snapshot!(
        plan,
        @r#"
Projection: max(person.age)
  Aggregate: groupBy=[[person.state]], aggr=[[max(person.age)]]
    TableScan: person
"#
    );
}

#[test]
fn select_group_by_count_star() {
    let sql = "SELECT state, count(*) FROM person GROUP BY state";
    let plan = logical_plan(sql).unwrap();
    assert_snapshot!(
        plan,
        @r#"
Projection: person.state, count(*)
  Aggregate: groupBy=[[person.state]], aggr=[[count(*)]]
    TableScan: person
"#
    );
}

#[test]
fn select_group_by_needs_projection() {
    let sql = "SELECT count(state), state FROM person GROUP BY state";
    let plan = logical_plan(sql).unwrap();
    assert_snapshot!(
        plan,
        @r#"
        Projection: count(person.state), person.state
          Aggregate: groupBy=[[person.state]], aggr=[[count(person.state)]]
            TableScan: person
        "#
    );
}

#[test]
fn select_7480_1() {
    let sql = "SELECT c1, MIN(c12) FROM aggregate_test_100 GROUP BY c1, c13";
    let plan = logical_plan(sql).unwrap();
    assert_snapshot!(
        plan,
        @r#"
Projection: aggregate_test_100.c1, min(aggregate_test_100.c12)
  Aggregate: groupBy=[[aggregate_test_100.c1, aggregate_test_100.c13]], aggr=[[min(aggregate_test_100.c12)]]
    TableScan: aggregate_test_100
"#
    );
}

#[test]
fn select_7480_2() {
    let sql = "SELECT c1, c13, MIN(c12) FROM aggregate_test_100 GROUP BY c1";
    let err = logical_plan(sql).expect_err("query should have failed");

    assert_snapshot!(
        err.strip_backtrace(),
        @r#"
        Error during planning: Column in SELECT must be in GROUP BY or an aggregate function: While expanding wildcard, column "aggregate_test_100.c13" must appear in the GROUP BY clause or must be part of an aggregate function, currently only "aggregate_test_100.c1, min(aggregate_test_100.c12)" appears in the SELECT clause satisfies this requirement
        "#
    );
}

#[test]
fn create_external_table_csv() {
    let sql = "CREATE EXTERNAL TABLE t(c1 int) STORED AS CSV LOCATION 'foo.csv'";
    let plan = logical_plan(sql).unwrap();
    assert_snapshot!(
        plan,
        @r#"
CreateExternalTable: Bare { table: "t" }
"#
    );
}

#[test]
fn create_external_table_with_pk() {
    let sql = "CREATE EXTERNAL TABLE t(c1 int, primary key(c1)) STORED AS CSV LOCATION 'foo.csv'";
    let plan = logical_plan(sql).unwrap();
    assert_snapshot!(
        plan,
        @r#"
CreateExternalTable: Bare { table: "t" } constraints=[PrimaryKey([0])]
    "#
    );
}

#[test]
fn create_external_table_wih_schema() {
    let sql = "CREATE EXTERNAL TABLE staging.foo STORED AS CSV LOCATION 'foo.csv'";
    let plan = logical_plan(sql).unwrap();
    assert_snapshot!(
        plan,
        @r#"
CreateExternalTable: Partial { schema: "staging", table: "foo" }
"#
    );
}

#[test]
fn create_schema_with_quoted_name() {
    let sql = "CREATE SCHEMA \"quoted_schema_name\"";
    let plan = logical_plan(sql).unwrap();
    assert_snapshot!(
        plan,
        @r#"
CreateCatalogSchema: "quoted_schema_name"
"#
    );
}

#[test]
fn create_schema_with_quoted_unnormalized_name() {
    let sql = "CREATE SCHEMA \"Foo\"";
    let plan = logical_plan(sql).unwrap();
    assert_snapshot!(
        plan,
        @r#"
CreateCatalogSchema: "Foo"
"#
    );
}

#[test]
fn create_schema_with_unquoted_normalized_name() {
    let sql = "CREATE SCHEMA Foo";
    let plan = logical_plan(sql).unwrap();
    assert_snapshot!(
        plan,
        @r#"
CreateCatalogSchema: "foo"
"#
    );
}

#[test]
fn create_external_table_custom() {
    let sql = "CREATE EXTERNAL TABLE dt STORED AS DELTATABLE LOCATION 's3://bucket/schema/table';";
    let plan = logical_plan(sql).unwrap();
    assert_snapshot!(
        plan,
        @r#"
CreateExternalTable: Bare { table: "dt" }
"#
    );
}

#[test]
fn create_external_table_csv_no_schema() {
    let sql = "CREATE EXTERNAL TABLE t STORED AS CSV LOCATION 'foo.csv'";
    let plan = logical_plan(sql).unwrap();
    assert_snapshot!(
        plan,
        @r#"
CreateExternalTable: Bare { table: "t" }
"#
    );
}

#[test]
fn create_external_table_with_compression_type() {
    // positive case
    let sqls = vec![
        "CREATE EXTERNAL TABLE t(c1 int) STORED AS CSV LOCATION 'foo.csv.gz' OPTIONS ('format.compression' 'gzip')",
        "CREATE EXTERNAL TABLE t(c1 int) STORED AS CSV LOCATION 'foo.csv.bz2' OPTIONS ('format.compression' 'bzip2')",
        "CREATE EXTERNAL TABLE t(c1 int) STORED AS JSON LOCATION 'foo.json.gz' OPTIONS ('format.compression' 'gzip')",
        "CREATE EXTERNAL TABLE t(c1 int) STORED AS JSON LOCATION 'foo.json.bz2' OPTIONS ('format.compression' 'bzip2')",
        "CREATE EXTERNAL TABLE t(c1 int) STORED AS NONSTANDARD LOCATION 'foo.unk' OPTIONS ('format.compression' 'gzip')",
         ];

    allow_duplicates! {
        for sql in sqls {
            let plan = logical_plan(sql).unwrap();
            assert_snapshot!(
                plan,
                @r#"
                CreateExternalTable: Bare { table: "t" }
                "#
            );
        }

    }

    // negative case
    let sqls = vec![
        "CREATE EXTERNAL TABLE t STORED AS AVRO LOCATION 'foo.avro' OPTIONS ('format.compression' 'gzip')",
        "CREATE EXTERNAL TABLE t STORED AS AVRO LOCATION 'foo.avro' OPTIONS ('format.compression' 'bzip2')",
        "CREATE EXTERNAL TABLE t STORED AS PARQUET LOCATION 'foo.parquet' OPTIONS ('format.compression' 'gzip')",
        "CREATE EXTERNAL TABLE t STORED AS PARQUET LOCATION 'foo.parquet' OPTIONS ('format.compression' 'bzip2')",
        "CREATE EXTERNAL TABLE t STORED AS ARROW LOCATION 'foo.arrow' OPTIONS ('format.compression' 'gzip')",
        "CREATE EXTERNAL TABLE t STORED AS ARROW LOCATION 'foo.arrow' OPTIONS ('format.compression' 'bzip2')",
    ];

    allow_duplicates! {
        for sql in sqls {
            let err = logical_plan(sql).expect_err("query should have failed");

            assert_snapshot!(
                err.strip_backtrace(),
                @r#"
                Error during planning: File compression type cannot be set for PARQUET, AVRO, or ARROW files.
                "#
            );

        }
    }
}

#[test]
fn create_external_table_parquet() {
    let sql = "CREATE EXTERNAL TABLE t(c1 int) STORED AS PARQUET LOCATION 'foo.parquet'";
    let plan = logical_plan(sql).unwrap();
    assert_snapshot!(
        plan,
        @r#"
CreateExternalTable: Bare { table: "t" }
"#
    );
}

#[test]
fn create_external_table_parquet_sort_order() {
    let sql = "create external table foo(a varchar, b varchar, c timestamp) stored as parquet location '/tmp/foo' with order (c)";
    let plan = logical_plan(sql).unwrap();
    assert_snapshot!(
        plan,
        @r#"
CreateExternalTable: Bare { table: "foo" }
"#
    );
}

#[test]
fn create_external_table_parquet_no_schema() {
    let sql = "CREATE EXTERNAL TABLE t STORED AS PARQUET LOCATION 'foo.parquet'";
    let plan = logical_plan(sql).unwrap();
    assert_snapshot!(
        plan,
        @r#"CreateExternalTable: Bare { table: "t" }"#
    );
}

#[test]
fn create_external_table_parquet_no_schema_sort_order() {
    let sql = "CREATE EXTERNAL TABLE t STORED AS PARQUET LOCATION 'foo.parquet' WITH ORDER (id)";
    let plan = logical_plan(sql).unwrap();
    assert_snapshot!(
        plan,
        @r#"
CreateExternalTable: Bare { table: "t" }
"#
    );
}

#[test]
fn equijoin_explicit_syntax() {
    let sql = "SELECT id, order_id \
            FROM person \
            JOIN orders \
            ON id = customer_id";
    let plan = logical_plan(sql).unwrap();
    assert_snapshot!(
        plan,
        @r#"
Projection: person.id, orders.order_id
  Inner Join:  Filter: person.id = orders.customer_id
    TableScan: person
    TableScan: orders
"#
    );
}

#[test]
fn equijoin_with_condition() {
    let sql = "SELECT id, order_id \
            FROM person \
            JOIN orders \
            ON id = customer_id AND order_id > 1 ";
    let plan = logical_plan(sql).unwrap();
    assert_snapshot!(
        plan,
        @r#"
Projection: person.id, orders.order_id
  Inner Join:  Filter: person.id = orders.customer_id AND orders.order_id > Int64(1)
    TableScan: person
    TableScan: orders
"#
    );
}

#[test]
fn left_equijoin_with_conditions() {
    let sql = "SELECT id, order_id \
            FROM person \
            LEFT JOIN orders \
            ON id = customer_id AND order_id > 1 AND age < 30";
    let plan = logical_plan(sql).unwrap();
    assert_snapshot!(
        plan,
        @r#"
Projection: person.id, orders.order_id
  Left Join:  Filter: person.id = orders.customer_id AND orders.order_id > Int64(1) AND person.age < Int64(30)
    TableScan: person
    TableScan: orders
"#
    );
}

#[test]
fn right_equijoin_with_conditions() {
    let sql = "SELECT id, order_id \
            FROM person \
            RIGHT JOIN orders \
            ON id = customer_id AND id > 1 AND order_id < 100";
    let plan = logical_plan(sql).unwrap();
    assert_snapshot!(
        plan,
        @r#"
Projection: person.id, orders.order_id
  Right Join:  Filter: person.id = orders.customer_id AND person.id > Int64(1) AND orders.order_id < Int64(100)
    TableScan: person
    TableScan: orders
"#
    );
}

#[test]
fn full_equijoin_with_conditions() {
    let sql = "SELECT id, order_id \
            FROM person \
            FULL JOIN orders \
            ON id = customer_id AND id > 1 AND order_id < 100";
    let plan = logical_plan(sql).unwrap();
    assert_snapshot!(
        plan,
        @r#"
Projection: person.id, orders.order_id
  Full Join:  Filter: person.id = orders.customer_id AND person.id > Int64(1) AND orders.order_id < Int64(100)
    TableScan: person
    TableScan: orders
"#
    );
}

#[test]
fn join_with_table_name() {
    let sql = "SELECT id, order_id \
            FROM person \
            JOIN orders \
            ON person.id = orders.customer_id";
    let plan = logical_plan(sql).unwrap();
    assert_snapshot!(
        plan,
        @r#"
Projection: person.id, orders.order_id
  Inner Join:  Filter: person.id = orders.customer_id
    TableScan: person
    TableScan: orders
"#
    );
}

#[test]
fn join_with_using() {
    let sql = "SELECT person.first_name, id \
            FROM person \
            JOIN person as person2 \
            USING (id)";
    let plan = logical_plan(sql).unwrap();
    assert_snapshot!(
        plan,
        @r#"
Projection: person.first_name, person.id
  Inner Join: Using person.id = person2.id
    TableScan: person
    SubqueryAlias: person2
      TableScan: person
"#
    );
}

#[test]
fn equijoin_explicit_syntax_3_tables() {
    let sql = "SELECT id, order_id, l_description \
            FROM person \
            JOIN orders ON id = customer_id \
            JOIN lineitem ON o_item_id = l_item_id";
    let plan = logical_plan(sql).unwrap();
    assert_snapshot!(
        plan,
        @r#"
Projection: person.id, orders.order_id, lineitem.l_description
  Inner Join:  Filter: orders.o_item_id = lineitem.l_item_id
    Inner Join:  Filter: person.id = orders.customer_id
      TableScan: person
      TableScan: orders
    TableScan: lineitem
"#
    );
}

#[test]
fn boolean_literal_in_condition_expression() {
    let sql = "SELECT order_id \
        FROM orders \
        WHERE delivered = false OR delivered = true";
    let plan = logical_plan(sql).unwrap();
    assert_snapshot!(
        plan,
        @r#"
Projection: orders.order_id
  Filter: orders.delivered = Boolean(false) OR orders.delivered = Boolean(true)
    TableScan: orders
"#
    );
}

#[test]
fn union() {
    let sql = "SELECT order_id from orders UNION SELECT order_id FROM orders";
    let plan = logical_plan(sql).unwrap();
    assert_snapshot!(
        plan,
        @r#"
Distinct:
  Union
    Projection: orders.order_id
      TableScan: orders
    Projection: orders.order_id
      TableScan: orders
"#
    );
}

#[test]
fn union_by_name_different_columns() {
    let sql = "SELECT order_id from orders UNION BY NAME SELECT order_id, 1 FROM orders";
    let plan = logical_plan(sql).unwrap();
    assert_snapshot!(
        plan,
        @r#"
Distinct:
  Union
    Projection: order_id, NULL AS Int64(1)
      Projection: orders.order_id
        TableScan: orders
    Projection: order_id, Int64(1)
      Projection: orders.order_id, Int64(1)
        TableScan: orders
"#
    );
}

#[test]
fn union_by_name_same_column_names() {
    let sql = "SELECT order_id from orders UNION SELECT order_id FROM orders";
    let plan = logical_plan(sql).unwrap();
    assert_snapshot!(
        plan,
        @r#"
Distinct:
  Union
    Projection: orders.order_id
      TableScan: orders
    Projection: orders.order_id
      TableScan: orders
"#
    );
}

#[test]
fn union_all() {
    let sql = "SELECT order_id from orders UNION ALL SELECT order_id FROM orders";
    let plan = logical_plan(sql).unwrap();
    assert_snapshot!(
        plan,
        @r#"
Union
  Projection: orders.order_id
    TableScan: orders
  Projection: orders.order_id
    TableScan: orders
"#
    );
}

#[test]
fn union_all_by_name_different_columns() {
    let sql =
        "SELECT order_id from orders UNION ALL BY NAME SELECT order_id, 1 FROM orders";
    let plan = logical_plan(sql).unwrap();
    assert_snapshot!(
        plan,
        @r#"
Union
  Projection: order_id, NULL AS Int64(1)
    Projection: orders.order_id
      TableScan: orders
  Projection: order_id, Int64(1)
    Projection: orders.order_id, Int64(1)
      TableScan: orders
"#
    );
}

#[test]
fn union_all_by_name_same_column_names() {
    let sql = "SELECT order_id from orders UNION ALL BY NAME SELECT order_id FROM orders";
    let plan = logical_plan(sql).unwrap();
    assert_snapshot!(
        plan,
        @r#"
Union
  Projection: order_id
    Projection: orders.order_id
      TableScan: orders
  Projection: order_id
    Projection: orders.order_id
      TableScan: orders
"#
    );
}

#[test]
fn empty_over() {
    let sql = "SELECT order_id, MAX(order_id) OVER () from orders";
    let plan = logical_plan(sql).unwrap();
    assert_snapshot!(
        plan,
        @r#"
Projection: orders.order_id, max(orders.order_id) ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
  WindowAggr: windowExpr=[[max(orders.order_id) ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING]]
    TableScan: orders
"#
    );
}

#[test]
fn empty_over_with_alias() {
    let sql = "SELECT order_id oid, MAX(order_id) OVER () max_oid from orders";
    let plan = logical_plan(sql).unwrap();
    assert_snapshot!(
        plan,
        @r#"
Projection: orders.order_id AS oid, max(orders.order_id) ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING AS max_oid
  WindowAggr: windowExpr=[[max(orders.order_id) ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING]]
    TableScan: orders
"#
    );
}

#[test]
fn empty_over_dup_with_alias() {
    let sql = "SELECT order_id oid, MAX(order_id) OVER () max_oid, MAX(order_id) OVER () max_oid_dup from orders";
    let plan = logical_plan(sql).unwrap();
    assert_snapshot!(
        plan,
        @r#"
Projection: orders.order_id AS oid, max(orders.order_id) ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING AS max_oid, max(orders.order_id) ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING AS max_oid_dup
  WindowAggr: windowExpr=[[max(orders.order_id) ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING]]
    TableScan: orders
"#
    );
}

#[test]
fn empty_over_dup_with_different_sort() {
    let sql = "SELECT order_id oid, MAX(order_id) OVER (), MAX(order_id) OVER (ORDER BY order_id) from orders";
    let plan = logical_plan(sql).unwrap();
    assert_snapshot!(
        plan,
        @r#"
Projection: orders.order_id AS oid, max(orders.order_id) ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING, max(orders.order_id) ORDER BY [orders.order_id ASC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  WindowAggr: windowExpr=[[max(orders.order_id) ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING]]
    WindowAggr: windowExpr=[[max(orders.order_id) ORDER BY [orders.order_id ASC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW]]
      TableScan: orders
"#
    );
}

#[test]
fn empty_over_plus() {
    let sql = "SELECT order_id, MAX(qty * 1.1) OVER () from orders";
    let plan = logical_plan(sql).unwrap();
    assert_snapshot!(
        plan,
        @r#"
Projection: orders.order_id, max(orders.qty * Float64(1.1)) ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
  WindowAggr: windowExpr=[[max(orders.qty * Float64(1.1)) ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING]]
    TableScan: orders
"#
    );
}

#[test]
fn empty_over_multiple() {
    let sql = "SELECT order_id, MAX(qty) OVER (), min(qty) over (), avg(qty) OVER () from orders";
    let plan = logical_plan(sql).unwrap();
    assert_snapshot!(
        plan,
        @r#"
Projection: orders.order_id, max(orders.qty) ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING, min(orders.qty) ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING, avg(orders.qty) ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
  WindowAggr: windowExpr=[[max(orders.qty) ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING, min(orders.qty) ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING, avg(orders.qty) ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING]]
    TableScan: orders
"#
    );
}

/// psql result
/// ```text
///                               QUERY PLAN
/// ----------------------------------------------------------------------
/// WindowAgg  (cost=69.83..87.33 rows=1000 width=8)
///   ->  Sort  (cost=69.83..72.33 rows=1000 width=8)
///         Sort Key: order_id
///         ->  Seq Scan on orders  (cost=0.00..20.00 rows=1000 width=8)
/// ```
#[test]
fn over_partition_by() {
    let sql = "SELECT order_id, MAX(qty) OVER (PARTITION BY order_id) from orders";
    let plan = logical_plan(sql).unwrap();
    assert_snapshot!(
        plan,
        @r#"
Projection: orders.order_id, max(orders.qty) PARTITION BY [orders.order_id] ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
  WindowAggr: windowExpr=[[max(orders.qty) PARTITION BY [orders.order_id] ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING]]
    TableScan: orders
"#
    );
}

/// psql result
/// ```text
///                                     QUERY PLAN
/// ----------------------------------------------------------------------------------
/// WindowAgg  (cost=137.16..154.66 rows=1000 width=12)
/// ->  Sort  (cost=137.16..139.66 rows=1000 width=12)
///         Sort Key: order_id
///         ->  WindowAgg  (cost=69.83..87.33 rows=1000 width=12)
///             ->  Sort  (cost=69.83..72.33 rows=1000 width=8)
///                     Sort Key: order_id DESC
///                     ->  Seq Scan on orders  (cost=0.00..20.00 rows=1000 width=8)
/// ```
#[test]
fn over_order_by() {
    let sql = "SELECT order_id, MAX(qty) OVER (ORDER BY order_id), MIN(qty) OVER (ORDER BY order_id DESC) from orders";
    let plan = logical_plan(sql).unwrap();
    assert_snapshot!(
        plan,
        @r#"
Projection: orders.order_id, max(orders.qty) ORDER BY [orders.order_id ASC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW, min(orders.qty) ORDER BY [orders.order_id DESC NULLS FIRST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  WindowAggr: windowExpr=[[max(orders.qty) ORDER BY [orders.order_id ASC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW]]
    WindowAggr: windowExpr=[[min(orders.qty) ORDER BY [orders.order_id DESC NULLS FIRST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW]]
      TableScan: orders
"#
    );
}

#[test]
fn over_order_by_with_window_frame_double_end() {
    let sql = "SELECT order_id, MAX(qty) OVER (ORDER BY order_id ROWS BETWEEN 3 PRECEDING and 3 FOLLOWING), MIN(qty) OVER (ORDER BY order_id DESC) from orders";
    let plan = logical_plan(sql).unwrap();
    assert_snapshot!(
        plan,
        @r#"
Projection: orders.order_id, max(orders.qty) ORDER BY [orders.order_id ASC NULLS LAST] ROWS BETWEEN 3 PRECEDING AND 3 FOLLOWING, min(orders.qty) ORDER BY [orders.order_id DESC NULLS FIRST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  WindowAggr: windowExpr=[[max(orders.qty) ORDER BY [orders.order_id ASC NULLS LAST] ROWS BETWEEN 3 PRECEDING AND 3 FOLLOWING]]
    WindowAggr: windowExpr=[[min(orders.qty) ORDER BY [orders.order_id DESC NULLS FIRST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW]]
      TableScan: orders
"#
    );
}

#[test]
fn over_order_by_with_window_frame_single_end() {
    let sql = "SELECT order_id, MAX(qty) OVER (ORDER BY order_id ROWS 3 PRECEDING), MIN(qty) OVER (ORDER BY order_id DESC) from orders";
    let plan = logical_plan(sql).unwrap();
    assert_snapshot!(
        plan,
        @r#"
Projection: orders.order_id, max(orders.qty) ORDER BY [orders.order_id ASC NULLS LAST] ROWS BETWEEN 3 PRECEDING AND CURRENT ROW, min(orders.qty) ORDER BY [orders.order_id DESC NULLS FIRST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  WindowAggr: windowExpr=[[max(orders.qty) ORDER BY [orders.order_id ASC NULLS LAST] ROWS BETWEEN 3 PRECEDING AND CURRENT ROW]]
    WindowAggr: windowExpr=[[min(orders.qty) ORDER BY [orders.order_id DESC NULLS FIRST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW]]
      TableScan: orders
"#
    );
}

#[test]
fn over_order_by_with_window_frame_single_end_groups() {
    let sql = "SELECT order_id, MAX(qty) OVER (ORDER BY order_id GROUPS 3 PRECEDING), MIN(qty) OVER (ORDER BY order_id DESC) from orders";
    let plan = logical_plan(sql).unwrap();
    assert_snapshot!(
        plan,
        @r#"
Projection: orders.order_id, max(orders.qty) ORDER BY [orders.order_id ASC NULLS LAST] GROUPS BETWEEN 3 PRECEDING AND CURRENT ROW, min(orders.qty) ORDER BY [orders.order_id DESC NULLS FIRST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  WindowAggr: windowExpr=[[max(orders.qty) ORDER BY [orders.order_id ASC NULLS LAST] GROUPS BETWEEN 3 PRECEDING AND CURRENT ROW]]
    WindowAggr: windowExpr=[[min(orders.qty) ORDER BY [orders.order_id DESC NULLS FIRST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW]]
      TableScan: orders
"#
    );
}

/// psql result
/// ```text
///                                     QUERY PLAN
/// -----------------------------------------------------------------------------------
/// WindowAgg  (cost=142.16..162.16 rows=1000 width=16)
///   ->  Sort  (cost=142.16..144.66 rows=1000 width=16)
///         Sort Key: order_id
///         ->  WindowAgg  (cost=72.33..92.33 rows=1000 width=16)
///               ->  Sort  (cost=72.33..74.83 rows=1000 width=12)
///                     Sort Key: ((order_id + 1))
///                     ->  Seq Scan on orders  (cost=0.00..22.50 rows=1000 width=12)
/// ```
#[test]
fn over_order_by_two_sort_keys() {
    let sql = "SELECT order_id, MAX(qty) OVER (ORDER BY order_id), MIN(qty) OVER (ORDER BY (order_id + 1)) from orders";
    let plan = logical_plan(sql).unwrap();
    assert_snapshot!(
        plan,
        @r#"
Projection: orders.order_id, max(orders.qty) ORDER BY [orders.order_id ASC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW, min(orders.qty) ORDER BY [orders.order_id + Int64(1) ASC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  WindowAggr: windowExpr=[[max(orders.qty) ORDER BY [orders.order_id ASC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW]]
    WindowAggr: windowExpr=[[min(orders.qty) ORDER BY [orders.order_id + Int64(1) ASC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW]]
      TableScan: orders
"#
    );
}

/// psql result
/// ```text
///                                        QUERY PLAN
/// ----------------------------------------------------------------------------------------
/// WindowAgg  (cost=139.66..172.16 rows=1000 width=24)
///   ->  WindowAgg  (cost=139.66..159.66 rows=1000 width=16)
///         ->  Sort  (cost=139.66..142.16 rows=1000 width=12)
///               Sort Key: qty, order_id
///               ->  WindowAgg  (cost=69.83..89.83 rows=1000 width=12)
///                     ->  Sort  (cost=69.83..72.33 rows=1000 width=8)
///                           Sort Key: order_id, qty
///                           ->  Seq Scan on orders  (cost=0.00..20.00 rows=1000 width=8)
/// ```
#[test]
fn over_order_by_sort_keys_sorting() {
    let sql = "SELECT order_id, MAX(qty) OVER (ORDER BY qty, order_id), sum(qty) OVER (), MIN(qty) OVER (ORDER BY order_id, qty) from orders";
    let plan = logical_plan(sql).unwrap();
    assert_snapshot!(
        plan,
        @r#"
Projection: orders.order_id, max(orders.qty) ORDER BY [orders.qty ASC NULLS LAST, orders.order_id ASC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW, sum(orders.qty) ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING, min(orders.qty) ORDER BY [orders.order_id ASC NULLS LAST, orders.qty ASC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  WindowAggr: windowExpr=[[sum(orders.qty) ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING]]
    WindowAggr: windowExpr=[[max(orders.qty) ORDER BY [orders.qty ASC NULLS LAST, orders.order_id ASC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW]]
      WindowAggr: windowExpr=[[min(orders.qty) ORDER BY [orders.order_id ASC NULLS LAST, orders.qty ASC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW]]
        TableScan: orders
"#
    );
}

/// psql result
/// ```text
///                                     QUERY PLAN
/// ----------------------------------------------------------------------------------
/// WindowAgg  (cost=69.83..117.33 rows=1000 width=24)
///   ->  WindowAgg  (cost=69.83..104.83 rows=1000 width=16)
///         ->  WindowAgg  (cost=69.83..89.83 rows=1000 width=12)
///               ->  Sort  (cost=69.83..72.33 rows=1000 width=8)
///                     Sort Key: order_id, qty
///                     ->  Seq Scan on orders  (cost=0.00..20.00 rows=1000 width=8)
/// ```
#[test]
fn over_order_by_sort_keys_sorting_prefix_compacting() {
    let sql = "SELECT order_id, MAX(qty) OVER (ORDER BY order_id), sum(qty) OVER (), MIN(qty) OVER (ORDER BY order_id, qty) from orders";
    let plan = logical_plan(sql).unwrap();
    assert_snapshot!(
        plan,
        @r#"
Projection: orders.order_id, max(orders.qty) ORDER BY [orders.order_id ASC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW, sum(orders.qty) ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING, min(orders.qty) ORDER BY [orders.order_id ASC NULLS LAST, orders.qty ASC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  WindowAggr: windowExpr=[[sum(orders.qty) ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING]]
    WindowAggr: windowExpr=[[max(orders.qty) ORDER BY [orders.order_id ASC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW]]
      WindowAggr: windowExpr=[[min(orders.qty) ORDER BY [orders.order_id ASC NULLS LAST, orders.qty ASC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW]]
        TableScan: orders
"#
    );
}

/// psql result
/// ```text
///                                        QUERY PLAN
/// ----------------------------------------------------------------------------------------
/// WindowAgg  (cost=139.66..172.16 rows=1000 width=24)
///   ->  WindowAgg  (cost=139.66..159.66 rows=1000 width=16)
///         ->  Sort  (cost=139.66..142.16 rows=1000 width=12)
///               Sort Key: order_id, qty
///               ->  WindowAgg  (cost=69.83..89.83 rows=1000 width=12)
///                     ->  Sort  (cost=69.83..72.33 rows=1000 width=8)
///                           Sort Key: qty, order_id
///                           ->  Seq Scan on orders  (cost=0.00..20.00 rows=1000 width=8)
/// ```
///
/// FIXME: for now we are not detecting prefix of sorting keys in order to re-arrange with global
/// sort
#[test]
fn over_order_by_sort_keys_sorting_global_order_compacting() {
    let sql = "SELECT order_id, MAX(qty) OVER (ORDER BY qty, order_id), sum(qty) OVER (), MIN(qty) OVER (ORDER BY order_id, qty) from orders ORDER BY order_id";
    let plan = logical_plan(sql).unwrap();
    assert_snapshot!(
        plan,
        @r#"
Sort: orders.order_id ASC NULLS LAST
  Projection: orders.order_id, max(orders.qty) ORDER BY [orders.qty ASC NULLS LAST, orders.order_id ASC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW, sum(orders.qty) ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING, min(orders.qty) ORDER BY [orders.order_id ASC NULLS LAST, orders.qty ASC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    WindowAggr: windowExpr=[[sum(orders.qty) ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING]]
      WindowAggr: windowExpr=[[max(orders.qty) ORDER BY [orders.qty ASC NULLS LAST, orders.order_id ASC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW]]
        WindowAggr: windowExpr=[[min(orders.qty) ORDER BY [orders.order_id ASC NULLS LAST, orders.qty ASC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW]]
          TableScan: orders
"#
    );
}

/// psql result
/// ```text
///                               QUERY PLAN
/// ----------------------------------------------------------------------
/// WindowAgg  (cost=69.83..89.83 rows=1000 width=12)
///   ->  Sort  (cost=69.83..72.33 rows=1000 width=8)
///         Sort Key: order_id, qty
///         ->  Seq Scan on orders  (cost=0.00..20.00 rows=1000 width=8)
/// ```
#[test]
fn over_partition_by_order_by() {
    let sql =
        "SELECT order_id, MAX(qty) OVER (PARTITION BY order_id ORDER BY qty) from orders";
    let plan = logical_plan(sql).unwrap();
    assert_snapshot!(
        plan,
        @r#"
Projection: orders.order_id, max(orders.qty) PARTITION BY [orders.order_id] ORDER BY [orders.qty ASC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  WindowAggr: windowExpr=[[max(orders.qty) PARTITION BY [orders.order_id] ORDER BY [orders.qty ASC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW]]
    TableScan: orders
"#
    );
}

/// psql result
/// ```text
///                               QUERY PLAN
/// ----------------------------------------------------------------------
/// WindowAgg  (cost=69.83..89.83 rows=1000 width=12)
///   ->  Sort  (cost=69.83..72.33 rows=1000 width=8)
///         Sort Key: order_id, qty
///         ->  Seq Scan on orders  (cost=0.00..20.00 rows=1000 width=8)
/// ```
#[test]
fn over_partition_by_order_by_no_dup() {
    let sql =
        "SELECT order_id, MAX(qty) OVER (PARTITION BY order_id, qty ORDER BY qty) from orders";
    let plan = logical_plan(sql).unwrap();
    assert_snapshot!(
        plan,
        @r#"
Projection: orders.order_id, max(orders.qty) PARTITION BY [orders.order_id, orders.qty] ORDER BY [orders.qty ASC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  WindowAggr: windowExpr=[[max(orders.qty) PARTITION BY [orders.order_id, orders.qty] ORDER BY [orders.qty ASC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW]]
    TableScan: orders
"#
    );
}

/// psql result
/// ```text
///                                     QUERY PLAN
/// ----------------------------------------------------------------------------------
/// WindowAgg  (cost=142.16..162.16 rows=1000 width=16)
///   ->  Sort  (cost=142.16..144.66 rows=1000 width=12)
///         Sort Key: qty, order_id
///         ->  WindowAgg  (cost=69.83..92.33 rows=1000 width=12)
///               ->  Sort  (cost=69.83..72.33 rows=1000 width=8)
///                     Sort Key: order_id, qty
///                     ->  Seq Scan on orders  (cost=0.00..20.00 rows=1000 width=8)
/// ```
#[test]
fn over_partition_by_order_by_mix_up() {
    let sql =
            "SELECT order_id, MAX(qty) OVER (PARTITION BY order_id, qty ORDER BY qty), MIN(qty) OVER (PARTITION BY qty ORDER BY order_id) from orders";
    let plan = logical_plan(sql).unwrap();
    assert_snapshot!(
        plan,
        @r#"
Projection: orders.order_id, max(orders.qty) PARTITION BY [orders.order_id, orders.qty] ORDER BY [orders.qty ASC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW, min(orders.qty) PARTITION BY [orders.qty] ORDER BY [orders.order_id ASC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  WindowAggr: windowExpr=[[min(orders.qty) PARTITION BY [orders.qty] ORDER BY [orders.order_id ASC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW]]
    WindowAggr: windowExpr=[[max(orders.qty) PARTITION BY [orders.order_id, orders.qty] ORDER BY [orders.qty ASC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW]]
      TableScan: orders
"#
    );
}

/// psql result
/// ```text
///                                  QUERY PLAN
/// -----------------------------------------------------------------------------
/// WindowAgg  (cost=69.83..109.83 rows=1000 width=24)
///   ->  WindowAgg  (cost=69.83..92.33 rows=1000 width=20)
///         ->  Sort  (cost=69.83..72.33 rows=1000 width=16)
///               Sort Key: order_id, qty, price
///               ->  Seq Scan on orders  (cost=0.00..20.00 rows=1000 width=16)
/// ```
/// FIXME: for now we are not detecting prefix of sorting keys in order to save one sort exec phase
#[test]
fn over_partition_by_order_by_mix_up_prefix() {
    let sql =
            "SELECT order_id, MAX(qty) OVER (PARTITION BY order_id ORDER BY qty), MIN(qty) OVER (PARTITION BY order_id, qty ORDER BY price) from orders";
    let plan = logical_plan(sql).unwrap();
    assert_snapshot!(
        plan,
        @r#"
Projection: orders.order_id, max(orders.qty) PARTITION BY [orders.order_id] ORDER BY [orders.qty ASC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW, min(orders.qty) PARTITION BY [orders.order_id, orders.qty] ORDER BY [orders.price ASC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  WindowAggr: windowExpr=[[max(orders.qty) PARTITION BY [orders.order_id] ORDER BY [orders.qty ASC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW]]
    WindowAggr: windowExpr=[[min(orders.qty) PARTITION BY [orders.order_id, orders.qty] ORDER BY [orders.price ASC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW]]
      TableScan: orders
"#
    );
}

#[test]
fn approx_median_window() {
    let sql =
        "SELECT order_id, APPROX_MEDIAN(qty) OVER(PARTITION BY order_id) from orders";
    let plan = logical_plan(sql).unwrap();
    assert_snapshot!(
        plan,
        @r#"
Projection: orders.order_id, approx_median(orders.qty) PARTITION BY [orders.order_id] ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
  WindowAggr: windowExpr=[[approx_median(orders.qty) PARTITION BY [orders.order_id] ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING]]
    TableScan: orders
"#
    );
}

#[test]
fn select_typed_date_string() {
    let sql = "SELECT date '2020-12-10' AS date";
    let plan = logical_plan(sql).unwrap();
    assert_snapshot!(
        plan,
        @r#"
    Projection: CAST(Utf8("2020-12-10") AS Date32) AS date
      EmptyRelation: rows=1
    "#
    );
}

#[test]
fn select_typed_time_string() {
    let sql = "SELECT TIME '08:09:10.123' AS time";
    let plan = logical_plan(sql).unwrap();
    assert_snapshot!(
        plan,
        @r#"
    Projection: CAST(Utf8("08:09:10.123") AS Time64(ns)) AS time
      EmptyRelation: rows=1
    "#
    );
}

#[test]
fn select_multibyte_column() {
    let sql = r#"SELECT "😀" FROM person"#;
    let plan = logical_plan(sql).unwrap();
    assert_snapshot!(
        plan,
        @r#"
Projection: person.😀
  TableScan: person
"#
    );
}

#[test]
fn select_groupby_orderby() {
    // ensure that references are correctly resolved in the order by clause
    // see https://github.com/apache/datafusion/issues/4854

    let sqls = vec![
        r#"
        SELECT
            avg(age) AS "value",
            date_trunc('month', birth_date) AS "birth_date"
            FROM person GROUP BY birth_date ORDER BY birth_date;
        "#,
        // Use fully qualified `person.birth_date` as argument to date_trunc, plan should be the same
        r#"
        SELECT
            avg(age) AS "value",
            date_trunc('month', person.birth_date) AS "birth_date"
            FROM person GROUP BY birth_date ORDER BY birth_date;
        "#,
        // Use fully qualified `person.birth_date` as group by, plan should be the same
        r#"
        SELECT
            avg(age) AS "value",
            date_trunc('month', birth_date) AS "birth_date"
            FROM person GROUP BY person.birth_date ORDER BY birth_date;
        "#,
        // Use fully qualified `person.birth_date` in both group and date_trunc, plan should be the same
        r#"
        SELECT
            avg(age) AS "value",
            date_trunc('month', person.birth_date) AS "birth_date"
            FROM person GROUP BY person.birth_date ORDER BY birth_date;
        "#,
    ];
    for sql in sqls {
        let plan = logical_plan(sql).unwrap();
        allow_duplicates! {
            assert_snapshot!(
                plan,
                // expect that this is not an ambiguous reference
                @r#"
        Sort: birth_date ASC NULLS LAST
          Projection: avg(person.age) AS value, date_trunc(Utf8("month"), person.birth_date) AS birth_date
            Aggregate: groupBy=[[person.birth_date]], aggr=[[avg(person.age)]]
              TableScan: person
        "#
            );
        }
    }

    // Use columnized `avg(age)` in the order by
    let sql = r#"SELECT
  avg(age) + avg(age),
  date_trunc('month', person.birth_date) AS "birth_date"
  FROM person GROUP BY person.birth_date ORDER BY avg(age) + avg(age);
"#;

    let plan = logical_plan(sql).unwrap();
    assert_snapshot!(
        plan,
        @r#"
Sort: avg(person.age) + avg(person.age) ASC NULLS LAST
  Projection: avg(person.age) + avg(person.age), date_trunc(Utf8("month"), person.birth_date) AS birth_date
    Aggregate: groupBy=[[person.birth_date]], aggr=[[avg(person.age)]]
      TableScan: person
"#
    );
}

#[test]
fn select_groupby_orderby_aggregate_on_non_selected_column() {
    let sql = "SELECT state FROM person GROUP BY state ORDER BY MIN(age)";
    let plan = logical_plan(sql).unwrap();
    assert_snapshot!(
        plan,
        @r#"
Projection: person.state
  Sort: min(person.age) ASC NULLS LAST
    Projection: person.state, min(person.age)
      Aggregate: groupBy=[[person.state]], aggr=[[min(person.age)]]
        TableScan: person
"#
    );
}

#[test]
fn select_groupby_orderby_multiple_aggregates_on_non_selected_columns() {
    let sql =
        "SELECT state FROM person GROUP BY state ORDER BY MIN(age), MAX(salary) DESC";
    let plan = logical_plan(sql).unwrap();
    assert_snapshot!(
        plan,
        @r#"
Projection: person.state
  Sort: min(person.age) ASC NULLS LAST, max(person.salary) DESC NULLS FIRST
    Projection: person.state, min(person.age), max(person.salary)
      Aggregate: groupBy=[[person.state]], aggr=[[min(person.age), max(person.salary)]]
        TableScan: person
"#
    );
}

#[test]
fn select_groupby_orderby_aggregate_on_non_selected_column_original_issue() {
    let sql = "SELECT id FROM person GROUP BY id ORDER BY min(age)";
    let plan = logical_plan(sql).unwrap();
    assert_snapshot!(
        plan,
        @r#"
Projection: person.id
  Sort: min(person.age) ASC NULLS LAST
    Projection: person.id, min(person.age)
      Aggregate: groupBy=[[person.id]], aggr=[[min(person.age)]]
        TableScan: person
"#
    );
}

fn logical_plan(sql: &str) -> Result<LogicalPlan> {
    logical_plan_with_options(sql, ParserOptions::default())
}

fn logical_plan_with_options(sql: &str, options: ParserOptions) -> Result<LogicalPlan> {
    let dialect = &GenericDialect {};
    logical_plan_with_dialect_and_options(sql, dialect, options)
}

fn logical_plan_with_dialect(sql: &str, dialect: &dyn Dialect) -> Result<LogicalPlan> {
    let state = MockSessionState::default().with_aggregate_function(sum_udaf());
    let context = MockContextProvider { state };
    let planner = SqlToRel::new(&context);
    let result = DFParser::parse_sql_with_dialect(sql, dialect);
    let mut ast = result?;
    planner.statement_to_plan(ast.pop_front().unwrap())
}

fn logical_plan_with_dialect_and_options(
    sql: &str,
    dialect: &dyn Dialect,
    options: ParserOptions,
) -> Result<LogicalPlan> {
    let state = MockSessionState::default()
        .with_scalar_function(Arc::new(unicode::character_length().as_ref().clone()))
        .with_scalar_function(Arc::new(string::concat().as_ref().clone()))
        .with_scalar_function(Arc::new(make_udf(
            "nullif",
            vec![DataType::Int32, DataType::Int32],
            DataType::Int32,
        )))
        .with_scalar_function(Arc::new(make_udf(
            "round",
            vec![DataType::Float64, DataType::Int64],
            DataType::Float32,
        )))
        .with_scalar_function(Arc::new(make_udf(
            "arrow_cast",
            vec![DataType::Int64, DataType::Utf8],
            DataType::Float64,
        )))
        .with_scalar_function(Arc::new(make_udf(
            "date_trunc",
            vec![DataType::Utf8, DataType::Timestamp(Nanosecond, None)],
            DataType::Int32,
        )))
        .with_scalar_function(Arc::new(make_udf(
            "sqrt",
            vec![DataType::Int64],
            DataType::Int64,
        )))
        .with_aggregate_function(sum_udaf())
        .with_aggregate_function(approx_median_udaf())
        .with_aggregate_function(count_udaf())
        .with_aggregate_function(avg_udaf())
        .with_aggregate_function(min_udaf())
        .with_aggregate_function(max_udaf())
        .with_aggregate_function(grouping_udaf())
        .with_window_function(rank_udwf())
        .with_window_function(row_number_udwf())
        .with_expr_planner(Arc::new(CoreFunctionPlanner::default()));

    let context = MockContextProvider { state };
    let planner = SqlToRel::new_with_options(&context, options);
    let result = DFParser::parse_sql_with_dialect(sql, dialect);
    let mut ast = result?;
    planner.statement_to_plan(ast.pop_front().unwrap())
}

fn make_udf(name: &'static str, args: Vec<DataType>, return_type: DataType) -> ScalarUDF {
    ScalarUDF::new_from_impl(DummyUDF::new(name, args, return_type))
}

/// Mocked UDF
#[derive(Debug, PartialEq, Eq, Hash)]
struct DummyUDF {
    name: &'static str,
    signature: Signature,
    return_type: DataType,
}

impl DummyUDF {
    fn new(name: &'static str, args: Vec<DataType>, return_type: DataType) -> Self {
        Self {
            name,
            signature: Signature::exact(args, Volatility::Immutable),
            return_type,
        }
    }
}

impl ScalarUDFImpl for DummyUDF {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(self.return_type.clone())
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        panic!("dummy - not implemented")
    }
}

fn parse_decimals_parser_options() -> ParserOptions {
    ParserOptions {
        parse_float_as_decimal: true,
        enable_ident_normalization: false,
        support_varchar_with_length: false,
        map_string_types_to_utf8view: true,
        enable_options_value_normalization: false,
        collect_spans: false,
        default_null_ordering: NullOrdering::NullsMax,
    }
}

fn ident_normalization_parser_options_no_ident_normalization() -> ParserOptions {
    ParserOptions {
        parse_float_as_decimal: true,
        enable_ident_normalization: false,
        support_varchar_with_length: false,
        map_string_types_to_utf8view: true,
        enable_options_value_normalization: false,
        collect_spans: false,
        default_null_ordering: NullOrdering::NullsMax,
    }
}

fn ident_normalization_parser_options_ident_normalization() -> ParserOptions {
    ParserOptions {
        parse_float_as_decimal: true,
        enable_ident_normalization: true,
        support_varchar_with_length: false,
        map_string_types_to_utf8view: true,
        enable_options_value_normalization: false,
        collect_spans: false,
        default_null_ordering: NullOrdering::NullsMax,
    }
}

#[test]
fn select_partially_qualified_column() {
    let sql = "SELECT person.first_name FROM public.person";
    let plan = logical_plan(sql).unwrap();
    assert_snapshot!(
        plan,
        @r#"
Projection: public.person.first_name
  TableScan: public.person
"#
    );
}

#[test]
fn cross_join_not_to_inner_join() {
    let sql =
        "select person.id from person, orders, lineitem where person.id = person.age;";
    let plan = logical_plan(sql).unwrap();
    assert_snapshot!(
        plan,
        @r#"
Projection: person.id
  Filter: person.id = person.age
    Cross Join: 
      Cross Join: 
        TableScan: person
        TableScan: orders
      TableScan: lineitem
"#
    );
}

#[test]
fn join_with_aliases() {
    let sql = "select peeps.id, folks.first_name from person as peeps join person as folks on peeps.id = folks.id";
    let plan = logical_plan(sql).unwrap();
    assert_snapshot!(
        plan,
        @r#"
Projection: peeps.id, folks.first_name
  Inner Join:  Filter: peeps.id = folks.id
    SubqueryAlias: peeps
      TableScan: person
    SubqueryAlias: folks
      TableScan: person
"#
    );
}

#[test]
fn negative_interval_plus_interval_in_projection() {
    let sql = "select -interval '2 days' + interval '5 days';";
    let plan = logical_plan(sql).unwrap();
    assert_snapshot!(
        plan,
        @r#"
    Projection: IntervalMonthDayNano("IntervalMonthDayNano { months: 0, days: -2, nanoseconds: 0 }") + IntervalMonthDayNano("IntervalMonthDayNano { months: 0, days: 5, nanoseconds: 0 }")
      EmptyRelation: rows=1
    "#
    );
}

#[test]
fn complex_interval_expression_in_projection() {
    let sql = "select -interval '2 days' + interval '5 days'+ (-interval '3 days' + interval '5 days');";
    let plan = logical_plan(sql).unwrap();
    assert_snapshot!(
        plan,
        @r#"
    Projection: IntervalMonthDayNano("IntervalMonthDayNano { months: 0, days: -2, nanoseconds: 0 }") + IntervalMonthDayNano("IntervalMonthDayNano { months: 0, days: 5, nanoseconds: 0 }") + IntervalMonthDayNano("IntervalMonthDayNano { months: 0, days: -3, nanoseconds: 0 }") + IntervalMonthDayNano("IntervalMonthDayNano { months: 0, days: 5, nanoseconds: 0 }")
      EmptyRelation: rows=1
    "#
    );
}

#[test]
fn negative_sum_intervals_in_projection() {
    let sql = "select -((interval '2 days' + interval '5 days') + -(interval '4 days' + interval '7 days'));";
    let plan = logical_plan(sql).unwrap();
    assert_snapshot!(
        plan,
        @r#"
    Projection: (- IntervalMonthDayNano("IntervalMonthDayNano { months: 0, days: 2, nanoseconds: 0 }") + IntervalMonthDayNano("IntervalMonthDayNano { months: 0, days: 5, nanoseconds: 0 }") + (- IntervalMonthDayNano("IntervalMonthDayNano { months: 0, days: 4, nanoseconds: 0 }") + IntervalMonthDayNano("IntervalMonthDayNano { months: 0, days: 7, nanoseconds: 0 }")))
      EmptyRelation: rows=1
    "#
    );
}

#[test]
fn date_plus_interval_in_projection() {
    let sql = "select t_date32 + interval '5 days' FROM test";
    let plan = logical_plan(sql).unwrap();
    assert_snapshot!(
        plan,
        @r#"
Projection: test.t_date32 + IntervalMonthDayNano("IntervalMonthDayNano { months: 0, days: 5, nanoseconds: 0 }")
  TableScan: test
"#
    );
}

#[test]
fn date_plus_interval_in_filter() {
    let sql = "select t_date64 FROM test \
                    WHERE t_date64 \
                    BETWEEN cast('1999-12-31' as date) \
                        AND cast('1999-12-31' as date) + interval '30 days'";
    let plan = logical_plan(sql).unwrap();
    assert_snapshot!(
        plan,
        @r#"
Projection: test.t_date64
  Filter: test.t_date64 BETWEEN CAST(Utf8("1999-12-31") AS Date32) AND CAST(Utf8("1999-12-31") AS Date32) + IntervalMonthDayNano("IntervalMonthDayNano { months: 0, days: 30, nanoseconds: 0 }")
    TableScan: test
"#
    );
}

#[test]
fn exists_subquery() {
    let sql = "SELECT id FROM person p WHERE EXISTS \
            (SELECT first_name FROM person \
            WHERE last_name = p.last_name \
            AND state = p.state)";
    let plan = logical_plan(sql).unwrap();
    assert_snapshot!(
        plan,
        @r#"
Projection: p.id
  Filter: EXISTS (<subquery>)
    Subquery:
      Projection: person.first_name
        Filter: person.last_name = outer_ref(p.last_name) AND person.state = outer_ref(p.state)
          TableScan: person
    SubqueryAlias: p
      TableScan: person
"#
    );
}

#[test]
fn exists_subquery_schema_outer_schema_overlap() {
    // both the outer query and the schema select from unaliased "person"
    let sql = "SELECT person.id FROM person, person p \
            WHERE person.id = p.id AND EXISTS \
            (SELECT person.first_name FROM person, person p2 \
            WHERE person.id = p2.id \
            AND person.last_name = p.last_name \
            AND person.state = p.state)";
    let plan = logical_plan(sql).unwrap();
    assert_snapshot!(
        plan,
        @r#"
Projection: person.id
  Filter: person.id = p.id AND EXISTS (<subquery>)
    Subquery:
      Projection: person.first_name
        Filter: person.id = p2.id AND person.last_name = outer_ref(p.last_name) AND person.state = outer_ref(p.state)
          Cross Join: 
            TableScan: person
            SubqueryAlias: p2
              TableScan: person
    Cross Join: 
      TableScan: person
      SubqueryAlias: p
        TableScan: person
"#
    );
}

#[test]
fn in_subquery_uncorrelated() {
    let sql = "SELECT id FROM person p WHERE id IN \
            (SELECT id FROM person)";
    let plan = logical_plan(sql).unwrap();
    assert_snapshot!(
        plan,
        @r#"
Projection: p.id
  Filter: p.id IN (<subquery>)
    Subquery:
      Projection: person.id
        TableScan: person
    SubqueryAlias: p
      TableScan: person
"#
    );
}

#[test]
fn not_in_subquery_correlated() {
    let sql = "SELECT id FROM person p WHERE id NOT IN \
            (SELECT id FROM person WHERE last_name = p.last_name AND state = 'CO')";
    let plan = logical_plan(sql).unwrap();
    assert_snapshot!(
        plan,
        @r#"
Projection: p.id
  Filter: p.id NOT IN (<subquery>)
    Subquery:
      Projection: person.id
        Filter: person.last_name = outer_ref(p.last_name) AND person.state = Utf8("CO")
          TableScan: person
    SubqueryAlias: p
      TableScan: person
"#
    );
}

#[test]
fn scalar_subquery() {
    let sql =
        "SELECT p.id, (SELECT MAX(id) FROM person WHERE last_name = p.last_name) FROM person p";
    let plan = logical_plan(sql).unwrap();
    assert_snapshot!(
        plan,
        @r#"
Projection: p.id, (<subquery>)
  Subquery:
    Projection: max(person.id)
      Aggregate: groupBy=[[]], aggr=[[max(person.id)]]
        Filter: person.last_name = outer_ref(p.last_name)
          TableScan: person
  SubqueryAlias: p
    TableScan: person
"#
    );
}

#[test]
fn scalar_subquery_reference_outer_field() {
    let sql = "SELECT j1_string, j2_string \
        FROM j1, j2 \
        WHERE j1_id = j2_id - 1 \
        AND j2_id < (SELECT count(*) \
            FROM j1, j3 \
            WHERE j2_id = j1_id \
            AND j1_id = j3_id)";
    let plan = logical_plan(sql).unwrap();
    assert_snapshot!(
        plan,
        @r#"
Projection: j1.j1_string, j2.j2_string
  Filter: j1.j1_id = j2.j2_id - Int64(1) AND j2.j2_id < (<subquery>)
    Subquery:
      Projection: count(*)
        Aggregate: groupBy=[[]], aggr=[[count(*)]]
          Filter: outer_ref(j2.j2_id) = j1.j1_id AND j1.j1_id = j3.j3_id
            Cross Join: 
              TableScan: j1
              TableScan: j3
    Cross Join: 
      TableScan: j1
      TableScan: j2
"#
    );
}

#[test]
fn aggregate_with_rollup() {
    let sql =
        "SELECT id, state, age, count(*) FROM person GROUP BY id, ROLLUP (state, age)";
    let plan = logical_plan(sql).unwrap();
    assert_snapshot!(
        plan,
        @r#"
Projection: person.id, person.state, person.age, count(*)
  Aggregate: groupBy=[[GROUPING SETS ((person.id), (person.id, person.state), (person.id, person.state, person.age))]], aggr=[[count(*)]]
    TableScan: person
"#
    );
}

#[test]
fn aggregate_with_rollup_with_grouping() {
    let sql = "SELECT id, state, age, grouping(state), grouping(age), grouping(state) + grouping(age), count(*) \
        FROM person GROUP BY id, ROLLUP (state, age)";
    let plan = logical_plan(sql).unwrap();
    assert_snapshot!(
        plan,
        @r#"
Projection: person.id, person.state, person.age, grouping(person.state), grouping(person.age), grouping(person.state) + grouping(person.age), count(*)
  Aggregate: groupBy=[[GROUPING SETS ((person.id), (person.id, person.state), (person.id, person.state, person.age))]], aggr=[[grouping(person.state), grouping(person.age), count(*)]]
    TableScan: person
"#
    );
}

#[test]
fn rank_partition_grouping() {
    let sql = "select
            sum(age) as total_sum,
            state,
            last_name,
            grouping(state) + grouping(last_name) as x,
            rank() over (
                partition by grouping(state) + grouping(last_name),
                case when grouping(last_name) = 0 then state end
                order by sum(age) desc
                ) as the_rank
            from
                person
            group by rollup(state, last_name)";
    let plan = logical_plan(sql).unwrap();
    assert_snapshot!(
        plan,
        @r#"
Projection: sum(person.age) AS total_sum, person.state, person.last_name, grouping(person.state) + grouping(person.last_name) AS x, rank() PARTITION BY [grouping(person.state) + grouping(person.last_name), CASE WHEN grouping(person.last_name) = Int64(0) THEN person.state END] ORDER BY [sum(person.age) DESC NULLS FIRST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW AS the_rank
  WindowAggr: windowExpr=[[rank() PARTITION BY [grouping(person.state) + grouping(person.last_name), CASE WHEN grouping(person.last_name) = Int64(0) THEN person.state END] ORDER BY [sum(person.age) DESC NULLS FIRST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW]]
    Aggregate: groupBy=[[ROLLUP (person.state, person.last_name)]], aggr=[[sum(person.age), grouping(person.state), grouping(person.last_name)]]
      TableScan: person
"#
    );
}

#[test]
fn aggregate_with_cube() {
    let sql =
        "SELECT id, state, age, count(*) FROM person GROUP BY id, CUBE (state, age)";
    let plan = logical_plan(sql).unwrap();
    assert_snapshot!(
        plan,
        @r#"
Projection: person.id, person.state, person.age, count(*)
  Aggregate: groupBy=[[GROUPING SETS ((person.id), (person.id, person.state), (person.id, person.age), (person.id, person.state, person.age))]], aggr=[[count(*)]]
    TableScan: person
"#
    );
}

#[test]
fn round_decimal() {
    let sql = "SELECT round(price/3, 2) FROM test_decimal";
    let plan = logical_plan(sql).unwrap();
    assert_snapshot!(
        plan,
        @r#"
Projection: round(test_decimal.price / Int64(3), Int64(2))
  TableScan: test_decimal
"#
    );
}

#[test]
fn aggregate_with_grouping_sets() {
    let sql = "SELECT id, state, age, count(*) FROM person GROUP BY id, GROUPING SETS ((state), (state, age), (id, state))";
    let plan = logical_plan(sql).unwrap();
    assert_snapshot!(
        plan,
        @r#"
Projection: person.id, person.state, person.age, count(*)
  Aggregate: groupBy=[[GROUPING SETS ((person.id, person.state), (person.id, person.state, person.age), (person.id, person.id, person.state))]], aggr=[[count(*)]]
    TableScan: person
"#
    );
}

#[test]
fn join_on_disjunction_condition() {
    let sql = "SELECT id, order_id \
            FROM person \
            JOIN orders ON id = customer_id OR person.age > 30";
    let plan = logical_plan(sql).unwrap();
    assert_snapshot!(
        plan,
        @r#"
Projection: person.id, orders.order_id
  Inner Join:  Filter: person.id = orders.customer_id OR person.age > Int64(30)
    TableScan: person
    TableScan: orders
"#
    );
}

#[test]
fn join_on_complex_condition() {
    let sql = "SELECT id, order_id \
            FROM person \
            JOIN orders ON id = customer_id AND (person.age > 30 OR person.last_name = 'X')";
    let plan = logical_plan(sql).unwrap();
    assert_snapshot!(
        plan,
        @r#"
Projection: person.id, orders.order_id
  Inner Join:  Filter: person.id = orders.customer_id AND (person.age > Int64(30) OR person.last_name = Utf8("X"))
    TableScan: person
    TableScan: orders
"#
    );
}

#[test]
fn hive_aggregate_with_filter() -> Result<()> {
    let dialect = &HiveDialect {};
    let sql = "SELECT sum(age) FILTER (WHERE age > 4) FROM person";
    let plan = logical_plan_with_dialect(sql, dialect)?;

    assert_snapshot!(
        plan,
        @r##"
        Projection: sum(person.age) FILTER (WHERE person.age > Int64(4))
          Aggregate: groupBy=[[]], aggr=[[sum(person.age) FILTER (WHERE person.age > Int64(4))]]
            TableScan: person
        "##
    );

    Ok(())
}

#[test]
fn order_by_unaliased_name() {
    // https://github.com/apache/datafusion/issues/3160
    // This query was failing with:
    // SchemaError(FieldNotFound { qualifier: Some("p"), name: "state", valid_fields: ["z", "q"] })
    let sql =
        "select p.state z, sum(age) q from person p group by p.state order by p.state";
    let plan = logical_plan(sql).unwrap();
    assert_snapshot!(
        plan,
        @r#"
Sort: z ASC NULLS LAST
  Projection: p.state AS z, sum(p.age) AS q
    Aggregate: groupBy=[[p.state]], aggr=[[sum(p.age)]]
      SubqueryAlias: p
        TableScan: person
"#
    );
}

#[test]
fn order_by_ambiguous_name() {
    let sql = "select * from person a join person b using (id) order by age";
    let err = logical_plan(sql).unwrap_err().strip_backtrace();

    assert_snapshot!(
        err,
        @r###"
        Schema error: Ambiguous reference to unqualified field age
        "###
    );
}

#[test]
fn group_by_ambiguous_name() {
    let sql = "select max(id) from person a join person b using (id) group by age";
    let err = logical_plan(sql).unwrap_err().strip_backtrace();

    assert_snapshot!(
        err,
        @r###"
        Schema error: Ambiguous reference to unqualified field age
        "###
    );
}

#[test]
fn test_zero_offset_with_limit() {
    let sql = "select id from person where person.id > 100 LIMIT 5 OFFSET 0;";
    let plan = logical_plan(sql).unwrap();
    assert_snapshot!(
        plan,
        @r#"
Limit: skip=0, fetch=5
  Projection: person.id
    Filter: person.id > Int64(100)
      TableScan: person
"#
    );
    // Flip the order of LIMIT and OFFSET in the query. Plan should remain the same.
    let sql = "SELECT id FROM person WHERE person.id > 100 OFFSET 0 LIMIT 5;";
    let plan = logical_plan(sql).unwrap();
    assert_snapshot!(
        plan,
        @r#"
Limit: skip=0, fetch=5
  Projection: person.id
    Filter: person.id > Int64(100)
      TableScan: person
"#
    );
}

#[test]
fn test_offset_no_limit() {
    let sql = "SELECT id FROM person WHERE person.id > 100 OFFSET 5;";
    let plan = logical_plan(sql).unwrap();
    assert_snapshot!(
        plan,
        @r#"
Limit: skip=5, fetch=None
  Projection: person.id
    Filter: person.id > Int64(100)
      TableScan: person
"#
    );
}

#[test]
fn test_offset_after_limit() {
    let sql = "select id from person where person.id > 100 LIMIT 5 OFFSET 3;";
    let plan = logical_plan(sql).unwrap();
    assert_snapshot!(
        plan,
        @r#"
Limit: skip=3, fetch=5
  Projection: person.id
    Filter: person.id > Int64(100)
      TableScan: person
"#
    );
}

#[test]
fn fetch_clause_is_not_supported() {
    let sql = "SELECT 1 FETCH NEXT 1 ROW ONLY";
    let err = logical_plan(sql).unwrap_err();
    assert_contains!(err.to_string(), "FETCH clause is not supported yet");
}

#[test]
fn test_offset_before_limit() {
    let sql = "select id from person where person.id > 100 OFFSET 3 LIMIT 5;";
    let plan = logical_plan(sql).unwrap();
    assert_snapshot!(
        plan,
        @r#"
Limit: skip=3, fetch=5
  Projection: person.id
    Filter: person.id > Int64(100)
      TableScan: person
"#
    );
}

#[test]
fn test_distribute_by() {
    let sql = "select id from person distribute by state";
    let plan = logical_plan(sql).unwrap();
    assert_snapshot!(
        plan,
        @r#"
Repartition: DistributeBy(person.state)
  Projection: person.id
    TableScan: person
"#
    );
}

#[test]
fn test_double_quoted_literal_string() {
    // Assert double quoted literal string is parsed correctly like single quoted one in specific
    // dialect.
    let dialect = &MySqlDialect {};
    let single_quoted_res = format!(
        "{:?}",
        logical_plan_with_dialect("SELECT '1'", dialect).unwrap()
    );
    let double_quoted_res = format!(
        "{:?}",
        logical_plan_with_dialect("SELECT \"1\"", dialect).unwrap()
    );
    assert_eq!(single_quoted_res, double_quoted_res);

    // It should return error in other dialect.
    assert!(logical_plan("SELECT \"1\"").is_err());
}

#[test]
fn test_constant_expr_eq_join() {
    let sql = "SELECT id, order_id \
            FROM person \
            INNER JOIN orders \
            ON person.id = 10";
    let plan = logical_plan(sql).unwrap();
    assert_snapshot!(
        plan,
        @r#"
Projection: person.id, orders.order_id
  Inner Join:  Filter: person.id = Int64(10)
    TableScan: person
    TableScan: orders
"#
    );
}

#[test]
fn test_right_left_expr_eq_join() {
    let sql = "SELECT id, order_id \
            FROM person \
            INNER JOIN orders \
            ON orders.customer_id * 2 = person.id + 10";
    let plan = logical_plan(sql).unwrap();
    assert_snapshot!(
        plan,
        @r#"
Projection: person.id, orders.order_id
  Inner Join:  Filter: orders.customer_id * Int64(2) = person.id + Int64(10)
    TableScan: person
    TableScan: orders
"#
    );
}

#[test]
fn test_single_column_expr_eq_join() {
    let sql = "SELECT id, order_id \
            FROM person \
            INNER JOIN orders \
            ON person.id + 10 = orders.customer_id * 2";
    let plan = logical_plan(sql).unwrap();
    assert_snapshot!(
        plan,
        @r#"
Projection: person.id, orders.order_id
  Inner Join:  Filter: person.id + Int64(10) = orders.customer_id * Int64(2)
    TableScan: person
    TableScan: orders
"#
    );
}

#[test]
fn test_multiple_column_expr_eq_join() {
    let sql = "SELECT id, order_id \
            FROM person \
            INNER JOIN orders \
            ON person.id + person.age + 10 = orders.customer_id * 2 - orders.price";
    let plan = logical_plan(sql).unwrap();
    assert_snapshot!(
        plan,
        @r#"
Projection: person.id, orders.order_id
  Inner Join:  Filter: person.id + person.age + Int64(10) = orders.customer_id * Int64(2) - orders.price
    TableScan: person
    TableScan: orders
"#
    );
}

#[test]
fn test_left_expr_eq_join() {
    let sql = "SELECT id, order_id \
            FROM person \
            INNER JOIN orders \
            ON person.id + person.age + 10 = orders.customer_id";
    let plan = logical_plan(sql).unwrap();
    assert_snapshot!(
        plan,
        @r#"
Projection: person.id, orders.order_id
  Inner Join:  Filter: person.id + person.age + Int64(10) = orders.customer_id
    TableScan: person
    TableScan: orders
"#
    );
}

#[test]
fn test_right_expr_eq_join() {
    let sql = "SELECT id, order_id \
            FROM person \
            INNER JOIN orders \
            ON person.id = orders.customer_id * 2 - orders.price";
    let plan = logical_plan(sql).unwrap();
    assert_snapshot!(
        plan,
        @r#"
Projection: person.id, orders.order_id
  Inner Join:  Filter: person.id = orders.customer_id * Int64(2) - orders.price
    TableScan: person
    TableScan: orders
"#
    );
}

#[test]
fn test_noneq_with_filter_join() {
    // inner join
    let sql = "SELECT person.id, person.first_name \
        FROM person INNER JOIN orders \
        ON person.age > 10";
    let plan = logical_plan(sql).unwrap();
    assert_snapshot!(
        plan,
        @r#"
Projection: person.id, person.first_name
  Inner Join:  Filter: person.age > Int64(10)
    TableScan: person
    TableScan: orders
"#
    );
    // left join
    let sql = "SELECT person.id, person.first_name \
        FROM person LEFT JOIN orders \
        ON person.age > 10";
    let plan = logical_plan(sql).unwrap();
    assert_snapshot!(
        plan,
        @r#"
Projection: person.id, person.first_name
  Left Join:  Filter: person.age > Int64(10)
    TableScan: person
    TableScan: orders
"#
    );
    // right join
    let sql = "SELECT person.id, person.first_name \
        FROM person RIGHT JOIN orders \
        ON person.age > 10";
    let plan = logical_plan(sql).unwrap();
    assert_snapshot!(
        plan,
        @r#"
Projection: person.id, person.first_name
  Right Join:  Filter: person.age > Int64(10)
    TableScan: person
    TableScan: orders
"#
    );
    // full join
    let sql = "SELECT person.id, person.first_name \
        FROM person FULL JOIN orders \
        ON person.age > 10";
    let plan = logical_plan(sql).unwrap();
    assert_snapshot!(
        plan,
        @r#"
Projection: person.id, person.first_name
  Full Join:  Filter: person.age > Int64(10)
    TableScan: person
    TableScan: orders
"#
    );
}

#[test]
fn test_one_side_constant_full_join() {
    // TODO: this sql should be parsed as join after
    // https://github.com/apache/datafusion/issues/2877 is resolved.
    let sql = "SELECT id, order_id \
            FROM person \
            FULL OUTER JOIN orders \
            ON person.id = 10";
    let plan = logical_plan(sql).unwrap();
    assert_snapshot!(
        plan,
        @r#"
Projection: person.id, orders.order_id
  Full Join:  Filter: person.id = Int64(10)
    TableScan: person
    TableScan: orders
"#
    );
}

#[test]
fn test_select_join_key_inner_join() {
    let sql = "SELECT  orders.customer_id * 2,  person.id + 10
            FROM person
            INNER JOIN orders
            ON orders.customer_id * 2 = person.id + 10";
    let plan = logical_plan(sql).unwrap();
    assert_snapshot!(
        plan,
        @r#"
Projection: orders.customer_id * Int64(2), person.id + Int64(10)
  Inner Join:  Filter: orders.customer_id * Int64(2) = person.id + Int64(10)
    TableScan: person
    TableScan: orders
"#
    );
}

#[test]
fn test_select_order_by() {
    let sql = "SELECT '1' from person order by id";
    let plan = logical_plan(sql).unwrap();
    assert_snapshot!(
        plan,
        @r#"
Projection: Utf8("1")
  Sort: person.id ASC NULLS LAST
    Projection: Utf8("1"), person.id
      TableScan: person
"#
    );
}

#[test]
fn test_select_distinct_order_by() {
    let sql = "SELECT distinct '1' from person order by id";

    // It should return error.
    let result = logical_plan(sql);
    assert!(result.is_err());
    let err = result.err().unwrap().strip_backtrace();

    assert_snapshot!(
        err,
        @r###"
        Error during planning: For SELECT DISTINCT, ORDER BY expressions person.id must appear in select list
        "###
    );
}

#[test]
fn test_select_qualify_basic() {
    let sql = "SELECT person.id, ROW_NUMBER() OVER (PARTITION BY person.age ORDER BY person.id) as rn FROM person QUALIFY rn = 1";
    let plan = logical_plan(sql).unwrap();
    assert_snapshot!(
        plan,
        @r#"
Projection: person.id, row_number() PARTITION BY [person.age] ORDER BY [person.id ASC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW AS rn
  Filter: row_number() PARTITION BY [person.age] ORDER BY [person.id ASC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW = Int64(1)
    WindowAggr: windowExpr=[[row_number() PARTITION BY [person.age] ORDER BY [person.id ASC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW]]
      TableScan: person
"#
    );
}

#[test]
fn test_select_qualify_aggregate_reference() {
    let sql = "
        SELECT
            person.id,
            ROW_NUMBER() OVER (PARTITION BY person.id ORDER BY person.id) as rn
        FROM person
        GROUP BY
            person.id
        QUALIFY rn = 1 AND SUM(person.age) > 0";
    let plan = logical_plan(sql).unwrap();
    assert_snapshot!(
        plan,
        @r"
    Projection: person.id, row_number() PARTITION BY [person.id] ORDER BY [person.id ASC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW AS rn
      Filter: row_number() PARTITION BY [person.id] ORDER BY [person.id ASC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW = Int64(1) AND sum(person.age) > Int64(0)
        WindowAggr: windowExpr=[[row_number() PARTITION BY [person.id] ORDER BY [person.id ASC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW]]
          Aggregate: groupBy=[[person.id]], aggr=[[sum(person.age)]]
            TableScan: person
    "
    );
}

#[test]
fn test_select_qualify_aggregate_reference_within_window_function() {
    let sql = "
        SELECT
            person.id
        FROM person
        GROUP BY
            person.id
        QUALIFY ROW_NUMBER() OVER (PARTITION BY person.id ORDER BY SUM(person.age) DESC) = 1";
    let plan = logical_plan(sql).unwrap();
    assert_snapshot!(
        plan,
        @r"
    Projection: person.id
      Filter: row_number() PARTITION BY [person.id] ORDER BY [sum(person.age) DESC NULLS FIRST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW = Int64(1)
        WindowAggr: windowExpr=[[row_number() PARTITION BY [person.id] ORDER BY [sum(person.age) DESC NULLS FIRST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW]]
          Aggregate: groupBy=[[person.id]], aggr=[[sum(person.age)]]
            TableScan: person
    "
    );
}

#[test]
fn test_select_qualify_aggregate_invalid_column_reference() {
    let sql = "
        SELECT
            person.id
        FROM person
        GROUP BY
            person.id
        QUALIFY ROW_NUMBER() OVER (PARTITION BY person.id ORDER BY person.age DESC) = 1";
    let err = logical_plan(sql).unwrap_err();
    assert_snapshot!(
        err.strip_backtrace(),
        @r#"Error during planning: Column in QUALIFY must be in GROUP BY or an aggregate function: While expanding wildcard, column "person.age" must appear in the GROUP BY clause or must be part of an aggregate function, currently only "person.id" appears in the SELECT clause satisfies this requirement"#
    );
}

#[test]
fn test_select_qualify_without_window_function() {
    let sql = "SELECT person.id FROM person QUALIFY person.id > 1";
    let err = logical_plan(sql).unwrap_err();
    assert_eq!(
        err.strip_backtrace(),
        "Error during planning: QUALIFY clause requires window functions in the SELECT list or QUALIFY clause"
    );
}

#[test]
fn test_select_qualify_complex_condition() {
    let sql = "SELECT person.id, person.age, ROW_NUMBER() OVER (PARTITION BY person.age ORDER BY person.id) as rn, RANK() OVER (ORDER BY person.salary) as rank FROM person QUALIFY rn <= 2 AND rank <= 5";
    let plan = logical_plan(sql).unwrap();
    assert_snapshot!(
        plan,
        @r#"
Projection: person.id, person.age, row_number() PARTITION BY [person.age] ORDER BY [person.id ASC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW AS rn, rank() ORDER BY [person.salary ASC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW AS rank
  Filter: row_number() PARTITION BY [person.age] ORDER BY [person.id ASC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW <= Int64(2) AND rank() ORDER BY [person.salary ASC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW <= Int64(5)
    WindowAggr: windowExpr=[[rank() ORDER BY [person.salary ASC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW]]
      WindowAggr: windowExpr=[[row_number() PARTITION BY [person.age] ORDER BY [person.id ASC NULLS LAST] RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW]]
        TableScan: person
"#
    );
}

#[rstest]
#[case::select_cluster_by_unsupported(
    "SELECT customer_name, sum(order_total) as total_order_amount FROM orders CLUSTER BY customer_name",
    "This feature is not implemented: CLUSTER BY"
)]
#[case::select_lateral_view_unsupported(
    "SELECT id, number FROM person LATERAL VIEW explode(numbers) exploded_table AS number",
    "This feature is not implemented: LATERAL VIEWS"
)]
#[case::select_top_unsupported(
    "SELECT TOP (5) * FROM person",
    "This feature is not implemented: TOP"
)]
#[case::select_sort_by_unsupported(
    "SELECT * FROM person SORT BY id",
    "This feature is not implemented: SORT BY"
)]
#[test]
fn test_select_unsupported_syntax_errors(#[case] sql: &str, #[case] error: &str) {
    let err = logical_plan(sql).unwrap_err();
    assert_eq!(err.strip_backtrace(), error)
}

#[test]
fn select_order_by_with_cast() {
    let sql =
        "SELECT first_name AS first_name FROM (SELECT first_name AS first_name FROM person) ORDER BY CAST(first_name as INT)";
    let plan = logical_plan(sql).unwrap();
    assert_snapshot!(
        plan,
        @r#"
Sort: CAST(person.first_name AS Int32) ASC NULLS LAST
  Projection: person.first_name
    Projection: person.first_name
      TableScan: person
"#
    );
}

#[test]
fn test_avoid_add_alias() {
    // avoiding adding an alias if the column name is the same.
    // plan1 = plan2
    let sql = "select person.id as id from person order by person.id";
    let plan1 = logical_plan(sql).unwrap();
    let sql = "select id from person order by id";
    let plan2 = logical_plan(sql).unwrap();
    assert_eq!(format!("{plan1:?}"), format!("{plan2:?}"));
}

#[test]
fn test_duplicated_left_join_key_inner_join() {
    //  person.id * 2 happen twice in left side.
    let sql = "SELECT person.id, person.age
            FROM person
            INNER JOIN orders
            ON person.id * 2 = orders.customer_id + 10 and person.id * 2 = orders.order_id";
    let plan = logical_plan(sql).unwrap();
    assert_snapshot!(
        plan,
        @r#"
Projection: person.id, person.age
  Inner Join:  Filter: person.id * Int64(2) = orders.customer_id + Int64(10) AND person.id * Int64(2) = orders.order_id
    TableScan: person
    TableScan: orders
"#
    );
}

#[test]
fn test_duplicated_right_join_key_inner_join() {
    //  orders.customer_id + 10 happen twice in right side.
    let sql = "SELECT person.id, person.age
            FROM person
            INNER JOIN orders
            ON person.id * 2 = orders.customer_id + 10 and person.id =  orders.customer_id + 10";
    let plan = logical_plan(sql).unwrap();
    assert_snapshot!(
        plan,
        @r#"
Projection: person.id, person.age
  Inner Join:  Filter: person.id * Int64(2) = orders.customer_id + Int64(10) AND person.id = orders.customer_id + Int64(10)
    TableScan: person
    TableScan: orders
"#
    );
}

#[test]
fn test_ambiguous_column_references_in_on_join() {
    let sql = "select p1.id, p1.age, p2.id
            from person as p1
            INNER JOIN person as p2
            ON id = 1";

    // It should return error.
    let result = logical_plan(sql);
    assert!(result.is_err());
    let err = result.err().unwrap().strip_backtrace();

    assert_snapshot!(
        err,
        @r###"
        Schema error: Ambiguous reference to unqualified field id
        "###
    );
}

#[test]
fn test_ambiguous_column_references_with_in_using_join() {
    let sql = "select p1.id, p1.age, p2.id
            from person as p1
            INNER JOIN person as p2
            using(id)";
    let plan = logical_plan(sql).unwrap();
    assert_snapshot!(
        plan,
        @r#"
Projection: p1.id, p1.age, p2.id
  Inner Join: Using p1.id = p2.id
    SubqueryAlias: p1
      TableScan: person
    SubqueryAlias: p2
      TableScan: person
"#
    );
}

#[test]
fn test_inner_join_with_cast_key() {
    let sql = "SELECT person.id, person.age
            FROM person
            INNER JOIN orders
            ON cast(person.id as Int) = cast(orders.customer_id as Int)";
    let plan = logical_plan(sql).unwrap();
    assert_snapshot!(
        plan,
        @r#"
Projection: person.id, person.age
  Inner Join:  Filter: CAST(person.id AS Int32) = CAST(orders.customer_id AS Int32)
    TableScan: person
    TableScan: orders
"#
    );
}

#[test]
fn test_multi_grouping_sets() {
    let sql = "SELECT person.id, person.age
            FROM person
            GROUP BY
                person.id,
                GROUPING SETS ((person.age,person.salary),(person.age))";
    let plan = logical_plan(sql).unwrap();
    assert_snapshot!(
        plan,
        @r#"
Projection: person.id, person.age
  Aggregate: groupBy=[[GROUPING SETS ((person.id, person.age, person.salary), (person.id, person.age))]], aggr=[[]]
    TableScan: person
"#
    );
    let sql = "SELECT person.id, person.age
            FROM person
            GROUP BY
                person.id,
                GROUPING SETS ((person.age, person.salary),(person.age)),
                ROLLUP(person.state, person.birth_date)";
    let plan = logical_plan(sql).unwrap();
    assert_snapshot!(
        plan,
        @r#"
Projection: person.id, person.age
  Aggregate: groupBy=[[GROUPING SETS ((person.id, person.age, person.salary), (person.id, person.age, person.salary, person.state), (person.id, person.age, person.salary, person.state, person.birth_date), (person.id, person.age), (person.id, person.age, person.state), (person.id, person.age, person.state, person.birth_date))]], aggr=[[]]
    TableScan: person
"#
    );
}

#[test]
fn test_field_not_found_window_function() {
    let order_by_sql = "SELECT count() OVER (order by a);";
    let order_by_err = logical_plan(order_by_sql)
        .expect_err("query should have failed")
        .strip_backtrace();

    assert_snapshot!(
        order_by_err,
        @r###"
        Schema error: No field named a.
        "###
    );

    let partition_by_sql = "SELECT count() OVER (PARTITION BY a);";
    let partition_by_err = logical_plan(partition_by_sql)
        .expect_err("query should have failed")
        .strip_backtrace();

    assert_snapshot!(
        partition_by_err,
        @r###"
        Schema error: No field named a.
        "###
    );

    let sql = "SELECT order_id, MAX(qty) OVER (PARTITION BY orders.order_id) from orders";
    let plan = logical_plan(sql).unwrap();
    assert_snapshot!(
        plan,
        @r#"
Projection: orders.order_id, max(orders.qty) PARTITION BY [orders.order_id] ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
  WindowAggr: windowExpr=[[max(orders.qty) PARTITION BY [orders.order_id] ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING]]
    TableScan: orders
"#
    );
}

#[test]
fn test_parse_escaped_string_literal_value() {
    let sql = r"SELECT character_length('\r\n') AS len";
    let plan = logical_plan(sql).unwrap();
    assert_snapshot!(
        plan,
        @r#"
    Projection: character_length(Utf8("\r\n")) AS len
      EmptyRelation: rows=1
    "#
    );
    let sql = "SELECT character_length(E'\r\n') AS len";
    let plan = logical_plan(sql).unwrap();
    assert_snapshot!(
        plan,
        @r#"
    Projection: character_length(Utf8("
    ")) AS len
      EmptyRelation: rows=1
    "#
    );
    let sql =
        r"SELECT character_length(E'\445') AS len, E'\x4B' AS hex, E'\u0001' AS unicode";
    let plan = logical_plan(sql).unwrap();
    assert_snapshot!(
        plan,
        @"Projection: character_length(Utf8(\"%\")) AS len, Utf8(\"K\") AS hex, Utf8(\"\u{1}\") AS unicode\n  EmptyRelation: rows=1"
    );

    let sql = r"SELECT character_length(E'\000') AS len";

    assert_snapshot!(
        logical_plan(sql).unwrap_err(),
        @r###"
        SQL error: TokenizerError("Unterminated encoded string literal at Line: 1, Column: 25")
        "###
    );
}

#[test]
fn plan_create_index() {
    let sql =
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_name ON test USING btree (name, age DESC)";
    let plan = logical_plan_with_options(sql, ParserOptions::default()).unwrap();
    match plan {
        LogicalPlan::Ddl(DdlStatement::CreateIndex(CreateIndex {
            name,
            table,
            using,
            columns,
            unique,
            if_not_exists,
            ..
        })) => {
            assert_eq!(name, Some("idx_name".to_string()));
            assert_eq!(format!("{table}"), "test");
            assert_eq!(using, Some("btree".to_string()));
            assert_eq!(
                columns,
                vec![col("name").sort(true, false), col("age").sort(false, true),]
            );
            assert!(unique);
            assert!(if_not_exists);
        }
        _ => panic!("wrong plan type"),
    }
}

fn assert_field_not_found(mut err: DataFusionError, name: &str) {
    let err = loop {
        match err {
            DataFusionError::Diagnostic(_, wrapped_err) => {
                err = *wrapped_err;
            }
            DataFusionError::Collection(errs) => {
                err = errs.into_iter().next().unwrap();
            }
            err => break err,
        }
    };
    match err {
        DataFusionError::SchemaError(_, _) => {
            let msg = format!("{err}");
            let expected = format!("Schema error: No field named {name}.");
            if !msg.starts_with(&expected) {
                panic!("error [{msg}] did not start with [{expected}]");
            }
        }
        _ => panic!("assert_field_not_found wrong error type"),
    }
}

#[cfg(test)]
#[ctor::ctor]
fn init() {
    // Enable RUST_LOG logging configuration for tests
    let _ = env_logger::try_init();
}

#[test]
fn test_no_functions_registered() {
    let sql = "SELECT foo()";

    let options = ParserOptions::default();
    let dialect = &GenericDialect {};
    let state = MockSessionState::default();
    let context = MockContextProvider { state };
    let planner = SqlToRel::new_with_options(&context, options);
    let result = DFParser::parse_sql_with_dialect(sql, dialect);
    let mut ast = result.unwrap();

    let err = planner.statement_to_plan(ast.pop_front().unwrap());

    assert_contains!(
        err.unwrap_err().to_string(),
        "Internal error: No functions registered with this context."
    );
}

#[test]
fn test_no_substring_registered() {
    // substring requires an expression planner
    let sql = "SELECT SUBSTRING(foo, bar, baz) FROM person";
    let err = logical_plan(sql).expect_err("query should have failed");

    assert_snapshot!(
        err.strip_backtrace(),
        @"This feature is not implemented: Substring could not be planned by registered expr planner. Hint: Please try with `unicode_expressions` DataFusion feature enabled"
    );
}

#[test]
fn test_no_substring_registered_alt_syntax() {
    // Alternate syntax for substring
    let sql = "SELECT SUBSTRING(foo FROM bar) FROM person";
    let err = logical_plan(sql).expect_err("query should have failed");

    assert_snapshot!(
        err.strip_backtrace(),
        @"This feature is not implemented: Substring could not be planned by registered expr planner. Hint: Please try with `unicode_expressions` DataFusion feature enabled"
    );
}

#[test]
fn test_custom_type_plan() -> Result<()> {
    let sql = "SELECT DATETIME '2001-01-01 18:00:00'";

    // test the default behavior
    let options = ParserOptions::default();
    let dialect = &GenericDialect {};
    let state = MockSessionState::default();
    let context = MockContextProvider { state };
    let planner = SqlToRel::new_with_options(&context, options);
    let result = DFParser::parse_sql_with_dialect(sql, dialect);
    let mut ast = result.unwrap();
    let err = planner.statement_to_plan(ast.pop_front().unwrap());
    assert_contains!(
        err.unwrap_err().to_string(),
        "This feature is not implemented: Unsupported SQL type DATETIME"
    );

    fn plan_sql(sql: &str) -> LogicalPlan {
        let options = ParserOptions::default();
        let dialect = &GenericDialect {};
        let state = MockSessionState::default()
            .with_scalar_function(make_array_udf())
            .with_expr_planner(Arc::new(CustomExprPlanner {}))
            .with_type_planner(Arc::new(CustomTypePlanner {}));
        let context = MockContextProvider { state };
        let planner = SqlToRel::new_with_options(&context, options);
        let result = DFParser::parse_sql_with_dialect(sql, dialect);
        let mut ast = result.unwrap();
        planner.statement_to_plan(ast.pop_front().unwrap()).unwrap()
    }

    let plan = plan_sql(sql);

    assert_snapshot!(
        plan,
        @r#"
    Projection: CAST(Utf8("2001-01-01 18:00:00") AS Timestamp(ns))
      EmptyRelation: rows=1
    "#
    );

    let plan = plan_sql("SELECT CAST(TIMESTAMP '2001-01-01 18:00:00' AS DATETIME)");

    assert_snapshot!(
        plan,
        @r#"
    Projection: CAST(CAST(Utf8("2001-01-01 18:00:00") AS Timestamp(ns)) AS Timestamp(ns))
      EmptyRelation: rows=1
    "#
    );

    let plan = plan_sql(
        "SELECT ARRAY[DATETIME '2001-01-01 18:00:00', DATETIME '2001-01-02 18:00:00']",
    );

    assert_snapshot!(
        plan,
        @r#"
    Projection: make_array(CAST(Utf8("2001-01-01 18:00:00") AS Timestamp(ns)), CAST(Utf8("2001-01-02 18:00:00") AS Timestamp(ns)))
      EmptyRelation: rows=1
    "#
    );

    Ok(())
}

fn error_message_test(sql: &str, err_msg_starts_with: &str) {
    let err = logical_plan(sql).expect_err("query should have failed");
    assert!(
        err.strip_backtrace().starts_with(err_msg_starts_with),
        "Expected error to start with '{}', but got: '{}'",
        err_msg_starts_with,
        err.strip_backtrace(),
    );
}

#[test]
fn test_error_message_invalid_scalar_function_signature() {
    error_message_test(
        "select sqrt()",
        "Error during planning: 'sqrt' does not support zero arguments",
    );
    error_message_test(
        "select sqrt(1, 2)",
        "Error during planning: Failed to coerce arguments",
    );
}

#[test]
fn test_error_message_invalid_aggregate_function_signature() {
    error_message_test(
        "select sum()",
        "Error during planning: Execution error: Function 'sum' user-defined coercion failed with \"Execution error: sum function requires 1 argument, got 0\"",
    );
    // We keep two different prefixes because they clarify each other.
    // It might be incorrect, and we should consider keeping only one.
    error_message_test(
        "select max(9, 3)",
        "Error during planning: Execution error: Function 'max' user-defined coercion failed",
    );
}

#[test]
fn test_error_message_invalid_window_function_signature() {
    error_message_test(
        "select rank(1) over()",
        "Error during planning: The function 'rank' expected zero argument but received 1",
    );
}

#[test]
fn test_error_message_invalid_window_aggregate_function_signature() {
    error_message_test(
        "select sum() over()",
        "Error during planning: Execution error: Function 'sum' user-defined coercion failed with \"Execution error: sum function requires 1 argument, got 0\"",
    );
}

// Test issue: https://github.com/apache/datafusion/issues/14058
// Select with wildcard over a USING/NATURAL JOIN should deduplicate condition columns.
#[test]
fn test_using_join_wildcard_schema() {
    let sql = "SELECT * FROM orders o1 JOIN orders o2 USING (order_id)";
    let plan = logical_plan(sql).unwrap();
    let count = plan
        .schema()
        .iter()
        .filter(|(_, f)| f.name() == "order_id")
        .count();
    // Only one order_id column
    assert_eq!(count, 1);

    let sql = "SELECT * FROM orders o1 NATURAL JOIN orders o2";
    let plan = logical_plan(sql).unwrap();
    // Only columns from one join side should be present
    let expected_fields = vec![
        "o1.order_id".to_string(),
        "o1.customer_id".to_string(),
        "o1.o_item_id".to_string(),
        "o1.qty".to_string(),
        "o1.price".to_string(),
        "o1.delivered".to_string(),
    ];
    assert_eq!(plan.schema().field_names(), expected_fields);

    // Reproducible example of issue #14058
    let sql = "WITH t1 AS (SELECT 1 AS id, 'a' AS value1),
        t2 AS (SELECT 1 AS id, 'x' AS value2)
        SELECT * FROM t1 NATURAL JOIN t2";
    let plan = logical_plan(sql).unwrap();
    assert_eq!(
        plan.schema().field_names(),
        [
            "t1.id".to_string(),
            "t1.value1".to_string(),
            "t2.value2".to_string()
        ]
    );

    // Multiple joins
    let sql = "WITH t1 AS (SELECT 1 AS a, 1 AS b),
        t2 AS (SELECT 1 AS a, 2 AS c),
        t3 AS (SELECT 1 AS c, 2 AS d)
        SELECT * FROM t1 NATURAL JOIN t2 RIGHT JOIN t3 USING (c)";
    let plan = logical_plan(sql).unwrap();
    assert_eq!(
        plan.schema().field_names(),
        [
            "t1.a".to_string(),
            "t1.b".to_string(),
            "t2.c".to_string(),
            "t3.d".to_string()
        ]
    );

    // Subquery
    let sql = "WITH t1 AS (SELECT 1 AS a, 1 AS b),
        t2 AS (SELECT 1 AS a, 2 AS c),
        t3 AS (SELECT 1 AS c, 2 AS d)
        SELECT * FROM (SELECT * FROM t1 LEFT JOIN t2 USING(a)) NATURAL JOIN t3";
    let plan = logical_plan(sql).unwrap();
    assert_eq!(
        plan.schema().field_names(),
        [
            "t1.a".to_string(),
            "t1.b".to_string(),
            "t2.c".to_string(),
            "t3.d".to_string()
        ]
    );
}
