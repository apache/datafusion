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

use arrow::util::pretty::{pretty_format_batches, pretty_format_columns};
use arrow_array::builder::{ListBuilder, StringBuilder};
use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray, StructArray};
use arrow_schema::{DataType, Field};
use datafusion::prelude::*;
use datafusion_common::{DFSchema, ScalarValue};
use datafusion_expr::ExprFunctionExt;
use datafusion_functions::core::expr_ext::FieldAccessor;
use datafusion_functions_aggregate::first_last::first_value_udaf;
use datafusion_functions_aggregate::sum::sum_udaf;
use datafusion_functions_nested::expr_ext::{IndexAccessor, SliceAccessor};
use sqlparser::ast::NullTreatment;
/// Tests of using and evaluating `Expr`s outside the context of a LogicalPlan
use std::sync::{Arc, OnceLock};

mod parse_sql_expr;
mod simplification;

#[test]
fn test_octet_length() {
    #[rustfmt::skip]
    evaluate_expr_test(
        octet_length(col("list")),
        vec![
            "+------+",
            "| expr |",
            "+------+",
            "| 5    |",
            "| 18   |",
            "| 6    |",
            "+------+",
        ],
    );
}

#[test]
fn test_eq() {
    // id = '2'
    evaluate_expr_test(
        col("id").eq(lit("2")),
        vec![
            "+-------+",
            "| expr  |",
            "+-------+",
            "| false |",
            "| true  |",
            "| false |",
            "+-------+",
        ],
    );
}

#[test]
fn test_eq_with_coercion() {
    // id = 2 (need to coerce the 2 to '2' to evaluate)
    evaluate_expr_test(
        col("id").eq(lit(2i32)),
        vec![
            "+-------+",
            "| expr  |",
            "+-------+",
            "| false |",
            "| true  |",
            "| false |",
            "+-------+",
        ],
    );
}

#[test]
fn test_get_field() {
    evaluate_expr_test(
        col("props").field("a"),
        vec![
            "+------------+",
            "| expr       |",
            "+------------+",
            "| 2021-02-01 |",
            "| 2021-02-02 |",
            "| 2021-02-03 |",
            "+------------+",
        ],
    );
}

#[test]
fn test_get_field_null() {
    #[rustfmt::skip]
    evaluate_expr_test(
        lit(ScalarValue::Null).field("a"),
        vec![
            "+------+",
            "| expr |",
            "+------+",
            "|      |",
            "+------+",
        ],
    );
}

#[test]
fn test_nested_get_field() {
    evaluate_expr_test(
        col("props")
            .field("a")
            .eq(lit("2021-02-02"))
            .or(col("id").eq(lit(1))),
        vec![
            "+-------+",
            "| expr  |",
            "+-------+",
            "| true  |",
            "| true  |",
            "| false |",
            "+-------+",
        ],
    );
}

#[test]
fn test_list_index() {
    #[rustfmt::skip]
    evaluate_expr_test(
        col("list").index(lit(1i64)),
        vec![
            "+------+",
            "| expr |",
            "+------+",
            "| one  |",
            "| two  |",
            "| five |",
            "+------+",
        ],
    );
}

#[test]
fn test_list_range() {
    evaluate_expr_test(
        col("list").range(lit(1i64), lit(2i64)),
        vec![
            "+--------------+",
            "| expr         |",
            "+--------------+",
            "| [one]        |",
            "| [two, three] |",
            "| [five]       |",
            "+--------------+",
        ],
    );
}

#[tokio::test]
async fn test_aggregate_ext_order_by() {
    let agg = first_value_udaf().call(vec![col("props")]);

    // ORDER BY id ASC
    let agg_asc = agg
        .clone()
        .order_by(vec![col("id").sort(true, true)])
        .build()
        .unwrap()
        .alias("asc");

    // ORDER BY id DESC
    let agg_desc = agg
        .order_by(vec![col("id").sort(false, true)])
        .build()
        .unwrap()
        .alias("desc");

    evaluate_agg_test(
        agg_asc,
        vec![
            "+-----------------+",
            "| asc             |",
            "+-----------------+",
            "| {a: 2021-02-01} |",
            "+-----------------+",
        ],
    )
    .await;

    evaluate_agg_test(
        agg_desc,
        vec![
            "+-----------------+",
            "| desc            |",
            "+-----------------+",
            "| {a: 2021-02-03} |",
            "+-----------------+",
        ],
    )
    .await;
}

#[tokio::test]
async fn test_aggregate_ext_filter() {
    let agg = first_value_udaf()
        .call(vec![col("i")])
        .order_by(vec![col("i").sort(true, true)])
        .filter(col("i").is_not_null())
        .build()
        .unwrap()
        .alias("val");

    #[rustfmt::skip]
    evaluate_agg_test(
        agg,
        vec![
            "+-----+",
            "| val |",
            "+-----+",
            "| 5   |",
            "+-----+",
        ],
    )
        .await;
}

#[tokio::test]
async fn test_aggregate_ext_distinct() {
    let agg = sum_udaf()
        .call(vec![lit(5)])
        // distinct sum should be 5, not 15
        .distinct()
        .build()
        .unwrap()
        .alias("distinct");

    evaluate_agg_test(
        agg,
        vec![
            "+----------+",
            "| distinct |",
            "+----------+",
            "| 5        |",
            "+----------+",
        ],
    )
    .await;
}

#[tokio::test]
async fn test_aggregate_ext_null_treatment() {
    let agg = first_value_udaf()
        .call(vec![col("i")])
        .order_by(vec![col("i").sort(true, true)]);

    let agg_respect = agg
        .clone()
        .null_treatment(NullTreatment::RespectNulls)
        .build()
        .unwrap()
        .alias("respect");

    let agg_ignore = agg
        .null_treatment(NullTreatment::IgnoreNulls)
        .build()
        .unwrap()
        .alias("ignore");

    evaluate_agg_test(
        agg_respect,
        vec![
            "+---------+",
            "| respect |",
            "+---------+",
            "|         |",
            "+---------+",
        ],
    )
    .await;

    evaluate_agg_test(
        agg_ignore,
        vec![
            "+--------+",
            "| ignore |",
            "+--------+",
            "| 5      |",
            "+--------+",
        ],
    )
    .await;
}

/// Evaluates the specified expr as an aggregate and compares the result to the
/// expected result.
async fn evaluate_agg_test(expr: Expr, expected_lines: Vec<&str>) {
    let batch = test_batch();

    let ctx = SessionContext::new();
    let group_expr = vec![];
    let agg_expr = vec![expr];
    let result = ctx
        .read_batch(batch)
        .unwrap()
        .aggregate(group_expr, agg_expr)
        .unwrap()
        .collect()
        .await
        .unwrap();

    let result = pretty_format_batches(&result).unwrap().to_string();
    let actual_lines = result.lines().collect::<Vec<_>>();

    assert_eq!(
        expected_lines, actual_lines,
        "\n\nexpected:\n\n{:#?}\nactual:\n\n{:#?}\n\n",
        expected_lines, actual_lines
    );
}

/// Converts the `Expr` to a `PhysicalExpr`, evaluates it against the provided
/// `RecordBatch` and compares the result to the expected result.
fn evaluate_expr_test(expr: Expr, expected_lines: Vec<&str>) {
    let batch = test_batch();
    let df_schema = DFSchema::try_from(batch.schema()).unwrap();
    let physical_expr = SessionContext::new()
        .create_physical_expr(expr, &df_schema)
        .unwrap();

    let result = physical_expr.evaluate(&batch).unwrap();
    let array = result.into_array(1).unwrap();
    let result = pretty_format_columns("expr", &[array]).unwrap().to_string();
    let actual_lines = result.lines().collect::<Vec<_>>();

    assert_eq!(
        expected_lines, actual_lines,
        "\n\nexpected:\n\n{:#?}\nactual:\n\n{:#?}\n\n",
        expected_lines, actual_lines
    );
}

static TEST_BATCH: OnceLock<RecordBatch> = OnceLock::new();

fn test_batch() -> RecordBatch {
    TEST_BATCH
        .get_or_init(|| {
            let string_array: ArrayRef = Arc::new(StringArray::from(vec!["1", "2", "3"]));
            let int_array: ArrayRef =
                Arc::new(Int64Array::from_iter(vec![Some(10), None, Some(5)]));

            // { a: "2021-02-01" } { a: "2021-02-02" } { a: "2021-02-03" }
            let struct_array: ArrayRef = Arc::from(StructArray::from(vec![(
                Arc::new(Field::new("a", DataType::Utf8, false)),
                Arc::new(StringArray::from(vec![
                    "2021-02-01",
                    "2021-02-02",
                    "2021-02-03",
                ])) as _,
            )]));

            // ["one"] ["two", "three", "four"] ["five"]
            let mut builder = ListBuilder::new(StringBuilder::new());
            builder.append_value([Some("one")]);
            builder.append_value([Some("two"), Some("three"), Some("four")]);
            builder.append_value([Some("five")]);
            let list_array: ArrayRef = Arc::new(builder.finish());

            RecordBatch::try_from_iter(vec![
                ("id", string_array),
                ("i", int_array),
                ("props", struct_array),
                ("list", list_array),
            ])
            .unwrap()
        })
        .clone()
}
