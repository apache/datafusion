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

//! Tests for the DataFusion SQL query planner that require functions from the
//! datafusion-functions crate.

use datafusion_expr::execution_props::ExecutionProps;
use datafusion_expr::simplify::SimplifyContext;
use datafusion_optimizer::simplify_expressions::ExprSimplifier;
use insta::assert_snapshot;
use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

use arrow::datatypes::{
    DataType, Field, Fields, Schema, SchemaBuilder, SchemaRef, TimeUnit,
};
use datafusion::functions::datetime::expr_fn;
use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::TransformedResult;
use datafusion_common::{
    plan_err, DFSchema, DFSchemaRef, Result, ScalarValue, TableReference,
};
use datafusion_expr::interval_arithmetic::{Interval, NullableInterval};
use datafusion_expr::{
    and, col, lit, or, AggregateUDF, BinaryExpr, Expr, ExprSchemable, LogicalPlan,
    Operator, ScalarUDF, TableSource, WindowUDF,
};
use datafusion_functions::core::expr_ext::FieldAccessor;
use datafusion_optimizer::analyzer::Analyzer;
use datafusion_optimizer::optimizer::Optimizer;
use datafusion_optimizer::{OptimizerConfig, OptimizerContext};
use datafusion_sql::planner::{ContextProvider, SqlToRel};
use datafusion_sql::sqlparser::ast::Statement;
use datafusion_sql::sqlparser::dialect::GenericDialect;
use datafusion_sql::sqlparser::parser::Parser;

use chrono::DateTime;
use datafusion_expr::expr_rewriter::rewrite_with_guarantees;
use datafusion_functions::datetime;

#[cfg(test)]
#[ctor::ctor]
fn init() {
    // enable logging so RUST_LOG works
    let _ = env_logger::try_init();
}

#[test]
fn select_arrow_cast() {
    let sql = "SELECT arrow_cast(1234, 'Float64') as f64, arrow_cast('foo', 'LargeUtf8') as large";
    let plan = test_sql(sql).unwrap();
    assert_snapshot!(
        plan,
        @r#"
    Projection: Float64(1234) AS f64, LargeUtf8("foo") AS large
      EmptyRelation: rows=1
    "#
    );
}
#[test]
fn timestamp_nano_ts_none_predicates() -> Result<()> {
    let sql = "SELECT col_int32
        FROM test
        WHERE col_ts_nano_none < (now() - interval '1 hour')";
    // a scan should have the now()... predicate folded to a single
    // constant and compared to the column without a cast so it can be
    // pushed down / pruned
    let plan = test_sql(sql).unwrap();
    assert_snapshot!(
        plan,
        @r"
    Projection: test.col_int32
      Filter: test.col_ts_nano_none < TimestampNanosecond(1666612093000000000, None)
        TableScan: test projection=[col_int32, col_ts_nano_none]
    "
    );
    Ok(())
}

#[test]
fn timestamp_nano_ts_utc_predicates() {
    let sql = "SELECT col_int32
        FROM test
        WHERE col_ts_nano_utc < (now() - interval '1 hour')";
    // a scan should have the now()... predicate folded to a single
    // constant and compared to the column without a cast so it can be
    // pushed down / pruned
    let plan = test_sql(sql).unwrap();
    assert_snapshot!(
        plan,
        @r#"
    Projection: test.col_int32
      Filter: test.col_ts_nano_utc < TimestampNanosecond(1666612093000000000, Some("+00:00"))
        TableScan: test projection=[col_int32, col_ts_nano_utc]
    "#
    );
}

#[test]
fn concat_literals() -> Result<()> {
    let sql = "SELECT concat(true, col_int32, false, null, 'hello', col_utf8, 12, 3.4) \
        AS col
        FROM test";
    let plan = test_sql(sql).unwrap();
    assert_snapshot!(
        plan,
        @r#"
    Projection: concat(Utf8("true"), CAST(test.col_int32 AS Utf8), Utf8("falsehello"), test.col_utf8, Utf8("123.4")) AS col
      TableScan: test projection=[col_int32, col_utf8]
    "#
    );
    Ok(())
}

#[test]
fn concat_ws_literals() -> Result<()> {
    let sql = "SELECT concat_ws('-', true, col_int32, false, null, 'hello', col_utf8, 12, '', 3.4) \
        AS col
        FROM test";
    let plan = test_sql(sql).unwrap();
    assert_snapshot!(
        plan,
        @r#"
    Projection: concat_ws(Utf8("-"), Utf8("true"), CAST(test.col_int32 AS Utf8), Utf8("false-hello"), test.col_utf8, Utf8("12--3.4")) AS col
      TableScan: test projection=[col_int32, col_utf8]
    "#
    );
    Ok(())
}

fn test_sql(sql: &str) -> Result<LogicalPlan> {
    // parse the SQL
    let dialect = GenericDialect {}; // or AnsiDialect, or your own dialect ...
    let ast: Vec<Statement> = Parser::parse_sql(&dialect, sql).unwrap();
    let statement = &ast[0];

    // create a logical query plan
    let config = ConfigOptions::default();
    let context_provider = MyContextProvider::default()
        .with_udf(datetime::now(&config))
        .with_udf(datafusion_functions::core::arrow_cast())
        .with_udf(datafusion_functions::string::concat())
        .with_udf(datafusion_functions::string::concat_ws());
    let sql_to_rel = SqlToRel::new(&context_provider);
    let plan = sql_to_rel.sql_statement_to_plan(statement.clone()).unwrap();

    // hard code the return value of now()
    let now_time = DateTime::from_timestamp(1666615693, 0).unwrap();
    let config = OptimizerContext::new()
        .with_skip_failing_rules(false)
        .with_query_execution_start_time(now_time);
    let analyzer = Analyzer::new();
    let optimizer = Optimizer::new();
    // analyze and optimize the logical plan
    let plan = analyzer.execute_and_check(plan, &config.options(), |_, _| {})?;
    optimizer.optimize(plan, &config, |_, _| {})
}

#[derive(Default)]
struct MyContextProvider {
    options: ConfigOptions,
    udfs: HashMap<String, Arc<ScalarUDF>>,
}

impl MyContextProvider {
    fn with_udf(mut self, udf: Arc<ScalarUDF>) -> Self {
        self.udfs.insert(udf.name().to_string(), udf);
        self
    }
}

impl ContextProvider for MyContextProvider {
    fn get_table_source(&self, name: TableReference) -> Result<Arc<dyn TableSource>> {
        let table_name = name.table();
        if table_name.starts_with("test") {
            let schema = Schema::new_with_metadata(
                vec![
                    Field::new("col_int32", DataType::Int32, true),
                    Field::new("col_uint32", DataType::UInt32, true),
                    Field::new("col_utf8", DataType::Utf8, true),
                    Field::new("col_date32", DataType::Date32, true),
                    Field::new("col_date64", DataType::Date64, true),
                    // timestamp with no timezone
                    Field::new(
                        "col_ts_nano_none",
                        DataType::Timestamp(TimeUnit::Nanosecond, None),
                        true,
                    ),
                    // timestamp with UTC timezone
                    Field::new(
                        "col_ts_nano_utc",
                        DataType::Timestamp(TimeUnit::Nanosecond, Some("+00:00".into())),
                        true,
                    ),
                ],
                HashMap::new(),
            );

            Ok(Arc::new(MyTableSource {
                schema: Arc::new(schema),
            }))
        } else {
            plan_err!("table does not exist")
        }
    }

    fn get_function_meta(&self, name: &str) -> Option<Arc<ScalarUDF>> {
        self.udfs.get(name).cloned()
    }

    fn get_aggregate_meta(&self, _name: &str) -> Option<Arc<AggregateUDF>> {
        None
    }

    fn get_variable_type(&self, _variable_names: &[String]) -> Option<DataType> {
        None
    }

    fn get_window_meta(&self, _name: &str) -> Option<Arc<WindowUDF>> {
        None
    }

    fn options(&self) -> &ConfigOptions {
        &self.options
    }

    fn udf_names(&self) -> Vec<String> {
        Vec::new()
    }

    fn udaf_names(&self) -> Vec<String> {
        Vec::new()
    }

    fn udwf_names(&self) -> Vec<String> {
        Vec::new()
    }
}

struct MyTableSource {
    schema: SchemaRef,
}

impl TableSource for MyTableSource {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

#[test]
fn test_nested_schema_nullability() {
    let mut builder = SchemaBuilder::new();
    builder.push(Field::new("foo", DataType::Int32, true));
    builder.push(Field::new(
        "parent",
        DataType::Struct(Fields::from(vec![Field::new(
            "child",
            DataType::Int64,
            false,
        )])),
        true,
    ));
    let schema = builder.finish();

    let dfschema = DFSchema::from_field_specific_qualified_schema(
        vec![Some("table_name".into()), None],
        &Arc::new(schema),
    )
    .unwrap();

    let expr = col("parent").field("child");
    assert!(expr.nullable(&dfschema).unwrap());
}

#[test]
fn test_inequalities_non_null_bounded() {
    let guarantees = [
        // x ∈ [1, 3] (not null)
        (
            col("x"),
            NullableInterval::NotNull {
                values: Interval::make(Some(1_i32), Some(3_i32)).unwrap(),
            },
        ),
        // s.y ∈ [1, 3] (not null)
        (
            col("s").field("y"),
            NullableInterval::NotNull {
                values: Interval::make(Some(1_i32), Some(3_i32)).unwrap(),
            },
        ),
    ];

    // (original_expr, expected_simplification)
    let simplified_cases = &[
        (col("x").lt(lit(0)), false),
        (col("s").field("y").lt(lit(0)), false),
        (col("x").lt_eq(lit(3)), true),
        (col("x").gt(lit(3)), false),
        (col("x").gt(lit(0)), true),
        (col("x").eq(lit(0)), false),
        (col("x").not_eq(lit(0)), true),
        (col("x").between(lit(0), lit(5)), true),
        (col("x").between(lit(5), lit(10)), false),
        (col("x").not_between(lit(0), lit(5)), false),
        (col("x").not_between(lit(5), lit(10)), true),
        (
            Expr::BinaryExpr(BinaryExpr {
                left: Box::new(col("x")),
                op: Operator::IsDistinctFrom,
                right: Box::new(lit(ScalarValue::Null)),
            }),
            true,
        ),
        (
            Expr::BinaryExpr(BinaryExpr {
                left: Box::new(col("x")),
                op: Operator::IsDistinctFrom,
                right: Box::new(lit(5)),
            }),
            true,
        ),
    ];

    validate_simplified_cases(&guarantees, simplified_cases);

    let unchanged_cases = &[
        col("x").gt(lit(2)),
        col("x").lt_eq(lit(2)),
        col("x").eq(lit(2)),
        col("x").not_eq(lit(2)),
        col("x").between(lit(3), lit(5)),
        col("x").not_between(lit(3), lit(10)),
    ];

    validate_unchanged_cases(&guarantees, unchanged_cases);
}

fn validate_simplified_cases<T>(
    guarantees: &[(Expr, NullableInterval)],
    cases: &[(Expr, T)],
) where
    ScalarValue: From<T>,
    T: Clone,
{
    for (expr, expected_value) in cases {
        let output = rewrite_with_guarantees(expr.clone(), guarantees)
            .data()
            .unwrap();
        let expected = lit(ScalarValue::from(expected_value.clone()));
        assert_eq!(
            output, expected,
            "{expr} simplified to {output}, but expected {expected}"
        );
    }
}
fn validate_unchanged_cases(guarantees: &[(Expr, NullableInterval)], cases: &[Expr]) {
    for expr in cases {
        let output = rewrite_with_guarantees(expr.clone(), guarantees)
            .data()
            .unwrap();
        assert_eq!(
            &output, expr,
            "{expr} was simplified to {output}, but expected it to be unchanged"
        );
    }
}

// DatePart preimage tests
#[test]
fn test_preimage_date_part_date32_eq() {
    let schema = expr_test_schema();
    // date_part(c1, DatePart::Year) = 2024 -> c1 >= 2024-01-01 AND c1 < 2025-01-01
    let expr_lt = expr_fn::date_part(lit("year"), col("date32")).eq(lit(2024i32));
    let expected = and(
        col("date32").gt_eq(lit(ScalarValue::Date32(Some(19723)))),
        col("date32").lt(lit(ScalarValue::Date32(Some(20089)))),
    );
    assert_eq!(optimize_test(expr_lt, &schema), expected)
}

#[test]
fn test_preimage_date_part_date64_not_eq() {
    let schema = expr_test_schema();
    // date_part(c1, DatePart::Year) <> 2024 -> c1 < 2024-01-01 AND c1 >= 2025-01-01
    let expr_lt = expr_fn::date_part(lit("year"), col("date64")).not_eq(lit(2024i32));
    let expected = or(
        col("date64").lt(lit(ScalarValue::Date64(Some(19723 * 86_400_000)))),
        col("date64").gt_eq(lit(ScalarValue::Date64(Some(20089 * 86_400_000)))),
    );
    assert_eq!(optimize_test(expr_lt, &schema), expected)
}

#[test]
fn test_preimage_date_part_timestamp_nano_lt() {
    let schema = expr_test_schema();
    let expr_lt = expr_fn::date_part(lit("year"), col("ts_nano_none")).lt(lit(2024i32));
    let expected = col("ts_nano_none").lt(lit(ScalarValue::TimestampNanosecond(
        Some(19723 * 86_400_000_000_000),
        None,
    )));
    assert_eq!(optimize_test(expr_lt, &schema), expected)
}

#[test]
fn test_preimage_date_part_timestamp_nano_utc_gt() {
    let schema = expr_test_schema();
    let expr_lt = expr_fn::date_part(lit("year"), col("ts_nano_utc")).gt(lit(2024i32));
    let expected = col("ts_nano_utc").gt_eq(lit(ScalarValue::TimestampNanosecond(
        Some(20089 * 86_400_000_000_000),
        None,
    )));
    assert_eq!(optimize_test(expr_lt, &schema), expected)
}

#[test]
fn test_preimage_date_part_timestamp_sec_est_gt_eq() {
    let schema = expr_test_schema();
    let expr_lt = expr_fn::date_part(lit("year"), col("ts_sec_est")).gt_eq(lit(2024i32));
    let expected = col("ts_sec_est").gt_eq(lit(ScalarValue::TimestampSecond(
        Some(19723 * 86_400),
        None,
    )));
    assert_eq!(optimize_test(expr_lt, &schema), expected)
}

#[test]
fn test_preimage_date_part_timestamp_sec_est_lt_eq() {
    let schema = expr_test_schema();
    let expr_lt = expr_fn::date_part(lit("year"), col("ts_mic_pt")).lt_eq(lit(2024i32));
    let expected = col("ts_mic_pt").lt(lit(ScalarValue::TimestampMicrosecond(
        Some(20089 * 86_400_000_000),
        None,
    )));
    assert_eq!(optimize_test(expr_lt, &schema), expected)
}

#[test]
fn test_preimage_date_part_timestamp_nano_lt_swap() {
    let schema = expr_test_schema();
    let expr_lt = lit(2024i32).gt(expr_fn::date_part(lit("year"), col("ts_nano_none")));
    let expected = col("ts_nano_none").lt(lit(ScalarValue::TimestampNanosecond(
        Some(19723 * 86_400_000_000_000),
        None,
    )));
    assert_eq!(optimize_test(expr_lt, &schema), expected)
}

#[test]
// Should not simplify
fn test_preimage_date_part_not_year_date32_eq() {
    let schema = expr_test_schema();
    // date_part(c1, DatePart::Year) = 2024 -> c1 >= 2024-01-01 AND c1 < 2025-01-01
    let expr_lt = expr_fn::date_part(lit("month"), col("date32")).eq(lit(1i32));
    let expected = expr_fn::date_part(lit("month"), col("date32")).eq(lit(1i32));
    assert_eq!(optimize_test(expr_lt, &schema), expected)
}

fn optimize_test(expr: Expr, schema: &DFSchemaRef) -> Expr {
    let props = ExecutionProps::new();
    let simplifier =
        ExprSimplifier::new(SimplifyContext::new(&props).with_schema(Arc::clone(schema)));

    simplifier.simplify(expr).unwrap()
}

fn expr_test_schema() -> DFSchemaRef {
    Arc::new(
        DFSchema::from_unqualified_fields(
            vec![
                Field::new("date32", DataType::Date32, false),
                Field::new("date64", DataType::Date64, false),
                Field::new("ts_nano_none", timestamp_nano_none_type(), false),
                Field::new("ts_nano_utc", timestamp_nano_utc_type(), false),
                Field::new("ts_sec_est", timestamp_sec_est_type(), false),
                Field::new("ts_mic_pt", timestamp_mic_pt_type(), false),
            ]
            .into(),
            HashMap::new(),
        )
        .unwrap(),
    )
}

fn timestamp_nano_none_type() -> DataType {
    DataType::Timestamp(TimeUnit::Nanosecond, None)
}

// this is the type that now() returns
fn timestamp_nano_utc_type() -> DataType {
    let utc = Some("+0:00".into());
    DataType::Timestamp(TimeUnit::Nanosecond, utc)
}

fn timestamp_sec_est_type() -> DataType {
    let est = Some("-5:00".into());
    DataType::Timestamp(TimeUnit::Second, est)
}

fn timestamp_mic_pt_type() -> DataType {
    let pt = Some("-8::00".into());
    DataType::Timestamp(TimeUnit::Microsecond, pt)
}
