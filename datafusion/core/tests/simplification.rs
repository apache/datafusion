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

//! This program demonstrates the DataFusion expression simplification API.

use arrow::datatypes::{DataType, Field, Schema};
use arrow_array::{ArrayRef, Int32Array};
use chrono::{DateTime, TimeZone, Utc};
use datafusion::common::DFSchema;
use datafusion::{error::Result, execution::context::ExecutionProps, prelude::*};
use datafusion_common::cast::as_int32_array;
use datafusion_common::ScalarValue;
use datafusion_expr::expr::ScalarFunction;
use datafusion_expr::{
    expr, table_scan, BuiltinScalarFunction, Cast, ColumnarValue, Expr, ExprSchemable,
    LogicalPlan, LogicalPlanBuilder, ScalarUDF, Volatility,
};
use datafusion_optimizer::simplify_expressions::{
    ExprSimplifier, SimplifyExpressions, SimplifyInfo,
};
use datafusion_optimizer::{OptimizerContext, OptimizerRule};
use std::sync::Arc;

/// In order to simplify expressions, DataFusion must have information
/// about the expressions.
///
/// You can provide that information using DataFusion [DFSchema]
/// objects or from some other implementation
struct MyInfo {
    /// The input schema
    schema: DFSchema,

    /// Execution specific details needed for constant evaluation such
    /// as the current time for `now()` and [VariableProviders]
    execution_props: ExecutionProps,
}

impl SimplifyInfo for MyInfo {
    fn is_boolean_type(&self, expr: &Expr) -> Result<bool> {
        Ok(matches!(expr.get_type(&self.schema)?, DataType::Boolean))
    }

    fn nullable(&self, expr: &Expr) -> Result<bool> {
        expr.nullable(&self.schema)
    }

    fn execution_props(&self) -> &ExecutionProps {
        &self.execution_props
    }

    fn get_data_type(&self, expr: &Expr) -> Result<DataType> {
        expr.get_type(&self.schema)
    }
}

impl From<DFSchema> for MyInfo {
    fn from(schema: DFSchema) -> Self {
        Self {
            schema,
            execution_props: ExecutionProps::new(),
        }
    }
}

/// A schema like:
///
/// a: Int32 (possibly with nulls)
/// b: Int32
/// s: Utf8
fn schema() -> DFSchema {
    Schema::new(vec![
        Field::new("a", DataType::Int32, true),
        Field::new("b", DataType::Int32, false),
        Field::new("s", DataType::Utf8, false),
    ])
    .try_into()
    .unwrap()
}

fn test_table_scan() -> LogicalPlan {
    let schema = Schema::new(vec![
        Field::new("a", DataType::Boolean, false),
        Field::new("b", DataType::Boolean, false),
        Field::new("c", DataType::Boolean, false),
        Field::new("d", DataType::UInt32, false),
        Field::new("e", DataType::UInt32, true),
    ]);
    table_scan(Some("test"), &schema, None)
        .expect("creating scan")
        .build()
        .expect("building plan")
}

fn get_optimized_plan_formatted(plan: &LogicalPlan, date_time: &DateTime<Utc>) -> String {
    let config = OptimizerContext::new().with_query_execution_start_time(*date_time);
    let rule = SimplifyExpressions::new();

    let optimized_plan = rule
        .try_optimize(plan, &config)
        .unwrap()
        .expect("failed to optimize plan");
    format!("{optimized_plan:?}")
}

// ------------------------------
// --- ConstEvaluator tests -----
// ------------------------------
fn test_evaluate_with_start_time(
    input_expr: Expr,
    expected_expr: Expr,
    date_time: &DateTime<Utc>,
) {
    let execution_props =
        ExecutionProps::new().with_query_execution_start_time(*date_time);

    let info: MyInfo = MyInfo {
        schema: schema(),
        execution_props,
    };
    let simplifier = ExprSimplifier::new(info);
    let simplified_expr = simplifier
        .simplify(input_expr.clone())
        .expect("successfully evaluated");

    assert_eq!(
        simplified_expr, expected_expr,
        "Mismatch evaluating {input_expr}\n  Expected:{expected_expr}\n  Got:{simplified_expr}"
    );
}

fn test_evaluate(input_expr: Expr, expected_expr: Expr) {
    test_evaluate_with_start_time(input_expr, expected_expr, &Utc::now())
}

// Make a UDF that adds its two values together, with the specified volatility
fn make_udf_add(volatility: Volatility) -> Arc<ScalarUDF> {
    let input_types = vec![DataType::Int32, DataType::Int32];
    let return_type = Arc::new(DataType::Int32);

    let fun = Arc::new(|args: &[ColumnarValue]| {
        let args = ColumnarValue::values_to_arrays(args)?;

        let arg0 = as_int32_array(&args[0])?;
        let arg1 = as_int32_array(&args[1])?;

        // 2. perform the computation
        let array = arg0
            .iter()
            .zip(arg1.iter())
            .map(|args| {
                if let (Some(arg0), Some(arg1)) = args {
                    Some(arg0 + arg1)
                } else {
                    // one or both args were Null
                    None
                }
            })
            .collect::<Int32Array>();

        Ok(ColumnarValue::from(Arc::new(array) as ArrayRef))
    });

    Arc::new(create_udf(
        "udf_add",
        input_types,
        return_type,
        volatility,
        fun,
    ))
}

fn now_expr() -> Expr {
    call_fn("now", vec![]).unwrap()
}

fn cast_to_int64_expr(expr: Expr) -> Expr {
    Expr::Cast(Cast::new(expr.into(), DataType::Int64))
}

fn to_timestamp_expr(arg: impl Into<String>) -> Expr {
    to_timestamp(vec![lit(arg.into())])
}

#[test]
fn basic() {
    let info: MyInfo = schema().into();

    // The `Expr` is a core concept in DataFusion, and DataFusion can
    // help simplify it.

    // For example 'a < (2 + 3)' can be rewritten into the easier to
    // optimize form `a < 5` automatically
    let expr = col("a").lt(lit(2i32) + lit(3i32));

    let simplifier = ExprSimplifier::new(info);
    let simplified = simplifier.simplify(expr).unwrap();
    assert_eq!(simplified, col("a").lt(lit(5i32)));
}

#[test]
fn fold_and_simplify() {
    let info: MyInfo = schema().into();

    // What will it do with the expression `concat('foo', 'bar') == 'foobar')`?
    let expr = concat(&[lit("foo"), lit("bar")]).eq(lit("foobar"));

    // Since datafusion applies both simplification *and* rewriting
    // some expressions can be entirely simplified
    let simplifier = ExprSimplifier::new(info);
    let simplified = simplifier.simplify(expr).unwrap();
    assert_eq!(simplified, lit(true))
}

#[test]
/// Ensure that timestamp expressions are folded so they aren't invoked on each row
fn to_timestamp_expr_folded() -> Result<()> {
    let table_scan = test_table_scan();
    let proj = vec![to_timestamp_expr("2020-09-08T12:00:00+00:00")];

    let plan = LogicalPlanBuilder::from(table_scan)
        .project(proj)?
        .build()?;

    let expected = "Projection: TimestampNanosecond(1599566400000000000, None) AS to_timestamp(Utf8(\"2020-09-08T12:00:00+00:00\"))\
            \n  TableScan: test"
        .to_string();
    let actual = get_optimized_plan_formatted(&plan, &Utc::now());
    assert_eq!(expected, actual);
    Ok(())
}

#[test]
fn now_less_than_timestamp() -> Result<()> {
    let table_scan = test_table_scan();

    let ts_string = "2020-09-08T12:05:00+00:00";
    let time = Utc.timestamp_nanos(1599566400000000000i64);

    //  cast(now() as int) < cast(to_timestamp(...) as int) + 50000_i64
    let plan = LogicalPlanBuilder::from(table_scan)
        .filter(
            cast_to_int64_expr(now_expr())
                .lt(cast_to_int64_expr(to_timestamp_expr(ts_string)) + lit(50000_i64)),
        )?
        .build()?;

    // Note that constant folder runs and folds the entire
    // expression down to a single constant (true)
    let expected = "Filter: Boolean(true)\
                        \n  TableScan: test";
    let actual = get_optimized_plan_formatted(&plan, &time);

    assert_eq!(expected, actual);
    Ok(())
}

#[test]
fn select_date_plus_interval() -> Result<()> {
    let table_scan = test_table_scan();

    let ts_string = "2020-09-08T12:05:00+00:00";
    let time = Utc.timestamp_nanos(1599566400000000000i64);

    //  now() < cast(to_timestamp(...) as int) + 5000000000
    let schema = table_scan.schema();

    let date_plus_interval_expr = to_timestamp_expr(ts_string)
        .cast_to(&DataType::Date32, schema)?
        + Expr::Literal(ScalarValue::IntervalDayTime(Some(123i64 << 32)));

    let plan = LogicalPlanBuilder::from(table_scan.clone())
        .project(vec![date_plus_interval_expr])?
        .build()?;

    // Note that constant folder runs and folds the entire
    // expression down to a single constant (true)
    let expected = r#"Projection: Date32("18636") AS to_timestamp(Utf8("2020-09-08T12:05:00+00:00")) + IntervalDayTime("528280977408")
  TableScan: test"#;
    let actual = get_optimized_plan_formatted(&plan, &time);

    assert_eq!(expected, actual);
    Ok(())
}

#[test]
fn test_const_evaluator() {
    // true --> true
    test_evaluate(lit(true), lit(true));
    // true or true --> true
    test_evaluate(lit(true).or(lit(true)), lit(true));
    // true or false --> true
    test_evaluate(lit(true).or(lit(false)), lit(true));

    // "foo" == "foo" --> true
    test_evaluate(lit("foo").eq(lit("foo")), lit(true));
    // "foo" != "foo" --> false
    test_evaluate(lit("foo").not_eq(lit("foo")), lit(false));

    // c = 1 --> c = 1
    test_evaluate(col("c").eq(lit(1)), col("c").eq(lit(1)));
    // c = 1 + 2 --> c + 3
    test_evaluate(col("c").eq(lit(1) + lit(2)), col("c").eq(lit(3)));
    // (foo != foo) OR (c = 1) --> false OR (c = 1)
    test_evaluate(
        (lit("foo").not_eq(lit("foo"))).or(col("c").eq(lit(1))),
        col("c").eq(lit(1)),
    );
}

#[test]
fn test_const_evaluator_scalar_functions() {
    // concat("foo", "bar") --> "foobar"
    let expr = call_fn("concat", vec![lit("foo"), lit("bar")]).unwrap();
    test_evaluate(expr, lit("foobar"));

    // ensure arguments are also constant folded
    // concat("foo", concat("bar", "baz")) --> "foobarbaz"
    let concat1 = call_fn("concat", vec![lit("bar"), lit("baz")]).unwrap();
    let expr = call_fn("concat", vec![lit("foo"), concat1]).unwrap();
    test_evaluate(expr, lit("foobarbaz"));

    // Check non string arguments
    // to_timestamp("2020-09-08T12:00:00+00:00") --> timestamp(1599566400i64)
    let expr = to_timestamp(vec![lit("2020-09-08T12:00:00+00:00")]);
    test_evaluate(expr, lit_timestamp_nano(1599566400000000000i64));

    // check that non foldable arguments are folded
    // to_timestamp(a) --> to_timestamp(a) [no rewrite possible]
    let expr = to_timestamp(vec![col("a")]);
    test_evaluate(expr.clone(), expr);

    // volatile / stable functions should not be evaluated
    // rand() + (1 + 2) --> rand() + 3
    let fun = BuiltinScalarFunction::Random;
    assert_eq!(fun.volatility(), Volatility::Volatile);
    let rand = Expr::ScalarFunction(ScalarFunction::new(fun, vec![]));
    let expr = rand.clone() + (lit(1) + lit(2));
    let expected = rand + lit(3);
    test_evaluate(expr, expected);

    // parenthesization matters: can't rewrite
    // (rand() + 1) + 2 --> (rand() + 1) + 2)
    let fun = BuiltinScalarFunction::Random;
    let rand = Expr::ScalarFunction(ScalarFunction::new(fun, vec![]));
    let expr = (rand + lit(1)) + lit(2);
    test_evaluate(expr.clone(), expr);
}

#[test]
fn test_const_evaluator_now() {
    let ts_nanos = 1599566400000000000i64;
    let time = chrono::Utc.timestamp_nanos(ts_nanos);
    let ts_string = "2020-09-08T12:05:00+00:00";
    // now() --> ts
    test_evaluate_with_start_time(now_expr(), lit_timestamp_nano(ts_nanos), &time);

    // CAST(now() as int64) + 100_i64 --> ts + 100_i64
    let expr = cast_to_int64_expr(now_expr()) + lit(100_i64);
    test_evaluate_with_start_time(expr, lit(ts_nanos + 100), &time);

    //  CAST(now() as int64) < cast(to_timestamp(...) as int64) + 50000_i64 ---> true
    let expr = cast_to_int64_expr(now_expr())
        .lt(cast_to_int64_expr(to_timestamp_expr(ts_string)) + lit(50000i64));
    test_evaluate_with_start_time(expr, lit(true), &time);
}

#[test]
fn test_evaluator_udfs() {
    let args = vec![lit(1) + lit(2), lit(30) + lit(40)];
    let folded_args = vec![lit(3), lit(70)];

    // immutable UDF should get folded
    // udf_add(1+2, 30+40) --> 73
    let expr = Expr::ScalarFunction(expr::ScalarFunction::new_udf(
        make_udf_add(Volatility::Immutable),
        args.clone(),
    ));
    test_evaluate(expr, lit(73));

    // stable UDF should be entirely folded
    // udf_add(1+2, 30+40) --> 73
    let fun = make_udf_add(Volatility::Stable);
    let expr = Expr::ScalarFunction(expr::ScalarFunction::new_udf(
        Arc::clone(&fun),
        args.clone(),
    ));
    test_evaluate(expr, lit(73));

    // volatile UDF should have args folded
    // udf_add(1+2, 30+40) --> udf_add(3, 70)
    let fun = make_udf_add(Volatility::Volatile);
    let expr =
        Expr::ScalarFunction(expr::ScalarFunction::new_udf(Arc::clone(&fun), args));
    let expected_expr = Expr::ScalarFunction(expr::ScalarFunction::new_udf(
        Arc::clone(&fun),
        folded_args,
    ));
    test_evaluate(expr, expected_expr);
}
