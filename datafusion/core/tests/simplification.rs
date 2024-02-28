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
use chrono::{DateTime, TimeZone, Utc};
use datafusion::common::DFSchema;
use datafusion::{error::Result, execution::context::ExecutionProps, prelude::*};
use datafusion_common::ScalarValue;
use datafusion_expr::{
    table_scan, Cast, Expr, ExprSchemable, LogicalPlan, LogicalPlanBuilder,
};
use datafusion_optimizer::simplify_expressions::{
    ExprSimplifier, SimplifyExpressions, SimplifyInfo,
};
use datafusion_optimizer::{OptimizerContext, OptimizerRule};

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
