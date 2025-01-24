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

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{BooleanArray, Int32Array, Int8Array};
use arrow::record_batch::RecordBatch;

use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use datafusion::common::DFSchema;
use datafusion::error::Result;
use datafusion::functions_aggregate::first_last::first_value_udaf;
use datafusion::optimizer::simplify_expressions::ExprSimplifier;
use datafusion::physical_expr::{analyze, AnalysisContext, ExprBoundaries};
use datafusion::prelude::*;
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_common::{ScalarValue, ToDFSchema};
use datafusion_expr::execution_props::ExecutionProps;
use datafusion_expr::expr::BinaryExpr;
use datafusion_expr::interval_arithmetic::Interval;
use datafusion_expr::simplify::SimplifyContext;
use datafusion_expr::{ColumnarValue, ExprFunctionExt, ExprSchemable, Operator};
use datafusion_optimizer::analyzer::type_coercion::TypeCoercionRewriter;

/// This example demonstrates the DataFusion [`Expr`] API.
///
/// DataFusion comes with a powerful and extensive system for
/// representing and manipulating expressions such as `A + 5` and `X
/// IN ('foo', 'bar', 'baz')`.
///
/// In addition to building and manipulating [`Expr`]s, DataFusion
/// also comes with APIs for evaluation, simplification, and analysis.
///
/// The code in this example shows how to:
/// 1. Create [`Expr`]s using different APIs: [`main`]`
/// 2. Use the fluent API to easily create complex [`Expr`]s:  [`expr_fn_demo`]
/// 3. Evaluate [`Expr`]s against data: [`evaluate_demo`]
/// 4. Simplify expressions: [`simplify_demo`]
/// 5. Analyze predicates for boundary ranges: [`range_analysis_demo`]
/// 6. Get the types of the expressions: [`expression_type_demo`]
/// 7. Apply type coercion to expressions: [`type_coercion_demo`]
#[tokio::main]
async fn main() -> Result<()> {
    // The easiest way to do create expressions is to use the
    // "fluent"-style API:
    let expr = col("a") + lit(5);

    // The same same expression can be created directly, with much more code:
    let expr2 = Expr::BinaryExpr(BinaryExpr::new(
        Box::new(col("a")),
        Operator::Plus,
        Box::new(Expr::Literal(ScalarValue::Int32(Some(5)))),
    ));
    assert_eq!(expr, expr2);

    // See how to build aggregate functions with the expr_fn API
    expr_fn_demo()?;

    // See how to evaluate expressions
    evaluate_demo()?;

    // See how to simplify expressions
    simplify_demo()?;

    // See how to analyze ranges in expressions
    range_analysis_demo()?;

    // See how to determine the data types of expressions
    expression_type_demo()?;

    // See how to type coerce expressions.
    type_coercion_demo()?;

    Ok(())
}

/// DataFusion's `expr_fn` API makes it easy to create [`Expr`]s for the
/// full range of expression types such as aggregates and window functions.
fn expr_fn_demo() -> Result<()> {
    // Let's say you want to call the "first_value" aggregate function
    let first_value = first_value_udaf();

    // For example, to create the expression `FIRST_VALUE(price)`
    // These expressions can be passed to `DataFrame::aggregate` and other
    // APIs that take aggregate expressions.
    let agg = first_value.call(vec![col("price")]);
    assert_eq!(agg.to_string(), "first_value(price)");

    // You can use the ExprFunctionExt trait to create more complex aggregates
    // such as `FIRST_VALUE(price FILTER quantity > 100 ORDER BY ts )
    let agg = first_value
        .call(vec![col("price")])
        .order_by(vec![col("ts").sort(false, false)])
        .filter(col("quantity").gt(lit(100)))
        .build()?; // build the aggregate
    assert_eq!(
        agg.to_string(),
        "first_value(price) FILTER (WHERE quantity > Int32(100)) ORDER BY [ts DESC NULLS LAST]"
    );

    Ok(())
}

/// DataFusion can also evaluate arbitrary expressions on Arrow arrays.
fn evaluate_demo() -> Result<()> {
    // For example, let's say you have some integers in an array
    let batch = RecordBatch::try_from_iter([(
        "a",
        Arc::new(Int32Array::from(vec![4, 5, 6, 7, 8, 7, 4])) as _,
    )])?;

    // If you want to find all rows where the expression `a < 5 OR a = 8` is true
    let expr = col("a").lt(lit(5)).or(col("a").eq(lit(8)));

    // First, you make a "physical expression" from the logical `Expr`
    let df_schema = DFSchema::try_from(batch.schema())?;
    let physical_expr = SessionContext::new().create_physical_expr(expr, &df_schema)?;

    // Now, you can evaluate the expression against the RecordBatch
    let result = physical_expr.evaluate(&batch)?;

    // The result contain an array that is true only for where `a < 5 OR a = 8`
    let expected_result = Arc::new(BooleanArray::from(vec![
        true, false, false, false, true, false, true,
    ])) as _;
    assert!(
        matches!(&result, ColumnarValue::Array(r) if r == &expected_result),
        "result: {:?}",
        result
    );

    Ok(())
}

/// In addition to easy construction, DataFusion exposes APIs for simplifying
/// such expression so they are more efficient to evaluate. This code is also
/// used by the query engine to optimize queries.
fn simplify_demo() -> Result<()> {
    // For example, lets say you have has created an expression such
    // ts = to_timestamp("2020-09-08T12:00:00+00:00")
    let expr = col("ts").eq(to_timestamp(vec![lit("2020-09-08T12:00:00+00:00")]));

    // Naively evaluating such an expression against a large number of
    // rows would involve re-converting "2020-09-08T12:00:00+00:00" to a
    // timestamp for each row which gets expensive
    //
    // However, DataFusion's simplification logic can do this for you

    // you need to tell DataFusion the type of column "ts":
    let schema = Schema::new(vec![make_ts_field("ts")]).to_dfschema_ref()?;

    // And then build a simplifier
    // the ExecutionProps carries information needed to simplify
    // expressions, such as the current time (to evaluate `now()`
    // correctly)
    let props = ExecutionProps::new();
    let context = SimplifyContext::new(&props).with_schema(schema);
    let simplifier = ExprSimplifier::new(context);

    // And then call the simplify_expr function:
    let expr = simplifier.simplify(expr)?;

    // DataFusion has simplified the expression to a comparison with a constant
    // ts = 1599566400000000000; Tada!
    assert_eq!(
        expr,
        col("ts").eq(lit_timestamp_nano(1599566400000000000i64))
    );

    // here are some other examples of what DataFusion is capable of
    let schema = Schema::new(vec![make_field("i", DataType::Int64)]).to_dfschema_ref()?;
    let context = SimplifyContext::new(&props).with_schema(schema.clone());
    let simplifier = ExprSimplifier::new(context);

    // basic arithmetic simplification
    // i + 1 + 2 => i + 3
    // (note this is not done if the expr is (col("i") + (lit(1) + lit(2))))
    assert_eq!(
        simplifier.simplify(col("i") + (lit(1) + lit(2)))?,
        col("i") + lit(3)
    );

    // (i * 0) > 5 --> false (only if null)
    assert_eq!(
        simplifier.simplify((col("i") * lit(0)).gt(lit(5)))?,
        lit(false)
    );

    // Logical simplification

    // ((i > 5) AND FALSE) OR (i < 10)   --> i < 10
    assert_eq!(
        simplifier
            .simplify(col("i").gt(lit(5)).and(lit(false)).or(col("i").lt(lit(10))))?,
        col("i").lt(lit(10))
    );

    // String --> Date simplification
    // `cast('2020-09-01' as date)` --> 18506 # number of days since epoch 1970-01-01
    assert_eq!(
        simplifier.simplify(lit("2020-09-01").cast_to(&DataType::Date32, &schema)?)?,
        lit(ScalarValue::Date32(Some(18506)))
    );

    Ok(())
}

/// DataFusion also has APIs for analyzing predicates (boolean expressions) to
/// determine any ranges restrictions on the inputs required for the predicate
/// evaluate to true.
fn range_analysis_demo() -> Result<()> {
    // For example, let's say you are interested in finding data for all days
    // in the month of September, 2020
    let september_1 = ScalarValue::Date32(Some(18506)); // 2020-09-01
    let october_1 = ScalarValue::Date32(Some(18536)); // 2020-10-01

    //  The predicate to find all such days could be
    // `date > '2020-09-01' AND date < '2020-10-01'`
    let expr = col("date")
        .gt(lit(september_1.clone()))
        .and(col("date").lt(lit(october_1.clone())));

    // Using the analysis API, DataFusion can determine that the value of `date`
    // must be in the range `['2020-09-01', '2020-10-01']`. If your data is
    // organized in files according to day, this information permits skipping
    // entire files without reading them.
    //
    // While this simple example could be handled with a special case, the
    // DataFusion API handles arbitrary expressions (so for example, you don't
    // have to handle the case where the predicate clauses are reversed such as
    // `date < '2020-10-01' AND date > '2020-09-01'`

    // As always, we need to tell DataFusion the type of column "date"
    let schema = Arc::new(Schema::new(vec![make_field("date", DataType::Date32)]));

    // You can provide DataFusion any known boundaries on the values of `date`
    // (for example, maybe you know you only have data up to `2020-09-15`), but
    // in this case, let's say we don't know any boundaries beforehand so we use
    // `try_new_unknown`
    let boundaries = ExprBoundaries::try_new_unbounded(&schema)?;

    // Now, we invoke the analysis code to perform the range analysis
    let df_schema = DFSchema::try_from(schema)?;
    let physical_expr = SessionContext::new().create_physical_expr(expr, &df_schema)?;
    let analysis_result = analyze(
        &physical_expr,
        AnalysisContext::new(boundaries),
        df_schema.as_ref(),
    )?;

    // The results of the analysis is an range, encoded as an `Interval`,  for
    // each column in the schema, that must be true in order for the predicate
    // to be true.
    //
    // In this case, we can see that, as expected, `analyze` has figured out
    // that in this case,  `date` must be in the range `['2020-09-01', '2020-10-01']`
    let expected_range = Interval::try_new(september_1, october_1)?;
    assert_eq!(analysis_result.boundaries[0].interval, Some(expected_range));

    Ok(())
}

fn make_field(name: &str, data_type: DataType) -> Field {
    let nullable = false;
    Field::new(name, data_type, nullable)
}

fn make_ts_field(name: &str) -> Field {
    let tz = None;
    make_field(name, DataType::Timestamp(TimeUnit::Nanosecond, tz))
}

/// This function shows how to use `Expr::get_type` to retrieve the DataType
/// of an expression
fn expression_type_demo() -> Result<()> {
    let expr = col("c");

    // To determine the DataType of an expression, DataFusion must know the
    // types of the input expressions. You can provide this information using
    // a schema. In this case we create a schema where the column `c` is of
    // type Utf8 (a String / VARCHAR)
    let schema = DFSchema::from_unqualified_fields(
        vec![Field::new("c", DataType::Utf8, true)].into(),
        HashMap::new(),
    )?;
    assert_eq!("Utf8", format!("{}", expr.get_type(&schema).unwrap()));

    // Using a schema where the column `foo` is of type Int32
    let schema = DFSchema::from_unqualified_fields(
        vec![Field::new("c", DataType::Int32, true)].into(),
        HashMap::new(),
    )?;
    assert_eq!("Int32", format!("{}", expr.get_type(&schema).unwrap()));

    // Get the type of an expression that adds 2 columns. Adding an Int32
    // and Float32 results in Float32 type
    let expr = col("c1") + col("c2");
    let schema = DFSchema::from_unqualified_fields(
        vec![
            Field::new("c1", DataType::Int32, true),
            Field::new("c2", DataType::Float32, true),
        ]
        .into(),
        HashMap::new(),
    )?;
    assert_eq!("Float32", format!("{}", expr.get_type(&schema).unwrap()));

    Ok(())
}

/// This function demonstrates how to apply type coercion to expressions, such as binary expressions.
///
/// In most cases, manual type coercion is not required since DataFusion handles it implicitly.
/// However, certain projects may construct `ExecutionPlan`s directly from DataFusion logical expressions,
/// bypassing the construction of DataFusion logical plans.
/// Since constructing `ExecutionPlan`s from logical expressions does not automatically apply type coercion,
/// you may need to handle type coercion manually in these cases.
///
/// The codes in this function shows various ways to perform type coercion on expressions:
/// 1. Using `SessionContext::create_physical_expr`
/// 2. Using `ExprSimplifier::coerce`
/// 3. Using `TreeNodeRewriter::rewrite` based on `TypeCoercionRewriter`
/// 4. Using `TreeNode::transform`
///
/// Note, this list may not be complete and there may be other methods to apply type coercion to expressions.
fn type_coercion_demo() -> Result<()> {
    // Creates a record batch for demo.
    let df_schema = DFSchema::from_unqualified_fields(
        vec![Field::new("a", DataType::Int8, false)].into(),
        HashMap::new(),
    )?;
    let i8_array = Int8Array::from_iter_values(vec![0, 1, 2]);
    let batch = RecordBatch::try_new(
        Arc::new(df_schema.as_arrow().to_owned()),
        vec![Arc::new(i8_array) as _],
    )?;

    // Constructs a binary expression for demo.
    // By default, the literal `1` is translated into the Int32 type and cannot be directly compared with the Int8 type.
    let expr = col("a").gt(lit(1));

    // Evaluation with an expression that has not been type coerced cannot succeed.
    let props = ExecutionProps::default();
    let physical_expr =
        datafusion_physical_expr::create_physical_expr(&expr, &df_schema, &props)?;
    let e = physical_expr.evaluate(&batch).unwrap_err();
    assert!(e
        .find_root()
        .to_string()
        .contains("Invalid comparison operation: Int8 > Int32"));

    // 1. Type coercion with `SessionContext::create_physical_expr` which implicitly applies type coercion before constructing the physical expr.
    let physical_expr =
        SessionContext::new().create_physical_expr(expr.clone(), &df_schema)?;
    assert!(physical_expr.evaluate(&batch).is_ok());

    // 2. Type coercion with `ExprSimplifier::coerce`.
    let context = SimplifyContext::new(&props).with_schema(Arc::new(df_schema.clone()));
    let simplifier = ExprSimplifier::new(context);
    let coerced_expr = simplifier.coerce(expr.clone(), &df_schema)?;
    let physical_expr = datafusion_physical_expr::create_physical_expr(
        &coerced_expr,
        &df_schema,
        &props,
    )?;
    assert!(physical_expr.evaluate(&batch).is_ok());

    // 3. Type coercion with `TypeCoercionRewriter`.
    let coerced_expr = expr
        .clone()
        .rewrite(&mut TypeCoercionRewriter::new(&df_schema))?
        .data;
    let physical_expr = datafusion_physical_expr::create_physical_expr(
        &coerced_expr,
        &df_schema,
        &props,
    )?;
    assert!(physical_expr.evaluate(&batch).is_ok());

    // 4. Apply explicit type coercion by manually rewriting the expression
    let coerced_expr = expr
        .transform(|e| {
            // Only type coerces binary expressions.
            let Expr::BinaryExpr(e) = e else {
                return Ok(Transformed::no(e));
            };
            if let Expr::Column(ref col_expr) = *e.left {
                let field = df_schema.field_with_name(None, col_expr.name())?;
                let cast_to_type = field.data_type();
                let coerced_right = e.right.cast_to(cast_to_type, &df_schema)?;
                Ok(Transformed::yes(Expr::BinaryExpr(BinaryExpr::new(
                    e.left,
                    e.op,
                    Box::new(coerced_right),
                ))))
            } else {
                Ok(Transformed::no(Expr::BinaryExpr(e)))
            }
        })?
        .data;
    let physical_expr = datafusion_physical_expr::create_physical_expr(
        &coerced_expr,
        &df_schema,
        &props,
    )?;
    assert!(physical_expr.evaluate(&batch).is_ok());

    Ok(())
}
