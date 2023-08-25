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

use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use datafusion::error::Result;
use datafusion::optimizer::simplify_expressions::{ExprSimplifier, SimplifyContext};
use datafusion::physical_expr::execution_props::ExecutionProps;
use datafusion::prelude::*;
use datafusion_common::{ScalarValue, ToDFSchema};
use datafusion_expr::expr::BinaryExpr;
use datafusion_expr::Operator;

/// This example demonstrates the DataFusion [`Expr`] API.
///
/// DataFusion comes with a powerful and extensive system for
/// representing and manipulating expressions such as `A + 5` and `X
/// IN ('foo', 'bar', 'baz')` and many other constructs.
#[tokio::main]
async fn main() -> Result<()> {
    // The easiest way to do create expressions is to use the
    // "fluent"-style API, like this:
    let expr = col("a") + lit(5);

    // this creates the same expression as the following though with
    // much less code,
    let expr2 = Expr::BinaryExpr(BinaryExpr::new(
        Box::new(col("a")),
        Operator::Plus,
        Box::new(Expr::Literal(ScalarValue::Int32(Some(5)))),
    ));
    assert_eq!(expr, expr2);

    simplify_demo()?;

    Ok(())
}

/// In addition to easy construction, DataFusion exposes APIs for
/// working with and simplifying such expressions that call into the
/// same powerful and extensive implementation used for the query
/// engine.
fn simplify_demo() -> Result<()> {
    // For example, lets say you have has created an expression such
    // ts = to_timestamp("2020-09-08T12:00:00+00:00")
    let expr = col("ts").eq(call_fn(
        "to_timestamp",
        vec![lit("2020-09-08T12:00:00+00:00")],
    )?);

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
    let schema = Schema::new(vec![
        make_field("i", DataType::Int64),
        make_field("b", DataType::Boolean),
    ])
    .to_dfschema_ref()?;
    let context = SimplifyContext::new(&props).with_schema(schema);
    let simplifier = ExprSimplifier::new(context);

    // basic arithmetic simplification
    // i + 1 + 2 => a + 3
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
