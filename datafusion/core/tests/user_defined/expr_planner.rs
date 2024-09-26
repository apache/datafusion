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

use arrow_array::RecordBatch;
use std::sync::Arc;

use datafusion::common::{assert_batches_eq, DFSchema};
use datafusion::error::Result;
use datafusion::execution::FunctionRegistry;
use datafusion::logical_expr::Operator;
use datafusion::prelude::*;
use datafusion::sql::sqlparser::ast::BinaryOperator;
use datafusion_common::ScalarValue;
use datafusion_expr::expr::Alias;
use datafusion_expr::planner::{ExprPlanner, PlannerResult, RawBinaryExpr};
use datafusion_expr::BinaryExpr;

#[derive(Debug)]
struct MyCustomPlanner;

impl ExprPlanner for MyCustomPlanner {
    fn plan_binary_op(
        &self,
        expr: RawBinaryExpr,
        _schema: &DFSchema,
    ) -> Result<PlannerResult<RawBinaryExpr>> {
        match &expr.op {
            BinaryOperator::Arrow => {
                Ok(PlannerResult::Planned(Expr::BinaryExpr(BinaryExpr {
                    left: Box::new(expr.left.clone()),
                    right: Box::new(expr.right.clone()),
                    op: Operator::StringConcat,
                })))
            }
            BinaryOperator::LongArrow => {
                Ok(PlannerResult::Planned(Expr::BinaryExpr(BinaryExpr {
                    left: Box::new(expr.left.clone()),
                    right: Box::new(expr.right.clone()),
                    op: Operator::Plus,
                })))
            }
            BinaryOperator::Question => {
                Ok(PlannerResult::Planned(Expr::Alias(Alias::new(
                    Expr::Literal(ScalarValue::Boolean(Some(true))),
                    None::<&str>,
                    format!("{} ? {}", expr.left, expr.right),
                ))))
            }
            _ => Ok(PlannerResult::Original(expr)),
        }
    }
}

async fn plan_and_collect(sql: &str) -> Result<Vec<RecordBatch>> {
    let config =
        SessionConfig::new().set_str("datafusion.sql_parser.dialect", "postgres");
    let mut ctx = SessionContext::new_with_config(config);
    ctx.register_expr_planner(Arc::new(MyCustomPlanner))?;
    ctx.sql(sql).await?.collect().await
}

#[tokio::test]
async fn test_custom_operators_arrow() {
    let actual = plan_and_collect("select 'foo'->'bar';").await.unwrap();
    let expected = [
        "+----------------------------+",
        "| Utf8(\"foo\") || Utf8(\"bar\") |",
        "+----------------------------+",
        "| foobar                     |",
        "+----------------------------+",
    ];
    assert_batches_eq!(&expected, &actual);
}

#[tokio::test]
async fn test_custom_operators_long_arrow() {
    let actual = plan_and_collect("select 1->>2;").await.unwrap();
    let expected = [
        "+---------------------+",
        "| Int64(1) + Int64(2) |",
        "+---------------------+",
        "| 3                   |",
        "+---------------------+",
    ];
    assert_batches_eq!(&expected, &actual);
}

#[tokio::test]
async fn test_question_select() {
    let actual = plan_and_collect("select a ? 2 from (select 1 as a);")
        .await
        .unwrap();
    let expected = [
        "+--------------+",
        "| a ? Int64(2) |",
        "+--------------+",
        "| true         |",
        "+--------------+",
    ];
    assert_batches_eq!(&expected, &actual);
}

#[tokio::test]
async fn test_question_filter() {
    let actual = plan_and_collect("select a from (select 1 as a) where a ? 2;")
        .await
        .unwrap();
    let expected = ["+---+", "| a |", "+---+", "| 1 |", "+---+"];
    assert_batches_eq!(&expected, &actual);
}
