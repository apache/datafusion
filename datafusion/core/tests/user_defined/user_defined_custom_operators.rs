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

use std::sync::Arc;
use arrow_array::RecordBatch;

use datafusion::arrow::datatypes::DataType;
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::Transformed;
use datafusion::common::{DFSchema, assert_batches_eq};
use datafusion::error::Result;
use datafusion::execution::FunctionRegistry;
use datafusion::logical_expr::expr_rewriter::FunctionRewrite;
use datafusion::logical_expr::{
    CustomOperator, Operator, ParseCustomOperator, WrapCustomOperator,
};
use datafusion::prelude::*;
use datafusion::sql::sqlparser::ast::BinaryOperator;

#[derive(Debug)]
enum MyCustomOperator {
    Arrow,
    LongArrow,
}

impl std::fmt::Display for MyCustomOperator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MyCustomOperator::Arrow => write!(f, "->"),
            MyCustomOperator::LongArrow => write!(f, "->>"),
        }
    }
}

impl CustomOperator for MyCustomOperator {
    fn binary_signature(
        &self,
        lhs: &DataType,
        rhs: &DataType,
    ) -> Result<(DataType, DataType, DataType)> {
        Ok((lhs.clone(), rhs.clone(), lhs.clone()))
    }

    fn op_to_sql(&self) -> Result<BinaryOperator> {
        match self {
            MyCustomOperator::Arrow => Ok(BinaryOperator::Arrow),
            MyCustomOperator::LongArrow => Ok(BinaryOperator::LongArrow),
        }
    }

    fn name(&self) -> &'static str {
        match self {
            MyCustomOperator::Arrow => "Arrow",
            MyCustomOperator::LongArrow => "LongArrow",
        }
    }
}

impl TryFrom<&str> for MyCustomOperator {
    type Error = ();

    fn try_from(value: &str) -> std::result::Result<Self, Self::Error> {
        match value {
            "Arrow" => Ok(MyCustomOperator::Arrow),
            "LongArrow" => Ok(MyCustomOperator::LongArrow),
            _ => Err(()),
        }
    }
}

#[derive(Debug)]
struct CustomOperatorParser;

impl ParseCustomOperator for CustomOperatorParser {
    fn name(&self) -> &str {
        "CustomOperatorParser"
    }

    fn op_from_ast(&self, op: &BinaryOperator) -> Result<Option<Operator>> {
        match op {
            BinaryOperator::Arrow => Ok(Some(MyCustomOperator::Arrow.into())),
            BinaryOperator::LongArrow => Ok(Some(MyCustomOperator::LongArrow.into())),
            _ => Ok(None),
        }
    }

    fn op_from_name(&self, raw_op: &str) -> Result<Option<Operator>> {
        if let Ok(op) = MyCustomOperator::try_from(raw_op) {
            Ok(Some(op.into()))
        } else {
            Ok(None)
        }
    }
}

impl FunctionRewrite for CustomOperatorParser {
    fn name(&self) -> &str {
        "CustomOperatorParser"
    }

    fn rewrite(
        &self,
        expr: Expr,
        _schema: &DFSchema,
        _config: &ConfigOptions,
    ) -> Result<Transformed<Expr>> {
        if let Expr::BinaryExpr(bin_expr) = &expr {
            if let Operator::Custom(WrapCustomOperator(op)) = &bin_expr.op {
                if let Ok(pg_op) = MyCustomOperator::try_from(op.name()) {
                    // return BinaryExpr with a different operator
                    let mut bin_expr = bin_expr.clone();
                    bin_expr.op = match pg_op {
                        MyCustomOperator::Arrow => Operator::StringConcat,
                        MyCustomOperator::LongArrow => Operator::Plus,
                    };
                    return Ok(Transformed::yes(Expr::BinaryExpr(bin_expr)));
                }
            }
        }
        Ok(Transformed::no(expr))
    }
}

async fn plan_and_collect(sql: &str) -> Result<Vec<RecordBatch>> {
    let mut ctx = SessionContext::new();
    ctx.register_function_rewrite(Arc::new(CustomOperatorParser))?;
    ctx.register_parse_custom_operator(Arc::new(CustomOperatorParser))?;
    ctx.sql(sql).await?.collect().await
}

#[tokio::test]
async fn test_custom_operators_arrow() {
    let actual = plan_and_collect("select 'foo'->'bar';").await.unwrap();
    let expected = [
        "+----------------------------+",
        "| Utf8(\"foo\") -> Utf8(\"bar\") |",
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
        "+-----------------------+",
        "| Int64(1) ->> Int64(2) |",
        "+-----------------------+",
        "| 3                     |",
        "+-----------------------+",
    ];
    assert_batches_eq!(&expected, &actual);
}
