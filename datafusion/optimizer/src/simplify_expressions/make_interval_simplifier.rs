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

//! This module implements a rule that simplifies the values for `InList`s

use arrow::datatypes::IntervalDayTime;
use datafusion_common::tree_node::{Transformed, TreeNodeRewriter};
use datafusion_common::Result;
use datafusion_common::ScalarValue;
use datafusion_expr::BinaryExpr;
use datafusion_expr::Expr;
use datafusion_expr::Operator::{Minus, Plus};

pub(super) struct MakeIntervalSimplifier {}

impl MakeIntervalSimplifier {
    pub(super) fn new() -> Self {
        Self {}
    }
}

impl TreeNodeRewriter for MakeIntervalSimplifier {
    type Node = Expr;

    fn f_up(&mut self, expr: Expr) -> Result<Transformed<Expr>> {
        if let Expr::BinaryExpr(BinaryExpr { left, op, right }) = &expr {
            if matches!(
                &**left,
                Expr::Literal(ScalarValue::Date32(_), _)
                    | Expr::Literal(ScalarValue::Date64(_), _)
            ) && matches!(op, Plus | Minus)
                && (matches!(&**right, Expr::Literal(ScalarValue::Int32(_), _))
                    || matches!(&**right, Expr::Literal(ScalarValue::Int64(_), _)))
            {
                let new_right: Expr = match &**right {
                    Expr::Literal(ScalarValue::Int32(Some(i)), meta) => Expr::Literal(
                        ScalarValue::IntervalDayTime(Some(IntervalDayTime {
                            days: *i,
                            milliseconds: 0,
                        })),
                        meta.clone(),
                    ),
                    Expr::Literal(ScalarValue::Int64(Some(i)), meta) => Expr::Literal(
                        ScalarValue::IntervalDayTime(Some(IntervalDayTime {
                            days: *i as i32,
                            milliseconds: 0,
                        })),
                        meta.clone(),
                    ),
                    _ => unreachable!(),
                };

                return Ok(Transformed::yes(Expr::BinaryExpr(BinaryExpr {
                    left: Box::new(*left.clone()),
                    right: Box::new(new_right),
                    op: *op,
                })));
            }
        }

        Ok(Transformed::no(expr))
    }
}
