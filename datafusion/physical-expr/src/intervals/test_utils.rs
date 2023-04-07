// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Test utilities for the interval arithmetic library

use std::sync::Arc;

use crate::expressions::{BinaryExpr, Literal};
use crate::PhysicalExpr;
use datafusion_common::ScalarValue;
use datafusion_expr::Operator;

#[allow(clippy::too_many_arguments)]
/// This test function generates a conjunctive statement with two numeric
/// terms with the following form:
/// left_col (op_1) a  > right_col (op_2) b AND left_col (op_3) c < right_col (op_4) d
pub fn gen_conjunctive_numeric_expr_open_bounds(
    left_col: Arc<dyn PhysicalExpr>,
    right_col: Arc<dyn PhysicalExpr>,
    op_1: Operator,
    op_2: Operator,
    op_3: Operator,
    op_4: Operator,
    a: i32,
    b: i32,
    c: i32,
    d: i32,
) -> Arc<dyn PhysicalExpr> {
    let left_and_1 = Arc::new(BinaryExpr::new(
        left_col.clone(),
        op_1,
        Arc::new(Literal::new(ScalarValue::Int32(Some(a)))),
    ));
    let left_and_2 = Arc::new(BinaryExpr::new(
        right_col.clone(),
        op_2,
        Arc::new(Literal::new(ScalarValue::Int32(Some(b)))),
    ));

    let right_and_1 = Arc::new(BinaryExpr::new(
        left_col,
        op_3,
        Arc::new(Literal::new(ScalarValue::Int32(Some(c)))),
    ));
    let right_and_2 = Arc::new(BinaryExpr::new(
        right_col,
        op_4,
        Arc::new(Literal::new(ScalarValue::Int32(Some(d)))),
    ));
    let left_expr = Arc::new(BinaryExpr::new(left_and_1, Operator::Gt, left_and_2));
    let right_expr = Arc::new(BinaryExpr::new(right_and_1, Operator::Lt, right_and_2));
    Arc::new(BinaryExpr::new(left_expr, Operator::And, right_expr))
}

#[allow(clippy::too_many_arguments)]
/// This test function generates a conjunctive statement with two numeric
/// terms with the following form:
/// left_col (op_1) a  >= right_col (op_2) b AND left_col (op_3) c <= right_col (op_4) d
pub fn gen_conjunctive_numeric_expr_closed_bounds(
    left_col: Arc<dyn PhysicalExpr>,
    right_col: Arc<dyn PhysicalExpr>,
    op_1: Operator,
    op_2: Operator,
    op_3: Operator,
    op_4: Operator,
    a: i32,
    b: i32,
    c: i32,
    d: i32,
) -> Arc<dyn PhysicalExpr> {
    let left_and_1 = Arc::new(BinaryExpr::new(
        left_col.clone(),
        op_1,
        Arc::new(Literal::new(ScalarValue::Int32(Some(a)))),
    ));
    let left_and_2 = Arc::new(BinaryExpr::new(
        right_col.clone(),
        op_2,
        Arc::new(Literal::new(ScalarValue::Int32(Some(b)))),
    ));

    let right_and_1 = Arc::new(BinaryExpr::new(
        left_col,
        op_3,
        Arc::new(Literal::new(ScalarValue::Int32(Some(c)))),
    ));
    let right_and_2 = Arc::new(BinaryExpr::new(
        right_col,
        op_4,
        Arc::new(Literal::new(ScalarValue::Int32(Some(d)))),
    ));
    let left_expr = Arc::new(BinaryExpr::new(left_and_1, Operator::GtEq, left_and_2));
    let right_expr = Arc::new(BinaryExpr::new(right_and_1, Operator::LtEq, right_and_2));
    Arc::new(BinaryExpr::new(left_expr, Operator::And, right_expr))
}
