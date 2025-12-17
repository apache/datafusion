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

//! Simplify NOT expressions in physical expressions
//!
//! This module provides optimizations for NOT expressions such as:
//! - Double negation elimination: NOT(NOT(expr)) -> expr
//! - NOT with binary comparisons: NOT(a = b) -> a != b
//! - NOT with IN expressions: NOT(a IN (list)) -> a NOT IN (list)
//! - De Morgan's laws: NOT(A AND B) -> NOT A OR NOT B
//! - Constant folding: NOT(TRUE) -> FALSE, NOT(FALSE) -> TRUE
//!
//! This function is designed to work with TreeNodeRewriter's f_up traversal,
//! which means children are already simplified when this function is called.
//! The TreeNodeRewriter will automatically call this function repeatedly until
//! no more transformations are possible.

use std::sync::Arc;

use arrow::datatypes::Schema;
use datafusion_common::{Result, ScalarValue, tree_node::Transformed};
use datafusion_expr::Operator;

use crate::PhysicalExpr;
use crate::expressions::{BinaryExpr, InListExpr, Literal, NotExpr, in_list, lit};

/// Attempts to simplify NOT expressions by applying one level of transformation
///
/// This function applies a single simplification rule and returns. When used with
/// TreeNodeRewriter, multiple passes will automatically be applied until no more
/// transformations are possible.
pub fn simplify_not_expr(
    expr: &Arc<dyn PhysicalExpr>,
    schema: &Schema,
) -> Result<Transformed<Arc<dyn PhysicalExpr>>> {
    // Check if this is a NOT expression
    let not_expr = match expr.as_any().downcast_ref::<NotExpr>() {
        Some(not_expr) => not_expr,
        None => return Ok(Transformed::no(Arc::clone(expr))),
    };

    let inner_expr = not_expr.arg();

    // Handle NOT(NOT(expr)) -> expr (double negation elimination)
    if let Some(inner_not) = inner_expr.as_any().downcast_ref::<NotExpr>() {
        return Ok(Transformed::yes(Arc::clone(inner_not.arg())));
    }

    // Handle NOT(literal) -> !literal
    if let Some(literal) = inner_expr.as_any().downcast_ref::<Literal>() {
        if let ScalarValue::Boolean(Some(val)) = literal.value() {
            return Ok(Transformed::yes(lit(ScalarValue::Boolean(Some(!val)))));
        }
        if let ScalarValue::Boolean(None) = literal.value() {
            return Ok(Transformed::yes(lit(ScalarValue::Boolean(None))));
        }
    }

    // Handle NOT(IN list) -> NOT IN list
    if let Some(in_list_expr) = inner_expr.as_any().downcast_ref::<InListExpr>() {
        let negated = !in_list_expr.negated();
        let new_in_list = in_list(
            Arc::clone(in_list_expr.expr()),
            in_list_expr.list().to_vec(),
            &negated,
            schema,
        )?;
        return Ok(Transformed::yes(new_in_list));
    }

    // Handle NOT(binary_expr)
    if let Some(binary_expr) = inner_expr.as_any().downcast_ref::<BinaryExpr>() {
        if let Some(negated_op) = binary_expr.op().negate() {
            let new_binary = Arc::new(BinaryExpr::new(
                Arc::clone(binary_expr.left()),
                negated_op,
                Arc::clone(binary_expr.right()),
            ));
            return Ok(Transformed::yes(new_binary));
        }

        // Handle De Morgan's laws for AND/OR
        match binary_expr.op() {
            Operator::And => {
                // NOT(A AND B) -> NOT A OR NOT B
                let not_left: Arc<dyn PhysicalExpr> =
                    Arc::new(NotExpr::new(Arc::clone(binary_expr.left())));
                let not_right: Arc<dyn PhysicalExpr> =
                    Arc::new(NotExpr::new(Arc::clone(binary_expr.right())));
                let new_binary =
                    Arc::new(BinaryExpr::new(not_left, Operator::Or, not_right));
                return Ok(Transformed::yes(new_binary));
            }
            Operator::Or => {
                // NOT(A OR B) -> NOT A AND NOT B
                let not_left: Arc<dyn PhysicalExpr> =
                    Arc::new(NotExpr::new(Arc::clone(binary_expr.left())));
                let not_right: Arc<dyn PhysicalExpr> =
                    Arc::new(NotExpr::new(Arc::clone(binary_expr.right())));
                let new_binary =
                    Arc::new(BinaryExpr::new(not_left, Operator::And, not_right));
                return Ok(Transformed::yes(new_binary));
            }
            _ => {}
        }
    }

    // If no simplification possible, return the original expression
    Ok(Transformed::no(Arc::clone(expr)))
}
