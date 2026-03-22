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

//! Constant expression evaluation for the physical expression simplifier

use std::sync::Arc;

use arrow::array::new_null_array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion_common::tree_node::{Transformed, TreeNode, TreeNodeRecursion};
use datafusion_common::{Result, ScalarValue};
use datafusion_expr_common::columnar_value::ColumnarValue;

use crate::PhysicalExpr;
use crate::expressions::{Column, Literal};

/// Simplify expressions that consist only of literals by evaluating them.
///
/// This function checks if all children of the given expression are literals.
/// If so, it evaluates the expression against a dummy RecordBatch and returns
/// the result as a new Literal.
///
/// # Example transformations
/// - `1 + 2` -> `3`
/// - `(1 + 2) * 3` -> `9` (with bottom-up traversal)
/// - `'hello' || ' world'` -> `'hello world'`
#[deprecated(
    since = "53.0.0",
    note = "This function will be removed in a future release in favor of a private implementation that depends on other implementation details. Please open an issue if you have a use case for keeping it."
)]
pub fn simplify_const_expr(
    expr: Arc<dyn PhysicalExpr>,
) -> Result<Transformed<Arc<dyn PhysicalExpr>>> {
    let batch = create_dummy_batch()?;
    // If expr is already a const literal or can't be evaluated into one.
    if expr.as_any().is::<Literal>() || (!can_evaluate_as_constant(&expr)) {
        return Ok(Transformed::no(expr));
    }

    // Evaluate the expression
    match expr.evaluate(&batch) {
        Ok(ColumnarValue::Scalar(scalar)) => {
            Ok(Transformed::yes(Arc::new(Literal::new(scalar))))
        }
        Ok(ColumnarValue::Array(arr)) if arr.len() == 1 => {
            // Some operations return an array even for scalar inputs
            let scalar = ScalarValue::try_from_array(&arr, 0)?;
            Ok(Transformed::yes(Arc::new(Literal::new(scalar))))
        }
        Ok(_) => {
            // Unexpected result - keep original expression
            Ok(Transformed::no(expr))
        }
        Err(_) => {
            // On error, keep original expression
            // The expression might succeed at runtime due to short-circuit evaluation
            // or other runtime conditions
            Ok(Transformed::no(expr))
        }
    }
}

/// Simplify expressions whose immediate children are all literals.
///
/// This function only checks the direct children of the expression,
/// not the entire subtree. It is designed to be used with bottom-up tree
/// traversal, where children are simplified before parents.
///
/// # Example transformations
/// - `1 + 2` -> `3`
/// - `(1 + 2) * 3` -> `9` (with bottom-up traversal, inner expr simplified first)
/// - `'hello' || ' world'` -> `'hello world'`
pub(crate) fn simplify_const_expr_immediate(
    expr: Arc<dyn PhysicalExpr>,
    batch: &RecordBatch,
) -> Result<Transformed<Arc<dyn PhysicalExpr>>> {
    // Already a literal - nothing to do
    if expr.as_any().is::<Literal>() {
        return Ok(Transformed::no(expr));
    }

    // Column references cannot be evaluated at plan time
    if expr.as_any().is::<Column>() {
        return Ok(Transformed::no(expr));
    }

    // Volatile nodes cannot be evaluated at plan time
    if expr.is_volatile_node() {
        return Ok(Transformed::no(expr));
    }

    // Since transform visits bottom-up, children have already been simplified.
    // If all children are now Literals, this node can be const-evaluated.
    // This is O(k) where k = number of children, instead of O(subtree).
    let all_children_literal = expr
        .children()
        .iter()
        .all(|child| child.as_any().is::<Literal>());

    if !all_children_literal {
        return Ok(Transformed::no(expr));
    }

    // Evaluate the expression
    match expr.evaluate(batch) {
        Ok(ColumnarValue::Scalar(scalar)) => {
            Ok(Transformed::yes(Arc::new(Literal::new(scalar))))
        }
        Ok(ColumnarValue::Array(arr)) if arr.len() == 1 => {
            // Some operations return an array even for scalar inputs
            let scalar = ScalarValue::try_from_array(&arr, 0)?;
            Ok(Transformed::yes(Arc::new(Literal::new(scalar))))
        }
        Ok(_) => {
            // Unexpected result - keep original expression
            Ok(Transformed::no(expr))
        }
        Err(_) => {
            // On error, keep original expression
            // The expression might succeed at runtime due to short-circuit evaluation
            // or other runtime conditions
            Ok(Transformed::no(expr))
        }
    }
}

/// Create a 1-row dummy RecordBatch for evaluating constant expressions.
///
/// The batch is never actually accessed for data - it's just needed because
/// the PhysicalExpr::evaluate API requires a RecordBatch. For expressions
/// that only contain literals, the batch content is irrelevant.
///
/// This is the same approach used in the logical expression `ConstEvaluator`.
pub(crate) fn create_dummy_batch() -> Result<RecordBatch> {
    // RecordBatch requires at least one column
    let dummy_schema = Arc::new(Schema::new(vec![Field::new("_", DataType::Null, true)]));
    let col = new_null_array(&DataType::Null, 1);
    Ok(RecordBatch::try_new(dummy_schema, vec![col])?)
}

fn can_evaluate_as_constant(expr: &Arc<dyn PhysicalExpr>) -> bool {
    let mut can_evaluate = true;

    expr.apply(|e| {
        if e.as_any().is::<Column>() || e.is_volatile_node() {
            can_evaluate = false;
            Ok(TreeNodeRecursion::Stop)
        } else {
            Ok(TreeNodeRecursion::Continue)
        }
    })
    .expect("apply should not fail");

    can_evaluate
}

/// Check if this expression has any column references.
#[deprecated(
    since = "53.0.0",
    note = "This function isn't used internally and is trivial to implement, therefore it will be removed in a future release."
)]
pub fn has_column_references(expr: &Arc<dyn PhysicalExpr>) -> bool {
    let mut has_columns = false;
    expr.apply(|expr| {
        if expr.as_any().downcast_ref::<Column>().is_some() {
            has_columns = true;
            Ok(TreeNodeRecursion::Stop)
        } else {
            Ok(TreeNodeRecursion::Continue)
        }
    })
    .expect("apply should not fail");
    has_columns
}
