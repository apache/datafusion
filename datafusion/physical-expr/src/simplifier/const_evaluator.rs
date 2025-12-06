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
use datafusion_common::tree_node::Transformed;
use datafusion_common::{Result, ScalarValue};
use datafusion_expr_common::columnar_value::ColumnarValue;

use crate::expressions::Literal;
use crate::PhysicalExpr;

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
pub fn simplify_const_expr(
    expr: &Arc<dyn PhysicalExpr>,
) -> Result<Transformed<Arc<dyn PhysicalExpr>>> {
    // Skip if already a literal - nothing to simplify
    if expr.as_any().downcast_ref::<Literal>().is_some() {
        return Ok(Transformed::no(Arc::clone(expr)));
    }

    // Check if all children are literals
    let children = expr.children();
    if children.is_empty() {
        // No children means this is a leaf node (but not a Literal, handled above)
        // This could be a Column or other leaf expression - don't simplify
        return Ok(Transformed::no(Arc::clone(expr)));
    }

    let all_literals = children
        .iter()
        .all(|child| child.as_any().downcast_ref::<Literal>().is_some());

    if !all_literals {
        return Ok(Transformed::no(Arc::clone(expr)));
    }

    // All children are literals - we can evaluate this expression at plan time
    // Create a 1-row dummy batch for evaluation
    let batch = create_dummy_batch()?;

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
            Ok(Transformed::no(Arc::clone(expr)))
        }
        Err(_) => {
            // On error, keep original expression
            // The expression might succeed at runtime due to short-circuit evaluation
            // or other runtime conditions
            Ok(Transformed::no(Arc::clone(expr)))
        }
    }
}

/// Create a 1-row dummy RecordBatch for evaluating constant expressions.
///
/// The batch is never actually accessed for data - it's just needed because
/// the PhysicalExpr::evaluate API requires a RecordBatch. For expressions
/// that only contain literals, the batch content is irrelevant.
fn create_dummy_batch() -> Result<RecordBatch> {
    // RecordBatch requires at least one column
    let dummy_schema = Arc::new(Schema::new(vec![Field::new("_", DataType::Null, true)]));
    let col = new_null_array(&DataType::Null, 1);
    Ok(RecordBatch::try_new(dummy_schema, vec![col])?)
}
