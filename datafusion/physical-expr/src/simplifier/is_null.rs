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
// software distributed under this License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Simplify IS NULL and IS NOT NULL expressions on literals
//!
//! - IS NULL(literal) -> true/false based on literal.value().is_null()
//! - IS NOT NULL(literal) -> true/false based on !literal.value().is_null()

use std::sync::Arc;

use datafusion_common::{Result, ScalarValue, tree_node::Transformed};

use crate::PhysicalExpr;
use crate::expressions::{IsNotNullExpr, IsNullExpr, Literal, lit};

/// Attempts to simplify IS NULL and IS NOT NULL expressions on literals
///
/// This function simplifies:
/// - IS NULL(literal) -> true if literal is NULL, false otherwise
/// - IS NOT NULL(literal) -> false if literal is NULL, true otherwise
///
/// This function applies a single simplification rule and returns. When used with
/// TreeNodeRewriter, multiple passes will automatically be applied until no more
/// transformations are possible.
pub fn simplify_is_null_expr(
    expr: &Arc<dyn PhysicalExpr>,
) -> Result<Transformed<Arc<dyn PhysicalExpr>>> {
    // Handle IS NULL(literal)
    if let Some(is_null_expr) = expr.as_any().downcast_ref::<IsNullExpr>() {
        if let Some(literal) = is_null_expr.arg().as_any().downcast_ref::<Literal>() {
            let result = literal.value().is_null();
            return Ok(Transformed::yes(lit(ScalarValue::Boolean(Some(result)))));
        }
    }

    // Handle IS NOT NULL(literal)
    if let Some(is_not_null_expr) = expr.as_any().downcast_ref::<IsNotNullExpr>() {
        if let Some(literal) = is_not_null_expr.arg().as_any().downcast_ref::<Literal>() {
            let result = !literal.value().is_null();
            return Ok(Transformed::yes(lit(ScalarValue::Boolean(Some(result)))));
        }
    }

    // If no simplification possible, return the original expression
    Ok(Transformed::no(Arc::clone(expr)))
}
