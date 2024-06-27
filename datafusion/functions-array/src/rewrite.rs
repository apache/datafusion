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

//! Rewrites for using Array Functions

use crate::array_has::array_has_all;
use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::Transformed;
use datafusion_common::DFSchema;
use datafusion_common::Result;
use datafusion_expr::expr::ScalarFunction;
use datafusion_expr::expr_rewriter::FunctionRewrite;
use datafusion_expr::{BinaryExpr, Expr, Operator};

/// Rewrites expressions into function calls to array functions
pub(crate) struct ArrayFunctionRewriter {}

impl FunctionRewrite for ArrayFunctionRewriter {
    fn name(&self) -> &str {
        "ArrayFunctionRewriter"
    }

    fn rewrite(
        &self,
        expr: Expr,
        _schema: &DFSchema,
        _config: &ConfigOptions,
    ) -> Result<Transformed<Expr>> {
        let transformed = match expr {
            // array1 @> array2 -> array_has_all(array1, array2)
            Expr::BinaryExpr(BinaryExpr { left, op, right })
                if op == Operator::AtArrow
                    && is_func(&left, "make_array")
                    && is_func(&right, "make_array") =>
            {
                Transformed::yes(array_has_all(*left, *right))
            }

            // array1 <@ array2 -> array_has_all(array2, array1)
            Expr::BinaryExpr(BinaryExpr { left, op, right })
                if op == Operator::ArrowAt
                    && is_func(&left, "make_array")
                    && is_func(&right, "make_array") =>
            {
                Transformed::yes(array_has_all(*right, *left))
            }

            _ => Transformed::no(expr),
        };
        Ok(transformed)
    }
}

/// Returns true if expr is a function call to the specified named function.
/// Returns false otherwise.
fn is_func(expr: &Expr, func_name: &str) -> bool {
    let Expr::ScalarFunction(ScalarFunction { func, args: _ }) = expr else {
        return false;
    };

    func.name() == func_name
}
