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

//! This module implements a rule that do function simplification.

use datafusion_common::tree_node::TreeNodeRewriter;
use datafusion_common::Result;
use datafusion_expr::{expr::ScalarFunction, Expr, ScalarFunctionDefinition};
use datafusion_expr::Simplified;


#[derive(Default)]
pub(super) struct FunctionSimplifier {}

impl FunctionSimplifier {
    pub(super) fn new() -> Self {
        Self {}
    }
}

impl TreeNodeRewriter for FunctionSimplifier {
    type N = Expr;

    fn mutate(&mut self, expr: Expr) -> Result<Expr> {
        if let Expr::ScalarFunction(ScalarFunction {
            func_def: ScalarFunctionDefinition::UDF(udf),
            args,
        }) = &expr
        {
            let simplified_expr = udf.simplify(args)?;
            match simplified_expr {
                Simplified::Original => Ok(expr),
                Simplified::Rewritten(expr) => Ok(expr),
            }
        } else {
            Ok(expr)
        }
    }
}
