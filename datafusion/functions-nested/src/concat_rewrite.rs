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

//! [`ConcatArrayRewrite`] rewrites `concat(array, ...)` to `array_concat(array, ...)`.

use std::any::Any;

use arrow::datatypes::DataType;
use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::Transformed;
use datafusion_common::{DFSchema, Result, plan_err};
use datafusion_expr::Expr;
use datafusion_expr::ExprSchemable;
use datafusion_expr::expr::ScalarFunction;
use datafusion_expr::expr_rewriter::FunctionRewrite;
use datafusion_functions::string::concat::ConcatFunc;

use crate::concat::array_concat_udf;

/// [`FunctionRewrite`] that turns `concat(array, ...)` into
/// `array_concat(array, ...)` at the analyzer phase.
///
/// The string `concat` UDF only handles string and binary inputs. When it
/// receives array arguments it falls through to a debug-only code path and
/// produces surprising output. Rewriting at the analyzer phase means every
/// logical plan gets the corrected behavior.
///
/// `concat` calls with only non-array arguments are left unchanged.
/// Mixed array and non-array arguments are rejected with a plan error.
#[derive(Debug, Default)]
pub struct ConcatArrayRewrite;

impl FunctionRewrite for ConcatArrayRewrite {
    fn name(&self) -> &str {
        "concat_array_rewrite"
    }

    fn rewrite(
        &self,
        expr: Expr,
        schema: &DFSchema,
        _config: &ConfigOptions,
    ) -> Result<Transformed<Expr>> {
        let Expr::ScalarFunction(ScalarFunction { func, args }) = &expr else {
            return Ok(Transformed::no(expr));
        };
        if !(func.inner().as_ref() as &dyn Any).is::<ConcatFunc>() {
            return Ok(Transformed::no(expr));
        }

        let mut any_list = false;
        let mut any_non_list = false;
        for arg in args {
            match arg.get_type(schema)? {
                DataType::List(_)
                | DataType::LargeList(_)
                | DataType::FixedSizeList(_, _) => any_list = true,
                DataType::Null => {}
                _ => any_non_list = true,
            }
        }

        if !any_list {
            return Ok(Transformed::no(expr));
        }
        if any_non_list {
            return plan_err!(
                "Cannot mix array and non-array arguments in concat function"
            );
        }

        let Expr::ScalarFunction(ScalarFunction { args, .. }) = expr else {
            unreachable!("already matched above")
        };
        Ok(Transformed::yes(Expr::ScalarFunction(
            ScalarFunction::new_udf(array_concat_udf(), args),
        )))
    }
}
