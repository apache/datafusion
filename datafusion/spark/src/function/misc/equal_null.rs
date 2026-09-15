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

use arrow::datatypes::DataType;
use datafusion_common::utils::take_function_args;
use datafusion_common::{Result, plan_err};
use datafusion_expr::simplify::{ExprSimplifyResult, SimplifyContext};
use datafusion_expr::type_coercion::binary::comparison_coercion;
use datafusion_expr::{
    ColumnarValue, Expr, Operator, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    Volatility, binary_expr,
};
use datafusion_physical_expr_common::datum::apply_cmp;

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkEqualNull {
    signature: Signature,
}

impl Default for SparkEqualNull {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkEqualNull {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SparkEqualNull {
    fn name(&self) -> &str {
        "equal_null"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        let [lhs, rhs] = arg_types else {
            return plan_err!(
                "Function 'equal_null' expects 2 arguments but received {}",
                arg_types.len()
            );
        };
        // simplify() emits a comparison, and the type coercion pass has already run by then
        let Some(common) = comparison_coercion(lhs, rhs) else {
            return plan_err!(
                "For function 'equal_null' {lhs} and {rhs} are not comparable"
            );
        };
        Ok(vec![common.clone(), common])
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [lhs, rhs] = take_function_args(self.name(), args.args)?;
        apply_cmp(Operator::IsNotDistinctFrom, &lhs, &rhs)
    }

    fn simplify(
        &self,
        args: Vec<Expr>,
        _info: &SimplifyContext,
    ) -> Result<ExprSimplifyResult> {
        let [lhs, rhs] = take_function_args(self.name(), args)?;
        Ok(ExprSimplifyResult::Simplified(binary_expr(
            lhs,
            Operator::IsNotDistinctFrom,
            rhs,
        )))
    }
}
