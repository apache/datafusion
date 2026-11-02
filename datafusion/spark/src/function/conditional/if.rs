// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use arrow::datatypes::DataType;
use datafusion_common::{Result, internal_err, plan_err};
use datafusion_expr::{
    ColumnarValue, Expr, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
    binary::try_type_union_resolution, simplify::ExprSimplifyResult, when,
};

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkIf {
    signature: Signature,
}

impl Default for SparkIf {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkIf {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SparkIf {
    fn name(&self) -> &str {
        "if"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.len() != 3 {
            return plan_err!(
                "Function 'if' expects 3 arguments but received {}",
                arg_types.len()
            );
        }

        if arg_types[0] != DataType::Boolean && arg_types[0] != DataType::Null {
            return plan_err!(
                "For function 'if' {} is not a boolean or null",
                arg_types[0]
            );
        }

        let target_types = try_type_union_resolution(&arg_types[1..])?;
        let mut result = vec![DataType::Boolean];
        result.extend(target_types);
        Ok(result)
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Ok(arg_types[1].clone())
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        internal_err!("if should have been simplified to case")
    }

    fn simplify(
        &self,
        args: Vec<Expr>,
        _info: &datafusion_expr::simplify::SimplifyContext,
    ) -> Result<ExprSimplifyResult> {
        let condition = args[0].clone();
        let then_expr = args[1].clone();
        let else_expr = args[2].clone();

        // Convert IF(condition, then_expr, else_expr) to
        // CASE WHEN condition THEN then_expr ELSE else_expr END
        let case_expr = when(condition, then_expr).otherwise(else_expr)?;

        Ok(ExprSimplifyResult::Simplified(case_expr))
    }
}
