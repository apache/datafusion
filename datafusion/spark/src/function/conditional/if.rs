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

use arrow::{
    array::{ArrayRef, BooleanArray},
    compute::kernels::zip::zip,
    datatypes::DataType,
};
use datafusion_common::{plan_err, utils::take_function_args, Result};
use datafusion_expr::{
    binary::comparison_coercion_numeric, ColumnarValue, ScalarFunctionArgs,
    ScalarUDFImpl, Signature, Volatility,
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
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

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

        let Some(target_type) = comparison_coercion_numeric(&arg_types[1], &arg_types[2])
        else {
            return plan_err!(
                "For function 'if' {} and {} is not comparable",
                arg_types[1],
                arg_types[2]
            );
        };
        // Convert null to String type.
        if target_type.is_null() {
            Ok(vec![
                DataType::Boolean,
                DataType::Utf8View,
                DataType::Utf8View,
            ])
        } else {
            Ok(vec![DataType::Boolean, target_type.clone(), target_type])
        }
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Ok(arg_types[1].clone())
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let args = ColumnarValue::values_to_arrays(&args.args)?;
        let [expr1, expr2, expr3] = take_function_args::<3, ArrayRef>("if", args)?;
        let expr1 = expr1.as_any().downcast_ref::<BooleanArray>().unwrap();
        let result = zip(expr1, &expr2, &expr3)?;
        Ok(ColumnarValue::Array(result))
    }
}
