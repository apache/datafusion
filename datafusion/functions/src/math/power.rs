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

//! Math function: `power()`.

use arrow::datatypes::DataType;
use datafusion_common::{exec_err, DataFusionError, Result, ScalarValue};
use datafusion_expr::expr::ScalarFunction;
use datafusion_expr::simplify::{ExprSimplifyResult, SimplifyInfo};
use datafusion_expr::{ColumnarValue, Expr, ScalarFunctionDefinition};

use arrow::array::{ArrayRef, Float64Array, Int64Array};
use datafusion_expr::TypeSignature::*;
use datafusion_expr::{ScalarUDFImpl, Signature, Volatility};
use std::any::Any;
use std::sync::Arc;

use super::log::LogFunc;

#[derive(Debug)]
pub struct PowerFunc {
    signature: Signature,
    aliases: Vec<String>,
}

impl Default for PowerFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl PowerFunc {
    pub fn new() -> Self {
        use DataType::*;
        Self {
            signature: Signature::one_of(
                vec![Exact(vec![Int64, Int64]), Exact(vec![Float64, Float64])],
                Volatility::Immutable,
            ),
            aliases: vec![String::from("pow")],
        }
    }
}

impl ScalarUDFImpl for PowerFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "power"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        match arg_types[0] {
            DataType::Int64 => Ok(DataType::Int64),
            _ => Ok(DataType::Float64),
        }
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        let args = ColumnarValue::values_to_arrays(args)?;

        let arr: ArrayRef = match args[0].data_type() {
            DataType::Float64 => Arc::new(make_function_inputs2!(
                &args[0],
                &args[1],
                "base",
                "exponent",
                Float64Array,
                { f64::powf }
            )),

            DataType::Int64 => Arc::new(make_function_inputs2!(
                &args[0],
                &args[1],
                "base",
                "exponent",
                Int64Array,
                { i64::pow }
            )),

            other => {
                return exec_err!(
                    "Unsupported data type {other:?} for function {}",
                    self.name()
                )
            }
        };

        Ok(ColumnarValue::Array(arr))
    }

    /// Simplify the `power` function by the relevant rules:
    /// 1. Power(a, 0) ===> 0
    /// 2. Power(a, 1) ===> a
    /// 3. Power(a, Log(a, b)) ===> b
    fn simplify(
        &self,
        args: Vec<Expr>,
        info: &dyn SimplifyInfo,
    ) -> Result<ExprSimplifyResult> {
        let base = &args[0];
        let exponent = &args[1];

        match exponent {
            Expr::Literal(value)
                if value == &ScalarValue::new_zero(&info.get_data_type(exponent)?)? =>
            {
                Ok(ExprSimplifyResult::Simplified(Expr::Literal(
                    ScalarValue::new_one(&info.get_data_type(base)?)?,
                )))
            }
            Expr::Literal(value)
                if value == &ScalarValue::new_one(&info.get_data_type(exponent)?)? =>
            {
                Ok(ExprSimplifyResult::Simplified(base.clone()))
            }
            Expr::ScalarFunction(ScalarFunction {
                func_def: ScalarFunctionDefinition::UDF(fun),
                args,
            }) if base == &args[0]
                && fun
                    .as_ref()
                    .inner()
                    .as_any()
                    .downcast_ref::<LogFunc>()
                    .is_some() =>
            {
                Ok(ExprSimplifyResult::Simplified(args[1].clone()))
            }
            _ => Ok(ExprSimplifyResult::Original(args)),
        }
    }
}

#[cfg(test)]
mod tests {
    use datafusion_common::cast::{as_float64_array, as_int64_array};

    use super::*;

    #[test]
    fn test_power_f64() {
        let args = [
            ColumnarValue::Array(Arc::new(Float64Array::from(vec![2.0, 2.0, 3.0, 5.0]))), // base
            ColumnarValue::Array(Arc::new(Float64Array::from(vec![3.0, 2.0, 4.0, 4.0]))), // exponent
        ];

        let result = PowerFunc::new()
            .invoke(&args)
            .expect("failed to initialize function power");

        match result {
            ColumnarValue::Array(arr) => {
                let floats = as_float64_array(&arr)
                    .expect("failed to convert result to a Float64Array");
                assert_eq!(floats.len(), 4);
                assert_eq!(floats.value(0), 8.0);
                assert_eq!(floats.value(1), 4.0);
                assert_eq!(floats.value(2), 81.0);
                assert_eq!(floats.value(3), 625.0);
            }
            ColumnarValue::Scalar(_) => {
                panic!("Expected an array value")
            }
        }
    }

    #[test]
    fn test_power_i64() {
        let args = [
            ColumnarValue::Array(Arc::new(Int64Array::from(vec![2, 2, 3, 5]))), // base
            ColumnarValue::Array(Arc::new(Int64Array::from(vec![3, 2, 4, 4]))), // exponent
        ];

        let result = PowerFunc::new()
            .invoke(&args)
            .expect("failed to initialize function power");

        match result {
            ColumnarValue::Array(arr) => {
                let ints = as_int64_array(&arr)
                    .expect("failed to convert result to a Int64Array");

                assert_eq!(ints.len(), 4);
                assert_eq!(ints.value(0), 8);
                assert_eq!(ints.value(1), 4);
                assert_eq!(ints.value(2), 81);
                assert_eq!(ints.value(3), 625);
            }
            ColumnarValue::Scalar(_) => {
                panic!("Expected an array value")
            }
        }
    }
}
