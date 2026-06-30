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

use arrow::array::ArrayRef;
use arrow::datatypes::{
    DataType, Decimal32Type, Decimal64Type, Decimal128Type, Decimal256Type, Int64Type,
};

use crate::math::common::{lcm_signed, lcm_signed_int};
use crate::utils::{calculate_binary_decimal_math_cast, calculate_binary_math};
use datafusion_common::utils::take_function_args;
use datafusion_common::{Result, exec_err, plan_err};
use datafusion_expr::{
    ColumnarValue, Documentation, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    Volatility,
};
use datafusion_expr_common::type_coercion::binary::decimal_coercion;
use datafusion_macros::user_doc;

#[user_doc(
    doc_section(label = "Math Functions"),
    description = "Returns the least common multiple of `expression_x` and `expression_y`. Returns 0 if either input is zero.",
    syntax_example = "lcm(expression_x, expression_y)",
    sql_example = r#"```sql
> SELECT lcm(4, 5);
+----------+
| lcm(4,5) |
+----------+
| 20       |
+----------+
```"#,
    standard_argument(name = "expression_x", prefix = "First numeric"),
    standard_argument(name = "expression_y", prefix = "Second numeric")
)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct LcmFunc {
    signature: Signature,
}

impl Default for LcmFunc {
    fn default() -> Self {
        LcmFunc::new()
    }
}

impl LcmFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for LcmFunc {
    fn name(&self) -> &str {
        "lcm"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Ok(arg_types[0].clone())
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        let [arg1, arg2] = take_function_args(self.name(), arg_types)?;

        let coerced_type = match (arg1, arg2) {
            (DataType::Null, _) | (_, DataType::Null) => Ok(DataType::Int64),
            (lhs, rhs) if lhs.is_integer() && rhs.is_integer() => Ok(DataType::Int64),
            (lhs, rhs) if lhs.is_decimal() || rhs.is_decimal() => {
                decimal_coercion(lhs, rhs).map(Ok).unwrap_or_else(|| {
                    plan_err!(
                        "Unsupported argument types {lhs:?} and {rhs:?} for function {}",
                        self.name()
                    )
                })
            }
            (lhs, rhs) => {
                plan_err!(
                    "Unsupported argument types {lhs:?} and {rhs:?} for function {}",
                    self.name()
                )
            }
        }?;
        Ok(vec![coerced_type.clone(), coerced_type])
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let left = &args.args[0].to_array(args.number_rows)?;
        let right = &args.args[1];

        let arr: ArrayRef = match (left.data_type(), right.data_type()) {
            (DataType::Int64, _) => calculate_binary_math::<
                Int64Type,
                Int64Type,
                Int64Type,
                _,
            >(&left, right, lcm_signed_int)?,
            (
                lhs @ DataType::Decimal32(precision, scale),
                rhs @ DataType::Decimal32(_, _),
            ) if *lhs == rhs => {
                calculate_binary_decimal_math_cast::<
                    Decimal32Type,
                    Decimal32Type,
                    Decimal32Type,
                    _,
                >(&left, right, lcm_signed, *precision, *scale, lhs)?
            }
            (
                lhs @ DataType::Decimal64(precision, scale),
                rhs @ DataType::Decimal64(_, _),
            ) if *lhs == rhs => {
                calculate_binary_decimal_math_cast::<
                    Decimal64Type,
                    Decimal64Type,
                    Decimal64Type,
                    _,
                >(&left, right, lcm_signed, *precision, *scale, lhs)?
            }
            (
                lhs @ DataType::Decimal128(precision, scale),
                rhs @ DataType::Decimal128(_, _),
            ) if *lhs == rhs => {
                calculate_binary_decimal_math_cast::<
                    Decimal128Type,
                    Decimal128Type,
                    Decimal128Type,
                    _,
                >(&left, right, lcm_signed, *precision, *scale, lhs)?
            }
            (
                lhs @ DataType::Decimal256(precision, scale),
                rhs @ DataType::Decimal256(_, _),
            ) if *lhs == rhs => {
                calculate_binary_decimal_math_cast::<
                    Decimal256Type,
                    Decimal256Type,
                    Decimal256Type,
                    _,
                >(&left, right, lcm_signed, *precision, *scale, lhs)?
            }
            (lhs, rhs) => {
                return exec_err!(
                    "Unsupported data types {lhs:?} and {rhs:?} for function {}",
                    self.name()
                );
            }
        };
        Ok(ColumnarValue::Array(arr))
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}
