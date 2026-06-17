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
use super::log::LogFunc;

use crate::utils::calculate_binary_math;
use arrow::array::{Array, ArrayRef};
use arrow::datatypes::{DataType, Float64Type};
use arrow::error::ArrowError;
use datafusion_common::types::{NativeType, logical_float64};
use datafusion_common::utils::take_function_args;
use datafusion_common::{Result, ScalarValue, internal_err};
use datafusion_expr::expr::ScalarFunction;
use datafusion_expr::simplify::{ExprSimplifyResult, SimplifyContext};
use datafusion_expr::{
    Cast, Coercion, ColumnarValue, Documentation, Expr, ScalarFunctionArgs, ScalarUDF,
    ScalarUDFImpl, Signature, TypeSignatureClass, Volatility, lit,
};
use datafusion_macros::user_doc;

/// Matches PostgreSQL: `power(0::float8, negative)` is undefined (IEEE 754 would yield infinity).
#[inline]
fn float64_power_checked(base: f64, exp: f64) -> Result<f64, ArrowError> {
    if base == 0.0 && exp < 0.0 {
        return Err(ArrowError::ComputeError(
            "zero raised to a negative power is undefined".to_string(),
        ));
    }
    Ok(base.powf(exp))
}

#[user_doc(
    doc_section(label = "Math Functions"),
    description = "Returns a base expression raised to the power of an exponent.",
    syntax_example = "power(base, exponent)",
    sql_example = r#"```sql
> SELECT power(2, 3);
+-------------+
| power(2,3)  |
+-------------+
| 8           |
+-------------+
```"#,
    standard_argument(name = "base", prefix = "Numeric"),
    standard_argument(name = "exponent", prefix = "Exponent numeric")
)]
#[derive(Debug, PartialEq, Eq, Hash)]
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
        let float = Coercion::new_implicit(
            TypeSignatureClass::Native(logical_float64()),
            vec![TypeSignatureClass::Numeric],
            NativeType::Float64,
        );
        Self {
            signature: Signature::coercible(vec![float; 2], Volatility::Immutable),
            aliases: vec![String::from("pow")],
        }
    }
}

impl ScalarUDFImpl for PowerFunc {
    fn name(&self) -> &str {
        "power"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        let [_base, _exponent] = take_function_args(self.name(), arg_types)?;
        Ok(DataType::Float64)
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [base, exponent] = take_function_args(self.name(), &args.args)?;
        let base = base.to_array(args.number_rows)?;

        let arr: ArrayRef = match (base.data_type(), exponent.data_type()) {
            (DataType::Float64, DataType::Float64) => {
                calculate_binary_math::<Float64Type, Float64Type, Float64Type, _>(
                    &base,
                    exponent,
                    float64_power_checked,
                )?
            }
            (base_type, exp_type) => {
                return internal_err!(
                    "Unsupported data types for base {base_type:?} and exponent {exp_type:?} for power"
                );
            }
        };
        Ok(ColumnarValue::Array(arr))
    }

    /// Simplify the `power` function by the relevant rules:
    /// 1. Power(a, 0) ===> 1
    /// 2. Power(a, 1) ===> a
    /// 3. Power(a, Log(a, b)) ===> b
    fn simplify(
        &self,
        args: Vec<Expr>,
        info: &SimplifyContext,
    ) -> Result<ExprSimplifyResult> {
        let [base, exponent] = take_function_args("power", args)?;
        let base_type = info.get_data_type(&base)?;
        let exponent_type = info.get_data_type(&exponent)?;
        let return_type =
            self.return_type(&[base_type.clone(), exponent_type.clone()])?;

        // Null propagation
        if base_type.is_null() || exponent_type.is_null() {
            return Ok(ExprSimplifyResult::Simplified(lit(
                ScalarValue::Null.cast_to(&return_type)?
            )));
        }

        // `simplify` runs on the logical expression *before* type coercion,
        // so a simplified sub-expression may still carry its original type
        // rather than the Float64 that `power` is declared to return. Cast it
        // back when needed to preserve the schema the optimizer already
        // committed to — e.g. `power(int_col, 1)` simplifies to `int_col`,
        // and the `b` in `power(b, log(b, uint_col))` simplifies to `uint_col`,
        // both of which must become Float64.
        let cast_to_return_type = |expr: Expr, expr_type: &DataType| {
            if expr_type == &return_type {
                expr
            } else {
                Expr::Cast(Cast::new(Box::new(expr), return_type.clone()))
            }
        };

        match exponent {
            Expr::Literal(value, _)
                if value == ScalarValue::new_zero(&exponent_type)? =>
            {
                Ok(ExprSimplifyResult::Simplified(lit(ScalarValue::new_one(
                    &return_type,
                )?)))
            }
            Expr::Literal(value, _) if value == ScalarValue::new_one(&exponent_type)? => {
                Ok(ExprSimplifyResult::Simplified(cast_to_return_type(
                    base, &base_type,
                )))
            }
            Expr::ScalarFunction(ScalarFunction { func, mut args })
                if is_log(&func) && args.len() == 2 && base == args[0] =>
            {
                let b = args.pop().unwrap(); // length checked above
                let b_type = info.get_data_type(&b)?;
                Ok(ExprSimplifyResult::Simplified(cast_to_return_type(
                    b, &b_type,
                )))
            }
            _ => Ok(ExprSimplifyResult::Original(vec![base, exponent])),
        }
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

/// Return true if this function call is a call to `Log`
fn is_log(func: &ScalarUDF) -> bool {
    func.inner().is::<LogFunc>()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_float64_power_checked_zero_negative_exp() {
        assert_eq!(float64_power_checked(0.0, 1.0).unwrap(), 0.0);
        assert_eq!(float64_power_checked(2.0, -1.0).unwrap(), 0.5);
        for base in [0.0f64, -0.0] {
            assert!(float64_power_checked(base, -1.0).is_err());
            assert!(float64_power_checked(base, -0.5).is_err());
        }
    }
}
