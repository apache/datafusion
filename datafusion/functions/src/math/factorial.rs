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

use arrow::array::{ArrayRef, AsArray, Decimal256Array};
use std::sync::Arc;

use arrow::datatypes::DataType::{Decimal256, Int64};
use arrow::datatypes::{DECIMAL256_MAX_PRECISION, DataType, Int64Type};
use arrow_buffer::i256;

use datafusion_common::{
    DataFusionError, Result, ScalarValue, exec_err, internal_err,
    utils::take_function_args,
};
use datafusion_expr::{
    ColumnarValue, Documentation, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    Volatility,
};
use datafusion_macros::user_doc;

#[user_doc(
    doc_section(label = "Math Functions"),
    description = "Factorial of a non-negative integer. Errors if the argument is negative or the result overflows.",
    syntax_example = "factorial(numeric_expression)",
    sql_example = r#"```sql
> SELECT factorial(5);
+---------------+
| factorial(5)  |
+---------------+
| 120           |
+---------------+
```"#,
    standard_argument(name = "numeric_expression", prefix = "Numeric")
)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct FactorialFunc {
    signature: Signature,
}

impl Default for FactorialFunc {
    fn default() -> Self {
        FactorialFunc::new()
    }
}

impl FactorialFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::uniform(1, vec![Int64], Volatility::Immutable),
        }
    }
}

const FACTORIAL_RETURN_TYPE: DataType = Decimal256(DECIMAL256_MAX_PRECISION, 0);

impl ScalarUDFImpl for FactorialFunc {
    fn name(&self) -> &str {
        "factorial"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(FACTORIAL_RETURN_TYPE)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [arg] = take_function_args(self.name(), args.args)?;

        match arg {
            ColumnarValue::Scalar(scalar) => {
                if scalar.is_null() {
                    return Ok(ColumnarValue::Scalar(ScalarValue::Decimal256(
                        None,
                        DECIMAL256_MAX_PRECISION,
                        0,
                    )));
                }

                match scalar {
                    ScalarValue::Int64(Some(v)) => {
                        let result = compute_factorial(v)?;
                        Ok(ColumnarValue::Scalar(ScalarValue::Decimal256(
                            Some(result),
                            DECIMAL256_MAX_PRECISION,
                            0,
                        )))
                    }
                    _ => {
                        internal_err!(
                            "Unexpected data type {:?} for function factorial",
                            scalar.data_type()
                        )
                    }
                }
            }
            ColumnarValue::Array(array) => match array.data_type() {
                Int64 => {
                    let result = array
                        .as_primitive::<Int64Type>()
                        .iter()
                        .map(|value| value.map(compute_factorial).transpose())
                        .collect::<Result<Vec<_>>>()?;
                    let result = Decimal256Array::from(result)
                        .with_precision_and_scale(DECIMAL256_MAX_PRECISION, 0)?;
                    Ok(ColumnarValue::Array(Arc::new(result) as ArrayRef))
                }
                other => {
                    internal_err!("Unexpected data type {other:?} for function factorial")
                }
            },
        }
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

const FACTORIALS: [i64; 21] = [
    1,
    1,
    2,
    6,
    24,
    120,
    720,
    5040,
    40320,
    362880,
    3628800,
    39916800,
    479001600,
    6227020800,
    87178291200,
    1307674368000,
    20922789888000,
    355687428096000,
    6402373705728000,
    121645100408832000,
    2432902008176640000,
];

fn compute_factorial(n: i64) -> Result<i256> {
    if n < 0 {
        return exec_err!("factorial of a negative number is undefined");
    }

    if n < FACTORIALS.len() as i64 {
        return Ok(i256::from(FACTORIALS[n as usize]));
    }

    let mut result = i256::from(FACTORIALS[FACTORIALS.len() - 1]);
    for value in FACTORIALS.len() as i64..=n {
        result = result.checked_mul(i256::from(value)).ok_or_else(|| {
            DataFusionError::Execution(format!("Overflow happened on FACTORIAL({n})"))
        })?;
    }

    if result.to_string().len() > DECIMAL256_MAX_PRECISION as usize {
        return exec_err!("Overflow happened on FACTORIAL({n})");
    }

    Ok(result)
}
