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

use arrow::array::{ArrayRef, AsArray, Int64Array};
use std::any::Any;
use std::sync::Arc;

use arrow::datatypes::DataType::Int64;
use arrow::datatypes::{DataType, Int64Type};

use datafusion_common::{
    Result, ScalarValue, exec_err, internal_err, utils::take_function_args,
};
use datafusion_expr::{
    ColumnarValue, Documentation, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    Volatility,
};
use datafusion_macros::user_doc;

#[user_doc(
    doc_section(label = "Math Functions"),
    description = "Factorial. Returns 1 if value is less than 2.",
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

impl ScalarUDFImpl for FactorialFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "factorial"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(Int64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [arg] = take_function_args(self.name(), args.args)?;

        match arg {
            ColumnarValue::Scalar(scalar) => {
                if scalar.is_null() {
                    return Ok(ColumnarValue::Scalar(ScalarValue::Int64(None)));
                }

                match scalar {
                    ScalarValue::Int64(Some(v)) => {
                        let result = compute_factorial(v)?;
                        Ok(ColumnarValue::Scalar(ScalarValue::Int64(Some(result))))
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
                    let result: Int64Array = array
                        .as_primitive::<Int64Type>()
                        .try_unary(compute_factorial)?;
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
]; // if return type changes, this constant needs to be updated accordingly

fn compute_factorial(n: i64) -> Result<i64> {
    if n < 0 {
        Ok(1)
    } else if n < FACTORIALS.len() as i64 {
        Ok(FACTORIALS[n as usize])
    } else {
        exec_err!("Overflow happened on FACTORIAL({n})")
    }
}
