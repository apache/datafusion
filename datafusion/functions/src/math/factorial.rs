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

use crate::utils::make_scalar_function;
use datafusion_common::{Result, exec_err};
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
        make_scalar_function(factorial, vec![])(&args.args)
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

/// Factorial SQL function
fn factorial(args: &[ArrayRef]) -> Result<ArrayRef> {
    match args[0].data_type() {
        Int64 => {
            let result: Int64Array =
                args[0].as_primitive::<Int64Type>().try_unary(|a| {
                    if a < 0 {
                        Ok(1)
                    } else if a < FACTORIALS.len() as i64 {
                        Ok(FACTORIALS[a as usize])
                    } else {
                        exec_err!("Overflow happened on FACTORIAL({a})")
                    }
                })?;
            Ok(Arc::new(result) as ArrayRef)
        }
        other => exec_err!("Unsupported data type {other:?} for function factorial."),
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use datafusion_common::cast::as_int64_array;

    #[test]
    fn test_factorial_i64() {
        let args: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from(vec![0, 1, 2, 4, 20, -1])), // input
        ];

        let result = factorial(&args).expect("failed to initialize function factorial");
        let ints =
            as_int64_array(&result).expect("failed to initialize function factorial");

        let expected = Int64Array::from(vec![1, 1, 2, 24, 2432902008176640000, 1]);

        assert_eq!(ints, &expected);
    }

    #[test]
    fn test_overflow() {
        let args: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from(vec![21])), // input
        ];

        let result = factorial(&args);
        assert!(result.is_err());
    }
}
