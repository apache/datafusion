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

use std::any::Any;
use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array};
use arrow::datatypes::DataType;
use arrow::datatypes::DataType::Int64;

use arrow::error::ArrowError;
use datafusion_common::{arrow_datafusion_err, exec_err, Result};
use datafusion_expr::{
    ColumnarValue, Documentation, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    Volatility,
};
use datafusion_macros::user_doc;

use super::gcd::unsigned_gcd;
use crate::utils::make_scalar_function;

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
        use DataType::*;
        Self {
            signature: Signature::uniform(2, vec![Int64], Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for LcmFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "lcm"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(Int64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        make_scalar_function(lcm, vec![])(&args.args)
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

/// Lcm SQL function
fn lcm(args: &[ArrayRef]) -> Result<ArrayRef> {
    let compute_lcm = |x: i64, y: i64| {
        if x == 0 || y == 0 {
            return Ok(0);
        }

        // lcm(x, y) = |x| * |y| / gcd(|x|, |y|)
        let a = x.unsigned_abs();
        let b = y.unsigned_abs();
        let gcd = unsigned_gcd(a, b);
        // gcd is not zero since both a and b are not zero, so the division is safe.
        (a / gcd)
            .checked_mul(b)
            .and_then(|v| i64::try_from(v).ok())
            .ok_or_else(|| {
                arrow_datafusion_err!(ArrowError::ComputeError(format!(
                    "Signed integer overflow in LCM({x}, {y})"
                )))
            })
    };

    match args[0].data_type() {
        Int64 => {
            let arg1 = downcast_named_arg!(&args[0], "x", Int64Array);
            let arg2 = downcast_named_arg!(&args[1], "y", Int64Array);

            Ok(arg1
                .iter()
                .zip(arg2.iter())
                .map(|(a1, a2)| match (a1, a2) {
                    (Some(a1), Some(a2)) => Ok(Some(compute_lcm(a1, a2)?)),
                    _ => Ok(None),
                })
                .collect::<Result<Int64Array>>()
                .map(Arc::new)? as ArrayRef)
        }
        other => exec_err!("Unsupported data type {other:?} for function lcm"),
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use arrow::array::{ArrayRef, Int64Array};

    use datafusion_common::cast::as_int64_array;

    use crate::math::lcm::lcm;

    #[test]
    fn test_lcm_i64() {
        let args: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from(vec![0, 3, 25, -16])), // x
            Arc::new(Int64Array::from(vec![0, -2, 15, 8])),  // y
        ];

        let result = lcm(&args).expect("failed to initialize function lcm");
        let ints = as_int64_array(&result).expect("failed to initialize function lcm");

        assert_eq!(ints.len(), 4);
        assert_eq!(ints.value(0), 0);
        assert_eq!(ints.value(1), 6);
        assert_eq!(ints.value(2), 75);
        assert_eq!(ints.value(3), 16);
    }
}
