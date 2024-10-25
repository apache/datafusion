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

use arrow::array::{ArrayRef, Int64Array};
use arrow::error::ArrowError;
use std::any::Any;
use std::mem::swap;
use std::sync::{Arc, OnceLock};

use arrow::datatypes::DataType;
use arrow::datatypes::DataType::Int64;

use crate::utils::make_scalar_function;
use datafusion_common::{arrow_datafusion_err, exec_err, DataFusionError, Result};
use datafusion_expr::scalar_doc_sections::DOC_SECTION_MATH;
use datafusion_expr::{
    ColumnarValue, Documentation, ScalarUDFImpl, Signature, Volatility,
};

#[derive(Debug)]
pub struct GcdFunc {
    signature: Signature,
}

impl Default for GcdFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl GcdFunc {
    pub fn new() -> Self {
        use DataType::*;
        Self {
            signature: Signature::uniform(2, vec![Int64], Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for GcdFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "gcd"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(Int64)
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        make_scalar_function(gcd, vec![])(args)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(get_gcd_doc())
    }
}

static DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

fn get_gcd_doc() -> &'static Documentation {
    DOCUMENTATION.get_or_init(|| {
        Documentation::builder()
            .with_doc_section(DOC_SECTION_MATH)
            .with_description(
                "Returns the greatest common divisor of `expression_x` and `expression_y`. Returns 0 if both inputs are zero.",
            )
            .with_syntax_example("gcd(expression_x, expression_y)")
            .with_standard_argument("expression_x", Some("First numeric"))
            .with_standard_argument("expression_y", Some("Second numeric"))
            .build()
            .unwrap()
    })
}

/// Gcd SQL function
fn gcd(args: &[ArrayRef]) -> Result<ArrayRef> {
    match args[0].data_type() {
        Int64 => {
            let arg1 = downcast_arg!(&args[0], "x", Int64Array);
            let arg2 = downcast_arg!(&args[1], "y", Int64Array);

            Ok(arg1
                .iter()
                .zip(arg2.iter())
                .map(|(a1, a2)| match (a1, a2) {
                    (Some(a1), Some(a2)) => Ok(Some(compute_gcd(a1, a2)?)),
                    _ => Ok(None),
                })
                .collect::<Result<Int64Array>>()
                .map(Arc::new)? as ArrayRef)
        }
        other => exec_err!("Unsupported data type {other:?} for function gcd"),
    }
}

/// Computes gcd of two unsigned integers using Binary GCD algorithm.
pub(super) fn unsigned_gcd(mut a: u64, mut b: u64) -> u64 {
    if a == 0 {
        return b;
    }
    if b == 0 {
        return a;
    }

    let shift = (a | b).trailing_zeros();
    a >>= a.trailing_zeros();
    loop {
        b >>= b.trailing_zeros();
        if a > b {
            swap(&mut a, &mut b);
        }
        b -= a;
        if b == 0 {
            return a << shift;
        }
    }
}

/// Computes greatest common divisor using Binary GCD algorithm.
pub fn compute_gcd(x: i64, y: i64) -> Result<i64> {
    let a = x.unsigned_abs();
    let b = y.unsigned_abs();
    let r = unsigned_gcd(a, b);
    // gcd(i64::MIN, i64::MIN) = i64::MIN.unsigned_abs() cannot fit into i64
    r.try_into().map_err(|_| {
        arrow_datafusion_err!(ArrowError::ComputeError(format!(
            "Signed integer overflow in GCD({x}, {y})"
        )))
    })
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use arrow::{
        array::{ArrayRef, Int64Array},
        error::ArrowError,
    };

    use crate::math::gcd::gcd;
    use datafusion_common::{cast::as_int64_array, DataFusionError};

    #[test]
    fn test_gcd_i64() {
        let args: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from(vec![0, 3, 25, -16])), // x
            Arc::new(Int64Array::from(vec![0, -2, 15, 8])),  // y
        ];

        let result = gcd(&args).expect("failed to initialize function gcd");
        let ints = as_int64_array(&result).expect("failed to initialize function gcd");

        assert_eq!(ints.len(), 4);
        assert_eq!(ints.value(0), 0);
        assert_eq!(ints.value(1), 1);
        assert_eq!(ints.value(2), 5);
        assert_eq!(ints.value(3), 8);
    }

    #[test]
    fn overflow_on_both_param_i64_min() {
        let args: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from(vec![i64::MIN])), // x
            Arc::new(Int64Array::from(vec![i64::MIN])), // y
        ];

        match gcd(&args) {
            // we expect a overflow
            Err(DataFusionError::ArrowError(ArrowError::ComputeError(_), _)) => {}
            Err(_) => {
                panic!("failed to initialize function gcd")
            }
            Ok(_) => panic!("GCD({0}, {0}) should have overflown", i64::MIN),
        };
    }
}
