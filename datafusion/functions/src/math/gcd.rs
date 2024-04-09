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
use std::any::Any;
use std::mem::swap;
use std::sync::Arc;

use arrow::datatypes::DataType;
use arrow::datatypes::DataType::Int64;

use crate::utils::make_scalar_function;
use datafusion_common::{exec_err, DataFusionError, Result};
use datafusion_expr::ColumnarValue;
use datafusion_expr::{ScalarUDFImpl, Signature, Volatility};

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
}

/// Gcd SQL function
fn gcd(args: &[ArrayRef]) -> Result<ArrayRef> {
    match args[0].data_type() {
        Int64 => Ok(Arc::new(make_function_inputs2!(
            &args[0],
            &args[1],
            "x",
            "y",
            Int64Array,
            Int64Array,
            { compute_gcd }
        )) as ArrayRef),
        other => exec_err!("Unsupported data type {other:?} for function gcd"),
    }
}

/// Computes greatest common divisor using Binary GCD algorithm.
pub fn compute_gcd(x: i64, y: i64) -> i64 {
    let mut a = x.wrapping_abs();
    let mut b = y.wrapping_abs();

    if a == 0 {
        return b;
    }
    if b == 0 {
        return a;
    }

    let shift = (a | b).trailing_zeros();
    a >>= shift;
    b >>= shift;
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

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use arrow::array::{ArrayRef, Int64Array};

    use crate::math::gcd::gcd;
    use datafusion_common::cast::as_int64_array;

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
}
