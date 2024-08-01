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

use arrow::array::{ArrayRef, BooleanArray, Float32Array, Float64Array};
use arrow::datatypes::DataType;
use arrow::datatypes::DataType::{Boolean, Float32, Float64};

use datafusion_common::{exec_err, DataFusionError, Result};
use datafusion_expr::ColumnarValue;
use datafusion_expr::TypeSignature::Exact;
use datafusion_expr::{ScalarUDFImpl, Signature, Volatility};

use crate::utils::make_scalar_function;

#[derive(Debug)]
pub struct IsZeroFunc {
    signature: Signature,
}

impl Default for IsZeroFunc {
    fn default() -> Self {
        IsZeroFunc::new()
    }
}

impl IsZeroFunc {
    pub fn new() -> Self {
        use DataType::*;
        Self {
            signature: Signature::one_of(
                vec![Exact(vec![Float32]), Exact(vec![Float64])],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for IsZeroFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "iszero"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(Boolean)
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        make_scalar_function(iszero, vec![])(args)
    }
}

/// Iszero SQL function
pub fn iszero(args: &[ArrayRef]) -> Result<ArrayRef> {
    match args[0].data_type() {
        Float64 => Ok(Arc::new(make_function_scalar_inputs_return_type!(
            &args[0],
            "x",
            Float64Array,
            BooleanArray,
            { |x: f64| { x == 0_f64 } }
        )) as ArrayRef),

        Float32 => Ok(Arc::new(make_function_scalar_inputs_return_type!(
            &args[0],
            "x",
            Float32Array,
            BooleanArray,
            { |x: f32| { x == 0_f32 } }
        )) as ArrayRef),

        other => exec_err!("Unsupported data type {other:?} for function iszero"),
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use arrow::array::{ArrayRef, Float32Array, Float64Array};

    use datafusion_common::cast::as_boolean_array;

    use crate::math::iszero::iszero;

    #[test]
    fn test_iszero_f64() {
        let args: Vec<ArrayRef> =
            vec![Arc::new(Float64Array::from(vec![1.0, 0.0, 3.0, -0.0]))];

        let result = iszero(&args).expect("failed to initialize function iszero");
        let booleans =
            as_boolean_array(&result).expect("failed to initialize function iszero");

        assert_eq!(booleans.len(), 4);
        assert!(!booleans.value(0));
        assert!(booleans.value(1));
        assert!(!booleans.value(2));
        assert!(booleans.value(3));
    }

    #[test]
    fn test_iszero_f32() {
        let args: Vec<ArrayRef> =
            vec![Arc::new(Float32Array::from(vec![1.0, 0.0, 3.0, -0.0]))];

        let result = iszero(&args).expect("failed to initialize function iszero");
        let booleans =
            as_boolean_array(&result).expect("failed to initialize function iszero");

        assert_eq!(booleans.len(), 4);
        assert!(!booleans.value(0));
        assert!(booleans.value(1));
        assert!(!booleans.value(2));
        assert!(booleans.value(3));
    }
}
