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

use arrow::array::{ArrayRef, Float32Array, Float64Array};
use arrow::datatypes::DataType;
use arrow::datatypes::DataType::{Float32, Float64};

use datafusion_common::{exec_err, DataFusionError, Result};
use datafusion_expr::ColumnarValue;
use datafusion_expr::TypeSignature::Exact;
use datafusion_expr::{ScalarUDFImpl, Signature, Volatility};

use crate::utils::make_scalar_function;

#[derive(Debug)]
pub struct NanvlFunc {
    signature: Signature,
}

impl Default for NanvlFunc {
    fn default() -> Self {
        NanvlFunc::new()
    }
}

impl NanvlFunc {
    pub fn new() -> Self {
        use DataType::*;
        Self {
            signature: Signature::one_of(
                vec![Exact(vec![Float32, Float32]), Exact(vec![Float64, Float64])],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for NanvlFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "nanvl"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        match &arg_types[0] {
            Float32 => Ok(Float32),
            _ => Ok(Float64),
        }
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        make_scalar_function(nanvl, vec![])(args)
    }
}

/// Nanvl SQL function
fn nanvl(args: &[ArrayRef]) -> Result<ArrayRef> {
    match args[0].data_type() {
        Float64 => {
            let compute_nanvl = |x: f64, y: f64| {
                if x.is_nan() {
                    y
                } else {
                    x
                }
            };

            Ok(Arc::new(make_function_inputs2!(
                &args[0],
                &args[1],
                "x",
                "y",
                Float64Array,
                { compute_nanvl }
            )) as ArrayRef)
        }
        Float32 => {
            let compute_nanvl = |x: f32, y: f32| {
                if x.is_nan() {
                    y
                } else {
                    x
                }
            };

            Ok(Arc::new(make_function_inputs2!(
                &args[0],
                &args[1],
                "x",
                "y",
                Float32Array,
                { compute_nanvl }
            )) as ArrayRef)
        }
        other => exec_err!("Unsupported data type {other:?} for function nanvl"),
    }
}

#[cfg(test)]
mod test {
    use crate::math::nanvl::nanvl;
    use arrow::array::{ArrayRef, Float32Array, Float64Array};
    use datafusion_common::cast::{as_float32_array, as_float64_array};
    use std::sync::Arc;

    #[test]
    fn test_nanvl_f64() {
        let args: Vec<ArrayRef> = vec![
            Arc::new(Float64Array::from(vec![1.0, f64::NAN, 3.0, f64::NAN])), // y
            Arc::new(Float64Array::from(vec![5.0, 6.0, f64::NAN, f64::NAN])), // x
        ];

        let result = nanvl(&args).expect("failed to initialize function nanvl");
        let floats =
            as_float64_array(&result).expect("failed to initialize function nanvl");

        assert_eq!(floats.len(), 4);
        assert_eq!(floats.value(0), 1.0);
        assert_eq!(floats.value(1), 6.0);
        assert_eq!(floats.value(2), 3.0);
        assert!(floats.value(3).is_nan());
    }

    #[test]
    fn test_nanvl_f32() {
        let args: Vec<ArrayRef> = vec![
            Arc::new(Float32Array::from(vec![1.0, f32::NAN, 3.0, f32::NAN])), // y
            Arc::new(Float32Array::from(vec![5.0, 6.0, f32::NAN, f32::NAN])), // x
        ];

        let result = nanvl(&args).expect("failed to initialize function nanvl");
        let floats =
            as_float32_array(&result).expect("failed to initialize function nanvl");

        assert_eq!(floats.len(), 4);
        assert_eq!(floats.value(0), 1.0);
        assert_eq!(floats.value(1), 6.0);
        assert_eq!(floats.value(2), 3.0);
        assert!(floats.value(3).is_nan());
    }
}
