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
use std::sync::{Arc, OnceLock};

use crate::utils::make_scalar_function;

use arrow::array::{ArrayRef, AsArray, Float32Array, Float64Array};
use arrow::datatypes::DataType::{Float32, Float64};
use arrow::datatypes::{DataType, Float32Type, Float64Type};
use datafusion_common::{exec_err, DataFusionError, Result};
use datafusion_expr::scalar_doc_sections::DOC_SECTION_MATH;
use datafusion_expr::TypeSignature::Exact;
use datafusion_expr::{
    ColumnarValue, Documentation, ScalarUDFImpl, Signature, Volatility,
};

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

    fn documentation(&self) -> Option<&Documentation> {
        Some(get_nanvl_doc())
    }
}

static DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

fn get_nanvl_doc() -> &'static Documentation {
    DOCUMENTATION.get_or_init(|| {
        Documentation::builder()
            .with_doc_section(DOC_SECTION_MATH)
            .with_description(
                r#"Returns the first argument if it's not _NaN_.
Returns the second argument otherwise."#,
            )
            .with_syntax_example("nanvl(expression_x, expression_y)")
            .with_argument("expression_x", "Numeric expression to return if it's not _NaN_. Can be a constant, column, or function, and any combination of arithmetic operators.")
            .with_argument("expression_y", "Numeric expression to return if the first expression is _NaN_. Can be a constant, column, or function, and any combination of arithmetic operators.")
            .build()
            .unwrap()
    })
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

            let x = args[0].as_primitive() as &Float64Array;
            let y = args[1].as_primitive() as &Float64Array;
            arrow::compute::binary::<_, _, _, Float64Type>(x, y, compute_nanvl)
                .map(|res| Arc::new(res) as _)
                .map_err(DataFusionError::from)
        }
        Float32 => {
            let compute_nanvl = |x: f32, y: f32| {
                if x.is_nan() {
                    y
                } else {
                    x
                }
            };

            let x = args[0].as_primitive() as &Float32Array;
            let y = args[1].as_primitive() as &Float32Array;
            arrow::compute::binary::<_, _, _, Float32Type>(x, y, compute_nanvl)
                .map(|res| Arc::new(res) as _)
                .map_err(DataFusionError::from)
        }
        other => exec_err!("Unsupported data type {other:?} for function nanvl"),
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use crate::math::nanvl::nanvl;

    use arrow::array::{ArrayRef, Float32Array, Float64Array};
    use datafusion_common::cast::{as_float32_array, as_float64_array};

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
