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

use arrow::array::{ArrayRef, AsArray};
use arrow::datatypes::DataType::{Float32, Float64};
use arrow::datatypes::{DataType, Float32Type, Float64Type};

use crate::utils::make_scalar_function;
use datafusion_common::{exec_err, Result};
use datafusion_expr::scalar_doc_sections::DOC_SECTION_MATH;
use datafusion_expr::{ColumnarValue, Documentation};
use datafusion_expr::{ScalarUDFImpl, Signature, Volatility};

#[derive(Debug)]
pub struct CotFunc {
    signature: Signature,
}

impl Default for CotFunc {
    fn default() -> Self {
        CotFunc::new()
    }
}

static DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

fn get_cot_doc() -> &'static Documentation {
    DOCUMENTATION.get_or_init(|| {
        Documentation::builder()
            .with_doc_section(DOC_SECTION_MATH)
            .with_description("Returns the cotangent of a number.")
            .with_syntax_example(r#"cot(numeric_expression)"#)
            .with_standard_argument("numeric_expression", Some("Numeric"))
            .build()
            .unwrap()
    })
}

impl CotFunc {
    pub fn new() -> Self {
        use DataType::*;
        Self {
            // math expressions expect 1 argument of type f64 or f32
            // priority is given to f64 because e.g. `sqrt(1i32)` is in IR (real numbers) and thus we
            // return the best approximation for it (in f64).
            // We accept f32 because in this case it is clear that the best approximation
            // will be as good as the number of digits in the number
            signature: Signature::uniform(
                1,
                vec![Float64, Float32],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for CotFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "cot"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        match arg_types[0] {
            Float32 => Ok(Float32),
            _ => Ok(Float64),
        }
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(get_cot_doc())
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        make_scalar_function(cot, vec![])(args)
    }
}

///cot SQL function
fn cot(args: &[ArrayRef]) -> Result<ArrayRef> {
    match args[0].data_type() {
        Float64 => Ok(Arc::new(
            args[0]
                .as_primitive::<Float64Type>()
                .unary::<_, Float64Type>(|x: f64| compute_cot64(x)),
        ) as ArrayRef),
        Float32 => Ok(Arc::new(
            args[0]
                .as_primitive::<Float32Type>()
                .unary::<_, Float32Type>(|x: f32| compute_cot32(x)),
        ) as ArrayRef),
        other => exec_err!("Unsupported data type {other:?} for function cot"),
    }
}

fn compute_cot32(x: f32) -> f32 {
    let a = f32::tan(x);
    1.0 / a
}

fn compute_cot64(x: f64) -> f64 {
    let a = f64::tan(x);
    1.0 / a
}

#[cfg(test)]
mod test {
    use crate::math::cot::cot;
    use arrow::array::{ArrayRef, Float32Array, Float64Array};
    use datafusion_common::cast::{as_float32_array, as_float64_array};
    use std::sync::Arc;

    #[test]
    fn test_cot_f32() {
        let args: Vec<ArrayRef> =
            vec![Arc::new(Float32Array::from(vec![12.1, 30.0, 90.0, -30.0]))];
        let result = cot(&args).expect("failed to initialize function cot");
        let floats =
            as_float32_array(&result).expect("failed to initialize function cot");

        let expected = Float32Array::from(vec![
            -1.986_460_4,
            -0.156_119_96,
            -0.501_202_8,
            0.156_119_96,
        ]);

        let eps = 1e-6;
        assert_eq!(floats.len(), 4);
        assert!((floats.value(0) - expected.value(0)).abs() < eps);
        assert!((floats.value(1) - expected.value(1)).abs() < eps);
        assert!((floats.value(2) - expected.value(2)).abs() < eps);
        assert!((floats.value(3) - expected.value(3)).abs() < eps);
    }

    #[test]
    fn test_cot_f64() {
        let args: Vec<ArrayRef> =
            vec![Arc::new(Float64Array::from(vec![12.1, 30.0, 90.0, -30.0]))];
        let result = cot(&args).expect("failed to initialize function cot");
        let floats =
            as_float64_array(&result).expect("failed to initialize function cot");

        let expected = Float64Array::from(vec![
            -1.986_458_685_881_4,
            -0.156_119_952_161_6,
            -0.501_202_783_380_1,
            0.156_119_952_161_6,
        ]);

        let eps = 1e-12;
        assert_eq!(floats.len(), 4);
        assert!((floats.value(0) - expected.value(0)).abs() < eps);
        assert!((floats.value(1) - expected.value(1)).abs() < eps);
        assert!((floats.value(2) - expected.value(2)).abs() < eps);
        assert!((floats.value(3) - expected.value(3)).abs() < eps);
    }
}
