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

use arrow::{
    array::{ArrayRef, Int64Array},
    error::ArrowError,
};
use std::any::Any;
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

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        make_scalar_function(factorial, vec![])(args)
    }

    fn documentation(&self) -> Option<&Documentation> {
        Some(get_factorial_doc())
    }
}

static DOCUMENTATION: OnceLock<Documentation> = OnceLock::new();

fn get_factorial_doc() -> &'static Documentation {
    DOCUMENTATION.get_or_init(|| {
        Documentation::builder()
            .with_doc_section(DOC_SECTION_MATH)
            .with_description("Factorial. Returns 1 if value is less than 2.")
            .with_syntax_example("factorial(numeric_expression)")
            .with_standard_argument("numeric_expression", "Numeric")
            .build()
            .unwrap()
    })
}

/// Factorial SQL function
fn factorial(args: &[ArrayRef]) -> Result<ArrayRef> {
    match args[0].data_type() {
        DataType::Int64 => {
            let arg = downcast_arg!((&args[0]), "value", Int64Array);
            Ok(arg
                .iter()
                .map(|a| match a {
                    Some(a) => (2..=a)
                        .try_fold(1i64, i64::checked_mul)
                        .ok_or_else(|| {
                            arrow_datafusion_err!(ArrowError::ComputeError(format!(
                                "Overflow happened on FACTORIAL({a})"
                            )))
                        })
                        .map(Some),
                    _ => Ok(None),
                })
                .collect::<Result<Int64Array>>()
                .map(Arc::new)? as ArrayRef)
        }
        other => exec_err!("Unsupported data type {other:?} for function factorial."),
    }
}

#[cfg(test)]
mod test {

    use datafusion_common::cast::as_int64_array;

    use super::*;

    #[test]
    fn test_factorial_i64() {
        let args: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from(vec![0, 1, 2, 4])), // input
        ];

        let result = factorial(&args).expect("failed to initialize function factorial");
        let ints =
            as_int64_array(&result).expect("failed to initialize function factorial");

        let expected = Int64Array::from(vec![1, 1, 2, 24]);

        assert_eq!(ints, &expected);
    }
}
