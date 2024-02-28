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

//! Encoding expressions
use arrow::array::{Array, ArrayRef, OffsetSizeTrait};
use arrow::compute::kernels::regexp;
use arrow::datatypes::DataType;
use arrow::datatypes::Field;
use datafusion_common::exec_err;
use datafusion_common::ScalarValue;
use datafusion_common::{arrow_datafusion_err, plan_err};
use datafusion_common::{
    cast::as_generic_string_array, internal_err, DataFusionError, Result,
};
use datafusion_expr::ColumnarValue;
use datafusion_expr::TypeSignature::*;
use datafusion_expr::{ScalarUDFImpl, Signature, Volatility};
use std::any::Any;
use std::sync::Arc;

#[derive(Debug)]
pub(super) struct RegexpMatchFunc {
    signature: Signature,
}
impl RegexpMatchFunc {
    pub fn new() -> Self {
        use DataType::*;
        Self {
            signature: Signature::one_of(
                vec![
                    Exact(vec![Utf8, Utf8]),
                    Exact(vec![LargeUtf8, Utf8]),
                    Exact(vec![Utf8, Utf8, Utf8]),
                    Exact(vec![LargeUtf8, Utf8, Utf8]),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for RegexpMatchFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "regexp_match"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        use DataType::*;

        Ok(match &arg_types[0] {
            LargeUtf8 => List(Arc::new(Field::new("item", LargeUtf8, true))),
            Utf8 => List(Arc::new(Field::new("item", Utf8, true))),
            Null => Null,
            other => {
                return plan_err!(
                    "The regexp_match function can only accept strings. Got {other}"
                );
            }
        })
    }
    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        let len = args
            .iter()
            .fold(Option::<usize>::None, |acc, arg| match arg {
                ColumnarValue::Scalar(_) => acc,
                ColumnarValue::Array(a) => Some(a.len()),
            });

        let is_scalar = len.is_none();
        let inferred_length = len.unwrap_or(1);
        let args = args
            .iter()
            .map(|arg| arg.clone().into_array(inferred_length))
            .collect::<Result<Vec<_>>>()?;

        let result = regexp_match_func(&args);
        if is_scalar {
            // If all inputs are scalar, keeps output as scalar
            let result = result.and_then(|arr| ScalarValue::try_from_array(&arr, 0));
            result.map(ColumnarValue::Scalar)
        } else {
            result.map(ColumnarValue::Array)
        }
    }
}
fn regexp_match_func(args: &[ArrayRef]) -> Result<ArrayRef> {
    match args[0].data_type() {
        DataType::Utf8 => regexp_match::<i32>(args),
        DataType::LargeUtf8 => regexp_match::<i64>(args),
        other => {
            internal_err!("Unsupported data type {other:?} for function regexp_match")
        }
    }
}
pub fn regexp_match<T: OffsetSizeTrait>(args: &[ArrayRef]) -> Result<ArrayRef> {
    match args.len() {
        2 => {
            let values = as_generic_string_array::<T>(&args[0])?;
            let regex = as_generic_string_array::<T>(&args[1])?;
            regexp::regexp_match(values, regex, None)
                .map_err(|e| arrow_datafusion_err!(e))
        }
        3 => {
            let values = as_generic_string_array::<T>(&args[0])?;
            let regex = as_generic_string_array::<T>(&args[1])?;
            let flags = as_generic_string_array::<T>(&args[2])?;

            if flags.iter().any(|s| s == Some("g")) {
                return plan_err!("regexp_match() does not support the \"global\" option")
            }

            regexp::regexp_match(values, regex, Some(flags))
                .map_err(|e| arrow_datafusion_err!(e))
        }
        other => exec_err!(
            "regexp_match was called with {other} arguments. It requires at least 2 and at most 3."
        ),
    }
}
