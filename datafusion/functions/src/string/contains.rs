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

use crate::utils::make_scalar_function;
use arrow::array::{ArrayRef, OffsetSizeTrait};
use arrow::datatypes::DataType;
use arrow::datatypes::DataType::Boolean;
use datafusion_common::cast::as_generic_string_array;
use datafusion_common::DataFusionError;
use datafusion_common::Result;
use datafusion_common::{arrow_datafusion_err, exec_err};
use datafusion_expr::ScalarUDFImpl;
use datafusion_expr::TypeSignature::Exact;
use datafusion_expr::{ColumnarValue, Signature, Volatility};
use std::any::Any;
use std::sync::Arc;
#[derive(Debug)]
pub struct ContainsFunc {
    signature: Signature,
}

impl Default for ContainsFunc {
    fn default() -> Self {
        ContainsFunc::new()
    }
}

impl ContainsFunc {
    pub fn new() -> Self {
        use DataType::*;
        Self {
            signature: Signature::one_of(
                vec![Exact(vec![Utf8, Utf8]), Exact(vec![LargeUtf8, LargeUtf8])],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for ContainsFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "contains"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _: &[DataType]) -> Result<DataType> {
        Ok(Boolean)
    }

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        match args[0].data_type() {
            DataType::Utf8 => make_scalar_function(contains::<i32>, vec![])(args),
            DataType::LargeUtf8 => make_scalar_function(contains::<i64>, vec![])(args),
            other => {
                exec_err!("unsupported data type {other:?} for function contains")
            }
        }
    }
}

/// use regexp_is_match_utf8_scalar to do the calculation for contains
pub fn contains<T: OffsetSizeTrait>(
    args: &[ArrayRef],
) -> Result<ArrayRef, DataFusionError> {
    let mod_str = as_generic_string_array::<T>(&args[0])?;
    let match_str = as_generic_string_array::<T>(&args[1])?;
    let res = arrow::compute::kernels::comparison::regexp_is_match_utf8(
        mod_str, match_str, None,
    )
    .map_err(|e| arrow_datafusion_err!(e))?;

    Ok(Arc::new(res) as ArrayRef)
}

#[cfg(test)]
mod tests {
    use crate::string::contains::ContainsFunc;
    use crate::utils::test::test_function;
    use arrow::array::Array;
    use arrow::{array::BooleanArray, datatypes::DataType::Boolean};
    use datafusion_common::Result;
    use datafusion_common::ScalarValue;
    use datafusion_expr::ColumnarValue;
    use datafusion_expr::ScalarUDFImpl;
    #[test]
    fn test_functions() -> Result<()> {
        test_function!(
            ContainsFunc::new(),
            &[
                ColumnarValue::Scalar(ScalarValue::from("alphabet")),
                ColumnarValue::Scalar(ScalarValue::from("alph")),
            ],
            Ok(Some(true)),
            bool,
            Boolean,
            BooleanArray
        );
        test_function!(
            ContainsFunc::new(),
            &[
                ColumnarValue::Scalar(ScalarValue::from("alphabet")),
                ColumnarValue::Scalar(ScalarValue::from("dddddd")),
            ],
            Ok(Some(false)),
            bool,
            Boolean,
            BooleanArray
        );
        test_function!(
            ContainsFunc::new(),
            &[
                ColumnarValue::Scalar(ScalarValue::from("alphabet")),
                ColumnarValue::Scalar(ScalarValue::from("pha")),
            ],
            Ok(Some(true)),
            bool,
            Boolean,
            BooleanArray
        );
        Ok(())
    }
}
