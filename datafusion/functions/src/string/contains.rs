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

use crate::regexp_common::regexp_is_match_utf8;
use crate::utils::make_scalar_function;

use arrow::array::{Array, ArrayRef, AsArray, GenericStringArray, StringViewArray};
use arrow::datatypes::DataType;
use arrow::datatypes::DataType::{Boolean, LargeUtf8, Utf8, Utf8View};
use datafusion_common::exec_err;
use datafusion_common::DataFusionError;
use datafusion_common::Result;
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
                vec![
                    Exact(vec![Utf8View, Utf8View]),
                    Exact(vec![Utf8View, Utf8]),
                    Exact(vec![Utf8View, LargeUtf8]),
                    Exact(vec![Utf8, Utf8View]),
                    Exact(vec![Utf8, Utf8]),
                    Exact(vec![Utf8, LargeUtf8]),
                    Exact(vec![LargeUtf8, Utf8View]),
                    Exact(vec![LargeUtf8, Utf8]),
                    Exact(vec![LargeUtf8, LargeUtf8]),
                ],
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
        make_scalar_function(contains, vec![])(args)
    }
}

/// use regexp_is_match_utf8_scalar to do the calculation for contains
pub fn contains(args: &[ArrayRef]) -> Result<ArrayRef, DataFusionError> {
    match (args[0].data_type(), args[1].data_type()) {
        (Utf8View, Utf8View) => {
            let mod_str = args[0].as_string_view();
            let match_str = args[1].as_string_view();
            let res = regexp_is_match_utf8::<
                StringViewArray,
                StringViewArray,
                GenericStringArray<i32>,
            >(mod_str, match_str, None)?;

            Ok(Arc::new(res) as ArrayRef)
        }
        (Utf8View, Utf8) => {
            let mod_str = args[0].as_string_view();
            let match_str = args[1].as_string::<i32>();
            let res = regexp_is_match_utf8::<
                StringViewArray,
                GenericStringArray<i32>,
                GenericStringArray<i32>,
            >(mod_str, match_str, None)?;

            Ok(Arc::new(res) as ArrayRef)
        }
        (Utf8View, LargeUtf8) => {
            let mod_str = args[0].as_string_view();
            let match_str = args[1].as_string::<i64>();
            let res = regexp_is_match_utf8::<
                StringViewArray,
                GenericStringArray<i64>,
                GenericStringArray<i32>,
            >(mod_str, match_str, None)?;

            Ok(Arc::new(res) as ArrayRef)
        }
        (Utf8, Utf8View) => {
            let mod_str = args[0].as_string::<i32>();
            let match_str = args[1].as_string_view();
            let res = regexp_is_match_utf8::<
                GenericStringArray<i32>,
                StringViewArray,
                GenericStringArray<i32>,
            >(mod_str, match_str, None)?;

            Ok(Arc::new(res) as ArrayRef)
        }
        (Utf8, Utf8) => {
            let mod_str = args[0].as_string::<i32>();
            let match_str = args[1].as_string::<i32>();
            let res = regexp_is_match_utf8::<
                GenericStringArray<i32>,
                GenericStringArray<i32>,
                GenericStringArray<i32>,
            >(mod_str, match_str, None)?;

            Ok(Arc::new(res) as ArrayRef)
        }
        (Utf8, LargeUtf8) => {
            let mod_str = args[0].as_string::<i32>();
            let match_str = args[1].as_string::<i64>();
            let res = regexp_is_match_utf8::<
                GenericStringArray<i32>,
                GenericStringArray<i64>,
                GenericStringArray<i32>,
            >(mod_str, match_str, None)?;

            Ok(Arc::new(res) as ArrayRef)
        }
        (LargeUtf8, Utf8View) => {
            let mod_str = args[0].as_string::<i64>();
            let match_str = args[1].as_string_view();
            let res = regexp_is_match_utf8::<
                GenericStringArray<i64>,
                StringViewArray,
                GenericStringArray<i32>,
            >(mod_str, match_str, None)?;

            Ok(Arc::new(res) as ArrayRef)
        }
        (LargeUtf8, Utf8) => {
            let mod_str = args[0].as_string::<i64>();
            let match_str = args[1].as_string::<i32>();
            let res = regexp_is_match_utf8::<
                GenericStringArray<i64>,
                GenericStringArray<i32>,
                GenericStringArray<i32>,
            >(mod_str, match_str, None)?;

            Ok(Arc::new(res) as ArrayRef)
        }
        (LargeUtf8, LargeUtf8) => {
            let mod_str = args[0].as_string::<i64>();
            let match_str = args[1].as_string::<i64>();
            let res = regexp_is_match_utf8::<
                GenericStringArray<i64>,
                GenericStringArray<i64>,
                GenericStringArray<i32>,
            >(mod_str, match_str, None)?;

            Ok(Arc::new(res) as ArrayRef)
        }
        other => {
            exec_err!("Unsupported data type {other:?} for function `contains`.")
        }
    }
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

        test_function!(
            ContainsFunc::new(),
            &[
                ColumnarValue::Scalar(ScalarValue::Utf8View(Some(String::from(
                    "Apache"
                )))),
                ColumnarValue::Scalar(ScalarValue::Utf8View(Some(String::from("pac")))),
            ],
            Ok(Some(true)),
            bool,
            Boolean,
            BooleanArray
        );
        test_function!(
            ContainsFunc::new(),
            &[
                ColumnarValue::Scalar(ScalarValue::Utf8View(Some(String::from(
                    "Apache"
                )))),
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(String::from("ap")))),
            ],
            Ok(Some(false)),
            bool,
            Boolean,
            BooleanArray
        );
        test_function!(
            ContainsFunc::new(),
            &[
                ColumnarValue::Scalar(ScalarValue::Utf8View(Some(String::from(
                    "Apache"
                )))),
                ColumnarValue::Scalar(ScalarValue::LargeUtf8(Some(String::from(
                    "DataFusion"
                )))),
            ],
            Ok(Some(false)),
            bool,
            Boolean,
            BooleanArray
        );

        Ok(())
    }
}
