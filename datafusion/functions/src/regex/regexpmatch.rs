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

//! Regx expressions
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
pub struct RegexpMatchFunc {
    signature: Signature,
}

impl Default for RegexpMatchFunc {
    fn default() -> Self {
        Self::new()
    }
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
        Ok(match &arg_types[0] {
            DataType::Null => DataType::Null,
            other => DataType::List(Arc::new(Field::new("item", other.clone(), true))),
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
#[cfg(test)]
mod tests {
    use crate::regex::regexpmatch::regexp_match;
    use arrow::array::StringArray;
    use arrow::array::{GenericStringBuilder, ListBuilder};
    use std::sync::Arc;

    #[test]
    fn test_case_sensitive_regexp_match() {
        let values = StringArray::from(vec!["abc"; 5]);
        let patterns =
            StringArray::from(vec!["^(a)", "^(A)", "(b|d)", "(B|D)", "^(b|c)"]);

        let elem_builder: GenericStringBuilder<i32> = GenericStringBuilder::new();
        let mut expected_builder = ListBuilder::new(elem_builder);
        expected_builder.values().append_value("a");
        expected_builder.append(true);
        expected_builder.append(false);
        expected_builder.values().append_value("b");
        expected_builder.append(true);
        expected_builder.append(false);
        expected_builder.append(false);
        let expected = expected_builder.finish();

        let re = regexp_match::<i32>(&[Arc::new(values), Arc::new(patterns)]).unwrap();

        assert_eq!(re.as_ref(), &expected);
    }

    #[test]
    fn test_case_insensitive_regexp_match() {
        let values = StringArray::from(vec!["abc"; 5]);
        let patterns =
            StringArray::from(vec!["^(a)", "^(A)", "(b|d)", "(B|D)", "^(b|c)"]);
        let flags = StringArray::from(vec!["i"; 5]);

        let elem_builder: GenericStringBuilder<i32> = GenericStringBuilder::new();
        let mut expected_builder = ListBuilder::new(elem_builder);
        expected_builder.values().append_value("a");
        expected_builder.append(true);
        expected_builder.values().append_value("a");
        expected_builder.append(true);
        expected_builder.values().append_value("b");
        expected_builder.append(true);
        expected_builder.values().append_value("b");
        expected_builder.append(true);
        expected_builder.append(false);
        let expected = expected_builder.finish();

        let re =
            regexp_match::<i32>(&[Arc::new(values), Arc::new(patterns), Arc::new(flags)])
                .unwrap();

        assert_eq!(re.as_ref(), &expected);
    }

    #[test]
    fn test_unsupported_global_flag_regexp_match() {
        let values = StringArray::from(vec!["abc"]);
        let patterns = StringArray::from(vec!["^(a)"]);
        let flags = StringArray::from(vec!["g"]);

        let re_err =
            regexp_match::<i32>(&[Arc::new(values), Arc::new(patterns), Arc::new(flags)])
                .expect_err("unsupported flag should have failed");

        assert_eq!(re_err.strip_backtrace(), "Error during planning: regexp_match() does not support the \"global\" option");
    }
}
