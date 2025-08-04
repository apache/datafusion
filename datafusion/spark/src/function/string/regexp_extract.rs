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

//! Regex expressions

use std::any::Any;
use std::collections::{hash_map::Entry, HashMap};
use std::sync::Arc;

use arrow::array::builder::GenericStringBuilder;
use arrow::array::{
    Array, ArrayRef, OffsetSizeTrait, PrimitiveArray, StringArrayType, UInt32Array,
};
use arrow::datatypes::{DataType, Field, FieldRef, UInt32Type};
use datafusion_common::cast::{
    as_generic_string_array, as_string_view_array, as_uint32_array,
};
use datafusion_common::{internal_err, DataFusionError, Result};
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    TypeSignature, Volatility,
};
use datafusion_functions::utils::make_scalar_function;
use regex::Regex;

/// REGEXP_EXTRACT function extracts the first string in the str that match
/// the regexp expression and corresponding to the regex group index.
/// <https://spark.apache.org/docs/latest/api/sql/#regexp_extract>
#[derive(Debug)]
pub struct SparkRegexpExtract {
    signature: Signature,
}

impl Default for SparkRegexpExtract {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkRegexpExtract {
    pub fn new() -> Self {
        use DataType::*;

        Self {
            signature: Signature::one_of(
                vec![
                    TypeSignature::Exact(vec![Utf8View, Utf8]),
                    TypeSignature::Exact(vec![Utf8, Utf8]),
                    TypeSignature::Exact(vec![LargeUtf8, Utf8]),
                    TypeSignature::Exact(vec![Utf8View, Utf8, UInt32]),
                    TypeSignature::Exact(vec![Utf8, Utf8, UInt32]),
                    TypeSignature::Exact(vec![LargeUtf8, Utf8, UInt32]),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for SparkRegexpExtract {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "regexp_extract"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        internal_err!("return_field_from_args should be used instead")
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        // Length check handled in the signature
        debug_assert!(matches!(args.scalar_arguments.len(), 2 | 3));

        let nullable = args.arg_fields.iter().any(|f| f.is_nullable());
        let return_type = match args.arg_fields[0].data_type() {
            DataType::Utf8View => DataType::Utf8,
            other => other.to_owned(),
        };

        return Ok(Arc::new(Field::new(self.name(), return_type, nullable)));
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        make_scalar_function(spark_regexp_extract, vec![])(&args.args)
    }
}

fn spark_regexp_extract(args: &[ArrayRef]) -> Result<ArrayRef> {
    let idx = match args.len() {
        3 => as_uint32_array(&*args[2])?,
        _ => {
            // idx was not provided on the input, we will use default value = 1
            let data = vec![1u32; args[0].len()];
            &UInt32Array::from(data)
        }
    };

    match args[0].data_type() {
        DataType::Utf8 => {
            let target = as_generic_string_array::<i32>(&args[0])?;
            let pattern = as_generic_string_array::<i32>(&args[1])?;
            regexp_extract::<i32>(target, pattern, idx)
        }
        DataType::LargeUtf8 => {
            let target = as_generic_string_array::<i64>(&args[0])?;
            let pattern = as_generic_string_array::<i64>(&args[1])?;
            regexp_extract::<i64>(target, pattern, idx)
        }
        DataType::Utf8View => {
            let target = as_string_view_array(&args[0])?;
            let pattern = as_string_view_array(&args[1])?;
            regexp_extract::<i32>(target, pattern, idx)
        }
        other => {
            internal_err!("Unsupported data type {other:?} for function regexp_extract")
        }
    }
}

fn regexp_extract<'a, T>(
    target: impl StringArrayType<'a>,
    pattern: impl StringArrayType<'a>,
    idx: &PrimitiveArray<UInt32Type>,
) -> Result<ArrayRef>
where
    T: OffsetSizeTrait,
{
    let mut builder = GenericStringBuilder::<T>::new();
    let mut regex_cache: HashMap<&str, Regex> = HashMap::new();

    for ((t_opt, p_opt), i_opt) in target.iter().zip(pattern.iter()).zip(idx) {
        match (t_opt, p_opt) {
            (None, _) | (_, None) => {
                // If any of arguments is null, the result will be null too
                builder.append_null();
            }
            (Some(target_str), Some(pattern_str)) => {
                let idx_val = i_opt.unwrap_or(1);

                let re = match regex_cache.entry(pattern_str) {
                    Entry::Occupied(occupied_entry) => occupied_entry.into_mut(),
                    Entry::Vacant(vacant_entry) => {
                        let compiled = Regex::new(pattern_str).map_err(|e| {
                            DataFusionError::Execution(format!(
                                "Can't compile regexp: {e}"
                            ))
                        })?;
                        vacant_entry.insert(compiled)
                    }
                };

                let caps_opt = re.captures(target_str);

                match caps_opt {
                    Some(caps) => {
                        // If regexp matches string
                        let group_idx = idx_val as usize;
                        if group_idx < caps.len() {
                            // If specified group index really exists
                            if let Some(m) = caps.get(group_idx) {
                                // If the specified group has a match
                                builder.append_value(m.as_str());
                            } else {
                                builder.append_value("");
                            }
                        } else {
                            builder.append_value("");
                        }
                    }
                    None => {
                        builder.append_value("");
                    }
                }
            }
        }
    }

    Ok(Arc::new(builder.finish()))
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::function::utils::test::test_scalar_function;
    use arrow::array::{LargeStringArray, StringArray};
    use datafusion_common::ScalarValue;

    macro_rules! test_regexp_extract_string_invoke {
        ($INPUT1:expr, $INPUT2:expr, $EXPECTED:expr) => {
            test_scalar_function!(
                SparkRegexpExtract::new(),
                vec![
                    ColumnarValue::Scalar(ScalarValue::Utf8View($INPUT1)),
                    ColumnarValue::Scalar(ScalarValue::Utf8View($INPUT2))
                ],
                $EXPECTED,
                String,
                DataType::Utf8,
                StringArray
            );

            test_scalar_function!(
                SparkRegexpExtract::new(),
                vec![
                    ColumnarValue::Scalar(ScalarValue::Utf8($INPUT1)),
                    ColumnarValue::Scalar(ScalarValue::Utf8($INPUT2))
                ],
                $EXPECTED,
                String,
                DataType::Utf8,
                StringArray
            );

            test_scalar_function!(
                SparkRegexpExtract::new(),
                vec![
                    ColumnarValue::Scalar(ScalarValue::LargeUtf8($INPUT1)),
                    ColumnarValue::Scalar(ScalarValue::LargeUtf8($INPUT2))
                ],
                $EXPECTED,
                String,
                DataType::LargeUtf8,
                LargeStringArray
            );
        };
    }

    #[test]
    fn test_regexp_extract_basic_cases() {
        test_regexp_extract_string_invoke!(
            Some(String::from("100-200")),
            Some(String::from(r"(\d+)-(\d+)")),
            Ok(Some(String::from("100")))
        );
        test_regexp_extract_string_invoke!(
            Some(String::from("foo")),
            Some(String::from(r"(\d+)")),
            Ok(Some(String::from("")))
        );
        test_regexp_extract_string_invoke!(None, Some(String::from(r"(.*)")), Ok(None));
        test_regexp_extract_string_invoke!(Some(String::from("a")), None, Ok(None));
    }
}
