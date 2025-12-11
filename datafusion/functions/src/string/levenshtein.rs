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

use arrow::array::{ArrayRef, Int32Array, Int64Array, OffsetSizeTrait};
use arrow::datatypes::DataType;

use crate::utils::{make_scalar_function, utf8_to_int_type};
use datafusion_common::cast::{as_generic_string_array, as_string_view_array};
use datafusion_common::types::logical_string;
use datafusion_common::utils::datafusion_strsim;
use datafusion_common::utils::take_function_args;
use datafusion_common::{Result, exec_err};
use datafusion_expr::type_coercion::binary::{
    binary_to_string_coercion, string_coercion,
};
use datafusion_expr::{
    Coercion, ColumnarValue, Documentation, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    TypeSignatureClass, Volatility,
};
use datafusion_macros::user_doc;

#[user_doc(
    doc_section(label = "String Functions"),
    description = "Returns the [`Levenshtein distance`](https://en.wikipedia.org/wiki/Levenshtein_distance) between the two given strings.",
    syntax_example = "levenshtein(str1, str2)",
    sql_example = r#"```sql
> select levenshtein('kitten', 'sitting');
+---------------------------------------------+
| levenshtein(Utf8("kitten"),Utf8("sitting")) |
+---------------------------------------------+
| 3                                           |
+---------------------------------------------+
```"#,
    argument(
        name = "str1",
        description = "String expression to compute Levenshtein distance with str2."
    ),
    argument(
        name = "str2",
        description = "String expression to compute Levenshtein distance with str1."
    )
)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct LevenshteinFunc {
    signature: Signature,
}

impl Default for LevenshteinFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl LevenshteinFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::coercible(
                vec![
                    Coercion::new_exact(TypeSignatureClass::Native(logical_string())),
                    Coercion::new_exact(TypeSignatureClass::Native(logical_string())),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for LevenshteinFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "levenshtein"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        if let Some(coercion_data_type) = string_coercion(&arg_types[0], &arg_types[1])
            .or_else(|| binary_to_string_coercion(&arg_types[0], &arg_types[1]))
        {
            utf8_to_int_type(&coercion_data_type, "levenshtein")
        } else {
            exec_err!(
                "Unsupported data types for levenshtein. Expected Utf8, LargeUtf8 or Utf8View"
            )
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        match args.args[0].data_type() {
            DataType::Utf8View | DataType::Utf8 => {
                make_scalar_function(levenshtein::<i32>, vec![])(&args.args)
            }
            DataType::LargeUtf8 => {
                make_scalar_function(levenshtein::<i64>, vec![])(&args.args)
            }
            other => {
                exec_err!("Unsupported data type {other:?} for function levenshtein")
            }
        }
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

///Returns the Levenshtein distance between the two given strings.
/// LEVENSHTEIN('kitten', 'sitting') = 3
fn levenshtein<T: OffsetSizeTrait>(args: &[ArrayRef]) -> Result<ArrayRef> {
    let [str1, str2] = take_function_args("levenshtein", args)?;

    if let Some(coercion_data_type) =
        string_coercion(args[0].data_type(), args[1].data_type()).or_else(|| {
            binary_to_string_coercion(args[0].data_type(), args[1].data_type())
        })
    {
        let str1 = if str1.data_type() == &coercion_data_type {
            Arc::clone(str1)
        } else {
            arrow::compute::kernels::cast::cast(&str1, &coercion_data_type)?
        };
        let str2 = if str2.data_type() == &coercion_data_type {
            Arc::clone(str2)
        } else {
            arrow::compute::kernels::cast::cast(&str2, &coercion_data_type)?
        };

        match coercion_data_type {
            DataType::Utf8View => {
                let str1_array = as_string_view_array(&str1)?;
                let str2_array = as_string_view_array(&str2)?;
                let result = str1_array
                    .iter()
                    .zip(str2_array.iter())
                    .map(|(string1, string2)| match (string1, string2) {
                        (Some(string1), Some(string2)) => {
                            Some(datafusion_strsim::levenshtein(string1, string2) as i32)
                        }
                        _ => None,
                    })
                    .collect::<Int32Array>();
                Ok(Arc::new(result) as ArrayRef)
            }
            DataType::Utf8 => {
                let str1_array = as_generic_string_array::<T>(&str1)?;
                let str2_array = as_generic_string_array::<T>(&str2)?;
                let result = str1_array
                    .iter()
                    .zip(str2_array.iter())
                    .map(|(string1, string2)| match (string1, string2) {
                        (Some(string1), Some(string2)) => {
                            Some(datafusion_strsim::levenshtein(string1, string2) as i32)
                        }
                        _ => None,
                    })
                    .collect::<Int32Array>();
                Ok(Arc::new(result) as ArrayRef)
            }
            DataType::LargeUtf8 => {
                let str1_array = as_generic_string_array::<T>(&str1)?;
                let str2_array = as_generic_string_array::<T>(&str2)?;
                let result = str1_array
                    .iter()
                    .zip(str2_array.iter())
                    .map(|(string1, string2)| match (string1, string2) {
                        (Some(string1), Some(string2)) => {
                            Some(datafusion_strsim::levenshtein(string1, string2) as i64)
                        }
                        _ => None,
                    })
                    .collect::<Int64Array>();
                Ok(Arc::new(result) as ArrayRef)
            }
            other => {
                exec_err!(
                    "levenshtein was called with {other} datatype arguments. It requires Utf8View, Utf8 or LargeUtf8."
                )
            }
        }
    } else {
        exec_err!(
            "Unsupported data types for levenshtein. Expected Utf8, LargeUtf8 or Utf8View"
        )
    }
}

#[cfg(test)]
mod tests {
    use arrow::array::StringArray;

    use datafusion_common::cast::as_int32_array;

    use super::*;

    #[test]
    fn to_levenshtein() -> Result<()> {
        let string1_array =
            Arc::new(StringArray::from(vec!["123", "abc", "xyz", "kitten"]));
        let string2_array =
            Arc::new(StringArray::from(vec!["321", "def", "zyx", "sitting"]));
        let res = levenshtein::<i32>(&[string1_array, string2_array]).unwrap();
        let result =
            as_int32_array(&res).expect("failed to initialized function levenshtein");
        let expected = Int32Array::from(vec![2, 3, 2, 3]);
        assert_eq!(&expected, result);

        Ok(())
    }
}
