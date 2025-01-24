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

use std::sync::Arc;

use arrow::array::builder::GenericStringBuilder;
use arrow::array::{Array, ArrayRef, AsArray, OffsetSizeTrait};
use arrow::datatypes::{DataType, Int64Type};
use datafusion_common::{
    cast::as_generic_string_array, exec_err, internal_err, plan_err, DataFusionError,
    Result,
};
use datafusion_expr::{
    ColumnarValue, Documentation, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    TypeSignature, Volatility,
};
use datafusion_macros::user_doc;
use regex::Regex;

#[user_doc(
    doc_section(label = "Regular Expression Functions"),
    description = "Extract a specific group matched by [regular expression](https://docs.rs/regex/latest/regex/#syntax). If the regex did not match, or the specified group did not match, an empty string is returned..",
    syntax_example = "regexp_extract(str, regexp[, idx])",
    sql_example = r#"```sql
            > select regexp_extract('100-200', '(\d+)-(\d+)', 1);
            +---------------------------------------------------------------+
            | regexp_extract(Utf8("100-200"),Utf8("(\d+)-(\d+)"), Int64(1)) |
            +---------------------------------------------------------------+
            | [100]                                                         |
            +---------------------------------------------------------------+
```
"#,
    standard_argument(name = "str", prefix = "String"),
    standard_argument(name = "regexp", prefix = "Regular"),
    standard_argument(name = "idx", prefix = "Integer")
)]
#[derive(Debug)]
pub struct RegexpExtractFunc {
    signature: Signature,
}

impl Default for RegexpExtractFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl RegexpExtractFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    TypeSignature::Exact(vec![
                        DataType::Utf8,
                        DataType::Utf8,
                        DataType::Int64,
                    ]),
                    TypeSignature::Exact(vec![
                        DataType::LargeUtf8,
                        DataType::LargeUtf8,
                        DataType::Int64,
                    ]),
                    TypeSignature::Exact(vec![
                        DataType::Utf8View,
                        DataType::Utf8View,
                        DataType::Int64,
                    ]),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for RegexpExtractFunc {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "regexp_extract"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        use DataType::*;
        Ok(match &arg_types[0] {
            LargeUtf8 | LargeBinary => LargeUtf8,
            Utf8 | Binary => Utf8,
            Utf8View | BinaryView => Utf8View,
            Null => Null,
            other => {
                return plan_err!(
                    "The regexp_extract function can only accept strings. Got {other}"
                );
            }
        })
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let args_len = args.args.len();
        if args_len != 3 {
            return exec_err!("regexp_extract was called with {args_len} arguments, but it can accept only 3.");
        }

        let target = args.args[0].to_array(args.number_rows)?;
        let pattern = args.args[1].to_array(args.number_rows)?;
        let idx = args.args[2].to_array(args.number_rows)?;

        let res = regexp_extract_func(&[target, pattern, idx])?;
        Ok(ColumnarValue::Array(res))
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

fn regexp_extract_func(args: &[ArrayRef; 3]) -> Result<ArrayRef> {
    match args[0].data_type() {
        DataType::Utf8 | DataType::Utf8View => regexp_extract::<i32>(args),
        DataType::LargeUtf8 => regexp_extract::<i64>(args),
        other => {
            internal_err!("Unsupported data type {other:?} for function regexp_extract")
        }
    }
}

fn regexp_extract<T: OffsetSizeTrait>(args: &[ArrayRef]) -> Result<ArrayRef> {
    let target = as_generic_string_array::<T>(&args[0])?;
    let pattern = as_generic_string_array::<T>(&args[1])?;
    let idx = args[2].as_primitive::<Int64Type>();

    let mut builder = GenericStringBuilder::<T>::new();

    for ((t_opt, p_opt), i_opt) in target.iter().zip(pattern).zip(idx) {
        match (t_opt, p_opt, i_opt) {
            (None, _, _) | (_, None, _) | (_, _, None) => {
                // If any of arguments is null, the result will be null too
                builder.append_null();
            }
            (Some(target_str), Some(pattern_str), Some(idx_val)) => {
                if idx_val < 0 {
                    return exec_err!("idx in regexp_extract can't be negative");
                }

                let re = Regex::new(pattern_str).map_err(|e| {
                    DataFusionError::Execution(format!("Can't compile regexp: {e}"))
                })?;
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
    use arrow::array::{Int64Array, StringArray};
    use datafusion_common::cast::as_generic_string_array;
    use std::sync::Arc;

    #[test]
    fn test_pyspark_cases() {
        let target_arr = StringArray::from(vec![
            Some("100-200"),   // 0
            Some("foo"),       // 1
            Some("aaaac"),     // 2
            Some("abc"),       // 3
            Some("xyz"),       // 4
            None,              // 5
            Some("some text"), // 6
        ]);

        let pattern_arr = StringArray::from(vec![
            Some(r"(\d+)-(\d+)"), // 0
            Some(r"(\d+)"),       // 1
            Some("(a+)(b)?(c)"),  // 2
            Some(r"(a)(b)(c)"),   // 3
            Some("abc"),          // 4
            Some(r"(\d+)"),       // 5
            None,                 // 6
        ]);

        let idx_arr = Int64Array::from(vec![
            Some(1), // 0
            Some(1), // 1
            Some(2), // 2
            Some(3), // 3
            Some(0), // 4
            Some(1), // 5
            Some(1), // 6
        ]);

        let expected = StringArray::from(vec![
            Some("100"),
            Some(""),
            Some(""),
            Some("c"), // "abc", (a)(b)(c), idx=4 => (group(0) = abc, group(1)=a, group(2)=b, group(3)=c) => "c"
            Some(""),  // "xyz", "abc" => ""
            None,      // target=NULL => NULL
            None,      // pattern=NULL => NULL
        ]);

        let args = ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(Arc::new(target_arr)),
                ColumnarValue::Array(Arc::new(pattern_arr)),
                ColumnarValue::Array(Arc::new(idx_arr)),
            ],
            number_rows: 7,
            return_type: &DataType::Utf8,
        };

        let udf = RegexpExtractFunc::new();

        let res = udf.invoke_with_args(args).unwrap();
        let result_array = match res {
            ColumnarValue::Array(arr) => arr,
            _ => {
                panic!("Expected an Array result");
            }
        };

        let result = as_generic_string_array::<i32>(&result_array).unwrap();

        assert_eq!(result.len(), expected.len());
        assert_eq!(result.null_count(), expected.null_count());

        for i in 0..result.len() {
            let res_is_null = result.is_null(i);
            let exp_is_null = expected.is_null(i);
            assert_eq!(
                res_is_null,
                exp_is_null,
                "Mismatch in nullity at row {i}. result={res_is_null}, expected={exp_is_null}"
            );

            if !res_is_null {
                let res_val = result.value(i);
                let exp_val = expected.value(i);
                assert_eq!(
                    res_val, exp_val,
                    "Mismatch at row {i}. result={res_val}, expected={exp_val}"
                );
            }
        }
    }
}
