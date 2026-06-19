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

//! [`JsonGetStr`] UDF implementation for extracting string values from JSON.

use arrow::array::{Array, AsArray, StringArray, StringBuilder};
use arrow::datatypes::DataType;
use datafusion_common::{Result, ScalarValue, exec_err, plan_err};
use datafusion_expr::{
    ColumnarValue, Documentation, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    TypeSignature, Volatility,
};
use datafusion_macros::user_doc;
use std::sync::Arc;

#[user_doc(
    doc_section(label = "JSON Functions"),
    description = r#"Extract a string value from a JSON string at the given path.

The path is specified as zero or more keys (strings for object access) or
indices (integers for array access). With no path keys, the function returns
the JSON value itself if it is a string (jq `.` semantics). Otherwise, the
function navigates into the JSON document and returns the value at the path.

Returns NULL if the input JSON is not a valid JSON string, the path does not
exist, the value at the path is not a string, or any argument (input JSON or
path key) is NULL. Invalid JSON is silently treated as NULL — no error is
returned."#,
    syntax_example = "json_get_str(json_string [, key1, key2, ...])",
    sql_example = r#"```sql
> select json_get_str('{"a": {"b": "hello"}}', 'a', 'b');
+-----------------------------------------------------------+
| json_get_str(Utf8("{"a": {"b": "hello"}}"),Utf8("a"),Utf8("b")) |
+-----------------------------------------------------------+
| hello                                                     |
+-----------------------------------------------------------+
```"#,
    argument(
        name = "json_string",
        description = "A string containing valid JSON data."
    ),
    argument(
        name = "keys",
        description = "Zero or more path keys (string for object key, integer for array index)."
    )
)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct JsonGetStr {
    signature: Signature,
}

impl Default for JsonGetStr {
    fn default() -> Self {
        Self::new()
    }
}

impl JsonGetStr {
    pub fn new() -> Self {
        Self {
            signature: Signature::new(TypeSignature::UserDefined, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for JsonGetStr {
    fn name(&self) -> &str {
        "json_get_str"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.is_empty() {
            return plan_err!(
                "json_get_str requires at least 1 argument (json_string), got 0"
            );
        }
        // First arg must be a string type; remaining are path keys (string or integer)
        let json_type = match &arg_types[0] {
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
                arg_types[0].clone()
            }
            DataType::Null => DataType::Utf8,
            other => {
                return plan_err!(
                    "json_get_str first argument must be a string type, got {other}"
                );
            }
        };
        let mut coerced = vec![json_type];
        for (i, dt) in arg_types[1..].iter().enumerate() {
            let coerced_type = match dt {
                DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => dt.clone(),
                DataType::Int8
                | DataType::Int16
                | DataType::Int32
                | DataType::Int64
                | DataType::UInt8
                | DataType::UInt16
                | DataType::UInt32
                | DataType::UInt64 => dt.clone(),
                DataType::Null => DataType::Utf8,
                other => {
                    return plan_err!(
                        "json_get_str path argument {} must be a string or integer type, got {other}",
                        i + 1
                    );
                }
            };
            coerced.push(coerced_type);
        }
        Ok(coerced)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let json_arg = &args.args[0];
        let path_args = &args.args[1..];

        // Build path keys. If any path argument is NULL, the result is NULL —
        // return early on the first NULL encountered without scanning the rest.
        let mut path_keys: Vec<PathKey<'_>> = Vec::with_capacity(path_args.len());
        for arg in path_args {
            let key = match arg {
                ColumnarValue::Scalar(
                    ScalarValue::Utf8(Some(s))
                    | ScalarValue::LargeUtf8(Some(s))
                    | ScalarValue::Utf8View(Some(s)),
                ) => PathKey::Key(s.as_str()),
                ColumnarValue::Scalar(ScalarValue::Int64(Some(i))) => {
                    PathKey::Index(*i as usize)
                }
                ColumnarValue::Scalar(ScalarValue::Int32(Some(i))) => {
                    PathKey::Index(*i as usize)
                }
                ColumnarValue::Scalar(ScalarValue::UInt64(Some(i))) => {
                    PathKey::Index(*i as usize)
                }
                ColumnarValue::Scalar(s) if s.is_null() => {
                    return Ok(null_result(json_arg));
                }
                _ => {
                    return exec_err!(
                        "json_get_str path arguments must be scalar strings or integers, got {:?}",
                        arg.data_type()
                    );
                }
            };
            path_keys.push(key);
        }

        match json_arg {
            ColumnarValue::Array(array) => {
                let len = array.len();
                let mut builder = StringBuilder::with_capacity(len, len * 32);

                match array.data_type() {
                    DataType::Utf8 => {
                        let arr = array.as_string::<i32>();
                        for i in 0..len {
                            if arr.is_null(i) {
                                builder.append_null();
                            } else {
                                match extract_str_at_path(arr.value(i), &path_keys) {
                                    Some(s) => builder.append_value(s),
                                    None => builder.append_null(),
                                }
                            }
                        }
                    }
                    DataType::LargeUtf8 => {
                        let arr = array.as_string::<i64>();
                        for i in 0..len {
                            if arr.is_null(i) {
                                builder.append_null();
                            } else {
                                match extract_str_at_path(arr.value(i), &path_keys) {
                                    Some(s) => builder.append_value(s),
                                    None => builder.append_null(),
                                }
                            }
                        }
                    }
                    DataType::Utf8View => {
                        let arr = array.as_string_view();
                        for i in 0..len {
                            if arr.is_null(i) {
                                builder.append_null();
                            } else {
                                match extract_str_at_path(arr.value(i), &path_keys) {
                                    Some(s) => builder.append_value(s),
                                    None => builder.append_null(),
                                }
                            }
                        }
                    }
                    other => {
                        return exec_err!(
                            "json_get_str first argument must be a string type, got {other:?}"
                        );
                    }
                }

                Ok(ColumnarValue::Array(Arc::new(builder.finish())))
            }
            ColumnarValue::Scalar(scalar) => {
                let json_str = match scalar {
                    ScalarValue::Utf8(Some(s))
                    | ScalarValue::LargeUtf8(Some(s))
                    | ScalarValue::Utf8View(Some(s)) => s,
                    _ => return Ok(ColumnarValue::Scalar(ScalarValue::Utf8(None))),
                };

                let result = extract_str_at_path(json_str, &path_keys);
                Ok(ColumnarValue::Scalar(ScalarValue::Utf8(result)))
            }
        }
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

/// Represents a path element for navigating JSON. Borrows from the input
/// arguments to avoid per-call clones of path strings.
#[derive(Debug, Clone, Copy)]
enum PathKey<'a> {
    Key(&'a str),
    Index(usize),
}

/// Helper: build the NULL-typed result matching the input shape.
fn null_result(json_arg: &ColumnarValue) -> ColumnarValue {
    match json_arg {
        ColumnarValue::Array(arr) => {
            ColumnarValue::Array(Arc::new(StringArray::new_null(arr.len())))
        }
        ColumnarValue::Scalar(_) => ColumnarValue::Scalar(ScalarValue::Utf8(None)),
    }
}

/// Navigate a JSON string using the given path and extract a string value.
///
/// Returns `None` (silently) if:
/// - The input is not valid JSON
/// - The path does not exist in the JSON document
/// - The value at the path is not a JSON string
///
/// With an empty `path`, returns the JSON value itself if it is a string
/// (jq `.` semantics).
fn extract_str_at_path(json_str: &str, path: &[PathKey<'_>]) -> Option<String> {
    let root: serde_json::Value = serde_json::from_str(json_str).ok()?;
    path.iter()
        .try_fold(&root, |value, key| match key {
            PathKey::Key(k) => value.get(*k),
            PathKey::Index(i) => value.get(*i),
        })?
        .as_str()
        .map(ToOwned::to_owned)
}

/// Return a [`ScalarUDF`](datafusion_expr::ScalarUDF) implementation of `json_get_str`
pub fn json_get_str_udf() -> Arc<datafusion_expr::ScalarUDF> {
    static INSTANCE: std::sync::LazyLock<Arc<datafusion_expr::ScalarUDF>> =
        std::sync::LazyLock::new(|| {
            Arc::new(datafusion_expr::ScalarUDF::new_from_impl(JsonGetStr::new()))
        });
    Arc::clone(&INSTANCE)
}

/// Create an [`Expr`](datafusion_expr::Expr) that calls `json_get_str`
pub fn json_get_str(args: Vec<datafusion_expr::Expr>) -> datafusion_expr::Expr {
    datafusion_expr::Expr::ScalarFunction(datafusion_expr::expr::ScalarFunction::new_udf(
        json_get_str_udf(),
        args,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::StringArray;
    use arrow::datatypes::Field;
    use datafusion_common::ScalarValue;
    use datafusion_common::config::ConfigOptions;
    use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl};

    fn invoke_json_get_str(
        json_values: ColumnarValue,
        keys: Vec<ColumnarValue>,
        num_rows: usize,
    ) -> Result<ColumnarValue> {
        let udf = JsonGetStr::new();
        let mut args = vec![json_values];
        args.extend(keys);
        let arg_fields: Vec<_> = args
            .iter()
            .map(|a| Field::new("a", a.data_type(), true).into())
            .collect();

        udf.invoke_with_args(ScalarFunctionArgs {
            args,
            arg_fields,
            number_rows: num_rows,
            return_field: Field::new("f", DataType::Utf8, true).into(),
            config_options: Arc::new(ConfigOptions::default()),
        })
    }

    #[test]
    fn test_simple_object_key() -> Result<()> {
        let json = ColumnarValue::Scalar(ScalarValue::Utf8(Some(
            r#"{"name": "DataFusion"}"#.to_string(),
        )));
        let key = ColumnarValue::Scalar(ScalarValue::Utf8(Some("name".to_string())));

        let result = invoke_json_get_str(json, vec![key], 1)?;
        match result {
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))) => {
                assert_eq!(s, "DataFusion");
            }
            other => panic!("expected Utf8 scalar, got {other:?}"),
        }
        Ok(())
    }

    #[test]
    fn test_nested_path() -> Result<()> {
        let json = ColumnarValue::Scalar(ScalarValue::Utf8(Some(
            r#"{"a": {"b": {"c": "deep"}}}"#.to_string(),
        )));
        let keys = vec![
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("a".to_string()))),
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("b".to_string()))),
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("c".to_string()))),
        ];

        let result = invoke_json_get_str(json, keys, 1)?;
        match result {
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))) => {
                assert_eq!(s, "deep");
            }
            other => panic!("expected Utf8 scalar, got {other:?}"),
        }
        Ok(())
    }

    #[test]
    fn test_array_index() -> Result<()> {
        let json = ColumnarValue::Scalar(ScalarValue::Utf8(Some(
            r#"{"items": ["zero", "one", "two"]}"#.to_string(),
        )));
        let keys = vec![
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("items".to_string()))),
            ColumnarValue::Scalar(ScalarValue::Int64(Some(1))),
        ];

        let result = invoke_json_get_str(json, keys, 1)?;
        match result {
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))) => {
                assert_eq!(s, "one");
            }
            other => panic!("expected Utf8 scalar, got {other:?}"),
        }
        Ok(())
    }

    #[test]
    fn test_missing_key_returns_null() -> Result<()> {
        let json = ColumnarValue::Scalar(ScalarValue::Utf8(Some(
            r#"{"a": "hello"}"#.to_string(),
        )));
        let key =
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("nonexistent".to_string())));

        let result = invoke_json_get_str(json, vec![key], 1)?;
        match result {
            ColumnarValue::Scalar(ScalarValue::Utf8(None)) => {}
            other => panic!("expected null Utf8 scalar, got {other:?}"),
        }
        Ok(())
    }

    #[test]
    fn test_non_string_value_returns_null() -> Result<()> {
        let json = ColumnarValue::Scalar(ScalarValue::Utf8(Some(
            r#"{"count": 42}"#.to_string(),
        )));
        let key = ColumnarValue::Scalar(ScalarValue::Utf8(Some("count".to_string())));

        let result = invoke_json_get_str(json, vec![key], 1)?;
        match result {
            ColumnarValue::Scalar(ScalarValue::Utf8(None)) => {}
            other => {
                panic!("expected null Utf8 scalar (non-string JSON value), got {other:?}")
            }
        }
        Ok(())
    }

    #[test]
    fn test_invalid_json_returns_null() -> Result<()> {
        let json =
            ColumnarValue::Scalar(ScalarValue::Utf8(Some("not valid json".to_string())));
        let key = ColumnarValue::Scalar(ScalarValue::Utf8(Some("a".to_string())));

        let result = invoke_json_get_str(json, vec![key], 1)?;
        match result {
            ColumnarValue::Scalar(ScalarValue::Utf8(None)) => {}
            other => panic!("expected null Utf8 scalar (invalid json), got {other:?}"),
        }
        Ok(())
    }

    #[test]
    fn test_null_json_returns_null() -> Result<()> {
        let json = ColumnarValue::Scalar(ScalarValue::Utf8(None));
        let key = ColumnarValue::Scalar(ScalarValue::Utf8(Some("a".to_string())));

        let result = invoke_json_get_str(json, vec![key], 1)?;
        match result {
            ColumnarValue::Scalar(ScalarValue::Utf8(None)) => {}
            other => panic!("expected null Utf8 scalar, got {other:?}"),
        }
        Ok(())
    }

    #[test]
    fn test_null_path_arg_returns_null() -> Result<()> {
        let json =
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(r#"{"a": "hi"}"#.to_string())));
        let key = ColumnarValue::Scalar(ScalarValue::Utf8(None));

        let result = invoke_json_get_str(json, vec![key], 1)?;
        match result {
            ColumnarValue::Scalar(ScalarValue::Utf8(None)) => {}
            other => panic!("expected null Utf8 scalar, got {other:?}"),
        }
        Ok(())
    }

    #[test]
    fn test_empty_path_returns_string_value() -> Result<()> {
        // jq `.` semantics: with no path keys, return the JSON value itself
        // if it is a string; otherwise null.
        let json = ColumnarValue::Scalar(ScalarValue::Utf8(Some(r#""hello""#.to_string())));
        let result = invoke_json_get_str(json, vec![], 1)?;
        match result {
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))) => assert_eq!(s, "hello"),
            other => panic!("expected Utf8 scalar 'hello', got {other:?}"),
        }
        Ok(())
    }

    #[test]
    fn test_empty_path_non_string_returns_null() -> Result<()> {
        let json = ColumnarValue::Scalar(ScalarValue::Utf8(Some("42".to_string())));
        let result = invoke_json_get_str(json, vec![], 1)?;
        match result {
            ColumnarValue::Scalar(ScalarValue::Utf8(None)) => {}
            other => panic!("expected null Utf8 scalar, got {other:?}"),
        }
        Ok(())
    }

    #[test]
    fn test_array_input() -> Result<()> {
        let json_array = ColumnarValue::Array(Arc::new(StringArray::from(vec![
            Some(r#"{"name": "Alice"}"#),
            Some(r#"{"name": "Bob"}"#),
            None,
            Some(r#"{"other": "value"}"#),
        ])));
        let key = ColumnarValue::Scalar(ScalarValue::Utf8(Some("name".to_string())));

        let result = invoke_json_get_str(json_array, vec![key], 4)?;
        match result {
            ColumnarValue::Array(arr) => {
                let string_arr = arr.as_any().downcast_ref::<StringArray>().unwrap();
                assert_eq!(string_arr.len(), 4);
                assert_eq!(string_arr.value(0), "Alice");
                assert_eq!(string_arr.value(1), "Bob");
                assert!(string_arr.is_null(2));
                assert!(string_arr.is_null(3)); // key "name" not found
            }
            other => panic!("expected array result, got {other:?}"),
        }
        Ok(())
    }
}
