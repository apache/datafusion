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

use arrow::array::{Array, ArrayRef, AsArray, GenericStringArray};
use arrow::compute::kernels::regexp;
use arrow::datatypes::DataType;
use arrow::datatypes::DataType::{LargeUtf8, Utf8, Utf8View};
use datafusion_common::types::logical_string;
use datafusion_common::{
    arrow_datafusion_err, exec_err, internal_err, plan_err, Result, ScalarValue,
};
use datafusion_expr::{
    binary_expr, cast, Coercion, ColumnarValue, Documentation, Expr, ScalarUDFImpl,
    Signature, TypeSignature, TypeSignatureClass, Volatility,
};
use datafusion_macros::user_doc;

use datafusion_expr::simplify::{ExprSimplifyResult, SimplifyInfo};
use datafusion_expr_common::operator::Operator;
use datafusion_expr_common::type_coercion::binary::BinaryTypeCoercer;
use std::any::Any;
use std::sync::Arc;

#[user_doc(
    doc_section(label = "Regular Expression Functions"),
    description = "Returns true if a [regular expression](https://docs.rs/regex/latest/regex/#syntax) has at least one match in a string, false otherwise.",
    syntax_example = "regexp_like(str, regexp[, flags])",
    sql_example = r#"```sql
select regexp_like('Köln', '[a-zA-Z]ö[a-zA-Z]{2}');
+--------------------------------------------------------+
| regexp_like(Utf8("Köln"),Utf8("[a-zA-Z]ö[a-zA-Z]{2}")) |
+--------------------------------------------------------+
| true                                                   |
+--------------------------------------------------------+
SELECT regexp_like('aBc', '(b|d)', 'i');
+--------------------------------------------------+
| regexp_like(Utf8("aBc"),Utf8("(b|d)"),Utf8("i")) |
+--------------------------------------------------+
| true                                             |
+--------------------------------------------------+
```
Additional examples can be found [here](https://github.com/apache/datafusion/blob/main/datafusion-examples/examples/builtin_functions/regexp.rs)
"#,
    standard_argument(name = "str", prefix = "String"),
    standard_argument(name = "regexp", prefix = "Regular"),
    argument(
        name = "flags",
        description = r#"Optional regular expression flags that control the behavior of the regular expression. The following flags are supported:
  - **i**: case-insensitive: letters match both upper and lower case
  - **m**: multi-line mode: ^ and $ match begin/end of line
  - **s**: allow . to match \n
  - **R**: enables CRLF mode: when multi-line mode is enabled, \r\n is used
  - **U**: swap the meaning of x* and x*?"#
    )
)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct RegexpLikeFunc {
    signature: Signature,
}

impl Default for RegexpLikeFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl RegexpLikeFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![
                    TypeSignature::Coercible(vec![
                        Coercion::new_exact(TypeSignatureClass::Native(logical_string())),
                        Coercion::new_exact(TypeSignatureClass::Native(logical_string())),
                    ]),
                    TypeSignature::Coercible(vec![
                        Coercion::new_exact(TypeSignatureClass::Native(logical_string())),
                        Coercion::new_exact(TypeSignatureClass::Native(logical_string())),
                        Coercion::new_exact(TypeSignatureClass::Native(logical_string())),
                    ]),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for RegexpLikeFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "regexp_like"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        use DataType::*;

        Ok(match &arg_types[0] {
            Null => Null,
            // Type coercion is done by DataFusion based on signature, so if we
            // get here, the first argument is always a string
            _ => Boolean,
        })
    }

    fn invoke_with_args(
        &self,
        args: datafusion_expr::ScalarFunctionArgs,
    ) -> Result<ColumnarValue> {
        let args = &args.args;

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
            .map(|arg| arg.to_array(inferred_length))
            .collect::<Result<Vec<_>>>()?;

        let result = regexp_like(&args);
        if is_scalar {
            // If all inputs are scalar, keeps output as scalar
            let result = result.and_then(|arr| ScalarValue::try_from_array(&arr, 0));
            result.map(ColumnarValue::Scalar)
        } else {
            result.map(ColumnarValue::Array)
        }
    }

    fn simplify(
        &self,
        mut args: Vec<Expr>,
        info: &dyn SimplifyInfo,
    ) -> Result<ExprSimplifyResult> {
        // Try to simplify regexp_like usage to one of the builtin operators since those have
        // optimized code paths for the case where the regular expression pattern is a scalar.
        // Additionally, the expression simplification optimization pass will attempt to further
        // simplify regular expression patterns used in operator expressions.
        let Some(op) = derive_operator(&args) else {
            return Ok(ExprSimplifyResult::Original(args));
        };

        let string_type = info.get_data_type(&args[0])?;
        let regexp_type = info.get_data_type(&args[1])?;
        let binary_type_coercer = BinaryTypeCoercer::new(&string_type, &op, &regexp_type);
        let Ok((coerced_string_type, coerced_regexp_type)) =
            binary_type_coercer.get_input_types()
        else {
            return Ok(ExprSimplifyResult::Original(args));
        };

        // regexp_like(str, regexp [, flags])
        let regexp = args.swap_remove(1);
        let string = args.swap_remove(0);

        Ok(ExprSimplifyResult::Simplified(binary_expr(
            if string_type != coerced_string_type {
                cast(string, coerced_string_type)
            } else {
                string
            },
            op,
            if regexp_type != coerced_regexp_type {
                cast(regexp, coerced_regexp_type)
            } else {
                regexp
            },
        )))
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

fn derive_operator(args: &[Expr]) -> Option<Operator> {
    match args.len() {
        // regexp_like(str, regexp, flags)
        3 => {
            match &args[2] {
                Expr::Literal(ScalarValue::Utf8(Some(flags)), _) => {
                    match flags.as_str() {
                        "i" => Some(Operator::RegexIMatch),
                        "" => Some(Operator::RegexMatch),
                        // Any flags besides 'i' have no operator equivalent
                        _ => None,
                    }
                }
                // `flags` is not a literal, so we can't derive the correct operator statically
                _ => None,
            }
        }
        // regexp_like(str, regexp)
        2 => Some(Operator::RegexMatch),
        // Should never happen, but just in case
        _ => None,
    }
}

/// Tests a string using a regular expression returning true if at
/// least one match, false otherwise.
///
/// The full list of supported features and syntax can be found at
/// <https://docs.rs/regex/latest/regex/#syntax>
///
/// Supported flags can be found at
/// <https://docs.rs/regex/latest/regex/#grouping-and-flags>
///
/// # Examples
///
/// ```ignore
/// # use datafusion::prelude::*;
/// # use datafusion::error::Result;
/// # #[tokio::main]
/// # async fn main() -> Result<()> {
/// let ctx = SessionContext::new();
/// let df = ctx.read_csv("tests/data/regex.csv", CsvReadOptions::new()).await?;
///
/// // use the regexp_like function to test col 'values',
/// // against patterns in col 'patterns' without flags
/// let df = df.with_column(
///     "a",
///     regexp_like(vec![col("values"), col("patterns")])
/// )?;
/// // use the regexp_like function to test col 'values',
/// // against patterns in col 'patterns' with flags
/// let df = df.with_column(
///     "b",
///     regexp_like(vec![col("values"), col("patterns"), col("flags")])
/// )?;
/// // literals can be used as well with dataframe calls
/// let df = df.with_column(
///     "c",
///     regexp_like(vec![lit("foobarbequebaz"), lit("(bar)(beque)")])
/// )?;
///
/// df.show().await?;
///
/// # Ok(())
/// # }
/// ```
pub fn regexp_like(args: &[ArrayRef]) -> Result<ArrayRef> {
    match args.len() {
        2 => handle_regexp_like(&args[0], &args[1], None),
        3 => {
            let flags = match args[2].data_type() {
                Utf8 => args[2].as_string::<i32>(),
                LargeUtf8 => {
                    let large_string_array = args[2].as_string::<i64>();
                    let string_vec: Vec<Option<&str>> = (0..large_string_array.len()).map(|i| {
                        if large_string_array.is_null(i) {
                            None
                        } else {
                            Some(large_string_array.value(i))
                        }
                    })
                    .collect();

                    &GenericStringArray::<i32>::from(string_vec)
                },
                _ => {
                    let string_view_array = args[2].as_string_view();
                    let string_vec: Vec<Option<String>> = (0..string_view_array.len()).map(|i| {
                        if string_view_array.is_null(i) {
                            None
                        } else {
                            Some(string_view_array.value(i).to_string())
                        }
                    })
                    .collect();
                    &GenericStringArray::<i32>::from(string_vec)
                },
            };

            if flags.iter().any(|s| s == Some("g")) {
                return plan_err!("regexp_like() does not support the \"global\" option");
            }

            handle_regexp_like(&args[0], &args[1], Some(flags))
        },
        other => exec_err!(
            "`regexp_like` was called with {other} arguments. It requires at least 2 and at most 3."
        ),
    }
}

fn handle_regexp_like(
    values: &ArrayRef,
    patterns: &ArrayRef,
    flags: Option<&GenericStringArray<i32>>,
) -> Result<ArrayRef> {
    let array = match (values.data_type(), patterns.data_type()) {
        (Utf8View, Utf8) => {
            let value = values.as_string_view();
            let pattern = patterns.as_string::<i32>();

            regexp::regexp_is_match(value, pattern, flags)
                .map_err(|e| arrow_datafusion_err!(e))?
        }
        (Utf8View, Utf8View) => {
            let value = values.as_string_view();
            let pattern = patterns.as_string_view();

            regexp::regexp_is_match(value, pattern, flags)
                .map_err(|e| arrow_datafusion_err!(e))?
        }
        (Utf8View, LargeUtf8) => {
            let value = values.as_string_view();
            let pattern = patterns.as_string::<i64>();

            regexp::regexp_is_match(value, pattern, flags)
                .map_err(|e| arrow_datafusion_err!(e))?
        }
        (Utf8, Utf8) => {
            let value = values.as_string::<i32>();
            let pattern = patterns.as_string::<i32>();

            regexp::regexp_is_match(value, pattern, flags)
                .map_err(|e| arrow_datafusion_err!(e))?
        }
        (Utf8, Utf8View) => {
            let value = values.as_string::<i32>();
            let pattern = patterns.as_string_view();

            regexp::regexp_is_match(value, pattern, flags)
                .map_err(|e| arrow_datafusion_err!(e))?
        }
        (Utf8, LargeUtf8) => {
            let value = values.as_string_view();
            let pattern = patterns.as_string::<i64>();

            regexp::regexp_is_match(value, pattern, flags)
                .map_err(|e| arrow_datafusion_err!(e))?
        }
        (LargeUtf8, Utf8) => {
            let value = values.as_string::<i64>();
            let pattern = patterns.as_string::<i32>();

            regexp::regexp_is_match(value, pattern, flags)
                .map_err(|e| arrow_datafusion_err!(e))?
        }
        (LargeUtf8, Utf8View) => {
            let value = values.as_string::<i64>();
            let pattern = patterns.as_string_view();

            regexp::regexp_is_match(value, pattern, flags)
                .map_err(|e| arrow_datafusion_err!(e))?
        }
        (LargeUtf8, LargeUtf8) => {
            let value = values.as_string::<i64>();
            let pattern = patterns.as_string::<i64>();

            regexp::regexp_is_match(value, pattern, flags)
                .map_err(|e| arrow_datafusion_err!(e))?
        }
        other => {
            return internal_err!(
                "Unsupported data type {other:?} for function `regexp_like`"
            )
        }
    };

    Ok(Arc::new(array) as ArrayRef)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::StringArray;
    use arrow::array::{BooleanBuilder, StringViewArray};

    use crate::regex::regexplike::regexp_like;

    #[test]
    fn test_case_sensitive_regexp_like_utf8() {
        let values = StringArray::from(vec!["abc"; 5]);

        let patterns =
            StringArray::from(vec!["^(a)", "^(A)", "(b|d)", "(B|D)", "^(b|c)"]);

        let mut expected_builder: BooleanBuilder = BooleanBuilder::new();
        expected_builder.append_value(true);
        expected_builder.append_value(false);
        expected_builder.append_value(true);
        expected_builder.append_value(false);
        expected_builder.append_value(false);
        let expected = expected_builder.finish();

        let re = regexp_like(&[Arc::new(values), Arc::new(patterns)]).unwrap();

        assert_eq!(re.as_ref(), &expected);
    }

    #[test]
    fn test_case_sensitive_regexp_like_utf8view() {
        let values = StringViewArray::from(vec!["abc"; 5]);

        let patterns =
            StringArray::from(vec!["^(a)", "^(A)", "(b|d)", "(B|D)", "^(b|c)"]);

        let mut expected_builder: BooleanBuilder = BooleanBuilder::new();
        expected_builder.append_value(true);
        expected_builder.append_value(false);
        expected_builder.append_value(true);
        expected_builder.append_value(false);
        expected_builder.append_value(false);
        let expected = expected_builder.finish();

        let re = regexp_like(&[Arc::new(values), Arc::new(patterns)]).unwrap();

        assert_eq!(re.as_ref(), &expected);
    }

    #[test]
    fn test_case_insensitive_regexp_like_utf8() {
        let values = StringArray::from(vec!["abc"; 5]);
        let patterns =
            StringArray::from(vec!["^(a)", "^(A)", "(b|d)", "(B|D)", "^(b|c)"]);
        let flags = StringArray::from(vec!["i"; 5]);

        let mut expected_builder: BooleanBuilder = BooleanBuilder::new();
        expected_builder.append_value(true);
        expected_builder.append_value(true);
        expected_builder.append_value(true);
        expected_builder.append_value(true);
        expected_builder.append_value(false);
        let expected = expected_builder.finish();

        let re = regexp_like(&[Arc::new(values), Arc::new(patterns), Arc::new(flags)])
            .unwrap();

        assert_eq!(re.as_ref(), &expected);
    }

    #[test]
    fn test_case_insensitive_regexp_like_utf8view() {
        let values = StringViewArray::from(vec!["abc"; 5]);
        let patterns =
            StringViewArray::from(vec!["^(a)", "^(A)", "(b|d)", "(B|D)", "^(b|c)"]);
        let flags = StringArray::from(vec!["i"; 5]);

        let mut expected_builder: BooleanBuilder = BooleanBuilder::new();
        expected_builder.append_value(true);
        expected_builder.append_value(true);
        expected_builder.append_value(true);
        expected_builder.append_value(true);
        expected_builder.append_value(false);
        let expected = expected_builder.finish();

        let re = regexp_like(&[Arc::new(values), Arc::new(patterns), Arc::new(flags)])
            .unwrap();

        assert_eq!(re.as_ref(), &expected);
    }

    #[test]
    fn test_unsupported_global_flag_regexp_like() {
        let values = StringArray::from(vec!["abc"]);
        let patterns = StringArray::from(vec!["^(a)"]);
        let flags = StringArray::from(vec!["g"]);

        let re_err =
            regexp_like(&[Arc::new(values), Arc::new(patterns), Arc::new(flags)])
                .expect_err("unsupported flag should have failed");

        assert_eq!(
            re_err.strip_backtrace(),
            "Error during planning: regexp_like() does not support the \"global\" option"
        );
    }
}
