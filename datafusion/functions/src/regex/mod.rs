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

//! "regex" DataFusion functions

use arrow::array::ArrayRef;
use arrow::compute::kernels::{cmp::eq, nullif::nullif};
use datafusion_common::{Result, ScalarValue};
use std::sync::Arc;

pub(crate) use datafusion_physical_expr_common::regex::explain_regexp_kernel_error;
// The compilation of a regular expression is shared with the physical
// expressions, so that every caller reports a failure in the same way. These
// re-exports keep the paths that callers of this crate already use.
pub use datafusion_physical_expr_common::regex::{
    compile_and_cache_regex, compile_regex,
};
pub mod regexpcount;
pub mod regexpinstr;
pub mod regexplike;
pub mod regexpmatch;
pub mod regexpreplace;

/// Arrow's regex kernels treat null flags as no flags, but reject empty flags.
/// Normalize empty strings without copying the string buffers.
fn normalize_empty_flags(flags: &ArrayRef) -> Result<ArrayRef> {
    let empty =
        ScalarValue::try_from_string(String::new(), flags.data_type())?.to_scalar()?;
    Ok(nullif(flags, &eq(flags, &empty)?)?)
}

// create UDFs
make_udf_function!(regexpcount::RegexpCountFunc, regexp_count);
make_udf_function!(regexpinstr::RegexpInstrFunc, regexp_instr);
make_udf_function!(regexpmatch::RegexpMatchFunc, regexp_match);
make_udf_function!(regexplike::RegexpLikeFunc, regexp_like);
make_udf_function!(regexpreplace::RegexpReplaceFunc, regexp_replace);

#[cfg(test)]
mod strict_tests {
    use super::*;
    use arrow::datatypes::Field;
    use datafusion_common::{Result, ScalarValue};
    use datafusion_expr::{
        ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDF,
    };
    use std::sync::Arc;

    #[test]
    fn strict_regex_functions_propagate_nulls() {
        let cases = vec![
            (
                regexp_count(),
                vec![ScalarValue::from("abcabc"), ScalarValue::from("b")],
            ),
            (
                regexp_count(),
                vec![
                    ScalarValue::from("abcabc"),
                    ScalarValue::from("b"),
                    ScalarValue::from(1_i64),
                ],
            ),
            (
                regexp_count(),
                vec![
                    ScalarValue::from("abcabc"),
                    ScalarValue::from("b"),
                    ScalarValue::from(1_i64),
                    ScalarValue::from("i"),
                ],
            ),
            (
                regexp_instr(),
                vec![ScalarValue::from("abcabc"), ScalarValue::from("b")],
            ),
            (
                regexp_instr(),
                vec![
                    ScalarValue::from("abcabc"),
                    ScalarValue::from("b"),
                    ScalarValue::from(1_i64),
                ],
            ),
            (
                regexp_instr(),
                vec![
                    ScalarValue::from("abcabc"),
                    ScalarValue::from("b"),
                    ScalarValue::from(1_i64),
                    ScalarValue::from(1_i64),
                ],
            ),
            (
                regexp_instr(),
                vec![
                    ScalarValue::from("abcabc"),
                    ScalarValue::from("b"),
                    ScalarValue::from(1_i64),
                    ScalarValue::from(1_i64),
                    ScalarValue::from("i"),
                ],
            ),
            (
                regexp_instr(),
                vec![
                    ScalarValue::from("abcabc"),
                    ScalarValue::from("(b)"),
                    ScalarValue::from(1_i64),
                    ScalarValue::from(1_i64),
                    ScalarValue::from("i"),
                    ScalarValue::from(1_i64),
                ],
            ),
            (
                regexp_replace(),
                vec![
                    ScalarValue::from("abc"),
                    ScalarValue::from("b"),
                    ScalarValue::from("x"),
                ],
            ),
            (
                regexp_replace(),
                vec![
                    ScalarValue::from("abc"),
                    ScalarValue::from("b"),
                    ScalarValue::from("x"),
                    ScalarValue::from("g"),
                ],
            ),
        ];

        for (func, valid_args) in cases {
            assert!(func.is_strict(), "{} should be marked strict", func.name());

            for null_mask in 0..(1 << valid_args.len()) {
                let mut args = valid_args.clone();
                for (arg_idx, arg) in args.iter_mut().enumerate() {
                    if null_mask & (1 << arg_idx) != 0 {
                        *arg = ScalarValue::try_new_null(&arg.data_type()).unwrap();
                    }
                }

                let result =
                    invoke_with_scalar_args(&func, args).unwrap_or_else(|error| {
                        panic!(
                            "{} failed for NULL mask {null_mask:b}: {error}",
                            func.name()
                        )
                    });
                let expected_null = null_mask != 0;
                let result = result.into_array(1).unwrap();
                assert_eq!(
                    result.null_count() == result.len(),
                    expected_null,
                    "{} returned {result:?} for NULL mask {null_mask:0width$b}",
                    func.name(),
                    width = valid_args.len(),
                );
            }
        }
    }

    fn invoke_with_scalar_args(
        func: &ScalarUDF,
        args: Vec<ScalarValue>,
    ) -> Result<ColumnarValue> {
        let arg_fields = args
            .iter()
            .enumerate()
            .map(|(idx, arg)| {
                Arc::new(Field::new(
                    format!("arg_{idx}"),
                    arg.data_type(),
                    arg.is_null(),
                ))
            })
            .collect::<Vec<_>>();
        let scalar_arguments = args.iter().map(Some).collect::<Vec<_>>();
        let return_field = func.return_field_from_args(ReturnFieldArgs {
            arg_fields: &arg_fields,
            scalar_arguments: &scalar_arguments,
        })?;
        func.invoke_with_args(ScalarFunctionArgs {
            args: args.into_iter().map(ColumnarValue::Scalar).collect(),
            arg_fields,
            number_rows: 1,
            return_field,
            config_options: Arc::new(Default::default()),
        })
    }
}

pub mod expr_fn {
    use datafusion_expr::Expr;

    /// Returns the number of consecutive occurrences of a regular expression in a string.
    pub fn regexp_count(
        values: Expr,
        regex: Expr,
        start: Option<Expr>,
        flags: Option<Expr>,
    ) -> Expr {
        let mut args = vec![values, regex];
        if let Some(start) = start {
            args.push(start);
        }

        if let Some(flags) = flags {
            args.push(flags);
        }
        super::regexp_count().call(args)
    }

    /// Returns a list of regular expression matches in a string.
    pub fn regexp_match(values: Expr, regex: Expr, flags: Option<Expr>) -> Expr {
        let mut args = vec![values, regex];
        if let Some(flags) = flags {
            args.push(flags);
        }
        super::regexp_match().call(args)
    }

    /// Returns index of regular expression matches in a string.
    pub fn regexp_instr(
        values: Expr,
        regex: Expr,
        start: Option<Expr>,
        n: Option<Expr>,
        endoption: Option<Expr>,
        flags: Option<Expr>,
        subexpr: Option<Expr>,
    ) -> Expr {
        let mut args = vec![values, regex];
        if let Some(start) = start {
            args.push(start);
        }
        if let Some(n) = n {
            args.push(n);
        }
        if let Some(endoption) = endoption {
            args.push(endoption);
        }
        if let Some(flags) = flags {
            args.push(flags);
        }
        if let Some(subexpr) = subexpr {
            args.push(subexpr);
        }
        super::regexp_instr().call(args)
    }
    /// Returns true if a regex has at least one match in a string, false otherwise.
    pub fn regexp_like(values: Expr, regex: Expr, flags: Option<Expr>) -> Expr {
        let mut args = vec![values, regex];
        if let Some(flags) = flags {
            args.push(flags);
        }
        super::regexp_like().call(args)
    }

    /// Replaces substrings in a string that match.
    pub fn regexp_replace(
        string: Expr,
        pattern: Expr,
        replacement: Expr,
        flags: Option<Expr>,
    ) -> Expr {
        let mut args = vec![string, pattern, replacement];
        if let Some(flags) = flags {
            args.push(flags);
        }
        super::regexp_replace().call(args)
    }
}

/// Returns all DataFusion functions defined in this package
pub fn functions() -> Vec<Arc<datafusion_expr::ScalarUDF>> {
    vec![
        regexp_count(),
        regexp_match(),
        regexp_instr(),
        regexp_like(),
        regexp_replace(),
    ]
}

/// Maps `start`, a 1-based character position, to a byte offset in `value`.
/// Positions `1..=n` (for an `n`-character string) map to the corresponding
/// character's first byte; position `n + 1`, the end of the string, maps to
/// `value.len()`. Returns `None` for larger positions. Callers must validate
/// `start >= 1`.
pub(crate) fn start_to_byte_offset(value: &str, start: i64) -> Option<usize> {
    // If `start - 1` does not fit in `usize`, it is necessarily past the end
    // of the string.
    let start_index = usize::try_from(start - 1).ok()?;
    value
        .char_indices()
        .map(|(offset, _)| offset)
        .chain(std::iter::once(value.len()))
        .nth(start_index)
}

#[cfg(test)]
mod tests {
    use super::start_to_byte_offset;

    #[test]
    fn empty_flags_match_omitted_flags() {
        use arrow::array::StringArray;
        use arrow::compute::cast;
        use arrow::datatypes::{DataType, Field};
        use datafusion_common::config::ConfigOptions;
        use datafusion_expr::{ColumnarValue, ScalarFunctionArgs};

        use super::*;

        for udf in [regexp_like(), regexp_match()] {
            for data_type in [DataType::Utf8, DataType::LargeUtf8, DataType::Utf8View] {
                // Exercise every scalar/array combination, bypassing simplification.
                for shape in 0..8 {
                    for pattern in ["b..", "B..", "", "(b)(..)"] {
                        let args = ["foobarbaz", pattern, ""]
                            .into_iter()
                            .enumerate()
                            .map(|(i, value)| {
                                let scalar = ScalarValue::try_from_string(
                                    value.to_string(),
                                    &data_type,
                                )
                                .unwrap();
                                if shape & (1 << i) == 0 {
                                    ColumnarValue::Scalar(scalar)
                                } else {
                                    ColumnarValue::Array(
                                        scalar.to_array_of_size(3).unwrap(),
                                    )
                                }
                            })
                            .collect::<Vec<_>>();
                        let invoke = |args: Vec<ColumnarValue>| {
                            let types = args
                                .iter()
                                .map(ColumnarValue::data_type)
                                .collect::<Vec<_>>();
                            let return_type = udf.return_type(&types).unwrap();
                            udf.invoke_with_args(ScalarFunctionArgs {
                                arg_fields: types
                                    .into_iter()
                                    .map(|t| Arc::new(Field::new("arg", t, true)))
                                    .collect(),
                                args,
                                number_rows: 3,
                                return_field: Arc::new(Field::new(
                                    "result",
                                    return_type,
                                    true,
                                )),
                                config_options: Arc::new(ConfigOptions::default()),
                            })
                            .unwrap()
                            .to_array(3)
                            .unwrap()
                        };
                        assert_eq!(
                            &invoke(args.clone()),
                            &invoke(args[..2].to_vec()),
                            "{} {data_type:?} shape={shape} pattern={pattern:?}",
                            udf.name()
                        );
                    }
                }

                // Empty and null flags both mean no flags; preserve nonempty flags
                // and null values/patterns in the same batch.
                let arrays = [
                    vec![
                        Some("abc"),
                        Some("ABC"),
                        Some("ABC"),
                        None,
                        Some("abc"),
                        Some("abc"),
                    ],
                    vec![Some("a"), Some("a"), Some("a"), Some("a"), None, Some("a")],
                    vec![Some(""), Some("i"), Some(""), Some(""), Some(""), None],
                ]
                .map(|values| cast(&StringArray::from(values), &data_type).unwrap());
                let expected_flags = cast(
                    &StringArray::from(vec![None, Some("i"), None, None, None, None]),
                    &data_type,
                )
                .unwrap();
                let kernel = if udf.name() == "regexp_like" {
                    regexplike::regexp_like
                } else {
                    regexpmatch::regexp_match
                };
                let expected = kernel(&[
                    Arc::clone(&arrays[0]),
                    Arc::clone(&arrays[1]),
                    expected_flags,
                ])
                .unwrap();
                assert_eq!(&kernel(&arrays).unwrap(), &expected);
            }
        }
    }

    #[test]
    fn start_to_byte_offset_ascii() {
        assert_eq!(start_to_byte_offset("abc", 1), Some(0));
        assert_eq!(start_to_byte_offset("abc", 3), Some(2));
        // The end of the string is a valid position.
        assert_eq!(start_to_byte_offset("abc", 4), Some(3));
        assert_eq!(start_to_byte_offset("abc", 5), None);
        assert_eq!(start_to_byte_offset("abc", i64::MAX), None);
    }

    #[test]
    fn start_to_byte_offset_empty_string() {
        assert_eq!(start_to_byte_offset("", 1), Some(0));
        assert_eq!(start_to_byte_offset("", 2), None);
    }

    #[test]
    fn start_to_byte_offset_multibyte() {
        assert_eq!(start_to_byte_offset("😀a", 1), Some(0));
        assert_eq!(start_to_byte_offset("😀a", 2), Some(4));
        assert_eq!(start_to_byte_offset("😀a", 3), Some(5));
        assert_eq!(start_to_byte_offset("😀a", 4), None);
    }
}
