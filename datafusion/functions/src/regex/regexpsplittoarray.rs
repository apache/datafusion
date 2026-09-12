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

//! Split strings into lists using regular-expression delimiters.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayBuilder, ArrayRef, AsArray, LargeStringBuilder, ListBuilder,
    StringArrayType, StringBuilder, StringViewBuilder, new_null_array,
};
use arrow::datatypes::{DataType, Field};
use datafusion_common::{Result, ScalarValue, exec_err};
use datafusion_expr::{
    ColumnarValue, Documentation, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    TypeSignature, Volatility,
};
use datafusion_macros::user_doc;

use super::{compile_and_cache_regex, compile_regex};

#[user_doc(
    doc_section(label = "Regular Expression Functions"),
    description = "Splits a string into an array using a [regular expression](https://docs.rs/regex/latest/regex/#syntax) as the delimiter. Zero-length matches at the beginning or end of the string, or immediately after a previous match, are ignored. A NULL argument returns NULL without validating the delimiter or flags. Uses DataFusion's regular expression engine, whose syntax, flags, and alternation matching differ from PostgreSQL; for example, `a|ab` matches `a` first rather than the longest alternative `ab`.",
    syntax_example = "regexp_split_to_array(str, regexp[, flags])",
    standard_argument(name = "str", prefix = "String"),
    argument(
        name = "regexp",
        description = "Regular expression to use as the delimiter. Can be a constant, column, or function."
    ),
    argument(
        name = "flags",
        description = "Optional regular expression flags. Refer to the flags reference above for supported flags. The global flag 'g' is not supported."
    ),
    sql_example = r#"```sql
> SELECT regexp_split_to_array('one,two;three', '[,;]') AS parts;
+-------------------+
| parts             |
+-------------------+
| [one, two, three] |
+-------------------+
```"#
)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct RegexpSplitToArrayFunc {
    signature: Signature,
}

impl Default for RegexpSplitToArrayFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl RegexpSplitToArrayFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![TypeSignature::String(2), TypeSignature::String(3)],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for RegexpSplitToArrayFunc {
    fn name(&self) -> &str {
        "regexp_split_to_array"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        match arg_types.first() {
            Some(kind @ (DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View)) => {
                Ok(DataType::List(Arc::new(Field::new_list_field(
                    kind.clone(),
                    true,
                ))))
            }
            _ => exec_err!("regexp_split_to_array requires string arguments"),
        }
    }

    fn is_strict(&self) -> bool {
        true
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        if !(2..=3).contains(&args.args.len()) {
            return exec_err!("regexp_split_to_array requires 2 or 3 arguments");
        }
        let array_length = args.args.iter().find_map(|arg| match arg {
            ColumnarValue::Array(array) => Some(array.len()),
            ColumnarValue::Scalar(_) => None,
        });
        let row_count = array_length.unwrap_or(1);
        for arg in &args.args {
            if let ColumnarValue::Array(array) = arg
                && array.len() != row_count
            {
                return exec_err!(
                    "regexp_split_to_array arguments must have matching array lengths"
                );
            }
        }
        let result = if args
            .args
            .iter()
            .any(|arg| matches!(arg, ColumnarValue::Scalar(value) if value.is_null()))
        {
            new_null_array(args.return_type(), row_count)
        } else {
            // Keep scalar arguments at length one, rather than copying a literal
            // pattern or string once per row. The kernel broadcasts these values.
            let arrays = args
                .args
                .iter()
                .map(|arg| match arg {
                    ColumnarValue::Scalar(value) => value.to_array_of_size(1),
                    ColumnarValue::Array(array) => Ok(Arc::clone(array)),
                })
                .collect::<Result<Vec<_>>>()?;
            split_arrays(&arrays, row_count)?
        };
        if array_length.is_none() {
            Ok(ColumnarValue::Scalar(ScalarValue::try_from_array(
                &result, 0,
            )?))
        } else {
            Ok(ColumnarValue::Array(result))
        }
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

fn split_arrays(args: &[ArrayRef], row_count: usize) -> Result<ArrayRef> {
    let kind = args[0].data_type();
    if args.iter().any(|arg| arg.data_type() != kind) {
        return exec_err!("regexp_split_to_array requires matching string types");
    }
    macro_rules! dispatch {
        ($accessor:ident $(::<$offset:ty>)?, $builder:expr, $byte_limit:expr) => {
            split_inner(
                args[0].$accessor $(::<$offset>)?(),
                args[1].$accessor $(::<$offset>)?(),
                args.get(2).map(|arg| arg.$accessor $(::<$offset>)?()),
                row_count,
                $builder,
                $byte_limit,
                |builder, value| builder.append_value(value),
            )
        };
    }
    match kind {
        DataType::Utf8 => {
            dispatch!(as_string::<i32>, StringBuilder::new(), i32::MAX as usize)
        }
        DataType::LargeUtf8 => dispatch!(
            as_string::<i64>,
            LargeStringBuilder::new(),
            usize::try_from(i64::MAX).unwrap_or(usize::MAX)
        ),
        DataType::Utf8View => {
            dispatch!(as_string_view, StringViewBuilder::new(), usize::MAX)
        }
        _ => exec_err!("regexp_split_to_array requires string arguments"),
    }
}

fn split_inner<'a, S: StringArrayType<'a> + Copy, B: ArrayBuilder>(
    values: S,
    patterns: S,
    flags: Option<S>,
    row_count: usize,
    builder: B,
    byte_limit: usize,
    mut append_value: impl FnMut(&mut B, &str),
) -> Result<ArrayRef> {
    let mut output = ListBuilder::with_capacity(builder, row_count);
    let mut cache = HashMap::new();
    let constant_pattern =
        patterns.len() == 1 && flags.is_none_or(|array| array.len() == 1);
    let mut constant_regex = None;
    let mut size = SplitOutputSize {
        bytes: 0,
        elements: 0,
        byte_limit,
    };
    for row in 0..row_count {
        let value_index = if values.len() == 1 { 0 } else { row };
        let pattern_index = if patterns.len() == 1 { 0 } else { row };
        let flag_index = flags
            .as_ref()
            .map(|array| if array.len() == 1 { 0 } else { row });
        if values.is_null(value_index)
            || patterns.is_null(pattern_index)
            || flags
                .as_ref()
                .zip(flag_index)
                .is_some_and(|(array, index)| array.is_null(index))
        {
            output.append(false);
            continue;
        }
        let value = values.value(value_index);
        let pattern = patterns.value(pattern_index);
        let flags = flags
            .as_ref()
            .zip(flag_index)
            .map(|(array, index)| array.value(index));
        // Compile a constant delimiter once, lazily: NULL rows and empty
        // batches must not evaluate an otherwise unused invalid delimiter.
        // Unlike regexp_like's eager scalar flag validation, this deliberately
        // follows PostgreSQL's strict NULL behavior for splitting.
        let regex = if constant_pattern {
            match constant_regex {
                Some(ref regex) => regex,
                None => {
                    validate_flags(flags)?;
                    constant_regex.insert(compile_regex(pattern, flags)?)
                }
            }
        } else {
            validate_flags(flags)?;
            compile_and_cache_regex(pattern, flags, &mut cache)?
        };
        let mut end = 0;
        for matched in regex.find_iter(value) {
            // PostgreSQL's split semantics exclude zero-width delimiters at
            // the edges and immediately after the previous delimiter.
            if matched.is_empty()
                && (matched.start() == 0
                    || matched.start() == value.len()
                    || matched.start() == end)
            {
                continue;
            }
            size.add(matched.start() - end)?;
            append_value(output.values(), &value[end..matched.start()]);
            end = matched.end();
        }
        size.add(value.len() - end)?;
        append_value(output.values(), &value[end..]);
        output.append(true);
    }
    Ok(Arc::new(output.finish()))
}

fn validate_flags(flags: Option<&str>) -> Result<()> {
    if flags.is_some_and(|flags| flags.contains('g')) {
        return exec_err!("regexp_split_to_array does not support the global flag");
    }
    Ok(())
}

// Check actual output growth before touching the builders. An estimate based
// on input sizes could reject a valid result when delimiters remove most bytes.
struct SplitOutputSize {
    bytes: usize,
    elements: usize,
    byte_limit: usize,
}

impl SplitOutputSize {
    fn add(&mut self, bytes: usize) -> Result<()> {
        let Some(total) = self.bytes.checked_add(bytes) else {
            return exec_err!("regexp_split_to_array output byte offset overflow");
        };
        if total > self.byte_limit || self.elements >= i32::MAX as usize {
            return exec_err!("regexp_split_to_array output offset overflow");
        }
        self.bytes = total;
        self.elements += 1;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::compute::cast;
    use datafusion_common::config::ConfigOptions;

    fn strings(values: &[Option<&str>], kind: &DataType) -> ArrayRef {
        cast(&arrow::array::StringArray::from(values.to_vec()), kind).unwrap()
    }

    fn invoke(args: Vec<ColumnarValue>) -> Result<ColumnarValue> {
        let udf = RegexpSplitToArrayFunc::new();
        let types = args
            .iter()
            .map(ColumnarValue::data_type)
            .collect::<Vec<_>>();
        udf.invoke_with_args(ScalarFunctionArgs {
            arg_fields: types
                .iter()
                .map(|kind| Arc::new(Field::new("arg", kind.clone(), true)))
                .collect(),
            return_field: Arc::new(Field::new("result", udf.return_type(&types)?, true)),
            args,
            number_rows: 3,
            config_options: Arc::new(ConfigOptions::default()),
        })
    }

    fn assert_rows(
        result: ColumnarValue,
        expected: &[Option<Vec<&str>>],
        kind: &DataType,
    ) {
        let array = match result {
            ColumnarValue::Array(array) => array,
            ColumnarValue::Scalar(value) => value.to_array_of_size(1).unwrap(),
        };
        assert_eq!(
            array.data_type(),
            &DataType::List(Arc::new(Field::new_list_field(kind.clone(), true)))
        );
        let lists = array.as_list::<i32>();
        assert_eq!(lists.len(), expected.len());
        for (row, expected) in expected.iter().enumerate() {
            match expected {
                None => assert!(lists.is_null(row), "row {row} must be a null list"),
                Some(values) => {
                    assert!(!lists.is_null(row));
                    let actual = cast(&lists.value(row), &DataType::Utf8).unwrap();
                    assert_eq!(
                        actual.as_string::<i32>().iter().collect::<Vec<_>>(),
                        values.iter().map(|s| Some(*s)).collect::<Vec<_>>()
                    );
                }
            }
        }
    }

    #[test]
    fn postgres_split_boundaries() {
        // Expected values checked against PostgreSQL, including Unicode and
        // zero-width matches that Regex::split handles differently.
        let cases = [
            ("", "", vec![""]),
            ("", "x", vec![""]),
            ("abc", "", vec!["a", "b", "c"]),
            ("abc", "^|$", vec!["abc"]),
            ("abc", "a*", vec!["", "b", "c"]),
            ("aaab", "a*", vec!["", "b"]),
            (" a b ", r"\s+", vec!["", "a", "b", ""]),
            ("a,b,", ",", vec!["a", "b", ""]),
            ("é猫", "", vec!["é", "猫"]),
            ("one,two;three", "([,;])", vec!["one", "two", "three"]),
            ("abc", "z", vec!["abc"]),
        ];
        for kind in [DataType::Utf8, DataType::LargeUtf8, DataType::Utf8View] {
            let values = strings(
                &cases.iter().map(|(s, _, _)| Some(*s)).collect::<Vec<_>>(),
                &kind,
            );
            let patterns = strings(
                &cases.iter().map(|(_, p, _)| Some(*p)).collect::<Vec<_>>(),
                &kind,
            );
            let expected = cases
                .iter()
                .map(|(_, _, parts)| Some(parts.clone()))
                .collect::<Vec<_>>();
            assert_rows(
                invoke(vec![
                    ColumnarValue::Array(values),
                    ColumnarValue::Array(patterns),
                ])
                .unwrap(),
                &expected,
                &kind,
            );
        }
    }

    #[test]
    fn scalar_and_array_arguments() {
        for kind in [DataType::Utf8, DataType::LargeUtf8, DataType::Utf8View] {
            for mask in 0..8 {
                let args = ["aBcB", "b", "i"]
                    .into_iter()
                    .enumerate()
                    .map(|(i, value)| {
                        let scalar =
                            ScalarValue::try_from_string(value.to_string(), &kind)
                                .unwrap();
                        if mask & (1 << i) == 0 {
                            ColumnarValue::Scalar(scalar)
                        } else {
                            ColumnarValue::Array(scalar.to_array_of_size(3).unwrap())
                        }
                    })
                    .collect();
                let result = invoke(args).unwrap();
                assert_eq!(matches!(result, ColumnarValue::Scalar(_)), mask == 0);
                assert_rows(
                    result,
                    &vec![Some(vec!["a", "c", ""]); if mask == 0 { 1 } else { 3 }],
                    &kind,
                );
            }
        }
    }

    #[test]
    fn row_flags_and_nulls() {
        for kind in [DataType::Utf8, DataType::LargeUtf8, DataType::Utf8View] {
            let args = [
                vec![
                    Some("aBc"),
                    Some("aBc"),
                    Some("aBc"),
                    None,
                    Some("aBc"),
                    Some("aBc"),
                ],
                vec![Some("b"), Some("b"), Some("b"), Some("b"), None, Some("b")],
                vec![Some("i"), Some(""), Some("i"), Some("i"), Some("i"), None],
            ]
            .map(|values| ColumnarValue::Array(strings(&values, &kind)));
            assert_rows(
                invoke(args.to_vec()).unwrap(),
                &[
                    Some(vec!["a", "c"]),
                    Some(vec!["aBc"]),
                    Some(vec!["a", "c"]),
                    None,
                    None,
                    None,
                ],
                &kind,
            );
            for null_arg in 0..3 {
                let mut args = ["abc", "b", "i"].map(|value| {
                    ColumnarValue::Scalar(
                        ScalarValue::try_from_string(value.to_string(), &kind).unwrap(),
                    )
                });
                args[null_arg] =
                    ColumnarValue::Scalar(ScalarValue::try_from(&kind).unwrap());
                assert_rows(invoke(args.to_vec()).unwrap(), &[None], &kind);
            }
        }
    }

    #[test]
    fn empty_batches_and_errors() {
        for kind in [DataType::Utf8, DataType::LargeUtf8, DataType::Utf8View] {
            assert_rows(
                invoke(vec![
                    ColumnarValue::Array(strings(&[], &kind)),
                    ColumnarValue::Scalar(
                        ScalarValue::try_from_string(",".to_string(), &kind).unwrap(),
                    ),
                ])
                .unwrap(),
                &[],
                &kind,
            );
        }
        for (pattern, flags, message) in [
            ("[", "", "Regular expression did not compile"),
            ("b", "g", "global flag"),
            ("b", "!", "Regular expression did not compile"),
        ] {
            let args = ["abc", pattern, flags].map(|value| {
                ColumnarValue::Scalar(ScalarValue::Utf8(Some(value.to_string())))
            });
            assert!(
                invoke(args.to_vec())
                    .unwrap_err()
                    .to_string()
                    .contains(message)
            );
        }
        for count in [0, 1, 4] {
            let result =
                RegexpSplitToArrayFunc::new().invoke_with_args(ScalarFunctionArgs {
                    args: vec![
                        ColumnarValue::Scalar(ScalarValue::Utf8(Some(
                            "a".to_string()
                        )));
                        count
                    ],
                    arg_fields: (0..count)
                        .map(|_| Arc::new(Field::new("arg", DataType::Utf8, true)))
                        .collect(),
                    number_rows: 1,
                    return_field: Arc::new(Field::new(
                        "result",
                        DataType::List(Arc::new(Field::new_list_field(
                            DataType::Utf8,
                            true,
                        ))),
                        true,
                    )),
                    config_options: Arc::new(ConfigOptions::default()),
                });
            assert!(
                result
                    .unwrap_err()
                    .to_string()
                    .contains("requires 2 or 3 arguments")
            );
        }
        let args = vec![
            ColumnarValue::Array(strings(&[Some("a"), Some("b")], &DataType::Utf8)),
            ColumnarValue::Array(strings(&[Some(",")], &DataType::Utf8)),
        ];
        assert!(
            invoke(args)
                .unwrap_err()
                .to_string()
                .contains("matching array lengths")
        );
    }

    #[test]
    fn unused_invalid_delimiters_are_not_evaluated() {
        for kind in [DataType::Utf8, DataType::LargeUtf8, DataType::Utf8View] {
            for rows in [vec![], vec![None, None]] {
                for (pattern, flags) in [("[", ""), ("b", "g")] {
                    let args = vec![
                        ColumnarValue::Array(strings(&rows, &kind)),
                        ColumnarValue::Scalar(
                            ScalarValue::try_from_string(pattern.to_string(), &kind)
                                .unwrap(),
                        ),
                        ColumnarValue::Scalar(
                            ScalarValue::try_from_string(flags.to_string(), &kind)
                                .unwrap(),
                        ),
                    ];
                    assert_rows(invoke(args).unwrap(), &vec![None; rows.len()], &kind);
                }
            }
        }
    }

    #[test]
    fn output_offset_limits() {
        let mut size = SplitOutputSize {
            bytes: i32::MAX as usize - 1,
            elements: 0,
            byte_limit: i32::MAX as usize,
        };
        size.add(1).unwrap();
        assert!(size.add(1).is_err());
        let mut size = SplitOutputSize {
            bytes: 0,
            elements: i32::MAX as usize - 1,
            byte_limit: usize::MAX,
        };
        size.add(0).unwrap();
        assert!(size.add(0).is_err());
        let mut size = SplitOutputSize {
            bytes: usize::MAX,
            elements: 0,
            byte_limit: usize::MAX,
        };
        assert!(size.add(1).is_err());
    }
}
