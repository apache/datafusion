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

//! "unicode" DataFusion functions

use std::sync::Arc;

use datafusion_expr::ScalarUDF;

pub mod character_length;
pub mod common;
pub mod find_in_set;
pub mod initcap;
pub mod left;
pub mod lpad;
pub mod planner;
pub mod reverse;
pub mod right;
pub mod rpad;
pub mod strpos;
pub mod substr;
pub mod substrindex;
pub mod translate;

// create UDFs
make_udf_function!(character_length::CharacterLengthFunc, character_length);
make_udf_function!(find_in_set::FindInSetFunc, find_in_set);
make_udf_function!(initcap::InitcapFunc, initcap);
make_udf_function!(left::LeftFunc, left);
make_udf_function!(lpad::LPadFunc, lpad);
make_udf_function!(right::RightFunc, right);
make_udf_function!(reverse::ReverseFunc, reverse);
make_udf_function!(rpad::RPadFunc, rpad);
make_udf_function!(strpos::StrposFunc, strpos);
make_udf_function!(substr::SubstrFunc, substr);
make_udf_function!(substr::SubstrFunc, substring);
make_udf_function!(substrindex::SubstrIndexFunc, substr_index);
make_udf_function!(translate::TranslateFunc, translate);

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
    fn strict_unicode_functions_propagate_nulls() {
        let cases = vec![
            (character_length(), vec![ScalarValue::from("abc")]),
            (
                find_in_set(),
                vec![ScalarValue::from("b"), ScalarValue::from("a,b,c")],
            ),
            (initcap(), vec![ScalarValue::from("hello world")]),
            (
                left(),
                vec![ScalarValue::from("abc"), ScalarValue::from(2_i64)],
            ),
            (
                lpad(),
                vec![ScalarValue::from("abc"), ScalarValue::from(5_i64)],
            ),
            (
                lpad(),
                vec![
                    ScalarValue::from("abc"),
                    ScalarValue::from(5_i64),
                    ScalarValue::from("x"),
                ],
            ),
            (reverse(), vec![ScalarValue::from("abc")]),
            (
                right(),
                vec![ScalarValue::from("abc"), ScalarValue::from(2_i64)],
            ),
            (
                rpad(),
                vec![ScalarValue::from("abc"), ScalarValue::from(5_i64)],
            ),
            (
                rpad(),
                vec![
                    ScalarValue::from("abc"),
                    ScalarValue::from(5_i64),
                    ScalarValue::from("x"),
                ],
            ),
            (
                strpos(),
                vec![ScalarValue::from("abc"), ScalarValue::from("b")],
            ),
            (
                substr(),
                vec![ScalarValue::from("abc"), ScalarValue::from(2_i64)],
            ),
            (
                substr(),
                vec![
                    ScalarValue::from("abc"),
                    ScalarValue::from(2_i64),
                    ScalarValue::from(1_i64),
                ],
            ),
            (
                substr_index(),
                vec![
                    ScalarValue::from("a.b.c"),
                    ScalarValue::from("."),
                    ScalarValue::from(2_i64),
                ],
            ),
            (
                translate(),
                vec![
                    ScalarValue::from("abc"),
                    ScalarValue::from("a"),
                    ScalarValue::from("x"),
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

    export_functions!((
        character_length,
        "the number of characters in the `string`",
        string
    ),(
        lpad,
        "fill up a string to the length by prepending the characters",
        args,
    ),(
        rpad,
        "fill up a string to the length by appending the characters",
        args,
    ),(
        reverse,
        "reverses the `string`",
        string
    ),(
        substr,
        "substring from the `position` to the end",
        string position
    ),(
        substr_index,
        "Returns the substring from str before count occurrences of the delimiter",
        string delimiter count
    ),(
        strpos,
        "finds the position from where the `substring` matches the `string`",
        string substring
    ),(
        substring,
        "substring from the `position` with `length` characters",
        string position length
    ),(
        translate,
        "replaces the characters in `from` with the counterpart in `to`",
        string from to
    ),(
        right,
        "returns the last `n` characters in the `string`",
        string n
    ),(
        left,
        "returns the first `n` characters in the `string`",
        string n
    ),(
        initcap,
        "converts the first letter of each word in `string` in uppercase and the remaining characters in lowercase",
        string
    ),(
        find_in_set,
        "Returns a value in the range of 1 to N if the string `str` is in the string list `strlist` consisting of N substrings",
        string strlist
    ));

    #[doc = "the number of characters in the `string`"]
    pub fn char_length(string: Expr) -> Expr {
        character_length(string)
    }

    #[doc = "finds the position from where the `substring` matches the `string`"]
    pub fn instr(string: Expr, substring: Expr) -> Expr {
        strpos(string, substring)
    }

    #[doc = "the number of characters in the `string`"]
    pub fn length(string: Expr) -> Expr {
        character_length(string)
    }

    #[doc = "finds the position from where the `substring` matches the `string`"]
    pub fn position(string: Expr, substring: Expr) -> Expr {
        strpos(string, substring)
    }
}

/// Returns all DataFusion functions defined in this package
pub fn functions() -> Vec<Arc<ScalarUDF>> {
    vec![
        character_length(),
        find_in_set(),
        initcap(),
        left(),
        lpad(),
        reverse(),
        right(),
        rpad(),
        strpos(),
        substr(),
        substr_index(),
        translate(),
    ]
}
