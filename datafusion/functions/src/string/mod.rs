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

//! "string" DataFusion functions

use std::sync::Arc;

use datafusion_expr::ScalarUDF;

pub mod ascii;
pub mod bit_length;
pub mod btrim;
pub mod chr;
pub mod common;
pub mod concat;
pub mod concat_ws;
pub mod contains;
pub mod ends_with;
pub mod levenshtein;
pub mod lower;
pub mod ltrim;
pub mod octet_length;
pub mod repeat;
pub mod replace;
pub mod rtrim;
pub mod split_part;
pub mod starts_with;
pub mod to_hex;
pub mod upper;
pub mod uuid;
// create UDFs
make_udf_function!(ascii::AsciiFunc, ascii);
make_udf_function!(bit_length::BitLengthFunc, bit_length);
make_udf_function!(btrim::BTrimFunc, btrim);
make_udf_function!(chr::ChrFunc, chr);
make_udf_function!(concat::ConcatFunc, concat);
make_udf_function!(concat_ws::ConcatWsFunc, concat_ws);
make_udf_function!(ends_with::EndsWithFunc, ends_with);
make_udf_function!(levenshtein::LevenshteinFunc, levenshtein);
make_udf_function!(ltrim::LtrimFunc, ltrim);
make_udf_function!(lower::LowerFunc, lower);
make_udf_function!(octet_length::OctetLengthFunc, octet_length);
make_udf_function!(repeat::RepeatFunc, repeat);
make_udf_function!(replace::ReplaceFunc, replace);
make_udf_function!(rtrim::RtrimFunc, rtrim);
make_udf_function!(starts_with::StartsWithFunc, starts_with);
make_udf_function!(split_part::SplitPartFunc, split_part);
make_udf_function!(to_hex::ToHexFunc, to_hex);
make_udf_function!(upper::UpperFunc, upper);
make_udf_function!(uuid::UuidFunc, uuid);
make_udf_function!(contains::ContainsFunc, contains);
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
    fn strict_string_functions_propagate_nulls() {
        let cases = vec![
            (ascii(), vec![ScalarValue::from("abc")]),
            (bit_length(), vec![ScalarValue::from("abc")]),
            (btrim(), vec![ScalarValue::from(" abc ")]),
            (
                btrim(),
                vec![ScalarValue::from("xabcx"), ScalarValue::from("x")],
            ),
            (chr(), vec![ScalarValue::from(65_i64)]),
            (
                contains(),
                vec![ScalarValue::from("abc"), ScalarValue::from("b")],
            ),
            (
                ends_with(),
                vec![ScalarValue::from("abc"), ScalarValue::from("c")],
            ),
            (
                levenshtein(),
                vec![ScalarValue::from("kitten"), ScalarValue::from("sitting")],
            ),
            (lower(), vec![ScalarValue::from("ABC")]),
            (ltrim(), vec![ScalarValue::from(" abc ")]),
            (
                ltrim(),
                vec![ScalarValue::from("xabcx"), ScalarValue::from("x")],
            ),
            (octet_length(), vec![ScalarValue::from("abc")]),
            (
                octet_length(),
                vec![ScalarValue::Binary(Some(b"abc".to_vec()))],
            ),
            (
                repeat(),
                vec![ScalarValue::from("ab"), ScalarValue::from(2_i64)],
            ),
            (
                replace(),
                vec![
                    ScalarValue::from("abc"),
                    ScalarValue::from("b"),
                    ScalarValue::from("x"),
                ],
            ),
            (rtrim(), vec![ScalarValue::from(" abc ")]),
            (
                rtrim(),
                vec![ScalarValue::from("xabcx"), ScalarValue::from("x")],
            ),
            (
                split_part(),
                vec![
                    ScalarValue::from("a,b"),
                    ScalarValue::from(","),
                    ScalarValue::from(1_i64),
                ],
            ),
            (
                starts_with(),
                vec![ScalarValue::from("abc"), ScalarValue::from("a")],
            ),
            (to_hex(), vec![ScalarValue::from(255_i64)]),
            (upper(), vec![ScalarValue::from("abc")]),
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
        ascii,
        "Returns the numeric code of the first character of the argument.",
        arg1
    ),(
        bit_length,
        "Returns the number of bits in the `string`",
        arg1
    ),(
        btrim,
        "Removes all characters, spaces by default, from both sides of a string",
        args,
    ),(
        chr,
        "Converts the Unicode code point to a UTF8 character",
        arg1
    ),(
        concat,
        "Concatenates the text representations of all the arguments. NULL arguments are ignored",
        args,
    ),(
        ends_with,
        "Returns true if the `string` ends with the `suffix`, false otherwise.",
        string suffix
    ),(
        levenshtein,
        "Returns the Levenshtein distance between the two given strings",
        arg1 arg2
    ),(
        lower,
        "Converts a string to lowercase.",
        arg1
    ),(
        ltrim,
        "Removes all characters, spaces by default, from the beginning of a string",
        args,
    ),(
        octet_length,
        "returns the number of bytes of a string",
        args
    ),(
        repeat,
        "Repeats the `string` to `n` times",
        string n
    ),(
        replace,
        "Replaces all occurrences of `from` with `to` in the `string`",
        string from to
    ),(
        rtrim,
        "Removes all characters, spaces by default, from the end of a string",
        args,
    ),(
        split_part,
        "Splits a string based on a delimiter and picks out the desired field based on the index.",
        string delimiter index
    ),(
        starts_with,
        "Returns true if string starts with prefix.",
        arg1 arg2
    ),(
        to_hex,
        "Converts an integer to a hexadecimal string.",
        arg1
    ),(
        upper,
        "Converts a string to uppercase.",
        arg1
    ),(
        uuid,
        "returns uuid v4 as a string value",
    ), (
        contains,
        "Return true if `search_string` is found within `string`.",
        string search_string
    ));

    #[doc = "Removes all characters, spaces by default, from both sides of a string"]
    pub fn trim(args: Vec<Expr>) -> Expr {
        super::btrim().call(args)
    }

    #[doc = "Concatenates all but the first argument, with separators. The first argument is used as the separator string, and should not be NULL. Other NULL arguments are ignored."]
    pub fn concat_ws(delimiter: Expr, args: Vec<Expr>) -> Expr {
        let mut args = args;
        args.insert(0, delimiter);
        super::concat_ws().call(args)
    }
}

/// Returns all DataFusion functions defined in this package
pub fn functions() -> Vec<Arc<ScalarUDF>> {
    vec![
        ascii(),
        bit_length(),
        btrim(),
        chr(),
        concat(),
        concat_ws(),
        ends_with(),
        levenshtein(),
        lower(),
        ltrim(),
        octet_length(),
        repeat(),
        replace(),
        rtrim(),
        split_part(),
        starts_with(),
        to_hex(),
        upper(),
        uuid(),
        contains(),
    ]
}
