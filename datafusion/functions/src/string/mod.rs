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
pub mod initcap;
pub mod levenshtein;
pub mod lower;
pub mod ltrim;
pub mod octet_length;
pub mod overlay;
pub mod repeat;
pub mod replace;
pub mod rtrim;
pub mod split_part;
pub mod starts_with;
pub mod to_hex;
pub mod upper;
pub mod uuid;
// create UDFs
make_udf_function!(ascii::AsciiFunc, ASCII, ascii);
make_udf_function!(bit_length::BitLengthFunc, BIT_LENGTH, bit_length);
make_udf_function!(btrim::BTrimFunc, BTRIM, btrim);
make_udf_function!(chr::ChrFunc, CHR, chr);
make_udf_function!(concat::ConcatFunc, CONCAT, concat);
make_udf_function!(concat_ws::ConcatWsFunc, CONCAT_WS, concat_ws);
make_udf_function!(ends_with::EndsWithFunc, ENDS_WITH, ends_with);
make_udf_function!(initcap::InitcapFunc, INITCAP, initcap);
make_udf_function!(levenshtein::LevenshteinFunc, LEVENSHTEIN, levenshtein);
make_udf_function!(ltrim::LtrimFunc, LTRIM, ltrim);
make_udf_function!(lower::LowerFunc, LOWER, lower);
make_udf_function!(octet_length::OctetLengthFunc, OCTET_LENGTH, octet_length);
make_udf_function!(overlay::OverlayFunc, OVERLAY, overlay);
make_udf_function!(repeat::RepeatFunc, REPEAT, repeat);
make_udf_function!(replace::ReplaceFunc, REPLACE, replace);
make_udf_function!(rtrim::RtrimFunc, RTRIM, rtrim);
make_udf_function!(starts_with::StartsWithFunc, STARTS_WITH, starts_with);
make_udf_function!(split_part::SplitPartFunc, SPLIT_PART, split_part);
make_udf_function!(to_hex::ToHexFunc, TO_HEX, to_hex);
make_udf_function!(upper::UpperFunc, UPPER, upper);
make_udf_function!(uuid::UuidFunc, UUID, uuid);
make_udf_function!(contains::ContainsFunc, CONTAINS, contains);
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
        initcap,
        "Converts the first letter of each word in `string` in uppercase and the remaining characters in lowercase",
        string
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
        overlay,
        "replace the substring of string that starts at the start'th character and extends for count characters with new substring",
        args,
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
        "Return true if search_string is found within string. treated it like a reglike",
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
        initcap(),
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
