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
pub mod find_in_set;
pub mod left;
pub mod lpad;
pub mod reverse;
pub mod right;
pub mod rpad;
pub mod strpos;
pub mod substr;
pub mod substrindex;
pub mod translate;

// create UDFs
make_udf_function!(
    character_length::CharacterLengthFunc,
    CHARACTER_LENGTH,
    character_length
);
make_udf_function!(find_in_set::FindInSetFunc, FIND_IN_SET, find_in_set);
make_udf_function!(left::LeftFunc, LEFT, left);
make_udf_function!(lpad::LPadFunc, LPAD, lpad);
make_udf_function!(right::RightFunc, RIGHT, right);
make_udf_function!(reverse::ReverseFunc, REVERSE, reverse);
make_udf_function!(rpad::RPadFunc, RPAD, rpad);
make_udf_function!(strpos::StrposFunc, STRPOS, strpos);
make_udf_function!(substr::SubstrFunc, SUBSTR, substr);
make_udf_function!(substr::SubstrFunc, SUBSTRING, substring);
make_udf_function!(substrindex::SubstrIndexFunc, SUBSTR_INDEX, substr_index);
make_udf_function!(translate::TranslateFunc, TRANSLATE, translate);

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
        find_in_set,
        "Returns a value in the range of 1 to N if the string str is in the string list strlist consisting of N substrings",
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
