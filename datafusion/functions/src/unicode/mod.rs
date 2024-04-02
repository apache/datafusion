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

mod character_length;
mod find_in_set;
mod left;
mod lpad;
mod reverse;
mod right;
mod rpad;
mod strpos;
mod substr;
mod substrindex;
mod translate;

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
make_udf_function!(substrindex::SubstrIndexFunc, SUBSTR_INDEX, substr_index);
make_udf_function!(translate::TranslateFunc, TRANSLATE, translate);

pub mod expr_fn {
    use datafusion_expr::Expr;

    #[doc = "the number of characters in the `string`"]
    pub fn char_length(string: Expr) -> Expr {
        character_length(string)
    }

    #[doc = "the number of characters in the `string`"]
    pub fn character_length(string: Expr) -> Expr {
        super::character_length().call(vec![string])
    }

    #[doc = "Returns a value in the range of 1 to N if the string str is in the string list strlist consisting of N substrings"]
    pub fn find_in_set(string: Expr, strlist: Expr) -> Expr {
        super::find_in_set().call(vec![string, strlist])
    }

    #[doc = "finds the position from where the `substring` matches the `string`"]
    pub fn instr(string: Expr, substring: Expr) -> Expr {
        strpos(string, substring)
    }

    #[doc = "the number of characters in the `string`"]
    pub fn length(string: Expr) -> Expr {
        character_length(string)
    }

    #[doc = "returns the first `n` characters in the `string`"]
    pub fn left(string: Expr, n: Expr) -> Expr {
        super::left().call(vec![string, n])
    }

    #[doc = "fill up a string to the length by prepending the characters"]
    pub fn lpad(args: Vec<Expr>) -> Expr {
        super::lpad().call(args)
    }

    #[doc = "finds the position from where the `substring` matches the `string`"]
    pub fn position(string: Expr, substring: Expr) -> Expr {
        strpos(string, substring)
    }

    #[doc = "reverses the `string`"]
    pub fn reverse(string: Expr) -> Expr {
        super::reverse().call(vec![string])
    }

    #[doc = "returns the last `n` characters in the `string`"]
    pub fn right(string: Expr, n: Expr) -> Expr {
        super::right().call(vec![string, n])
    }

    #[doc = "fill up a string to the length by appending the characters"]
    pub fn rpad(args: Vec<Expr>) -> Expr {
        super::rpad().call(args)
    }

    #[doc = "finds the position from where the `substring` matches the `string`"]
    pub fn strpos(string: Expr, substring: Expr) -> Expr {
        super::strpos().call(vec![string, substring])
    }

    #[doc = "substring from the `position` to the end"]
    pub fn substr(string: Expr, position: Expr) -> Expr {
        super::substr().call(vec![string, position])
    }

    #[doc = "substring from the `position` with `length` characters"]
    pub fn substring(string: Expr, position: Expr, length: Expr) -> Expr {
        super::substr().call(vec![string, position, length])
    }

    #[doc = "Returns the substring from str before count occurrences of the delimiter"]
    pub fn substr_index(string: Expr, delimiter: Expr, count: Expr) -> Expr {
        super::substr_index().call(vec![string, delimiter, count])
    }

    #[doc = "replaces the characters in `from` with the counterpart in `to`"]
    pub fn translate(string: Expr, from: Expr, to: Expr) -> Expr {
        super::translate().call(vec![string, from, to])
    }
}

///   Return a list of all functions in this package
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
