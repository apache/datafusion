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

mod btrim;
mod common;
mod ltrim;
mod rtrim;
mod starts_with;
mod to_hex;
mod upper;

// create UDFs
make_udf_function!(btrim::TrimFunc, BTRIM, btrim);
make_udf_function!(ltrim::LtrimFunc, LTRIM, ltrim);
make_udf_function!(rtrim::RtrimFunc, RTRIM, rtrim);
make_udf_function!(starts_with::StartsWithFunc, STARTS_WITH, starts_with);
make_udf_function!(to_hex::ToHexFunc, TO_HEX, to_hex);
make_udf_function!(upper::UpperFunc, UPPER, upper);

pub mod expr_fn {
    use datafusion_expr::Expr;

    #[doc = "Removes all characters, spaces by default, from both sides of a string"]
    pub fn btrim(args: Vec<Expr>) -> Expr {
        super::btrim().call(args)
    }

    #[doc = "Removes all characters, spaces by default, from the beginning of a string"]
    pub fn ltrim(args: Vec<Expr>) -> Expr {
        super::ltrim().call(args)
    }

    #[doc = "Removes all characters, spaces by default, from the end of a string"]
    pub fn rtrim(args: Vec<Expr>) -> Expr {
        super::rtrim().call(args)
    }

    #[doc = "Returns true if string starts with prefix."]
    pub fn starts_with(arg1: Expr, arg2: Expr) -> Expr {
        super::starts_with().call(vec![arg1, arg2])
    }

    #[doc = "Converts an integer to a hexadecimal string."]
    pub fn to_hex(arg1: Expr) -> Expr {
        super::to_hex().call(vec![arg1])
    }

    #[doc = "Converts a string to uppercase."]
    pub fn upper(arg1: Expr) -> Expr {
        super::upper().call(vec![arg1])
    }
}

///   Return a list of all functions in this package
pub fn functions() -> Vec<Arc<ScalarUDF>> {
    vec![btrim(), ltrim(), rtrim(), starts_with(), to_hex(), upper()]
}
