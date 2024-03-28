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

// create UDFs
make_udf_function!(
    character_length::CharacterLengthFunc,
    CHARACTER_LENGTH,
    character_length
);

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

    #[doc = "the number of characters in the `string`"]
    pub fn length(string: Expr) -> Expr {
        character_length(string)
    }
}

///   Return a list of all functions in this package
pub fn functions() -> Vec<Arc<ScalarUDF>> {
    vec![character_length()]
}
