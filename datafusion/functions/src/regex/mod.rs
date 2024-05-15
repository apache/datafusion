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

//! "regx" DataFusion functions

pub mod regexplike;
pub mod regexpmatch;
pub mod regexpreplace;
// create UDFs
make_udf_function!(regexpmatch::RegexpMatchFunc, REGEXP_MATCH, regexp_match);
make_udf_function!(regexplike::RegexpLikeFunc, REGEXP_LIKE, regexp_like);
make_udf_function!(
    regexpreplace::RegexpReplaceFunc,
    REGEXP_REPLACE,
    regexp_replace
);

pub mod expr_fn {
    #[doc = "returns a list of regular expression matches in a string. "]
    #[doc = r" Return $name(arg)"]
    pub fn regexp_match(
        input_arg1: datafusion_expr::Expr,
        input_arg2: datafusion_expr::Expr,
    ) -> datafusion_expr::Expr {
        let args = vec![input_arg1, input_arg2];
        super::regexp_match().call(args)
    }

    #[doc = "Returns true if a has at least one match in a string,false otherwise."]
    #[doc = r" Return $name(arg)"]
    pub fn regexp_like(
        input_arg1: datafusion_expr::Expr,
        input_arg2: datafusion_expr::Expr,
    ) -> datafusion_expr::Expr {
        let args = vec![input_arg1, input_arg2];
        super::regexp_like().call(args)
    }

    #[doc = "Replaces substrings in a string that match"]
    #[doc = r" Return $name(arg)"]
    pub fn regexp_replace(
        arg1: datafusion_expr::Expr,
        arg2: datafusion_expr::Expr,
        arg3: datafusion_expr::Expr,
        arg4: datafusion_expr::Expr,
    ) -> datafusion_expr::Expr {
        let args = vec![arg1, arg2, arg3, arg4];
        super::regexp_replace().call(args)
    }
}

#[doc = r" Return a list of all functions in this package"]
pub fn functions() -> Vec<std::sync::Arc<datafusion_expr::ScalarUDF>> {
    vec![regexp_match(), regexp_like(), regexp_replace()]
}
