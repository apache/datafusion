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

mod inner;

use datafusion_expr::{Expr, ScalarUDF};
use std::sync::{Arc, OnceLock};

pub mod expr_fn {
    use super::*;
    /// Return encode(arg)
    pub fn encode(args: Vec<Expr>) -> Expr {
        super::encode().call(args)
    }

    /// Return decode(arg)
    pub fn decode(args: Vec<Expr>) -> Expr {
        super::decode().call(args)
    }
}

/// If feature flag make a single global UDF instance and a function to return it
macro_rules! make_function {
    ($UDF:ty, $GNAME:ident, $NAME:ident) => {
        /// Singleton instance of the function
        static $GNAME: OnceLock<Arc<ScalarUDF>> = OnceLock::new();

        /// Return the function implementation
        fn $NAME() -> Arc<ScalarUDF> {
            $GNAME
                .get_or_init(|| Arc::new(ScalarUDF::new_from_impl(<$UDF>::default())))
                .clone()
        }
    };
}

make_function!(inner::EncodeFunc, ENCODE, encode);
make_function!(inner::DecodeFunc, DECODE, decode);

/// Return a list of all functions in this package
pub(crate) fn functions() -> Vec<Arc<ScalarUDF>> {
    vec![encode(), decode()]
}
