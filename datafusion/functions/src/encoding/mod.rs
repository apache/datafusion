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

#[cfg(feature = "encoding_expressions")]
mod inner;

use datafusion_expr::{Expr, FunctionImplementation, ScalarUDF};
use std::sync::Arc;

#[cfg(not(feature = "encoding_expressions"))]
pub mod expr_fn {}

#[cfg(feature = "encoding_expressions")]
pub mod expr_fn {
    use super::*;
    /// Return encode(arg)
    pub fn encode(args: Vec<Expr>) -> Expr {
        let udf = ScalarUDF::new_from_impl(Arc::new(inner::EncodeFunc::default()));
        Expr::ScalarFunction(datafusion_expr::expr::ScalarFunction::new_udf(
            Arc::new(udf),
            args,
        ))
    }

    /// Return decode(arg)
    pub fn decode(args: Vec<Expr>) -> Expr {
        let udf = ScalarUDF::new_from_impl(Arc::new(inner::DecodeFunc::default()));
        Expr::ScalarFunction(datafusion_expr::expr::ScalarFunction::new_udf(
            Arc::new(udf),
            args,
        ))
    }
}

/// If feature flag is enabled, ues actual implementation
#[cfg(feature = "encoding_expressions")]
macro_rules! use_function {
    ($UDF:ty, $NAME:expr) => {{
        Arc::new(<$UDF>::default()) as _
    }};
}

/// If feature flag is not enabled, registers a stub that will error with a nice message
#[cfg(not(feature = "encoding_expressions"))]
macro_rules! use_function {
    ($IGNORE:tt :: $IGNORE2:tt, $NAME:expr) => {{
        Arc::new(crate::stub::StubFunc::new(
            $NAME,
            "feature 'encoding_expressions' not enabled",
        ))
    }};
}

/// Return a list of all functions in this package
pub(crate) fn functions() -> Vec<Arc<dyn FunctionImplementation + Send + Sync>> {
    vec![
        use_function!(inner::EncodeFunc, "encode"),
        use_function!(inner::DecodeFunc, "decode"),
    ]
}
