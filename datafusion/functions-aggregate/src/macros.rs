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

macro_rules! make_udaf_function {
    ($UDAF:ty, $EXPR_FN:ident, $($arg:ident)*, $DOC:expr, $AGGREGATE_UDF_FN:ident) => {
        paste::paste! {
            // "fluent expr_fn" style function
            #[doc = $DOC]
            pub fn $EXPR_FN($($arg: Expr),*) -> Expr {
                Expr::AggregateFunction(datafusion_expr::expr::AggregateFunction::new_udf(
                    $AGGREGATE_UDF_FN(),
                    vec![$($arg),*],
                    // TODO: Support arguments for `expr` API
                    false,
                    None,
                    None,
                    None,
                ))
            }

            /// Singleton instance of [$UDAF], ensures the UDAF is only created once
            /// named STATIC_$(UDAF). For example `STATIC_FirstValue`
            #[allow(non_upper_case_globals)]
            static [< STATIC_ $UDAF >]: std::sync::OnceLock<std::sync::Arc<datafusion_expr::AggregateUDF>> =
                std::sync::OnceLock::new();

            /// AggregateFunction that returns a [AggregateUDF] for [$UDAF]
            ///
            /// [AggregateUDF]: datafusion_expr::AggregateUDF
            pub fn $AGGREGATE_UDF_FN() -> std::sync::Arc<datafusion_expr::AggregateUDF> {
                [< STATIC_ $UDAF >]
                    .get_or_init(|| {
                        std::sync::Arc::new(datafusion_expr::AggregateUDF::from(<$UDAF>::default()))
                    })
                    .clone()
            }
        }
    }
}
