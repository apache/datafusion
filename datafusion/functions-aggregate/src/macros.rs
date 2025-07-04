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

#[macro_export]
macro_rules! make_udaf_expr {
    ($EXPR_FN:ident, $($arg:ident)*, $DOC:expr, $AGGREGATE_UDF_FN:ident) => {
        // "fluent expr_fn" style function
        #[doc = $DOC]
        pub fn $EXPR_FN(
            $($arg: datafusion_expr::Expr,)*
        ) -> datafusion_expr::Expr {
            datafusion_expr::Expr::AggregateFunction(datafusion_expr::expr::AggregateFunction::new_udf(
                $AGGREGATE_UDF_FN(),
                vec![$($arg),*],
                false,
                None,
                vec![],
                None,
            ))
        }
    };
}

#[macro_export]
macro_rules! make_udaf_expr_and_func {
    ($UDAF:ty, $EXPR_FN:ident, $($arg:ident)*, $DOC:expr, $AGGREGATE_UDF_FN:ident) => {
        make_udaf_expr!($EXPR_FN, $($arg)*, $DOC, $AGGREGATE_UDF_FN);
        create_func!($UDAF, $AGGREGATE_UDF_FN);
    };
    ($UDAF:ty, $EXPR_FN:ident, $DOC:expr, $AGGREGATE_UDF_FN:ident) => {
        // "fluent expr_fn" style function
        #[doc = $DOC]
        pub fn $EXPR_FN(
            args: Vec<datafusion_expr::Expr>,
        ) -> datafusion_expr::Expr {
            datafusion_expr::Expr::AggregateFunction(datafusion_expr::expr::AggregateFunction::new_udf(
                $AGGREGATE_UDF_FN(),
                args,
                false,
                None,
                vec![],
                None,
            ))
        }

        create_func!($UDAF, $AGGREGATE_UDF_FN);
    };
}

#[macro_export]
macro_rules! create_func {
    ($UDAF:ty, $AGGREGATE_UDF_FN:ident) => {
        create_func!($UDAF, $AGGREGATE_UDF_FN, <$UDAF>::default());
    };
    ($UDAF:ty, $AGGREGATE_UDF_FN:ident, $CREATE:expr) => {
        paste::paste! {
            #[doc = concat!("AggregateFunction that returns a [`AggregateUDF`](datafusion_expr::AggregateUDF) for [`", stringify!($UDAF), "`]")]
            pub fn $AGGREGATE_UDF_FN() -> std::sync::Arc<datafusion_expr::AggregateUDF> {
                // Singleton instance of [$UDAF], ensures the UDAF is only created once
                static INSTANCE: std::sync::LazyLock<std::sync::Arc<datafusion_expr::AggregateUDF>> =
                    std::sync::LazyLock::new(|| {
                        std::sync::Arc::new(datafusion_expr::AggregateUDF::from($CREATE))
                    });
                std::sync::Arc::clone(&INSTANCE)
            }
        }
    }
}
