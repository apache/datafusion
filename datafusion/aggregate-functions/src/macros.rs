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

// macro_rules! make_udf_function {
//     ($UDF:ty, $EXPR_FN:ident, $($arg:ident)*, $DOC:expr , $AGGREGATE_UDF_FN:ident) => {
//         paste::paste! {
//             // "fluent expr_fn" style function
//             #[doc = $DOC]
//             pub fn $EXPR_FN($($arg: Expr),*) -> Expr {
//                 Expr::ScalarFunction(ScalarFunction::new_udf(
//                     $AGGREGATE_UDF_FN(),
//                     vec![$($arg),*],
//                 ))
//             }

//             /// Singleton instance of [`$UDF`], ensures the UDF is only created once
//             /// named STATIC_$(UDF). For example `STATIC_ArrayToString`
//             #[allow(non_upper_case_globals)]
//             static [< STATIC_ $UDF >]: std::sync::OnceLock<std::sync::Arc<datafusion_expr::ScalarUDF>> =
//                 std::sync::OnceLock::new();

//             /// ScalarFunction that returns a [`ScalarUDF`] for [`$UDF`]
//             ///
//             /// [`ScalarUDF`]: datafusion_expr::ScalarUDF
//             pub fn $AGGREGATE_UDF_FN() -> std::sync::Arc<datafusion_expr::ScalarUDF> {
//                 [< STATIC_ $UDF >]
//                     .get_or_init(|| {
//                         std::sync::Arc::new(datafusion_expr::ScalarUDF::new_from_impl(
//                             <$UDF>::new(),
//                         ))
//                     })
//                     .clone()
//             }
//         }
//     }
// }
