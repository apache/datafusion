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

macro_rules! create_udwf {
    ($STRUCT_NAME:ident, $FN_NAME:ident, $CTOR:path) => {
        paste::paste! {
            /// Singleton instance of [$STRUCT_NAME], ensures the user-defined
            /// window function is only created once.
            ///
            /// For example, `STATIC_RowNumber`
            #[allow(non_upper_case_globals)]
            static [<STATIC_ $STRUCT_NAME>]: std::sync::OnceLock<std::sync::Arc<datafusion_expr::WindowUDF>> =
                std::sync::OnceLock::new();

            /// Returns a [`WindowUDF`](datafusion_expr::WindowUDF) for [$STRUCT_NAME]
            pub fn [<$FN_NAME _udwf>]() -> std::sync::Arc<datafusion_expr::WindowUDF> {
                [<STATIC_ $STRUCT_NAME>]
                    .get_or_init(|| {
                        std::sync::Arc::new(datafusion_expr::WindowUDF::from($CTOR()))
                    })
                    .clone()
            }
        }
    }
}
