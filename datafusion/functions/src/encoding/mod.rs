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
#[cfg(feature = "encoding_expressions")]
mod meta;

use crate::utils::insert;
use datafusion_expr::ScalarUDF;
use std::collections::HashMap;
use std::sync::Arc;

/// Registers the `encode` and `decode` functions with the function registry
#[cfg(feature = "encoding_expressions")]
pub fn register(registry: &mut HashMap<String, Arc<ScalarUDF>>) {
    insert(registry, meta::EncodeFunc {});
    insert(registry, meta::DecodeFunc {});
}

/// Registers the `encode` and `decode` stubs with the function registry
#[cfg(not(feature = "encoding_expressions"))]
pub fn register(registry: &mut HashMap<String, Arc<ScalarUDF>>) {
    let hint = "Requires compilation with feature flag: encoding_expressions.";

    for function_name in ["encode", "decode"] {
        insert(registry, crate::utils::StubFunc::new(function_name, hint));
    }
}
