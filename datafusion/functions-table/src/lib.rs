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

pub mod generate_series;

use datafusion_catalog::TableFunction;
use std::sync::Arc;

/// Returns all default table functions
pub fn all_default_table_functions() -> Vec<Arc<TableFunction>> {
    vec![generate_series()]
}

/// Creates a singleton instance of a table function
/// - `$module`: A struct implementing `TableFunctionImpl` to create the function from
/// - `$name`: The name to give to the created function
///
/// This is used to ensure creating the list of `TableFunction` only happens once.
#[macro_export]
macro_rules! create_udtf_function {
    ($module:path, $name:expr) => {
        paste::paste! {
            pub fn [<$name:lower>]() -> Arc<TableFunction> {
                static INSTANCE: std::sync::LazyLock<Arc<TableFunction>> =
                    std::sync::LazyLock::new(|| {
                        std::sync::Arc::new(TableFunction::new(
                            $name.to_string(),
                            Arc::new($module {}),
                        ))
                    });
                std::sync::Arc::clone(&INSTANCE)
            }
        }
    };
}

create_udtf_function!(generate_series::GenerateSeriesFunc, "generate_series");
