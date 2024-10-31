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

/// Run all tests that are found in the `sql` directory
mod sql;

/// Run all tests that are found in the `dataframe` directory
mod dataframe;

/// Run all tests that are found in the `macro_hygiene` directory
mod macro_hygiene;

/// Run all tests that are found in the `execution` directory
mod execution;

/// Run all tests that are found in the `expr_api` directory
mod expr_api;

/// Run all tests that are found in the `fifo` directory
mod fifo;

/// Run all tests that are found in the `memory_limit` directory
mod memory_limit;

/// Run all tests that are found in the `custom_sources_cases` directory
mod custom_sources_cases;

/// Run all tests that are found in the `optimizer` directory
mod optimizer;

/// Run all tests that are found in the `physical_optimizer` directory
mod physical_optimizer;

#[cfg(test)]
#[ctor::ctor]
fn init() {
    // Enable RUST_LOG logging configuration for test
    let _ = env_logger::try_init();
}
