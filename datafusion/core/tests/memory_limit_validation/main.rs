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

//! Validates query's actual memory usage is consistent with the specified memory
//! limit. This target requires the `extended_tests` feature.
//!
//! Run with: cargo test -p datafusion --test memory_limit_validation --features extended_tests

mod aggregate_mem_validation;
mod hash_join_mem_validation;
mod nlj_mem_validation;
mod smj_mem_validation;
mod sort_mem_validation;
mod utils;
mod window_mem_validation;

#[global_allocator]
static ALLOC: test_utils::counting_allocator::CountingAllocator =
    test_utils::counting_allocator::CountingAllocator;

#[cfg(test)]
#[ctor::ctor(unsafe)]
fn init() {
    // Enable RUST_LOG logging configuration for test
    let _ = env_logger::try_init();
}
