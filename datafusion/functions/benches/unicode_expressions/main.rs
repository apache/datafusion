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

//! Benchmarks for functions behind the `unicode_expressions` feature.
//!
//! Each benchmark beside this file is a module rather than its own `[[bench]]`
//! target to reduce compile times and output size.
//!
//! Run one of them with `cargo bench --bench unicode_expressions -- <filter>`.

use criterion::criterion_main;

mod character_length;
mod find_in_set;
mod find_in_set_literal;
#[path = "../helper.rs"]
mod helper;
mod initcap;
mod left_right;
mod levenshtein;
mod pad;
mod reverse;
mod strpos;
mod substr;
mod substr_index;
mod translate;

criterion_main!(
    character_length::benches,
    find_in_set::benches,
    find_in_set_literal::benches,
    initcap::benches,
    left_right::benches,
    levenshtein::benches,
    pad::benches,
    reverse::benches,
    strpos::benches,
    substr::benches,
    substr_index::benches,
    translate::benches,
);
