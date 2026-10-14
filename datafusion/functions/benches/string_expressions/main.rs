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

//! Benchmarks for functions behind the `string_expressions` feature.
//!
//! Each benchmark beside this file is a module rather than its own `[[bench]]`
//! target to reduce compile times and output size.
//!
//! Run one of them with `cargo bench --bench string_expressions -- <filter>`.

use criterion::criterion_main;

mod ascii;
mod chr;
mod concat;
mod concat_ws;
mod contains;
mod ends_with;
#[path = "../helper.rs"]
mod helper;
mod lower;
mod repeat;
mod replace;
mod replace_scalar;
mod split_part;
mod starts_with;
mod to_hex;
mod trim;
mod upper_unicode;
mod uuid;

criterion_main!(
    ascii::benches,
    chr::benches,
    concat::benches,
    concat_ws::benches,
    contains::benches,
    ends_with::benches,
    lower::benches,
    repeat::benches,
    replace::benches,
    replace_scalar::benches,
    split_part::benches,
    starts_with::benches,
    to_hex::benches,
    trim::benches,
    upper_unicode::benches,
    uuid::benches,
);
