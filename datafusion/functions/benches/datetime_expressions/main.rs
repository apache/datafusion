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

//! Benchmarks for functions behind the `datetime_expressions` feature.
//!
//! Each benchmark beside this file is a module rather than its own `[[bench]]`
//! target to reduce compile times and output size.
//!
//! Run one of them with `cargo bench --bench datetime_expressions -- <filter>`.

use criterion::criterion_main;

mod date_bin;
mod date_part;
mod date_trunc;
mod make_date;
mod to_char;
mod to_local_time;
mod to_time;
mod to_timestamp;

criterion_main!(
    date_bin::benches,
    date_part::benches,
    date_trunc::benches,
    make_date::benches,
    to_char::benches,
    to_local_time::benches,
    to_time::benches,
    to_timestamp::benches,
);
