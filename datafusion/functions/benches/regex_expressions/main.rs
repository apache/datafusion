// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Benchmarks for functions behind the `regex_expressions` feature.
//!
//! Each benchmark beside this file is a module rather than its own `[[bench]]`
//! target to reduce compile times and output size.
//!
//! Run one of them with `cargo bench --bench regex_expressions -- <filter>`.

use criterion::criterion_main;

mod regexp_count;
mod regexp_instr;
mod regexp_match;
mod regx;

criterion_main!(
    regexp_count::benches,
    regexp_instr::benches,
    regexp_match::benches,
    regx::benches,
);
