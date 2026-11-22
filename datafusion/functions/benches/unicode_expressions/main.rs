// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

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
