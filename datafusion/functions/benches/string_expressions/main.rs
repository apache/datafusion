// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Benchmarks for functions behind the `string_expressions` feature.
//!
//! Each benchmark beside this file is a module rather than its own `[[bench]]`
//! target to reduce compile times and output size.
//!
//! Run one of them with `cargo bench --bench string_expressions -- <filter>`.

use criterion::criterion_main;

mod ascii;
mod case_conversion_inputs;
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
mod upper;
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
    upper::benches,
    uuid::benches,
);
