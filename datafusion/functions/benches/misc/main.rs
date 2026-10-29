// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Benchmarks for functions without a feature gate.
//!
//! Each benchmark beside this file is a module rather than its own `[[bench]]`
//! target to reduce compile times and output size.
//!
//! Run one of them with `cargo bench --bench misc -- <filter>`.

use criterion::criterion_main;

mod get_field;
#[path = "../helper.rs"]
mod helper;
mod nullif;
mod overlay;

criterion_main!(get_field::benches, nullif::benches, overlay::benches);
