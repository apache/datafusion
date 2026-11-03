// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Tests too slow to run on every PR: fuzz tests and memory limit validation.
//! This target requires the `extended_tests` feature.
//!
//! Run with: cargo test -p datafusion --test extended_tests --features extended_tests

mod fuzz_cases;
mod memory_limit_validation;

#[cfg(test)]
#[ctor::ctor(unsafe)]
fn init() {
    // Enable RUST_LOG logging configuration for test
    let _ = env_logger::try_init();
}
