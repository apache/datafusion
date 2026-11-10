// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

/// Run all tests that are found in the `user_defined` directory
mod user_defined;

#[cfg(test)]
#[ctor::ctor(unsafe)]
fn init() {
    // Enable RUST_LOG logging configuration for test
    let _ = env_logger::try_init();
}
