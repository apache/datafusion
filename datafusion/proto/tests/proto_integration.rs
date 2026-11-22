// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

// Test helpers take owned values for convenience, matching the `#![cfg_attr(test, ...)]`
// exemption the DataFusion crates apply to their own unit tests.
#![cfg_attr(test, allow(clippy::needless_pass_by_value))]

/// Run all tests that are found in the `cases` directory
mod cases;
