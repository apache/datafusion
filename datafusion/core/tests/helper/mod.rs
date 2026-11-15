// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Shared helpers for the `core_integration` test crate.
//!
//! Keep cross-cutting test utilities here when they are used by multiple test
//! modules under `core/tests`. Placing them in this submodule avoids creating
//! an additional Cargo integration test target for each helper file.
pub(crate) mod plan_metrics;
