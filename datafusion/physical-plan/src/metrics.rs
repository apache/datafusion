// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Metrics live in `datafusion-physical-expr-common`; this module re-exports
//! them to keep the public APIs stable.

pub use datafusion_physical_expr_common::metrics::*;
