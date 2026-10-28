// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

#![cfg_attr(test, allow(clippy::needless_pass_by_value))]
// Make sure fast / cheap clones on Arc are explicit:
// https://github.com/apache/datafusion/issues/11143
#![cfg_attr(not(test), deny(clippy::clone_on_ref_ptr))]

//! [`ArrowFormat`]: Apache Arrow file format abstractions
//!
//! Note: As of DataFusion 54.0.0, the `compression` feature of this crate
//! is a no-op, only kept for backwards compatibility purposes, and it will
//! be removed in a future release.

pub mod file_format;
pub mod source;

pub use file_format::*;
