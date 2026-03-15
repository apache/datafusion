// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Test helpers for example metadata parsing and validation.
//!
//! This module provides small, focused utilities to reduce duplication
//! and keep tests readable across the example metadata submodules.

use std::fs;

use datafusion::error::{DataFusionError, Result};
use tempfile::TempDir;

use crate::utils::example_metadata::{Category, ExampleGroup};

/// Asserts that an `Execution` error contains the expected message fragment.
///
/// Keeps tests focused on semantic error causes without coupling them
/// to full error string formatting.
pub fn assert_exec_err_contains(err: DataFusionError, needle: &str) {
    match err {
        DataFusionError::Execution(msg) => {
            assert!(
                msg.contains(needle),
                "expected '{needle}' in error message, got: {msg}"
            );
        }
        other => panic!("expected Execution error, got: {other:?}"),
    }
}

/// Helper for grammar-focused tests.
///
/// Creates a minimal temporary example group with a single `main.rs`
/// containing the provided docs. Intended for testing parsing and
/// validation rules, not full integration behavior.
pub fn example_group_from_docs(docs: &str) -> Result<ExampleGroup> {
    let tmp = TempDir::new().map_err(|e| {
        DataFusionError::Execution(format!("Failed initializing temp dir: {e}"))
    })?;
    let dir = tmp.path().join("group");
    fs::create_dir(&dir).map_err(|e| {
        DataFusionError::Execution(format!("Failed creating temp dir: {e}"))
    })?;
    fs::write(dir.join("main.rs"), docs).map_err(|e| {
        DataFusionError::Execution(format!("Failed writing to temp file: {e}"))
    })?;
    ExampleGroup::from_dir(&dir, Category::SingleProcess)
}
