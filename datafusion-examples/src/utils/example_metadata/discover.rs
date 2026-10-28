// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Utilities for discovering example groups in the repository filesystem.
//!
//! An example group is defined as a directory containing a `main.rs` file
//! under the examples root. This module is intentionally filesystem-focused
//! and does not perform any parsing or rendering.
//! Discovery fails if no valid example groups are found.

use std::fs;
use std::path::{Path, PathBuf};

use datafusion::common::exec_err;
use datafusion::error::Result;

/// Discovers all example group directories under the given root.
///
/// A directory is considered an example group if it contains a `main.rs` file.
pub fn discover_example_groups(root: &Path) -> Result<Vec<PathBuf>> {
    let mut groups = Vec::new();
    for entry in fs::read_dir(root)? {
        let entry = entry?;
        let path = entry.path();

        if path.is_dir() && path.join("main.rs").is_file() {
            groups.push(path);
        }
    }

    if groups.is_empty() {
        return exec_err!("No example groups found under: {}", root.display());
    }

    groups.sort();
    Ok(groups)
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::utils::example_metadata::test_utils::assert_exec_err_contains;

    use std::fs::{self, File};

    use tempfile::TempDir;

    #[test]
    fn discover_example_groups_finds_dirs_with_main_rs() -> Result<()> {
        let tmp = TempDir::new()?;
        let root = tmp.path();

        // valid example group
        let group1 = root.join("group1");
        fs::create_dir(&group1)?;
        File::create(group1.join("main.rs"))?;

        // not an example group
        let group2 = root.join("group2");
        fs::create_dir(&group2)?;

        let groups = discover_example_groups(root)?;
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0], group1);
        Ok(())
    }

    #[test]
    fn discover_example_groups_errors_if_main_rs_is_a_directory() -> Result<()> {
        let tmp = TempDir::new()?;
        let root = tmp.path();
        let group = root.join("group");
        fs::create_dir(&group)?;
        fs::create_dir(group.join("main.rs"))?;

        let err = discover_example_groups(root).unwrap_err();
        assert_exec_err_contains(err, "No example groups found");
        Ok(())
    }

    #[test]
    fn discover_example_groups_errors_if_none_found() -> Result<()> {
        let tmp = TempDir::new()?;
        let err = discover_example_groups(tmp.path()).unwrap_err();
        assert_exec_err_contains(err, "No example groups found");
        Ok(())
    }
}
