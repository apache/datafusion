// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Repository layout utilities.
//!
//! This module provides a small helper (`RepoLayout`) that encapsulates
//! knowledge about the DataFusion repository structure, in particular
//! where example groups are located relative to the repository root.

use std::path::{Path, PathBuf};

use datafusion::error::{DataFusionError, Result};

/// Describes the layout of a DataFusion repository.
///
/// This type centralizes knowledge about where example-related
/// directories live relative to the repository root.
#[derive(Debug, Clone)]
pub struct RepoLayout {
    root: PathBuf,
}

impl From<&Path> for RepoLayout {
    fn from(path: &Path) -> Self {
        Self {
            root: path.to_path_buf(),
        }
    }
}

impl RepoLayout {
    /// Creates a layout from an explicit repository root.
    pub fn from_root(root: PathBuf) -> Self {
        Self { root }
    }

    /// Detects the repository root based on `CARGO_MANIFEST_DIR`.
    ///
    /// This is intended for use from binaries inside the workspace.
    pub fn detect() -> Result<Self> {
        let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));

        let root = manifest_dir.parent().ok_or_else(|| {
            DataFusionError::Execution(
                "CARGO_MANIFEST_DIR does not have a parent".to_string(),
            )
        })?;

        Ok(Self {
            root: root.to_path_buf(),
        })
    }

    /// Returns the repository root directory.
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Returns the `datafusion-examples/examples` directory.
    pub fn examples_root(&self) -> PathBuf {
        self.root.join("datafusion-examples").join("examples")
    }

    /// Returns the directory for a single example group.
    ///
    /// Example: `examples/udf`
    pub fn example_group_dir(&self, group: &str) -> PathBuf {
        self.examples_root().join(group)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detect_sets_non_empty_root() -> Result<()> {
        let layout = RepoLayout::detect()?;
        assert!(!layout.root().as_os_str().is_empty());
        Ok(())
    }

    #[test]
    fn examples_root_is_under_repo_root() -> Result<()> {
        let layout = RepoLayout::detect()?;
        let examples_root = layout.examples_root();
        assert!(examples_root.starts_with(layout.root()));
        assert!(examples_root.ends_with("datafusion-examples/examples"));
        Ok(())
    }

    #[test]
    fn example_group_dir_appends_group_name() -> Result<()> {
        let layout = RepoLayout::detect()?;
        let group_dir = layout.example_group_dir("foo");
        assert!(group_dir.ends_with("datafusion-examples/examples/foo"));
        Ok(())
    }
}
