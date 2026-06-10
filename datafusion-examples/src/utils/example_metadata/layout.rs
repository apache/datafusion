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
