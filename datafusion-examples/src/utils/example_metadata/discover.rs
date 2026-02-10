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

//! Utilities for discovering example groups in the repository filesystem.
//!
//! An example group is defined as a directory containing a `main.rs` file
//! under the examples root. This module is intentionally filesystem-focused
//! and does not perform any parsing or rendering.

use std::fs;
use std::path::{Path, PathBuf};

use datafusion::error::Result;

/// Discovers all example group directories under the given root.
///
/// A directory is considered an example group if it contains a `main.rs` file.
pub fn discover_example_groups(root: &Path) -> Result<Vec<PathBuf>> {
    let mut groups = Vec::new();
    for entry in fs::read_dir(root)? {
        let entry = entry?;
        let path = entry.path();

        if path.is_dir() && path.join("main.rs").exists() {
            groups.push(path);
        }
    }
    groups.sort();
    Ok(groups)
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
