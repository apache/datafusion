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

//! Markdown renderer for DataFusion example documentation.
//!
//! This module takes parsed example metadata and generates the
//! `README.md` content for `datafusion-examples`, including group
//! sections and example tables.

use std::path::PathBuf;

use datafusion::error::{DataFusionError, Result};

use crate::utils::example_metadata::discover::discover_example_groups;
use crate::utils::example_metadata::model::ExampleGroup;
use crate::utils::example_metadata::{Category, RepoLayout};

const STATIC_HEADER: &str = r#"<!---
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# DataFusion Examples

This crate includes end to end, highly commented examples of how to use
various DataFusion APIs to help you get started.

## Prerequisites

Run `git submodule update --init` to init test files.

## Running Examples

To run an example, use the `cargo run` command, such as:

```bash
git clone https://github.com/apache/datafusion
cd datafusion
# Download test data
git submodule update --init

# Change to the examples directory
cd datafusion-examples/examples

# Run all examples in a group
cargo run --example <group> -- all

# Run a specific example within a group
cargo run --example <group> -- <subcommand>

# Run all examples in the `dataframe` group
cargo run --example dataframe -- all

# Run a single example from the `dataframe` group
# (apply the same pattern for any other group)
cargo run --example dataframe -- dataframe
```
"#;

/// Well-known abbreviations used to preserve correct capitalization
/// when generating human-readable documentation titles.
pub const ABBREVIATIONS: &[(&str, &str)] = &[
    ("dataframe", "DataFrame"),
    ("io", "IO"),
    ("sql", "SQL"),
    ("udf", "UDF"),
];

/// Generates Markdown documentation for DataFusion examples.
///
/// If `group` is `None`, documentation is generated for all example groups.
/// If `group` is `Some`, only that group is rendered.
///
/// # Errors
///
/// Returns an error if:
/// - the requested group does not exist
/// - a `main.rs` file is missing
/// - documentation comments are malformed
pub fn generate_examples_readme(
    layout: &RepoLayout,
    group: Option<&str>,
) -> Result<String> {
    let examples_root = layout.examples_root();

    let mut out = String::new();
    out.push_str(STATIC_HEADER);

    let group_dirs: Vec<PathBuf> = match group {
        Some(name) => {
            let dir = examples_root.join(name);
            if !dir.is_dir() {
                return Err(DataFusionError::Execution(format!(
                    "Example group `{name}` does not exist"
                )));
            }
            vec![dir]
        }
        None => discover_example_groups(&examples_root)?,
    };

    for group_dir in group_dirs {
        let raw_name =
            group_dir
                .file_name()
                .and_then(|s| s.to_str())
                .ok_or_else(|| {
                    DataFusionError::Execution("Invalid example group dir".to_string())
                })?;

        let category = Category::for_group(raw_name);
        let group = ExampleGroup::from_dir(&group_dir, category)?;

        out.push_str(&group.render_markdown());
    }

    Ok(out)
}

impl ExampleGroup {
    /// Renders this example group as a Markdown section for the README.
    pub fn render_markdown(&self) -> String {
        let mut out = String::new();
        out.push_str(&format!("\n## {} Examples\n\n", self.name.title()));
        out.push_str(&format!("### Group: `{}`\n\n", self.name.raw()));
        out.push_str(&format!("#### Category: {}\n\n", self.category.name()));
        out.push_str("| Subcommand | File Path | Description |\n");
        out.push_str("| --- | --- | --- |\n");

        for example in &self.examples {
            out.push_str(&format!(
                "| {} | [`{}/{}`](examples/{}/{}) | {} |\n",
                example.subcommand,
                self.name.raw(),
                example.file,
                self.name.raw(),
                example.file,
                example.desc
            ));
        }

        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::utils::example_metadata::test_utils::assert_exec_err_contains;

    use std::fs;

    use tempfile::TempDir;

    #[test]
    fn single_group_generation_works() {
        let tmp = TempDir::new().unwrap();
        // Fake repo root
        let layout = RepoLayout::from_root(tmp.path().to_path_buf());

        // Create: datafusion-examples/examples/builtin_functions
        let examples_dir = layout.example_group_dir("builtin_functions");
        fs::create_dir_all(&examples_dir).unwrap();

        fs::write(
            examples_dir.join("main.rs"),
            "//! - `x`\n//! (file: foo.rs, desc: test)",
        )
        .unwrap();

        let out = generate_examples_readme(&layout, Some("builtin_functions")).unwrap();
        assert!(out.contains("Builtin Functions"));
        assert!(out.contains("| x | [`builtin_functions/foo.rs`]"));
    }

    #[test]
    fn single_group_generation_fails_if_group_missing() {
        let tmp = TempDir::new().unwrap();
        let layout = RepoLayout::from_root(tmp.path().to_path_buf());
        let err = generate_examples_readme(&layout, Some("missing_group")).unwrap_err();
        assert_exec_err_contains(err, "Example group `missing_group` does not exist");
    }
}
