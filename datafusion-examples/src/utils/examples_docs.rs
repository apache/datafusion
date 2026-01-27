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

//! Documentation generator for DataFusion examples.
//!
//! # Design goals
//!
//! - Keep README.md in sync with runnable examples
//! - Fail fast on malformed documentation
//!
//! # Overview
//!
//! Each example group corresponds to a directory under
//! `datafusion-examples/examples/<group>` containing a `main.rs` file.
//! Documentation is extracted from structured `//!` comments in that file.
//!
//! For each example group, the generator produces:
//!
//! ```text
//! ## <Group Name> Examples
//! ### Group: `<group>`
//! #### Category: Single Process | Distributed
//!
//! | Subcommand | File Path | Description |
//! ```
//!
//! # Usage
//!
//! Generate documentation for a single group only:
//!
//! ```bash
//! cargo run --bin examples-docs -- dataframe
//! ```
//!
//! Generate documentation for all examples:
//!
//! ```bash
//! cargo run --bin examples-docs  
//! ```

use std::fs;
use std::path::{Path, PathBuf};

use datafusion::error::{DataFusionError, Result};

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

const ABBREVIATIONS: &[(&str, &str)] = &[
    ("dataframe", "DataFrame"),
    ("io", "IO"),
    ("sql", "SQL"),
    ("udf", "UDF"),
];

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

/// A group of related examples (e.g. `builtin_functions`, `udf`).
///
/// Each group corresponds to a directory containing a `main.rs` file
/// with structured documentation comments.
#[derive(Debug)]
pub struct ExampleGroup {
    pub name: GroupName,
    pub examples: Vec<ExampleEntry>,
    pub category: Category,
}

impl ExampleGroup {
    /// Parses an example group from its directory.
    ///
    /// The group name is derived from the directory name, and example
    /// entries are extracted from `main.rs`.
    pub fn from_dir(dir: &Path, category: Category) -> Result<Self> {
        let raw_name = dir
            .file_name()
            .and_then(|s| s.to_str())
            .ok_or_else(|| {
                DataFusionError::Execution("Invalid example group dir".to_string())
            })?
            .to_string();

        let name = GroupName::from_dir_name(raw_name);
        let main_rs = dir.join("main.rs");
        let examples = parse_main_rs_docs(&main_rs)?;

        Ok(Self {
            name,
            examples,
            category,
        })
    }

    // Renders this example group as a Markdown section.
    pub fn render_markdown(&self) -> String {
        let mut out = String::new();
        out.push_str(&format!("\n## {} Examples\n\n", self.name.title()));
        out.push_str(&format!("### Group: `{}`\n\n", self.name.raw()));
        out.push_str(&format!("#### Category: {}\n\n", self.category.name()));
        out.push_str("| Subcommand | File Path | Description |\n");
        out.push_str("| --- | --- | --- |\n");

        for ex in &self.examples {
            out.push_str(&format!(
                "| {} | [`{}/{}`](examples/{}/{}) | {} |\n",
                ex.subcommand,
                self.name.raw(),
                ex.file,
                self.name.raw(),
                ex.file,
                ex.desc
            ));
        }

        out
    }
}

/// Represents an example group name in both raw and human-readable forms.
///
/// For example:
/// - raw: `builtin_functions`
/// - title: `Builtin Functions`
#[derive(Debug)]
pub struct GroupName {
    raw: String,
    title: String,
}

impl GroupName {
    /// Creates a group name from a directory name.
    pub fn from_dir_name(raw: String) -> Self {
        let title = raw
            .split('_')
            .map(format_part)
            .collect::<Vec<_>>()
            .join(" ");

        Self { raw, title }
    }

    /// Returns the raw group name (directory name).
    pub fn raw(&self) -> &str {
        &self.raw
    }

    /// Returns a title-cased name for documentation.
    pub fn title(&self) -> &str {
        &self.title
    }
}

/// A single runnable example within a group.
///
/// Each entry corresponds to a subcommand documented in `main.rs`.
#[derive(Debug)]
pub struct ExampleEntry {
    /// CLI subcommand name.
    pub subcommand: String,
    /// Rust source file name.
    pub file: String,
    /// Human-readable description.
    pub desc: String,
}

/// Execution category of an example group.
#[derive(Debug, Default)]
pub enum Category {
    /// Runs in a single process.
    #[default]
    SingleProcess,
    /// Requires a distributed setup.
    Distributed,
}

impl Category {
    /// Returns the display name used in documentation.
    pub fn name(&self) -> &str {
        match self {
            Self::SingleProcess => "Single Process",
            Self::Distributed => "Distributed",
        }
    }

    /// Determines the category for a group by name.
    pub fn for_group(name: &str) -> Self {
        match name {
            "flight" => Category::Distributed,
            _ => Category::SingleProcess,
        }
    }
}

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

/// Parses example entries from a group's `main.rs` file.
pub fn parse_main_rs_docs(path: &Path) -> Result<Vec<ExampleEntry>> {
    let content = fs::read_to_string(path)?;
    let mut entries = Vec::new();
    let mut pending_subcommand: Option<String> = None;

    for raw_line in content.lines() {
        let line = raw_line.trim();

        if let Some(sub) = parse_subcommand_line(line) {
            if sub != "all" {
                pending_subcommand = Some(sub);
            }
            continue;
        }

        if let Some((file, desc)) = parse_metadata_line(line) {
            let subcommand = pending_subcommand.take().ok_or_else(|| {
                DataFusionError::Execution(
                    "Metadata without preceding subcommand".to_string(),
                )
            })?;

            entries.push(ExampleEntry {
                subcommand,
                file,
                desc,
            });
        }
    }

    Ok(entries)
}

/// Parses a subcommand declaration line from `main.rs` docs.
///
/// Expected format:
/// ```text
/// //! - `<subcommand>`
/// ```
fn parse_subcommand_line(line: &str) -> Option<String> {
    line.strip_prefix("//! - `")
        .and_then(|rest| rest.strip_suffix('`'))
        .map(|s| s.to_string())
}

/// Parses example metadata (file name and description) from `main.rs` docs.
///
/// Expected format:
/// ```text
/// //! (file: <file>.rs, desc: <description>)
/// ```
fn parse_metadata_line(line: &str) -> Option<(String, String)> {
    let line = line.strip_prefix("//!").map(str::trim)?;
    if !line.starts_with("(file:") || !line.ends_with(')') {
        return None;
    }
    let inner = line.strip_prefix('(')?.strip_suffix(')')?;
    let (file_part, desc_part) = inner.split_once(", desc:")?;
    let file = file_part.strip_prefix("file:")?.trim().to_string();
    let desc = desc_part.trim().to_string();
    Some((file, desc))
}

/// Discovers all example group directories under the given root.
fn discover_example_groups(root: &Path) -> Result<Vec<PathBuf>> {
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

/// Formats a single group-name segment for display.
///
/// This function applies DataFusion-specific capitalization rules:
/// - Known abbreviations (e.g. `sql`, `io`, `udf`) are rendered in all caps
/// - All other segments fall back to standard Title Case
fn format_part(part: &str) -> String {
    let lower = part.to_ascii_lowercase();

    if let Some((_, replacement)) = ABBREVIATIONS.iter().find(|(k, _)| *k == lower) {
        return replacement.to_string();
    }

    let mut chars = part.chars();
    match chars.next() {
        Some(first) => first.to_uppercase().collect::<String>() + chars.as_str(),
        None => String::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use tempfile::TempDir;

    #[test]
    fn all_subcommand_is_ignored() -> Result<()> {
        let tmp = TempDir::new().unwrap();
        let group_dir = tmp.path().join("foo");
        fs::create_dir(&group_dir)?;

        fs::write(
            group_dir.join("main.rs"),
            r#"
//! - `all`
//!
//! - `foo`
//!   (file: foo.rs, desc: foo example)
"#,
        )?;

        let group = ExampleGroup::from_dir(&group_dir, Category::SingleProcess)?;

        assert_eq!(group.examples.len(), 1);
        assert_eq!(group.examples[0].subcommand, "foo");

        Ok(())
    }

    #[test]
    fn parse_subcommand_line_works() {
        let line = "//! - `date_time`";
        let sub = parse_subcommand_line(line).unwrap();
        assert_eq!(sub, "date_time");
    }

    #[test]
    fn parse_metadata_line_works() {
        let line =
            "//!   (file: date_time.rs, desc: Examples of date-time related functions)";
        let (file, desc) = parse_metadata_line(line).unwrap();
        assert_eq!(file, "date_time.rs");
        assert_eq!(desc, "Examples of date-time related functions");

        let line = "//!   (file: foo.rs, desc: Foo, bar, baz)";
        let (file, desc) = parse_metadata_line(line).unwrap();
        assert_eq!(file, "foo.rs");
        assert_eq!(desc, "Foo, bar, baz");
    }

    #[test]
    fn metadata_without_subcommand_fails() {
        let tmp = TempDir::new().unwrap();
        let group_dir = tmp.path().join("bad");
        fs::create_dir(&group_dir).unwrap();

        fs::write(
            group_dir.join("main.rs"),
            "//! (file: foo.rs, desc: missing subcommand)",
        )
        .unwrap();

        let err =
            ExampleGroup::from_dir(&group_dir, Category::SingleProcess).unwrap_err();

        assert!(
            err.to_string()
                .contains("Metadata without preceding subcommand"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn parse_group_example_works() -> Result<()> {
        let tmp = TempDir::new().unwrap();

        // Simulate: examples/builtin_functions/
        let group_dir = tmp.path().join("builtin_functions");
        fs::create_dir(&group_dir)?;

        // Write a fake main.rs with docs
        let main_rs = group_dir.join("main.rs");
        fs::write(
            &main_rs,
            r#"
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
// 
//! # These are miscellaneous function-related examples
//!
//! These examples demonstrate miscellaneous function-related features.
//!
//! ## Usage
//! ```bash
//! cargo run --example builtin_functions -- [all|date_time|function_factory|regexp]
//! ```
//!
//! Each subcommand runs a corresponding example:
//! - `all` — run all examples included in this module
//!
//! - `date_time`
//!   (file: date_time.rs, desc: Examples of date-time related functions and queries)
//!
//! - `function_factory`  
//!   (file: function_factory.rs, desc: Register `CREATE FUNCTION` handler to implement SQL macros)
//!
//! - `regexp`
//!   (file: regexp.rs, desc: Examples of using regular expression functions)
"#,
        )?;

        let group = ExampleGroup::from_dir(&group_dir, Category::SingleProcess)?;

        // Assert group-level data
        assert_eq!(group.name.title(), "Builtin Functions");
        assert_eq!(group.examples.len(), 3);

        // Assert 1 example
        assert_eq!(group.examples[0].subcommand, "date_time");
        assert_eq!(group.examples[0].file, "date_time.rs");
        assert_eq!(
            group.examples[0].desc,
            "Examples of date-time related functions and queries"
        );

        // Assert 2 example
        assert_eq!(group.examples[1].subcommand, "function_factory");
        assert_eq!(group.examples[1].file, "function_factory.rs");
        assert_eq!(
            group.examples[1].desc,
            "Register `CREATE FUNCTION` handler to implement SQL macros"
        );

        // Assert 3 example
        assert_eq!(group.examples[2].subcommand, "regexp");
        assert_eq!(group.examples[2].file, "regexp.rs");
        assert_eq!(
            group.examples[2].desc,
            "Examples of using regular expression functions"
        );

        Ok(())
    }

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
    }

    #[test]
    fn single_group_generation_fails_if_group_missing() {
        let tmp = TempDir::new().unwrap();
        let layout = RepoLayout::from_root(tmp.path().to_path_buf());

        let err = generate_examples_readme(&layout, Some("missing_group")).unwrap_err();

        assert!(
            err.to_string()
                .contains("Example group `missing_group` does not exist"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn group_name_title_is_human_readable() {
        let name = GroupName::from_dir_name("very_long_group_name".to_string());
        assert_eq!(name.title(), "Very Long Group Name");
    }

    #[test]
    fn group_name_handles_abbreviations() {
        assert_eq!(
            GroupName::from_dir_name("dataframe".to_string()).title(),
            "DataFrame"
        );
        assert_eq!(
            GroupName::from_dir_name("data_io".to_string()).title(),
            "Data IO"
        );
        assert_eq!(
            GroupName::from_dir_name("sql_ops".to_string()).title(),
            "SQL Ops"
        );
        assert_eq!(GroupName::from_dir_name("udf".to_string()).title(), "UDF");
    }
}
