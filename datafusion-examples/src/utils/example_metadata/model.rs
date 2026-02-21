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

//! Domain model for DataFusion example documentation.
//!
//! This module defines the core data structures used to represent
//! example groups, individual examples, and their categorization
//! as parsed from `main.rs` documentation comments.

use std::path::Path;

use datafusion::error::{DataFusionError, Result};

use crate::utils::example_metadata::parse_main_rs_docs;

/// Well-known abbreviations used to preserve correct capitalization
/// when generating human-readable documentation titles.
const ABBREVIATIONS: &[(&str, &str)] = &[
    ("dataframe", "DataFrame"),
    ("io", "IO"),
    ("sql", "SQL"),
    ("udf", "UDF"),
];

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

    use crate::utils::example_metadata::test_utils::{
        assert_exec_err_contains, example_group_from_docs,
    };

    use std::fs;

    use tempfile::TempDir;

    #[test]
    fn category_for_group_works() {
        assert!(matches!(
            Category::for_group("flight"),
            Category::Distributed
        ));
        assert!(matches!(
            Category::for_group("anything_else"),
            Category::SingleProcess
        ));
    }

    #[test]
    fn all_subcommand_is_ignored() -> Result<()> {
        let group = example_group_from_docs(
            r#"
        //! - `all` — run all examples included in this module
        //!
        //! - `foo`
        //!   (file: foo.rs, desc: foo example)
        "#,
        )?;
        assert_eq!(group.examples.len(), 1);
        assert_eq!(group.examples[0].subcommand, "foo");
        Ok(())
    }

    #[test]
    fn metadata_without_subcommand_fails() {
        let err = example_group_from_docs("//! (file: foo.rs, desc: missing subcommand)")
            .unwrap_err();
        assert_exec_err_contains(err, "Metadata without preceding subcommand");
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

    #[test]
    fn group_name_title_cases() {
        let cases = [
            ("very_long_group_name", "Very Long Group Name"),
            ("foo", "Foo"),
            ("dataframe", "DataFrame"),
            ("data_io", "Data IO"),
            ("sql_ops", "SQL Ops"),
            ("udf", "UDF"),
        ];
        for (input, expected) in cases {
            let name = GroupName::from_dir_name(input.to_string());
            assert_eq!(name.title(), expected);
        }
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
    fn duplicate_metadata_without_repeating_subcommand_fails() {
        let err = example_group_from_docs(
            r#"
        //! - `foo`
        //! (file: a.rs, desc: first)
        //! (file: b.rs, desc: second)
        "#,
        )
        .unwrap_err();
        assert_exec_err_contains(err, "Metadata without preceding subcommand");
    }

    #[test]
    fn duplicate_metadata_for_same_subcommand_fails() {
        let err = example_group_from_docs(
            r#"
        //! - `foo`
        //! (file: a.rs, desc: first)
        //!
        //! - `foo`
        //! (file: b.rs, desc: second)
        "#,
        )
        .unwrap_err();
        assert_exec_err_contains(err, "Duplicate metadata for subcommand `foo`");
    }

    #[test]
    fn metadata_must_follow_subcommand() {
        let err = example_group_from_docs(
            r#"
        //! - `foo`
        //! some unrelated comment
        //! (file: foo.rs, desc: test)
        "#,
        )
        .unwrap_err();
        assert_exec_err_contains(err, "Metadata without preceding subcommand");
    }

    #[test]
    fn preserves_example_order_from_main_rs() -> Result<()> {
        let group = example_group_from_docs(
            r#"
        //! - `second`
        //! (file: second.rs, desc: second example)
        //!
        //! - `first`
        //! (file: first.rs, desc: first example)
        //!
        //! - `third`
        //! (file: third.rs, desc: third example)
        "#,
        )?;

        let subcommands: Vec<&str> = group
            .examples
            .iter()
            .map(|e| e.subcommand.as_str())
            .collect();

        assert_eq!(
            subcommands,
            vec!["second", "first", "third"],
            "examples must preserve the order defined in main.rs"
        );

        Ok(())
    }

    #[test]
    fn metadata_can_follow_blank_doc_line() -> Result<()> {
        let group = example_group_from_docs(
            r#"
        //! - `foo`
        //!
        //! (file: foo.rs, desc: test)
        "#,
        )?;
        assert_eq!(group.examples.len(), 1);
        Ok(())
    }
}
