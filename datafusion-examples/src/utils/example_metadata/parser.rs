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

//! Parser for example metadata embedded in `main.rs` documentation comments.
//!
//! This module scans `//!` doc comments to extract example subcommands
//! and their associated metadata (file name and description), enforcing
//! a strict ordering and structure to avoid ambiguous documentation.

use std::path::Path;
use std::{collections::HashSet, fs};

use datafusion_common::{DataFusionError, Result};
use nom::{
    IResult, Parser,
    bytes::complete::{tag, take_until, take_while},
    character::complete::multispace0,
    combinator::all_consuming,
    sequence::{delimited, preceded},
};

use crate::utils::example_metadata::ExampleEntry;

/// Parsing state machine used while scanning `main.rs` docs.
///
/// This makes the "subcommand - metadata" relationship explicit:
/// metadata is only valid immediately after a subcommand has been seen.
enum ParserState<'a> {
    /// Not currently expecting metadata.
    Idle,
    /// A subcommand was just parsed; the next valid metadata (if any)
    /// must belong to this subcommand.
    SeenSubcommand(&'a str),
}

/// Parses a subcommand declaration line from `main.rs` docs.
///
/// Expected format:
/// ```text
/// //! - `<subcommand>`
/// ```
fn parse_subcommand_line(input: &str) -> IResult<&str, &str> {
    let parser = preceded(
        multispace0,
        delimited(tag("//! - `"), take_until("`"), tag("`")),
    );
    all_consuming(parser).parse(input)
}

/// Parses example metadata (file name and description) from `main.rs` docs.
///
/// Expected format:
/// ```text
/// //! (file: <file>.rs, desc: <description>)
/// ```
fn parse_metadata_line(input: &str) -> IResult<&str, (&str, &str)> {
    let parser = preceded(
        multispace0,
        preceded(tag("//!"), preceded(multispace0, take_while(|_| true))),
    );
    let (rest, payload) = all_consuming(parser).parse(input)?;

    let content = payload
        .strip_prefix("(")
        .and_then(|s| s.strip_suffix(")"))
        .ok_or_else(|| {
            nom::Err::Error(nom::error::Error::new(payload, nom::error::ErrorKind::Tag))
        })?;

    let (file, desc) = content
        .strip_prefix("file:")
        .ok_or_else(|| {
            nom::Err::Error(nom::error::Error::new(payload, nom::error::ErrorKind::Tag))
        })?
        .split_once(", desc:")
        .ok_or_else(|| {
            nom::Err::Error(nom::error::Error::new(payload, nom::error::ErrorKind::Tag))
        })?;

    Ok((rest, (file.trim(), desc.trim())))
}

/// Parses example entries from a group's `main.rs` file.
pub fn parse_main_rs_docs(path: &Path) -> Result<Vec<ExampleEntry>> {
    let content = fs::read_to_string(path)?;
    let mut entries = vec![];
    let mut state = ParserState::Idle;
    let mut seen_subcommands = HashSet::new();

    for (line_no, raw_line) in content.lines().enumerate() {
        let line = raw_line.trim();

        // Try parsing subcommand, excluding `all` because it's not used in README
        if let Ok((_, sub)) = parse_subcommand_line(line) {
            state = if sub == "all" {
                ParserState::Idle
            } else {
                ParserState::SeenSubcommand(sub)
            };
            continue;
        }

        // Try parsing metadata
        if let Ok((_, (file, desc))) = parse_metadata_line(line) {
            let subcommand = match state {
                ParserState::SeenSubcommand(s) => s,
                ParserState::Idle => {
                    return Err(DataFusionError::Execution(format!(
                        "Metadata without preceding subcommand at {}:{}",
                        path.display(),
                        line_no + 1
                    )));
                }
            };

            if !seen_subcommands.insert(subcommand) {
                return Err(DataFusionError::Execution(format!(
                    "Duplicate metadata for subcommand `{subcommand}`"
                )));
            }

            entries.push(ExampleEntry {
                subcommand: subcommand.to_string(),
                file: file.to_string(),
                desc: desc.to_string(),
            });

            state = ParserState::Idle;
            continue;
        }

        // If a non-blank doc line interrupts a pending subcommand, reset the state
        if let ParserState::SeenSubcommand(_) = state
            && is_non_blank_doc_line(line)
        {
            state = ParserState::Idle;
        }
    }

    Ok(entries)
}

/// Returns `true` for non-blank Rust doc comment lines (`//!`).
///
/// Used to detect when a subcommand is interrupted by unrelated documentation,
/// so metadata is only accepted immediately after a subcommand (blank doc lines
/// are allowed in between).
fn is_non_blank_doc_line(line: &str) -> bool {
    line.starts_with("//!") && !line.trim_start_matches("//!").trim().is_empty()
}

#[cfg(test)]
mod tests {
    use super::*;

    use tempfile::TempDir;

    #[test]
    fn parse_subcommand_line_accepts_valid_input() {
        let line = "//! - `date_time`";
        let sub = parse_subcommand_line(line);
        assert_eq!(sub, Ok(("", "date_time")));
    }

    #[test]
    fn parse_subcommand_line_invalid_inputs() {
        let err_lines = [
            "//! - ",
            "//! - foo",
            "//! - `foo` bar",
            "//! --",
            "//!-",
            "//!--",
            "//!",
            "//",
            "/",
            "",
        ];
        for line in err_lines {
            assert!(
                parse_subcommand_line(line).is_err(),
                "expected error for input: {line}"
            );
        }
    }

    #[test]
    fn parse_metadata_line_accepts_valid_input() {
        let line =
            "//! (file: date_time.rs, desc: Examples of date-time related functions)";
        let res = parse_metadata_line(line);
        assert_eq!(
            res,
            Ok((
                "",
                ("date_time.rs", "Examples of date-time related functions")
            ))
        );

        let line = "//! (file: foo.rs, desc: Foo, bar, baz)";
        let res = parse_metadata_line(line);
        assert_eq!(res, Ok(("", ("foo.rs", "Foo, bar, baz"))));

        let line = "//! (file: foo.rs, desc: Foo(FOO))";
        let res = parse_metadata_line(line);
        assert_eq!(res, Ok(("", ("foo.rs", "Foo(FOO)"))));
    }

    #[test]
    fn parse_metadata_line_invalid_inputs() {
        let bad_lines = [
            "//! (file: foo.rs)",
            "//! (desc: missing file)",
            "//! file: foo.rs, desc: test",
            "//! file: foo.rs,desc: test",
            "//! (file: foo.rs desc: test)",
            "//! (file: foo.rs,desc: test)",
            "//! (desc: test, file: foo.rs)",
            "//! ()",
            "//! (file: foo.rs, desc: test) extra",
            "",
        ];
        for line in bad_lines {
            assert!(
                parse_metadata_line(line).is_err(),
                "expected error for input: {line}"
            );
        }
    }

    #[test]
    fn parse_main_rs_docs_extracts_entries() -> Result<()> {
        let tmp = TempDir::new().unwrap();
        let main_rs = tmp.path().join("main.rs");

        fs::write(
            &main_rs,
            r#"
        //! - `foo`
        //! (file: foo.rs, desc: first example)
        //!
        //! - `bar`
        //! (file: bar.rs, desc: second example)
        "#,
        )?;

        let entries = parse_main_rs_docs(&main_rs)?;

        assert_eq!(entries.len(), 2);

        assert_eq!(entries[0].subcommand, "foo");
        assert_eq!(entries[0].file, "foo.rs");
        assert_eq!(entries[0].desc, "first example");

        assert_eq!(entries[1].subcommand, "bar");
        assert_eq!(entries[1].file, "bar.rs");
        assert_eq!(entries[1].desc, "second example");
        Ok(())
    }
}
