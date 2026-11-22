// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

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

pub mod discover;
pub mod layout;
pub mod model;
pub mod parser;
pub mod render;

#[cfg(test)]
pub mod test_utils;

pub use layout::RepoLayout;
pub use model::{Category, ExampleEntry, ExampleGroup, GroupName};
pub use parser::parse_main_rs_docs;
pub use render::generate_examples_readme;
