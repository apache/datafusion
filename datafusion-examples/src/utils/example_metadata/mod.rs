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
