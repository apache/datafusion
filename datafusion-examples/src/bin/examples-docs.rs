// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Generates Markdown documentation for DataFusion example groups.
//!
//! This binary scans `datafusion-examples/examples`, extracts structured
//! documentation from each group's `main.rs` file, and renders a README-style
//! Markdown document.
//!
//! By default, documentation is generated for all example groups. If a group
//! name is provided as the first CLI argument, only that group is rendered.
//!
//! ## Usage
//!
//! ```bash
//! # Generate docs for all example groups
//! cargo run --bin examples-docs
//!
//! # Generate docs for a single group
//! cargo run --bin examples-docs -- dataframe
//! ```

use datafusion_examples::utils::example_metadata::{
    RepoLayout, generate_examples_readme,
};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let layout = RepoLayout::detect()?;
    let group = std::env::args().nth(1);
    let markdown = generate_examples_readme(&layout, group.as_deref())?;
    print!("{markdown}");
    Ok(())
}
