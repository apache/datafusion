// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

#![doc(
    html_logo_url = "https://raw.githubusercontent.com/apache/datafusion/19fe44cf2f30cbdd63d4a4f52c74055163c6cc38/docs/logos/standalone_logo/logo_original.svg",
    html_favicon_url = "https://raw.githubusercontent.com/apache/datafusion/19fe44cf2f30cbdd63d4a4f52c74055163c6cc38/docs/logos/standalone_logo/logo_original.svg"
)]
#![cfg_attr(docsrs, feature(doc_cfg))]
#![cfg_attr(doc, doc = include_str!("../README.md"))]
pub const DATAFUSION_CLI_VERSION: &str = env!("CARGO_PKG_VERSION");

pub mod catalog;
pub mod cli_context;
pub mod command;
pub mod exec;
pub mod functions;
pub mod helper;
pub mod highlighter;
pub mod object_storage;
pub mod pool_type;
pub mod print_format;
pub mod print_options;
