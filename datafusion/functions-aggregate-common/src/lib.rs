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

//! Common Aggregate functionality for [DataFusion]
//!
//! This crate contains traits and utilities commonly used to implement aggregate functions
//! They are kept in their own crate to avoid physical expressions depending on logical expressions.
//!
//! [DataFusion]: <https://crates.io/crates/datafusion>

#![doc(
    html_logo_url = "https://raw.githubusercontent.com/apache/datafusion/19fe44cf2f30cbdd63d4a4f52c74055163c6cc38/docs/logos/standalone_logo/logo_original.svg",
    html_favicon_url = "https://raw.githubusercontent.com/apache/datafusion/19fe44cf2f30cbdd63d4a4f52c74055163c6cc38/docs/logos/standalone_logo/logo_original.svg"
)]
#![cfg_attr(docsrs, feature(doc_cfg))]
// Make sure fast / cheap clones on Arc are explicit:
// https://github.com/apache/datafusion/issues/11143
#![deny(clippy::clone_on_ref_ptr)]
// https://github.com/apache/datafusion/issues/18503
#![deny(clippy::needless_pass_by_value)]
#![cfg_attr(test, allow(clippy::needless_pass_by_value))]
// https://github.com/apache/datafusion/issues/18881
#![deny(clippy::allow_attributes)]

pub mod accumulator;
pub mod aggregate;
pub mod merge_arrays;
pub mod min_max;
pub mod noop_accumulator;
pub mod order;
pub mod stats;
pub mod tdigest;
pub mod utils;
