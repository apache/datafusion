// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Logical Expr types and traits for [DataFusion]
//!
//! This crate contains types and traits that are used by both Logical and Physical expressions.
//! They are kept in their own crate to avoid physical expressions depending on logical expressions.
//!
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
#![cfg_attr(test, allow(clippy::needless_pass_by_value))]

pub mod accumulator;
pub mod casts;
pub mod columnar_value;
pub mod dyn_eq;
pub mod groups_accumulator;
pub mod interval_arithmetic;
pub mod operator;
pub mod placement;
pub mod signature;
pub mod sort_properties;
pub mod statistics;
pub mod type_coercion;

pub use placement::ExpressionPlacement;
