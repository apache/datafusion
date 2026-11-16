// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

#![doc(
    html_logo_url = "https://raw.githubusercontent.com/apache/datafusion/19fe44cf2f30cbdd63d4a4f52c74055163c6cc38/docs/logos/standalone_logo/logo_original.svg",
    html_favicon_url = "https://raw.githubusercontent.com/apache/datafusion/19fe44cf2f30cbdd63d4a4f52c74055163c6cc38/docs/logos/standalone_logo/logo_original.svg"
)]
#![cfg_attr(docsrs, feature(doc_cfg))]
// Make sure fast / cheap clones on Arc are explicit:
// https://github.com/apache/datafusion/issues/11143
#![deny(clippy::clone_on_ref_ptr)]
#![cfg_attr(test, allow(clippy::needless_pass_by_value))]

//! This crate provides:
//!
//! 1. A SQL parser, [`DFParser`], that translates SQL query text into
//!    an abstract syntax tree (AST), [`Statement`].
//!
//! 2. A SQL query planner [`SqlToRel`] that creates [`LogicalPlan`]s
//!    from [`Statement`]s.
//!
//! 3. A SQL [`unparser`] that converts [`Expr`]s and [`LogicalPlan`]s
//!    into SQL query text.
//!
//! [`DFParser`]: parser::DFParser
//! [`Statement`]: parser::Statement
//! [`SqlToRel`]: planner::SqlToRel
//! [`LogicalPlan`]: datafusion_expr::logical_plan::LogicalPlan
//! [`Expr`]: datafusion_expr::expr::Expr

mod cte;
mod expr;
pub mod parser;
pub mod planner;
mod query;
mod relation;
pub mod resolve;
mod select;
mod set_expr;
mod stack;
mod statement;
#[cfg(feature = "unparser")]
pub mod unparser;
pub mod utils;
mod values;
pub use sqlparser;
