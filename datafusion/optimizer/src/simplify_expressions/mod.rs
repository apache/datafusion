// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! [`SimplifyExpressions`] simplifies expressions in the logical plan,
//! [`ExprSimplifier`] simplifies individual `Expr`s.

mod cast_preimage;
pub mod expr_simplifier;
mod inlist_simplifier;
mod linear_aggregates;
mod regex;
mod reorder_predicates;
pub mod simplify_exprs;
pub mod simplify_literal;
mod simplify_predicates;
mod udf_preimage;
mod utils;

// backwards compatibility
pub use datafusion_expr::simplify::SimplifyContext;

pub use expr_simplifier::*;
pub(crate) use reorder_predicates::reorder_predicates;
pub use simplify_exprs::*;
pub use simplify_predicates::simplify_predicates;

// Export for test in datafusion/core/tests/optimizer_integration.rs
pub use datafusion_expr::expr_rewriter::GuaranteeRewriter;
