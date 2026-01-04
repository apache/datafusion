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

//! [`SimplifyExpressions`] simplifies expressions in the logical plan,
//! [`ExprSimplifier`] simplifies individual `Expr`s.

pub mod expr_simplifier;
mod inlist_simplifier;
mod regex;
pub mod simplify_exprs;
pub mod simplify_literal;
mod simplify_predicates;
mod unwrap_cast;
mod utils;

// backwards compatibility
pub use datafusion_expr::simplify::{SimplifyContext, SimplifyInfo};

pub use expr_simplifier::*;
pub use simplify_exprs::*;
pub use simplify_predicates::simplify_predicates;

// Export for test in datafusion/core/tests/optimizer_integration.rs
pub use datafusion_expr::expr_rewriter::GuaranteeRewriter;
