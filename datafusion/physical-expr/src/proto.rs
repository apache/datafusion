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

//! The session side of decoding a `PhysicalExpr` from protobuf.
//!
//! The decode context in `datafusion-physical-expr-common` is generic over its
//! session type: a decoder may need the session, and what it needs —
//! [`FunctionRegistry`] and [`ConfigOptions`] — is defined in crates above
//! `physical-expr-common` but below this one. This module supplies
//! [`ExprDecodeSession`], which bundles exactly those two, and the built-in
//! expressions decode under it. `datafusion-proto` builds one from the
//! `TaskContext` it decodes under.

use datafusion_common::config::ConfigOptions;
use datafusion_expr::registry::FunctionRegistry;
pub use datafusion_physical_expr_common::physical_expr::proto_decode::PhysicalExprDecodeCtx;

/// What an expression decoder can see of the session it runs under: the
/// function registry, for resolving UDFs by name, and the configuration
/// options.
///
/// This is the `S` in [`PhysicalExprDecodeCtx<'_, S>`] that the built-in
/// expressions decode under. It is a pair of references rather
/// than a `TaskContext` because `datafusion-execution` is not a dependency of
/// this crate — `physical-expr` and `execution` are siblings above
/// `datafusion-expr` — and nothing an expression decoder does needs the rest
/// of the task context. `datafusion-proto` builds one from the `TaskContext`
/// it decodes under.
#[derive(Clone, Copy)]
pub struct ExprDecodeSession<'a> {
    functions: &'a dyn FunctionRegistry,
    config: &'a ConfigOptions,
}

impl<'a> ExprDecodeSession<'a> {
    /// Bundle a function registry and configuration options.
    pub fn new(functions: &'a dyn FunctionRegistry, config: &'a ConfigOptions) -> Self {
        Self { functions, config }
    }

    /// The function registry, for resolving UDFs by name.
    pub fn function_registry(&self) -> &'a dyn FunctionRegistry {
        self.functions
    }

    /// The session's configuration options.
    pub fn config_options(&self) -> &'a ConfigOptions {
        self.config
    }
}

impl std::fmt::Debug for ExprDecodeSession<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ExprDecodeSession").finish_non_exhaustive()
    }
}
