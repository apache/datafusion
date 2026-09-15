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

//! The decode half of [`PhysicalExpr::try_to_proto`]: [`PhysicalExprFromProto`],
//! the contract every self-serializing expression implements.
//!
//! These live here rather than next to the encode context in
//! `datafusion-physical-expr-common` because a decoder may need the session,
//! and what it needs — [`FunctionRegistry`] and [`ConfigOptions`] — is defined
//! in crates above `physical-expr-common` but below this one. The decode
//! context is generic over its session type for that reason; this module
//! fixes it to [`ExprDecodeSession`], which bundles exactly those two.
//! `datafusion-proto` builds one from the `TaskContext` it decodes under.

use std::sync::Arc;

use datafusion_common::Result;
use datafusion_common::config::ConfigOptions;
use datafusion_expr::registry::FunctionRegistry;
use datafusion_physical_expr_common::physical_expr::PhysicalExpr;
pub use datafusion_physical_expr_common::physical_expr::proto_decode::PhysicalExprDecodeCtx;
use datafusion_proto_models::protobuf::PhysicalExprNode;

/// What an expression decoder can see of the session it runs under: the
/// function registry, for resolving UDFs by name, and the configuration
/// options.
///
/// This is the `S` in [`PhysicalExprDecodeCtx<'_, S>`] for every
/// [`PhysicalExprFromProto`] implementor. It is a pair of references rather
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

/// The decode half of [`PhysicalExpr::try_to_proto`]: an expression type
/// that can be rebuilt from the `PhysicalExprNode` its `try_to_proto` wrote.
///
/// Every self-serializing expression implements this. Built-in expressions
/// are dispatched to their `try_from_proto` by their `ExprType` variant, so
/// for them [`NAME`](Self::NAME) is informational. A third-party expression
/// shares the single `PhysicalExtensionExprNode` variant with every other
/// extension, so it is dispatched by `NAME` instead.
///
/// The encode half is [`PhysicalExpr::try_to_proto`], which writes a
/// `PhysicalExtensionExprNode` carrying the payload, the children (encoded
/// through the context) and [`NAME`](Self::NAME):
///
/// ```ignore
/// impl PhysicalExprFromProto for MyExpr {
///     const NAME: &'static str = "my_crate.MyExpr";
///
///     fn try_from_proto(
///         node: &PhysicalExprNode,
///         ctx: &PhysicalExprDecodeCtx<'_, ExprDecodeSession<'_>>,
///     ) -> Result<Arc<dyn PhysicalExpr>> {
///         let extension = expect_expr_variant!(
///             node,
///             physical_expr_node::ExprType::Extension,
///             "Extension",
///         );
///         // `extension.expr` is the payload `try_to_proto` wrote; `inputs`
///         // are the children.
///         let state = MyState::from_bytes(&extension.expr)?;
///         let children = ctx.decode_children_expressions(&extension.inputs)?;
///         Ok(Arc::new(MyExpr::new(state, children)))
///     }
/// }
/// ```
///
pub trait PhysicalExprFromProto: PhysicalExpr + Sized {
    /// The expression type's name.
    ///
    /// For an extension expression this is the wire discriminator and the
    /// registry key: namespace it — `"my_crate.MyExpr"`, not `"MyExpr"` —
    /// so that two independent crates registering into the same session
    /// collide at registration time instead of silently decoding each
    /// other's nodes. Built-in expressions use their type name under the
    /// `datafusion.` namespace; it never reaches the wire for them.
    const NAME: &'static str;

    /// Reconstruct the expression from its proto node.
    ///
    /// Takes the whole [`PhysicalExprNode`] — the exact inverse of what
    /// [`PhysicalExpr::try_to_proto`] returns — so the constructor can also
    /// see outer-node fields such as `expr_id`. This is the same signature
    /// the built-in expressions' inherent `try_from_proto` uses.
    ///
    fn try_from_proto(
        node: &PhysicalExprNode,
        ctx: &PhysicalExprDecodeCtx<'_, ExprDecodeSession<'_>>,
    ) -> Result<Arc<dyn PhysicalExpr>>;
}
