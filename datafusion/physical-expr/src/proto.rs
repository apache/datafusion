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

//! The session an expression decode runs under, and the `PhysicalExpr`
//! face of the session-scoped [`ProtoDecoderRegistry`] that routes extension
//! expressions to their decoders by name — [`register_physical_expr`],
//! [`decode_physical_expr`] and [`physical_expr_names`], all keyed on
//! `dyn PhysicalExpr`. The store itself is shared with every other extension
//! kind, so a session carries one registry.
//!
//! The decode context in `datafusion-physical-expr-common` is generic over its
//! session type: a decoder may need the session, and what it needs —
//! [`FunctionRegistry`] and [`ConfigOptions`] — is defined in crates above
//! `physical-expr-common` but below this one. This module supplies
//! [`ExprDecodeSession`], which bundles exactly those two, and the built-in
//! expressions decode under it. `datafusion-proto` builds one from the
//! `TaskContext` it decodes under.

use std::sync::Arc;

use datafusion_common::Result;
use datafusion_common::config::ConfigOptions;
use datafusion_expr::registry::FunctionRegistry;
use datafusion_physical_expr_common::physical_expr::PhysicalExpr;
pub use datafusion_physical_expr_common::physical_expr::proto_decode::PhysicalExprDecodeCtx;
pub use datafusion_physical_expr_common::physical_expr::proto_encode::PhysicalExprEncodeCtx;
pub use datafusion_proto_models::ProtoDecoderRegistry;
use datafusion_proto_models::protobuf::PhysicalExprNode;
use datafusion_proto_models::protobuf::physical_expr_node::ExprType;

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
/// The wire name of an extension [`PhysicalExpr`], and the constructor that
/// rebuilds it.
///
/// Only extension expressions implement this. A built-in expression has an
/// `ExprType` variant of its own, keeps the inherent `try_from_proto`
/// dispatched from that variant, and has no wire name to be registered under —
/// so [`register_physical_expr`] will not accept one.
///
/// One impl block says what the expression is called and how it decodes; its
/// `try_to_proto` on [`PhysicalExpr`] writes the matching node with
/// [`extension_expr_node`]:
///
/// ```ignore
/// impl PhysicalExpr for MyExpr {
///     // ...
///     fn try_to_proto(
///         &self,
///         ctx: &PhysicalExprEncodeCtx<'_>,
///     ) -> Result<Option<PhysicalExprNode>> {
///         // Stamps `Self::NAME`, so the encoded name and the registry key
///         // cannot drift apart.
///         Ok(Some(extension_expr_node::<Self>(
///             ctx,
///             self.state.to_bytes()?,
///             self.children(),
///         )?))
///     }
/// }
///
/// impl ExtensionExprFromProto for MyExpr {
///     // Namespace the name with the owning crate, so a collision with
///     // another crate is an error at registration and not a wrong decode.
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
///         let state = MyState::from_bytes(&extension.expr)?;
///         let children = ctx.decode_children_expressions(&extension.inputs)?;
///         Ok(Arc::new(MyExpr::new(state, children)))
///     }
/// }
///
/// let mut registry = ProtoDecoderRegistry::new();
/// register_physical_expr::<MyExpr>(&mut registry)?;
/// let config = SessionConfig::new().with_extension(Arc::new(registry));
/// ```
pub trait ExtensionExprFromProto: PhysicalExpr + Sized {
    /// The name this expression type is written and registered under.
    ///
    /// Namespace it with the owning crate — `"my_crate.MyExpr"`, not
    /// `"MyExpr"` — so that two independent crates registering into the same
    /// session collide at registration time instead of silently decoding each
    /// other's nodes. Never write it by hand on the wire:
    /// [`extension_expr_node`] stamps it for you.
    const NAME: &'static str;

    /// Rebuild the expression from the `PhysicalExprNode` its
    /// [`PhysicalExpr::try_to_proto`] wrote.
    ///
    /// Takes the whole node — the exact inverse of what `try_to_proto`
    /// returns — so the constructor can also see outer-node fields such as
    /// `expr_id`. `ctx.session()` gives the function registry and the
    /// configuration options.
    fn try_from_proto(
        node: &PhysicalExprNode,
        ctx: &PhysicalExprDecodeCtx<'_, ExprDecodeSession<'_>>,
    ) -> Result<Arc<dyn PhysicalExpr>>;
}

/// Build the `PhysicalExprNode` for an extension expression `T`: an
/// `Extension` variant carrying `payload`, the encoded `children`, and `T`'s
/// [`NAME`](ExtensionExprFromProto::NAME).
///
/// The one supported way for an extension expression to write itself. It is a
/// free function rather than a method on [`PhysicalExprEncodeCtx`] because the
/// context lives in `datafusion-physical-expr-common`, below the crate that
/// can name [`ExtensionExprFromProto`] — the same layering that makes the
/// decode context generic over its session.
pub fn extension_expr_node<T: ExtensionExprFromProto>(
    ctx: &PhysicalExprEncodeCtx<'_>,
    payload: Vec<u8>,
    children: Vec<&Arc<dyn PhysicalExpr>>,
) -> Result<PhysicalExprNode> {
    Ok(PhysicalExprNode {
        expr_type: Some(ExprType::Extension(
            datafusion_proto_models::protobuf::PhysicalExtensionExprNode {
                expr: payload,
                inputs: ctx.encode_children_expressions(children)?,
                expr_name: Some(T::NAME.to_string()),
            },
        )),
        ..Default::default()
    })
}

/// How this facade stores a decoder in the shared registry: a function pointer
/// to the monomorphized [`ExtensionExprFromProto::try_from_proto`].
///
/// Deliberately private, and the same type on both the
/// [`register_physical_expr`] and the [`decode_physical_expr`] side.
/// [`ExtensionExprFromProto`] is the public contract and
/// [`decode_physical_expr`] is the public way to invoke one, so this can become
/// something else — a `dyn` decoder object, to admit stateful or closure
/// decoders, which is what an FFI decoder needs — without a breaking change.
type PhysicalExprDecoder = fn(
    &PhysicalExprNode,
    &PhysicalExprDecodeCtx<'_, ExprDecodeSession<'_>>,
) -> Result<Arc<dyn PhysicalExpr>>;

/// Register `T` in `registry` under its [`ExtensionExprFromProto::NAME`].
///
/// Registering the same type twice is a no-op. Registering a *different* type
/// under a name already taken is an error, so collisions surface here rather
/// than as a wrong decode later. The name is scoped to `dyn PhysicalExpr`, so a
/// plan or a data source may use the same name in the same registry.
///
/// # Who builds the registry
///
/// The application that owns the session builds it. A session carries at most
/// one registry, and `SessionConfig::with_extension` replaces what is there, so
/// a library must never attach a registry of its own: it would silently discard
/// another library's. A library exposes a function that fills a registry it is
/// handed, and the application composes them. One registry holds every kind, so
/// a library registers its plans and its expressions into the same object.
///
/// # A built-in expression cannot be registered
///
/// The bound is [`ExtensionExprFromProto`], which only extension expressions
/// implement. A built-in keeps an inherent `try_from_proto`, dispatched from
/// its own `ExprType` variant:
///
/// ```
/// use datafusion_physical_expr::expressions::Column;
///
/// let _ = Column::try_from_proto;
/// ```
///
/// but it is not an extension, and registering it does not compile:
///
/// ```compile_fail
/// use datafusion_physical_expr::expressions::Column;
/// use datafusion_physical_expr::proto::register_physical_expr;
/// use datafusion_proto_models::ProtoDecoderRegistry;
///
/// let mut registry = ProtoDecoderRegistry::new();
/// register_physical_expr::<Column>(&mut registry).unwrap();
/// ```
pub fn register_physical_expr<T: ExtensionExprFromProto>(
    registry: &mut ProtoDecoderRegistry,
) -> Result<()> {
    registry.register_decoder::<dyn PhysicalExpr, T, PhysicalExprDecoder>(
        T::NAME,
        T::try_from_proto,
    )
}

/// Decode `node` with the extension expression decoder registered under the
/// name `node` carries.
///
/// The name is read from the node's `Extension` variant rather than passed in,
/// so a caller cannot pair a node with a name it does not carry.
///
/// `None` means "this node names no extension expression decoder of ours": it
/// is not an extension node, it carries no name, or no registered name matches.
/// The caller then falls back to the `PhysicalExtensionCodec` chain.
/// `Some(Err(..))` means the decoder that *does* own the name failed, which is
/// fatal: falling back there would let a codec decode the payload wrongly, the
/// very thing the name exists to prevent.
pub fn decode_physical_expr(
    registry: &ProtoDecoderRegistry,
    node: &PhysicalExprNode,
    ctx: &PhysicalExprDecodeCtx<'_, ExprDecodeSession<'_>>,
) -> Option<Result<Arc<dyn PhysicalExpr>>> {
    let name = expr_name(node)?;
    let decoder = registry.decoder::<dyn PhysicalExpr, PhysicalExprDecoder>(name)?;
    Some(decoder(node, ctx))
}

/// Every extension expression name registered in `registry`, in arbitrary
/// order.
///
/// Names registered for another kind are not included.
pub fn physical_expr_names(
    registry: &ProtoDecoderRegistry,
) -> impl Iterator<Item = &str> {
    registry.names::<dyn PhysicalExpr>()
}

/// The registry name `node` was written with, if it is an extension node that
/// carries one.
fn expr_name(node: &PhysicalExprNode) -> Option<&str> {
    match node.expr_type.as_ref()? {
        ExprType::Extension(extension) => extension.expr_name.as_deref(),
        _ => None,
    }
}
