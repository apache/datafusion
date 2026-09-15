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
//! the contract every self-serializing expression implements, and the
//! session-scoped [`PhysicalExprRegistry`] that routes extension expressions
//! to theirs by name.
//!
//! These live here rather than next to the encode context in
//! `datafusion-physical-expr-common` because a decoder may need the session,
//! and what it needs — [`FunctionRegistry`] and [`ConfigOptions`] — is defined
//! in crates above `physical-expr-common` but below this one. The decode
//! context is generic over its session type for that reason; this module
//! fixes it to [`ExprDecodeSession`], which bundles exactly those two.
//! `datafusion-proto` builds one from the `TaskContext` it decodes under.

use std::any::{TypeId, type_name};
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::sync::Arc;

use datafusion_common::config::ConfigOptions;
use datafusion_common::{Result, config_err};
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
/// extension, so it is dispatched by `NAME` instead: it writes the name
/// in its `try_to_proto` and is registered in a
/// [`PhysicalExprRegistry`] attached to the session that will decode it.
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

/// How the registry stores a decoder internally: a function pointer to the
/// monomorphized [`PhysicalExprFromProto::try_from_proto`].
///
/// Deliberately private. [`PhysicalExprFromProto`] is the public contract,
/// and [`PhysicalExprRegistry::decode`] is the public way to invoke one, so
/// the storage can become something else (a `dyn` decoder object, to admit
/// stateful or closure decoders — an FFI decoder carries a vtable and
/// private data, which a bare `fn` never can) without a breaking change.
type PhysicalExprDecoder = fn(
    &PhysicalExprNode,
    &PhysicalExprDecodeCtx<'_, ExprDecodeSession<'_>>,
) -> Result<Arc<dyn PhysicalExpr>>;

/// One registered decoder, plus the identity used to make re-registering
/// the same type idempotent while a genuine name collision is an error.
#[derive(Debug, Clone, Copy)]
struct RegisteredExpr {
    decoder: PhysicalExprDecoder,
    type_id: TypeId,
    type_name: &'static str,
}

/// A name-keyed set of extension [`PhysicalExpr`] decoders.
///
/// Build one, register every extension expression the session must
/// decode, and attach it with `SessionConfig::with_extension`. A session
/// carries at most one registry: attaching another replaces it, so compose
/// everything into one registry first.
///
/// ```ignore
/// let mut registry = PhysicalExprRegistry::new();
/// registry.register::<MyExpr>()?;
/// let config = SessionConfig::new().with_extension(Arc::new(registry));
/// ```
///
/// Lookup is by name, so resolution does not depend on registration order
/// and a collision between two crates surfaces as an error at registration
/// rather than as a silent wrong decode.
///
/// A name absent from the registry falls back to
/// `PhysicalExtensionCodec::try_decode_expr`. For nodes written before this
/// mechanism existed that fallback is exactly the old behavior. For an
/// expression whose `try_to_proto` writes its own payload it is not: the
/// payload on the wire is then the expression's own message, and handing it to a codec
/// that still recognizes the expression can decode it as the old message
/// rather than failing. Register migrated expressions everywhere their
/// plans are read.
///
#[derive(Debug, Clone, Default)]
pub struct PhysicalExprRegistry {
    decoders: HashMap<String, RegisteredExpr>,
}

impl PhysicalExprRegistry {
    /// Create an empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Register `T` under its [`PhysicalExprFromProto::NAME`].
    ///
    /// Registering the same type twice is a no-op. Registering a
    /// *different* type under a name already taken is an error, so
    /// collisions surface here rather than as a wrong decode later.
    pub fn register<T: PhysicalExprFromProto>(&mut self) -> Result<()> {
        let registered = RegisteredExpr {
            decoder: T::try_from_proto,
            type_id: TypeId::of::<T>(),
            type_name: type_name::<T>(),
        };
        if T::NAME.is_empty() {
            return config_err!(
                "Cannot register the extension PhysicalExpr decoder for {} under an empty name",
                registered.type_name
            );
        }
        match self.decoders.entry(T::NAME.to_string()) {
            Entry::Vacant(entry) => {
                entry.insert(registered);
                Ok(())
            }
            // Re-registering the same type is a no-op: sessions are often
            // configured by more than one layer of an application.
            Entry::Occupied(entry) if entry.get().type_id == registered.type_id => Ok(()),
            Entry::Occupied(entry) => config_err!(
                "Extension PhysicalExpr name '{}' is already registered by {}, cannot register {}. \
                 Namespace the name with the owning crate to avoid the collision.",
                entry.key(),
                entry.get().type_name,
                registered.type_name
            ),
        }
    }

    /// Decode `node` with the decoder registered under `name`.
    ///
    /// `None` means "no decoder claims this name" — the caller falls back
    /// to the `PhysicalExtensionCodec` chain. `Some(Err(..))` means the
    /// decoder that *does* own the name failed, which is fatal: falling
    /// back there would let a codec decode the payload wrongly, the very thing
    /// the name exists to prevent.
    pub fn decode(
        &self,
        name: &str,
        node: &PhysicalExprNode,
        ctx: &PhysicalExprDecodeCtx<'_, ExprDecodeSession<'_>>,
    ) -> Option<Result<Arc<dyn PhysicalExpr>>> {
        let registered = self.decoders.get(name)?;
        Some((registered.decoder)(node, ctx))
    }

    /// Every registered name, in arbitrary order.
    pub fn names(&self) -> impl Iterator<Item = &str> {
        self.decoders.keys().map(String::as_str)
    }
}
