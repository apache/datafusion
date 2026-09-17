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

//! Name-keyed decoding for extension [`ExecutionPlan`]s.
//!
//! A built-in plan has a `PhysicalPlanType` variant of its own, so the wire
//! format names it. Extension plans all share `PhysicalExtensionNode`, which
//! carried no discriminator, so the `PhysicalExtensionCodec` *was* the
//! discriminator — and `ComposedPhysicalExtensionCodec` resolves by
//! registration order, which two independent crates cannot agree on.
//!
//! An extension plan implements [`ExtensionPlanFromProto`] instead, and
//! [`register_execution_plan`] puts its decoder in the
//! [`ProtoDecoderRegistry`] the decoding session carries. Lookup is by name,
//! so registration order does not matter. Built-ins cannot be registered;
//! anything unnamed or unregistered still takes the codec chain, unchanged.
//!
//! This is only the `ExecutionPlan` face of the registry: the store is shared
//! with every other extension kind, keyed by the trait as well as the name.
//! `datafusion-examples/examples/proto/extension_plan_registry.rs` is a
//! worked example.

use std::sync::Arc;

use datafusion_common::Result;
use datafusion_proto_models::ProtoDecoderRegistry;
use datafusion_proto_models::protobuf::PhysicalPlanNode;
use datafusion_proto_models::protobuf::physical_plan_node::PhysicalPlanType;

use crate::ExecutionPlan;
use crate::proto::ExecutionPlanDecodeCtx;

/// The wire name of an extension [`ExecutionPlan`], and the constructor that
/// rebuilds it.
///
/// Only extension plans implement this, so a built-in cannot be registered by
/// mistake. One impl block carries both halves, and
/// [`ExecutionPlan::try_to_proto`] writes the matching node:
///
/// ```ignore
/// impl ExecutionPlan for MyExec {
///     fn try_to_proto(
///         &self,
///         ctx: &ExecutionPlanEncodeCtx<'_>,
///     ) -> Result<Option<PhysicalPlanNode>> {
///         // Stamps `Self::NAME`, so the encoded name and the registry key
///         // cannot drift apart.
///         Ok(Some(ctx.extension_node::<Self>(self.payload()?, self.children())?))
///     }
/// }
///
/// impl ExtensionPlanFromProto for MyExec {
///     const NAME: &'static str = "my-crate.MyExec";
///
///     fn try_from_proto(
///         node: &PhysicalPlanNode,
///         ctx: &ExecutionPlanDecodeCtx<'_>,
///     ) -> Result<Arc<dyn ExecutionPlan>> {
///         let extension = expect_plan_variant!(node, PhysicalPlanType::Extension, "Extension");
///         let children = ctx.decode_children(&extension.inputs)?;
///         my_plan_from_bytes(&extension.node, children)
///     }
/// }
/// ```
pub trait ExtensionPlanFromProto: ExecutionPlan + Sized {
    /// The name this plan type is written and registered under.
    ///
    /// Namespace it with the owning crate (`"my-crate.MyExec"`) so that a
    /// collision between two independent crates surfaces as a registration
    /// error rather than as a wrong decode. Never write it by hand on the
    /// wire: [`ExecutionPlanEncodeCtx::extension_node`] stamps it for you.
    ///
    /// [`ExecutionPlanEncodeCtx::extension_node`]: crate::proto::ExecutionPlanEncodeCtx::extension_node
    const NAME: &'static str;

    /// Rebuild the plan from the `PhysicalPlanNode` its
    /// [`ExecutionPlan::try_to_proto`] wrote.
    ///
    /// Match the `Extension` variant (`expect_plan_variant!`), read the
    /// payload from `node`, and decode `inputs` with
    /// [`ExecutionPlanDecodeCtx::decode_children`]. `ctx` also carries the
    /// decoding session, so a plan that rebuilds session state at decode time
    /// can do so.
    fn try_from_proto(
        node: &PhysicalPlanNode,
        ctx: &ExecutionPlanDecodeCtx<'_>,
    ) -> Result<Arc<dyn ExecutionPlan>>;
}

/// How this facade stores a decoder in the shared registry: a function pointer
/// to the monomorphized [`ExtensionPlanFromProto::try_from_proto`].
///
/// Deliberately private, and the same type on both the
/// [`register_execution_plan`] and the [`decode_execution_plan`] side.
/// [`ExtensionPlanFromProto`] is the public contract and
/// [`decode_execution_plan`] is the public way to invoke one, so this can
/// become something else — a `dyn` decoder object, to admit stateful or
/// closure decoders, which is what an FFI decoder needs — without a breaking
/// change.
type ExecutionPlanDecoder =
    fn(&PhysicalPlanNode, &ExecutionPlanDecodeCtx<'_>) -> Result<Arc<dyn ExecutionPlan>>;

/// Register `T` in `registry` under its [`ExtensionPlanFromProto::NAME`].
///
/// Registering the same type twice is a no-op; a *different* type under a
/// taken name is an error, so collisions surface here rather than as a wrong
/// decode. The name is scoped to `dyn ExecutionPlan`, so an expression or a
/// data source may use the same name in the same registry.
///
/// The application that owns the session builds the registry and attaches it
/// once with `SessionConfig::with_extension`; a library exposes a function
/// that fills a registry it is handed. See [`ProtoDecoderRegistry`].
///
/// A built-in plan is not an extension and cannot be registered:
///
/// ```compile_fail
/// use datafusion_physical_plan::filter::FilterExec;
/// use datafusion_physical_plan::proto::{ProtoDecoderRegistry, register_execution_plan};
///
/// let mut registry = ProtoDecoderRegistry::new();
/// register_execution_plan::<FilterExec>(&mut registry).unwrap();
/// ```
///
/// The same imports without that call compile, so the failure above is the
/// missing bound and not a bad path:
///
/// ```
/// use datafusion_physical_plan::filter::FilterExec;
/// #[allow(unused_imports)]
/// use datafusion_physical_plan::proto::{ProtoDecoderRegistry, register_execution_plan};
///
/// let mut registry = ProtoDecoderRegistry::new();
/// let _ = (FilterExec::try_from_proto, &mut registry);
/// ```
pub fn register_execution_plan<T: ExtensionPlanFromProto>(
    registry: &mut ProtoDecoderRegistry,
) -> Result<()> {
    registry.register_decoder::<dyn ExecutionPlan, T, ExecutionPlanDecoder>(
        T::NAME,
        T::try_from_proto,
    )
}

/// Decode `node` with the extension plan decoder registered under the name
/// `node` carries.
///
/// The name is read from the node's `Extension` variant rather than passed in,
/// so a caller cannot pair a node with a name it does not carry.
///
/// `None` means "this node names no extension plan decoder of ours": it is not
/// an extension node, it carries no name, or no registered name matches. The
/// caller then falls back to the `PhysicalExtensionCodec` chain.
/// `Some(Err(..))` means the decoder that *does* own the name failed, which is
/// fatal: falling back there would let another codec decode the payload
/// wrongly, the very thing the name exists to prevent.
pub fn decode_execution_plan(
    registry: &ProtoDecoderRegistry,
    node: &PhysicalPlanNode,
    ctx: &ExecutionPlanDecodeCtx<'_>,
) -> Option<Result<Arc<dyn ExecutionPlan>>> {
    let name = plan_name(node)?;
    let decoder = registry.decoder::<dyn ExecutionPlan, ExecutionPlanDecoder>(name)?;
    Some(decoder(node, ctx))
}

/// Every extension plan name registered in `registry`, in arbitrary order.
///
/// Names registered for another kind are not included.
pub fn execution_plan_names(
    registry: &ProtoDecoderRegistry,
) -> impl Iterator<Item = &str> {
    registry.names::<dyn ExecutionPlan>()
}

/// The registry name `node` was written with, if it is an extension node that
/// carries one.
fn plan_name(node: &PhysicalPlanNode) -> Option<&str> {
    match node.physical_plan_type.as_ref()? {
        PhysicalPlanType::Extension(extension) => extension.plan_name.as_deref(),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto_test_util::registry_test_plan::{
        OtherRegisteredExec, RegisteredExec, RegisteredExecClone, UnnamedExec,
    };

    #[test]
    fn register_is_idempotent_for_the_same_type() -> Result<()> {
        let mut registry = ProtoDecoderRegistry::new();
        register_execution_plan::<RegisteredExec>(&mut registry)?;
        register_execution_plan::<RegisteredExec>(&mut registry)?;

        assert_eq!(execution_plan_names(&registry).count(), 1);
        assert!(execution_plan_names(&registry).any(|name| name == RegisteredExec::NAME));
        Ok(())
    }

    #[test]
    fn register_rejects_a_name_collision_between_types() -> Result<()> {
        let mut registry = ProtoDecoderRegistry::new();
        register_execution_plan::<RegisteredExec>(&mut registry)?;

        // A distinct type declaring the same `NAME`: the collision two
        // independent crates could hit, caught at registration.
        let err = register_execution_plan::<RegisteredExecClone>(&mut registry)
            .expect_err("colliding registration must fail");
        assert!(
            err.to_string().contains(RegisteredExec::NAME)
                && err.to_string().contains("already registered"),
            "unexpected error: {err}"
        );
        Ok(())
    }

    #[test]
    fn register_keeps_distinct_names_apart() -> Result<()> {
        let mut registry = ProtoDecoderRegistry::new();
        register_execution_plan::<RegisteredExec>(&mut registry)?;
        register_execution_plan::<OtherRegisteredExec>(&mut registry)?;

        assert_eq!(execution_plan_names(&registry).count(), 2);
        let mut names: Vec<&str> = execution_plan_names(&registry).collect();
        names.sort_unstable();
        assert_eq!(names, vec![OtherRegisteredExec::NAME, RegisteredExec::NAME]);
        assert!(!execution_plan_names(&registry).any(|name| name == "not.registered"));
        Ok(())
    }

    #[test]
    fn register_rejects_an_empty_plan_name() {
        let mut registry = ProtoDecoderRegistry::new();
        let err = register_execution_plan::<UnnamedExec>(&mut registry)
            .expect_err("an empty NAME must fail");
        assert!(
            err.to_string().contains("empty name"),
            "unexpected error: {err}"
        );
        assert!(execution_plan_names(&registry).next().is_none());
    }

    #[test]
    fn a_name_registered_for_another_kind_is_not_an_execution_plan() -> Result<()> {
        // One session carries one registry, so the `ExecutionPlan` face must
        // not see a name another kind registered, and must not collide with it.
        trait OtherKind {}
        let mut registry = ProtoDecoderRegistry::new();
        registry.register_decoder::<dyn OtherKind, u8, u32>(RegisteredExec::NAME, 7)?;

        assert!(execution_plan_names(&registry).next().is_none());
        register_execution_plan::<RegisteredExec>(&mut registry)?;
        assert_eq!(
            execution_plan_names(&registry).collect::<Vec<_>>(),
            vec![RegisteredExec::NAME]
        );
        Ok(())
    }
}
