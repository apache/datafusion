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

//! Name-keyed decode registry for extension [`ExecutionPlan`]s.
//!
//! Built-in plans are dispatched by their `PhysicalPlanType` oneof variant, so
//! the wire format names them. Extension plans all share the single
//! `PhysicalExtensionNode` variant, which historically carried no
//! discriminator: the `PhysicalExtensionCodec` *was* the discriminator, and
//! `ComposedPhysicalExtensionCodec` had to try each registered codec in
//! sequence and read a decode error as "not mine".
//!
//! This module supplies the missing discriminator. An extension plan declares a
//! globally unique name via [`ExtensionExecutionPlan::PLAN_NAME`], stamps it on
//! the wire with
//! [`ExecutionPlanEncodeCtx::encode_extension`](super::ExecutionPlanEncodeCtx::encode_extension),
//! and registers its decoder on the session with
//! [`ExecutionPlanRegistryExt::register_execution_plan`]. Decoding then selects
//! the decoder by name instead of by trial and error.
//!
//! The registry is *session scoped*, matching the `FunctionRegistry`
//! precedent: it lives in the [`SessionConfig`] extension map, so it travels
//! with the session into every [`TaskContext`] without any new plumbing, stays
//! testable, and stays multi-tenant safe.
//!
//! Registration is per plan type and entirely opt-in. A `PhysicalExtensionNode`
//! with no name — or with a name no decoder claims — falls back to the existing
//! `PhysicalExtensionCodec` chain, unchanged.
//!
//! [`TaskContext`]: datafusion_execution::TaskContext

use std::any::{TypeId, type_name};
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::sync::Arc;

use datafusion_common::{Result, config_err};
use datafusion_execution::config::SessionConfig;
use datafusion_proto_models::protobuf::PhysicalPlanNode;

use crate::ExecutionPlan;
use crate::proto::ExecutionPlanDecodeCtx;

/// How the registry stores a decoder internally: a function pointer to the
/// monomorphized [`ExtensionExecutionPlan::try_from_proto`].
///
/// Deliberately private. [`ExtensionExecutionPlan`] is the public contract, and
/// [`ExecutionPlanRegistry::decode`] is the public way to invoke one, so the
/// storage can become something else (a `dyn` decoder object, to admit
/// stateful or closure decoders) without a breaking change.
///
/// Note that returning `Arc<dyn ExecutionPlan>` is *not* what keeps
/// `try_from_proto` off `dyn` dispatch — `ExecutionPlan::with_new_children`
/// returns exactly that from an object-safe trait. It is that `try_from_proto`
/// is a constructor: a receiver-less associated function has no `self` to
/// dispatch on, so it cannot be called through a trait object and a fn pointer
/// is what a registry can hold.
type ExecutionPlanDecoder =
    fn(&PhysicalPlanNode, &ExecutionPlanDecodeCtx<'_>) -> Result<Arc<dyn ExecutionPlan>>;

/// An extension [`ExecutionPlan`] that serializes itself, without a
/// `PhysicalExtensionCodec`.
///
/// Implement this alongside
/// [`ExecutionPlan::try_to_proto`], then
/// register the type on the session that will decode it:
///
/// ```
/// # use std::any::Any;
/// # use std::fmt::Formatter;
/// # use std::sync::Arc;
/// # use datafusion_common::Result;
/// # use datafusion_execution::config::SessionConfig;
/// # use datafusion_physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream};
/// # use datafusion_physical_plan::proto::{
/// #     ExecutionPlanDecodeCtx, ExecutionPlanEncodeCtx, ExecutionPlanRegistryExt,
/// #     ExtensionExecutionPlan,
/// # };
/// # use datafusion_proto_models::protobuf::PhysicalPlanNode;
/// # #[derive(Debug)]
/// # struct MyExec { properties: Arc<PlanProperties> }
/// # impl DisplayAs for MyExec {
/// #     fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result { write!(f, "MyExec") }
/// # }
/// # impl ExecutionPlan for MyExec {
/// #     fn name(&self) -> &str { "MyExec" }
/// #     fn properties(&self) -> &Arc<PlanProperties> { &self.properties }
/// #     fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> { vec![] }
/// #     fn apply_expressions(&self, _f: &mut dyn FnMut(&Arc<dyn datafusion_physical_expr::PhysicalExpr>) -> Result<datafusion_common::tree_node::TreeNodeRecursion>) -> Result<datafusion_common::tree_node::TreeNodeRecursion> { Ok(datafusion_common::tree_node::TreeNodeRecursion::Continue) }
/// #     fn with_new_children(self: Arc<Self>, _: Vec<Arc<dyn ExecutionPlan>>) -> Result<Arc<dyn ExecutionPlan>> { Ok(self) }
/// #     fn execute(&self, _: usize, _: Arc<datafusion_execution::TaskContext>) -> Result<SendableRecordBatchStream> { unimplemented!() }
/// #     fn try_to_proto(&self, ctx: &ExecutionPlanEncodeCtx<'_>) -> Result<Option<PhysicalPlanNode>> {
/// #         Ok(Some(ctx.encode_extension::<Self, _>(vec![], self.children())?))
/// #     }
/// # }
/// impl ExtensionExecutionPlan for MyExec {
///     const PLAN_NAME: &'static str = "my-crate.MyExec";
///
///     fn try_from_proto(
///         node: &PhysicalPlanNode,
///         ctx: &ExecutionPlanDecodeCtx<'_>,
///     ) -> Result<Arc<dyn ExecutionPlan>> {
///         let parts = ctx.decode_extension(node, Self::PLAN_NAME)?;
///         // ... rebuild `MyExec` from the payload and children ...
/// #       unimplemented!()
///     }
/// }
///
/// let mut config = SessionConfig::new();
/// config.register_execution_plan::<MyExec>()?;
/// # Ok::<(), datafusion_common::DataFusionError>(())
/// ```
pub trait ExtensionExecutionPlan: ExecutionPlan + Sized {
    /// A globally unique name for this plan type, used as the wire
    /// discriminator.
    ///
    /// Namespace it with the owning crate (`"my-crate.MyExec"`) so that a
    /// collision between two independent crates surfaces as a registration
    /// error rather than as a mis-decode.
    const PLAN_NAME: &'static str;

    /// Reconstruct the plan from the `PhysicalPlanNode` written by
    /// [`ExecutionPlan::try_to_proto`].
    ///
    /// Use
    /// [`ExecutionPlanDecodeCtx::decode_extension`](super::ExecutionPlanDecodeCtx::decode_extension)
    /// to unwrap the payload and decode the children.
    fn try_from_proto(
        node: &PhysicalPlanNode,
        ctx: &ExecutionPlanDecodeCtx<'_>,
    ) -> Result<Arc<dyn ExecutionPlan>>;
}

/// One registered decoder, plus the identity used to make re-registering the
/// same type idempotent while a genuine name collision is an error.
#[derive(Debug, Clone, Copy)]
struct RegisteredPlan {
    decoder: ExecutionPlanDecoder,
    type_id: TypeId,
    type_name: &'static str,
}

/// A name-keyed set of extension [`ExecutionPlan`] decoders.
///
/// Usually not constructed directly: use
/// [`ExecutionPlanRegistryExt::register_execution_plan`] on a [`SessionConfig`],
/// which creates, updates and stores the registry for you.
#[derive(Debug, Clone, Default)]
pub struct ExecutionPlanRegistry {
    decoders: HashMap<String, RegisteredPlan>,
}

impl ExecutionPlanRegistry {
    /// Create an empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Register `T` under its [`ExtensionExecutionPlan::PLAN_NAME`].
    ///
    /// Registering the same type twice is a no-op. Registering a *different*
    /// type under a name already taken is an error, so collisions surface here
    /// rather than as a mis-decode later.
    pub fn register<T: ExtensionExecutionPlan>(&mut self) -> Result<()> {
        self.insert(
            T::PLAN_NAME,
            RegisteredPlan {
                decoder: T::try_from_proto,
                type_id: TypeId::of::<T>(),
                type_name: type_name::<T>(),
            },
        )
    }

    fn insert(&mut self, name: impl Into<String>, plan: RegisteredPlan) -> Result<()> {
        let name = name.into();
        if name.is_empty() {
            return config_err!(
                "Cannot register the extension ExecutionPlan decoder for {} under an empty name",
                plan.type_name
            );
        }
        match self.decoders.entry(name) {
            Entry::Vacant(entry) => {
                entry.insert(plan);
                Ok(())
            }
            // Re-registering the same type is a no-op: sessions are often
            // configured by more than one layer of an application.
            Entry::Occupied(entry) if entry.get().type_id == plan.type_id => Ok(()),
            Entry::Occupied(entry) => config_err!(
                "Extension ExecutionPlan name '{}' is already registered by {}, cannot register {}. \
                 Namespace the name with the owning crate to avoid the collision.",
                entry.key(),
                entry.get().type_name,
                plan.type_name
            ),
        }
    }

    /// Decode `node` with the decoder registered under `name`.
    ///
    /// `None` means "no decoder claims this name" — the caller falls back to
    /// the `PhysicalExtensionCodec` chain. `Some(Err(..))` means the decoder
    /// that *does* own the name failed, which is fatal: falling back there
    /// would let another codec mis-decode the payload, the very thing the name
    /// exists to prevent.
    pub fn decode(
        &self,
        name: &str,
        node: &PhysicalPlanNode,
        ctx: &ExecutionPlanDecodeCtx<'_>,
    ) -> Option<Result<Arc<dyn ExecutionPlan>>> {
        let plan = self.decoders.get(name)?;
        Some((plan.decoder)(node, ctx))
    }

    /// Whether a decoder is registered under `name`.
    pub fn contains(&self, name: &str) -> bool {
        self.decoders.contains_key(name)
    }

    /// Every registered name, in arbitrary order.
    pub fn names(&self) -> impl Iterator<Item = &str> {
        self.decoders.keys().map(String::as_str)
    }

    /// Number of registered decoders.
    pub fn len(&self) -> usize {
        self.decoders.len()
    }

    /// Whether no decoder is registered.
    pub fn is_empty(&self) -> bool {
        self.decoders.is_empty()
    }
}

/// Session-scoped registration of extension [`ExecutionPlan`] decoders.
///
/// Implemented for [`SessionConfig`], which carries the registry into every
/// `TaskContext` derived from the session. Register on the session that will
/// *decode* the plan — for a distributed engine, that means every worker as
/// well as the coordinator.
///
/// ```
/// # use datafusion_execution::config::SessionConfig;
/// # use datafusion_physical_plan::proto::{ExecutionPlanRegistry, ExecutionPlanRegistryExt};
/// let mut config = SessionConfig::new();
/// assert!(config.execution_plan_registry().is_none());
///
/// config.set_execution_plan_registry(ExecutionPlanRegistry::new().into());
/// assert!(config.execution_plan_registry().is_some());
/// ```
pub trait ExecutionPlanRegistryExt {
    /// The registry attached to this session, if any has been set.
    fn execution_plan_registry(&self) -> Option<Arc<ExecutionPlanRegistry>>;

    /// Replace the registry attached to this session.
    fn set_execution_plan_registry(&mut self, registry: Arc<ExecutionPlanRegistry>);

    /// Register `T`'s decoder on this session so that a `PhysicalExtensionNode`
    /// naming it decodes through
    /// [`ExtensionExecutionPlan::try_from_proto`] instead of through a
    /// `PhysicalExtensionCodec`.
    ///
    /// Errors if a different type is already registered under the same name.
    fn register_execution_plan<T: ExtensionExecutionPlan>(&mut self) -> Result<()>;

    /// Builder form of [`Self::register_execution_plan`].
    fn with_execution_plan<T: ExtensionExecutionPlan>(self) -> Result<Self>
    where
        Self: Sized;
}

impl ExecutionPlanRegistryExt for SessionConfig {
    fn execution_plan_registry(&self) -> Option<Arc<ExecutionPlanRegistry>> {
        self.get_extension::<ExecutionPlanRegistry>()
    }

    fn set_execution_plan_registry(&mut self, registry: Arc<ExecutionPlanRegistry>) {
        self.set_extension(registry);
    }

    fn register_execution_plan<T: ExtensionExecutionPlan>(&mut self) -> Result<()> {
        // `SessionConfig` extensions are immutable `Arc`s, so update a clone.
        // Registration happens at session setup, not on the hot path.
        let mut registry = self
            .execution_plan_registry()
            .map(|registry| (*registry).clone())
            .unwrap_or_default();
        registry.register::<T>()?;
        self.set_execution_plan_registry(Arc::new(registry));
        Ok(())
    }

    fn with_execution_plan<T: ExtensionExecutionPlan>(mut self) -> Result<Self> {
        self.register_execution_plan::<T>()?;
        Ok(self)
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
        let mut registry = ExecutionPlanRegistry::new();
        registry.register::<RegisteredExec>()?;
        registry.register::<RegisteredExec>()?;

        assert_eq!(registry.len(), 1);
        assert!(registry.contains(RegisteredExec::PLAN_NAME));
        Ok(())
    }

    #[test]
    fn register_rejects_a_name_collision_between_types() -> Result<()> {
        let mut registry = ExecutionPlanRegistry::new();
        registry.register::<RegisteredExec>()?;

        // A distinct type declaring the same `PLAN_NAME`: the collision two
        // independent crates could hit, caught at registration.
        let err = registry
            .register::<RegisteredExecClone>()
            .expect_err("colliding registration must fail");
        assert!(
            err.to_string().contains(RegisteredExec::PLAN_NAME)
                && err.to_string().contains("already registered"),
            "unexpected error: {err}"
        );
        Ok(())
    }

    #[test]
    fn register_keeps_distinct_names_apart() -> Result<()> {
        let mut registry = ExecutionPlanRegistry::new();
        registry.register::<RegisteredExec>()?;
        registry.register::<OtherRegisteredExec>()?;

        assert_eq!(registry.len(), 2);
        let mut names: Vec<&str> = registry.names().collect();
        names.sort_unstable();
        assert_eq!(
            names,
            vec![OtherRegisteredExec::PLAN_NAME, RegisteredExec::PLAN_NAME]
        );
        assert!(!registry.contains("not.registered"));
        Ok(())
    }

    #[test]
    fn register_rejects_an_empty_plan_name() {
        let mut registry = ExecutionPlanRegistry::new();
        let err = registry
            .register::<UnnamedExec>()
            .expect_err("an empty PLAN_NAME must fail");
        assert!(
            err.to_string().contains("empty name"),
            "unexpected error: {err}"
        );
        assert!(registry.is_empty());
    }

    #[test]
    fn session_config_accumulates_registrations() -> Result<()> {
        let mut config = SessionConfig::new();
        assert!(config.execution_plan_registry().is_none());

        config.register_execution_plan::<RegisteredExec>()?;
        config.register_execution_plan::<OtherRegisteredExec>()?;

        let registry = config
            .execution_plan_registry()
            .expect("registry must be attached to the session");
        assert_eq!(registry.len(), 2);
        assert!(registry.contains(RegisteredExec::PLAN_NAME));
        assert!(registry.contains(OtherRegisteredExec::PLAN_NAME));
        Ok(())
    }

    #[test]
    fn session_config_registration_survives_a_clone() -> Result<()> {
        let config = SessionConfig::new().with_execution_plan::<RegisteredExec>()?;
        let cloned = config.clone();

        assert!(
            cloned
                .execution_plan_registry()
                .expect("registry must survive a clone")
                .contains(RegisteredExec::PLAN_NAME)
        );
        Ok(())
    }

    #[test]
    fn session_config_surfaces_collisions() -> Result<()> {
        let mut config = SessionConfig::new();
        config.register_execution_plan::<RegisteredExec>()?;

        let err = config
            .register_execution_plan::<RegisteredExecClone>()
            .expect_err("colliding registration must fail");
        assert!(
            err.to_string().contains("already registered"),
            "unexpected error: {err}"
        );

        // The failed registration left the existing entry untouched.
        let registry = config.execution_plan_registry().expect("registry");
        assert_eq!(registry.len(), 1);
        Ok(())
    }
}
