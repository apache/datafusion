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
//! discriminator: the `PhysicalExtensionCodec` *was* the discriminator.
//! `ComposedPhysicalExtensionCodec` copes by writing the *position* of the
//! codec that encoded a payload into the bytes, so the decoding side must
//! register the same codecs in the same order, and a plan two codecs both
//! claim resolves to whichever was registered first.
//!
//! This module supplies the missing discriminator. [`ExecutionPlanFromProto`]
//! is the decode contract every self-serializing plan implements — built-ins
//! included, dispatched by their `PhysicalPlanType` variant. An extension plan
//! is dispatched by its [`NAME`](ExecutionPlanFromProto::NAME) instead: its
//! `try_to_proto` stamps the name on the `PhysicalExtensionNode` it writes, and
//! it is registered in an [`ExecutionPlanRegistry`] that the decoding session
//! carries. Decoding then selects the decoder by name, independent of
//! registration order.
//!
//! The registry is *session scoped*, matching the `FunctionRegistry`
//! precedent: it is attached with `SessionConfig::with_extension`, so it
//! travels with the session into every [`TaskContext`] without any new
//! plumbing, stays testable, and stays multi-tenant safe.
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
use datafusion_proto_models::protobuf::PhysicalPlanNode;

use crate::ExecutionPlan;
use crate::proto::{ExecutionPlanDecodeCtx, ExecutionPlanFromProto};

/// How the registry stores a decoder internally: a function pointer to the
/// monomorphized [`ExecutionPlanFromProto::try_from_proto`].
///
/// Deliberately private. [`ExecutionPlanFromProto`] is the public contract, and
/// [`ExecutionPlanRegistry::decode`] is the public way to invoke one, so the
/// storage can become something else (a `dyn` decoder object, to admit
/// stateful or closure decoders — an FFI decoder carries a vtable and private
/// data, which a bare `fn` never can) without a breaking change.
type ExecutionPlanDecoder =
    fn(&PhysicalPlanNode, &ExecutionPlanDecodeCtx<'_>) -> Result<Arc<dyn ExecutionPlan>>;

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
/// Build one, register every extension plan the session must decode, and
/// attach it with `SessionConfig::with_extension`. A session carries at most
/// one registry: attaching another replaces it, so compose everything into
/// one registry first. Register on the session that will *decode* the plan —
/// for a distributed engine, that means every worker as well as the
/// coordinator.
#[derive(Debug, Clone, Default)]
pub struct ExecutionPlanRegistry {
    decoders: HashMap<String, RegisteredPlan>,
}

impl ExecutionPlanRegistry {
    /// Create an empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Register `T` under its [`ExecutionPlanFromProto::NAME`].
    ///
    /// Registering the same type twice is a no-op. Registering a *different*
    /// type under a name already taken is an error, so collisions surface here
    /// rather than as a wrong decode later.
    pub fn register<T: ExecutionPlanFromProto>(&mut self) -> Result<()> {
        self.insert(
            T::NAME,
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
    /// would let another codec decode the payload wrongly, the very thing the name
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

    /// Every registered name, in arbitrary order.
    pub fn names(&self) -> impl Iterator<Item = &str> {
        self.decoders.keys().map(String::as_str)
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

        assert_eq!(registry.names().count(), 1);
        assert!(registry.names().any(|name| name == RegisteredExec::NAME));
        Ok(())
    }

    #[test]
    fn register_rejects_a_name_collision_between_types() -> Result<()> {
        let mut registry = ExecutionPlanRegistry::new();
        registry.register::<RegisteredExec>()?;

        // A distinct type declaring the same `NAME`: the collision two
        // independent crates could hit, caught at registration.
        let err = registry
            .register::<RegisteredExecClone>()
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
        let mut registry = ExecutionPlanRegistry::new();
        registry.register::<RegisteredExec>()?;
        registry.register::<OtherRegisteredExec>()?;

        assert_eq!(registry.names().count(), 2);
        let mut names: Vec<&str> = registry.names().collect();
        names.sort_unstable();
        assert_eq!(names, vec![OtherRegisteredExec::NAME, RegisteredExec::NAME]);
        assert!(!registry.names().any(|name| name == "not.registered"));
        Ok(())
    }

    #[test]
    fn register_rejects_an_empty_plan_name() {
        let mut registry = ExecutionPlanRegistry::new();
        let err = registry
            .register::<UnnamedExec>()
            .expect_err("an empty NAME must fail");
        assert!(
            err.to_string().contains("empty name"),
            "unexpected error: {err}"
        );
        assert!(registry.names().next().is_none());
    }
}
