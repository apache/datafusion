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

//! A name-keyed store of decoders for extension types.
//!
//! DataFusion serializes several kinds of polymorphic value: `ExecutionPlan`,
//! `PhysicalExpr`, and in time the `DataSource`, `DataSink` and
//! `LazyBatchGenerator` families. A built-in gets its own wire variant, so the
//! wire format names it. An extension shares one catch-all variant with every
//! other extension of its kind, so the payload has to carry a name and the
//! decoding session has to map that name back to a decoder.
//!
//! [`ProtoDecoderRegistry`] is that map, once, for every kind. The key is a
//! pair: the trait the decoder produces, and the name on the wire. Keying on
//! the trait as well as the name means one session carries one registry rather
//! than one per kind, and two kinds may use the same name without a collision.
//!
//! The registry never names a decode context, or any trait it dispatches to.
//! It stores each decoder type-erased and hands it back on a downcast, which is
//! what lets it sit in this crate, below every crate that owns one of those
//! traits. Each kind supplies a small typed facade next to its own trait —
//! `datafusion-physical-plan` for `ExecutionPlan`, and so on — so that a caller
//! never writes a `TypeId` or a downcast by hand.

use std::any::{Any, TypeId, type_name};
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::fmt;
use std::sync::Arc;

use datafusion_common::{Result, config_err};

/// One registered decoder, plus the identity that makes re-registering the
/// same type a no-op while a genuine name collision is an error.
#[derive(Clone)]
struct Registered {
    decoder: Arc<dyn Any + Send + Sync>,
    type_id: TypeId,
    type_name: &'static str,
}

/// A store of extension decoders, keyed by the trait they produce and the name
/// the encoder wrote on the wire.
///
/// Build one, let every library that ships extension types register into it,
/// and attach it to the decoding session with `SessionConfig::with_extension`.
/// A session carries at most one, and attaching another replaces it, so the
/// application that owns the session is the one that builds it. A library
/// exposes a function that fills a registry it is handed:
///
/// ```rust,ignore
/// // in each library
/// pub fn register(registry: &mut ProtoDecoderRegistry) -> Result<()> {
///     register_execution_plan::<MyExec>(registry)?;
///     register_physical_expr::<MyExpr>(registry)
/// }
///
/// // in the application that owns the session
/// let mut registry = ProtoDecoderRegistry::new();
/// lib_one::register(&mut registry)?;
/// lib_two::register(&mut registry)?;
/// let config = SessionConfig::new().with_extension(Arc::new(registry));
/// ```
///
/// The libraries need to know nothing about each other, and the order they are
/// called in does not matter: lookup is by name, and a real collision is an
/// error from [`register_decoder`](Self::register_decoder).
///
/// Callers normally reach this type through a kind's typed facade rather than
/// through the methods here.
#[derive(Clone, Default)]
pub struct ProtoDecoderRegistry {
    decoders: HashMap<(TypeId, String), Registered>,
}

impl ProtoDecoderRegistry {
    /// Create an empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Register `decoder` under `name`, for extensions dispatched as `Kind`.
    ///
    /// `Kind` is the trait object the decoder produces, such as
    /// `dyn ExecutionPlan`. `T` is the concrete type being registered; it is
    /// used only for identity, so that registering the same type twice is a
    /// no-op while a *different* type under a taken name is an error. `D` is
    /// the decoder itself, whose type the matching
    /// [`decoder`](Self::decoder) call must name exactly.
    pub fn register_decoder<Kind, T, D>(&mut self, name: &str, decoder: D) -> Result<()>
    where
        Kind: ?Sized + 'static,
        T: ?Sized + 'static,
        D: Any + Send + Sync,
    {
        let registered = Registered {
            decoder: Arc::new(decoder),
            type_id: TypeId::of::<T>(),
            type_name: type_name::<T>(),
        };
        if name.is_empty() {
            return config_err!(
                "Cannot register the extension {} decoder for {} under an empty name",
                kind_label::<Kind>(),
                registered.type_name
            );
        }
        match self
            .decoders
            .entry((TypeId::of::<Kind>(), name.to_string()))
        {
            Entry::Vacant(entry) => {
                entry.insert(registered);
                Ok(())
            }
            // Re-registering the same type is a no-op: sessions are often
            // configured by more than one layer of an application.
            Entry::Occupied(entry) if entry.get().type_id == registered.type_id => Ok(()),
            Entry::Occupied(entry) => config_err!(
                "Extension {} name '{}' is already registered by {}, cannot register {}. \
                 Namespace the name with the owning crate to avoid the collision.",
                kind_label::<Kind>(),
                entry.key().1,
                entry.get().type_name,
                registered.type_name
            ),
        }
    }

    /// The decoder registered under `name` for extensions dispatched as `Kind`.
    ///
    /// `D` must be the same type the matching
    /// [`register_decoder`](Self::register_decoder) call supplied. A different
    /// `D` reads as "not registered" rather than as an error, because the two
    /// calls are always made by the same facade.
    pub fn decoder<Kind, D>(&self, name: &str) -> Option<&D>
    where
        Kind: ?Sized + 'static,
        D: Any + Send + Sync,
    {
        let registered = self
            .decoders
            .get(&(TypeId::of::<Kind>(), name.to_string()))?;
        registered.decoder.downcast_ref::<D>()
    }

    /// Every name registered for `Kind`, in arbitrary order.
    pub fn names<Kind: ?Sized + 'static>(&self) -> impl Iterator<Item = &str> {
        let kind = TypeId::of::<Kind>();
        self.decoders
            .keys()
            .filter(move |(registered_kind, _)| *registered_kind == kind)
            .map(|(_, name)| name.as_str())
    }
}

impl fmt::Debug for ProtoDecoderRegistry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // The decoders are opaque; the registered types are the useful part.
        let mut entries: Vec<&str> =
            self.decoders.values().map(|r| r.type_name).collect();
        entries.sort_unstable();
        f.debug_struct("ProtoDecoderRegistry")
            .field("registered", &entries)
            .finish()
    }
}

/// The last path segment of a trait object's type name, for error messages:
/// `dyn datafusion_physical_plan::execution_plan::ExecutionPlan` reads as
/// `ExecutionPlan`.
fn kind_label<Kind: ?Sized + 'static>() -> &'static str {
    let name = type_name::<Kind>();
    name.rsplit("::").next().unwrap_or(name)
}

#[cfg(test)]
mod tests {
    use super::*;

    trait Plan {}
    trait Expr {}

    struct OnePlan;
    struct OtherPlan;

    type PlanDecoder = fn(u8) -> u8;
    type ExprDecoder = fn(u8) -> u16;

    fn plan_decoder(v: u8) -> u8 {
        v
    }
    fn other_plan_decoder(v: u8) -> u8 {
        v + 1
    }
    fn expr_decoder(v: u8) -> u16 {
        u16::from(v) + 1000
    }

    #[test]
    fn register_and_look_up_a_decoder() -> Result<()> {
        let mut registry = ProtoDecoderRegistry::new();
        registry.register_decoder::<dyn Plan, OnePlan, PlanDecoder>(
            "a-crate.OnePlan",
            plan_decoder,
        )?;

        let decoder = registry
            .decoder::<dyn Plan, PlanDecoder>("a-crate.OnePlan")
            .expect("the decoder must be registered");
        assert_eq!(decoder(7), 7);
        assert!(
            registry
                .decoder::<dyn Plan, PlanDecoder>("a-crate.Missing")
                .is_none()
        );
        Ok(())
    }

    #[test]
    fn the_same_name_under_two_kinds_does_not_collide() -> Result<()> {
        // The point of keying on the trait as well as the name: one session
        // carries one registry, and two kinds never see each other's names.
        let mut registry = ProtoDecoderRegistry::new();
        registry
            .register_decoder::<dyn Plan, OnePlan, PlanDecoder>("shared", plan_decoder)?;
        registry.register_decoder::<dyn Expr, OtherPlan, ExprDecoder>(
            "shared",
            expr_decoder,
        )?;

        assert_eq!(
            registry.decoder::<dyn Plan, PlanDecoder>("shared").unwrap()(1),
            1
        );
        assert_eq!(
            registry.decoder::<dyn Expr, ExprDecoder>("shared").unwrap()(1),
            1001
        );
        assert_eq!(
            registry.names::<dyn Plan>().collect::<Vec<_>>(),
            vec!["shared"]
        );
        assert_eq!(
            registry.names::<dyn Expr>().collect::<Vec<_>>(),
            vec!["shared"]
        );
        Ok(())
    }

    #[test]
    fn register_is_idempotent_for_the_same_type() -> Result<()> {
        let mut registry = ProtoDecoderRegistry::new();
        registry
            .register_decoder::<dyn Plan, OnePlan, PlanDecoder>("one", plan_decoder)?;
        registry
            .register_decoder::<dyn Plan, OnePlan, PlanDecoder>("one", plan_decoder)?;

        assert_eq!(registry.names::<dyn Plan>().count(), 1);
        Ok(())
    }

    #[test]
    fn register_rejects_a_name_collision_between_types() -> Result<()> {
        let mut registry = ProtoDecoderRegistry::new();
        registry
            .register_decoder::<dyn Plan, OnePlan, PlanDecoder>("one", plan_decoder)?;

        let err = registry
            .register_decoder::<dyn Plan, OtherPlan, PlanDecoder>(
                "one",
                other_plan_decoder,
            )
            .expect_err("a colliding registration must fail");
        let err = err.to_string();
        assert!(
            err.contains("already registered"),
            "unexpected error: {err}"
        );
        assert!(err.contains("Plan"), "the kind must be named: {err}");
        assert!(
            err.contains("OnePlan") && err.contains("OtherPlan"),
            "{err}"
        );
        Ok(())
    }

    #[test]
    fn register_rejects_an_empty_name() {
        let mut registry = ProtoDecoderRegistry::new();
        let err = registry
            .register_decoder::<dyn Plan, OnePlan, PlanDecoder>("", plan_decoder)
            .expect_err("an empty name must fail");
        assert!(
            err.to_string().contains("empty name"),
            "unexpected error: {err}"
        );
        assert_eq!(registry.names::<dyn Plan>().count(), 0);
    }

    #[test]
    fn a_decoder_of_another_type_reads_as_absent() -> Result<()> {
        let mut registry = ProtoDecoderRegistry::new();
        registry
            .register_decoder::<dyn Plan, OnePlan, PlanDecoder>("one", plan_decoder)?;

        // A facade always pairs the same `D` with the same `Kind`; a mismatch
        // means the caller asked the wrong facade.
        assert!(registry.decoder::<dyn Plan, ExprDecoder>("one").is_none());
        Ok(())
    }
}
