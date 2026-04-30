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

//! Format-agnostic serialization for [`PhysicalExpr`].
//!
//! This module provides the building blocks that let any [`serde`]-compatible
//! format (JSON for debugging, protobuf via a serde adapter, etc.) serialize
//! an `Arc<dyn PhysicalExpr>` without being coupled to a specific wire format.
//!
//! # Encoding
//!
//! Each expression is encoded as a `{tag, data}` envelope. The `tag` is the
//! string returned from [`PhysicalExpr::serde_tag`] and is used by the
//! deserialization side to dispatch back to a concrete type. The `data` is
//! the body produced by [`PhysicalExpr::erased_serialize`].
//!
//! `Arc<dyn PhysicalExpr>` is serializable too: serde's `rc` feature provides
//! a blanket `Serialize` impl for `Arc<T>` whenever `T: Serialize`.
//!
//! # Opt-in
//!
//! Both `serde_tag` and `erased_serialize` have default implementations on the
//! `PhysicalExpr` trait. Expressions that don't override them are not
//! serializable — the [`Serialize`] impl below will fail at runtime with a
//! descriptive error.
//!
//! # Deserialization
//!
//! Decoding goes through a [`PhysicalExprRegistry`]: implementers register a
//! constructor for each tag, and the registry's `deserialize_*` methods
//! dispatch on the tag to call the right constructor. Implementers opt in
//! by implementing [`PhysicalExprDeserialize`].
//!
//! Decoding is format-agnostic: the trait method receives a
//! [`DeserializeContext`] holding `&mut dyn erased_serde::Deserializer<'de>`
//! plus the registry. Convenience entry points like
//! [`PhysicalExprRegistry::deserialize_json`] wrap a concrete serde
//! `Deserializer` for callers; other formats can be added by writing a
//! similar wrapper.
//!
//! Wire stability across DataFusion versions is **not** a goal of this layer.
//! Use the proto crate for stable cross-version wire formats.

use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use datafusion_common::{Result, exec_datafusion_err};
use serde::Serialize;
use serde::de::{Deserialize, DeserializeSeed, Error as DeError, MapAccess, Visitor};
use serde::ser::{Error as _, SerializeStruct, Serializer};

use crate::physical_expr::PhysicalExpr;

impl Serialize for dyn PhysicalExpr {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let tag = self.serde_tag();
        if tag.is_empty() {
            return Err(S::Error::custom(format!(
                "PhysicalExpr serialization not implemented for {self}"
            )));
        }

        let body = self.erased_serialize();
        let mut state = serializer.serialize_struct("PhysicalExpr", 2)?;
        state.serialize_field("tag", tag)?;
        // `&dyn erased_serde::Serialize` implements `serde::Serialize` via the
        // `serialize_trait_object!(Serialize)` invocation inside erased_serde.
        state.serialize_field("data", &*body)?;
        state.end()
    }
}

/// Sentinel returned by the default [`PhysicalExpr::erased_serialize`] impl.
///
/// Serializing this value fails with a descriptive error. Used so that the
/// trait method can have a default implementation without needing
/// specialization or a separate "is serializable" branch outside the tag
/// check.
#[doc(hidden)]
pub struct NotSerializable(pub String);

impl Serialize for NotSerializable {
    fn serialize<S: Serializer>(&self, _serializer: S) -> Result<S::Ok, S::Error> {
        Err(S::Error::custom(&self.0))
    }
}

/// Trait implemented by expressions that opt in to deserialization.
///
/// Each implementer pairs a stable string tag (`TAG`) with a constructor
/// (`deserialize`) that rebuilds the expression from a serde
/// [`Deserializer`]. The tag must be globally unique and must agree with
/// what the type returns from [`PhysicalExpr::serde_tag`] — the convention
/// is to use `TAG` for both:
///
/// ```ignore
/// impl PhysicalExpr for MyExpr {
///     fn serde_tag(&self) -> &'static str { Self::TAG }
///     // ...
/// }
/// ```
///
/// # Implementing for leaf expressions (no trait-object children)
///
/// Derive `serde::Deserialize` on the type and call `ctx.deserialize::<Self>()`:
///
/// ```ignore
/// fn deserialize(ctx: &mut DeserializeContext<'_, '_>) -> Result<Self> {
///     ctx.deserialize::<Self>()
/// }
/// ```
///
/// # Implementing for expressions with trait-object children
///
/// `Arc<dyn PhysicalExpr>` is not directly `Deserialize` — child expressions
/// need to recurse through the registry. Use [`DeserializeContext::expr_seed`]
/// inside a hand-written `Visitor`:
///
/// ```ignore
/// struct V<'r> { registry: &'r PhysicalExprRegistry }
/// impl<'de, 'r> Visitor<'de> for V<'r> {
///     type Value = MyExpr;
///     fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<MyExpr, A::Error> {
///         let mut child = None;
///         while let Some(k) = map.next_key::<String>()? {
///             match k.as_str() {
///                 "child" => child = Some(map.next_value_seed(self.registry.expr_seed())?),
///                 _ => { let _: serde::de::IgnoredAny = map.next_value()?; }
///             }
///         }
///         Ok(MyExpr { child: child.ok_or_else(|| Error::missing_field("child"))? })
///     }
/// }
/// ctx.deserializer().deserialize_map(V { registry: ctx.registry() })
/// ```
pub trait PhysicalExprDeserialize: PhysicalExpr + Sized {
    /// Stable identifier for this expression in serialized form.
    const TAG: &'static str;

    /// Rebuild `Self` from the body of an envelope.
    fn deserialize(ctx: &mut DeserializeContext<'_, '_>) -> Result<Self>;
}

/// Context passed to [`PhysicalExprDeserialize::deserialize`].
///
/// Holds the registry (so child expressions can recurse via
/// [`PhysicalExprRegistry::expr_seed`]) and a type-erased deserializer
/// pointing at the expression's data body.
pub struct DeserializeContext<'reg, 'de> {
    registry: &'reg PhysicalExprRegistry,
    de: &'reg mut dyn erased_serde::Deserializer<'de>,
}

impl<'reg, 'de> DeserializeContext<'reg, 'de> {
    /// Returns the registry, primarily to access [`PhysicalExprRegistry::expr_seed`]
    /// when deserializing trait-object children.
    pub fn registry(&self) -> &'reg PhysicalExprRegistry {
        self.registry
    }

    /// Direct access to the erased deserializer. Use this when implementing a
    /// custom `Visitor` for an expression with trait-object children.
    pub fn deserializer(&mut self) -> &mut dyn erased_serde::Deserializer<'de> {
        &mut *self.de
    }

    /// Deserialize the data body as `T`. Convenience for leaf expressions
    /// whose body is `T: serde::Deserialize`.
    pub fn deserialize<T: Deserialize<'de>>(&mut self) -> Result<T> {
        erased_serde::deserialize(&mut *self.de)
            .map_err(|e| exec_datafusion_err!("PhysicalExpr deserialize failed: {e}"))
    }

    /// Deserialize the data body as a struct with a single
    /// `Arc<dyn PhysicalExpr>` field named `field`. Helper for unary wrapper
    /// expressions like `NotExpr`, `NegativeExpr`, `IsNullExpr`, etc.
    pub fn deserialize_unary(
        &mut self,
        field: &'static str,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        struct V<'r> {
            registry: &'r PhysicalExprRegistry,
            field: &'static str,
        }
        impl<'de, 'r> Visitor<'de> for V<'r> {
            type Value = Arc<dyn PhysicalExpr>;
            fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "a unary expression body {{{}}}", self.field)
            }
            fn visit_map<A: MapAccess<'de>>(
                self,
                mut map: A,
            ) -> std::result::Result<Self::Value, A::Error> {
                let mut child: Option<Arc<dyn PhysicalExpr>> = None;
                while let Some(k) = map.next_key::<String>()? {
                    if k == self.field {
                        child = Some(map.next_value_seed(self.registry.expr_seed())?);
                    } else {
                        let _: serde::de::IgnoredAny = map.next_value()?;
                    }
                }
                child.ok_or_else(|| A::Error::missing_field(self.field))
            }
        }
        let registry = self.registry;
        serde::Deserializer::deserialize_map(&mut *self.de, V { registry, field })
            .map_err(|e| exec_datafusion_err!("unary expr deserialize failed: {e}"))
    }
}

/// `DeserializeSeed` that reads a `{tag, data}` envelope and dispatches to the
/// registered constructor for `tag`. Use via [`PhysicalExprRegistry::expr_seed`]
/// inside `next_value_seed` / `next_element_seed` calls.
pub struct ExprSeed<'reg> {
    registry: &'reg PhysicalExprRegistry,
}

impl<'de, 'reg> DeserializeSeed<'de> for ExprSeed<'reg> {
    type Value = Arc<dyn PhysicalExpr>;

    fn deserialize<D: serde::Deserializer<'de>>(
        self,
        deserializer: D,
    ) -> std::result::Result<Self::Value, D::Error> {
        deserializer.deserialize_map(EnvelopeVisitor {
            registry: self.registry,
        })
    }
}

struct EnvelopeVisitor<'reg> {
    registry: &'reg PhysicalExprRegistry,
}

impl<'de, 'reg> Visitor<'de> for EnvelopeVisitor<'reg> {
    type Value = Arc<dyn PhysicalExpr>;

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("a {tag, data} PhysicalExpr envelope")
    }

    fn visit_map<A: MapAccess<'de>>(
        self,
        mut map: A,
    ) -> std::result::Result<Self::Value, A::Error> {
        // Tag must come first — that's how we serialize, and reading it first
        // lets us dispatch the data field to the correct constructor without
        // buffering.
        let tag_key: String = map
            .next_key()?
            .ok_or_else(|| A::Error::missing_field("tag"))?;
        if tag_key != "tag" {
            return Err(A::Error::custom(format!(
                "expected `tag` as first key in PhysicalExpr envelope, got {tag_key:?}"
            )));
        }
        let tag: String = map.next_value()?;

        let constructor =
            *self
                .registry
                .constructors
                .get(tag.as_str())
                .ok_or_else(|| {
                    A::Error::custom(format!(
                        "no PhysicalExpr registered under tag {tag:?}"
                    ))
                })?;

        let data_key: String = map
            .next_key()?
            .ok_or_else(|| A::Error::missing_field("data"))?;
        if data_key != "data" {
            return Err(A::Error::custom(format!(
                "expected `data` after `tag` in PhysicalExpr envelope, got {data_key:?}"
            )));
        }

        map.next_value_seed(DataSeed {
            registry: self.registry,
            constructor,
        })
    }
}

struct DataSeed<'reg> {
    registry: &'reg PhysicalExprRegistry,
    constructor: Constructor,
}

impl<'de, 'reg> DeserializeSeed<'de> for DataSeed<'reg> {
    type Value = Arc<dyn PhysicalExpr>;

    fn deserialize<D: serde::Deserializer<'de>>(
        self,
        deserializer: D,
    ) -> std::result::Result<Self::Value, D::Error> {
        let mut erased = <dyn erased_serde::Deserializer<'de>>::erase(deserializer);
        let mut ctx = DeserializeContext {
            registry: self.registry,
            de: &mut erased,
        };
        (self.constructor)(&mut ctx).map_err(D::Error::custom)
    }
}

type Constructor = for<'reg, 'de> fn(
    &mut DeserializeContext<'reg, 'de>,
) -> Result<Arc<dyn PhysicalExpr>>;

/// Registry mapping serialization tags to constructors.
///
/// Built up with [`PhysicalExprRegistry::empty`] and
/// [`PhysicalExprRegistry::with`] in builder style. The `physical-expr` crate
/// provides a `default_registry()` that returns a registry pre-populated with
/// all of DataFusion's built-in expressions.
pub struct PhysicalExprRegistry {
    constructors: HashMap<&'static str, Constructor>,
}

impl PhysicalExprRegistry {
    /// Returns an empty registry. Use [`Self::with`] to add constructors.
    pub fn empty() -> Self {
        Self {
            constructors: HashMap::new(),
        }
    }

    /// Register the constructor for `T`.
    ///
    /// Panics if `T::TAG` is empty (the "not serializable" sentinel) or if a
    /// constructor was already registered under `T::TAG`. Tag collisions
    /// almost always indicate a programming error — two types claiming the
    /// same identifier — so failing loudly during registry construction is
    /// the safer default. Use [`Self::try_with`] if you need a fallible
    /// version (e.g. when registering plugins discovered at runtime).
    pub fn with<T: PhysicalExprDeserialize + 'static>(self) -> Self {
        match self.try_with::<T>() {
            Ok(reg) => reg,
            Err(e) => panic!("{e}"),
        }
    }

    /// Fallible version of [`Self::with`]. Returns the registry unchanged on
    /// success, or a [`DataFusionError`] on tag collision / empty tag.
    pub fn try_with<T: PhysicalExprDeserialize + 'static>(mut self) -> Result<Self> {
        let tag = T::TAG;
        if tag.is_empty() {
            return Err(exec_datafusion_err!(
                "PhysicalExprDeserialize::TAG must not be empty"
            ));
        }
        if self.constructors.contains_key(tag) {
            return Err(exec_datafusion_err!(
                "PhysicalExprRegistry: duplicate registration for tag {tag:?}"
            ));
        }
        self.constructors.insert(tag, |ctx| {
            T::deserialize(ctx).map(|v| Arc::new(v) as Arc<dyn PhysicalExpr>)
        });
        Ok(self)
    }

    /// Returns true if a constructor is registered for `tag`.
    pub fn contains_tag(&self, tag: &str) -> bool {
        self.constructors.contains_key(tag)
    }

    /// Returns a `DeserializeSeed` that reads a `{tag, data}` envelope and
    /// produces an `Arc<dyn PhysicalExpr>`. Use this inside a custom
    /// `Visitor` to deserialize trait-object children of an expression.
    pub fn expr_seed(&self) -> ExprSeed<'_> {
        ExprSeed { registry: self }
    }

    /// Decode an expression from any serde [`Deserializer`].
    ///
    /// This is the format-agnostic entry point. Convenience methods like
    /// [`Self::deserialize_json`] wrap this for specific formats.
    pub fn deserialize<'de, D: serde::Deserializer<'de>>(
        &self,
        deserializer: D,
    ) -> std::result::Result<Arc<dyn PhysicalExpr>, D::Error> {
        self.expr_seed().deserialize(deserializer)
    }

    /// Decode a JSON-serialized expression. Convenience wrapper around
    /// [`Self::deserialize`] with a `serde_json` deserializer.
    pub fn deserialize_json(&self, s: &str) -> Result<Arc<dyn PhysicalExpr>> {
        let mut de = serde_json::Deserializer::from_str(s);
        let expr = self
            .deserialize(&mut de)
            .map_err(|e| exec_datafusion_err!("PhysicalExpr JSON decode failed: {e}"))?;
        de.end().map_err(|e| {
            exec_datafusion_err!("PhysicalExpr JSON had trailing data: {e}")
        })?;
        Ok(expr)
    }
}

impl Default for PhysicalExprRegistry {
    fn default() -> Self {
        Self::empty()
    }
}
