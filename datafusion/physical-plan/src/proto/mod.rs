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

//! Serialization hooks for [`ExecutionPlan`], mirroring the
//! `try_to_proto`/`try_from_proto` pattern used for `PhysicalExpr`.
//!
//! # Why the indirection
//!
//! An `ExecutionPlan` must be able to (de)serialize its child plans and its
//! child physical expressions recursively. The concrete recursion lives in
//! `datafusion-proto` (it owns the extension codec, the session context and the
//! central converter), but `datafusion-proto` sits *above* `datafusion-physical-plan`
//! in the crate graph. To let a plan drive that recursion without a dependency
//! cycle, this module defines:
//!
//! * [`ExecutionPlanEncodeCtx`] / [`ExecutionPlanDecodeCtx`] — the stable,
//!   concrete context types a plan author interacts with. New capabilities can
//!   be added here without changing every plan's hook signature.
//! * [`ExecutionPlanEncode`] / [`ExecutionPlanDecode`] — internal dispatch
//!   traits, *defined* here but *implemented* in `datafusion-proto`, that the
//!   context types delegate to. This is the dependency inversion that keeps the
//!   proto types flowing in one direction only. They are `#[doc(hidden)]`: not
//!   public API, `pub` only because their implementors live in another crate.
//!
//! `datafusion-physical-plan` depends on the pure prost types in
//! `datafusion-proto-models` (feature `proto`), never on `datafusion-proto`.
//!
//! # Function-carrying plans
//!
//! Plans that reference UD(A/W)Fs (`AggregateExec`, the window execs, …) also
//! ride the hook: the context exposes typed, *bytes-only* function serde —
//! [`encode_udaf`](ExecutionPlanEncodeCtx::encode_udaf) /
//! [`decode_udaf`](ExecutionPlanDecodeCtx::decode_udaf) and the udf/udwf
//! siblings. These take/return `datafusion-expr` types plus `Vec<u8>` and never
//! name a proto type, so the `PhysicalExtensionCodec` (which only
//! `datafusion-proto` can name) stays fully encapsulated behind the adapter that
//! backs these traits. The lookup-order policy (payload → codec; else registry →
//! codec fallback) lives once, in that adapter, rather than in every plan.
//!
//! This is possible because `datafusion-physical-plan` sits *above*
//! `datafusion-expr` in the crate graph; the expression-side ctx (in
//! `physical-expr-common`, *below* `datafusion-expr`) cannot do this, which is
//! why `ScalarFunctionExpr` remains special-cased there.
//!
//! # Extension plans
//!
//! Third-party plans have no `PhysicalPlanType` variant of their own; they all
//! share `PhysicalExtensionNode`, whose payload is opaque bytes. Such a plan
//! rides the same hooks by pairing
//! [`ExecutionPlanEncodeCtx::encode_extension`] with
//! [`ExecutionPlanDecodeCtx::decode_extension`], and declaring a name through
//! [`ExtensionExecutionPlan`] so the decoding session can select its decoder by
//! name rather than by trying every registered `PhysicalExtensionCodec` in
//! turn. See [`registry`].
//!
//! [`ExecutionPlan`]: crate::ExecutionPlan

pub mod registry;

use std::sync::Arc;

use arrow::datatypes::Schema;
use datafusion_common::{Result, internal_datafusion_err, internal_err};
use datafusion_execution::TaskContext;
use datafusion_expr::physical_planning_context::ScalarSubqueryResults;
use datafusion_expr::{AggregateUDF, ScalarUDF, WindowUDF};
use datafusion_physical_expr::PhysicalExpr;
use datafusion_physical_expr_common::physical_expr::proto_decode::{
    PhysicalExprDecode, PhysicalExprDecodeCtx,
};
use datafusion_physical_expr_common::physical_expr::proto_encode::{
    PhysicalExprEncode, PhysicalExprEncodeCtx,
};
use datafusion_proto_models::protobuf::{
    PhysicalExprNode, PhysicalExtensionNode, PhysicalPlanNode,
    physical_plan_node::PhysicalPlanType,
};

use crate::ExecutionPlan;

pub use registry::{
    ExecutionPlanDecoder, ExecutionPlanRegistry, ExecutionPlanRegistryExt,
    ExtensionExecutionPlan,
};

/// Internal dispatch trait backing [`ExecutionPlanEncodeCtx`].
///
/// Implemented by `datafusion-proto`. Plan authors never name this trait; they
/// call methods on [`ExecutionPlanEncodeCtx`] instead.
///
/// **Not public API.** `pub` only because the implementors live in another
/// crate; `#[doc(hidden)]` records that, so encoding primitives can be added
/// here as the serialization hooks grow without breaking downstream code.
#[doc(hidden)]
pub trait ExecutionPlanEncode {
    /// Serialize a child execution plan (recursing through the central
    /// serializer, so the child's own `try_to_proto` hook is honored).
    fn encode_plan(&self, plan: &Arc<dyn ExecutionPlan>) -> Result<PhysicalPlanNode>;

    /// Serialize a physical expression owned by the plan.
    fn encode_expr(&self, expr: &Arc<dyn PhysicalExpr>) -> Result<PhysicalExprNode>;

    /// Serialize a scalar UDF to an opaque payload. `None` means "decodable by
    /// name alone" (built-ins). Bytes-only: no proto types cross this boundary.
    fn encode_udf(&self, udf: &ScalarUDF) -> Result<Option<Vec<u8>>>;

    /// Serialize an aggregate UDF to an opaque payload. `None` means "decodable
    /// by name alone".
    fn encode_udaf(&self, udaf: &AggregateUDF) -> Result<Option<Vec<u8>>>;

    /// Serialize a window UDF to an opaque payload. `None` means "decodable by
    /// name alone".
    fn encode_udwf(&self, udwf: &WindowUDF) -> Result<Option<Vec<u8>>>;
}

/// Internal dispatch trait backing [`ExecutionPlanDecodeCtx`].
///
/// Implemented by `datafusion-proto`. Plan authors never name this trait; they
/// call methods on [`ExecutionPlanDecodeCtx`] instead.
///
/// **Not public API.** `pub` only because the implementors live in another
/// crate; `#[doc(hidden)]` records that, so decoding primitives can be added
/// here as the serialization hooks grow without breaking downstream code.
#[doc(hidden)]
pub trait ExecutionPlanDecode {
    /// Deserialize a child execution plan (recursing through the central
    /// deserializer, so the child's own `try_from_proto` is honored).
    fn decode_plan(&self, node: &PhysicalPlanNode) -> Result<Arc<dyn ExecutionPlan>>;

    /// Deserialize a child plan with `results` active for scalar subquery
    /// expressions in that plan's subtree.
    fn decode_plan_with_scalar_subquery_results(
        &self,
        node: &PhysicalPlanNode,
        results: ScalarSubqueryResults,
    ) -> Result<Arc<dyn ExecutionPlan>>;

    /// Deserialize a physical expression against `input_schema`.
    fn decode_expr(
        &self,
        node: &PhysicalExprNode,
        input_schema: &Schema,
    ) -> Result<Arc<dyn PhysicalExpr>>;

    /// The session task context, used by plans that need the function registry
    /// or session configuration. Never exposes the proto extension codec.
    fn task_ctx(&self) -> &TaskContext;

    /// Reconstruct a scalar UDF from its name and optional payload. Encapsulates
    /// the lookup-order policy (payload → codec; else registry → codec fallback)
    /// so no plan re-derives it. Bytes-only: no proto types cross this boundary.
    fn decode_udf(&self, name: &str, payload: Option<&[u8]>) -> Result<Arc<ScalarUDF>>;

    /// Reconstruct an aggregate UDF from its name and optional payload.
    fn decode_udaf(
        &self,
        name: &str,
        payload: Option<&[u8]>,
    ) -> Result<Arc<AggregateUDF>>;

    /// Reconstruct a window UDF from its name and optional payload.
    fn decode_udwf(&self, name: &str, payload: Option<&[u8]>) -> Result<Arc<WindowUDF>>;
}

/// The wire parts of an extension plan: its opaque payload, borrowed from the
/// node it was read from, and its already-decoded children.
///
/// Returned by [`ExecutionPlanDecodeCtx::decode_extension`].
pub type ExtensionPlanParts<'n> = (&'n [u8], Vec<Arc<dyn ExecutionPlan>>);

/// Context handed to [`ExecutionPlan::try_to_proto`].
///
///
/// Provides the primitives a plan needs to serialize its children and
/// expressions without naming `datafusion-proto`.
pub struct ExecutionPlanEncodeCtx<'a> {
    encoder: &'a dyn ExecutionPlanEncode,
}

impl<'a> ExecutionPlanEncodeCtx<'a> {
    /// Create a new encode context wrapping an [`ExecutionPlanEncode`]
    /// implementation (supplied by `datafusion-proto`).
    pub fn new(encoder: &'a dyn ExecutionPlanEncode) -> Self {
        Self { encoder }
    }

    /// Serialize a single child plan.
    pub fn encode_child(
        &self,
        plan: &Arc<dyn ExecutionPlan>,
    ) -> Result<PhysicalPlanNode> {
        self.encoder.encode_plan(plan)
    }

    /// Serialize an iterator of child plans.
    pub fn encode_children<'b, I>(&self, plans: I) -> Result<Vec<PhysicalPlanNode>>
    where
        I: IntoIterator<Item = &'b Arc<dyn ExecutionPlan>>,
    {
        plans.into_iter().map(|p| self.encode_child(p)).collect()
    }

    /// Serialize a single physical expression.
    pub fn encode_expr(&self, expr: &Arc<dyn PhysicalExpr>) -> Result<PhysicalExprNode> {
        self.encoder.encode_expr(expr)
    }

    /// Serialize an iterator of physical expressions.
    pub fn encode_expressions<'b, I>(&self, exprs: I) -> Result<Vec<PhysicalExprNode>>
    where
        I: IntoIterator<Item = &'b Arc<dyn PhysicalExpr>>,
    {
        exprs.into_iter().map(|e| self.encode_expr(e)).collect()
    }

    /// Serialize a scalar UDF to an opaque payload (`None` = built-in, decodable
    /// by name). No proto types cross this boundary.
    pub fn encode_udf(&self, udf: &ScalarUDF) -> Result<Option<Vec<u8>>> {
        self.encoder.encode_udf(udf)
    }

    /// Serialize an aggregate UDF to an opaque payload (`None` = decodable by
    /// name).
    pub fn encode_udaf(&self, udaf: &AggregateUDF) -> Result<Option<Vec<u8>>> {
        self.encoder.encode_udaf(udaf)
    }

    /// Serialize a window UDF to an opaque payload (`None` = decodable by name).
    pub fn encode_udwf(&self, udwf: &WindowUDF) -> Result<Option<Vec<u8>>> {
        self.encoder.encode_udwf(udwf)
    }

    /// Serialize an extension plan: an opaque `payload` plus its children,
    /// tagged with `T`'s [`PLAN_NAME`](ExtensionExecutionPlan::PLAN_NAME).
    ///
    /// This is the supported way for a third-party plan to be written by
    /// [`ExecutionPlan::try_to_proto`]:
    /// `PhysicalPlanType` is a closed `oneof`, so extension plans share the
    /// single `PhysicalExtensionNode` variant and carry their own bytes.
    ///
    /// Taking the name from `T` rather than as a string is what keeps the
    /// written name and the registered name from drifting: a mismatch would
    /// compile, then silently fall back to the `PhysicalExtensionCodec` on
    /// whichever machine does the decoding.
    ///
    /// The name is the wire discriminator. Register `T` on the decoding session
    /// (see [`registry`]) and decode selects the plan's own `try_from_proto` by
    /// name; a reader that does not know the name still falls back to its
    /// `PhysicalExtensionCodec`, so stamping it is safe against older readers.
    ///
    /// Recursing into `children` here is what keeps a plan's inputs on the
    /// wire — hand-rolling the `PhysicalExtensionNode` is easy to get wrong.
    pub fn encode_extension<'b, T, I>(
        &self,
        payload: Vec<u8>,
        children: I,
    ) -> Result<PhysicalPlanNode>
    where
        T: ExtensionExecutionPlan,
        I: IntoIterator<Item = &'b Arc<dyn ExecutionPlan>>,
    {
        self.encode_extension_named(T::PLAN_NAME, payload, children)
    }

    /// [`Self::encode_extension`] with the name supplied explicitly.
    ///
    /// For plans registered through
    /// [`ExecutionPlanRegistry::register_decoder`](registry::ExecutionPlanRegistry::register_decoder),
    /// whose name is not a compile-time constant of a single type. Prefer
    /// [`Self::encode_extension`], which cannot disagree with the registration.
    pub fn encode_extension_named<'b, I>(
        &self,
        plan_name: &str,
        payload: Vec<u8>,
        children: I,
    ) -> Result<PhysicalPlanNode>
    where
        I: IntoIterator<Item = &'b Arc<dyn ExecutionPlan>>,
    {
        Ok(PhysicalPlanNode {
            physical_plan_type: Some(PhysicalPlanType::Extension(
                PhysicalExtensionNode {
                    node: payload,
                    inputs: self.encode_children(children)?,
                    plan_name: Some(plan_name.to_string()),
                },
            )),
        })
    }

    /// An expression-level encode context backed by this plan context.
    ///
    /// Lets a plan hand `ctx` to expression-level conversions that own their own
    /// wire logic — e.g.
    /// [`Partitioning::try_to_proto`](datafusion_physical_expr::Partitioning::try_to_proto)
    /// and
    /// [`PhysicalSortExpr::try_to_proto`](datafusion_physical_expr::PhysicalSortExpr::try_to_proto).
    pub fn expr_ctx(&self) -> PhysicalExprEncodeCtx<'_> {
        PhysicalExprEncodeCtx::new(self)
    }
}

/// Lets [`ExecutionPlanEncodeCtx`] back a [`PhysicalExprEncodeCtx`], so
/// expression-level conversions can be reused from plan hooks.
impl PhysicalExprEncode for ExecutionPlanEncodeCtx<'_> {
    fn encode(&self, expr: &Arc<dyn PhysicalExpr>) -> Result<PhysicalExprNode> {
        self.encode_expr(expr)
    }
}

/// Context handed to a plan's `try_from_proto` associated function.
///
/// Provides the primitives a plan needs to deserialize its children and
/// expressions without naming `datafusion-proto`.
pub struct ExecutionPlanDecodeCtx<'a> {
    decoder: &'a dyn ExecutionPlanDecode,
}

impl<'a> ExecutionPlanDecodeCtx<'a> {
    /// Create a new decode context wrapping an [`ExecutionPlanDecode`]
    /// implementation (supplied by `datafusion-proto`).
    pub fn new(decoder: &'a dyn ExecutionPlanDecode) -> Self {
        Self { decoder }
    }

    /// Deserialize a single child plan.
    pub fn decode_child(
        &self,
        node: &PhysicalPlanNode,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.decoder.decode_plan(node)
    }

    /// Deserialize a child plan with `results` active for scalar subquery
    /// expressions in that plan's subtree.
    pub fn decode_child_with_scalar_subquery_results(
        &self,
        node: &PhysicalPlanNode,
        results: ScalarSubqueryResults,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.decoder
            .decode_plan_with_scalar_subquery_results(node, results)
    }

    /// Deserialize a required child plan, producing a uniform "missing required
    /// field" error when the optional wire field is absent.
    pub fn decode_required_child(
        &self,
        node: Option<&PhysicalPlanNode>,
        plan_name: &str,
        field: &str,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let node = node.ok_or_else(|| {
            internal_datafusion_err!("{plan_name} is missing required field '{field}'")
        })?;
        self.decode_child(node)
    }

    /// Deserialize a physical expression against `input_schema`.
    pub fn decode_expr(
        &self,
        node: &PhysicalExprNode,
        input_schema: &Schema,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        self.decoder.decode_expr(node, input_schema)
    }

    /// Deserialize a required physical expression against `input_schema`.
    pub fn decode_required_expr(
        &self,
        node: Option<&PhysicalExprNode>,
        input_schema: &Schema,
        plan_name: &str,
        field: &str,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        let node = node.ok_or_else(|| {
            internal_datafusion_err!("{plan_name} is missing required field '{field}'")
        })?;
        self.decode_expr(node, input_schema)
    }

    /// The session task context (function registry + session config). Never
    /// exposes the proto extension codec.
    pub fn task_ctx(&self) -> &TaskContext {
        self.decoder.task_ctx()
    }

    /// Reconstruct a scalar UDF from its name and optional payload. The
    /// lookup-order policy is owned by `datafusion-proto`; no proto types cross
    /// this boundary.
    pub fn decode_udf(
        &self,
        name: &str,
        payload: Option<&[u8]>,
    ) -> Result<Arc<ScalarUDF>> {
        self.decoder.decode_udf(name, payload)
    }

    /// Reconstruct an aggregate UDF from its name and optional payload.
    pub fn decode_udaf(
        &self,
        name: &str,
        payload: Option<&[u8]>,
    ) -> Result<Arc<AggregateUDF>> {
        self.decoder.decode_udaf(name, payload)
    }

    /// Reconstruct a window UDF from its name and optional payload.
    pub fn decode_udwf(
        &self,
        name: &str,
        payload: Option<&[u8]>,
    ) -> Result<Arc<WindowUDF>> {
        self.decoder.decode_udwf(name, payload)
    }

    /// Deserialize a slice of child plans.
    pub fn decode_children(
        &self,
        nodes: &[PhysicalPlanNode],
    ) -> Result<Vec<Arc<dyn ExecutionPlan>>> {
        nodes.iter().map(|node| self.decode_child(node)).collect()
    }

    /// Deserialize an extension plan written by
    /// [`ExecutionPlanEncodeCtx::encode_extension`], returning its opaque
    /// payload and its decoded children.
    ///
    /// `plan_name` names the caller in the error raised when `node` is not an
    /// extension node at all; it is not re-checked against the wire, because
    /// the name is what routed decoding here in the first place.
    pub fn decode_extension<'n>(
        &self,
        node: &'n PhysicalPlanNode,
        plan_name: &str,
    ) -> Result<ExtensionPlanParts<'n>> {
        let Some(PhysicalPlanType::Extension(extension)) = &node.physical_plan_type
        else {
            return internal_err!(
                "PhysicalPlanNode is not an extension node ({plan_name})"
            );
        };
        Ok((
            extension.node.as_slice(),
            self.decode_children(&extension.inputs)?,
        ))
    }

    /// An expression-level decode context backed by this plan context, bound to
    /// `input_schema`.
    ///
    /// The decode counterpart of
    /// [`ExecutionPlanEncodeCtx::expr_ctx`], for calling conversions such as
    /// [`Partitioning::try_from_proto`](datafusion_physical_expr::Partitioning::try_from_proto).
    pub fn expr_ctx<'s>(&'s self, input_schema: &'s Schema) -> PhysicalExprDecodeCtx<'s> {
        PhysicalExprDecodeCtx::new(input_schema, self)
    }
}

/// Lets [`ExecutionPlanDecodeCtx`] back a [`PhysicalExprDecodeCtx`], so
/// expression-level conversions can be reused from plan hooks.
impl PhysicalExprDecode for ExecutionPlanDecodeCtx<'_> {
    fn decode(
        &self,
        node: &PhysicalExprNode,
        schema: &Schema,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        self.decode_expr(node, schema)
    }
}

/// Assert that a [`PhysicalPlanNode`] carries the expected `PhysicalPlanType`
/// variant, returning a reference to the inner payload, else an `internal_err!`.
/// Mirrors `expect_expr_variant!` on the expression side. Field access on the
/// result auto-derefs through the `Box` that boxed variants use.
#[macro_export]
macro_rules! expect_plan_variant {
    ($node:expr, $variant:path, $plan_name:literal $(,)?) => {{
        match &$node.physical_plan_type {
            Some($variant(inner)) => inner,
            _ => {
                return ::datafusion_common::internal_err!(concat!(
                    "PhysicalPlanNode is not a ",
                    $plan_name
                ));
            }
        }
    }};
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto_test_util::registry_test_plan::RegisteredExec;
    use crate::proto_test_util::{
        StubPlanDecoder, StubPlanEncoder, encoded_child_node, stub_child,
    };

    /// The wire node a `RegisteredExec` with one child encodes to.
    fn encode(plan: &RegisteredExec, encoder: &StubPlanEncoder) -> PhysicalExtensionNode {
        let node = plan
            .try_to_proto(&ExecutionPlanEncodeCtx::new(encoder))
            .expect("encode must succeed")
            .expect("an extension plan must serialize itself");
        match node.physical_plan_type {
            Some(PhysicalPlanType::Extension(extension)) => extension,
            other => panic!("expected an extension node, got {other:?}"),
        }
    }

    #[test]
    fn encode_extension_stamps_the_name_and_recurses_into_children() {
        let encoder = StubPlanEncoder::ok();
        let extension = encode(
            &RegisteredExec::new("payload", vec![stub_child()]),
            &encoder,
        );

        assert_eq!(
            extension.plan_name.as_deref(),
            Some(RegisteredExec::PLAN_NAME)
        );
        assert_eq!(extension.node, b"payload");
        // The child rode the central serializer rather than being dropped.
        assert_eq!(encoder.plan_calls(), 1);
        assert_eq!(extension.inputs, vec![encoded_child_node()]);
    }

    #[test]
    fn encode_extension_propagates_a_child_failure() {
        let encoder = StubPlanEncoder::failing_on_plan(1);
        let err = RegisteredExec::new("payload", vec![stub_child()])
            .try_to_proto(&ExecutionPlanEncodeCtx::new(&encoder))
            .expect_err("a failing child encode must fail the plan");
        assert!(
            err.to_string().contains("stub plan encode failure"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn decode_extension_returns_the_payload_and_children() {
        let node = PhysicalPlanNode {
            physical_plan_type: Some(PhysicalPlanType::Extension(
                PhysicalExtensionNode {
                    node: b"payload".to_vec(),
                    inputs: vec![encoded_child_node(), encoded_child_node()],
                    plan_name: Some(RegisteredExec::PLAN_NAME.to_string()),
                },
            )),
        };
        let decoder = StubPlanDecoder::ok();
        let ctx = ExecutionPlanDecodeCtx::new(&decoder);

        let (payload, children) = ctx
            .decode_extension(&node, RegisteredExec::PLAN_NAME)
            .expect("decode must succeed");

        assert_eq!(payload, b"payload");
        assert_eq!(children.len(), 2);
        assert_eq!(decoder.plan_calls(), 2);
    }

    #[test]
    fn decode_extension_rejects_a_non_extension_node() {
        let decoder = StubPlanDecoder::ok();
        let ctx = ExecutionPlanDecodeCtx::new(&decoder);

        let err = ctx
            .decode_extension(&encoded_child_node(), RegisteredExec::PLAN_NAME)
            .expect_err("a built-in node must not decode as an extension");
        assert!(
            err.to_string().contains(RegisteredExec::PLAN_NAME),
            "unexpected error: {err}"
        );
        assert_eq!(decoder.plan_calls(), 0);
    }

    #[test]
    fn extension_plan_round_trips_through_the_hooks() {
        let encoder = StubPlanEncoder::ok();
        let extension = encode(
            &RegisteredExec::new("payload", vec![stub_child()]),
            &encoder,
        );
        let node = PhysicalPlanNode {
            physical_plan_type: Some(PhysicalPlanType::Extension(extension)),
        };

        let decoder = StubPlanDecoder::ok();
        let decoded =
            RegisteredExec::try_from_proto(&node, &ExecutionPlanDecodeCtx::new(&decoder))
                .expect("decode must succeed");

        let decoded = decoded
            .downcast_ref::<RegisteredExec>()
            .expect("decoded plan must be a RegisteredExec");
        assert_eq!(decoded.payload, "payload");
        assert_eq!(decoded.children.len(), 1);
    }
}
