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

//! Extension `ExecutionPlan`s that decode through the session's name-keyed
//! registry rather than through a `PhysicalExtensionCodec`.
//!
//! The registry closes the loop `try_to_proto` opened: encode already lived on
//! the plan, decode did not. These tests pin the parts the colocated unit tests
//! in `datafusion-physical-plan` structurally cannot reach — that the central
//! dispatch really routes to the registered decoder, that a codec is genuinely
//! not required, and that codec-encoded plans keep decoding exactly as before.

use std::fmt::Formatter;
use std::sync::Arc;

use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::execution::TaskContext;
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_plan::proto::{
    ExecutionPlanDecodeCtx, ExecutionPlanEncodeCtx, ExecutionPlanRegistry,
    ExecutionPlanRegistryExt, ExtensionExecutionPlan,
};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PhysicalExpr, PlanProperties,
    SendableRecordBatchStream,
};
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_common::tree_node::TreeNodeRecursion;
use datafusion_common::{Result, internal_datafusion_err, internal_err};
use datafusion_proto::physical_plan::{
    AsExecutionPlan, DefaultPhysicalExtensionCodec, PhysicalExtensionCodec,
    PhysicalProtoConverterExtension,
};
use datafusion_proto::protobuf::physical_plan_node::PhysicalPlanType;
use datafusion_proto::protobuf::{PhysicalExtensionNode, PhysicalPlanNode};
use prost::Message;

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]))
}

fn child() -> Arc<dyn ExecutionPlan> {
    Arc::new(EmptyExec::new(schema()))
}

/// Session state an extension plan needs at decode time but never puts on the
/// wire — the connection pool a distributed engine rebuilds per worker.
#[derive(Debug, PartialEq, Eq)]
struct WorkerPool {
    endpoints: Vec<String>,
}

/// Boilerplate shared by the test plans: a single child, properties borrowed
/// from it, and no expressions of their own.
macro_rules! single_child_exec {
    // A plan that serializes itself through the hook.
    ($name:ident, $display:literal, self_serializing) => {
        single_child_exec!(@shape $name, $display,
            fn try_to_proto(
                &self,
                ctx: &ExecutionPlanEncodeCtx<'_>,
            ) -> Result<Option<PhysicalPlanNode>> {
                Ok(Some(self.encode_self(ctx)?))
            }
        );
    };
    // A plan that leaves serialization to a `PhysicalExtensionCodec`.
    ($name:ident, $display:literal) => {
        single_child_exec!(@shape $name, $display,);
    };
    (@shape $name:ident, $display:literal, $($hook:item)*) => {
        impl DisplayAs for $name {
            fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
                write!(f, $display)
            }
        }

        impl ExecutionPlan for $name {
            fn name(&self) -> &str {
                $display
            }

            fn properties(&self) -> &Arc<PlanProperties> {
                self.child.properties()
            }

            fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
                vec![&self.child]
            }

            fn apply_expressions(
                &self,
                _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
            ) -> Result<TreeNodeRecursion> {
                Ok(TreeNodeRecursion::Continue)
            }

            fn with_new_children(
                self: Arc<Self>,
                mut children: Vec<Arc<dyn ExecutionPlan>>,
            ) -> Result<Arc<dyn ExecutionPlan>> {
                Ok(Arc::new(self.with_child(children.remove(0))))
            }

            fn execute(
                &self,
                _partition: usize,
                _context: Arc<TaskContext>,
            ) -> Result<SendableRecordBatchStream> {
                internal_err!("{} is a serde-only test plan", $display)
            }

            $($hook)*
        }
    };
}

/// Exactly one child, or a decode error naming the plan.
fn only_child(
    plan_name: &str,
    mut children: Vec<Arc<dyn ExecutionPlan>>,
) -> Result<Arc<dyn ExecutionPlan>> {
    if children.len() != 1 {
        return internal_err!(
            "{plan_name} expects exactly one input, got {}",
            children.len()
        );
    }
    Ok(children.remove(0))
}

// ---------------------------------------------------------------------------
// A session-dependent extension plan, modeled on `NetworkShuffleExec`: the
// stage number is on the wire, the worker pool is rebuilt from the decoding
// session.
// ---------------------------------------------------------------------------

#[derive(Debug)]
struct ShuffleExec {
    stage: u32,
    pool: Arc<WorkerPool>,
    child: Arc<dyn ExecutionPlan>,
}

impl ShuffleExec {
    fn with_child(&self, child: Arc<dyn ExecutionPlan>) -> Self {
        Self {
            stage: self.stage,
            pool: Arc::clone(&self.pool),
            child,
        }
    }
}

#[derive(Clone, PartialEq, Message)]
struct ShuffleExecProto {
    #[prost(uint32, tag = "1")]
    stage: u32,
}

single_child_exec!(ShuffleExec, "ShuffleExec", self_serializing);

impl ShuffleExec {
    fn encode_self(&self, ctx: &ExecutionPlanEncodeCtx<'_>) -> Result<PhysicalPlanNode> {
        let mut payload = vec![];
        ShuffleExecProto { stage: self.stage }
            .encode(&mut payload)
            .map_err(|e| internal_datafusion_err!("failed to encode ShuffleExec: {e}"))?;
        ctx.encode_extension::<Self, _>(payload, self.children())
    }
}

impl ExtensionExecutionPlan for ShuffleExec {
    const PLAN_NAME: &'static str = "datafusion-test.ShuffleExec";

    fn try_from_proto(
        node: &PhysicalPlanNode,
        ctx: &ExecutionPlanDecodeCtx<'_>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let (payload, children) = ctx.decode_extension(node, Self::PLAN_NAME)?;
        let proto = ShuffleExecProto::decode(payload)
            .map_err(|e| internal_datafusion_err!("failed to decode ShuffleExec: {e}"))?;
        // Session-dependent decode: the pool never crossed the wire.
        let pool = ctx
            .task_ctx()
            .session_config()
            .get_extension::<WorkerPool>()
            .ok_or_else(|| {
                internal_datafusion_err!("no WorkerPool configured on this session")
            })?;
        Ok(Arc::new(ShuffleExec {
            stage: proto.stage,
            pool,
            child: only_child(Self::PLAN_NAME, children)?,
        }))
    }
}

// ---------------------------------------------------------------------------
// A second, entirely independent extension plan. Registering both needs no
// composition step: the two names never interact.
// ---------------------------------------------------------------------------

#[derive(Debug)]
struct SamplerExec {
    fraction: f64,
    child: Arc<dyn ExecutionPlan>,
}

impl SamplerExec {
    fn with_child(&self, child: Arc<dyn ExecutionPlan>) -> Self {
        Self {
            fraction: self.fraction,
            child,
        }
    }
}

#[derive(Clone, PartialEq, Message)]
struct SamplerExecProto {
    #[prost(double, tag = "1")]
    fraction: f64,
}

single_child_exec!(SamplerExec, "SamplerExec", self_serializing);

impl SamplerExec {
    fn encode_self(&self, ctx: &ExecutionPlanEncodeCtx<'_>) -> Result<PhysicalPlanNode> {
        let mut payload = vec![];
        SamplerExecProto {
            fraction: self.fraction,
        }
        .encode(&mut payload)
        .map_err(|e| internal_datafusion_err!("failed to encode SamplerExec: {e}"))?;
        ctx.encode_extension::<Self, _>(payload, self.children())
    }
}

impl ExtensionExecutionPlan for SamplerExec {
    const PLAN_NAME: &'static str = "other-crate.SamplerExec";

    fn try_from_proto(
        node: &PhysicalPlanNode,
        ctx: &ExecutionPlanDecodeCtx<'_>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let (payload, children) = ctx.decode_extension(node, Self::PLAN_NAME)?;
        let proto = SamplerExecProto::decode(payload)
            .map_err(|e| internal_datafusion_err!("failed to decode SamplerExec: {e}"))?;
        Ok(Arc::new(SamplerExec {
            fraction: proto.fraction,
            child: only_child(Self::PLAN_NAME, children)?,
        }))
    }
}

// ---------------------------------------------------------------------------
// An unmigrated extension plan: no hook, no name on the wire, decoded by a
// `PhysicalExtensionCodec` exactly as before this feature existed.
// ---------------------------------------------------------------------------

#[derive(Debug)]
struct LegacyExec {
    child: Arc<dyn ExecutionPlan>,
}

impl LegacyExec {
    fn with_child(&self, child: Arc<dyn ExecutionPlan>) -> Self {
        Self { child }
    }
}

single_child_exec!(LegacyExec, "LegacyExec");

#[derive(Debug)]
struct LegacyCodec {}

impl PhysicalExtensionCodec for LegacyCodec {
    fn try_decode(
        &self,
        buf: &[u8],
        inputs: &[Arc<dyn ExecutionPlan>],
        _ctx: &TaskContext,
        _proto_converter: &dyn PhysicalProtoConverterExtension,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if buf != b"legacy" {
            return internal_err!("LegacyCodec does not recognize this payload");
        }
        Ok(Arc::new(LegacyExec {
            child: only_child("LegacyExec", inputs.to_vec())?,
        }))
    }

    fn try_encode(
        &self,
        node: Arc<dyn ExecutionPlan>,
        buf: &mut Vec<u8>,
        _proto_converter: &dyn PhysicalProtoConverterExtension,
    ) -> Result<()> {
        if node.downcast_ref::<LegacyExec>().is_none() {
            return internal_err!("LegacyCodec only encodes LegacyExec");
        }
        buf.extend_from_slice(b"legacy");
        Ok(())
    }
}

/// A codec that claims *every* extension payload, standing in for a codec whose
/// name-blind `try_decode` would happily mis-decode another crate's plan.
#[derive(Debug)]
struct GreedyCodec {}

impl PhysicalExtensionCodec for GreedyCodec {
    fn try_decode(
        &self,
        _buf: &[u8],
        inputs: &[Arc<dyn ExecutionPlan>],
        _ctx: &TaskContext,
        _proto_converter: &dyn PhysicalProtoConverterExtension,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(LegacyExec {
            child: only_child("LegacyExec", inputs.to_vec())?,
        }))
    }

    fn try_encode(
        &self,
        _node: Arc<dyn ExecutionPlan>,
        buf: &mut Vec<u8>,
        _proto_converter: &dyn PhysicalProtoConverterExtension,
    ) -> Result<()> {
        buf.extend_from_slice(b"greedy");
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// A session with `WorkerPool` attached and `plans` registered.
fn session_with(pool_endpoints: &[&str]) -> SessionConfig {
    SessionConfig::new().with_extension(Arc::new(WorkerPool {
        endpoints: pool_endpoints.iter().map(|e| e.to_string()).collect(),
    }))
}

fn encode(
    plan: Arc<dyn ExecutionPlan>,
    codec: &dyn PhysicalExtensionCodec,
) -> Result<PhysicalPlanNode> {
    PhysicalPlanNode::try_from_physical_plan(plan, codec)
}

fn decode(
    node: &PhysicalPlanNode,
    ctx: &SessionContext,
    codec: &dyn PhysicalExtensionCodec,
) -> Result<Arc<dyn ExecutionPlan>> {
    node.try_into_physical_plan(ctx.task_ctx().as_ref(), codec)
}

/// The `PhysicalExtensionNode` at the root of `node`.
fn extension_of(node: &PhysicalPlanNode) -> &PhysicalExtensionNode {
    match &node.physical_plan_type {
        Some(PhysicalPlanType::Extension(extension)) => extension,
        other => panic!("expected an extension node, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[test]
fn extension_plan_round_trips_with_no_codec_at_all() -> Result<()> {
    let config =
        session_with(&["worker-1", "worker-2"]).with_execution_plan::<ShuffleExec>()?;
    let ctx = SessionContext::new_with_config(config);
    // The default codec decodes nothing: whatever survives came from the
    // registry.
    let codec = DefaultPhysicalExtensionCodec {};

    let plan = Arc::new(ShuffleExec {
        stage: 7,
        pool: Arc::new(WorkerPool {
            endpoints: vec!["worker-1".to_string(), "worker-2".to_string()],
        }),
        child: child(),
    });

    let node = encode(plan.clone(), &codec)?;
    assert_eq!(
        extension_of(&node).plan_name.as_deref(),
        Some(ShuffleExec::PLAN_NAME),
        "the encode helper must stamp the plan name"
    );

    let decoded = decode(&node, &ctx, &codec)?;
    let decoded = decoded
        .downcast_ref::<ShuffleExec>()
        .expect("decoded plan must be a ShuffleExec");
    assert_eq!(decoded.stage, 7);
    assert_eq!(decoded.child.name(), "EmptyExec");
    Ok(())
}

#[test]
fn extension_plan_decode_reads_session_state() -> Result<()> {
    let codec = DefaultPhysicalExtensionCodec {};
    let writer = SessionContext::new_with_config(
        session_with(&["writer-only"]).with_execution_plan::<ShuffleExec>()?,
    );
    let node = encode(
        Arc::new(ShuffleExec {
            stage: 1,
            pool: Arc::clone(
                &writer
                    .copied_config()
                    .get_extension::<WorkerPool>()
                    .expect("pool"),
            ),
            child: child(),
        }),
        &codec,
    )?;

    // Decoding on a *different* session rebuilds the pool from that session,
    // proving the pool never rode the wire.
    let reader = SessionContext::new_with_config(
        session_with(&["reader-a", "reader-b"]).with_execution_plan::<ShuffleExec>()?,
    );
    let decoded = decode(&node, &reader, &codec)?;
    let decoded = decoded
        .downcast_ref::<ShuffleExec>()
        .expect("decoded plan must be a ShuffleExec");
    assert_eq!(decoded.pool.endpoints, vec!["reader-a", "reader-b"]);
    Ok(())
}

#[test]
fn independent_extension_plans_coexist_without_a_composed_codec() -> Result<()> {
    let config = session_with(&["worker-1"])
        .with_execution_plan::<ShuffleExec>()?
        .with_execution_plan::<SamplerExec>()?;
    let ctx = SessionContext::new_with_config(config);
    let codec = DefaultPhysicalExtensionCodec {};

    // Two extension plans from two notional crates, nested.
    let plan: Arc<dyn ExecutionPlan> = Arc::new(ShuffleExec {
        stage: 3,
        pool: Arc::new(WorkerPool {
            endpoints: vec!["worker-1".to_string()],
        }),
        child: Arc::new(SamplerExec {
            fraction: 0.25,
            child: child(),
        }),
    });

    let decoded = decode(&encode(Arc::clone(&plan), &codec)?, &ctx, &codec)?;
    let shuffle = decoded
        .downcast_ref::<ShuffleExec>()
        .expect("outer plan must be a ShuffleExec");
    assert_eq!(shuffle.stage, 3);
    let sampler = shuffle
        .child
        .downcast_ref::<SamplerExec>()
        .expect("inner plan must be a SamplerExec");
    assert_eq!(sampler.fraction, 0.25);
    Ok(())
}

#[test]
fn codec_encoded_extension_plans_are_unaffected() -> Result<()> {
    // A plan with no hook: no name on the wire, decoded by its codec, exactly
    // as before the registry existed. The session registers an unrelated plan
    // to prove the registry does not interfere.
    let ctx = SessionContext::new_with_config(
        SessionConfig::new().with_execution_plan::<SamplerExec>()?,
    );
    let codec = LegacyCodec {};

    let node = encode(Arc::new(LegacyExec { child: child() }), &codec)?;
    assert_eq!(
        extension_of(&node).plan_name,
        None,
        "codec-encoded plans must stay anonymous on the wire"
    );

    let decoded = decode(&node, &ctx, &codec)?;
    assert!(decoded.downcast_ref::<LegacyExec>().is_some());
    Ok(())
}

#[test]
fn a_registered_name_wins_over_a_codec_that_claims_everything() -> Result<()> {
    let config = session_with(&["worker-1"]).with_execution_plan::<ShuffleExec>()?;
    let ctx = SessionContext::new_with_config(config);
    // This codec would decode *any* payload into a `LegacyExec`; the name must
    // route around it.
    let codec = GreedyCodec {};

    let plan = Arc::new(ShuffleExec {
        stage: 9,
        pool: Arc::new(WorkerPool {
            endpoints: vec!["worker-1".to_string()],
        }),
        child: child(),
    });

    let decoded = decode(&encode(plan, &codec)?, &ctx, &codec)?;
    assert!(
        decoded.downcast_ref::<ShuffleExec>().is_some(),
        "the registry must take precedence over the codec, got {decoded:?}"
    );
    Ok(())
}

#[test]
fn a_named_plan_the_session_does_not_know_reports_the_missing_registration() -> Result<()>
{
    let codec = DefaultPhysicalExtensionCodec {};
    let writer = SessionContext::new_with_config(
        session_with(&["worker-1"]).with_execution_plan::<ShuffleExec>()?,
    );
    let node = encode(
        Arc::new(ShuffleExec {
            stage: 1,
            pool: Arc::clone(
                &writer
                    .copied_config()
                    .get_extension::<WorkerPool>()
                    .expect("pool"),
            ),
            child: child(),
        }),
        &codec,
    )?;

    // A reader that registered a *different* plan: the fallback runs, fails,
    // and the error has to point at the missing registration rather than at the
    // codec's generic complaint.
    let reader = SessionContext::new_with_config(
        SessionConfig::new().with_execution_plan::<SamplerExec>()?,
    );
    let err = decode(&node, &reader, &codec)
        .expect_err("an unregistered plan name must fail to decode");
    let err = err.to_string();
    assert!(
        err.contains(ShuffleExec::PLAN_NAME),
        "unexpected error: {err}"
    );
    assert!(
        err.contains("ExecutionPlanRegistryExt::register_execution_plan"),
        "unexpected error: {err}"
    );
    assert!(
        !err.contains("bug in DataFusion"),
        "a missing registration is a session-configuration mistake, not a bug: {err}"
    );
    assert!(
        err.contains(SamplerExec::PLAN_NAME),
        "unexpected error: {err}"
    );
    Ok(())
}

#[test]
fn a_registered_decoder_failing_does_not_fall_back_to_the_codec() -> Result<()> {
    // The point of the name is that it *decides* the decoder. Falling back to
    // the codec after a registered decoder failed would resurrect exactly the
    // mis-decode this replaces: `GreedyCodec` would happily turn a corrupt
    // ShuffleExec into a LegacyExec.
    let config = session_with(&["worker-1"]).with_execution_plan::<ShuffleExec>()?;
    let ctx = SessionContext::new_with_config(config);

    let node = PhysicalPlanNode {
        physical_plan_type: Some(PhysicalPlanType::Extension(PhysicalExtensionNode {
            // Not a valid `ShuffleExecProto`: field 1 is declared `uint32` but
            // this is a length-delimited payload, so prost rejects it.
            node: vec![0x0a, 0x01, 0x00],
            inputs: vec![encode(child(), &DefaultPhysicalExtensionCodec {})?],
            plan_name: Some(ShuffleExec::PLAN_NAME.to_string()),
        })),
    };

    let err = decode(&node, &ctx, &GreedyCodec {})
        .expect_err("a registered decoder's failure must be fatal");
    assert!(
        err.to_string().contains("failed to decode ShuffleExec"),
        "the registered decoder's own error must survive, got: {err}"
    );
    Ok(())
}

#[test]
fn a_decoder_registered_by_name_is_reachable_through_dispatch() -> Result<()> {
    // The `register_decoder` escape hatch: one Rust type served under a second,
    // legacy name.
    let mut registry = ExecutionPlanRegistry::new();
    registry.register::<ShuffleExec>()?;
    registry.register_decoder("legacy-alias.ShuffleExec", ShuffleExec::try_from_proto)?;
    let mut config = session_with(&["worker-1"]);
    config.set_execution_plan_registry(Arc::new(registry));
    let ctx = SessionContext::new_with_config(config);
    let codec = DefaultPhysicalExtensionCodec {};

    // A node stamped with the alias rather than with `PLAN_NAME`.
    let mut node = encode(
        Arc::new(ShuffleExec {
            stage: 5,
            pool: Arc::new(WorkerPool {
                endpoints: vec!["worker-1".to_string()],
            }),
            child: child(),
        }),
        &codec,
    )?;
    match &mut node.physical_plan_type {
        Some(PhysicalPlanType::Extension(extension)) => {
            extension.plan_name = Some("legacy-alias.ShuffleExec".to_string());
        }
        other => panic!("expected an extension node, got {other:?}"),
    }

    let decoded = decode(&node, &ctx, &codec)?;
    assert_eq!(
        decoded
            .downcast_ref::<ShuffleExec>()
            .expect("the alias must resolve to the same decoder")
            .stage,
        5
    );
    Ok(())
}

#[test]
fn the_plan_name_survives_the_wire_and_is_absent_for_old_writers() -> Result<()> {
    let config = session_with(&["worker-1"]).with_execution_plan::<ShuffleExec>()?;
    let ctx = SessionContext::new_with_config(config);
    let codec = DefaultPhysicalExtensionCodec {};

    let node = encode(
        Arc::new(ShuffleExec {
            stage: 11,
            pool: Arc::new(WorkerPool {
                endpoints: vec!["worker-1".to_string()],
            }),
            child: child(),
        }),
        &codec,
    )?;

    // Through real bytes, not just the in-memory struct.
    let decoded = PhysicalPlanNode::decode(node.encode_to_vec().as_slice())
        .map_err(|e| internal_datafusion_err!("failed to decode the plan node: {e}"))?;
    assert_eq!(
        extension_of(&decoded).plan_name.as_deref(),
        Some(ShuffleExec::PLAN_NAME)
    );
    assert_eq!(
        decode(&decoded, &ctx, &codec)?
            .downcast_ref::<ShuffleExec>()
            .expect("decoded plan must be a ShuffleExec")
            .stage,
        11
    );

    // A writer that predates the field emits the same bytes without field 3;
    // dropping it must read back as "unnamed", i.e. the codec path.
    let mut old_writer = node.clone();
    match &mut old_writer.physical_plan_type {
        Some(PhysicalPlanType::Extension(extension)) => extension.plan_name = None,
        other => panic!("expected an extension node, got {other:?}"),
    }
    let old_writer = PhysicalPlanNode::decode(old_writer.encode_to_vec().as_slice())
        .map_err(|e| internal_datafusion_err!("failed to decode the plan node: {e}"))?;
    assert_eq!(extension_of(&old_writer).plan_name, None);
    assert!(
        decode(&old_writer, &ctx, &codec).is_err(),
        "an unnamed node must reach the codec, which cannot decode it"
    );
    Ok(())
}

#[test]
fn a_named_plan_can_still_be_decoded_by_a_codec() -> Result<()> {
    // A name nobody registered is not fatal on its own: a codec that
    // understands the payload still decodes it, which is what keeps a mixed
    // fleet (some nodes upgraded, some not) working.
    let ctx = SessionContext::new();
    let node = PhysicalPlanNode {
        physical_plan_type: Some(PhysicalPlanType::Extension(PhysicalExtensionNode {
            node: b"legacy".to_vec(),
            inputs: vec![encode(child(), &DefaultPhysicalExtensionCodec {})?],
            plan_name: Some("some-crate.UnknownExec".to_string()),
        })),
    };

    let decoded = decode(&node, &ctx, &LegacyCodec {})?;
    assert!(decoded.downcast_ref::<LegacyExec>().is_some());
    Ok(())
}
