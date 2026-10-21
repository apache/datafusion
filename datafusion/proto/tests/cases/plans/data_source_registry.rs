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

//! Public serialization APIs for custom sources, including execution after decoding.

use std::fmt::{self, Formatter};
use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, Int64Array};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::execution::TaskContext;
use datafusion::physical_expr::projection::ProjectionExprs;
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::proto::{
    ExecutionPlanDecodeCtx, ExecutionPlanEncodeCtx, ExtensionPlanFromProto,
    register_execution_plan,
};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PhysicalExpr, PlanProperties,
    SendableRecordBatchStream, collect,
};
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_common::tree_node::TreeNodeRecursion;
use datafusion_common::{DataFusionError, Result, Statistics, internal_err};
use datafusion_datasource::memory::MemorySourceConfig;
use datafusion_datasource::proto::{
    ExtensionDataSourceFromProto, ProtoDecoderRegistry, extension_data_source_node,
    register_data_source,
};
use datafusion_datasource::source::{DataSource, DataSourceExec};
use datafusion_proto::bytes::{
    physical_plan_from_bytes_with_extension_codec,
    physical_plan_to_bytes_with_extension_codec,
};
use datafusion_proto::physical_plan::{
    AsExecutionPlan, DefaultPhysicalExtensionCodec, PhysicalExtensionCodec,
    PhysicalProtoConverterExtension,
};
use datafusion_proto::protobuf::physical_plan_node::PhysicalPlanType;
use datafusion_proto::protobuf::{
    PhysicalExtensionDataSourceNode, PhysicalExtensionNode, PhysicalPlanNode,
};
use prost::Message;

#[derive(Debug)]
struct ReaderState(i64);

// Share the DataSource boilerplate, as the extension-plan tests do for their
// execution plans. Each source's serialization behavior remains explicit.
macro_rules! memory_source {
    ($name:ident, $($hook:item)*) => {
        #[derive(Debug, Clone)]
        struct $name {
            value: i64,
            memory: MemorySourceConfig,
        }

        impl $name {
            fn new(value: i64, reader_state: Arc<ReaderState>) -> Result<Self> {
                let array: ArrayRef = Arc::new(Int64Array::from(vec![value + reader_state.0]));
                let batch = RecordBatch::try_from_iter([("a", array)])?;
                let memory = MemorySourceConfig::try_new(&[vec![batch.clone()]], batch.schema(), None)?;
                Ok(Self { value, memory })
            }
        }

        impl DataSource for $name {
            fn open(&self, partition: usize, ctx: Arc<TaskContext>) -> Result<SendableRecordBatchStream> {
                self.memory.open(partition, ctx)
            }
            fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> fmt::Result {
                write!(f, "{}({})", stringify!($name), self.value)
            }
            fn output_partitioning(&self) -> Partitioning { self.memory.output_partitioning() }
            fn eq_properties(&self) -> EquivalenceProperties { self.memory.eq_properties() }
            fn partition_statistics(&self, partition: Option<usize>) -> Result<Arc<Statistics>> {
                self.memory.partition_statistics(partition)
            }
            fn with_fetch(&self, _limit: Option<usize>) -> Option<Arc<dyn DataSource>> { None }
            fn fetch(&self) -> Option<usize> { None }
            fn try_swapping_with_projection(&self, _projection: &ProjectionExprs) -> Result<Option<Arc<dyn DataSource>>> { Ok(None) }
            fn apply_expressions(&self, f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>) -> Result<TreeNodeRecursion> {
                self.memory.apply_expressions(f)
            }
            $($hook)*
        }
    };
}

memory_source!(
    RegisteredSource,
    fn try_to_proto(
        &self,
        ctx: &ExecutionPlanEncodeCtx<'_>,
    ) -> Result<Option<PhysicalPlanNode>> {
        Ok(Some(extension_data_source_node::<Self>(
            ctx,
            self.value.to_le_bytes().to_vec(),
        )?))
    }
);

impl ExtensionDataSourceFromProto for RegisteredSource {
    const NAME: &'static str = "test.RegisteredSource";

    fn try_from_proto(
        node: &PhysicalExtensionDataSourceNode,
        ctx: &ExecutionPlanDecodeCtx<'_>,
    ) -> Result<Arc<dyn DataSource>> {
        let value = read_value(&node.node)?;
        let state = ctx
            .task_ctx()
            .session_config()
            .get_extension::<ReaderState>()
            .unwrap_or_else(|| Arc::new(ReaderState(0)));
        Ok(Arc::new(Self::new(value, state)?))
    }
}

memory_source!(
    OtherSource,
    fn try_to_proto(
        &self,
        ctx: &ExecutionPlanEncodeCtx<'_>,
    ) -> Result<Option<PhysicalPlanNode>> {
        Ok(Some(extension_data_source_node::<Self>(
            ctx,
            self.value.to_le_bytes().to_vec(),
        )?))
    }
);

impl ExtensionDataSourceFromProto for OtherSource {
    const NAME: &'static str = "other.OtherSource";

    fn try_from_proto(
        node: &PhysicalExtensionDataSourceNode,
        ctx: &ExecutionPlanDecodeCtx<'_>,
    ) -> Result<Arc<dyn DataSource>> {
        let value = read_value(&node.node)?;
        let state = ctx
            .task_ctx()
            .session_config()
            .get_extension::<ReaderState>()
            .unwrap_or_else(|| Arc::new(ReaderState(0)));
        Ok(Arc::new(Self::new(value, state)?))
    }
}

// No serialization hook: this source is owned by the legacy codec.
memory_source!(LegacySource,);

fn read_value(buf: &[u8]) -> Result<i64> {
    let Ok(bytes) = buf.try_into() else {
        return internal_err!("expected eight bytes");
    };
    Ok(i64::from_le_bytes(bytes))
}

fn source(value: i64) -> Result<Arc<dyn ExecutionPlan>> {
    Ok(DataSourceExec::from_data_source(RegisteredSource::new(
        value,
        Arc::new(ReaderState(0)),
    )?))
}

fn session(
    register: impl FnOnce(&mut ProtoDecoderRegistry) -> Result<()>,
) -> Result<SessionContext> {
    let mut registry = ProtoDecoderRegistry::new();
    register(&mut registry)?;
    let config = SessionConfig::new().with_extension(Arc::new(registry));
    Ok(SessionContext::new_with_config(config))
}

fn decoded_source<T: DataSource>(plan: &Arc<dyn ExecutionPlan>) -> &T {
    plan.downcast_ref::<DataSourceExec>()
        .unwrap()
        .data_source()
        .downcast_ref::<T>()
        .unwrap()
}

fn encode(plan: Arc<dyn ExecutionPlan>) -> Result<PhysicalPlanNode> {
    PhysicalPlanNode::try_from_physical_plan(plan, &DefaultPhysicalExtensionCodec {})
}

fn decode(
    node: &PhysicalPlanNode,
    ctx: &SessionContext,
    codec: &dyn PhysicalExtensionCodec,
) -> Result<Arc<dyn ExecutionPlan>> {
    // Exercise the actual extension-plan wire format.
    let node = PhysicalPlanNode::decode(node.encode_to_vec().as_slice()).unwrap();
    node.try_into_physical_plan(ctx.task_ctx().as_ref(), codec)
}

fn extension_of(node: &PhysicalPlanNode) -> &PhysicalExtensionNode {
    let Some(PhysicalPlanType::Extension(extension)) = &node.physical_plan_type else {
        panic!("expected an extension plan");
    };
    extension
}

fn extension_of_mut(node: &mut PhysicalPlanNode) -> &mut PhysicalExtensionNode {
    let Some(PhysicalPlanType::Extension(extension)) = &mut node.physical_plan_type
    else {
        panic!("expected an extension plan");
    };
    extension
}

fn source_payload(node: &PhysicalPlanNode) -> PhysicalExtensionDataSourceNode {
    PhysicalExtensionDataSourceNode::decode(extension_of(node).node.as_slice())
        .expect("valid source payload")
}

fn update_source_payload(
    node: &mut PhysicalPlanNode,
    update: impl FnOnce(&mut PhysicalExtensionDataSourceNode),
) {
    let mut source = source_payload(node);
    update(&mut source);
    extension_of_mut(node).node = source.encode_to_vec();
}

#[tokio::test]
async fn source_registry_roundtrip_executes_without_codec() -> Result<()> {
    let mut registry = ProtoDecoderRegistry::new();
    register_data_source::<RegisteredSource>(&mut registry)?;
    let config = SessionConfig::new().with_extension(Arc::new(registry));
    let reader = SessionContext::new_with_config(config);
    let codec = DefaultPhysicalExtensionCodec {};
    let original = source(7)?;
    let node = encode(original.clone())?;
    assert_eq!(
        extension_of(&node).plan_name.as_deref(),
        Some(DataSourceExec::NAME)
    );
    assert!(extension_of(&node).inputs.is_empty());
    let payload = source_payload(&node);
    assert_eq!(payload.source_name.as_deref(), Some(RegisteredSource::NAME));
    assert_eq!(payload.node, 7_i64.to_le_bytes());
    let bytes = physical_plan_to_bytes_with_extension_codec(original.clone(), &codec)?;
    let decoded = physical_plan_from_bytes_with_extension_codec(
        &bytes,
        reader.task_ctx().as_ref(),
        &codec,
    )?;
    let source = decoded_source::<RegisteredSource>(&decoded);
    assert_eq!(source.value, 7);
    assert_eq!(original.schema(), decoded.schema());
    let batches = collect(decoded, reader.task_ctx()).await?;
    assert_eq!(
        batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        7
    );
    Ok(())
}

#[tokio::test]
async fn source_decode_reads_decoding_session() -> Result<()> {
    let mut registry = ProtoDecoderRegistry::new();
    register_data_source::<RegisteredSource>(&mut registry)?;
    let config = SessionConfig::new()
        .with_extension(Arc::new(ReaderState(100)))
        .with_extension(Arc::new(registry));
    let reader = SessionContext::new_with_config(config);
    let decoded = decode(
        &encode(source(7)?)?,
        &reader,
        &DefaultPhysicalExtensionCodec {},
    )?;
    let batches = collect(decoded, reader.task_ctx()).await?;
    assert_eq!(
        batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0),
        107
    );
    Ok(())
}

#[test]
fn independent_source_types_share_one_registry() -> Result<()> {
    let ctx = session(|r| {
        register_data_source::<RegisteredSource>(r)?;
        register_data_source::<OtherSource>(r)
    })?;
    let codec = DefaultPhysicalExtensionCodec {};
    let decoded = decode(&encode(source(11)?)?, &ctx, &codec)?;
    assert_eq!(decoded_source::<RegisteredSource>(&decoded).value, 11);

    let other =
        DataSourceExec::from_data_source(OtherSource::new(22, Arc::new(ReaderState(0)))?);
    let decoded = decode(&encode(other)?, &ctx, &codec)?;
    assert_eq!(decoded_source::<OtherSource>(&decoded).value, 22);
    Ok(())
}

/// Codec for sources that leave serialization to PhysicalExtensionCodec.
#[derive(Debug)]
struct LegacyCodec {}

impl PhysicalExtensionCodec for LegacyCodec {
    fn try_decode(
        &self,
        buf: &[u8],
        inputs: &[Arc<dyn ExecutionPlan>],
        _ctx: &TaskContext,
        _converter: &dyn PhysicalProtoConverterExtension,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        assert!(inputs.is_empty());
        Ok(DataSourceExec::from_data_source(LegacySource::new(
            read_value(buf)?,
            Arc::new(ReaderState(0)),
        )?))
    }
    fn try_encode(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        buf: &mut Vec<u8>,
        _converter: &dyn PhysicalProtoConverterExtension,
    ) -> Result<()> {
        buf.extend_from_slice(&decoded_source::<LegacySource>(&plan).value.to_le_bytes());
        Ok(())
    }
}

// This codec would accept even a malformed source payload. It makes accidental
// fallback after a registered decoder's error observable.
#[derive(Debug)]
struct GreedyCodec {}

impl PhysicalExtensionCodec for GreedyCodec {
    fn try_decode(
        &self,
        _buf: &[u8],
        inputs: &[Arc<dyn ExecutionPlan>],
        _ctx: &TaskContext,
        _converter: &dyn PhysicalProtoConverterExtension,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        assert!(inputs.is_empty());
        Ok(DataSourceExec::from_data_source(LegacySource::new(
            0,
            Arc::new(ReaderState(0)),
        )?))
    }
    fn try_encode(
        &self,
        _plan: Arc<dyn ExecutionPlan>,
        _buf: &mut Vec<u8>,
        _converter: &dyn PhysicalProtoConverterExtension,
    ) -> Result<()> {
        internal_err!("GreedyCodec does not encode")
    }
}

#[test]
fn registered_source_does_not_call_plan_codec() -> Result<()> {
    let ctx = session(register_data_source::<RegisteredSource>)?;
    let decoded = decode(&encode(source(9)?)?, &ctx, &GreedyCodec {})?;
    assert_eq!(decoded_source::<RegisteredSource>(&decoded).value, 9);
    Ok(())
}

#[test]
fn registered_decoder_failure_does_not_fall_back_to_codec() -> Result<()> {
    let ctx = session(register_data_source::<RegisteredSource>)?;
    let mut node = encode(source(9)?)?;
    // The source's real decoder requires eight bytes for its i64 payload.
    update_source_payload(&mut node, |source| source.node = vec![0]);
    let err = decode(&node, &ctx, &GreedyCodec {})
        .expect_err("a registered decoder's failure must be fatal");
    assert!(err.to_string().contains("expected eight bytes"), "{err}");
    Ok(())
}

#[test]
fn malformed_wrapper_payload_does_not_fall_back_to_codec() -> Result<()> {
    let ctx = session(register_data_source::<RegisteredSource>)?;
    let mut node = encode(source(9)?)?;
    // Zero is not a valid protobuf tag.
    extension_of_mut(&mut node).node = vec![0];
    let err = decode(&node, &ctx, &GreedyCodec {}).unwrap_err();
    assert!(
        err.to_string()
            .contains("Failed to decode DataSourceExec payload"),
        "{err}"
    );
    Ok(())
}

#[test]
fn source_wrapper_rejects_input_plans() -> Result<()> {
    let ctx = session(register_data_source::<RegisteredSource>)?;
    let mut node = encode(source(9)?)?;
    extension_of_mut(&mut node).inputs.push(encode(source(1)?)?);
    let err = decode(&node, &ctx, &GreedyCodec {}).unwrap_err();
    assert!(
        err.to_string()
            .contains("DataSourceExec expects no input plans"),
        "{err}"
    );
    Ok(())
}

#[test]
fn unregistered_sources_do_not_fall_back_to_plan_codec() -> Result<()> {
    let node = encode(source(9)?)?;
    for ctx in [
        session(register_execution_plan::<DataSourceExec>)?,
        session(register_data_source::<OtherSource>)?,
    ] {
        // The greedy codec accepts these bytes, so falling back would silently
        // return the wrong plan instead of reporting the missing registration.
        let err = decode(&node, &ctx, &GreedyCodec {})
            .expect_err("an unregistered source must not reach the plan codec");
        assert!(matches!(err.find_root(), DataFusionError::Plan(_)));
        let message = err.to_string();
        assert!(message.contains("No decoder is registered"), "{message}");
        assert!(message.contains(RegisteredSource::NAME), "{message}");
    }
    Ok(())
}

#[test]
fn unnamed_sources_do_not_fall_back_to_plan_codec() -> Result<()> {
    let ctx = session(register_data_source::<RegisteredSource>)?;
    for source_name in [None, Some(String::new())] {
        let mut node = encode(source(9)?)?;
        update_source_payload(&mut node, |source| source.source_name = source_name);
        let err = decode(&node, &ctx, &GreedyCodec {})
            .expect_err("an unnamed source must not reach the plan codec");
        assert!(matches!(err.find_root(), DataFusionError::Plan(_)));
        assert!(
            err.to_string().contains("missing a non-empty source name"),
            "{err}"
        );
    }
    Ok(())
}

#[test]
fn missing_source_registration_reports_registered_names() -> Result<()> {
    let ctx = session(register_data_source::<OtherSource>)?;
    let node = encode(source(9)?)?;
    let err = decode(&node, &ctx, &DefaultPhysicalExtensionCodec {}).unwrap_err();
    let message = err.to_string();
    assert!(message.contains(RegisteredSource::NAME), "{message}");
    assert!(message.contains(OtherSource::NAME), "{message}");
    assert!(message.contains("No decoder is registered"), "{message}");
    let err = decode(
        &node,
        &session(register_execution_plan::<DataSourceExec>)?,
        &DefaultPhysicalExtensionCodec {},
    )
    .unwrap_err();
    assert!(err.to_string().contains("extension data sources: none"));
    Ok(())
}

#[test]
fn unregistered_wrapper_uses_existing_plan_codec_fallback() -> Result<()> {
    let node = encode(source(9)?)?;
    let ctx = SessionContext::new();
    // With no DataSourceExec decoder registered, the outer node is an unknown
    // extension plan. The existing plan-codec policy still applies to it.
    let decoded = decode(&node, &ctx, &GreedyCodec {})?;
    assert_eq!(decoded_source::<LegacySource>(&decoded).value, 0);
    let err = decode(&node, &ctx, &DefaultPhysicalExtensionCodec {}).unwrap_err();
    assert!(err.to_string().contains(DataSourceExec::NAME), "{err}");
    Ok(())
}

#[test]
fn codec_owned_sources_keep_the_legacy_plan_wire_format() -> Result<()> {
    let ctx = session(register_data_source::<RegisteredSource>)?;
    let codec = LegacyCodec {};
    let node = PhysicalPlanNode::try_from_physical_plan(
        DataSourceExec::from_data_source(LegacySource::new(
            33,
            Arc::new(ReaderState(0)),
        )?),
        &codec,
    )?;
    let Some(PhysicalPlanType::Extension(extension)) = &node.physical_plan_type else {
        panic!("expected legacy extension node")
    };
    assert_eq!(extension.plan_name, None);
    assert_eq!(
        decoded_source::<LegacySource>(&decode(&node, &ctx, &codec)?).value,
        33
    );
    Ok(())
}

/// A custom plan with the same wire name as RegisteredSource.
#[derive(Debug)]
struct NamedPlan(Arc<dyn ExecutionPlan>);

impl DisplayAs for NamedPlan {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> fmt::Result {
        write!(f, "NamedPlan")
    }
}

impl ExecutionPlan for NamedPlan {
    fn name(&self) -> &str {
        "NamedPlan"
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        self.0.properties()
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.0]
    }
    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(Self(Arc::clone(&children[0]))))
    }
    fn execute(
        &self,
        partition: usize,
        ctx: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        self.0.execute(partition, ctx)
    }
    fn try_to_proto(
        &self,
        ctx: &ExecutionPlanEncodeCtx<'_>,
    ) -> Result<Option<PhysicalPlanNode>> {
        Ok(Some(ctx.extension_node::<Self>(vec![], self.children())?))
    }
}

impl ExtensionPlanFromProto for NamedPlan {
    const NAME: &'static str = RegisteredSource::NAME;
    fn try_from_proto(
        node: &PhysicalPlanNode,
        ctx: &ExecutionPlanDecodeCtx<'_>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let Some(PhysicalPlanType::Extension(extension)) = &node.physical_plan_type
        else {
            return internal_err!("expected plan extension");
        };
        if extension.inputs.len() != 1 {
            return internal_err!("expected one child");
        }
        Ok(Arc::new(Self(ctx.decode_child(&extension.inputs[0])?)))
    }
}

#[test]
fn plan_and_source_with_same_name_roundtrip_together() -> Result<()> {
    let ctx = session(|r| {
        register_execution_plan::<NamedPlan>(r)?;
        register_data_source::<RegisteredSource>(r)
    })?;
    let plan: Arc<dyn ExecutionPlan> = Arc::new(NamedPlan(source(42)?));
    let decoded = decode(&encode(plan)?, &ctx, &DefaultPhysicalExtensionCodec {})?;
    let decoded = decoded.downcast_ref::<NamedPlan>().unwrap();
    assert_eq!(decoded_source::<RegisteredSource>(&decoded.0).value, 42);
    Ok(())
}

#[cfg(feature = "json")]
#[test]
fn source_registry_json_roundtrip() -> Result<()> {
    let ctx = session(register_data_source::<RegisteredSource>)?;
    let node = encode(source(55)?)?;
    let json = serde_json::to_string(&node).unwrap();
    assert!(json.contains("planName"));
    assert!(json.contains(DataSourceExec::NAME));
    let node = serde_json::from_str(&json).unwrap();
    let decoded = decode(&node, &ctx, &DefaultPhysicalExtensionCodec {})?;
    assert_eq!(decoded_source::<RegisteredSource>(&decoded).value, 55);
    Ok(())
}
