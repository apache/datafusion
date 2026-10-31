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

//! Public serialization APIs for custom sinks, including execution after decoding.

use std::fmt::{self, Formatter};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use datafusion::arrow::array::{ArrayRef, Int64Array, UInt64Array};
use datafusion::arrow::compute::SortOptions;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::execution::TaskContext;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::{LexRequirement, PhysicalSortRequirement};
use datafusion::physical_plan::proto::{
    ExecutionPlanDecodeCtx, ExecutionPlanEncodeCtx, ExtensionPlanFromProto,
};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, SendableRecordBatchStream, collect,
};
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_common::{DataFusionError, Result, internal_err};
use datafusion_datasource::memory::MemorySourceConfig;
use datafusion_datasource::proto::{
    ExtensionDataSinkFromProto, ProtoDecoderRegistry, extension_data_sink_node,
    register_data_sink,
};
use datafusion_datasource::sink::{DataSink, DataSinkExec};
use datafusion_datasource::source::DataSourceExec;
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
    PhysicalExtensionDataSinkNode, PhysicalExtensionNode, PhysicalPlanNode,
};
use prost::Message;

#[derive(Debug, Default)]
struct WrittenValues(Mutex<Vec<i64>>);

fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]))
}

// Only the DataSink boilerplate is shared. Each decoder remains explicit.
macro_rules! test_sink {
    ($name:ident, $($hook:item)*) => {
        #[derive(Debug)]
        struct $name {
            value: i64,
            schema: SchemaRef,
            written: Arc<WrittenValues>,
        }

        impl $name {
            fn new(value: i64, written: Arc<WrittenValues>) -> Self {
                Self { value, schema: schema(), written }
            }
        }

        impl DisplayAs for $name {
            fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> fmt::Result {
                write!(f, "{}({})", stringify!($name), self.value)
            }
        }

        #[async_trait]
        impl DataSink for $name {
            fn schema(&self) -> &SchemaRef { &self.schema }

            async fn write_all(
                &self,
                data: SendableRecordBatchStream,
                _context: &Arc<TaskContext>,
            ) -> Result<u64> {
                let mut rows = 0;
                for batch in datafusion::physical_plan::common::collect(data).await? {
                    let values = batch.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
                    self.written.0.lock().unwrap().extend(values.values().iter().map(|value| value + self.value));
                    rows += batch.num_rows() as u64;
                }
                Ok(rows)
            }
            $($hook)*
        }
    };
}

test_sink!(
    RegisteredSink,
    fn try_to_proto(
        &self,
        exec: &DataSinkExec,
        ctx: &ExecutionPlanEncodeCtx<'_>,
    ) -> Result<Option<PhysicalPlanNode>> {
        Ok(Some(extension_data_sink_node::<Self>(
            ctx,
            exec,
            self.value.to_le_bytes().to_vec(),
        )?))
    }
);

impl ExtensionDataSinkFromProto for RegisteredSink {
    const NAME: &'static str = "test.RegisteredSink";

    fn try_from_proto(
        node: &PhysicalExtensionDataSinkNode,
        ctx: &ExecutionPlanDecodeCtx<'_>,
    ) -> Result<Arc<dyn DataSink>> {
        let written = ctx
            .task_ctx()
            .session_config()
            .get_extension::<WrittenValues>()
            .unwrap_or_default();
        Ok(Arc::new(Self::new(read_value(&node.node)?, written)))
    }
}

test_sink!(
    OtherSink,
    fn try_to_proto(
        &self,
        exec: &DataSinkExec,
        ctx: &ExecutionPlanEncodeCtx<'_>,
    ) -> Result<Option<PhysicalPlanNode>> {
        Ok(Some(extension_data_sink_node::<Self>(
            ctx,
            exec,
            self.value.to_le_bytes().to_vec(),
        )?))
    }
);

impl ExtensionDataSinkFromProto for OtherSink {
    // Deliberately share the wrapper's plan name: kinds must remain independent.
    const NAME: &'static str = "datafusion.DataSinkExec";

    fn try_from_proto(
        node: &PhysicalExtensionDataSinkNode,
        _ctx: &ExecutionPlanDecodeCtx<'_>,
    ) -> Result<Arc<dyn DataSink>> {
        Ok(Arc::new(Self::new(read_value(&node.node)?, Arc::default())))
    }
}

test_sink!(LegacySink,);

fn read_value(buf: &[u8]) -> Result<i64> {
    let Ok(bytes) = buf.try_into() else {
        return internal_err!("expected eight bytes");
    };
    Ok(i64::from_le_bytes(bytes))
}

fn input() -> Result<Arc<dyn ExecutionPlan>> {
    let values: ArrayRef = Arc::new(Int64Array::from(vec![3, 5]));
    let batch = RecordBatch::try_new(schema(), vec![values])?;
    Ok(DataSourceExec::from_data_source(
        MemorySourceConfig::try_new(&[vec![batch]], schema(), None)?,
    ))
}

fn plan(
    value: i64,
    sort_order: Option<LexRequirement>,
) -> Result<Arc<dyn ExecutionPlan>> {
    Ok(Arc::new(DataSinkExec::new(
        input()?,
        Arc::new(RegisteredSink::new(value, Arc::default())),
        sort_order,
    )))
}

fn session(
    register: impl FnOnce(&mut ProtoDecoderRegistry) -> Result<()>,
) -> Result<SessionContext> {
    let mut config = SessionConfig::new();
    config.update_extension(register)?;
    Ok(SessionContext::new_with_config(config))
}

fn encode(plan: Arc<dyn ExecutionPlan>) -> Result<PhysicalPlanNode> {
    PhysicalPlanNode::try_from_physical_plan(plan, &DefaultPhysicalExtensionCodec {})
}

fn decode(
    node: &PhysicalPlanNode,
    ctx: &SessionContext,
    codec: &dyn PhysicalExtensionCodec,
) -> Result<Arc<dyn ExecutionPlan>> {
    let node = PhysicalPlanNode::decode(node.encode_to_vec().as_slice()).unwrap();
    node.try_into_physical_plan(ctx.task_ctx().as_ref(), codec)
}

fn extension(node: &PhysicalPlanNode) -> &PhysicalExtensionNode {
    let Some(PhysicalPlanType::Extension(extension)) = &node.physical_plan_type else {
        panic!("expected extension plan");
    };
    extension
}

fn extension_mut(node: &mut PhysicalPlanNode) -> &mut PhysicalExtensionNode {
    let Some(PhysicalPlanType::Extension(extension)) = &mut node.physical_plan_type
    else {
        panic!("expected extension plan");
    };
    extension
}

fn update_payload(
    node: &mut PhysicalPlanNode,
    update: impl FnOnce(&mut PhysicalExtensionDataSinkNode),
) {
    let mut sink =
        PhysicalExtensionDataSinkNode::decode(extension(node).node.as_slice()).unwrap();
    update(&mut sink);
    extension_mut(node).node = sink.encode_to_vec();
}

fn decoded_sink<T: DataSink>(plan: &Arc<dyn ExecutionPlan>) -> &T {
    plan.downcast_ref::<DataSinkExec>()
        .unwrap()
        .sink()
        .downcast_ref::<T>()
        .unwrap()
}

#[tokio::test]
async fn sink_registry_roundtrip_executes_with_receiving_session_state() -> Result<()> {
    let written = Arc::new(WrittenValues::default());
    let mut config = SessionConfig::new().with_extension(Arc::clone(&written));
    config.update_extension(register_data_sink::<RegisteredSink>)?;
    let reader = SessionContext::new_with_config(config);
    let codec = DefaultPhysicalExtensionCodec {};
    let original = plan(7, None)?;
    let node = encode(original.clone())?;
    assert_eq!(
        extension(&node).plan_name.as_deref(),
        Some(DataSinkExec::NAME)
    );
    assert_eq!(extension(&node).inputs.len(), 1);
    let payload =
        PhysicalExtensionDataSinkNode::decode(extension(&node).node.as_slice()).unwrap();
    assert_eq!(payload.sink_name.as_deref(), Some(RegisteredSink::NAME));
    assert_eq!(payload.node, 7_i64.to_le_bytes());
    assert!(payload.sort_order.is_none());
    let bytes = physical_plan_to_bytes_with_extension_codec(original, &codec)?;
    let decoded = physical_plan_from_bytes_with_extension_codec(
        &bytes,
        &reader.task_ctx(),
        &codec,
    )?;
    assert_eq!(decoded_sink::<RegisteredSink>(&decoded).value, 7);
    let batches = collect(decoded, reader.task_ctx()).await?;
    assert_eq!(
        batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap()
            .value(0),
        2
    );
    assert_eq!(*written.0.lock().unwrap(), [10, 12]);
    Ok(())
}

#[test]
fn sink_input_and_sort_requirements_roundtrip() -> Result<()> {
    let order: LexRequirement = [PhysicalSortRequirement::new(
        Arc::new(Column::new("a", 0)),
        Some(SortOptions {
            descending: true,
            nulls_first: false,
        }),
    )]
    .into();
    let original = plan(7, Some(order.clone()))?;
    let reader = session(register_data_sink::<RegisteredSink>)?;
    let decoded = decode(
        &encode(original.clone())?,
        &reader,
        &DefaultPhysicalExtensionCodec {},
    )?;
    let decoded = decoded.downcast_ref::<DataSinkExec>().unwrap();
    let original = original.downcast_ref::<DataSinkExec>().unwrap();
    assert_eq!(decoded.sort_order(), &Some(order));
    assert_eq!(decoded.input().schema(), original.input().schema());
    assert_eq!(decoded.schema(), original.schema());
    Ok(())
}

#[derive(Debug)]
struct GreedyCodec;

impl PhysicalExtensionCodec for GreedyCodec {
    fn try_decode(
        &self,
        _buf: &[u8],
        inputs: &[Arc<dyn ExecutionPlan>],
        _ctx: &TaskContext,
        _converter: &dyn PhysicalProtoConverterExtension,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(DataSinkExec::new(
            Arc::clone(&inputs[0]),
            Arc::new(LegacySink::new(0, Arc::default())),
            None,
        )))
    }
    fn try_encode(
        &self,
        _node: Arc<dyn ExecutionPlan>,
        _buf: &mut Vec<u8>,
        _converter: &dyn PhysicalProtoConverterExtension,
    ) -> Result<()> {
        internal_err!("unexpected codec encode")
    }
}

#[test]
fn registered_sink_does_not_call_plan_codec() -> Result<()> {
    let reader = session(register_data_sink::<RegisteredSink>)?;
    let decoded = decode(&encode(plan(9, None)?)?, &reader, &GreedyCodec)?;
    assert_eq!(decoded_sink::<RegisteredSink>(&decoded).value, 9);
    Ok(())
}

#[test]
fn malformed_sink_and_wrapper_payloads_do_not_fall_back_to_codec() -> Result<()> {
    let reader = session(register_data_sink::<RegisteredSink>)?;
    let mut node = encode(plan(9, None)?)?;
    update_payload(&mut node, |sink| sink.node = vec![0]);
    let err = decode(&node, &reader, &GreedyCodec).unwrap_err();
    assert!(matches!(err.find_root(), DataFusionError::Internal(_)));
    assert!(err.to_string().contains("expected eight bytes"), "{err}");
    extension_mut(&mut node).node = vec![0];
    let err = decode(&node, &reader, &GreedyCodec).unwrap_err();
    assert!(
        err.to_string()
            .contains("Failed to decode DataSinkExec payload"),
        "{err}"
    );
    Ok(())
}

#[test]
fn sink_wrapper_requires_exactly_one_input() -> Result<()> {
    let reader = session(register_data_sink::<RegisteredSink>)?;
    for count in [0, 2] {
        let mut node = encode(plan(9, None)?)?;
        extension_mut(&mut node).inputs = vec![encode(input()?)?; count];
        let err = decode(&node, &reader, &GreedyCodec).unwrap_err();
        assert!(
            err.to_string().contains("expects exactly one input plan"),
            "{err}"
        );
    }
    Ok(())
}

#[test]
fn invalid_input_and_ordering_do_not_fall_back_to_codec() -> Result<()> {
    let reader = session(register_data_sink::<RegisteredSink>)?;
    let mut node = encode(plan(9, None)?)?;
    extension_mut(&mut node).inputs[0] = PhysicalPlanNode::default();
    assert!(decode(&node, &reader, &GreedyCodec).is_err());

    let mut node = encode(plan(9, None)?)?;
    update_payload(&mut node, |sink| {
        sink.sort_order =
            Some(datafusion_proto::protobuf::PhysicalSortExprNodeCollection {
                physical_sort_expr_nodes: vec![
                    datafusion_proto::protobuf::PhysicalSortExprNode::default(),
                ],
            });
    });
    let err = decode(&node, &reader, &GreedyCodec).unwrap_err();
    assert!(
        err.to_string()
            .contains("Unexpected empty physical expression"),
        "{err}"
    );
    Ok(())
}

#[test]
fn unnamed_sinks_do_not_fall_back_to_plan_codec() -> Result<()> {
    let reader = session(register_data_sink::<RegisteredSink>)?;
    for name in [None, Some(String::new())] {
        let mut node = encode(plan(9, None)?)?;
        update_payload(&mut node, |sink| sink.sink_name = name);
        let err = decode(&node, &reader, &GreedyCodec).unwrap_err();
        assert!(
            err.to_string().contains("missing a non-empty sink name"),
            "{err}"
        );
    }
    Ok(())
}

#[test]
fn unregistered_sinks_report_registered_names_without_codec_fallback() -> Result<()> {
    for (reader, expected) in [
        (session(DataSinkExec::register_proto)?, "none"),
        (session(register_data_sink::<OtherSink>)?, OtherSink::NAME),
    ] {
        let err = decode(&encode(plan(9, None)?)?, &reader, &GreedyCodec).unwrap_err();
        assert!(matches!(err.find_root(), DataFusionError::Plan(_)));
        let message = err.to_string();
        assert!(message.contains(RegisteredSink::NAME), "{message}");
        assert!(
            message.contains(&format!("Registered extension data sinks: {expected}")),
            "{message}"
        );
    }
    Ok(())
}

#[test]
fn unregistered_wrapper_uses_existing_plan_codec_fallback() -> Result<()> {
    let reader = SessionContext::new();
    let node = encode(plan(9, None)?)?;
    let decoded = decode(&node, &reader, &GreedyCodec)?;
    assert_eq!(decoded_sink::<LegacySink>(&decoded).value, 0);
    let err = decode(&node, &reader, &DefaultPhysicalExtensionCodec {}).unwrap_err();
    assert!(err.to_string().contains(DataSinkExec::NAME), "{err}");
    Ok(())
}

#[test]
fn independent_sink_types_and_matching_plan_names_coexist() -> Result<()> {
    let reader = session(|registry| {
        register_data_sink::<RegisteredSink>(registry)?;
        register_data_sink::<OtherSink>(registry)
    })?;
    let decoded = decode(
        &encode(plan(11, None)?)?,
        &reader,
        &DefaultPhysicalExtensionCodec {},
    )?;
    assert_eq!(decoded_sink::<RegisteredSink>(&decoded).value, 11);
    let other = Arc::new(DataSinkExec::new(
        input()?,
        Arc::new(OtherSink::new(22, Arc::default())),
        None,
    ));
    let decoded = decode(&encode(other)?, &reader, &DefaultPhysicalExtensionCodec {})?;
    assert_eq!(decoded_sink::<OtherSink>(&decoded).value, 22);
    Ok(())
}

#[derive(Debug)]
struct LegacyCodec;

impl PhysicalExtensionCodec for LegacyCodec {
    fn try_decode(
        &self,
        buf: &[u8],
        inputs: &[Arc<dyn ExecutionPlan>],
        _ctx: &TaskContext,
        _converter: &dyn PhysicalProtoConverterExtension,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(DataSinkExec::new(
            Arc::clone(&inputs[0]),
            Arc::new(LegacySink::new(read_value(buf)?, Arc::default())),
            None,
        )))
    }
    fn try_encode(
        &self,
        node: Arc<dyn ExecutionPlan>,
        buf: &mut Vec<u8>,
        _converter: &dyn PhysicalProtoConverterExtension,
    ) -> Result<()> {
        let sink = decoded_sink::<LegacySink>(&node);
        buf.extend_from_slice(&sink.value.to_le_bytes());
        Ok(())
    }
}

#[test]
fn codec_owned_sinks_keep_the_legacy_wire_format() -> Result<()> {
    let reader = session(register_data_sink::<RegisteredSink>)?;
    let original = Arc::new(DataSinkExec::new(
        input()?,
        Arc::new(LegacySink::new(33, Arc::default())),
        None,
    ));
    let node = PhysicalPlanNode::try_from_physical_plan(original, &LegacyCodec)?;
    assert!(extension(&node).plan_name.is_none());
    assert_eq!(extension(&node).node, 33_i64.to_le_bytes());
    let decoded = decode(&node, &reader, &LegacyCodec)?;
    assert_eq!(decoded_sink::<LegacySink>(&decoded).value, 33);
    Ok(())
}

#[cfg(feature = "json")]
#[test]
fn sink_registry_json_roundtrip() -> Result<()> {
    let reader = session(register_data_sink::<RegisteredSink>)?;
    let node = encode(plan(44, None)?)?;
    let json = serde_json::to_string(&node).unwrap();
    assert!(json.contains(DataSinkExec::NAME));
    let node = serde_json::from_str(&json).unwrap();
    let decoded = decode(&node, &reader, &DefaultPhysicalExtensionCodec {})?;
    assert_eq!(decoded_sink::<RegisteredSink>(&decoded).value, 44);
    Ok(())
}
