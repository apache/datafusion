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

//! Name-keyed decoding for extension [`DataSink`]s.
//!
//! Custom sinks serialize through the existing extension-plan wire format:
//! [`DataSinkExec`] is the registered plan type, and its payload identifies
//! the sink decoder. Built-in sinks keep their existing wire variants.

use std::sync::Arc;

use datafusion_common::{Result, plan_datafusion_err, plan_err};
use datafusion_physical_plan::ExecutionPlan;
use datafusion_physical_plan::expect_plan_variant;
use datafusion_physical_plan::proto::{
    ExecutionPlanDecodeCtx, ExecutionPlanEncodeCtx, ExtensionPlanFromProto,
};
use datafusion_proto_models::ProtoDecoderRegistry;
use datafusion_proto_models::protobuf::physical_plan_node::PhysicalPlanType;
use datafusion_proto_models::protobuf::{
    PhysicalExtensionDataSinkNode, PhysicalPlanNode,
};
use prost::Message;

use crate::sink::{DataSink, DataSinkExec};

/// The wire name of an extension [`DataSink`] and the constructor that
/// rebuilds it. Only custom sinks implement this trait.
///
/// Implement [`DataSink::try_to_proto`] using [`extension_data_sink_node`].
/// The helper writes a named `DataSinkExec` plan containing the sink's
/// [`Self::NAME`] and payload. The sink constructor receives that typed
/// payload and can read the decoding session through
/// [`ExecutionPlanDecodeCtx::task_ctx`]. Runtime dependencies should be rebuilt
/// from that session rather than serialized.
pub trait ExtensionDataSinkFromProto: DataSink + Sized {
    /// Namespace this name with the owning crate, e.g. `"my_crate.MySink"`.
    /// Names are scoped to `dyn DataSink`, independently of plan names.
    const NAME: &'static str;

    /// Rebuild the sink from its typed extension payload.
    /// [`DataSinkExec`] supplies the execution-plan wrapper.
    fn try_from_proto(
        node: &PhysicalExtensionDataSinkNode,
        ctx: &ExecutionPlanDecodeCtx<'_>,
    ) -> Result<Arc<dyn DataSink>>;
}

/// Build an extension plan for sink `T`, stamping the wrapper's plan name
/// and the sink's registered name.
///
/// This uses [`ExecutionPlanEncodeCtx::extension_node`] for [`DataSinkExec`],
/// whose payload contains `T::NAME`, the sink bytes, and the optional sort order.
/// The input plan is encoded as the wrapper's only child. The helper lives in
/// this crate because the plan encode context cannot depend on [`DataSink`].
///
/// A sink implementing [`ExtensionDataSinkFromProto`] can use this in its
/// [`DataSink::try_to_proto`] hook:
///
/// ```ignore
/// fn try_to_proto(
///     &self,
///     exec: &DataSinkExec,
///     ctx: &ExecutionPlanEncodeCtx<'_>,
/// ) -> Result<Option<PhysicalPlanNode>> {
///     let payload = self.partition_id.to_le_bytes().to_vec();
///     Ok(Some(extension_data_sink_node::<Self>(ctx, exec, payload)?))
/// }
/// ```
pub fn extension_data_sink_node<T: ExtensionDataSinkFromProto>(
    ctx: &ExecutionPlanEncodeCtx<'_>,
    exec: &DataSinkExec,
    payload: Vec<u8>,
) -> Result<PhysicalPlanNode> {
    let sink = PhysicalExtensionDataSinkNode {
        node: payload,
        sink_name: Some(T::NAME.to_string()),
        sort_order: exec.encode_sort_order(ctx)?,
    };
    ctx.extension_node::<DataSinkExec>(sink.encode_to_vec(), vec![exec.input()])
}

// Private so the decoder representation can evolve without changing the API.
type DataSinkDecoder = fn(
    &PhysicalExtensionDataSinkNode,
    &ExecutionPlanDecodeCtx<'_>,
) -> Result<Arc<dyn DataSink>>;

/// Register `T` under its [`ExtensionDataSinkFromProto::NAME`], together
/// with the shared [`DataSinkExec`] extension-plan decoder.
///
/// Registering the same type twice is a no-op; a different type under a taken
/// name, or an empty name, is an error. Sink names are scoped to `dyn DataSink`.
/// Add these decoders to the same [`ProtoDecoderRegistry`] as the application's
/// plans and expressions with
/// [`SessionConfig::update_extension`](datafusion_execution::config::SessionConfig::update_extension),
/// so decoders added by other libraries are preserved.
pub fn register_data_sink<T: ExtensionDataSinkFromProto>(
    registry: &mut ProtoDecoderRegistry,
) -> Result<()> {
    // The shared wrapper may remain registered if sink registration fails.
    // This is safe: it still requires a registered sink decoder and rejects
    // unknown sinks rather than decoding their payloads through a plan codec.
    DataSinkExec::register_proto(registry)?;
    registry
        .register_decoder::<dyn DataSink, T, DataSinkDecoder>(T::NAME, T::try_from_proto)
}

/// Every registered extension sink name, in arbitrary order.
/// Names registered for other extension kinds are not included.
pub fn data_sink_names(registry: &ProtoDecoderRegistry) -> impl Iterator<Item = &str> {
    registry.names::<dyn DataSink>()
}

/// Decode the wrapper through the same registry dispatch as other extension
/// plans. Only the sink-specific lookup belongs to this crate.
impl ExtensionPlanFromProto for DataSinkExec {
    const NAME: &'static str = "datafusion.DataSinkExec";

    fn try_from_proto(
        node: &PhysicalPlanNode,
        ctx: &ExecutionPlanDecodeCtx<'_>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let extension =
            expect_plan_variant!(node, PhysicalPlanType::Extension, "Extension");
        let [input] = extension.inputs.as_slice() else {
            return plan_err!("DataSinkExec expects exactly one input plan");
        };
        let payload = extension.node.as_slice();
        let sink = PhysicalExtensionDataSinkNode::decode(payload).map_err(|e| {
            plan_datafusion_err!("Failed to decode DataSinkExec payload: {e}")
        })?;
        let Some(name) = sink.sink_name.as_deref().filter(|name| !name.is_empty()) else {
            return plan_err!("DataSinkExec payload is missing a non-empty sink name");
        };
        let registry = ctx
            .task_ctx()
            .session_config()
            .get_extension::<ProtoDecoderRegistry>();
        if let Some(decoder) = registry
            .as_ref()
            .and_then(|registry| registry.decoder::<dyn DataSink, DataSinkDecoder>(name))
        {
            let input = ctx.decode_child(input)?;
            let sort_order = Self::decode_sort_order(
                sink.sort_order.as_ref(),
                ctx,
                input.schema().as_ref(),
            )?;
            let sink = decoder(&sink, ctx)?;
            return Ok(Arc::new(Self::new(input, sink, sort_order)));
        }
        let mut names: Vec<&str> = registry
            .as_ref()
            .map(|registry| data_sink_names(registry).collect())
            .unwrap_or_default();
        names.sort_unstable();
        let names = if names.is_empty() {
            "none".to_string()
        } else {
            names.join(", ")
        };
        plan_err!(
            "No decoder is registered for the extension DataSink '{name}'. Register \
             the sink in the ProtoDecoderRegistry attached to the decoding session's \
             SessionConfig. Registered extension data sinks: {names}"
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::SchemaRef;
    use async_trait::async_trait;
    use datafusion_common::internal_err;
    use datafusion_execution::TaskContext;
    use datafusion_physical_plan::proto::execution_plan_names;
    use datafusion_physical_plan::{
        DisplayAs, DisplayFormatType, SendableRecordBatchStream,
    };
    use std::fmt;

    // These fixtures only exercise registration; their decoders are never invoked.
    macro_rules! registration_sink {
        ($name:ident) => {
            #[derive(Debug)]
            struct $name;

            impl DisplayAs for $name {
                fn fmt_as(
                    &self,
                    _t: DisplayFormatType,
                    f: &mut fmt::Formatter,
                ) -> fmt::Result {
                    write!(f, stringify!($name))
                }
            }

            #[async_trait]
            impl DataSink for $name {
                fn schema(&self) -> &SchemaRef {
                    unreachable!("registration-only fixture")
                }
                async fn write_all(
                    &self,
                    _data: SendableRecordBatchStream,
                    _context: &Arc<TaskContext>,
                ) -> Result<u64> {
                    unreachable!("registration-only fixture")
                }
            }
        };
    }

    registration_sink!(RegisteredSink);
    impl ExtensionDataSinkFromProto for RegisteredSink {
        const NAME: &'static str = "test.RegisteredSink";

        fn try_from_proto(
            _node: &PhysicalExtensionDataSinkNode,
            _ctx: &ExecutionPlanDecodeCtx<'_>,
        ) -> Result<Arc<dyn DataSink>> {
            internal_err!("registration-only fixture")
        }
    }

    registration_sink!(CollidingSink);
    impl ExtensionDataSinkFromProto for CollidingSink {
        const NAME: &'static str = "test.RegisteredSink";

        fn try_from_proto(
            _node: &PhysicalExtensionDataSinkNode,
            _ctx: &ExecutionPlanDecodeCtx<'_>,
        ) -> Result<Arc<dyn DataSink>> {
            internal_err!("registration-only fixture")
        }
    }

    registration_sink!(OtherSink);
    impl ExtensionDataSinkFromProto for OtherSink {
        const NAME: &'static str = "other.OtherSink";

        fn try_from_proto(
            _node: &PhysicalExtensionDataSinkNode,
            _ctx: &ExecutionPlanDecodeCtx<'_>,
        ) -> Result<Arc<dyn DataSink>> {
            internal_err!("registration-only fixture")
        }
    }

    registration_sink!(UnnamedSink);
    impl ExtensionDataSinkFromProto for UnnamedSink {
        const NAME: &'static str = "";

        fn try_from_proto(
            _node: &PhysicalExtensionDataSinkNode,
            _ctx: &ExecutionPlanDecodeCtx<'_>,
        ) -> Result<Arc<dyn DataSink>> {
            internal_err!("registration-only fixture")
        }
    }

    #[test]
    fn register_is_idempotent_and_registers_one_shared_wrapper() -> Result<()> {
        let mut registry = ProtoDecoderRegistry::new();
        register_data_sink::<RegisteredSink>(&mut registry)?;
        register_data_sink::<RegisteredSink>(&mut registry)?;
        assert_eq!(
            data_sink_names(&registry).collect::<Vec<_>>(),
            [RegisteredSink::NAME]
        );
        register_data_sink::<OtherSink>(&mut registry)?;
        let mut names: Vec<_> = data_sink_names(&registry).collect();
        names.sort_unstable();
        assert_eq!(names, [OtherSink::NAME, RegisteredSink::NAME]);
        assert_eq!(
            execution_plan_names(&registry).collect::<Vec<_>>(),
            [DataSinkExec::NAME]
        );
        Ok(())
    }

    #[test]
    fn register_rejects_a_name_collision_between_types() -> Result<()> {
        let mut registry = ProtoDecoderRegistry::new();
        register_data_sink::<RegisteredSink>(&mut registry)?;
        let err = register_data_sink::<CollidingSink>(&mut registry).unwrap_err();
        assert!(err.to_string().contains("already registered"), "{err}");
        assert!(err.to_string().contains(RegisteredSink::NAME), "{err}");
        assert_eq!(
            data_sink_names(&registry).collect::<Vec<_>>(),
            [RegisteredSink::NAME]
        );
        Ok(())
    }

    #[test]
    fn register_rejects_an_empty_name_and_may_retain_the_wrapper() {
        let mut registry = ProtoDecoderRegistry::new();
        let err = register_data_sink::<UnnamedSink>(&mut registry).unwrap_err();
        assert!(err.to_string().contains("empty name"), "{err}");
        assert!(data_sink_names(&registry).next().is_none());
        assert_eq!(
            execution_plan_names(&registry).collect::<Vec<_>>(),
            [DataSinkExec::NAME]
        );
    }

    #[test]
    fn sink_and_source_names_are_independent() -> Result<()> {
        use crate::source::DataSource;
        let mut registry = ProtoDecoderRegistry::new();
        registry.register_decoder::<dyn DataSource, u8, u32>(RegisteredSink::NAME, 7)?;
        assert!(data_sink_names(&registry).next().is_none());
        register_data_sink::<RegisteredSink>(&mut registry)?;
        assert_eq!(
            data_sink_names(&registry).collect::<Vec<_>>(),
            [RegisteredSink::NAME]
        );
        assert_eq!(
            registry.decoder::<dyn DataSource, u32>(RegisteredSink::NAME),
            Some(&7)
        );
        Ok(())
    }

    #[test]
    fn wrapper_collision_does_not_register_the_sink() -> Result<()> {
        let mut registry = ProtoDecoderRegistry::new();
        registry.register_decoder::<dyn ExecutionPlan, u8, u32>(DataSinkExec::NAME, 7)?;
        let err = register_data_sink::<RegisteredSink>(&mut registry).unwrap_err();
        assert!(err.to_string().contains("already registered"), "{err}");
        assert!(data_sink_names(&registry).next().is_none());
        assert_eq!(
            registry.decoder::<dyn ExecutionPlan, u32>(DataSinkExec::NAME),
            Some(&7)
        );
        Ok(())
    }
}
