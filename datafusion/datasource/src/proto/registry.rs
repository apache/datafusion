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

//! Name-keyed decoding for extension [`DataSource`]s.
//!
//! Custom sources serialize through the existing extension-plan wire format:
//! [`DataSourceExec`] is the registered plan type, and its payload identifies
//! the source decoder. Built-in sources keep their existing wire variants.

use std::sync::Arc;

use datafusion_common::{Result, plan_datafusion_err, plan_err};
use datafusion_physical_plan::ExecutionPlan;
use datafusion_physical_plan::expect_plan_variant;
use datafusion_physical_plan::proto::{
    ExecutionPlanDecodeCtx, ExecutionPlanEncodeCtx, ExtensionPlanFromProto,
    register_execution_plan,
};
use datafusion_proto_models::ProtoDecoderRegistry;
use datafusion_proto_models::protobuf::physical_plan_node::PhysicalPlanType;
use datafusion_proto_models::protobuf::{
    PhysicalExtensionDataSourceNode, PhysicalPlanNode,
};
use prost::Message;

use crate::source::{DataSource, DataSourceExec};

/// The wire name of an extension [`DataSource`] and the constructor that
/// rebuilds it. Only custom sources implement this trait.
///
/// Implement [`DataSource::try_to_proto`] using [`extension_data_source_node`].
/// The helper writes a named `DataSourceExec` plan containing the source's
/// [`Self::NAME`] and payload. The source constructor receives that typed
/// payload and can read the decoding session through
/// [`ExecutionPlanDecodeCtx::task_ctx`]. Runtime dependencies should be rebuilt
/// from that session rather than serialized.
pub trait ExtensionDataSourceFromProto: DataSource + Sized {
    /// Namespace this name with the owning crate, e.g. `"my_crate.MySource"`.
    /// Names are scoped to `dyn DataSource`, independently of plan names.
    const NAME: &'static str;

    /// Rebuild the source from its typed extension payload.
    /// [`DataSourceExec`] supplies the execution-plan wrapper.
    fn try_from_proto(
        node: &PhysicalExtensionDataSourceNode,
        ctx: &ExecutionPlanDecodeCtx<'_>,
    ) -> Result<Arc<dyn DataSource>>;
}

/// Build an extension plan for source `T`, stamping the wrapper's plan name
/// and the source's registered name.
///
/// This uses [`ExecutionPlanEncodeCtx::extension_node`] for [`DataSourceExec`],
/// whose payload contains `T::NAME` and the source bytes. The helper lives in
/// this crate because the plan encode context cannot depend on [`DataSource`].
///
/// A source implementing [`ExtensionDataSourceFromProto`] can use this in its
/// [`DataSource::try_to_proto`] hook:
///
/// ```ignore
/// fn try_to_proto(
///     &self,
///     ctx: &ExecutionPlanEncodeCtx<'_>,
/// ) -> Result<Option<PhysicalPlanNode>> {
///     let payload = self.partition_id.to_le_bytes().to_vec();
///     Ok(Some(extension_data_source_node::<Self>(ctx, payload)?))
/// }
/// ```
pub fn extension_data_source_node<T: ExtensionDataSourceFromProto>(
    ctx: &ExecutionPlanEncodeCtx<'_>,
    payload: Vec<u8>,
) -> Result<PhysicalPlanNode> {
    let source = PhysicalExtensionDataSourceNode {
        node: payload,
        source_name: Some(T::NAME.to_string()),
    };
    ctx.extension_node::<DataSourceExec>(source.encode_to_vec(), vec![])
}

// Private so the decoder representation can evolve without changing the API.
type DataSourceDecoder = fn(
    &PhysicalExtensionDataSourceNode,
    &ExecutionPlanDecodeCtx<'_>,
) -> Result<Arc<dyn DataSource>>;

/// Register `T` under its [`ExtensionDataSourceFromProto::NAME`], together
/// with the shared [`DataSourceExec`] extension-plan decoder.
///
/// Registering the same type twice is a no-op; a different type under a taken
/// name, or an empty name, is an error. Source names are scoped to `dyn DataSource`.
/// Add these decoders to the same [`ProtoDecoderRegistry`] as the application's
/// plans and expressions. Libraries should add to the application's registry
/// rather than replace it with their own.
///
/// Built-in sources do not implement the extension trait:
///
/// ```compile_fail
/// use datafusion_datasource::memory::MemorySourceConfig;
/// use datafusion_datasource::proto::{ProtoDecoderRegistry, register_data_source};
/// let mut registry = ProtoDecoderRegistry::new();
/// register_data_source::<MemorySourceConfig>(&mut registry).unwrap();
/// ```
///
/// The same imports compile without attempting to register a built-in:
///
/// ```
/// use datafusion_datasource::memory::MemorySourceConfig;
/// #[allow(unused_imports)]
/// use datafusion_datasource::proto::{ProtoDecoderRegistry, register_data_source};
/// let _ = (MemorySourceConfig::try_from_proto, ProtoDecoderRegistry::new());
/// ```
pub fn register_data_source<T: ExtensionDataSourceFromProto>(
    registry: &mut ProtoDecoderRegistry,
) -> Result<()> {
    register_execution_plan::<DataSourceExec>(registry)?;
    registry.register_decoder::<dyn DataSource, T, DataSourceDecoder>(
        T::NAME,
        T::try_from_proto,
    )
}

/// Every registered extension source name, in arbitrary order.
/// Names registered for other extension kinds are not included.
pub fn data_source_names(registry: &ProtoDecoderRegistry) -> impl Iterator<Item = &str> {
    registry.names::<dyn DataSource>()
}

/// Decode the wrapper through the same registry dispatch as other extension
/// plans. Only the source-specific lookup belongs to this crate.
impl ExtensionPlanFromProto for DataSourceExec {
    const NAME: &'static str = "datafusion.DataSourceExec";

    fn try_from_proto(
        node: &PhysicalPlanNode,
        ctx: &ExecutionPlanDecodeCtx<'_>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let extension =
            expect_plan_variant!(node, PhysicalPlanType::Extension, "Extension");
        if !extension.inputs.is_empty() {
            return plan_err!("DataSourceExec expects no input plans");
        }
        let payload = extension.node.as_slice();
        let source = PhysicalExtensionDataSourceNode::decode(payload).map_err(|e| {
            plan_datafusion_err!("Failed to decode DataSourceExec payload: {e}")
        })?;
        let Some(name) = source
            .source_name
            .as_deref()
            .filter(|name| !name.is_empty())
        else {
            return plan_err!(
                "DataSourceExec payload is missing a non-empty source name"
            );
        };
        let registry = ctx
            .task_ctx()
            .session_config()
            .get_extension::<ProtoDecoderRegistry>();
        if let Some(decoder) = registry.as_ref().and_then(|registry| {
            registry.decoder::<dyn DataSource, DataSourceDecoder>(name)
        }) {
            return decoder(&source, ctx).map(|source| Arc::new(Self::new(source)) as _);
        }
        let mut names: Vec<&str> = registry
            .as_ref()
            .map(|registry| data_source_names(registry).collect())
            .unwrap_or_default();
        names.sort_unstable();
        let names = if names.is_empty() {
            "none".to_string()
        } else {
            names.join(", ")
        };
        plan_err!(
            "No decoder is registered for the extension DataSource '{name}'. Register \
             the source in the ProtoDecoderRegistry attached to the decoding session's \
             SessionConfig. Registered extension data sources: {names}"
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fmt::{self, Formatter};

    use datafusion_common::{Statistics, internal_err, tree_node::TreeNodeRecursion};
    use datafusion_execution::TaskContext;
    use datafusion_physical_expr::projection::ProjectionExprs;
    use datafusion_physical_expr::{EquivalenceProperties, Partitioning, PhysicalExpr};
    use datafusion_physical_plan::{DisplayFormatType, SendableRecordBatchStream};

    use crate::memory::MemorySourceConfig;

    // Registration-only fixtures. The memory source supplies DataSource's
    // unrelated methods; these tests never invoke a decoder.
    macro_rules! registration_source {
        ($name:ident) => {
            #[derive(Debug)]
            struct $name(MemorySourceConfig);

            impl DataSource for $name {
                fn open(
                    &self,
                    partition: usize,
                    ctx: Arc<TaskContext>,
                ) -> Result<SendableRecordBatchStream> {
                    self.0.open(partition, ctx)
                }
                fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter) -> fmt::Result {
                    self.0.fmt_as(t, f)
                }
                fn output_partitioning(&self) -> Partitioning {
                    self.0.output_partitioning()
                }
                fn eq_properties(&self) -> EquivalenceProperties {
                    self.0.eq_properties()
                }
                fn partition_statistics(
                    &self,
                    partition: Option<usize>,
                ) -> Result<Arc<Statistics>> {
                    self.0.partition_statistics(partition)
                }
                fn with_fetch(
                    &self,
                    limit: Option<usize>,
                ) -> Option<Arc<dyn DataSource>> {
                    self.0.with_fetch(limit)
                }
                fn fetch(&self) -> Option<usize> {
                    self.0.fetch()
                }
                fn try_swapping_with_projection(
                    &self,
                    projection: &ProjectionExprs,
                ) -> Result<Option<Arc<dyn DataSource>>> {
                    self.0.try_swapping_with_projection(projection)
                }
                fn apply_expressions(
                    &self,
                    f: &mut dyn FnMut(
                        &Arc<dyn PhysicalExpr>,
                    ) -> Result<TreeNodeRecursion>,
                ) -> Result<TreeNodeRecursion> {
                    self.0.apply_expressions(f)
                }
            }
        };
    }

    registration_source!(RegisteredSource);

    impl ExtensionDataSourceFromProto for RegisteredSource {
        const NAME: &'static str = "test.RegisteredSource";

        fn try_from_proto(
            _node: &PhysicalExtensionDataSourceNode,
            _ctx: &ExecutionPlanDecodeCtx<'_>,
        ) -> Result<Arc<dyn DataSource>> {
            internal_err!("registration-only test source")
        }
    }

    registration_source!(CollidingSource);

    impl ExtensionDataSourceFromProto for CollidingSource {
        const NAME: &'static str = "test.RegisteredSource";

        fn try_from_proto(
            _node: &PhysicalExtensionDataSourceNode,
            _ctx: &ExecutionPlanDecodeCtx<'_>,
        ) -> Result<Arc<dyn DataSource>> {
            internal_err!("registration-only test source")
        }
    }

    registration_source!(OtherSource);

    impl ExtensionDataSourceFromProto for OtherSource {
        const NAME: &'static str = "other.OtherSource";

        fn try_from_proto(
            _node: &PhysicalExtensionDataSourceNode,
            _ctx: &ExecutionPlanDecodeCtx<'_>,
        ) -> Result<Arc<dyn DataSource>> {
            internal_err!("registration-only test source")
        }
    }

    registration_source!(UnnamedSource);

    impl ExtensionDataSourceFromProto for UnnamedSource {
        const NAME: &'static str = "";

        fn try_from_proto(
            _node: &PhysicalExtensionDataSourceNode,
            _ctx: &ExecutionPlanDecodeCtx<'_>,
        ) -> Result<Arc<dyn DataSource>> {
            internal_err!("registration-only test source")
        }
    }

    #[test]
    fn register_is_idempotent_for_the_same_type() -> Result<()> {
        let mut registry = ProtoDecoderRegistry::new();
        register_data_source::<RegisteredSource>(&mut registry)?;
        register_data_source::<RegisteredSource>(&mut registry)?;
        assert_eq!(
            data_source_names(&registry).collect::<Vec<_>>(),
            [RegisteredSource::NAME]
        );
        Ok(())
    }

    #[test]
    fn register_rejects_a_name_collision_between_types() -> Result<()> {
        let mut registry = ProtoDecoderRegistry::new();
        register_data_source::<RegisteredSource>(&mut registry)?;
        let err = register_data_source::<CollidingSource>(&mut registry)
            .expect_err("colliding registration must fail");
        let message = err.to_string();
        assert!(message.contains(RegisteredSource::NAME), "{message}");
        assert!(message.contains("already registered"), "{message}");
        Ok(())
    }

    #[test]
    fn register_keeps_distinct_names_apart() -> Result<()> {
        let mut registry = ProtoDecoderRegistry::new();
        register_data_source::<RegisteredSource>(&mut registry)?;
        register_data_source::<OtherSource>(&mut registry)?;
        let mut names: Vec<_> = data_source_names(&registry).collect();
        names.sort_unstable();
        assert_eq!(names, [OtherSource::NAME, RegisteredSource::NAME]);
        Ok(())
    }

    #[test]
    fn register_rejects_an_empty_source_name() {
        let mut registry = ProtoDecoderRegistry::new();
        let err = register_data_source::<UnnamedSource>(&mut registry)
            .expect_err("an empty NAME must fail");
        assert!(err.to_string().contains("empty name"), "{err}");
        assert!(data_source_names(&registry).next().is_none());
    }

    #[test]
    fn a_name_registered_for_another_kind_is_not_a_data_source() -> Result<()> {
        trait OtherKind {}
        let mut registry = ProtoDecoderRegistry::new();
        registry.register_decoder::<dyn OtherKind, u8, u32>(RegisteredSource::NAME, 7)?;
        assert!(data_source_names(&registry).next().is_none());
        register_data_source::<RegisteredSource>(&mut registry)?;
        assert_eq!(
            data_source_names(&registry).collect::<Vec<_>>(),
            [RegisteredSource::NAME]
        );
        Ok(())
    }

    #[test]
    fn register_source_also_registers_the_shared_plan_decoder() -> Result<()> {
        use datafusion_physical_plan::proto::execution_plan_names;

        let mut registry = ProtoDecoderRegistry::new();
        register_data_source::<RegisteredSource>(&mut registry)?;
        register_data_source::<OtherSource>(&mut registry)?;
        assert_eq!(
            execution_plan_names(&registry).collect::<Vec<_>>(),
            [DataSourceExec::NAME]
        );
        Ok(())
    }
}
