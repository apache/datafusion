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

mod utils;

#[cfg(feature = "integration-tests")]
mod tests {
    use std::sync::{Arc, OnceLock, Weak};

    use arrow::datatypes::SchemaRef;
    use datafusion::execution::SessionStateBuilder;
    use datafusion::prelude::SessionContext;
    use datafusion_catalog::TableProvider;
    use datafusion_common::{
        DataFusionError, Result, TableReference, exec_err, not_impl_err,
    };
    use datafusion_execution::{TaskContext, TaskContextProvider};
    use datafusion_expr::LogicalPlan;
    use datafusion_expr::logical_plan::Extension;
    use datafusion_ffi::execution::FFI_TaskContextProvider;
    use datafusion_ffi::execution_plan::ForeignExecutionPlan;
    use datafusion_ffi::proto::logical_extension_codec::FFI_LogicalExtensionCodec;
    use datafusion_ffi::proto::physical_extension_codec::FFI_PhysicalExtensionCodec;
    use datafusion_ffi::query_planner::ForeignQueryPlanner;
    use datafusion_ffi::table_provider::ForeignTableProvider;
    use datafusion_ffi::tests::{
        create_test_schema,
        utils::{get_module, get_module_copy},
    };
    use datafusion_physical_plan::ExecutionPlan;
    use datafusion_physical_plan::empty::EmptyExec;
    use datafusion_physical_plan::union::UnionExec;
    use datafusion_proto::logical_plan::LogicalExtensionCodec;
    use datafusion_proto::physical_plan::{
        DefaultPhysicalExtensionCodec, PhysicalExtensionCodec,
        PhysicalProtoConverterExtension,
    };
    use datafusion_session::QueryPlanner;

    #[tokio::test]
    async fn test_ffi_query_planner() -> Result<(), DataFusionError> {
        let module = get_module()?;
        let (ctx, logical_codec) = crate::utils::ctx_and_codec();
        let task_ctx_provider = Arc::clone(&ctx) as Arc<dyn TaskContextProvider>;
        let task_ctx_provider = FFI_TaskContextProvider::from(&task_ctx_provider);
        let physical_codec = FFI_PhysicalExtensionCodec::new(
            Arc::new(DefaultPhysicalExtensionCodec {}),
            None,
            task_ctx_provider,
        );

        let ffi_planner = (module.create_query_planner)(logical_codec, physical_codec);
        let planner: Arc<dyn QueryPlanner + Send + Sync> = (&ffi_planner).into();

        let any_ref: &dyn std::any::Any = planner.as_ref();
        assert!(any_ref.downcast_ref::<ForeignQueryPlanner>().is_some());

        let logical_plan = datafusion_expr::LogicalPlanBuilder::empty(false).build()?;
        let state = ctx.state();
        let physical_plan = planner.create_physical_plan(&logical_plan, &state).await?;

        assert_eq!(physical_plan.name(), "EmptyExec");
        assert!(physical_plan.is::<EmptyExec>());

        Ok(())
    }

    /// Library A's logical codec stores library B's provider while the logical
    /// plan crosses into library C. A real application would encode enough
    /// metadata to reconstruct or locate the provider instead.
    #[derive(Debug, Default)]
    struct LibraryALogicalCodec {
        library_b_provider: OnceLock<Weak<dyn TableProvider>>,
    }

    impl LogicalExtensionCodec for LibraryALogicalCodec {
        fn try_decode(
            &self,
            _buf: &[u8],
            _inputs: &[LogicalPlan],
            _ctx: &TaskContext,
        ) -> Result<Extension> {
            not_impl_err!("logical extension nodes are not used in this test")
        }

        fn try_encode(&self, _node: &Extension, _buf: &mut Vec<u8>) -> Result<()> {
            not_impl_err!("logical extension nodes are not used in this test")
        }

        fn try_decode_table_provider(
            &self,
            buf: &[u8],
            _table_ref: &TableReference,
            _schema: SchemaRef,
            _ctx: &TaskContext,
        ) -> Result<Arc<dyn TableProvider>> {
            if buf != b"library-b-provider" {
                return exec_err!("unexpected library B provider payload");
            }
            self.library_b_provider
                .get()
                .and_then(Weak::upgrade)
                .ok_or_else(|| DataFusionError::Plan("missing library B provider".into()))
        }

        fn try_encode_table_provider(
            &self,
            _table_ref: &TableReference,
            node: Arc<dyn TableProvider>,
            buf: &mut Vec<u8>,
        ) -> Result<()> {
            self.library_b_provider
                .get_or_init(|| Arc::downgrade(&node));
            buf.extend_from_slice(b"library-b-provider");
            Ok(())
        }
    }

    /// Library A's physical codec reconstructs B's opaque foreign plan as an
    /// A-local test plan when the result returns from library C.
    #[derive(Debug)]
    struct LibraryAPhysicalCodec;

    impl PhysicalExtensionCodec for LibraryAPhysicalCodec {
        fn try_decode(
            &self,
            buf: &[u8],
            inputs: &[Arc<dyn ExecutionPlan>],
            _ctx: &TaskContext,
            _proto_converter: &dyn PhysicalProtoConverterExtension,
        ) -> Result<Arc<dyn ExecutionPlan>> {
            if buf != b"library-b-empty-exec" || !inputs.is_empty() {
                return exec_err!("unexpected library B execution plan payload");
            }
            Ok(Arc::new(EmptyExec::new(create_test_schema())))
        }

        fn try_encode(
            &self,
            node: Arc<dyn ExecutionPlan>,
            buf: &mut Vec<u8>,
            _proto_converter: &dyn PhysicalProtoConverterExtension,
        ) -> Result<()> {
            if !node.is::<ForeignExecutionPlan>() {
                return exec_err!("expected library B's plan to be foreign");
            }
            buf.extend_from_slice(b"library-b-empty-exec");
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_three_library_query_planner_restores_type_identity() -> Result<()> {
        // Library A: datafusion-python owns the session and codec registry.
        let state = SessionStateBuilder::new_with_default_features()
            .with_physical_optimizer_rules(vec![])
            .build();
        let ctx = Arc::new(SessionContext::new_with_state(state));
        let task_ctx_provider = Arc::clone(&ctx) as Arc<dyn TaskContextProvider>;
        let ffi_task_ctx_provider = FFI_TaskContextProvider::from(&task_ctx_provider);
        let logical_codec = FFI_LogicalExtensionCodec::new(
            Arc::new(LibraryALogicalCodec::default()),
            None,
            ffi_task_ctx_provider.clone(),
        );
        let physical_codec = FFI_PhysicalExtensionCodec::new(
            Arc::new(LibraryAPhysicalCodec),
            None,
            ffi_task_ctx_provider,
        );

        let library_b = get_module_copy("query_planner_library_b")?;
        let library_c = get_module_copy("query_planner_library_c")?;

        // Library B: reuse the synchronous table provider from the existing
        // FFI integration-test module.
        let ffi_provider = (library_b.create_table)(true, logical_codec.clone());
        let provider: Arc<dyn TableProvider> = (&ffi_provider).into();
        assert!(provider.downcast_ref::<ForeignTableProvider>().is_some());
        ctx.register_table("library_b", provider)?;
        let logical_plan = ctx.table("library_b").await?.into_optimized_plan()?;

        // Library C: a foreign query planner sees B's scan result as opaque,
        // but can downcast its own UnionExec. Its result is serialized rather
        // than returned as FFI_ExecutionPlan.
        let ffi_planner = (library_c.create_query_planner)(logical_codec, physical_codec);
        let planner: Arc<dyn QueryPlanner + Send + Sync> = (&ffi_planner).into();
        let planner_any: &dyn std::any::Any = planner.as_ref();
        assert!(planner_any.downcast_ref::<ForeignQueryPlanner>().is_some());

        let state = ctx.state();
        let physical_plan = planner.create_physical_plan(&logical_plan, &state).await?;

        // Deserialization in A reconstructs the full result as A-local
        // concrete nodes, including the plans that originated in B.
        assert!(physical_plan.is::<UnionExec>());
        assert!(!physical_plan.is::<ForeignExecutionPlan>());
        let children = physical_plan.children();
        assert_eq!(children.len(), 2);
        assert!(children.iter().all(|child| child.is::<EmptyExec>()));
        assert!(
            children
                .iter()
                .all(|child| !child.is::<ForeignExecutionPlan>())
        );

        Ok(())
    }
}
