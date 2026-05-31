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
    use std::any::Any;
    use std::sync::{Arc, RwLock};

    use datafusion::execution::context::{QueryPlanner, SessionContext};
    use datafusion::execution::{SessionState, SessionStateBuilder};
    use datafusion_common::DataFusionError;
    use datafusion_execution::TaskContextProvider;
    use datafusion_expr::LogicalPlan;
    use datafusion_ffi::execution::FFI_TaskContextProvider;
    use datafusion_ffi::execution_plan::tests::EmptyExec;
    use datafusion_ffi::proto::logical_extension_codec::FFI_LogicalExtensionCodec;
    use datafusion_ffi::session::planner::{FFI_QueryPlanner, QueryPlannerWeak};
    use datafusion_ffi::tests::utils::get_module;
    use datafusion_physical_plan::ExecutionPlan;
    use datafusion_proto::logical_plan::DefaultLogicalExtensionCodec;

    #[tokio::test]
    async fn test_ffi_query_planner() -> Result<(), DataFusionError> {
        let module = get_module()?;
        let (ctx, codec) = super::utils::ctx_and_codec();

        let ffi_planner = (module.create_query_planner)(codec);
        let foreign_planner: Arc<dyn QueryPlannerWeak> = (&ffi_planner).into();

        let df = ctx.sql("select 1 as i").await?;
        let logical_plan = df.logical_plan();

        let empty_exec = foreign_planner
            .create_physical_plan(logical_plan, &ctx.state())
            .await?;

        assert!(empty_exec.downcast_ref::<EmptyExec>().is_none());
        assert!(empty_exec.schema().field_with_name("i").is_ok());
        assert_eq!("EmptyExec", empty_exec.name());

        Ok(())
    }

    // Verifies that an FFI planner can be injected into an already-constructed context.
    //
    // `SessionContext` requires a `QueryPlanner` at construction time, but the FFI planner
    // is only available after the context exists (it needs a codec, which needs the context).
    // `DynamicForeignQueryPlaner` breaks this cycle by acting as a placeholder that can be
    // swapped for the real FFI planner once both sides are ready.
    #[tokio::test]
    async fn test_ffi_dynamic_query_planner() -> Result<(), DataFusionError> {
        let module = get_module()?;
        let (ctx, codec) = ctx_codec_planner();

        let ffi_planner = (module.create_query_planner)(codec);

        let state = ctx.state();
        let planner = state.query_planner().as_ref();
        let planer = (planner as &dyn Any)
            .downcast_ref::<DynamicForeignQueryPlanner>()
            .expect("proper query planner");

        planer.set_planner(&ffi_planner);
        let df = ctx.sql("select 1 as i").await?;
        let plan = df.create_physical_plan().await?;

        assert!(plan.downcast_ref::<EmptyExec>().is_none());
        assert!(plan.schema().field_with_name("i").is_ok());
        assert_eq!("EmptyExec", plan.name());

        Ok(())
    }

    /// Builds a `SessionContext` pre-wired with a `DynamicForeignQueryPlaner` placeholder
    /// and a matching `FFI_LogicalExtensionCodec`. The planner starts empty; call
    /// `DynamicForeignQueryPlaner::set_planner` to install the real FFI planner before use.
    pub fn ctx_codec_planner() -> (Arc<SessionContext>, FFI_LogicalExtensionCodec) {
        let query_planner = Arc::new(DynamicForeignQueryPlanner::default());
        let state = SessionStateBuilder::new_with_default_features()
            .with_query_planner(query_planner)
            .build();
        let ctx = Arc::new(SessionContext::from(state));

        let task_ctx_provider = Arc::clone(&ctx) as Arc<dyn TaskContextProvider>;
        let task_ctx_provider = FFI_TaskContextProvider::from(&task_ctx_provider);
        let codec = FFI_LogicalExtensionCodec::new(
            Arc::new(DefaultLogicalExtensionCodec {}),
            None,
            task_ctx_provider,
        );

        (ctx, codec)
    }

    /// A `QueryPlanner` placeholder that can be hot-swapped with an FFI-backed planner.
    ///
    /// Exists solely to break the construction-time dependency cycle between
    /// `SessionContext` and `FFI_QueryPlanner`. The inner planner starts as `None` and
    /// must be set via `set_planner` before any query is executed.
    #[derive(Debug, Default)]
    struct DynamicForeignQueryPlanner {
        inner: Arc<RwLock<Option<Arc<dyn QueryPlannerWeak>>>>,
    }

    impl DynamicForeignQueryPlanner {
        /// Installs `ffi_planner` as the active planner. Converts the FFI type to the
        /// `QueryPlannerWeak` trait object expected by `create_physical_plan`.
        pub fn set_planner(&self, ffi_planner: &FFI_QueryPlanner) {
            let foreign_planner: Arc<dyn QueryPlannerWeak> = (ffi_planner).into();
            let mut inner = self.inner.write().unwrap();
            *inner = Some(foreign_planner);
        }
    }

    #[async_trait::async_trait]
    impl QueryPlanner for DynamicForeignQueryPlanner {
        async fn create_physical_plan(
            &self,
            logical_plan: &LogicalPlan,
            session_state: &SessionState,
        ) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
            let planner = { self.inner.read().unwrap().clone() };

            match planner {
                Some(planner) => {
                    planner
                        .create_physical_plan(logical_plan, session_state)
                        .await
                }
                // we could use default planner here instead of panicking
                None => panic!("planner should be set before executing queries"),
            }
        }
    }
}
