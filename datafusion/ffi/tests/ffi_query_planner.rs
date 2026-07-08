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
    use std::sync::Arc;

    use datafusion_common::DataFusionError;
    use datafusion_expr::LogicalPlanBuilder;
    use datafusion_ffi::query_planner::ForeignQueryPlanner;
    use datafusion_ffi::tests::utils::get_module;
    use datafusion_session::QueryPlanner;

    #[tokio::test]
    async fn test_ffi_query_planner() -> Result<(), DataFusionError> {
        let module = get_module()?;
        let (ctx, codec) = crate::utils::ctx_and_codec();

        let ffi_planner = (module.create_query_planner)(codec);
        let planner: Arc<dyn QueryPlanner + Send + Sync> = (&ffi_planner).into();

        let any_ref: &dyn std::any::Any = planner.as_ref();
        assert!(any_ref.downcast_ref::<ForeignQueryPlanner>().is_some());

        let logical_plan = LogicalPlanBuilder::empty(false).build()?;
        let state = ctx.state();
        let physical_plan = planner.create_physical_plan(&logical_plan, &state).await?;

        assert_eq!(physical_plan.name(), "EmptyExec");

        Ok(())
    }
}
