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

use std::sync::Arc;

use datafusion_expr::LogicalPlan;
use datafusion_physical_plan::{ExecutionPlan, empty::EmptyExec};
use datafusion_session::Session;

use crate::{
    proto::logical_extension_codec::FFI_LogicalExtensionCodec,
    session::planner::{FFI_QueryPlanner, QueryPlannerWeak},
};

#[derive(Debug, Default)]
struct MockPlanner {}

#[async_trait::async_trait]
impl QueryPlannerWeak for MockPlanner {
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        _session_state: &dyn Session,
    ) -> datafusion_common::Result<Arc<dyn ExecutionPlan>> {
        let schema = logical_plan.schema().as_arrow().clone();
        Ok(Arc::new(EmptyExec::new(Arc::new(schema))))
    }
}

pub(crate) extern "C" fn create_query_planner(
    codec: FFI_LogicalExtensionCodec,
) -> FFI_QueryPlanner {
    let planner: Arc<dyn QueryPlannerWeak> = Arc::new(MockPlanner::default());
    FFI_QueryPlanner::new_with_ffi_codec(planner, codec)
}
