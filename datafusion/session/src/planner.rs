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

use crate::Session;
use async_trait::async_trait;
use datafusion_expr::LogicalPlan;
use datafusion_physical_plan::ExecutionPlan;
use std::fmt::Debug;
use std::sync::Arc;

/// A planner used to add extensions to DataFusion logical and physical plans.
#[async_trait]
pub trait QueryPlanner: Debug + Send + Sync {
    /// Given a [`LogicalPlan`], create an [`ExecutionPlan`] suitable for execution
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        session_state: &dyn Session,
    ) -> datafusion_common::Result<Arc<dyn ExecutionPlan>>;
}
