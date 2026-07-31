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

use arrow::datatypes::{DataType, Field, Schema};
use async_trait::async_trait;
use datafusion_catalog::default_table_source::source_as_provider;
use datafusion_common::{Result, exec_err};
use datafusion_expr::LogicalPlan;
use datafusion_physical_plan::ExecutionPlan;
use datafusion_physical_plan::empty::EmptyExec;
use datafusion_physical_plan::union::UnionExec;
use datafusion_session::{QueryPlanner, Session};

use crate::execution_plan::ForeignExecutionPlan;
use crate::proto::logical_extension_codec::FFI_LogicalExtensionCodec;
use crate::proto::physical_extension_codec::FFI_PhysicalExtensionCodec;
use crate::query_planner::{FFI_QueryPlanner, ForeignQueryPlanner};
use crate::session::ForeignSession;
use crate::table_provider::ForeignTableProvider;

#[derive(Debug)]
struct TestQueryPlanner;

#[async_trait]
impl QueryPlanner for TestQueryPlanner {
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        session: &dyn Session,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if let LogicalPlan::TableScan(scan) = logical_plan {
            if session.as_any().downcast_ref::<ForeignSession>().is_none() {
                return exec_err!("library A's session was not foreign to library C");
            }

            let provider = source_as_provider(&scan.source)?;
            if provider.downcast_ref::<ForeignTableProvider>().is_none() {
                return exec_err!("library B's provider was not foreign to library C");
            }
            let library_b_plan = provider
                .scan(session, scan.projection.as_ref(), &scan.filters, scan.fetch)
                .await?;

            if !library_b_plan.is::<ForeignExecutionPlan>() {
                return exec_err!("library B's plan unexpectedly downcast as C-local");
            }

            let plan = UnionExec::try_new(vec![
                Arc::clone(&library_b_plan),
                Arc::clone(&library_b_plan),
            ])?;
            if !plan.is::<UnionExec>() {
                return exec_err!("library C could not downcast its local UnionExec");
            }
            return Ok(plan);
        }

        let query_planner = session.query_planner();
        let planner_any: &dyn std::any::Any = query_planner.as_ref();
        if planner_any.downcast_ref::<ForeignQueryPlanner>().is_none() {
            return exec_err!("query planner did not cross the FFI boundary");
        }
        session.optimize(logical_plan)?;
        if session.physical_optimizers().is_empty() {
            return exec_err!("physical optimizers did not cross the FFI boundary");
        }

        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, true)]));
        Ok(Arc::new(EmptyExec::new(schema)))
    }
}

pub extern "C" fn create_query_planner(
    logical_codec: FFI_LogicalExtensionCodec,
    physical_codec: FFI_PhysicalExtensionCodec,
) -> FFI_QueryPlanner {
    FFI_QueryPlanner::new_with_ffi_codecs(
        Arc::new(TestQueryPlanner),
        logical_codec,
        physical_codec,
    )
}
