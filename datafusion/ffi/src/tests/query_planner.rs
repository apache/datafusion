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

use std::any::Any;
use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use datafusion_catalog::TableProvider;
use datafusion_catalog::default_table_source::source_as_provider;
use datafusion_common::{Result, exec_err};
use datafusion_expr::{Expr, LogicalPlan, LogicalPlanBuilder, TableType};
use datafusion_physical_plan::ExecutionPlan;
use datafusion_physical_plan::empty::EmptyExec;
use datafusion_physical_plan::sorts::sort::SortExec;
use datafusion_physical_plan::union::UnionExec;
use datafusion_session::{QueryPlanner, Session};

use crate::execution_plan::ForeignExecutionPlan;
use crate::execution_plan::tests::EmptyExec as TestExtensionExec;
use crate::proto::extension_codec_bundle::FFI_ExtensionCodecBundle;
use crate::query_planner::{FFI_QueryPlanner, ForeignQueryPlanner};
use crate::session::ForeignSession;
use crate::table_provider::{FFI_TableProvider, ForeignTableProvider};
use crate::util::FFI_Option;

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
        let planner_any: &dyn Any = query_planner.as_ref();
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

/// Library C's planner for the planner-swap deployment.
///
/// It holds the query planner library A exported *before* A swapped this planner
/// into its session, so delegating to it cannot re-enter library C.
#[derive(Debug)]
struct SwappedQueryPlanner {
    library_a_planner: Arc<dyn QueryPlanner + Send + Sync>,
}

#[async_trait]
impl QueryPlanner for SwappedQueryPlanner {
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        session: &dyn Session,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if session.as_any().downcast_ref::<ForeignSession>().is_none() {
            return exec_err!("library A's session was not foreign to library C");
        }

        // After the swap, the planner installed on library A's session is this
        // planner, so `session.query_planner()` and `session.create_physical_plan()`
        // are both self-references. Assert the hazard instead of triggering it:
        // calling either would recurse until the stack is exhausted.
        let installed = session.query_planner();
        let installed: &dyn Any = installed.as_ref();
        if installed.downcast_ref::<Self>().is_none() {
            return exec_err!(
                "expected the swapped session to report library C's own planner"
            );
        }

        // Delegate to library A. The result crosses the FFI boundary as
        // serialized bytes, so library C receives nodes carrying its own local
        // Rust type identities.
        let plan = self
            .library_a_planner
            .create_physical_plan(logical_plan, session)
            .await?;

        if plan.is::<ForeignExecutionPlan>() {
            return exec_err!("library A's plan was opaque to library C");
        }
        let Some(sort) = plan.downcast_ref::<SortExec>() else {
            return exec_err!(
                "library C could not downcast library A's SortExec; got {}",
                plan.name()
            );
        };
        // Library B's scan is still foreign to library C. Only a codec boundary
        // reconstructs it, and library A's codec hands back an A-local node.
        if !sort.input().is::<ForeignExecutionPlan>() {
            return exec_err!("library B's scan unexpectedly downcast as C-local");
        }

        Ok(UnionExec::try_new(vec![
            Arc::clone(&plan),
            Arc::clone(&plan),
        ])?)
    }
}

/// Library C's planner for the extension-node deployment.
///
/// It returns a node that has no built-in protobuf representation, so the node can
/// only reach another library through a physical extension codec.
#[derive(Debug)]
struct ExtensionNodeQueryPlanner;

#[async_trait]
impl QueryPlanner for ExtensionNodeQueryPlanner {
    async fn create_physical_plan(
        &self,
        _logical_plan: &LogicalPlan,
        _session: &dyn Session,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::new(
            TestExtensionExec::new(super::create_test_schema()),
        ))
    }
}

/// Library B's table provider for the extension-node deployment.
///
/// Instead of planning its own scan, it plans through the query planner reachable
/// on the session library A handed it. That planner is library C's, and the node it
/// returns must travel back through library A's physical codec.
#[derive(Debug)]
struct SessionPlanningTableProvider;

#[async_trait]
impl TableProvider for SessionPlanningTableProvider {
    fn schema(&self) -> SchemaRef {
        super::create_test_schema()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        session: &dyn Session,
        _projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if session.as_any().downcast_ref::<ForeignSession>().is_none() {
            return exec_err!("library A's session was not foreign to library B");
        }

        let planner = session.query_planner();
        let planner_any: &dyn Any = planner.as_ref();
        if planner_any.downcast_ref::<ForeignQueryPlanner>().is_none() {
            return exec_err!("library C's planner did not cross the FFI boundary");
        }

        // The planner ignores the plan; it exists so the call has an argument that
        // library A's logical codec can serialize.
        let logical_plan = LogicalPlanBuilder::empty(false).build()?;
        let plan = planner.create_physical_plan(&logical_plan, session).await?;

        // The node was reconstructed by library A's physical codec, so it is
        // A-local and therefore foreign here.
        if !plan.is::<ForeignExecutionPlan>() {
            return exec_err!(
                "expected library A to reconstruct the extension node; got {}",
                plan.name()
            );
        }

        Ok(plan)
    }
}

/// Creates library C's query planner.
///
/// `library_a_planner` is the planner library A exported before swapping this one
/// onto its session. When it is absent the planner does its own planning instead
/// of delegating.
pub extern "C" fn create_query_planner(
    codecs: FFI_ExtensionCodecBundle,
    library_a_planner: FFI_Option<FFI_QueryPlanner>,
) -> FFI_QueryPlanner {
    let planner: Arc<dyn QueryPlanner + Send + Sync> = match library_a_planner.as_ref() {
        Some(library_a_planner) => Arc::new(SwappedQueryPlanner {
            library_a_planner: library_a_planner.into(),
        }),
        None => Arc::new(TestQueryPlanner),
    };

    FFI_QueryPlanner::new(planner, codecs)
}

/// Creates library C's planner for the extension-node deployment.
pub extern "C" fn create_extension_node_query_planner(
    codecs: FFI_ExtensionCodecBundle,
) -> FFI_QueryPlanner {
    FFI_QueryPlanner::new(Arc::new(ExtensionNodeQueryPlanner), codecs)
}

/// Creates library B's table provider for the extension-node deployment.
pub extern "C" fn create_session_planning_table(
    codecs: FFI_ExtensionCodecBundle,
) -> FFI_TableProvider {
    FFI_TableProvider::new(Arc::new(SessionPlanningTableProvider), false, None, codecs)
}
