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

//! A table provider that reproduces the pattern used by table providers backed
//! by remote storage, such as `delta-rs`.
//!
//! The provider builds its *own* object store, registers it on the session it
//! is handed during planning, and returns a plan that looks that store up again
//! at execution time. Planning and execution happen on opposite sides of the
//! FFI boundary, so this exercises the session's runtime environment crossing
//! that boundary intact.
//!
//! The scan also reports the memory pool limit it observes at execution time,
//! letting the same test check that the host's memory limit reaches a foreign
//! plan.

use std::sync::Arc;

use arrow::array::{Int32Array, RecordBatch, UInt64Array};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use datafusion_catalog::{Session, TableProvider};
use datafusion_common::tree_node::TreeNodeRecursion;
use datafusion_common::{Result, exec_datafusion_err, exec_err};
use datafusion_execution::memory_pool::MemoryLimit;
use datafusion_execution::object_store::ObjectStoreUrl;
use datafusion_execution::{SendableRecordBatchStream, TaskContext};
use datafusion_expr::{Expr, TableType};
use datafusion_physical_expr::{EquivalenceProperties, PhysicalExpr};
use datafusion_physical_plan::Partitioning;
use datafusion_physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion_physical_plan::execution_plan::{
    ChildrenPropertiesMode, ReplaceChildrenOptions,
};
use datafusion_physical_plan::stream::RecordBatchStreamAdapter;
use datafusion_physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties,
};
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{ObjectStoreExt, PutPayload};
use url::Url;

use crate::proto::logical_extension_codec::FFI_LogicalExtensionCodec;
use crate::table_provider::FFI_TableProvider;

/// The URL the provider registers its store under.
pub const OBJECT_STORE_URL: &str = "ffitest://ffi-object-store";

/// The object within that store holding the scan's data.
const DATA_PATH: &str = "data.bin";

/// The values the scan produces, encoded little-endian into the object above.
pub const EXPECTED_VALUES: [i32; 5] = [10, 20, 30, 40, 50];

/// Reported by the scan when the memory pool it sees has no finite limit,
/// letting a test distinguish the session's pool from an unbounded default.
pub const UNLIMITED_MEMORY: u64 = u64::MAX;

pub fn object_store_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int32, false),
        Field::new("mem_limit", DataType::UInt64, false),
    ]))
}

#[derive(Debug)]
struct ObjectStoreTableProvider;

#[async_trait]
impl TableProvider for ObjectStoreTableProvider {
    fn schema(&self) -> SchemaRef {
        object_store_schema()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        session: &dyn Session,
        _projection: Option<&[usize]>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Build a store owned by *this* library and populate it.
        let store = InMemory::new();
        let payload: Vec<u8> = EXPECTED_VALUES
            .iter()
            .flat_map(|v| v.to_le_bytes())
            .collect();
        store
            .put(&Path::from(DATA_PATH), PutPayload::from(payload))
            .await
            .map_err(|e| exec_datafusion_err!("Unable to seed the test store: {e}"))?;

        // Register it on the session handed to us during planning. The plan
        // returned below looks it up again at execution time, by which point
        // the session lives on the other side of the boundary.
        let url = Url::parse(OBJECT_STORE_URL)
            .map_err(|e| exec_datafusion_err!("Invalid test store URL: {e}"))?;
        session
            .runtime_env()
            .register_object_store(&url, Arc::new(store));

        Ok(Arc::new(ObjectStoreScanExec::new()))
    }
}

#[derive(Debug)]
struct ObjectStoreScanExec {
    props: Arc<PlanProperties>,
}

impl ObjectStoreScanExec {
    fn new() -> Self {
        Self {
            props: Arc::new(PlanProperties::new(
                EquivalenceProperties::new(object_store_schema()),
                Partitioning::UnknownPartitioning(1),
                EmissionType::Incremental,
                Boundedness::Bounded,
            )),
        }
    }
}

impl DisplayAs for ObjectStoreScanExec {
    fn fmt_as(
        &self,
        _t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(f, "ObjectStoreScanExec")
    }
}

impl ExecutionPlan for ObjectStoreScanExec {
    fn name(&self) -> &'static str {
        "ObjectStoreScanExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.props
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        Vec::new()
    }

    fn replace_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
        _options: ReplaceChildrenOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.replace_children(
            children,
            ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
        )
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition != 0 {
            return exec_err!("ObjectStoreScanExec only has one partition");
        }

        let schema = object_store_schema();
        let stream_schema = Arc::clone(&schema);

        let stream = futures::stream::once(async move {
            let runtime_env = context.runtime_env();

            // The store registered during planning must be reachable here.
            let url = ObjectStoreUrl::parse(OBJECT_STORE_URL)?;
            let store = runtime_env.object_store(url)?;

            let bytes = store
                .get(&Path::from(DATA_PATH))
                .await
                .map_err(|e| exec_datafusion_err!("Unable to read the test store: {e}"))?
                .bytes()
                .await
                .map_err(|e| {
                    exec_datafusion_err!("Unable to collect the test store bytes: {e}")
                })?;

            let values: Vec<i32> = bytes
                .chunks_exact(4)
                .map(|c| i32::from_le_bytes([c[0], c[1], c[2], c[3]]))
                .collect();

            let memory_limit = match runtime_env.memory_pool.memory_limit() {
                MemoryLimit::Finite(limit) => limit as u64,
                MemoryLimit::Infinite | MemoryLimit::Unknown => UNLIMITED_MEMORY,
            };

            RecordBatch::try_new(
                schema,
                vec![
                    Arc::new(Int32Array::from(values)),
                    Arc::new(UInt64Array::from(vec![
                        memory_limit;
                        EXPECTED_VALUES.len()
                    ])),
                ],
            )
            .map_err(Into::into)
        });

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            stream_schema,
            stream,
        )))
    }
}

pub(crate) extern "C" fn create_object_store_table(
    codec: FFI_LogicalExtensionCodec,
) -> FFI_TableProvider {
    FFI_TableProvider::new_with_ffi_codec(
        Arc::new(ObjectStoreTableProvider),
        true,
        None,
        codec,
    )
}
