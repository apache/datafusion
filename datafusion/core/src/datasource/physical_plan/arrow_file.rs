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

//! Execution plan for reading Arrow files

use std::any::Any;
use std::sync::Arc;

use crate::datasource::physical_plan::{
    FileMeta, FileOpenFuture, FileOpener, FileScanConfig,
};
use crate::error::Result;
use crate::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricsSet};
use crate::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, SendableRecordBatchStream,
};

use arrow_schema::SchemaRef;
use datafusion_common::Statistics;
use datafusion_execution::TaskContext;
use datafusion_physical_expr::{
    schema_properties_helper, LexOrdering, PhysicalSortExpr, SchemaProperties,
};

use futures::StreamExt;
use object_store::{GetResultPayload, ObjectStore};

/// Execution plan for scanning Arrow data source
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct ArrowExec {
    base_config: FileScanConfig,
    projected_statistics: Statistics,
    projected_schema: SchemaRef,
    projected_output_ordering: Vec<LexOrdering>,
    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,
}

impl ArrowExec {
    /// Create a new Arrow reader execution plan provided base configurations
    pub fn new(base_config: FileScanConfig) -> Self {
        let (projected_schema, projected_statistics, projected_output_ordering) =
            base_config.project();

        Self {
            base_config,
            projected_schema,
            projected_statistics,
            projected_output_ordering,
            metrics: ExecutionPlanMetricsSet::new(),
        }
    }
    /// Ref to the base configs
    pub fn base_config(&self) -> &FileScanConfig {
        &self.base_config
    }
}

impl DisplayAs for ArrowExec {
    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(f, "ArrowExec: ")?;
        self.base_config.fmt_as(t, f)
    }
}

impl ExecutionPlan for ArrowExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.projected_schema.clone()
    }

    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(self.base_config.file_groups.len())
    }

    fn unbounded_output(&self, _: &[bool]) -> Result<bool> {
        Ok(self.base_config().infinite_source)
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        self.projected_output_ordering
            .first()
            .map(|ordering| ordering.as_slice())
    }

    fn schema_properties(&self) -> SchemaProperties {
        schema_properties_helper(self.schema(), &self.projected_output_ordering)
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        Vec::new()
    }

    fn with_new_children(
        self: Arc<Self>,
        _: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        use super::file_stream::FileStream;
        let object_store = context
            .runtime_env()
            .object_store(&self.base_config.object_store_url)?;

        let opener = ArrowOpener {
            object_store,
            projection: self.base_config.projection.clone(),
        };
        let stream =
            FileStream::new(&self.base_config, partition, opener, &self.metrics)?;
        Ok(Box::pin(stream))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn statistics(&self) -> Result<Statistics> {
        Ok(self.projected_statistics.clone())
    }
}

pub struct ArrowOpener {
    pub object_store: Arc<dyn ObjectStore>,
    pub projection: Option<Vec<usize>>,
}

impl FileOpener for ArrowOpener {
    fn open(&self, file_meta: FileMeta) -> Result<FileOpenFuture> {
        let object_store = self.object_store.clone();
        let projection = self.projection.clone();
        Ok(Box::pin(async move {
            let r = object_store.get(file_meta.location()).await?;
            match r.payload {
                GetResultPayload::File(file, _) => {
                    let arrow_reader =
                        arrow::ipc::reader::FileReader::try_new(file, projection)?;
                    Ok(futures::stream::iter(arrow_reader).boxed())
                }
                GetResultPayload::Stream(_) => {
                    let bytes = r.bytes().await?;
                    let cursor = std::io::Cursor::new(bytes);
                    let arrow_reader =
                        arrow::ipc::reader::FileReader::try_new(cursor, projection)?;
                    Ok(futures::stream::iter(arrow_reader).boxed())
                }
            }
        }))
    }
}
