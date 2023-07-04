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

//! Execution plan for streaming [`PartitionStream`]

use std::any::Any;
use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use futures::stream::StreamExt;

use datafusion_common::{DataFusionError, Result, Statistics};
use datafusion_physical_expr::{LexOrdering, PhysicalSortExpr};
use log::debug;

use crate::datasource::physical_plan::{OutputOrderingDisplay, ProjectSchemaDisplay};
use crate::physical_plan::stream::RecordBatchStreamAdapter;
use crate::physical_plan::{
    DisplayFormatType, ExecutionPlan, Partitioning, SendableRecordBatchStream,
};
use datafusion_execution::TaskContext;

/// A partition that can be converted into a [`SendableRecordBatchStream`]
pub trait PartitionStream: Send + Sync {
    /// Returns the schema of this partition
    fn schema(&self) -> &SchemaRef;

    /// Returns a stream yielding this partitions values
    fn execute(&self, ctx: Arc<TaskContext>) -> SendableRecordBatchStream;
}

/// An [`ExecutionPlan`] for [`PartitionStream`]
pub struct StreamingTableExec {
    partitions: Vec<Arc<dyn PartitionStream>>,
    projection: Option<Arc<[usize]>>,
    projected_schema: SchemaRef,
    projected_output_ordering: Option<LexOrdering>,
    infinite: bool,
}

impl StreamingTableExec {
    /// Try to create a new [`StreamingTableExec`] returning an error if the schema is incorrect
    pub fn try_new(
        schema: SchemaRef,
        partitions: Vec<Arc<dyn PartitionStream>>,
        projection: Option<&Vec<usize>>,
        projected_output_ordering: Option<LexOrdering>,
        infinite: bool,
    ) -> Result<Self> {
        for x in partitions.iter() {
            let partition_schema = x.schema();
            if !schema.contains(partition_schema) {
                debug!(
                    "target schema does not contain partition schema. \
                        Target_schema: {schema:?}. Partiton Schema: {partition_schema:?}"
                );
                return Err(DataFusionError::Plan(
                    "Mismatch between schema and batches".to_string(),
                ));
            }
        }

        let projected_schema = match projection {
            Some(p) => Arc::new(schema.project(p)?),
            None => schema,
        };

        Ok(Self {
            partitions,
            projected_schema,
            projection: projection.cloned().map(Into::into),
            projected_output_ordering,
            infinite,
        })
    }
}

impl std::fmt::Debug for StreamingTableExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LazyMemTableExec").finish_non_exhaustive()
    }
}

#[async_trait]
impl ExecutionPlan for StreamingTableExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.projected_schema.clone()
    }

    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(self.partitions.len())
    }

    fn unbounded_output(&self, _children: &[bool]) -> Result<bool> {
        Ok(self.infinite)
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        if let Some(ordering) = &self.projected_output_ordering {
            Some(ordering)
        } else {
            None
        }
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Err(DataFusionError::Internal(format!(
            "Children cannot be replaced in {self:?}"
        )))
    }

    fn execute(
        &self,
        partition: usize,
        ctx: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let stream = self.partitions[partition].execute(ctx);
        Ok(match self.projection.clone() {
            Some(projection) => Box::pin(RecordBatchStreamAdapter::new(
                self.projected_schema.clone(),
                stream.map(move |x| {
                    x.and_then(|b| b.project(projection.as_ref()).map_err(Into::into))
                }),
            )),
            None => stream,
        })
    }

    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(
                    f,
                    "StreamingTableExec: partition_sizes={:?}",
                    self.partitions.len(),
                )?;
                if !self.projected_schema.fields().is_empty() {
                    write!(
                        f,
                        ", projection={}",
                        ProjectSchemaDisplay(&self.projected_schema)
                    )?;
                }
                if self.infinite {
                    write!(f, ", infinite_source=true")?;
                }
                if let Some(ordering) = &self.projected_output_ordering {
                    if !ordering.is_empty() {
                        write!(
                            f,
                            ", output_ordering={}",
                            OutputOrderingDisplay(ordering)
                        )?;
                    }
                }
                Ok(())
            }
        }
    }

    fn statistics(&self) -> Statistics {
        Default::default()
    }
}
