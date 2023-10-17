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

use super::{DisplayAs, DisplayFormatType};
use crate::display::{OutputOrderingDisplay, ProjectSchemaDisplay};
use crate::stream::RecordBatchStreamAdapter;
use crate::{ExecutionPlan, Partitioning, SendableRecordBatchStream};

use arrow::datatypes::SchemaRef;
use datafusion_common::{internal_err, plan_err, DataFusionError, Result, Statistics};
use datafusion_execution::TaskContext;
use datafusion_physical_expr::{LexOrdering, PhysicalSortExpr};

use async_trait::async_trait;
use futures::stream::StreamExt;
use log::debug;

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
                return plan_err!("Mismatch between schema and batches");
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

impl DisplayAs for StreamingTableExec {
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

                self.projected_output_ordering
                    .as_deref()
                    .map_or(Ok(()), |ordering| {
                        if !ordering.is_empty() {
                            write!(
                                f,
                                ", output_ordering={}",
                                OutputOrderingDisplay(ordering)
                            )?;
                        }
                        Ok(())
                    })
            }
        }
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
        self.projected_output_ordering.as_deref()
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.is_empty() {
            Ok(self)
        } else {
            internal_err!("Children cannot be replaced in {self:?}")
        }
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

    fn statistics(&self) -> Result<Statistics> {
        Ok(Statistics::new_unknown(&self.schema()))
    }
}
