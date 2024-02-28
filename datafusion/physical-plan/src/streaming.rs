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

//! Generic plans for deferred execution: [`StreamingTableExec`] and [`PartitionStream`]

use std::any::Any;
use std::sync::Arc;

use super::{DisplayAs, DisplayFormatType, ExecutionMode, PlanProperties};
use crate::display::{display_orderings, ProjectSchemaDisplay};
use crate::stream::RecordBatchStreamAdapter;
use crate::{ExecutionPlan, Partitioning, SendableRecordBatchStream};

use arrow::datatypes::SchemaRef;
use arrow_schema::Schema;
use datafusion_common::{internal_err, plan_err, Result};
use datafusion_execution::TaskContext;
use datafusion_physical_expr::{EquivalenceProperties, LexOrdering};

use async_trait::async_trait;
use futures::stream::StreamExt;
use log::debug;

/// A partition that can be converted into a [`SendableRecordBatchStream`]
///
/// Combined with [`StreamingTableExec`], you can use this trait to implement
/// [`ExecutionPlan`] for a custom source with less boiler plate than
/// implementing `ExecutionPlan` directly for many use cases.
pub trait PartitionStream: Send + Sync {
    /// Returns the schema of this partition
    fn schema(&self) -> &SchemaRef;

    /// Returns a stream yielding this partitions values
    fn execute(&self, ctx: Arc<TaskContext>) -> SendableRecordBatchStream;
}

/// An [`ExecutionPlan`] for one or more [`PartitionStream`]s.
///
/// If your source can be represented as one or more [`PartitionStream`]s, you can
/// use this struct to implement [`ExecutionPlan`].
pub struct StreamingTableExec {
    partitions: Vec<Arc<dyn PartitionStream>>,
    projection: Option<Arc<[usize]>>,
    projected_schema: SchemaRef,
    projected_output_ordering: Vec<LexOrdering>,
    infinite: bool,
    cache: PlanProperties,
}

impl StreamingTableExec {
    /// Try to create a new [`StreamingTableExec`] returning an error if the schema is incorrect
    pub fn try_new(
        schema: SchemaRef,
        partitions: Vec<Arc<dyn PartitionStream>>,
        projection: Option<&Vec<usize>>,
        projected_output_ordering: impl IntoIterator<Item = LexOrdering>,
        infinite: bool,
    ) -> Result<Self> {
        for x in partitions.iter() {
            let partition_schema = x.schema();
            if !schema.eq(partition_schema) {
                debug!(
                    "Target schema does not match with partition schema. \
                        Target_schema: {schema:?}. Partiton Schema: {partition_schema:?}"
                );
                return plan_err!("Mismatch between schema and batches");
            }
        }

        let projected_schema = match projection {
            Some(p) => Arc::new(schema.project(p)?),
            None => schema,
        };
        let projected_output_ordering =
            projected_output_ordering.into_iter().collect::<Vec<_>>();
        let cache = Self::compute_properties(
            projected_schema.clone(),
            &projected_output_ordering,
            &partitions,
            infinite,
        );
        Ok(Self {
            partitions,
            projected_schema,
            projection: projection.cloned().map(Into::into),
            projected_output_ordering,
            infinite,
            cache,
        })
    }

    pub fn partitions(&self) -> &Vec<Arc<dyn PartitionStream>> {
        &self.partitions
    }

    pub fn partition_schema(&self) -> &SchemaRef {
        self.partitions[0].schema()
    }

    pub fn projection(&self) -> &Option<Arc<[usize]>> {
        &self.projection
    }

    pub fn projected_schema(&self) -> &Schema {
        &self.projected_schema
    }

    pub fn projected_output_ordering(&self) -> impl IntoIterator<Item = LexOrdering> {
        self.projected_output_ordering.clone()
    }

    pub fn is_infinite(&self) -> bool {
        self.infinite
    }

    /// This function creates the cache object that stores the plan properties such as schema, equivalence properties, ordering, partitioning, etc.
    fn compute_properties(
        schema: SchemaRef,
        orderings: &[LexOrdering],
        partitions: &[Arc<dyn PartitionStream>],
        is_infinite: bool,
    ) -> PlanProperties {
        // Calculate equivalence properties:
        let eq_properties = EquivalenceProperties::new_with_orderings(schema, orderings);

        // Get output partitioning:
        let output_partitioning = Partitioning::UnknownPartitioning(partitions.len());

        // Determine execution mode:
        let mode = if is_infinite {
            ExecutionMode::Unbounded
        } else {
            ExecutionMode::Bounded
        };

        PlanProperties::new(eq_properties, output_partitioning, mode)
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

                display_orderings(f, &self.projected_output_ordering)?;

                Ok(())
            }
        }
    }
}

#[async_trait]
impl ExecutionPlan for StreamingTableExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.cache
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
}
