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

//! Execution plan for reading in-memory batches of data

use super::expressions::PhysicalSortExpr;
use super::{
    common, project_schema, DisplayFormatType, ExecutionPlan, Partitioning,
    RecordBatchStream, SendableRecordBatchStream, Statistics,
};
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use core::fmt;
use datafusion_common::Result;
use std::any::Any;
use std::sync::Arc;
use std::task::{Context, Poll};

use datafusion_common::DataFusionError;
use datafusion_execution::TaskContext;
use futures::Stream;

/// Execution plan for reading in-memory batches of data
pub struct MemoryExec {
    /// The partitions to query
    partitions: Vec<Vec<RecordBatch>>,
    /// Schema representing the data before projection
    schema: SchemaRef,
    /// Schema representing the data after the optional projection is applied
    projected_schema: SchemaRef,
    /// Optional projection
    projection: Option<Vec<usize>>,
    // Optional sort information
    sort_information: Option<Vec<PhysicalSortExpr>>,
}

impl fmt::Debug for MemoryExec {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "partitions: [...]")?;
        write!(f, "schema: {:?}", self.projected_schema)?;
        write!(f, "projection: {:?}", self.projection)
    }
}

impl ExecutionPlan for MemoryExec {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    /// Get the schema for this execution plan
    fn schema(&self) -> SchemaRef {
        self.projected_schema.clone()
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        // this is a leaf node and has no children
        vec![]
    }

    /// Get the output partitioning of this plan
    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(self.partitions.len())
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        self.sort_information.as_deref()
    }

    fn with_new_children(
        self: Arc<Self>,
        _: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Err(DataFusionError::Internal(format!(
            "Children cannot be replaced in {self:?}"
        )))
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        Ok(Box::pin(MemoryStream::try_new(
            self.partitions[partition].clone(),
            self.projected_schema.clone(),
            self.projection.clone(),
        )?))
    }

    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                let partitions: Vec<_> =
                    self.partitions.iter().map(|b| b.len()).collect();
                write!(
                    f,
                    "MemoryExec: partitions={}, partition_sizes={:?}",
                    partitions.len(),
                    partitions
                )
            }
        }
    }

    /// We recompute the statistics dynamically from the arrow metadata as it is pretty cheap to do so
    fn statistics(&self) -> Statistics {
        common::compute_record_batch_statistics(
            &self.partitions,
            &self.schema,
            self.projection.clone(),
        )
    }
}

impl MemoryExec {
    /// Create a new execution plan for reading in-memory record batches
    /// The provided `schema` should not have the projection applied.
    pub fn try_new(
        partitions: &[Vec<RecordBatch>],
        schema: SchemaRef,
        projection: Option<Vec<usize>>,
    ) -> Result<Self> {
        let projected_schema = project_schema(&schema, projection.as_ref())?;
        Ok(Self {
            partitions: partitions.to_vec(),
            schema,
            projected_schema,
            projection,
            sort_information: None,
        })
    }

    /// Set sort information
    pub fn with_sort_information(
        mut self,
        sort_information: Vec<PhysicalSortExpr>,
    ) -> Self {
        self.sort_information = Some(sort_information);
        self
    }
}

/// Iterator over batches
pub struct MemoryStream {
    /// Vector of record batches
    data: Vec<RecordBatch>,
    /// Schema representing the data
    schema: SchemaRef,
    /// Optional projection for which columns to load
    projection: Option<Vec<usize>>,
    /// Index into the data
    index: usize,
}

impl MemoryStream {
    /// Create an iterator for a vector of record batches
    pub fn try_new(
        data: Vec<RecordBatch>,
        schema: SchemaRef,
        projection: Option<Vec<usize>>,
    ) -> Result<Self> {
        Ok(Self {
            data,
            schema,
            projection,
            index: 0,
        })
    }
}

impl Stream for MemoryStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        Poll::Ready(if self.index < self.data.len() {
            self.index += 1;
            let batch = &self.data[self.index - 1];

            // return just the columns requested
            let batch = match self.projection.as_ref() {
                Some(columns) => batch.project(columns)?,
                None => batch.clone(),
            };

            Some(Ok(batch))
        } else {
            None
        })
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.data.len(), Some(self.data.len()))
    }
}

impl RecordBatchStream for MemoryStream {
    /// Get the schema
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
