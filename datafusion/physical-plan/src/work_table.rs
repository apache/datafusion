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

//! Defines the work table query plan

use std::any::Any;
use std::sync::{Arc, Mutex};

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion_execution::TaskContext;
use datafusion_physical_expr::Partitioning;

use crate::memory::MemoryStream;
use crate::{DisplayAs, DisplayFormatType, ExecutionPlan};

use super::expressions::PhysicalSortExpr;

use super::{
    metrics::{ExecutionPlanMetricsSet, MetricsSet},
    SendableRecordBatchStream, Statistics,
};
use datafusion_common::{DataFusionError, Result};

/// The name is from PostgreSQL's terminology.
/// See <https://wiki.postgresql.org/wiki/CTEReadme#How_Recursion_Works>
/// This table serves as a mirror or buffer between each iteration of a recursive query.
#[derive(Debug)]
pub(super) struct WorkTable {
    batches: Mutex<Option<Vec<RecordBatch>>>,
}

impl WorkTable {
    /// Create a new work table.
    pub(super) fn new() -> Self {
        Self {
            batches: Mutex::new(None),
        }
    }

    /// Take the previously written batches from the work table.
    /// This will be called by the [`WorkTableExec`] when it is executed.
    fn take(&self) -> Vec<RecordBatch> {
        let batches = self.batches.lock().unwrap().take().unwrap_or_default();
        batches
    }

    /// Write the results of a recursive query iteration to the work table.
    pub(super) fn write(&self, input: Vec<RecordBatch>) {
        self.batches.lock().unwrap().replace(input);
    }
}

/// A temporary "working table" operation where the input data will be
/// taken from the named handle during the execution and will be re-published
/// as is (kind of like a mirror).
///
/// Most notably used in the implementation of recursive queries where the
/// underlying relation does not exist yet but the data will come as the previous
/// term is evaluated. This table will be used such that the recursive plan
/// will register a receiver in the task context and this plan will use that
/// receiver to get the data and stream it back up so that the batches are available
/// in the next iteration.
#[derive(Clone, Debug)]
pub struct WorkTableExec {
    /// Name of the relation handler
    name: String,
    /// The schema of the stream
    schema: SchemaRef,
    /// The work table
    work_table: Arc<WorkTable>,
    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,
}

impl WorkTableExec {
    /// Create a new execution plan for a worktable exec.
    pub fn new(name: String, schema: SchemaRef) -> Self {
        Self {
            name,
            schema,
            metrics: ExecutionPlanMetricsSet::new(),
            work_table: Arc::new(WorkTable::new()),
        }
    }

    pub(super) fn with_work_table(&self, work_table: Arc<WorkTable>) -> Self {
        Self {
            name: self.name.clone(),
            schema: self.schema.clone(),
            metrics: ExecutionPlanMetricsSet::new(),
            work_table,
        }
    }
}

impl DisplayAs for WorkTableExec {
    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "WorkTableExec: name={}", self.name)
            }
        }
    }
}

impl ExecutionPlan for WorkTableExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn output_partitioning(&self) -> Partitioning {
        Partitioning::UnknownPartitioning(1)
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![false]
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false]
    }

    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]> {
        None
    }

    fn with_new_children(
        self: Arc<Self>,
        _: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(self.clone())
    }

    /// Stream the batches that were written to the work table.
    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        // WorkTable streams must be the plan base.
        if partition != 0 {
            return Err(DataFusionError::Internal(format!(
                "WorkTableExec got an invalid partition {} (expected 0)",
                partition
            )));
        }

        let batches = self.work_table.take();
        Ok(Box::pin(MemoryStream::try_new(
            batches,
            self.schema.clone(),
            None,
        )?))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn statistics(&self) -> Result<Statistics> {
        Ok(Statistics::new_unknown(&self.schema()))
    }
}

#[cfg(test)]
mod tests {}
