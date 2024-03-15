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

use super::{
    metrics::{ExecutionPlanMetricsSet, MetricsSet},
    SendableRecordBatchStream, Statistics,
};
use crate::memory::MemoryStream;
use crate::{DisplayAs, DisplayFormatType, ExecutionMode, ExecutionPlan, PlanProperties};

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion_common::{internal_datafusion_err, internal_err, Result};
use datafusion_execution::memory_pool::MemoryReservation;
use datafusion_execution::TaskContext;
use datafusion_physical_expr::{EquivalenceProperties, Partitioning};

/// A vector of record batches with a memory reservation.
#[derive(Debug)]
struct ReservedBatches {
    batches: Vec<RecordBatch>,
    #[allow(dead_code)]
    reservation: MemoryReservation,
}

impl ReservedBatches {
    fn new(batches: Vec<RecordBatch>, reservation: MemoryReservation) -> Self {
        ReservedBatches {
            batches,
            reservation,
        }
    }

    fn take_batches(&mut self) -> Vec<RecordBatch> {
        std::mem::take(&mut self.batches)
    }
}

/// The name is from PostgreSQL's terminology.
/// See <https://wiki.postgresql.org/wiki/CTEReadme#How_Recursion_Works>
/// This table serves as a mirror or buffer between each iteration of a recursive query.
#[derive(Debug)]
pub(super) struct WorkTable {
    batches: Mutex<Option<ReservedBatches>>,
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
    /// The reservation will be freed when the next `update()` is called.
    fn take(&self) -> Result<Vec<RecordBatch>> {
        self.batches
            .lock()
            .unwrap()
            .as_mut()
            .map(|b| b.take_batches())
            .ok_or_else(|| internal_datafusion_err!("Unexpected empty work table"))
    }

    /// Update the results of a recursive query iteration to the work table.
    pub(super) fn update(
        &self,
        batches: Vec<RecordBatch>,
        reservation: MemoryReservation,
    ) {
        let reserved_batches = ReservedBatches::new(batches, reservation);
        self.batches.lock().unwrap().replace(reserved_batches);
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
    /// Cache holding plan properties like equivalences, output partitioning etc.
    cache: PlanProperties,
}

impl WorkTableExec {
    /// Create a new execution plan for a worktable exec.
    pub fn new(name: String, schema: SchemaRef) -> Self {
        let cache = Self::compute_properties(schema.clone());
        Self {
            name,
            schema,
            metrics: ExecutionPlanMetricsSet::new(),
            work_table: Arc::new(WorkTable::new()),
            cache,
        }
    }

    pub(super) fn with_work_table(&self, work_table: Arc<WorkTable>) -> Self {
        Self {
            name: self.name.clone(),
            schema: self.schema.clone(),
            metrics: ExecutionPlanMetricsSet::new(),
            work_table,
            cache: self.cache.clone(),
        }
    }

    /// This function creates the cache object that stores the plan properties such as schema, equivalence properties, ordering, partitioning, etc.
    fn compute_properties(schema: SchemaRef) -> PlanProperties {
        let eq_properties = EquivalenceProperties::new(schema);

        PlanProperties::new(
            eq_properties,
            Partitioning::UnknownPartitioning(1),
            ExecutionMode::Bounded,
        )
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

    fn properties(&self) -> &PlanProperties {
        &self.cache
    }

    fn children(&self) -> Vec<Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![false]
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false]
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
            return internal_err!(
                "WorkTableExec got an invalid partition {partition} (expected 0)"
            );
        }

        let batches = self.work_table.take()?;
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
mod tests {
    use super::*;
    use arrow_array::{ArrayRef, Int32Array, RecordBatch};
    use datafusion_execution::memory_pool::{MemoryConsumer, UnboundedMemoryPool};
    use std::sync::Arc;

    #[test]
    fn test_work_table() {
        let work_table = WorkTable::new();
        // take from empty work_table
        assert!(work_table.take().is_err());

        let pool = Arc::new(UnboundedMemoryPool::default()) as _;
        let reservation = MemoryConsumer::new("test_work_table").register(&pool);

        // update batch to work_table
        let array: ArrayRef = Arc::new((0..5).collect::<Int32Array>());
        let batch1 = RecordBatch::try_from_iter(vec![("col", array)]).unwrap();
        let mut reservation1 = reservation.new_empty();
        reservation1.try_grow(100).unwrap();
        work_table.update(vec![batch1.clone()], reservation1);
        // take from work_table
        assert_eq!(work_table.take().unwrap(), vec![batch1]);
        assert_eq!(pool.reserved(), 100);

        // update another batch to work_table
        let array: ArrayRef = Arc::new((5..10).collect::<Int32Array>());
        let batch2 = RecordBatch::try_from_iter(vec![("col", array)]).unwrap();
        let mut reservation2 = reservation.new_empty();
        reservation2.try_grow(200).unwrap();
        work_table.update(vec![batch2.clone()], reservation2);
        // reservation1 should be freed after update
        assert_eq!(pool.reserved(), 200);
        assert_eq!(work_table.take().unwrap(), vec![batch2]);

        drop(work_table);
        // all reservations should be freed after dropped
        assert_eq!(pool.reserved(), 0);
    }
}
