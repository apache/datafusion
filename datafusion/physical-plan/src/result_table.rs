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

//! Defines the result table query plan

use std::any::Any;
use std::fmt;
use std::sync::{Arc, Mutex};

use crate::coop::cooperative;
use crate::execution_plan::{Boundedness, EmissionType, SchedulingType};
use crate::memory::MemoryStream;
use crate::metrics::{ExecutionPlanMetricsSet, MetricsSet};
use crate::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties,
    SendableRecordBatchStream, Statistics,
};

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion_common::{internal_err, Result};
use datafusion_execution::memory_pool::MemoryReservation;
use datafusion_execution::TaskContext;
use datafusion_physical_expr::{EquivalenceProperties, Partitioning};

/// The name is from PostgreSQL's terminology.
/// See <https://wiki.postgresql.org/wiki/CTEReadme#How_Recursion_Works>
/// This table serves as a mirror or buffer, allowing to add or read data.
#[derive(Debug)]
pub struct ResultTable {
    inner: Mutex<ResultTableInner>,
}

#[derive(Debug)]
struct ResultTableInner {
    batches: Vec<RecordBatch>,
    reservation: Option<MemoryReservation>,
}

impl ResultTable {
    /// Create a new result table.
    pub(super) fn new() -> Self {
        Self {
            inner: Mutex::new(ResultTableInner {
                batches: Vec::new(),
                reservation: None,
            }),
        }
    }

    /// Return the content of the result table.
    /// This will be called by the [`ResultTableExec`] when it is executed.
    fn get(&self) -> Vec<RecordBatch> {
        self.inner.lock().unwrap().batches.clone()
    }

    /// Add extra data to the table
    pub(super) fn append(
        &self,
        batches: Vec<RecordBatch>,
        reservation: MemoryReservation,
    ) {
        let mut guard = self.inner.lock().unwrap();
        if let Some(r) = &mut guard.reservation {
            r.grow(reservation.size());
        } else {
            guard.reservation = Some(reservation);
        }
        guard.batches.extend(batches);
    }
}

/// A temporary "result table" operation where the input data will be
/// taken from the named handle during the execution and will be re-published
/// as is (kind of like a mirror).
///
/// It is used in deduplicating recursive queries to store previously emitted results and avoid
/// considering them again in future iterations.
///
/// This is key to avoiding infinite loops in transitive closures on arbitrary graphs.
#[derive(Clone, Debug)]
pub struct ResultTableExec {
    /// Name of the relation handler
    name: String,
    /// The schema of the stream
    schema: SchemaRef,
    /// The result table
    result_table: Arc<ResultTable>,
    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,
    /// Cache holding plan properties like equivalences, output partitioning etc.
    cache: PlanProperties,
}

impl ResultTableExec {
    /// Create a new execution plan for a result table exec.
    pub fn new(name: String, schema: SchemaRef) -> Self {
        let cache = Self::compute_properties(Arc::clone(&schema));
        Self {
            name,
            schema,
            metrics: ExecutionPlanMetricsSet::new(),
            result_table: Arc::new(ResultTable::new()),
            cache,
        }
    }

    /// Arc clone of ref to schema
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    /// This function creates the cache object that stores the plan properties such as schema, equivalence properties, ordering, partitioning, etc.
    fn compute_properties(schema: SchemaRef) -> PlanProperties {
        PlanProperties::new(
            EquivalenceProperties::new(schema),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        )
        .with_scheduling_type(SchedulingType::Cooperative)
    }
}

impl DisplayAs for ResultTableExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "ResultTableExec: name={}", self.name)
            }
            DisplayFormatType::TreeRender => {
                write!(f, "name={}", self.name)
            }
        }
    }
}

impl ExecutionPlan for ResultTableExec {
    fn name(&self) -> &'static str {
        "ResultTableExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        Vec::new()
    }

    fn with_new_children(
        self: Arc<Self>,
        _: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        Ok(Arc::clone(&self) as _)
    }

    /// Stream the batches that were written to the result table.
    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        // ResultTable streams must be the plan base.
        if partition != 0 {
            return internal_err!(
                "ResultTableExec got an invalid partition {partition} (expected 0)"
            );
        }
        Ok(Box::pin(cooperative(MemoryStream::try_new(
            self.result_table.get(),
            Arc::clone(&self.schema),
            None,
        )?)))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn statistics(&self) -> Result<Statistics> {
        Ok(Statistics::new_unknown(&self.schema()))
    }

    fn partition_statistics(&self, _partition: Option<usize>) -> Result<Statistics> {
        Ok(Statistics::new_unknown(&self.schema()))
    }

    /// Injects run-time state into this `ResultTableExec`.
    ///
    /// The only state this node currently understands is an [`Arc<ResultTable>`].
    /// If `state` can be down-cast to that type, a new `ResultTableExec` backed
    /// by the provided work table is returned.  Otherwise `None` is returned
    /// so that callers can attempt to propagate the state further down the
    /// execution plan tree.
    fn with_new_state(
        &self,
        state: Arc<dyn Any + Send + Sync>,
    ) -> Option<Arc<dyn ExecutionPlan>> {
        // Down-cast to the expected state type; propagate `None` on failure
        let work_table = state.downcast::<ResultTable>().ok()?;

        Some(Arc::new(Self {
            name: self.name.clone(),
            schema: Arc::clone(&self.schema),
            metrics: ExecutionPlanMetricsSet::new(),
            result_table: work_table,
            cache: self.cache.clone(),
        }))
    }
}
