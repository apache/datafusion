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

//! Defines the recursive query plan

use std::any::Any;
use std::sync::Arc;

use super::work_table::{ReservedBatches, WorkTable};
use crate::aggregates::group_values::{GroupValues, new_group_values};
use crate::aggregates::order::GroupOrdering;
use crate::common::project_plan_to_schema;
use crate::execution_plan::{Boundedness, EmissionType, reset_plan_states};
use crate::metrics::{
    BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet, RecordOutput,
};
use crate::stream::RecordBatchStreamAdapter;
use crate::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties,
    SendableRecordBatchStream,
};
use arrow::array::{BooleanArray, BooleanBuilder};
use arrow::compute::filter_record_batch;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion_common::{
    Result, exec_datafusion_err, internal_datafusion_err, not_impl_err,
};
use datafusion_execution::TaskContext;
use datafusion_execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion_physical_expr::{EquivalenceProperties, Partitioning};

use async_fn_stream::try_fn_stream;
use futures::StreamExt;

/// Recursive query execution plan.
///
/// This plan has two components: a base part (the static term) and
/// a dynamic part (the recursive term). The execution will start from
/// the base, and as long as the previous iteration produced at least
/// a single new row (taking care of the distinction) the recursive
/// part will be continuously executed.
///
/// Before each execution of the dynamic part, the rows from the previous
/// iteration will be available in a "working table" (not a real table,
/// can be only accessed using a continuance operation).
///
/// Note that there won't be any limit or checks applied to detect
/// an infinite recursion, so it is up to the planner to ensure that
/// it won't happen.
#[derive(Debug, Clone)]
pub struct RecursiveQueryExec {
    /// Name of the query handler
    name: String,
    /// The working table of cte
    work_table: Arc<WorkTable>,
    /// The base part (static term)
    static_term: Arc<dyn ExecutionPlan>,
    /// The dynamic part (recursive term)
    recursive_term: Arc<dyn ExecutionPlan>,
    /// Distinction
    is_distinct: bool,
    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,
    /// Cache holding plan properties like equivalences, output partitioning etc.
    cache: Arc<PlanProperties>,
}

impl RecursiveQueryExec {
    /// Create a new RecursiveQueryExec
    pub fn try_new(
        name: String,
        output_schema: SchemaRef,
        static_term: Arc<dyn ExecutionPlan>,
        recursive_term: Arc<dyn ExecutionPlan>,
        is_distinct: bool,
    ) -> Result<Self> {
        // Each recursive query needs its own work table
        let work_table = Arc::new(WorkTable::new(name.clone()));
        // Use the same work table for both the WorkTableExec and the recursive term
        let static_term = project_plan_to_schema(static_term, &output_schema)?;
        let recursive_term = assign_work_table(recursive_term, &work_table)?;
        let recursive_term = project_plan_to_schema(recursive_term, &output_schema)?;
        let cache = Self::compute_properties(output_schema);
        Ok(RecursiveQueryExec {
            name,
            static_term,
            recursive_term,
            is_distinct,
            work_table,
            metrics: ExecutionPlanMetricsSet::new(),
            cache: Arc::new(cache),
        })
    }

    /// Ref to name
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Ref to static term
    pub fn static_term(&self) -> &Arc<dyn ExecutionPlan> {
        &self.static_term
    }

    /// Ref to recursive term
    pub fn recursive_term(&self) -> &Arc<dyn ExecutionPlan> {
        &self.recursive_term
    }

    /// is distinct
    pub fn is_distinct(&self) -> bool {
        self.is_distinct
    }

    /// This function creates the cache object that stores the plan properties such as schema, equivalence properties, ordering, partitioning, etc.
    fn compute_properties(schema: SchemaRef) -> PlanProperties {
        let eq_properties = EquivalenceProperties::new(schema);

        PlanProperties::new(
            eq_properties,
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        )
    }
}

impl ExecutionPlan for RecursiveQueryExec {
    fn name(&self) -> &'static str {
        "RecursiveQueryExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.static_term, &self.recursive_term]
    }

    // TODO: control these hints and see whether we can
    // infer some from the child plans (static/recursive terms).
    fn maintains_input_order(&self) -> Vec<bool> {
        vec![false, false]
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false, false]
    }

    fn required_input_distribution(&self) -> Vec<crate::Distribution> {
        vec![
            crate::Distribution::SinglePartition,
            crate::Distribution::SinglePartition,
        ]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        RecursiveQueryExec::try_new(
            self.name.clone(),
            self.schema(),
            Arc::clone(&children[0]),
            Arc::clone(&children[1]),
            self.is_distinct,
        )
        .map(|e| Arc::new(e) as _)
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        // TODO: we might be able to handle multiple partitions in the future.
        if partition != 0 {
            return Err(internal_datafusion_err!(
                "RecursiveQueryExec got an invalid partition {partition} (expected 0)"
            ));
        }

        let static_stream = self.static_term.execute(partition, Arc::clone(&context))?;
        let baseline_metrics = BaselineMetrics::new(&self.metrics, partition);
        RecursiveQueryStream::build(
            context,
            Arc::clone(&self.work_table),
            Arc::clone(&self.recursive_term),
            static_stream,
            self.is_distinct,
            baseline_metrics,
        )
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}

impl DisplayAs for RecursiveQueryExec {
    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(
                    f,
                    "RecursiveQueryExec: name={}, is_distinct={}",
                    self.name, self.is_distinct
                )
            }
            DisplayFormatType::TreeRender => {
                // TODO: collect info
                write!(f, "")
            }
        }
    }
}

/// The actual logic of the recursive queries happens during the streaming
/// process. A simplified version of the algorithm is the following:
///
/// buffer = []
///
/// while batch := static_stream.next():
///    buffer.push(batch)
///    yield buffer
///
/// while buffer.len() > 0:
///    sender, receiver = Channel()
///    register_continuation(handle_name, receiver)
///    sender.send(buffer.drain())
///    recursive_stream = recursive_term.execute()
///    while batch := recursive_stream.next():
///        buffer.append(batch)
///        yield buffer
struct RecursiveQueryStream {
    /// The context to be used for managing handlers & executing new tasks
    task_context: Arc<TaskContext>,
    /// The working table state, representing the self referencing cte table
    work_table: Arc<WorkTable>,
    /// The dynamic part (recursive term) as is (without being executed)
    recursive_term: Arc<dyn ExecutionPlan>,
    /// In-memory buffer for storing a copy of the current results. Will be
    /// cleared after each iteration.
    buffer: Vec<RecordBatch>,
    /// Tracks the memory used by the buffer
    reservation: MemoryReservation,
    /// If the distinct flag is set, then we use this hash table to remove duplicates from result and work tables
    distinct_deduplicator: Option<DistinctDeduplicator>,
    /// Metrics.
    baseline_metrics: BaselineMetrics,
}

impl RecursiveQueryStream {
    /// Build the recursive-query output as a linear async generator.
    ///
    /// A recursive CTE is inherently iterative: emit the static term, then run
    /// the recursive term repeatedly — each iteration feeding the previous
    /// iteration's rows back through the work table — until an iteration
    /// produces no new rows. Expressed with `try_fn_stream`, that description is
    /// the code (see the algorithm doc above): a `while` over the static stream
    /// followed by a `loop` of recursive iterations, `emitter.emit`-ing every
    /// batch. State that must survive across iterations (buffer, reservation,
    /// dedup, metrics) lives on `self`, captured by the async body — the two
    /// `Option` sub-stream fields and the hand-rolled poll state machine are
    /// gone.
    fn build(
        task_context: Arc<TaskContext>,
        work_table: Arc<WorkTable>,
        recursive_term: Arc<dyn ExecutionPlan>,
        static_stream: SendableRecordBatchStream,
        is_distinct: bool,
        baseline_metrics: BaselineMetrics,
    ) -> Result<SendableRecordBatchStream> {
        let schema = static_stream.schema();
        let reservation =
            MemoryConsumer::new("RecursiveQuery").register(task_context.memory_pool());
        let distinct_deduplicator = is_distinct
            .then(|| DistinctDeduplicator::new(Arc::clone(&schema), &task_context))
            .transpose()?;

        let state = Self {
            task_context,
            work_table,
            recursive_term,
            buffer: vec![],
            reservation,
            distinct_deduplicator,
            baseline_metrics,
        };

        let stream = try_fn_stream(|emitter| async move {
            let mut state = state;
            let mut static_stream = static_stream;

            // Static term: forward and buffer every batch.
            while let Some(batch) = static_stream.next().await {
                emitter.emit(state.push_batch(batch?)?).await;
            }

            // Recursive term: iterate until an iteration produces no new rows.
            loop {
                let buffered_rows: usize =
                    state.buffer.iter().map(|b| b.num_rows()).sum();
                if buffered_rows == 0 {
                    return Ok(());
                }

                // Feed the previous iteration's rows into the work table.
                state.work_table.update(ReservedBatches::new(
                    std::mem::take(&mut state.buffer),
                    state.reservation.take(),
                ));

                // We always (re-)execute partition 0; downstream plans should
                // not expect any partitioning.
                let recursive_plan =
                    reset_plan_states(Arc::clone(&state.recursive_term))?;
                let mut recursive_stream =
                    recursive_plan.execute(0, Arc::clone(&state.task_context))?;
                while let Some(batch) = recursive_stream.next().await {
                    emitter.emit(state.push_batch(batch?)?).await;
                }
            }
        });

        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }

    /// Deduplicate (when `DISTINCT`), account for memory, buffer a copy for the
    /// next iteration's work table, record output metrics, and return the batch.
    fn push_batch(&mut self, mut batch: RecordBatch) -> Result<RecordBatch> {
        if let Some(deduplicator) = &mut self.distinct_deduplicator {
            let _timer_guard = self.baseline_metrics.elapsed_compute().timer();
            batch = deduplicator.deduplicate(&batch)?;
        }

        self.reservation.try_grow(batch.get_array_memory_size())?;
        self.buffer.push(batch.clone());
        (&batch).record_output(&self.baseline_metrics);
        Ok(batch)
    }
}

fn assign_work_table(
    plan: Arc<dyn ExecutionPlan>,
    work_table: &Arc<WorkTable>,
) -> Result<Arc<dyn ExecutionPlan>> {
    let mut work_table_refs = 0;
    plan.transform_down(|plan| {
        if let Some(new_plan) =
            plan.with_new_state(Arc::clone(work_table) as Arc<dyn Any + Send + Sync>)
        {
            if work_table_refs > 0 {
                not_impl_err!(
                    "Multiple recursive references to the same CTE are not supported"
                )
            } else {
                work_table_refs += 1;
                Ok(Transformed::yes(new_plan))
            }
        } else {
            Ok(Transformed::no(plan))
        }
    })
    .data()
}


/// Deduplicator based on a hash table.
struct DistinctDeduplicator {
    /// Grouped rows used for distinct
    group_values: Box<dyn GroupValues>,
    reservation: MemoryReservation,
    intern_output_buffer: Vec<usize>,
}

impl DistinctDeduplicator {
    fn new(schema: SchemaRef, task_context: &TaskContext) -> Result<Self> {
        let group_values = new_group_values(schema, &GroupOrdering::None)?;
        let reservation = MemoryConsumer::new("RecursiveQueryHashTable")
            .register(task_context.memory_pool());
        Ok(Self {
            group_values,
            reservation,
            intern_output_buffer: Vec::new(),
        })
    }

    /// Remove duplicated rows from the given batch, keeping a state between batches.
    ///
    /// We use a hash table to allocate new group ids for the new rows.
    /// [`GroupValues`] allocate increasing group ids.
    /// Hence, if groups (i.e., rows) are new, then they have ids >= length before interning, we keep them.
    /// We also detect duplicates by enforcing that group ids are increasing.
    fn deduplicate(&mut self, batch: &RecordBatch) -> Result<RecordBatch> {
        let size_before = self.group_values.len();
        let additional = batch.num_rows();
        self.intern_output_buffer
            .try_reserve(additional)
            .map_err(|e| {
                exec_datafusion_err!(
                    "failed to reserve {additional} recursive query group ids: {e}"
                )
            })?;
        self.group_values
            .intern(batch.columns(), &mut self.intern_output_buffer)?;
        let mask = new_groups_mask(&self.intern_output_buffer, size_before);
        self.intern_output_buffer.clear();
        // We update the reservation to reflect the new size of the hash table.
        self.reservation.try_resize(self.group_values.size())?;
        Ok(filter_record_batch(batch, &mask)?)
    }
}

/// Return a mask, each element being true if, and only if, the element is greater than all previous elements and greater or equal than the provided max_already_seen_group_id
fn new_groups_mask(
    values: &[usize],
    mut max_already_seen_group_id: usize,
) -> BooleanArray {
    let mut output = BooleanBuilder::with_capacity(values.len());
    for value in values {
        if *value >= max_already_seen_group_id {
            output.append_value(true);
            max_already_seen_group_id = *value + 1; // We want to be increasing
        } else {
            output.append_value(false);
        }
    }
    output.finish()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::empty::EmptyExec;
    use crate::projection::ProjectionExec;

    use arrow::datatypes::{DataType, Field, Schema};

    fn empty_exec(fields: Vec<Field>) -> Arc<dyn ExecutionPlan> {
        Arc::new(EmptyExec::new(Arc::new(Schema::new(fields))))
    }

    #[test]
    fn recursive_query_exec_projects_recursive_term_to_reconciled_schema() -> Result<()> {
        let static_term = empty_exec(vec![Field::new("value", DataType::Int32, false)]);
        let recursive_term =
            empty_exec(vec![Field::new("value + Int32(1)", DataType::Int32, false)]);

        let exec = RecursiveQueryExec::try_new(
            "numbers".to_string(),
            static_term.schema(),
            Arc::clone(&static_term),
            Arc::clone(&recursive_term),
            false,
        )?;

        assert_eq!(exec.schema(), static_term.schema());
        let projection = exec
            .recursive_term()
            .downcast_ref::<ProjectionExec>()
            .expect("recursive term should be aligned with ProjectionExec");
        assert!(Arc::ptr_eq(projection.input(), &recursive_term));
        assert!(!projection.schema().field(0).is_nullable());
        assert_eq!(projection.expr()[0].alias, "value");
        Ok(())
    }

    #[test]
    fn recursive_query_exec_reconciles_nullability() -> Result<()> {
        let static_term = empty_exec(vec![Field::new("value", DataType::Int32, false)]);
        let recursive_term =
            empty_exec(vec![Field::new("value + Int32(1)", DataType::Int32, true)]);
        let output_schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int32,
            true,
        )]));

        let exec = RecursiveQueryExec::try_new(
            "numbers".to_string(),
            Arc::clone(&output_schema),
            static_term,
            recursive_term,
            false,
        )?;

        assert!(exec.schema().field(0).is_nullable());
        assert!(exec.static_term().schema().field(0).is_nullable());
        assert!(exec.recursive_term().schema().field(0).is_nullable());
        Ok(())
    }
}
