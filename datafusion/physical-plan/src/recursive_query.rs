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
use std::task::{Context, Poll};

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
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, RecordBatchStream,
    SendableRecordBatchStream,
};
use arrow::array::{BooleanArray, BooleanBuilder};
use arrow::compute::filter_record_batch;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion_common::tree_node::TreeNodeRecursion;
use datafusion_common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion_common::{Result, internal_datafusion_err, not_impl_err};
use datafusion_execution::TaskContext;
use datafusion_execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion_physical_expr::PhysicalExpr;
use datafusion_physical_expr::{EquivalenceProperties, Partitioning};

use futures::{Stream, StreamExt, ready};

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
        static_term: Arc<dyn ExecutionPlan>,
        recursive_term: Arc<dyn ExecutionPlan>,
        output_schema: &SchemaRef,
        is_distinct: bool,
    ) -> Result<Self> {
        // Each recursive query needs its own work table
        let work_table = Arc::new(WorkTable::new(name.clone()));
        // The logical recursive CTE schema is authoritative. Align both
        // children at plan construction time instead of patching batches in
        // RecursiveQueryStream.
        let recursive_term = assign_work_table(recursive_term, &work_table)?;
        let static_term =
            align_recursive_child_to_logical_schema(static_term, output_schema)?;
        let recursive_term =
            align_recursive_child_to_logical_schema(recursive_term, output_schema)?;
        let cache = Self::compute_properties(Arc::clone(output_schema));
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

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&dyn PhysicalExpr) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
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
            Arc::clone(&children[0]),
            Arc::clone(&children[1]),
            &self.schema(),
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
        Ok(Box::pin(RecursiveQueryStream::new(
            context,
            Arc::clone(&self.work_table),
            Arc::clone(&self.recursive_term),
            static_stream,
            self.is_distinct,
            baseline_metrics,
        )?))
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
    /// The static part (static term) as a stream. If the processing of this
    /// part is completed, then it will be None.
    static_stream: Option<SendableRecordBatchStream>,
    /// The dynamic part (recursive term) as a stream. If the processing of this
    /// part has not started yet, or has been completed, then it will be None.
    recursive_stream: Option<SendableRecordBatchStream>,
    /// The schema of the output.
    schema: SchemaRef,
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
    /// Create a new recursive query stream
    fn new(
        task_context: Arc<TaskContext>,
        work_table: Arc<WorkTable>,
        recursive_term: Arc<dyn ExecutionPlan>,
        static_stream: SendableRecordBatchStream,
        is_distinct: bool,
        baseline_metrics: BaselineMetrics,
    ) -> Result<Self> {
        let schema = static_stream.schema();
        let reservation =
            MemoryConsumer::new("RecursiveQuery").register(task_context.memory_pool());
        let distinct_deduplicator = is_distinct
            .then(|| DistinctDeduplicator::new(Arc::clone(&schema), &task_context))
            .transpose()?;
        Ok(Self {
            task_context,
            work_table,
            recursive_term,
            static_stream: Some(static_stream),
            recursive_stream: None,
            schema,
            buffer: vec![],
            reservation,
            distinct_deduplicator,
            baseline_metrics,
        })
    }

    /// Push a clone of the given batch to the in memory buffer, and then return
    /// a poll with it.
    fn push_batch(
        mut self: std::pin::Pin<&mut Self>,
        mut batch: RecordBatch,
    ) -> Poll<Option<Result<RecordBatch>>> {
        let baseline_metrics = self.baseline_metrics.clone();

        if let Some(deduplicator) = &mut self.distinct_deduplicator {
            let _timer_guard = baseline_metrics.elapsed_compute().timer();
            batch = deduplicator.deduplicate(&batch)?;
        }

        if let Err(e) = self.reservation.try_grow(batch.get_array_memory_size()) {
            return Poll::Ready(Some(Err(e)));
        }
        self.buffer.push(batch.clone());
        (&batch).record_output(&baseline_metrics);
        Poll::Ready(Some(Ok(batch)))
    }

    /// Start polling for the next iteration, will be called either after the static term
    /// is completed or another term is completed. It will follow the algorithm above on
    /// to check whether the recursion has ended.
    fn poll_next_iteration(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<RecordBatch>>> {
        let total_length = self
            .buffer
            .iter()
            .fold(0, |acc, batch| acc + batch.num_rows());

        if total_length == 0 {
            return Poll::Ready(None);
        }

        // Update the work table with the current buffer
        let reserved_batches = ReservedBatches::new(
            std::mem::take(&mut self.buffer),
            self.reservation.take(),
        );
        self.work_table.update(reserved_batches);

        // We always execute (and re-execute iteratively) the first partition.
        // Downstream plans should not expect any partitioning.
        let partition = 0;

        let recursive_plan = reset_plan_states(Arc::clone(&self.recursive_term))?;
        self.recursive_stream =
            Some(recursive_plan.execute(partition, Arc::clone(&self.task_context))?);
        self.poll_next(cx)
    }
}

fn align_recursive_child_to_logical_schema(
    input: Arc<dyn ExecutionPlan>,
    output_schema: &SchemaRef,
) -> Result<Arc<dyn ExecutionPlan>> {
    match project_plan_to_schema(Arc::clone(&input), output_schema) {
        Ok(projected) => Ok(projected),
        Err(projection_error) => {
            match RecursiveSchemaRebindExec::try_new(input, Arc::clone(output_schema)) {
                Ok(exec) => Ok(Arc::new(exec)),
                Err(_) => Err(projection_error),
            }
        }
    }
}

#[derive(Debug, Clone)]
struct RecursiveSchemaRebindExec {
    input: Arc<dyn ExecutionPlan>,
    schema: SchemaRef,
    cache: Arc<PlanProperties>,
}

impl RecursiveSchemaRebindExec {
    fn try_new(input: Arc<dyn ExecutionPlan>, schema: SchemaRef) -> Result<Self> {
        validate_recursive_schema_rebind(&input.schema(), &schema)?;
        let input_properties = input.properties();
        let properties = PlanProperties::new(
            EquivalenceProperties::new(Arc::clone(&schema)),
            Partitioning::UnknownPartitioning(
                input_properties.partitioning.partition_count(),
            ),
            input_properties.emission_type,
            input_properties.boundedness,
        )
        .with_evaluation_type(input_properties.evaluation_type)
        .with_scheduling_type(input_properties.scheduling_type);

        Ok(Self {
            input,
            schema,
            cache: Arc::new(properties),
        })
    }
}

impl DisplayAs for RecursiveSchemaRebindExec {
    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "RecursiveSchemaRebindExec")
            }
            DisplayFormatType::TreeRender => Ok(()),
        }
    }
}

impl ExecutionPlan for RecursiveSchemaRebindExec {
    fn name(&self) -> &'static str {
        "RecursiveSchemaRebindExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&dyn PhysicalExpr) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let [input] = children.try_into().map_err(|children: Vec<_>| {
            internal_datafusion_err!(
                "RecursiveSchemaRebindExec expected 1 child, got {}",
                children.len()
            )
        })?;
        Ok(Arc::new(Self::try_new(input, Arc::clone(&self.schema))?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let schema = Arc::clone(&self.schema);
        let stream = self.input.execute(partition, context)?.map({
            let schema = Arc::clone(&schema);
            move |batch| {
                let batch = batch?;
                if batch.schema().as_ref() == schema.as_ref() {
                    Ok(batch)
                } else {
                    RecordBatch::try_new(Arc::clone(&schema), batch.columns().to_vec())
                        .map_err(Into::into)
                }
            }
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }
}

fn validate_recursive_schema_rebind(
    input_schema: &SchemaRef,
    output_schema: &SchemaRef,
) -> Result<()> {
    if input_schema.fields().len() != output_schema.fields().len() {
        return datafusion_common::plan_err!(
            "RecursiveQueryExec input and output schemas have different number of columns: {} != {}",
            input_schema.fields().len(),
            output_schema.fields().len()
        );
    }

    if input_schema.metadata() != output_schema.metadata() {
        return datafusion_common::plan_err!(
            "Cannot align recursive query input to output schema: schema metadata differ"
        );
    }

    if let Some((i, input_field, output_field, mismatch)) = input_schema
        .fields()
        .iter()
        .zip(output_schema.fields().iter())
        .enumerate()
        .find_map(|(i, (input_field, output_field))| {
            if input_field.data_type() != output_field.data_type() {
                Some((i, input_field, output_field, "type"))
            } else if input_field.metadata() != output_field.metadata() {
                Some((i, input_field, output_field, "metadata"))
            } else {
                None
            }
        })
    {
        return datafusion_common::plan_err!(
            "Cannot align recursive query column {i} ('{}') to output field '{}': field {mismatch} differs (input field: {:?}, output field: {:?})",
            input_field.name(),
            output_field.name(),
            input_field,
            output_field
        );
    }

    Ok(())
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

impl Stream for RecursiveQueryStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        if let Some(static_stream) = &mut self.static_stream {
            // While the static term's stream is available, we'll be forwarding the batches from it (also
            // saving them for the initial iteration of the recursive term).
            let batch_result = ready!(static_stream.poll_next_unpin(cx));
            match &batch_result {
                None => {
                    // Once this is done, we can start running the setup for the recursive term.
                    self.static_stream = None;
                    self.poll_next_iteration(cx)
                }
                Some(Ok(batch)) => self.push_batch(batch.clone()),
                _ => Poll::Ready(batch_result),
            }
        } else if let Some(recursive_stream) = &mut self.recursive_stream {
            let batch_result = ready!(recursive_stream.poll_next_unpin(cx));
            match batch_result {
                None => {
                    self.recursive_stream = None;
                    self.poll_next_iteration(cx)
                }
                Some(Ok(batch)) => self.push_batch(batch),
                _ => Poll::Ready(batch_result),
            }
        } else {
            Poll::Ready(None)
        }
    }
}

impl RecordBatchStream for RecursiveQueryStream {
    /// Get the schema
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
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
        self.intern_output_buffer.reserve(batch.num_rows());
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
    use std::collections::HashMap;

    fn empty_exec(fields: Vec<Field>) -> Arc<dyn ExecutionPlan> {
        empty_exec_with_schema(Arc::new(Schema::new(fields)))
    }

    fn empty_exec_with_schema(schema: SchemaRef) -> Arc<dyn ExecutionPlan> {
        Arc::new(EmptyExec::new(schema))
    }

    fn recursive_exec(
        static_term: Arc<dyn ExecutionPlan>,
        recursive_term: Arc<dyn ExecutionPlan>,
        output_schema: &SchemaRef,
    ) -> Result<RecursiveQueryExec> {
        RecursiveQueryExec::try_new(
            "numbers".to_string(),
            static_term,
            recursive_term,
            output_schema,
            false,
        )
    }

    #[test]
    fn recursive_query_exec_projects_recursive_term_to_reconciled_schema() -> Result<()> {
        let static_term = empty_exec(vec![Field::new("value", DataType::Int32, false)]);
        let recursive_term =
            empty_exec(vec![Field::new("value + Int32(1)", DataType::Int32, false)]);

        let exec = recursive_exec(
            Arc::clone(&static_term),
            Arc::clone(&recursive_term),
            &static_term.schema(),
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
    fn recursive_query_exec_widens_output_nullability_from_recursive_term() -> Result<()>
    {
        let static_term = empty_exec(vec![Field::new("value", DataType::Int32, false)]);
        let recursive_term =
            empty_exec(vec![Field::new("value + Int32(1)", DataType::Int32, true)]);

        let expected_schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int32,
            true,
        )]));
        let exec = recursive_exec(
            Arc::clone(&static_term),
            Arc::clone(&recursive_term),
            &expected_schema,
        )?;
        assert_eq!(exec.schema(), expected_schema);
        assert_eq!(exec.static_term().schema(), expected_schema);
        assert_eq!(exec.recursive_term().schema(), expected_schema);
        assert!(exec.schema().field(0).is_nullable());
        Ok(())
    }

    #[test]
    fn recursive_query_exec_uses_rebind_for_nullability_narrowing() -> Result<()> {
        let static_term = empty_exec(vec![Field::new("value", DataType::Int32, false)]);
        let recursive_term = empty_exec(vec![Field::new("value", DataType::Int32, true)]);
        let output_schema = static_term.schema();

        let exec = recursive_exec(
            Arc::clone(&static_term),
            Arc::clone(&recursive_term),
            &output_schema,
        )?;

        assert_eq!(exec.schema(), output_schema);
        assert_eq!(exec.recursive_term().schema(), output_schema);
        assert!(
            exec.recursive_term()
                .downcast_ref::<RecursiveSchemaRebindExec>()
                .is_some()
        );
        Ok(())
    }

    #[test]
    fn recursive_query_exec_rejects_field_metadata_mismatch() {
        let input_metadata = HashMap::from([("source".to_string(), "input".to_string())]);
        let output_metadata =
            HashMap::from([("source".to_string(), "output".to_string())]);
        let static_schema = Arc::new(Schema::new(vec![
            Field::new("value", DataType::Int32, false).with_metadata(input_metadata),
        ]));
        let output_schema = Arc::new(Schema::new(vec![
            Field::new("value", DataType::Int32, false).with_metadata(output_metadata),
        ]));

        let err = recursive_exec(
            empty_exec_with_schema(static_schema),
            empty_exec(vec![Field::new("value", DataType::Int32, false)]),
            &output_schema,
        )
        .unwrap_err();

        assert!(err.to_string().contains("field metadata differs"));
    }

    #[test]
    fn recursive_query_exec_rejects_schema_metadata_mismatch() {
        let static_schema = Arc::new(Schema::new_with_metadata(
            vec![Field::new("value", DataType::Int32, false)],
            HashMap::from([("source".to_string(), "input".to_string())]),
        ));
        let output_schema = Arc::new(Schema::new_with_metadata(
            vec![Field::new("value", DataType::Int32, false)],
            HashMap::from([("source".to_string(), "output".to_string())]),
        ));

        let err = recursive_exec(
            empty_exec_with_schema(static_schema),
            empty_exec(vec![Field::new("value", DataType::Int32, false)]),
            &output_schema,
        )
        .unwrap_err();

        assert!(err.to_string().contains("schema metadata differ"));
    }
}
