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

//! Stream and channel implementations for window function expressions.

use std::collections::VecDeque;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use super::utils::create_schema;
use crate::execution_plan::{CardinalityEffect, EmissionType};
use crate::metrics::{
    BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet, SpillMetrics,
};
use crate::spill::get_record_batch_memory_size;
use crate::spill::in_progress_spill_file::InProgressSpillFile;
use crate::spill::spill_manager::SpillManager;
use crate::stream::EmptyRecordBatchStream;
use crate::windows::{
    calc_requirements, get_ordered_partition_by_indices, get_partition_by_sort_exprs,
    window_equivalence_properties,
};
use crate::{
    ColumnStatistics, DisplayAs, DisplayFormatType, Distribution, ExecutionPlan,
    ExecutionPlanProperties, PhysicalExpr, PlanProperties, RecordBatchStream,
    SendableRecordBatchStream, Statistics, WindowExpr, check_if_same_properties,
};

use arrow::array::ArrayRef;
use arrow::compute::concat_batches;
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion_common::stats::Precision;
use datafusion_common::utils::{evaluate_partition_ranges, get_row_at_idx};
use datafusion_common::{
    DataFusionError, Result, ScalarValue, assert_eq_or_internal_err,
};
use datafusion_execution::TaskContext;
use datafusion_execution::disk_manager::RefCountedTempFile;
use datafusion_execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion_physical_expr_common::sort_expr::{
    OrderingRequirements, PhysicalSortExpr,
};

use futures::{Stream, StreamExt, ready};

/// Window execution plan
#[derive(Debug, Clone)]
pub struct WindowAggExec {
    /// Input plan
    pub(crate) input: Arc<dyn ExecutionPlan>,
    /// Window function expression
    window_expr: Vec<Arc<dyn WindowExpr>>,
    /// Schema after the window is run
    schema: SchemaRef,
    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,
    /// Partition by indices that defines preset for existing ordering
    // see `get_ordered_partition_by_indices` for more details.
    ordered_partition_by_indices: Vec<usize>,
    /// Cache holding plan properties like equivalences, output partitioning etc.
    cache: Arc<PlanProperties>,
    /// If `can_partition` is false, partition_keys is always empty.
    can_repartition: bool,
}

impl WindowAggExec {
    /// Create a new execution plan for window aggregates
    pub fn try_new(
        window_expr: Vec<Arc<dyn WindowExpr>>,
        input: Arc<dyn ExecutionPlan>,
        can_repartition: bool,
    ) -> Result<Self> {
        let schema = create_schema(&input.schema(), &window_expr)?;
        let schema = Arc::new(schema);

        let ordered_partition_by_indices =
            get_ordered_partition_by_indices(window_expr[0].partition_by(), &input)?;
        let cache = Self::compute_properties(&schema, &input, &window_expr)?;
        Ok(Self {
            input,
            window_expr,
            schema,
            metrics: ExecutionPlanMetricsSet::new(),
            ordered_partition_by_indices,
            cache: Arc::new(cache),
            can_repartition,
        })
    }

    /// Window expressions
    pub fn window_expr(&self) -> &[Arc<dyn WindowExpr>] {
        &self.window_expr
    }

    /// Input plan
    pub fn input(&self) -> &Arc<dyn ExecutionPlan> {
        &self.input
    }

    /// Return the output sort order of partition keys: For example
    /// OVER(PARTITION BY a, ORDER BY b) -> would give sorting of the column a
    // We are sure that partition by columns are always at the beginning of sort_keys
    // Hence returned `PhysicalSortExpr` corresponding to `PARTITION BY` columns can be used safely
    // to calculate partition separation points
    pub fn partition_by_sort_keys(&self) -> Result<Vec<PhysicalSortExpr>> {
        let partition_by = self.window_expr()[0].partition_by();
        get_partition_by_sort_exprs(
            &self.input,
            partition_by,
            &self.ordered_partition_by_indices,
        )
    }

    /// This function creates the cache object that stores the plan properties such as schema, equivalence properties, ordering, partitioning, etc.
    fn compute_properties(
        schema: &SchemaRef,
        input: &Arc<dyn ExecutionPlan>,
        window_exprs: &[Arc<dyn WindowExpr>],
    ) -> Result<PlanProperties> {
        // Calculate equivalence properties:
        let eq_properties = window_equivalence_properties(schema, input, window_exprs)?;

        // Get output partitioning:
        // Because we can have repartitioning using the partition keys this
        // would be either 1 or more than 1 depending on the presence of repartitioning.
        let output_partitioning = input.output_partitioning().clone();

        // Construct properties cache:
        Ok(PlanProperties::new(
            eq_properties,
            output_partitioning,
            // TODO: Emission type and boundedness information can be enhanced here
            EmissionType::Final,
            input.boundedness(),
        ))
    }

    pub fn partition_keys(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        if !self.can_repartition {
            vec![]
        } else {
            let all_partition_keys = self
                .window_expr()
                .iter()
                .map(|expr| expr.partition_by().to_vec())
                .collect::<Vec<_>>();

            all_partition_keys
                .into_iter()
                .min_by_key(|s| s.len())
                .unwrap_or_else(Vec::new)
        }
    }

    fn with_new_children_and_same_properties(
        &self,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Self {
        Self {
            input: children.swap_remove(0),
            metrics: ExecutionPlanMetricsSet::new(),
            ..Self::clone(self)
        }
    }
}

impl DisplayAs for WindowAggExec {
    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "WindowAggExec: ")?;
                let g: Vec<String> = self
                    .window_expr
                    .iter()
                    .map(|e| {
                        format!(
                            "{}: {:?}, frame: {:?}",
                            e.name().to_owned(),
                            e.field(),
                            e.get_window_frame()
                        )
                    })
                    .collect();
                write!(f, "wdw=[{}]", g.join(", "))?;
            }
            DisplayFormatType::TreeRender => {
                let g: Vec<String> = self
                    .window_expr
                    .iter()
                    .map(|e| e.name().to_owned().to_string())
                    .collect();
                writeln!(f, "select_list={}", g.join(", "))?;
            }
        }
        Ok(())
    }
}

impl ExecutionPlan for WindowAggExec {
    fn name(&self) -> &'static str {
        "WindowAggExec"
    }

    /// Return a reference to Any that can be used for downcasting
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }

    fn required_input_ordering(&self) -> Vec<Option<OrderingRequirements>> {
        let partition_bys = self.window_expr()[0].partition_by();
        let order_keys = self.window_expr()[0].order_by();
        if self.ordered_partition_by_indices.len() < partition_bys.len() {
            vec![calc_requirements(partition_bys, order_keys)]
        } else {
            let partition_bys = self
                .ordered_partition_by_indices
                .iter()
                .map(|idx| &partition_bys[*idx]);
            vec![calc_requirements(partition_bys, order_keys)]
        }
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        if self.partition_keys().is_empty() {
            vec![Distribution::SinglePartition]
        } else {
            vec![Distribution::HashPartitioned(self.partition_keys())]
        }
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        check_if_same_properties!(self, children);
        Ok(Arc::new(WindowAggExec::try_new(
            self.window_expr.clone(),
            children.swap_remove(0),
            true,
        )?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let input = self.input.execute(partition, Arc::clone(&context))?;
        let input_schema = input.schema();
        let can_spill = context.runtime_env().disk_manager.tmp_files_enabled();
        let reservation = MemoryConsumer::new(format!("WindowAggStream[{partition}]"))
            .with_can_spill(can_spill)
            .register(context.memory_pool());
        let spill_manager = SpillManager::new(
            context.runtime_env(),
            SpillMetrics::new(&self.metrics, partition),
            Arc::clone(&input_schema),
        )
        .with_compression_type(context.session_config().spill_compression());
        let stream = Box::pin(WindowAggStream::new(
            Arc::clone(&self.schema),
            input_schema,
            self.window_expr.clone(),
            input,
            BaselineMetrics::new(&self.metrics, partition),
            self.partition_by_sort_keys()?,
            self.ordered_partition_by_indices.clone(),
            reservation,
            spill_manager,
            can_spill,
        )?);
        Ok(stream)
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn partition_statistics(&self, partition: Option<usize>) -> Result<Arc<Statistics>> {
        let input_stat =
            Arc::unwrap_or_clone(self.input.partition_statistics(partition)?);
        let win_cols = self.window_expr.len();
        let input_cols = self.input.schema().fields().len();
        // TODO stats: some windowing function will maintain invariants such as min, max...
        let mut column_statistics = Vec::with_capacity(win_cols + input_cols);
        // copy stats of the input to the beginning of the schema.
        column_statistics.extend(input_stat.column_statistics);
        for _ in 0..win_cols {
            column_statistics.push(ColumnStatistics::new_unknown())
        }
        Ok(Arc::new(Statistics {
            num_rows: input_stat.num_rows,
            column_statistics,
            total_byte_size: Precision::Absent,
        }))
    }

    fn cardinality_effect(&self) -> CardinalityEffect {
        CardinalityEffect::Equal
    }
}

/// Compute the window aggregate columns
fn compute_window_aggregates(
    window_expr: &[Arc<dyn WindowExpr>],
    batch: &RecordBatch,
) -> Result<Vec<ArrayRef>> {
    window_expr
        .iter()
        .map(|window_expr| window_expr.evaluate(batch))
        .collect()
}

struct ActivePartition {
    key: Vec<ScalarValue>,
    in_memory_batches: Vec<RecordBatch>,
    memory_bytes: usize,
    spill_file: Option<InProgressSpillFile>,
}

enum CompletedPartition {
    InMemory {
        batches: Vec<RecordBatch>,
        memory_bytes: usize,
    },
    Spilled {
        spill_file: RefCountedTempFile,
    },
}

struct SpillReadState {
    stream: SendableRecordBatchStream,
    batches: Vec<RecordBatch>,
    memory_bytes: usize,
}

/// stream for window aggregation plan
pub struct WindowAggStream {
    schema: SchemaRef,
    input_schema: SchemaRef,
    input: SendableRecordBatchStream,
    finished: bool,
    input_finished: bool,
    window_expr: Vec<Arc<dyn WindowExpr>>,
    partition_by_sort_keys: Vec<PhysicalSortExpr>,
    baseline_metrics: BaselineMetrics,
    ordered_partition_by_indices: Vec<usize>,
    reservation: MemoryReservation,
    spill_manager: SpillManager,
    can_spill: bool,
    active_partition: Option<ActivePartition>,
    completed_partitions: VecDeque<CompletedPartition>,
    spill_read_state: Option<SpillReadState>,
}

impl WindowAggStream {
    /// Create a new WindowAggStream
    pub fn new(
        schema: SchemaRef,
        input_schema: SchemaRef,
        window_expr: Vec<Arc<dyn WindowExpr>>,
        input: SendableRecordBatchStream,
        baseline_metrics: BaselineMetrics,
        partition_by_sort_keys: Vec<PhysicalSortExpr>,
        ordered_partition_by_indices: Vec<usize>,
        reservation: MemoryReservation,
        spill_manager: SpillManager,
        can_spill: bool,
    ) -> Result<Self> {
        // In WindowAggExec all partition by columns should be ordered.
        assert_eq_or_internal_err!(
            window_expr[0].partition_by().len(),
            ordered_partition_by_indices.len(),
            "All partition by columns should have an ordering"
        );
        Ok(Self {
            schema,
            input_schema,
            input,
            finished: false,
            input_finished: false,
            window_expr,
            baseline_metrics,
            partition_by_sort_keys,
            ordered_partition_by_indices,
            reservation,
            spill_manager,
            can_spill,
            active_partition: None,
            completed_partitions: VecDeque::new(),
            spill_read_state: None,
        })
    }

    fn compute_partition_aggregates(
        &self,
        batches: &[RecordBatch],
    ) -> Result<Option<RecordBatch>> {
        // record compute time on drop
        let _timer = self.baseline_metrics.elapsed_compute().timer();

        let batch = concat_batches(&self.input_schema, batches)?;
        if batch.num_rows() == 0 {
            return Ok(None);
        }

        // combine with the original cols
        // note the setup of window aggregates is that they newly calculated window
        // expression results are always appended to the columns
        let mut batch_columns = batch.columns().to_vec();
        // calculate window cols
        let columns = compute_window_aggregates(&self.window_expr, &batch)?;
        batch_columns.extend_from_slice(&columns);
        Ok(Some(RecordBatch::try_new(
            Arc::clone(&self.schema),
            batch_columns,
        )?))
    }

    fn reserved_bytes_for_batch(batch: &RecordBatch) -> usize {
        get_record_batch_memory_size(batch)
    }

    fn err_with_oom_context(e: DataFusionError) -> DataFusionError {
        match e {
            DataFusionError::ResourcesExhausted(_) => e.context(
                "Not enough memory to buffer WindowAggExec partition. \
                 Configure a spill-capable disk manager or increase \
                 'datafusion.runtime.memory_limit'.",
            ),
            _ => e,
        }
    }

    fn process_input_batch(&mut self, batch: RecordBatch) -> Result<()> {
        if batch.num_rows() == 0 {
            return Ok(());
        }

        let partition_by_sort_keys = self
            .ordered_partition_by_indices
            .iter()
            .map(|idx| self.partition_by_sort_keys[*idx].evaluate_to_sort_column(&batch))
            .collect::<Result<Vec<_>>>()?;
        let partition_points =
            evaluate_partition_ranges(batch.num_rows(), &partition_by_sort_keys)?;
        let partition_key_columns = partition_by_sort_keys
            .iter()
            .map(|sort_column| Arc::clone(&sort_column.values))
            .collect::<Vec<_>>();

        for partition_point in partition_points {
            if partition_point.is_empty() {
                continue;
            }

            let key = if partition_key_columns.is_empty() {
                vec![]
            } else {
                get_row_at_idx(&partition_key_columns, partition_point.start)?
            };

            self.ensure_active_partition(key)?;

            let length = partition_point.end - partition_point.start;
            self.append_to_active_partition(batch.slice(partition_point.start, length))?;
        }

        Ok(())
    }

    fn ensure_active_partition(&mut self, key: Vec<ScalarValue>) -> Result<()> {
        match &self.active_partition {
            Some(active_partition) if active_partition.key == key => Ok(()),
            Some(_) => {
                self.finish_active_partition()?;
                self.active_partition = Some(ActivePartition {
                    key,
                    in_memory_batches: vec![],
                    memory_bytes: 0,
                    spill_file: None,
                });
                Ok(())
            }
            None => {
                self.active_partition = Some(ActivePartition {
                    key,
                    in_memory_batches: vec![],
                    memory_bytes: 0,
                    spill_file: None,
                });
                Ok(())
            }
        }
    }

    fn append_to_active_partition(&mut self, batch: RecordBatch) -> Result<()> {
        if self
            .active_partition
            .as_ref()
            .expect("active partition should be initialized before appending")
            .spill_file
            .is_some()
        {
            self.append_to_active_spill(&batch)?;
            return Ok(());
        }

        let memory_bytes = Self::reserved_bytes_for_batch(&batch);
        match self.reservation.try_grow(memory_bytes) {
            Ok(_) => {
                let active_partition = self
                    .active_partition
                    .as_mut()
                    .expect("active partition should be initialized before appending");
                active_partition.memory_bytes += memory_bytes;
                active_partition.in_memory_batches.push(batch);
                Ok(())
            }
            Err(DataFusionError::ResourcesExhausted(_)) if self.can_spill => {
                self.spill_active_partition()?;
                self.append_to_active_spill(&batch)?;
                Ok(())
            }
            Err(e) => Err(Self::err_with_oom_context(e)),
        }
    }

    fn append_to_active_spill(&mut self, batch: &RecordBatch) -> Result<()> {
        let active_partition = self
            .active_partition
            .as_mut()
            .expect("active partition should be initialized before spilling");

        if active_partition.spill_file.is_none() {
            active_partition.spill_file = Some(
                self.spill_manager
                    .create_in_progress_file("WindowAggSpill")?,
            );
        }

        active_partition
            .spill_file
            .as_mut()
            .expect("spill file should exist")
            .append_batch(batch)?;
        Ok(())
    }

    fn spill_active_partition(&mut self) -> Result<()> {
        let active_partition = self
            .active_partition
            .as_mut()
            .expect("active partition should be initialized before spilling");

        if active_partition.spill_file.is_none() {
            active_partition.spill_file = Some(
                self.spill_manager
                    .create_in_progress_file("WindowAggSpill")?,
            );
        }

        if let Some(spill_file) = &mut active_partition.spill_file {
            for batch in &active_partition.in_memory_batches {
                spill_file.append_batch(batch)?;
            }
        }

        active_partition.in_memory_batches.clear();
        if active_partition.memory_bytes > 0 {
            self.reservation.shrink(active_partition.memory_bytes);
            active_partition.memory_bytes = 0;
        }

        Ok(())
    }

    fn spill_active_partition_if_buffered(&mut self) -> Result<()> {
        if !self.can_spill {
            return Ok(());
        }

        let should_spill = self
            .active_partition
            .as_ref()
            .map(|active_partition| {
                active_partition.spill_file.is_none()
                    && !active_partition.in_memory_batches.is_empty()
            })
            .unwrap_or(false);

        if should_spill {
            self.spill_active_partition()?;
        }

        Ok(())
    }

    fn finish_active_partition(&mut self) -> Result<()> {
        let Some(mut active_partition) = self.active_partition.take() else {
            return Ok(());
        };

        if let Some(mut spill_file) = active_partition.spill_file.take() {
            for batch in &active_partition.in_memory_batches {
                spill_file.append_batch(batch)?;
            }
            active_partition.in_memory_batches.clear();
            if active_partition.memory_bytes > 0 {
                self.reservation.shrink(active_partition.memory_bytes);
            }

            if let Some(spill_file) = spill_file.finish()? {
                self.completed_partitions
                    .push_back(CompletedPartition::Spilled { spill_file });
            }
        } else if !active_partition.in_memory_batches.is_empty() {
            self.completed_partitions
                .push_back(CompletedPartition::InMemory {
                    batches: active_partition.in_memory_batches,
                    memory_bytes: active_partition.memory_bytes,
                });
        }

        Ok(())
    }

    fn poll_completed_partition(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<RecordBatch>>> {
        loop {
            if self.spill_read_state.is_some() {
                match ready!(self.poll_spill_read(cx)) {
                    Some(result) => return Poll::Ready(Some(result)),
                    None => continue,
                }
            }

            let Some(completed_partition) = self.completed_partitions.pop_front() else {
                return Poll::Ready(None);
            };

            match completed_partition {
                CompletedPartition::InMemory {
                    batches,
                    memory_bytes,
                } => {
                    let result = self.compute_partition_aggregates(&batches);
                    if memory_bytes > 0 {
                        self.reservation.shrink(memory_bytes);
                    }
                    match result {
                        Ok(Some(batch)) => return Poll::Ready(Some(Ok(batch))),
                        Ok(None) => continue,
                        Err(e) => return Poll::Ready(Some(Err(e))),
                    }
                }
                CompletedPartition::Spilled { spill_file } => {
                    if let Err(e) = self.spill_active_partition_if_buffered() {
                        return Poll::Ready(Some(Err(e)));
                    }
                    match self.spill_manager.read_spill_as_stream(spill_file, None) {
                        Ok(stream) => {
                            self.spill_read_state = Some(SpillReadState {
                                stream,
                                batches: vec![],
                                memory_bytes: 0,
                            });
                        }
                        Err(e) => return Poll::Ready(Some(Err(e))),
                    }
                }
            }
        }
    }

    fn poll_spill_read(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<RecordBatch>>> {
        loop {
            let next_batch = {
                let spill_read_state = self
                    .spill_read_state
                    .as_mut()
                    .expect("spill read state should exist");
                ready!(spill_read_state.stream.poll_next_unpin(cx))
            };

            match next_batch {
                Some(Ok(batch)) => {
                    let memory_bytes = Self::reserved_bytes_for_batch(&batch);
                    let memory_bytes = match self
                        .reservation
                        .try_grow(memory_bytes)
                        .map(|_| memory_bytes)
                    {
                        Ok(memory_bytes) => memory_bytes,
                        Err(e) => {
                            let spill_read_state = self
                                .spill_read_state
                                .take()
                                .expect("spill read state should exist when read fails");
                            if spill_read_state.memory_bytes > 0 {
                                self.reservation.shrink(spill_read_state.memory_bytes);
                            }
                            return Poll::Ready(Some(Err(Self::err_with_oom_context(e))));
                        }
                    };

                    let spill_read_state = self
                        .spill_read_state
                        .as_mut()
                        .expect("spill read state should exist");
                    spill_read_state.memory_bytes += memory_bytes;
                    spill_read_state.batches.push(batch);
                }
                Some(Err(e)) => {
                    let spill_read_state = self
                        .spill_read_state
                        .take()
                        .expect("spill read state should exist when read fails");
                    if spill_read_state.memory_bytes > 0 {
                        self.reservation.shrink(spill_read_state.memory_bytes);
                    }
                    return Poll::Ready(Some(Err(e)));
                }
                None => {
                    let spill_read_state = self
                        .spill_read_state
                        .take()
                        .expect("spill read state should exist when read finishes");
                    let result =
                        self.compute_partition_aggregates(&spill_read_state.batches);
                    if spill_read_state.memory_bytes > 0 {
                        self.reservation.shrink(spill_read_state.memory_bytes);
                    }
                    match result {
                        Ok(Some(batch)) => return Poll::Ready(Some(Ok(batch))),
                        Ok(None) => return Poll::Ready(None),
                        Err(e) => return Poll::Ready(Some(Err(e))),
                    }
                }
            }
        }
    }
}

impl Stream for WindowAggStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let poll = self.poll_next_inner(cx);
        self.baseline_metrics.record_poll(poll)
    }
}

impl WindowAggStream {
    #[inline]
    fn poll_next_inner(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<RecordBatch>>> {
        if self.finished {
            return Poll::Ready(None);
        }

        loop {
            match ready!(self.poll_completed_partition(cx)) {
                Some(result) => return Poll::Ready(Some(result)),
                None if self.input_finished => {
                    self.finished = true;
                    return Poll::Ready(None);
                }
                None => {}
            }

            return Poll::Ready(Some(match ready!(self.input.poll_next_unpin(cx)) {
                Some(Ok(batch)) => {
                    self.process_input_batch(batch)?;
                    continue;
                }
                Some(Err(e)) => Err(e),
                None => {
                    // Release the input pipeline's resources before computing
                    // the final aggregates.
                    self.input = Box::pin(EmptyRecordBatchStream::new(Arc::clone(
                        &self.input_schema,
                    )));
                    self.finish_active_partition()?;
                    self.input_finished = true;
                    continue;
                }
            }));
        }
    }
}

impl RecordBatchStream for WindowAggStream {
    /// Get the schema
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::collect;
    use crate::test::TestMemoryExec;
    use crate::windows::create_window_expr;
    use arrow::array::Int64Array;
    use arrow::compute::SortOptions;
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_common::{ScalarValue, assert_batches_eq, assert_contains};
    use datafusion_execution::disk_manager::{DiskManagerBuilder, DiskManagerMode};
    use datafusion_execution::runtime_env::RuntimeEnvBuilder;
    use datafusion_expr::{
        WindowFrame, WindowFrameBound, WindowFrameUnits, WindowFunctionDefinition,
    };
    use datafusion_functions_aggregate::count::count_udaf;
    use datafusion_functions_window::ntile::ntile_udwf;
    use datafusion_physical_expr::expressions::Literal;
    use datafusion_physical_expr::{LexOrdering, PhysicalExpr};

    #[test]
    fn test_window_agg_cardinality_effect() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, true)]));
        let input: Arc<dyn ExecutionPlan> =
            Arc::new(TestMemoryExec::try_new(&[], Arc::clone(&schema), None)?);
        let args = vec![crate::expressions::col("a", &schema)?];
        let window_expr = create_window_expr(
            &WindowFunctionDefinition::AggregateUDF(count_udaf()),
            "count(a)".to_string(),
            &args,
            &[],
            &[],
            Arc::new(WindowFrame::new_bounds(
                WindowFrameUnits::Rows,
                WindowFrameBound::Preceding(ScalarValue::UInt64(None)),
                WindowFrameBound::CurrentRow,
            )),
            Arc::clone(&schema),
            false,
            false,
            None,
        )?;

        let window = WindowAggExec::try_new(vec![window_expr], input, true)?;
        assert!(matches!(
            window.cardinality_effect(),
            CardinalityEffect::Equal
        ));
        Ok(())
    }

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("p", DataType::Int64, false),
            Field::new("v", DataType::Int64, false),
        ]))
    }

    fn make_batch(partitions: Vec<i64>, values: Vec<i64>) -> Result<RecordBatch> {
        assert_eq!(partitions.len(), values.len());
        RecordBatch::try_new(
            test_schema(),
            vec![
                Arc::new(Int64Array::from(partitions)),
                Arc::new(Int64Array::from(values)),
            ],
        )
        .map_err(Into::into)
    }

    fn window_ordering(schema: &SchemaRef) -> Result<LexOrdering> {
        Ok(LexOrdering::new([
            PhysicalSortExpr::new_default(crate::expressions::col("p", schema)?),
            PhysicalSortExpr::new_default(crate::expressions::col("v", schema)?),
        ])
        .expect("ordering is not empty"))
    }

    fn window_input(batches: Vec<RecordBatch>) -> Result<Arc<dyn ExecutionPlan>> {
        let schema = test_schema();
        let input = TestMemoryExec::try_new(&[batches], Arc::clone(&schema), None)?
            .try_with_sort_information(vec![window_ordering(&schema)?])?;
        Ok(Arc::new(input))
    }

    fn ntile_window_expr(schema: &SchemaRef) -> Result<Arc<dyn WindowExpr>> {
        let args =
            vec![Arc::new(Literal::new(ScalarValue::UInt64(Some(2))))
                as Arc<dyn PhysicalExpr>];
        let partition_by = vec![crate::expressions::col("p", schema)?];
        let order_by = vec![PhysicalSortExpr {
            expr: crate::expressions::col("v", schema)?,
            options: SortOptions::default(),
        }];

        create_window_expr(
            &WindowFunctionDefinition::WindowUDF(ntile_udwf()),
            "ntile".to_string(),
            &args,
            &partition_by,
            &order_by,
            Arc::new(WindowFrame::new_bounds(
                WindowFrameUnits::Rows,
                WindowFrameBound::Preceding(ScalarValue::UInt64(None)),
                WindowFrameBound::Following(ScalarValue::UInt64(None)),
            )),
            Arc::clone(schema),
            false,
            false,
            None,
        )
    }

    fn window_exec(input: Arc<dyn ExecutionPlan>) -> Result<WindowAggExec> {
        WindowAggExec::try_new(vec![ntile_window_expr(&input.schema())?], input, true)
    }

    fn partition_boundary_batches() -> Result<Vec<RecordBatch>> {
        Ok(vec![
            // Partition 1 starts in this batch and continues in the next.
            make_batch(vec![0, 0, 1, 1], vec![1, 2, 1, 2])?,
            // This batch contains the end of partition 1 and all of partition 2.
            make_batch(vec![1, 2, 2, 2], vec![3, 1, 2, 3])?,
            make_batch(vec![3, 3], vec![1, 2])?,
        ])
    }

    fn task_ctx_with_memory_limit(
        memory_limit: usize,
        disk_mode: DiskManagerMode,
    ) -> Result<Arc<TaskContext>> {
        let runtime = RuntimeEnvBuilder::new()
            .with_memory_limit(memory_limit, 1.0)
            .with_disk_manager_builder(DiskManagerBuilder::default().with_mode(disk_mode))
            .build_arc()?;

        Ok(Arc::new(TaskContext::default().with_runtime(runtime)))
    }

    async fn collect_window(
        batches: Vec<RecordBatch>,
        task_ctx: Arc<TaskContext>,
    ) -> Result<(Vec<RecordBatch>, MetricsSet)> {
        let window = window_exec(window_input(batches)?)?;
        let output = collect(window.execute(0, task_ctx)?).await?;
        let metrics = window.metrics().expect("metrics should exist");
        Ok((output, metrics))
    }

    fn assert_ntile_output(batches: &[RecordBatch]) {
        assert_batches_eq!(
            [
                "+---+---+-------+",
                "| p | v | ntile |",
                "+---+---+-------+",
                "| 0 | 1 | 1     |",
                "| 0 | 2 | 2     |",
                "| 1 | 1 | 1     |",
                "| 1 | 2 | 1     |",
                "| 1 | 3 | 2     |",
                "| 2 | 1 | 1     |",
                "| 2 | 2 | 1     |",
                "| 2 | 3 | 2     |",
                "| 3 | 1 | 1     |",
                "| 3 | 2 | 2     |",
                "+---+---+-------+",
            ],
            batches
        );
    }

    #[tokio::test]
    async fn test_window_agg_exec_partition_spill() -> Result<()> {
        let task_ctx = task_ctx_with_memory_limit(150, DiskManagerMode::OsTmpDirectory)?;
        let (batches, metrics) =
            collect_window(partition_boundary_batches()?, task_ctx).await?;

        assert_ntile_output(&batches);
        assert!(metrics.spill_count().unwrap_or(0) > 0);
        assert!(metrics.spilled_rows().unwrap_or(0) > 0);
        assert!(metrics.spilled_bytes().unwrap_or(0) > 0);
        Ok(())
    }

    #[tokio::test]
    async fn test_window_agg_exec_no_spill_when_memory_sufficient() -> Result<()> {
        let task_ctx =
            task_ctx_with_memory_limit(10_000_000, DiskManagerMode::OsTmpDirectory)?;
        let (batches, metrics) =
            collect_window(partition_boundary_batches()?, task_ctx).await?;

        assert_ntile_output(&batches);
        assert_eq!(metrics.spill_count().unwrap_or(0), 0);
        assert_eq!(metrics.spilled_rows().unwrap_or(0), 0);
        assert_eq!(metrics.spilled_bytes().unwrap_or(0), 0);
        Ok(())
    }

    #[tokio::test]
    async fn test_window_agg_exec_spill_requires_disk_manager() -> Result<()> {
        let task_ctx = task_ctx_with_memory_limit(150, DiskManagerMode::Disabled)?;
        let err = collect_window(partition_boundary_batches()?, task_ctx)
            .await
            .unwrap_err();

        assert_contains!(err.to_string(), "Resources exhausted");
        Ok(())
    }

    #[tokio::test]
    async fn test_window_agg_exec_empty_input() -> Result<()> {
        let task_ctx = task_ctx_with_memory_limit(150, DiskManagerMode::OsTmpDirectory)?;
        let (batches, metrics) = collect_window(vec![], task_ctx).await?;

        assert!(batches.is_empty());
        assert_eq!(metrics.spill_count().unwrap_or(0), 0);
        Ok(())
    }
}
