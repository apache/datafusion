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

use crate::execution_plan::{CardinalityEffect, ExecutionPlanProperties};
use crate::metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet};
use crate::metrics::{Count, Gauge, MetricBuilder, Time};
use crate::{DisplayAs, DisplayFormatType, ExecutionPlan, SendableRecordBatchStream};
use std::collections::VecDeque;

use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion_common::Result;
use datafusion_expr::match_recognize::{AfterMatchSkip, Pattern, RowsPerMatch};
use datafusion_physical_expr::expressions::col;
use datafusion_physical_expr::{EquivalenceProperties, PhysicalExpr};
use datafusion_physical_expr_common::sort_expr::LexOrdering;

use crate::execution_plan::Statistics;
use crate::ColumnStatistics;

use std::any::Any;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use crate::match_recognize::compile::CompiledPattern;
use crate::match_recognize::matcher::{pattern_schema, PatternMatcher};
use datafusion_execution::RecordBatchStream;
use futures::{
    ready,
    stream::{Stream, StreamExt},
};

use crate::windows::{
    calc_requirements, get_ordered_partition_by_indices, get_partition_by_sort_exprs,
};
use datafusion_common::utils::evaluate_partition_ranges;
use datafusion_physical_expr_common::sort_expr::{
    OrderingRequirements, PhysicalSortExpr,
};

use crate::match_recognize::nfa::PathStep;
use arrow::array::{
    ArrayBuilder, BooleanBuilder, StringBuilder, UInt32Builder, UInt64Builder,
};
use arrow::compute::take_record_batch;

/// Physical execution plan for MATCH_RECOGNIZE pattern detection
/// This node is responsible for:
/// 1. Pattern matching using DEFINE predicates
/// 2. Emitting match metadata (CLASSIFIER, MATCH_NUMBER, MATCH_SEQUENCE_NUMBER)
/// 3. Implementing AFTER MATCH SKIP logic
#[derive(Debug)]
pub struct MatchRecognizePatternExec {
    /// Input execution plan
    input: Arc<dyn ExecutionPlan>,
    /// Schema of the output (input + match metadata columns)
    schema: SchemaRef,
    /// Partition by expressions
    partition_by: Vec<Arc<dyn PhysicalExpr>>,
    /// Order by expressions
    order_by: Option<LexOrdering>,
    /// Defined symbols (from DEFINE clause)
    symbols: Vec<String>,
    /// After match skip strategy
    after_match_skip: Option<AfterMatchSkip>,
    /// Pre-compiled pattern shared across partitions
    compiled_pattern: Arc<CompiledPattern>,
    /// Baseline metrics
    metrics: ExecutionPlanMetricsSet,
    /// Cache for the properties
    cache: crate::execution_plan::PlanProperties,
    /// Indices that define ordering of PARTITION BY columns w.r.t the input ordering
    ordered_partition_by_indices: Vec<usize>,
}

impl MatchRecognizePatternExec {
    /// Create a new MatchRecognizePatternExec
    pub fn try_new(
        input: Arc<dyn ExecutionPlan>,
        partition_by: Vec<Arc<dyn PhysicalExpr>>,
        order_by: Option<LexOrdering>,
        pattern: Pattern,
        symbols: Vec<String>,
        after_match_skip: Option<AfterMatchSkip>,
        rows_per_match: Option<RowsPerMatch>,
    ) -> Result<Self> {
        // Compile the pattern once at plan construction time
        let compiled_pattern = Arc::new(
            CompiledPattern::builder(pattern.clone())
                .symbols(symbols.clone())
                .after_match_skip(after_match_skip.clone())
                .rows_per_match(rows_per_match.clone())
                .build()?,
        );

        Self::try_new_with_compiled_pattern(
            input,
            partition_by,
            order_by,
            symbols,
            after_match_skip,
            compiled_pattern,
        )
    }

    fn try_new_with_compiled_pattern(
        input: Arc<dyn ExecutionPlan>,
        partition_by: Vec<Arc<dyn PhysicalExpr>>,
        order_by: Option<LexOrdering>,
        symbols: Vec<String>,
        after_match_skip: Option<AfterMatchSkip>,
        compiled_pattern: Arc<CompiledPattern>,
    ) -> Result<Self> {
        let input_schema = input.schema();
        let schema = pattern_schema(&input_schema);

        let cache = Self::compute_properties(&input, &schema, &partition_by, &order_by)?;

        let ordered_partition_by_indices =
            get_ordered_partition_by_indices(&partition_by, &input)?;

        Ok(Self {
            input,
            schema,
            partition_by,
            order_by,
            symbols,
            after_match_skip,
            compiled_pattern,
            metrics: ExecutionPlanMetricsSet::new(),
            cache,
            ordered_partition_by_indices,
        })
    }

    /// Compute the properties for this execution plan
    fn compute_properties(
        input: &Arc<dyn ExecutionPlan>,
        schema: &SchemaRef,
        partition_by: &[Arc<dyn PhysicalExpr>],
        order_by: &Option<LexOrdering>,
    ) -> Result<crate::execution_plan::PlanProperties> {
        // Extend input equivalence properties with the output schema
        // This preserves input column equivalences while adding new schema information
        let mut eq_properties = EquivalenceProperties::new(schema.clone())
            .extend(input.equivalence_properties().clone())?;

        // Get the actual partition by sort keys that preserve input ordering
        let ordered_partition_by_indices =
            get_ordered_partition_by_indices(partition_by, input)?;
        let partition_by_sort_keys = get_partition_by_sort_exprs(
            input,
            partition_by,
            &ordered_partition_by_indices,
        )?;

        // Create the output ordering by combining partition_by and order_by
        let mut output_ordering = Vec::new();

        // Add partition_by expressions with their actual sort options from input
        output_ordering.extend(partition_by_sort_keys);

        // Add order_by expressions if present
        if let Some(order_by_exprs) = order_by {
            output_ordering.extend(order_by_exprs.iter().cloned());
        }

        // Add the virtual match_number column
        output_ordering.push(PhysicalSortExpr::new_default(col(
            "__mr_match_number",
            schema,
        )?));

        // Add the combined ordering to equivalence properties
        if !output_ordering.is_empty() {
            eq_properties.add_ordering(output_ordering);
        }

        Ok(crate::execution_plan::PlanProperties::new(
            eq_properties,
            input.output_partitioning().clone(),
            crate::execution_plan::EmissionType::Final,
            input.boundedness(),
        ))
    }

    /// Return the output sort order of partition keys similar to WindowAggExec
    pub fn partition_by_sort_keys(&self) -> Result<Vec<PhysicalSortExpr>> {
        if self.partition_by.is_empty() {
            return Ok(vec![]);
        }
        get_partition_by_sort_exprs(
            &self.input,
            &self.partition_by,
            &self.ordered_partition_by_indices,
        )
    }

    /// Determine the physical expressions used for repartitioning
    pub fn partition_keys(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        if self.partition_by.is_empty() {
            vec![]
        } else {
            self.partition_by.clone()
        }
    }
}

impl DisplayAs for MatchRecognizePatternExec {
    fn fmt_as(
        &self,
        _t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(
            f,
            "MatchRecognizePatternExec: pattern=[{}]",
            self.compiled_pattern.pattern
        )?;

        Ok(())
    }
}

impl ExecutionPlan for MatchRecognizePatternExec {
    fn name(&self) -> &'static str {
        "MatchRecognizePatternExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &crate::execution_plan::PlanProperties {
        &self.cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let new_input = children[0].clone();
        Ok(Arc::new(Self::try_new_with_compiled_pattern(
            new_input,
            self.partition_by.clone(),
            self.order_by.clone(),
            self.symbols.clone(),
            self.after_match_skip.clone(),
            self.compiled_pattern.clone(),
        )?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<datafusion_execution::TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let flush_threshold = context.session_config().batch_size();
        let input_stream = self.input.execute(partition, context)?;
        let metrics = PatternMetrics::new(&self.metrics, partition);

        let partition_by_sort_keys = self.partition_by_sort_keys()?;

        let stream = MatchRecognizePatternStream::try_new(
            input_stream,
            self.schema.clone(),
            self.compiled_pattern.clone(),
            metrics,
            partition_by_sort_keys,
            self.ordered_partition_by_indices.clone(),
            flush_threshold,
        )?;

        Ok(Box::pin(stream))
    }

    fn partition_statistics(&self, partition: Option<usize>) -> Result<Statistics> {
        // Get input statistics first
        let mut stats = self.input.partition_statistics(partition)?;

        // Append the five virtual columns: __mr_classifier, __mr_match_number, __mr_match_sequence_number, __mr_is_last_match_row, __mr_is_included_row
        for _ in 0..5 {
            stats
                .column_statistics
                .push(ColumnStatistics::new_unknown());
        }

        // Since pattern execution can change cardinality unpredictably, mark stats as inexact
        Ok(stats.to_inexact())
    }

    fn cardinality_effect(&self) -> CardinalityEffect {
        match self.after_match_skip {
            None | Some(AfterMatchSkip::PastLastRow) => CardinalityEffect::LowerEqual,
            _ => CardinalityEffect::GreaterEqual,
        }
    }

    fn required_input_ordering(&self) -> Vec<Option<OrderingRequirements>> {
        // Determine ordering requirements similar to WindowAggExec
        let partition_bys = &self.partition_by;
        let order_keys: &[PhysicalSortExpr] = match &self.order_by {
            Some(v) => &v[..],
            None => &[],
        };

        if self.ordered_partition_by_indices.len() < partition_bys.len() {
            vec![calc_requirements(partition_bys, order_keys)]
        } else {
            let reordered: Vec<_> = self
                .ordered_partition_by_indices
                .iter()
                .map(|idx| partition_bys[*idx].clone())
                .collect();
            vec![calc_requirements(&reordered, order_keys)]
        }
    }

    fn required_input_distribution(&self) -> Vec<crate::Distribution> {
        if self.partition_by.is_empty() {
            vec![crate::Distribution::SinglePartition]
        } else {
            vec![crate::Distribution::HashPartitioned(
                self.partition_by.clone(),
            )]
        }
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![false]
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn statistics(&self) -> Result<Statistics> {
        self.partition_statistics(None)
    }
}

/// Accumulates MATCH_RECOGNIZE output rows across many logical partitions and
/// flushes them into fewer, larger `RecordBatch`es.  The struct is self-contained
/// and does **not** allocate new builders between flushes; `finish()` on a builder
/// resets it so we can keep using the same instance.
pub(crate) struct MatchAccumulator {
    // Re-usable Arrow builders for metadata columns
    class_builder: StringBuilder,
    match_num_builder: UInt64Builder,
    seq_builder: UInt64Builder,
    last_flag_builder: BooleanBuilder,
    included_builder: BooleanBuilder,
    // Physical row indices that will be fetched from the input RecordBatch
    indices_builder: UInt32Builder,
    // Bitmap tracking which physical rows were already part of a match (for WITH UNMATCHED ROWS)
    seen_rows: Vec<bool>,
    // Count of logical matches recorded since the last flush
    matches_emitted: usize,
    // Cached input schema (without the virtual MR columns)
    input_schema: SchemaRef,
    // Compiled pattern (used for classifier string lookup and flags)
    compiled_pattern: Arc<CompiledPattern>,
}

impl MatchAccumulator {
    /// Create a new accumulator with empty builders and buffers.
    pub fn new(
        input_schema: SchemaRef,
        compiled_pattern: Arc<CompiledPattern>,
        flush_threshold: usize,
    ) -> Self {
        Self {
            class_builder: StringBuilder::new(),
            match_num_builder: UInt64Builder::new(),
            seq_builder: UInt64Builder::new(),
            last_flag_builder: BooleanBuilder::new(),
            included_builder: BooleanBuilder::new(),
            indices_builder: UInt32Builder::with_capacity(flush_threshold + 1),
            seen_rows: Vec::new(),
            matches_emitted: 0,
            input_schema,
            compiled_pattern,
        }
    }

    /// Current number of accumulated rows.
    #[inline]
    pub fn len(&self) -> usize {
        self.indices_builder.len()
    }

    /// Append the metadata for a single logical match.
    ///
    /// `match_id` – sequential match number (1-based).
    /// `path`     – ordered list of (row, symbol, excluded) steps returned by the matcher.
    /// `num_input_rows` – total rows in the input slice being processed.  Rows ≥ this
    ///                    value are virtual (e.g. anchor predicates) and are ignored.
    pub fn record_match(
        &mut self,
        match_id: u64,
        path: &[PathStep],
        num_input_rows: usize,
    ) {
        // Each call represents one logical match
        self.matches_emitted += 1;
        // Greedy matcher guarantees `path` is already in forward order.
        let mut seq_counter: u64 = 0;
        let compiled = &self.compiled_pattern;

        for (i, step) in path.iter().enumerate() {
            // Skip virtual positions beyond the physical input.
            if step.row >= num_input_rows {
                continue;
            }

            let included = !step.excluded;

            self.indices_builder.append_value(step.row as u32);

            // Track seen rows for WITH UNMATCHED ROWS handling
            if self.compiled_pattern.with_unmatched_rows {
                if step.row >= self.seen_rows.len() {
                    self.seen_rows.resize(step.row + 1, false);
                }
                self.seen_rows[step.row] = true;
            }

            // CLASSIFIER column
            let class_sym: &str = if step.sym.to_index()
                == crate::match_recognize::nfa::Sym::Empty.to_index()
            {
                "(empty)"
            } else {
                compiled.id_to_symbol[step.sym.to_index()]
                    .as_deref()
                    .unwrap_or("(unknown)")
            };
            self.class_builder.append_value(class_sym);

            // MATCH_NUMBER column
            self.match_num_builder.append_value(match_id);

            // MATCH_SEQUENCE_NUMBER column
            if included {
                seq_counter += 1;
                self.seq_builder.append_value(seq_counter);
            } else {
                self.seq_builder.append_value(0_u64);
            }

            // __mr_is_last_match_row flag: true if this is the last step in the path
            let is_last = i + 1 == path.len();
            self.last_flag_builder.append_value(is_last);

            // __mr_is_included_row flag
            self.included_builder.append_value(included);
        }
    }

    /// Flush accumulated rows into a single `RecordBatch`.
    ///
    /// Returns `Ok(None)` if no rows are buffered.
    pub fn flush(&mut self, input_batch: &RecordBatch) -> Result<Option<RecordBatch>> {
        // Previously timed materialisation here; metric removed.

        let num_rows = input_batch.num_rows();

        // WITH UNMATCHED ROWS – append synthetic rows that were not part of any match.
        if self.compiled_pattern.with_unmatched_rows {
            if self.seen_rows.len() < num_rows {
                self.seen_rows.resize(num_rows, false);
            }
            for phys_idx in 0..num_rows {
                if !self.seen_rows[phys_idx] {
                    self.indices_builder.append_value(phys_idx as u32);
                    self.class_builder.append_value("(empty)");
                    self.match_num_builder.append_value(0_u64);
                    self.seq_builder.append_value(0_u64);
                    self.last_flag_builder.append_value(false);
                    self.included_builder.append_value(false);
                }
            }
        }

        // If we still have no indices after considering WITH UNMATCHED ROWS,
        // there is genuinely nothing to output.
        if self.indices_builder.len() == 0 {
            return Ok(None);
        }

        // Build the UInt32 indices array once.
        let indices_array = self.indices_builder.finish();

        let mut output_columns = Vec::with_capacity(input_batch.num_columns() + 5);

        let data_part: RecordBatch = take_record_batch(input_batch, &indices_array)?;
        output_columns.extend(data_part.columns().iter().cloned());

        // Finish Arrow builders (this resets them for re-use).
        let classifier_array = self.class_builder.finish();
        let match_number_array = self.match_num_builder.finish();
        let sequence_number_array = self.seq_builder.finish();
        let last_flag_array = self.last_flag_builder.finish();
        let included_array = self.included_builder.finish();

        output_columns.push(Arc::new(classifier_array));
        output_columns.push(Arc::new(match_number_array));
        output_columns.push(Arc::new(sequence_number_array));
        output_columns.push(Arc::new(last_flag_array));
        output_columns.push(Arc::new(included_array));

        // Build output schema (input + 5 virtual columns)
        let output_schema = pattern_schema(&self.input_schema);
        let output_batch = RecordBatch::try_new(output_schema, output_columns)?;

        // Clear seen bitmap for the next accumulation cycle.
        self.seen_rows.clear();
        // Reset index builder for next cycle (already done by finish())

        Ok(Some(output_batch))
    }

    #[inline]
    pub fn take_matches_total(&mut self) -> usize {
        let m = self.matches_emitted;
        self.matches_emitted = 0;
        m
    }
}

/// Stream for MatchRecognizePatternExec
pub struct MatchRecognizePatternStream {
    /// Input stream
    input: SendableRecordBatchStream,
    /// Output schema
    schema: SchemaRef,
    /// Pattern matcher
    pattern_matcher: PatternMatcher,
    /// Metrics (baseline + custom counters)
    metrics: PatternMetrics,
    /// Accumulated input batches
    batches: Vec<RecordBatch>,
    /// Pending output batches ready to be streamed
    pending_output: VecDeque<RecordBatch>,
    /// Whether the stream is finished
    finished: bool,
    /// Pre-computed sort expressions for PARTITION BY columns
    partition_by_sort_keys: Vec<PhysicalSortExpr>,
    /// Indices giving the ordering of partition columns
    ordered_partition_by_indices: Vec<usize>,
    /// Threshold (in rows) for flushing accumulated matches, derived from the
    /// session's `batch_size` execution option.
    flush_threshold: usize,

    /// Total number of buffered rows across `batches` (cheap running counter)
    buffered_rows: usize,
}

/// Custom metrics tracked for MATCH_RECOGNIZE execution per partition
#[derive(Clone, Debug)]
pub(crate) struct PatternMetrics {
    /// Baseline metrics (CPU time, output rows, etc.)
    baseline_metrics: BaselineMetrics,
    /// Number of RecordBatches consumed from the input
    pub(crate) input_batches: Count,
    /// Number of physical rows scanned by the matcher
    pub(crate) input_rows: Count,
    /// Number of logical matches emitted
    pub(crate) matches_emitted: Count,
    /// Number of DEFINE predicate evaluations
    pub(crate) define_pred_evals: Count,
    /// Number of NFA state transitions visited
    pub(crate) nfa_state_transitions: Count,
    /// Peak active NFA states encountered
    pub(crate) active_states_max: Gauge,
    /// Time spent inside PatternMatcher::process_batch()
    pub(crate) match_compute_time: Time,
    /// Sub-metric: time evaluating symbol predicates
    pub(crate) symbol_eval_time: Time,
    /// Sub-metric: time computing epsilon closures
    pub(crate) epsilon_eval_time: Time,
    /// Sub-metric: time spent slicing boolean input columns per partition
    pub(crate) slice_time: Time,
    /// Sub-metric: time spent inside the NFA scanning loop (scan_from)
    pub(crate) nfa_eval_time: Time,
    /// Sub-metric: time spent per row in scan_from outer loop
    pub(crate) row_loop_time: Time,
    /// Sub-metric: time spent enumerating NFA symbol transitions
    pub(crate) transition_time: Time,
    /// Sub-metric: time spent evaluating / materialising best-match candidates
    pub(crate) candidate_time: Time,
    /// Sub-metric: time spent allocating PathNode structures
    pub(crate) alloc_time: Time,
}

impl PatternMetrics {
    fn new(metrics: &ExecutionPlanMetricsSet, partition: usize) -> Self {
        let input_batches =
            MetricBuilder::new(metrics).counter("input_batches", partition);
        let input_rows = MetricBuilder::new(metrics).counter("input_rows", partition);
        let matches_emitted =
            MetricBuilder::new(metrics).counter("matches_emitted", partition);
        let define_pred_evals =
            MetricBuilder::new(metrics).counter("define_pred_evals", partition);
        let nfa_state_transitions =
            MetricBuilder::new(metrics).counter("nfa_state_transitions", partition);
        let active_states_max =
            MetricBuilder::new(metrics).gauge("nfa_active_states_max", partition);
        let match_compute_time =
            MetricBuilder::new(metrics).subset_time("match_compute_time", partition);

        let symbol_eval_time =
            MetricBuilder::new(metrics).subset_time("symbol_eval_time", partition);

        let epsilon_eval_time =
            MetricBuilder::new(metrics).subset_time("epsilon_eval_time", partition);

        let slice_time = MetricBuilder::new(metrics).subset_time("slice_time", partition);

        let nfa_eval_time =
            MetricBuilder::new(metrics).subset_time("nfa_eval_time", partition);

        let row_loop_time =
            MetricBuilder::new(metrics).subset_time("row_loop_time", partition);

        let transition_time =
            MetricBuilder::new(metrics).subset_time("transition_time", partition);

        let candidate_time =
            MetricBuilder::new(metrics).subset_time("candidate_time", partition);

        let alloc_time = MetricBuilder::new(metrics).subset_time("alloc_time", partition);

        Self {
            baseline_metrics: BaselineMetrics::new(metrics, partition),
            input_batches,
            input_rows,
            matches_emitted,
            define_pred_evals,
            nfa_state_transitions,
            active_states_max,
            match_compute_time,
            symbol_eval_time,
            epsilon_eval_time,
            slice_time,
            nfa_eval_time,
            row_loop_time,
            transition_time,
            candidate_time,
            alloc_time,
        }
    }
}

impl MatchRecognizePatternStream {
    pub(crate) fn try_new(
        input: SendableRecordBatchStream,
        schema: SchemaRef,
        compiled_pattern: Arc<CompiledPattern>,
        metrics: PatternMetrics,
        partition_by_sort_keys: Vec<PhysicalSortExpr>,
        ordered_partition_by_indices: Vec<usize>,
        flush_threshold: usize,
    ) -> Result<Self> {
        // Reuse the pre-compiled pattern across all partitions
        let pattern_matcher = PatternMatcher::new_with_metrics(
            compiled_pattern.clone(),
            schema.clone(),
            &metrics,
        )?;

        Ok(Self {
            input,
            schema,
            pattern_matcher,
            metrics,
            batches: Vec::new(),
            finished: false,
            partition_by_sort_keys,
            ordered_partition_by_indices,
            flush_threshold,
            buffered_rows: 0,
            pending_output: VecDeque::new(),
        })
    }

    /// Compute pattern matches for all accumulated batches, applying partitioning semantics
    fn compute_matches(&mut self, final_flush: bool) -> Result<()> {
        // Record compute time
        let _timer = self.metrics.baseline_metrics.elapsed_compute().timer();

        let concatenated =
            arrow::compute::concat_batches(&self.input.schema(), &self.batches)?;
        if concatenated.num_rows() == 0 {
            return Ok(());
        }

        // Evaluate partition key columns to determine partition ranges
        let sort_columns = self
            .ordered_partition_by_indices
            .iter()
            .map(|idx| {
                self.partition_by_sort_keys[*idx].evaluate_to_sort_column(&concatenated)
            })
            .collect::<Result<Vec<_>>>()?;

        let partitions =
            evaluate_partition_ranges(concatenated.num_rows(), &sort_columns)?;

        // Streaming matcher + accumulator. The threshold comes from the session's
        // configured batch size and is passed in when constructing the stream.
        let flush_threshold = self.flush_threshold;

        let mut acc = MatchAccumulator::new(
            concatenated.schema().clone(),
            self.pattern_matcher.compiled_arc(),
            flush_threshold,
        );

        let mut start = 0;
        let mut partitions_iter = partitions.into_iter().peekable();
        while let Some(part) = partitions_iter.next() {
            // If this is the last partition *and* final_flush == false, we keep it buffered
            if !final_flush && partitions_iter.peek().is_none() {
                // Retain unprocessed tail for next round
                let leftover = concatenated.slice(part.start, part.end - part.start);
                self.batches.clear();
                self.batches.push(leftover);
                self.buffered_rows = self.batches[0].num_rows();
                break;
            }

            let end = part.end;

            // Process partition rows; matcher uses absolute row numbers relative to `concatenated`
            self.pattern_matcher
                .process_rows(&concatenated, start, end, &mut acc)?;

            // Reset matcher for next logical partition
            self.pattern_matcher.finish_partition();

            start = end;

            // Flush if we reached threshold
            if acc.len() >= flush_threshold {
                if let Some(batch) = acc.flush(&concatenated)? {
                    self.pending_output.push_back(batch);
                    let m = acc.take_matches_total();
                    if m > 0 {
                        self.metrics.matches_emitted.add(m);
                    }
                }
            }
        }

        // Flush remaining rows after processing all partitions
        if let Some(batch) = acc.flush(&concatenated)? {
            self.pending_output.push_back(batch);
            let m = acc.take_matches_total();
            if m > 0 {
                self.metrics.matches_emitted.add(m);
            }
        }

        // All processed partitions handled, clear buffered batches & row count
        if final_flush {
            self.batches.clear();
            self.buffered_rows = 0;
        }

        Ok(())
    }

    /// Internal helper that contains the core polling logic. This is separated
    /// from `Stream::poll_next` so we can wrap the result with
    /// `baseline_metrics.record_poll`, mirroring the implementation in
    /// `BoundedWindowAggStream`.
    fn poll_next_inner(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<RecordBatch>>> {
        // First, if we already have pending output, return it immediately
        if let Some(batch) = self.pending_output.pop_front() {
            return Poll::Ready(Some(Ok(batch)));
        }

        if self.finished {
            return Poll::Ready(None);
        }

        loop {
            match ready!(self.input.poll_next_unpin(cx)) {
                Some(Ok(batch)) => {
                    // Update input metrics
                    self.metrics.input_batches.add(1);
                    self.metrics.input_rows.add(batch.num_rows());
                    self.buffered_rows += batch.num_rows();
                    self.batches.push(batch);

                    // If buffered rows exceed threshold, flush completed partitions
                    if self.buffered_rows >= self.flush_threshold {
                        if let Err(e) = self.compute_matches(false) {
                            return Poll::Ready(Some(Err(e)));
                        }
                        if let Some(output) = self.pending_output.pop_front() {
                            return Poll::Ready(Some(Ok(output)));
                        }
                    }
                    continue;
                }
                Some(Err(e)) => {
                    return Poll::Ready(Some(Err(e)));
                }
                None => {
                    // Input exhausted, compute matches once per partition
                    if let Err(e) = self.compute_matches(true) {
                        return Poll::Ready(Some(Err(e)));
                    }
                    self.finished = true;
                    if let Some(batch) = self.pending_output.pop_front() {
                        return Poll::Ready(Some(Ok(batch)));
                    } else {
                        return Poll::Ready(None);
                    }
                }
            }
        }
    }
}

impl Stream for MatchRecognizePatternStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let poll = self.poll_next_inner(cx);
        self.metrics.baseline_metrics.record_poll(poll)
    }
}

impl futures::stream::FusedStream for MatchRecognizePatternStream {
    fn is_terminated(&self) -> bool {
        self.finished && self.batches.is_empty()
    }
}

impl RecordBatchStream for MatchRecognizePatternStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}
