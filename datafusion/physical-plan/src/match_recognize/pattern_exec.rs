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

use arrow::datatypes::{Schema, SchemaRef};
use arrow::record_batch::RecordBatch;
use datafusion_common::{internal_err, Result};
use datafusion_expr::match_recognize::columns::MrMetadataColumn;
use datafusion_expr::match_recognize::{
    AfterMatchSkip, EmptyMatchesMode, MatchRecognizeOutputSpec,
    OutputSpecWithArrowSchema, Pattern, RowsPerMatch,
};
use datafusion_physical_expr::equivalence::ProjectionMapping;
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
use crate::match_recognize::matcher::PatternMatcher;
use datafusion_execution::RecordBatchStream;
use futures::{
    ready,
    stream::{Stream, StreamExt},
};

use crate::windows::{get_ordered_partition_by_indices, get_partition_by_sort_exprs};
use datafusion_common::utils::evaluate_partition_ranges;
use datafusion_physical_expr_common::sort_expr::{
    OrderingRequirements, PhysicalSortExpr,
};

use crate::match_recognize::nfa::PathStep;
use arrow::array::{
    ArrayBuilder, BooleanBuilder, StringBuilder, UInt32Builder, UInt64Builder,
};
use arrow::compute::take_record_batch;

// Small helper to project a RecordBatch by column indices
fn project_record_batch_columns(
    batch: &RecordBatch,
    indices: &[usize],
) -> Result<RecordBatch> {
    let proj_fields: Vec<Arc<arrow::datatypes::Field>> = indices
        .iter()
        .map(|&i| Arc::clone(&batch.schema().fields()[i]))
        .collect();
    let proj_schema = Arc::new(Schema::new_with_metadata(
        proj_fields,
        batch.schema().metadata().clone(),
    ));
    let proj_arrays: Vec<Arc<dyn arrow::array::Array>> = indices
        .iter()
        .map(|&i| Arc::clone(batch.column(i)))
        .collect();
    Ok(RecordBatch::try_new(proj_schema, proj_arrays)?)
}

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
    after_match_skip: AfterMatchSkip,
    /// Pre-compiled pattern shared across partitions
    compiled_pattern: Arc<CompiledPattern>,
    /// Output projection/pruning specification (passthrough, metadata, bitsets)
    output_spec: MatchRecognizeOutputSpec,
    /// Baseline metrics
    metrics: ExecutionPlanMetricsSet,
    /// Cache for the properties
    cache: crate::execution_plan::PlanProperties,
    /// Whether this operator preserves per-partition input row order
    maintains_input_order: bool,
    /// Indices that define ordering of PARTITION BY columns w.r.t the input ordering
    ordered_partition_by_indices: Vec<usize>,
}

impl MatchRecognizePatternExec {
    /// Create a new MatchRecognizePatternExec
    #[allow(clippy::too_many_arguments)]
    pub fn try_new(
        input: Arc<dyn ExecutionPlan>,
        partition_by: Vec<Arc<dyn PhysicalExpr>>,
        order_by: Option<LexOrdering>,
        pattern: Pattern,
        symbols: Vec<String>,
        after_match_skip: AfterMatchSkip,
        rows_per_match: RowsPerMatch,
        output_spec: MatchRecognizeOutputSpec,
    ) -> Result<Self> {
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
            output_spec,
        )
    }

    fn try_new_with_compiled_pattern(
        input: Arc<dyn ExecutionPlan>,
        partition_by: Vec<Arc<dyn PhysicalExpr>>,
        order_by: Option<LexOrdering>,
        symbols: Vec<String>,
        after_match_skip: AfterMatchSkip,
        compiled_pattern: Arc<CompiledPattern>,
        output_spec: MatchRecognizeOutputSpec,
    ) -> Result<Self> {
        let input_schema = input.schema();
        // Build final output schema from output spec
        let schema = output_spec.build_arrow_schema(&input_schema);

        // Determine if the operator maintains input order
        let emits_unmatched = matches!(
            compiled_pattern.empty_matches_mode,
            EmptyMatchesMode::WithUnmatched
        );
        let maintains_input_order =
            matches!(after_match_skip, AfterMatchSkip::PastLastRow) && !emits_unmatched;

        let cache = Self::compute_properties(
            &input,
            &schema,
            &partition_by,
            &order_by,
            &output_spec.metadata_columns,
            maintains_input_order,
            &compiled_pattern,
            &output_spec.passthrough_input_indices,
        )?;

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
            maintains_input_order,
            ordered_partition_by_indices,
            output_spec,
        })
    }

    /// Compute the properties for this execution plan
    #[allow(clippy::too_many_arguments)]
    fn compute_properties(
        input: &Arc<dyn ExecutionPlan>,
        schema: &SchemaRef,
        partition_by: &[Arc<dyn PhysicalExpr>],
        order_by: &Option<LexOrdering>,
        metadata_columns: &[MrMetadataColumn],
        maintains_input_order: bool,
        compiled_pattern: &Arc<CompiledPattern>,
        passthrough_input_indices: &[usize],
    ) -> Result<crate::execution_plan::PlanProperties> {
        let eq_properties = mr_equivalence_properties(
            schema,
            input,
            partition_by,
            order_by,
            metadata_columns,
            maintains_input_order,
            compiled_pattern,
            passthrough_input_indices,
        )?;

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

/// Build equivalence properties for MATCH_RECOGNIZE, modeled after `window_equivalence_properties`.
#[allow(clippy::too_many_arguments)]
fn mr_equivalence_properties(
    schema: &SchemaRef,
    input: &Arc<dyn ExecutionPlan>,
    partition_by: &[Arc<dyn PhysicalExpr>],
    order_by: &Option<LexOrdering>,
    metadata_columns: &[MrMetadataColumn],
    maintains_input_order: bool,
    compiled_pattern: &Arc<CompiledPattern>,
    passthrough_input_indices: &[usize],
) -> Result<EquivalenceProperties> {
    // Project input equivalence properties to this node's output schema using passthrough mapping.
    let input_schema = input.schema();
    let mapping =
        ProjectionMapping::from_indices(passthrough_input_indices, &input_schema)?;
    let mut projected = input
        .equivalence_properties()
        .project(&mapping, Arc::clone(schema));
    // Preserve inherited orderings only when MR maintains input order
    if !maintains_input_order {
        projected.clear_orderings();
    }
    let mut eq_properties =
        EquivalenceProperties::new(Arc::clone(schema)).extend(projected)?;

    // Resolve PARTITION BY keys that actually preserve input ordering (from child schema),
    // then project them to the MR output schema.
    let ordered_partition_by_indices =
        get_ordered_partition_by_indices(partition_by, input)?;
    let partition_prefix_in =
        get_partition_by_sort_exprs(input, partition_by, &ordered_partition_by_indices)?;

    // Projection helper for sort exprs; returns None if any expr cannot be projected.
    let input_eq_properties = input.equivalence_properties();
    let project_sort_exprs =
        |exprs: Vec<PhysicalSortExpr>| -> Option<Vec<PhysicalSortExpr>> {
            let mut result = Vec::with_capacity(exprs.len());
            for se in exprs {
                let maybe = input_eq_properties.project_expr(&se.expr, &mapping);
                match maybe {
                    Some(proj_expr) => {
                        result.push(PhysicalSortExpr::new(proj_expr, se.options))
                    }
                    None => return None,
                }
            }
            Some(result)
        };

    let partition_prefix = project_sort_exprs(partition_prefix_in).unwrap_or_default();

    // Determine admissible suffix orderings specific to MR.
    let is_order_by_admissible = maintains_input_order
        && order_by.as_ref().map(|v| !v.is_empty()).unwrap_or(false);
    let order_by_suffix: Vec<PhysicalSortExpr> = if is_order_by_admissible {
        match order_by.clone() {
            Some(lex) => {
                project_sort_exprs(Vec::<PhysicalSortExpr>::from(lex)).unwrap_or_default()
            }
            None => Vec::new(),
        }
    } else {
        Vec::new()
    };

    let emits_unmatched = matches!(
        compiled_pattern.empty_matches_mode,
        EmptyMatchesMode::WithUnmatched
    );
    let is_match_number_projected =
        metadata_columns.contains(&MrMetadataColumn::MatchNumber);
    let is_match_sequence_projected =
        metadata_columns.contains(&MrMetadataColumn::MatchSequenceNumber);
    let is_match_number_admissible = is_match_number_projected && !emits_unmatched;
    let is_match_sequence_admissible = is_match_sequence_projected && !emits_unmatched;

    let match_number_sort = if is_match_number_admissible {
        Some(
            PhysicalSortExpr::new_default(col(
                MrMetadataColumn::MatchNumber.as_ref(),
                schema,
            )?)
            .nulls_last(),
        )
    } else {
        None
    };
    let match_sequence_sort = if is_match_sequence_admissible {
        Some(
            PhysicalSortExpr::new_default(col(
                MrMetadataColumn::MatchSequenceNumber.as_ref(),
                schema,
            )?)
            .nulls_last(),
        )
    } else {
        None
    };

    // Build suffix candidates in the style of WindowAgg (operator-specific ordering tails).
    let mut suffix_orderings: Vec<Vec<PhysicalSortExpr>> = Vec::new();
    if is_order_by_admissible && !order_by_suffix.is_empty() {
        suffix_orderings.push(order_by_suffix);
    }

    if let Some(mn) = match_number_sort.clone() {
        if let Some(msn) = match_sequence_sort.clone() {
            suffix_orderings.push(vec![mn, msn]);
        } else {
            suffix_orderings.push(vec![mn]);
        }
    }

    // If no explicit suffix and we have a non-empty prefix, advertise just the prefix.
    if suffix_orderings.is_empty() && !partition_prefix.is_empty() {
        suffix_orderings.push(Vec::new());
    }

    // Prefix all admissible suffixes with the partition prefix and register them.
    for tail in suffix_orderings {
        let mut full: Vec<PhysicalSortExpr> =
            Vec::with_capacity(partition_prefix.len() + tail.len());
        full.extend(partition_prefix.iter().cloned());
        full.extend(tail.into_iter());
        if !full.is_empty() {
            eq_properties.add_ordering(full);
        }
    }

    Ok(eq_properties)
}

impl DisplayAs for MatchRecognizePatternExec {
    fn fmt_as(
        &self,
        _t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        if !self.output_spec.is_empty() {
            write!(
                f,
                "{}: pattern=[{}] {}",
                Self::static_name(),
                self.compiled_pattern.pattern,
                OutputSpecWithArrowSchema::new(&self.output_spec, &self.input.schema(),),
            )?;
        } else {
            write!(
                f,
                "{}: pattern=[{}]",
                Self::static_name(),
                self.compiled_pattern.pattern,
            )?;
        }

        Ok(())
    }
}

impl ExecutionPlan for MatchRecognizePatternExec {
    fn name(&self) -> &'static str {
        Self::static_name()
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
        let new_input = Arc::clone(&children[0]);
        Ok(Arc::new(Self::try_new_with_compiled_pattern(
            new_input,
            self.partition_by.clone(),
            self.order_by.clone(),
            self.symbols.clone(),
            self.after_match_skip.clone(),
            Arc::clone(&self.compiled_pattern),
            self.output_spec.clone(),
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
            Arc::clone(&self.schema),
            Arc::clone(&self.compiled_pattern),
            metrics,
            partition_by_sort_keys,
            self.ordered_partition_by_indices.clone(),
            flush_threshold,
            self.output_spec.clone(),
        )?;

        Ok(Box::pin(stream))
    }

    fn partition_statistics(&self, partition: Option<usize>) -> Result<Statistics> {
        // Get input statistics first
        let input_stats = self.input.partition_statistics(partition)?;

        // Project input stats to passthrough columns to align with MR output schema
        let mut column_statistics: Vec<ColumnStatistics> = Vec::new();
        let in_cols = input_stats.column_statistics.len();
        for &idx in &self.output_spec.passthrough_input_indices {
            if idx < in_cols {
                column_statistics.push(input_stats.column_statistics[idx].clone());
            } else {
                column_statistics.push(ColumnStatistics::new_unknown());
            }
        }

        // Append virtual columns that will be materialized according to
        // the selected metadata subset
        for _ in 0..self.output_spec.metadata_columns.len() {
            column_statistics.push(ColumnStatistics::new_unknown());
        }

        // Append statistics for classifier bitset columns (one per selected symbol)
        for _ in 0..self.output_spec.classifier_bitset_symbols.len() {
            column_statistics.push(ColumnStatistics::new_unknown());
        }

        // Build new statistics aligned to MR output schema.
        // Best guess for number of rows and total size is to use those of the input.
        let stats = Statistics {
            num_rows: input_stats.num_rows,
            total_byte_size: input_stats.total_byte_size,
            column_statistics,
        };

        // Since pattern execution can change cardinality unpredictably, mark stats as inexact
        Ok(stats.to_inexact())
    }

    fn cardinality_effect(&self) -> CardinalityEffect {
        match self.after_match_skip {
            AfterMatchSkip::PastLastRow => CardinalityEffect::LowerEqual,
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
            vec![crate::windows::calc_requirements(partition_bys, order_keys)]
        } else {
            let reordered: Vec<_> = self
                .ordered_partition_by_indices
                .iter()
                .map(|idx| Arc::clone(&partition_bys[*idx]))
                .collect();
            vec![crate::windows::calc_requirements(&reordered, order_keys)]
        }
    }

    fn required_input_distribution(&self) -> Vec<crate::Distribution> {
        let keys = self.partition_keys();
        if keys.is_empty() {
            vec![crate::Distribution::SinglePartition]
        } else {
            vec![crate::Distribution::HashPartitioned(keys)]
        }
    }

    /// Indicates when this operator preserves the per-partition input row order.
    ///
    /// Order is preserved only if:
    /// - AFTER MATCH SKIP is `PastLastRow` (no backtracking/overlap), and
    /// - the rows-per-match mode does NOT emit unmatched rows
    ///   (`EmptyMatchesMode::WithUnmatched` would append extra rows and disturb order).
    ///
    /// In all other cases, emitted rows may not follow the exact input order.
    fn maintains_input_order(&self) -> Vec<bool> {
        vec![self.maintains_input_order]
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
    // Re-usable Arrow builders for metadata columns (created only if selected)
    class_builder: Option<StringBuilder>,
    match_num_builder: Option<UInt64Builder>,
    seq_builder: Option<UInt64Builder>,
    // One boolean builder per classifier symbol, in declaration order
    classifier_bits_builders: Vec<BooleanBuilder>,
    // Cached mapping from builder index to compiled symbol id
    classifier_symbol_ids: Vec<usize>,
    // Physical row indices that will be fetched from the input RecordBatch
    indices_builder: UInt32Builder,
    // Bitmap tracking which physical rows were already part of a match (for WITH UNMATCHED ROWS)
    seen_rows: Vec<bool>,
    // Count of logical matches recorded since the last flush
    matches_emitted: usize,
    // Cached output schema (input + selected virtual columns)
    output_schema: SchemaRef,
    // Compiled pattern (used for classifier string lookup and flags)
    compiled_pattern: Arc<CompiledPattern>,
    // Output projection/pruning specification (passthrough, metadata, bitsets)
    output_spec: MatchRecognizeOutputSpec,
}

impl MatchAccumulator {
    /// Create a new accumulator with empty builders and buffers.
    pub fn try_new(
        output_schema: SchemaRef,
        compiled_pattern: Arc<CompiledPattern>,
        flush_threshold: usize,
        output_spec: MatchRecognizeOutputSpec,
    ) -> Result<Self> {
        // Determine which metadata columns are selected
        let has_classifier = output_spec
            .metadata_columns
            .contains(&MrMetadataColumn::Classifier);
        let has_match_number = output_spec
            .metadata_columns
            .contains(&MrMetadataColumn::MatchNumber);
        let has_sequence_number = output_spec
            .metadata_columns
            .contains(&MrMetadataColumn::MatchSequenceNumber);

        // Map selected classifier symbol names to compiled ids in the same order
        let name_to_id: std::collections::HashMap<String, usize> = compiled_pattern
            .symbols_iter()
            .map(|(id, name)| (name.to_string(), id))
            .collect();
        let mut classifier_symbol_ids: Vec<usize> =
            Vec::with_capacity(output_spec.classifier_bitset_symbols.len());
        for sym in &output_spec.classifier_bitset_symbols {
            match name_to_id.get(sym) {
                Some(&id) => classifier_symbol_ids.push(id),
                None => {
                    return internal_err!(
                        "Unknown classifier bitset symbol '{sym}' requested in MATCH_RECOGNIZE output; declared symbols: {:?}",
                        compiled_pattern
                            .symbols_iter()
                            .map(|(_, s)| s)
                            .collect::<Vec<_>>()
                    );
                }
            }
        }

        Ok(Self {
            class_builder: if has_classifier {
                Some(StringBuilder::new())
            } else {
                None
            },
            match_num_builder: if has_match_number {
                Some(UInt64Builder::new())
            } else {
                None
            },
            seq_builder: if has_sequence_number {
                Some(UInt64Builder::new())
            } else {
                None
            },
            classifier_bits_builders: output_spec
                .classifier_bitset_symbols
                .iter()
                .map(|_| BooleanBuilder::new())
                .collect(),
            classifier_symbol_ids,
            indices_builder: UInt32Builder::with_capacity(flush_threshold + 1),
            seen_rows: Vec::new(),
            matches_emitted: 0,
            output_schema,
            compiled_pattern,
            output_spec,
        })
    }

    /// Current number of accumulated rows.
    #[inline]
    pub fn len(&self) -> usize {
        self.indices_builder.len()
    }

    /// Append the metadata for a single logical match.
    ///
    /// `match_id` – sequential match number (1-based).
    /// `path`     – ordered list of (row, symbol) steps returned by the matcher.
    /// `num_input_rows` – total rows in the input slice being processed.  Rows ≥ this
    ///                    value are virtual (e.g. anchor predicates) and are ignored.
    pub fn record_match(
        &mut self,
        match_id: u64,
        path: &[PathStep],
        num_input_rows: usize,
    ) -> Result<()> {
        // Defer incrementing the logical match counter until we know
        // whether this match produced at least one output row.
        // Greedy matcher guarantees `path` is already in forward order.
        let mut seq_counter: u64 = 0;
        let compiled = &self.compiled_pattern;
        let omit_empty_matches =
            matches!(compiled.empty_matches_mode, EmptyMatchesMode::Omit);
        let mut emitted_any_row = false;

        for step in path.iter() {
            // Skip virtual positions beyond the physical input.
            if step.row >= num_input_rows {
                continue;
            }

            // Determine whether this step represents an empty match row
            let is_empty_symbol =
                step.sym.to_index() == crate::match_recognize::nfa::Sym::Empty.to_index();

            // Always advance the per-match sequence counter for every logical step
            // to preserve numbering semantics when EMPTY MATCH rows are omitted.
            if self.seq_builder.is_some() {
                seq_counter = seq_counter.saturating_add(1);
            }

            // In OMIT mode, do not emit rows corresponding to EMPTY symbol steps
            if omit_empty_matches && is_empty_symbol {
                continue;
            }

            self.indices_builder.append_value(step.row as u32);
            emitted_any_row = true;

            // Track seen rows for WITH UNMATCHED ROWS handling
            if matches!(
                self.compiled_pattern.empty_matches_mode,
                EmptyMatchesMode::WithUnmatched
            ) {
                if step.row >= self.seen_rows.len() {
                    self.seen_rows.resize(step.row + 1, false);
                }
                self.seen_rows[step.row] = true;
            }

            // CLASSIFIER column (only if selected)
            if let Some(builder) = &mut self.class_builder {
                // CLASSIFIER is NULL for Empty symbol and for unmatched rows (handled in flush)
                if is_empty_symbol {
                    builder.append_null();
                } else {
                    match compiled.id_to_symbol[step.sym.to_index()].as_deref() {
                        Some(name) => builder.append_value(name),
                        None => {
                            return internal_err!(
                                "Unknown classifier symbol id {}",
                                step.sym.to_index()
                            );
                        }
                    }
                }
            }

            // Per-symbol classifier bitsets: true if classified as that symbol, else false
            // The builders align with `output_spec.classifier_bitset_symbols`.
            if !self.output_spec.classifier_bitset_symbols.is_empty() {
                let current_sym_index = step.sym.to_index();
                for (i, bits_builder) in
                    self.classifier_bits_builders.iter_mut().enumerate()
                {
                    let expected_sym_id = self.classifier_symbol_ids[i];
                    bits_builder.append_value(current_sym_index == expected_sym_id);
                }
            }

            // MATCH_NUMBER column
            if let Some(builder) = &mut self.match_num_builder {
                builder.append_value(match_id);
            }

            // MATCH_SEQUENCE_NUMBER column
            if let Some(builder) = &mut self.seq_builder {
                builder.append_value(seq_counter);
            }
        }
        if emitted_any_row {
            // Count only matches that resulted in at least one output row
            self.matches_emitted += 1;
        }
        Ok(())
    }

    /// Flush accumulated rows into a single `RecordBatch`.
    ///
    /// Returns `Ok(None)` if no rows are buffered.
    pub fn flush(&mut self, input_batch: &RecordBatch) -> Result<Option<RecordBatch>> {
        let num_rows = input_batch.num_rows();

        // WITH UNMATCHED ROWS – append synthetic rows that were not part of any match.
        if matches!(
            self.compiled_pattern.empty_matches_mode,
            EmptyMatchesMode::WithUnmatched
        ) {
            if self.seen_rows.len() < num_rows {
                self.seen_rows.resize(num_rows, false);
            }
            for phys_idx in 0..num_rows {
                if !self.seen_rows[phys_idx] {
                    self.indices_builder.append_value(phys_idx as u32);
                    if let Some(builder) = &mut self.class_builder {
                        builder.append_null();
                    }
                    // For unmatched rows, all classifier bitsets are false
                    for bits_builder in &mut self.classifier_bits_builders {
                        bits_builder.append_value(false);
                    }
                    if let Some(builder) = &mut self.match_num_builder {
                        builder.append_null();
                    }
                    if let Some(builder) = &mut self.seq_builder {
                        builder.append_null();
                    }
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

        // Project columns first (if requested), then take rows
        let data_part = {
            let projected = project_record_batch_columns(
                input_batch,
                &self.output_spec.passthrough_input_indices,
            )?;
            take_record_batch(&projected, &indices_array)?
        };

        let data_cols_len = data_part.num_columns();
        let mut final_columns: Vec<Arc<dyn arrow::array::Array>> =
            Vec::with_capacity(data_cols_len + self.output_spec.metadata_columns.len());
        final_columns.extend(data_part.columns().iter().cloned());

        // Finish only selected builders and append arrays following the selected order
        // Note: finishing resets the builders for reuse in the next accumulation cycle
        for col in &self.output_spec.metadata_columns {
            match col {
                MrMetadataColumn::Classifier => {
                    if let Some(builder) = &mut self.class_builder {
                        final_columns.push(Arc::new(builder.finish()));
                    }
                }
                MrMetadataColumn::MatchNumber => {
                    if let Some(builder) = &mut self.match_num_builder {
                        final_columns.push(Arc::new(builder.finish()));
                    }
                }
                MrMetadataColumn::MatchSequenceNumber => {
                    if let Some(builder) = &mut self.seq_builder {
                        final_columns.push(Arc::new(builder.finish()));
                    }
                }
            }
        }

        // Append classifier bitset columns in declaration order
        for builder in &mut self.classifier_bits_builders {
            final_columns.push(Arc::new(builder.finish()));
        }

        let output_batch =
            RecordBatch::try_new(Arc::clone(&self.output_schema), final_columns)?;

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

    /// Parsed metadata mask and order
    output_spec: MatchRecognizeOutputSpec,
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
        output_spec: MatchRecognizeOutputSpec,
    ) -> Result<Self> {
        #![allow(clippy::too_many_arguments)]
        // Align with WindowAggStream::new: require all PARTITION BY columns to be ordered
        if partition_by_sort_keys.len() != ordered_partition_by_indices.len() {
            return internal_err!("All partition by columns should have an ordering");
        }
        // Reuse the pre-compiled pattern across all partitions. The matcher needs to resolve
        // DEFINE-derived symbol columns (e.g. __mr_symbol_*), which live on the input stream's
        // schema (child of this exec), not on the exec's output schema. Use the input schema here.
        let pattern_matcher = PatternMatcher::new_with_metrics(
            Arc::clone(&compiled_pattern),
            &input.schema(),
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
            output_spec,
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

        let mut acc = MatchAccumulator::try_new(
            Arc::clone(&self.schema),
            self.pattern_matcher.compiled_arc(),
            flush_threshold,
            self.output_spec.clone(),
        )?;

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
        Arc::clone(&self.schema)
    }
}
