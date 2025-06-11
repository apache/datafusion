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
use crate::metrics::{BaselineMetrics, ExecutionPlanMetricsSet};
use crate::{DisplayAs, DisplayFormatType, ExecutionPlan, SendableRecordBatchStream};

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
        let compiled_pattern = Arc::new(CompiledPattern::compile(
            pattern.clone(),
            symbols.clone(),
            after_match_skip.clone(),
            rows_per_match.clone(),
        )?);

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
        let input_stream = self.input.execute(partition, context)?;
        let baseline_metrics = BaselineMetrics::new(&self.metrics, partition);

        let partition_by_sort_keys = self.partition_by_sort_keys()?;
        let stream = MatchRecognizePatternStream::try_new(
            input_stream,
            self.schema.clone(),
            self.compiled_pattern.clone(),
            baseline_metrics,
            partition_by_sort_keys,
            self.ordered_partition_by_indices.clone(),
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

    // fn maintains_input_order(&self) -> Vec<bool> {
    //     vec![true]
    // }
}

/// Stream for MatchRecognizePatternExec
pub struct MatchRecognizePatternStream {
    /// Input stream
    input: SendableRecordBatchStream,
    /// Output schema
    schema: SchemaRef,
    /// Pattern matcher
    pattern_matcher: PatternMatcher,
    /// Baseline metrics
    baseline_metrics: BaselineMetrics,
    /// Accumulated input batches
    batches: Vec<RecordBatch>,
    /// Whether the stream is finished
    finished: bool,
    /// Pre-computed sort expressions for PARTITION BY columns
    partition_by_sort_keys: Vec<PhysicalSortExpr>,
    /// Indices giving the ordering of partition columns
    ordered_partition_by_indices: Vec<usize>,
}

impl MatchRecognizePatternStream {
    pub fn try_new(
        input: SendableRecordBatchStream,
        schema: SchemaRef,
        compiled_pattern: Arc<CompiledPattern>,
        baseline_metrics: BaselineMetrics,
        partition_by_sort_keys: Vec<PhysicalSortExpr>,
        ordered_partition_by_indices: Vec<usize>,
    ) -> Result<Self> {
        // Reuse the pre-compiled pattern across all partitions
        let pattern_matcher =
            PatternMatcher::new(compiled_pattern.clone(), schema.clone())?;

        Ok(Self {
            input,
            schema,
            pattern_matcher,
            baseline_metrics,
            batches: Vec::new(),
            finished: false,
            partition_by_sort_keys,
            ordered_partition_by_indices,
        })
    }

    /// Compute pattern matches for all accumulated batches, applying partitioning semantics
    fn compute_matches(&mut self) -> Result<Option<RecordBatch>> {
        // Record compute time
        let _timer = self.baseline_metrics.elapsed_compute().timer();

        let concatenated =
            arrow::compute::concat_batches(&self.input.schema(), &self.batches)?;
        if concatenated.num_rows() == 0 {
            return Ok(None);
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

        let mut result_batches = Vec::new();
        for part in partitions {
            let len = part.end - part.start;
            let slice = concatenated.slice(part.start, len);
            self.pattern_matcher.reset();
            let mut matches = self.pattern_matcher.process_batch(slice)?;
            result_batches.append(&mut matches);
        }

        if result_batches.is_empty() {
            return Ok(None);
        }

        let combined = arrow::compute::concat_batches(&self.schema, &result_batches)?;
        Ok(Some(combined))
    }
}

impl Stream for MatchRecognizePatternStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        if self.finished {
            return Poll::Ready(None);
        }

        loop {
            match ready!(self.input.poll_next_unpin(cx)) {
                Some(Ok(batch)) => {
                    self.batches.push(batch);
                    continue;
                }
                Some(Err(e)) => {
                    return Poll::Ready(Some(Err(e)));
                }
                None => {
                    // Input exhausted, compute matches once per partition
                    let result = self.compute_matches();
                    self.finished = true;
                    return match result {
                        Ok(Some(batch)) => Poll::Ready(Some(Ok(batch))),
                        Ok(None) => Poll::Ready(None),
                        Err(e) => Poll::Ready(Some(Err(e))),
                    };
                }
            }
        }
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
