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

//! Hash aggregation

use std::sync::Arc;
use std::task::{Context, Poll};
use std::vec;

use super::AggregateExec;
use super::order::GroupOrdering;
use crate::aggregates::group_values::{GroupByMetrics, GroupValues, new_group_values};
use crate::aggregates::order::GroupOrderingFull;
use crate::aggregates::{
    AggregateMode, PhysicalGroupBy, create_schema, evaluate_group_by, evaluate_many,
    evaluate_optional,
};
use crate::metrics::{BaselineMetrics, MetricBuilder, RecordOutput};
use crate::sorts::sort::sort_batch;
use crate::sorts::streaming_merge::{SortedSpillFile, StreamingMergeBuilder};
use crate::spill::spill_manager::SpillManager;
use crate::{PhysicalExpr, aggregates, metrics};
use crate::{RecordBatchStream, SendableRecordBatchStream};

use arrow::array::*;
use arrow::datatypes::SchemaRef;
use datafusion_common::{
    DataFusionError, Result, assert_eq_or_internal_err, assert_or_internal_err,
    internal_err,
};
use datafusion_execution::TaskContext;
use datafusion_execution::memory_pool::proxy::VecAllocExt;
use datafusion_execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion_expr::{EmitTo, GroupsAccumulator};
use datafusion_physical_expr::aggregate::AggregateFunctionExpr;
use datafusion_physical_expr::expressions::Column;
use datafusion_physical_expr::{GroupsAccumulatorAdapter, PhysicalSortExpr};
use datafusion_physical_expr_common::sort_expr::LexOrdering;

use datafusion_common::instant::Instant;
use futures::ready;
use futures::stream::{Stream, StreamExt};
use log::debug;

#[derive(Debug, Clone)]
/// This object tracks the aggregation phase (input/output)
pub(crate) enum ExecutionState {
    ReadingInput,
    /// When producing output, the remaining rows to output are stored
    /// here and are sliced off as needed in batch_size chunks
    ProducingOutput(RecordBatch),
    /// Produce intermediate aggregate state for each input row without
    /// aggregation.
    ///
    /// See "partial aggregation" discussion on [`GroupedHashAggregateStream`]
    SkippingAggregation,
    /// All input has been consumed and all groups have been emitted
    Done,
}

/// This encapsulates the spilling state
struct SpillState {
    // ========================================================================
    // PROPERTIES:
    // These fields are initialized at the start and remain constant throughout
    // the execution.
    // ========================================================================
    /// Sorting expression for spilling batches
    spill_expr: LexOrdering,

    /// Schema for spilling batches
    spill_schema: SchemaRef,

    /// aggregate_arguments for merging spilled data
    merging_aggregate_arguments: Vec<Vec<Arc<dyn PhysicalExpr>>>,

    /// GROUP BY expressions for merging spilled data
    merging_group_by: PhysicalGroupBy,

    /// Manages the process of spilling and reading back intermediate data
    spill_manager: SpillManager,

    // ========================================================================
    // STATES:
    // Fields changes during execution. Can be buffer, or state flags that
    // influence the execution in parent `GroupedHashAggregateStream`
    // ========================================================================
    /// If data has previously been spilled, the locations of the
    /// spill files (in Arrow IPC format)
    spills: Vec<SortedSpillFile>,

    /// true when streaming merge is in progress
    is_stream_merging: bool,

    // ========================================================================
    // METRICS:
    // ========================================================================
    /// Peak memory used for buffered data.
    /// Calculated as sum of peak memory values across partitions
    peak_mem_used: metrics::Gauge,
    // Metrics related to spilling are managed inside `spill_manager`
}

/// Tracks if the aggregate should skip partial aggregations
///
/// See "partial aggregation" discussion on [`GroupedHashAggregateStream`]
struct SkipAggregationProbe {
    // ========================================================================
    // PROPERTIES:
    // These fields are initialized at the start and remain constant throughout
    // the execution.
    // ========================================================================
    /// Aggregation ratio check performed when the number of input rows exceeds
    /// this threshold (from `SessionConfig`)
    probe_rows_threshold: usize,
    /// Maximum ratio of `num_groups` to `input_rows` for continuing aggregation
    /// (from `SessionConfig`). If the ratio exceeds this value, aggregation
    /// is skipped and input rows are directly converted to output
    probe_ratio_threshold: f64,

    // ========================================================================
    // STATES:
    // Fields changes during execution. Can be buffer, or state flags that
    // influence the execution in parent `GroupedHashAggregateStream`
    // ========================================================================
    /// Number of processed input rows (updated during probing)
    input_rows: usize,
    /// Number of total group values for `input_rows` (updated during probing)
    num_groups: usize,

    /// Flag indicating further data aggregation may be skipped (decision made
    /// when probing complete)
    should_skip: bool,
    /// Flag indicating further updates of `SkipAggregationProbe` state won't
    /// make any effect (set either while probing or on probing completion)
    is_locked: bool,

    // ========================================================================
    // METRICS:
    // ========================================================================
    /// Number of rows where state was output without aggregation.
    ///
    /// * If 0, all input rows were aggregated (should_skip was always false)
    ///
    /// * if greater than zero, the number of rows which were output directly
    ///   without aggregation
    skipped_aggregation_rows: metrics::Count,
}

impl SkipAggregationProbe {
    fn new(
        probe_rows_threshold: usize,
        probe_ratio_threshold: f64,
        skipped_aggregation_rows: metrics::Count,
    ) -> Self {
        Self {
            input_rows: 0,
            num_groups: 0,
            probe_rows_threshold,
            probe_ratio_threshold,
            should_skip: false,
            is_locked: false,
            skipped_aggregation_rows,
        }
    }

    /// Updates `SkipAggregationProbe` state:
    /// - increments the number of input rows
    /// - replaces the number of groups with the new value
    /// - on `probe_rows_threshold` exceeded calculates
    ///   aggregation ratio and sets `should_skip` flag
    /// - if `should_skip` is set, locks further state updates
    fn update_state(&mut self, input_rows: usize, num_groups: usize) {
        if self.is_locked {
            return;
        }
        self.input_rows += input_rows;
        self.num_groups = num_groups;
        if self.input_rows >= self.probe_rows_threshold {
            self.should_skip = self.num_groups as f64 / self.input_rows as f64
                >= self.probe_ratio_threshold;
            // Set is_locked to true only if we have decided to skip, otherwise we can try to skip
            // during processing the next record_batch.
            self.is_locked = self.should_skip;
        }
    }

    fn should_skip(&self) -> bool {
        self.should_skip
    }

    /// Record the number of rows that were output directly without aggregation
    fn record_skipped(&mut self, batch: &RecordBatch) {
        self.skipped_aggregation_rows.add(batch.num_rows());
    }
}

/// Controls the behavior when an out-of-memory condition occurs.
#[derive(PartialEq, Debug)]
enum OutOfMemoryMode {
    /// When out of memory occurs, spill state to disk
    Spill,
    /// When out of memory occurs, attempt to emit group values early
    EmitEarly,
    /// When out of memory occurs, immediately report the error
    ReportError,
}

/// HashTable based Grouping Aggregator
///
/// # Design Goals
///
/// This structure is designed so that updating the aggregates can be
/// vectorized (done in a tight loop) without allocations. The
/// accumulator state is *not* managed by this operator (e.g in the
/// hash table) and instead is delegated to the individual
/// accumulators which have type specialized inner loops that perform
/// the aggregation.
///
/// # Architecture
///
/// ```text
///
///     Assigns a consecutive group           internally stores aggregate values
///     index for each unique set                     for all groups
///         of group values
///
///         ┌────────────┐              ┌──────────────┐       ┌──────────────┐
///         │ ┌────────┐ │              │┌────────────┐│       │┌────────────┐│
///         │ │  "A"   │ │              ││accumulator ││       ││accumulator ││
///         │ ├────────┤ │              ││     0      ││       ││     N      ││
///         │ │  "Z"   │ │              ││ ┌────────┐ ││       ││ ┌────────┐ ││
///         │ └────────┘ │              ││ │ state  │ ││       ││ │ state  │ ││
///         │            │              ││ │┌─────┐ │ ││  ...  ││ │┌─────┐ │ ││
///         │    ...     │              ││ │├─────┤ │ ││       ││ │├─────┤ │ ││
///         │            │              ││ │└─────┘ │ ││       ││ │└─────┘ │ ││
///         │            │              ││ │        │ ││       ││ │        │ ││
///         │ ┌────────┐ │              ││ │  ...   │ ││       ││ │  ...   │ ││
///         │ │  "Q"   │ │              ││ │        │ ││       ││ │        │ ││
///         │ └────────┘ │              ││ │┌─────┐ │ ││       ││ │┌─────┐ │ ││
///         │            │              ││ │└─────┘ │ ││       ││ │└─────┘ │ ││
///         └────────────┘              ││ └────────┘ ││       ││ └────────┘ ││
///                                     │└────────────┘│       │└────────────┘│
///                                     └──────────────┘       └──────────────┘
///
///         group_values                             accumulators
///
///  ```
///
/// For example, given a query like `COUNT(x), SUM(y) ... GROUP BY z`,
/// [`group_values`] will store the distinct values of `z`. There will
/// be one accumulator for `COUNT(x)`, specialized for the data type
/// of `x` and one accumulator for `SUM(y)`, specialized for the data
/// type of `y`.
///
/// # Discussion
///
/// [`group_values`] does not store any aggregate state inline. It only
/// assigns "group indices", one for each (distinct) group value. The
/// accumulators manage the in-progress aggregate state for each
/// group, with the group values themselves are stored in
/// [`group_values`] at the corresponding group index.
///
/// The accumulator state (e.g partial sums) is managed by and stored
/// by a [`GroupsAccumulator`] accumulator. There is one accumulator
/// per aggregate expression (COUNT, AVG, etc) in the
/// stream. Internally, each `GroupsAccumulator` manages the state for
/// multiple groups, and is passed `group_indexes` during update. Note
/// The accumulator state is not managed by this operator (e.g in the
/// hash table).
///
/// [`group_values`]: Self::group_values
///
/// # Partial Aggregate and multi-phase grouping
///
/// As described on [`Accumulator::state`], this operator is used in the context
/// "multi-phase" grouping when the mode is [`AggregateMode::Partial`].
///
/// An important optimization for multi-phase partial aggregation is to skip
/// partial aggregation when it is not effective enough to warrant the memory or
/// CPU cost, as is often the case for queries many distinct groups (high
/// cardinality group by). Memory is particularly important because each Partial
/// aggregator must store the intermediate state for each group.
///
/// If the ratio of the number of groups to the number of input rows exceeds a
/// threshold, and [`GroupsAccumulator::supports_convert_to_state`] is
/// supported, this operator will stop applying Partial aggregation and directly
/// pass the input rows to the next aggregation phase.
///
/// [`Accumulator::state`]: datafusion_expr::Accumulator::state
///
/// # Spilling (to disk)
///
/// The sizes of group values and accumulators can become large. Before that causes out of memory,
/// this hash aggregator outputs partial states early for partial aggregation or spills to local
/// disk using Arrow IPC format for final aggregation. For every input [`RecordBatch`], the memory
/// manager checks whether the new input size meets the memory configuration. If not, outputting or
/// spilling happens. For outputting, the final aggregation takes care of re-grouping. For spilling,
/// later stream-merge sort on reading back the spilled data does re-grouping. Note the rows cannot
/// be grouped once spilled onto disk, the read back data needs to be re-grouped again. In addition,
/// re-grouping may cause out of memory again. Thus, re-grouping has to be a sort based aggregation.
/// ```text
/// Partial Aggregation [batch_size = 2] (max memory = 3 rows)
///
///  INPUTS        PARTIALLY AGGREGATED (UPDATE BATCH)   OUTPUTS
/// ┌─────────┐    ┌─────────────────┐                  ┌─────────────────┐
/// │ a │ b   │    │ a │    AVG(b)   │                  │ a │    AVG(b)   │
/// │---│-----│    │   │[count]│[sum]│                  │   │[count]│[sum]│
/// │ 3 │ 3.0 │ ─▶ │---│-------│-----│                  │---│-------│-----│
/// │ 2 │ 2.0 │    │ 2 │ 1     │ 2.0 │ ─▶ early emit ─▶ │ 2 │ 1     │ 2.0 │
/// └─────────┘    │ 3 │ 2     │ 7.0 │               │  │ 3 │ 2     │ 7.0 │
/// ┌─────────┐ ─▶ │ 4 │ 1     │ 8.0 │               │  └─────────────────┘
/// │ 3 │ 4.0 │    └─────────────────┘               └▶ ┌─────────────────┐
/// │ 4 │ 8.0 │    ┌─────────────────┐                  │ 4 │ 1     │ 8.0 │
/// └─────────┘    │ a │    AVG(b)   │               ┌▶ │ 1 │ 1     │ 1.0 │
/// ┌─────────┐    │---│-------│-----│               │  └─────────────────┘
/// │ 1 │ 1.0 │ ─▶ │ 1 │ 1     │ 1.0 │ ─▶ early emit ─▶ ┌─────────────────┐
/// │ 3 │ 2.0 │    │ 3 │ 1     │ 2.0 │                  │ 3 │ 1     │ 2.0 │
/// └─────────┘    └─────────────────┘                  └─────────────────┘
///
///
/// Final Aggregation [batch_size = 2] (max memory = 3 rows)
///
/// PARTIALLY INPUTS       FINAL AGGREGATION (MERGE BATCH)       RE-GROUPED (SORTED)
/// ┌─────────────────┐    [keep using the partial schema]       [Real final aggregation
/// │ a │    AVG(b)   │    ┌─────────────────┐                    output]
/// │   │[count]│[sum]│    │ a │    AVG(b)   │                   ┌────────────┐
/// │---│-------│-----│ ─▶ │   │[count]│[sum]│                   │ a │ AVG(b) │
/// │ 3 │ 3     │ 3.0 │    │---│-------│-----│ ─▶ spill ─┐       │---│--------│
/// │ 2 │ 2     │ 1.0 │    │ 2 │ 2     │ 1.0 │           │       │ 1 │    4.0 │
/// └─────────────────┘    │ 3 │ 4     │ 8.0 │           ▼       │ 2 │    1.0 │
/// ┌─────────────────┐ ─▶ │ 4 │ 1     │ 7.0 │     Streaming  ─▶ └────────────┘
/// │ 3 │ 1     │ 5.0 │    └─────────────────┘     merge sort ─▶ ┌────────────┐
/// │ 4 │ 1     │ 7.0 │    ┌─────────────────┐            ▲      │ a │ AVG(b) │
/// └─────────────────┘    │ a │    AVG(b)   │            │      │---│--------│
/// ┌─────────────────┐    │---│-------│-----│ ─▶ memory ─┘      │ 3 │    2.0 │
/// │ 1 │ 2     │ 8.0 │ ─▶ │ 1 │ 2     │ 8.0 │                   │ 4 │    7.0 │
/// │ 2 │ 2     │ 3.0 │    │ 2 │ 2     │ 3.0 │                   └────────────┘
/// └─────────────────┘    └─────────────────┘
/// ```
pub(crate) struct GroupedHashAggregateStream {
    // ========================================================================
    // PROPERTIES:
    // These fields are initialized at the start and remain constant throughout
    // the execution.
    // ========================================================================
    schema: SchemaRef,
    input: SendableRecordBatchStream,
    mode: AggregateMode,

    /// Arguments to pass to each accumulator.
    ///
    /// The arguments in `accumulator[i]` is passed `aggregate_arguments[i]`
    ///
    /// The argument to each accumulator is itself a `Vec` because
    /// some aggregates such as `CORR` can accept more than one
    /// argument.
    aggregate_arguments: Vec<Vec<Arc<dyn PhysicalExpr>>>,

    /// Optional filter expression to evaluate, one for each for
    /// accumulator. If present, only those rows for which the filter
    /// evaluate to true should be included in the aggregate results.
    ///
    /// For example, for an aggregate like `SUM(x) FILTER (WHERE x >= 100)`,
    /// the filter expression is  `x > 100`.
    filter_expressions: Vec<Option<Arc<dyn PhysicalExpr>>>,

    /// GROUP BY expressions
    group_by: PhysicalGroupBy,

    /// max rows in output RecordBatches
    batch_size: usize,

    /// Optional soft limit on the number of `group_values` in a batch
    /// If the number of `group_values` in a single batch exceeds this value,
    /// the `GroupedHashAggregateStream` operation immediately switches to
    /// output mode and emits all groups.
    group_values_soft_limit: Option<usize>,

    // ========================================================================
    // STATE FLAGS:
    // These fields will be updated during the execution. And control the flow of
    // the execution.
    // ========================================================================
    /// Tracks if this stream is generating input or output
    exec_state: ExecutionState,

    /// Have we seen the end of the input
    input_done: bool,

    // ========================================================================
    // STATE BUFFERS:
    // These fields will accumulate intermediate results during the execution.
    // ========================================================================
    /// An interning store of group keys
    group_values: Box<dyn GroupValues>,

    /// scratch space for the current input [`RecordBatch`] being
    /// processed. Reused across batches here to avoid reallocations
    current_group_indices: Vec<usize>,

    /// Accumulators, one for each `AggregateFunctionExpr` in the query
    ///
    /// For example, if the query has aggregates, `SUM(x)`,
    /// `COUNT(y)`, there will be two accumulators, each one
    /// specialized for that particular aggregate and its input types
    accumulators: Vec<Box<dyn GroupsAccumulator>>,

    // ========================================================================
    // TASK-SPECIFIC STATES:
    // Inner states groups together properties, states for a specific task.
    // ========================================================================
    /// Optional ordering information, that might allow groups to be
    /// emitted from the hash table prior to seeing the end of the
    /// input
    group_ordering: GroupOrdering,

    /// The spill state object
    spill_state: SpillState,

    /// Optional probe for skipping data aggregation, if supported by
    /// current stream.
    skip_aggregation_probe: Option<SkipAggregationProbe>,

    // ========================================================================
    // EXECUTION RESOURCES:
    // Fields related to managing execution resources and monitoring performance.
    // ========================================================================
    /// The memory reservation for this grouping
    reservation: MemoryReservation,

    /// The behavior to trigger when out of memory occurs
    oom_mode: OutOfMemoryMode,

    /// Execution metrics
    baseline_metrics: BaselineMetrics,

    /// Aggregation-specific metrics
    group_by_metrics: GroupByMetrics,

    /// Reduction factor metric, calculated as `output_rows/input_rows` (only for partial aggregation)
    reduction_factor: Option<metrics::RatioMetrics>,
}

impl GroupedHashAggregateStream {
    /// Create a new GroupedHashAggregateStream
    pub fn new(
        agg: &AggregateExec,
        context: &Arc<TaskContext>,
        partition: usize,
    ) -> Result<Self> {
        debug!("Creating GroupedHashAggregateStream");
        let agg_schema = Arc::clone(&agg.schema);
        let agg_group_by = agg.group_by.clone();
        let agg_filter_expr = agg.filter_expr.clone();

        let batch_size = context.session_config().batch_size();
        let input = agg.input.execute(partition, Arc::clone(context))?;
        let baseline_metrics = BaselineMetrics::new(&agg.metrics, partition);
        let group_by_metrics = GroupByMetrics::new(&agg.metrics, partition);

        let timer = baseline_metrics.elapsed_compute().timer();

        let aggregate_exprs = agg.aggr_expr.clone();

        // arguments for each aggregate, one vec of expressions per
        // aggregate
        let aggregate_arguments = aggregates::aggregate_expressions(
            &agg.aggr_expr,
            &agg.mode,
            agg_group_by.num_group_exprs(),
        )?;
        // arguments for aggregating spilled data is the same as the one for final aggregation
        let merging_aggregate_arguments = aggregates::aggregate_expressions(
            &agg.aggr_expr,
            &AggregateMode::Final,
            agg_group_by.num_group_exprs(),
        )?;

        let filter_expressions = match agg.mode {
            AggregateMode::Partial
            | AggregateMode::Single
            | AggregateMode::SinglePartitioned => agg_filter_expr,
            AggregateMode::Final | AggregateMode::FinalPartitioned => {
                vec![None; agg.aggr_expr.len()]
            }
        };

        // Instantiate the accumulators
        let accumulators: Vec<_> = aggregate_exprs
            .iter()
            .map(create_group_accumulator)
            .collect::<Result<_>>()?;

        let group_schema = agg_group_by.group_schema(&agg.input().schema())?;

        // fix https://github.com/apache/datafusion/issues/13949
        // Builds a **partial aggregation** schema by combining the group columns and
        // the accumulator state columns produced by each aggregate expression.
        //
        // # Why Partial Aggregation Schema Is Needed
        //
        // In a multi-stage (partial/final) aggregation strategy, each partial-aggregate
        // operator produces *intermediate* states (e.g., partial sums, counts) rather
        // than final scalar values. These extra columns do **not** exist in the original
        // input schema (which may be something like `[colA, colB, ...]`). Instead,
        // each aggregator adds its own internal state columns (e.g., `[acc_state_1, acc_state_2, ...]`).
        //
        // Therefore, when we spill these intermediate states or pass them to another
        // aggregation operator, we must use a schema that includes both the group
        // columns **and** the partial-state columns.
        let spill_schema = Arc::new(create_schema(
            &agg.input().schema(),
            &agg_group_by,
            &aggregate_exprs,
            AggregateMode::Partial,
        )?);

        // Need to update the GROUP BY expressions to point to the correct column after schema change
        let merging_group_by_expr = agg_group_by
            .expr
            .iter()
            .enumerate()
            .map(|(idx, (_, name))| {
                (Arc::new(Column::new(name.as_str(), idx)) as _, name.clone())
            })
            .collect();

        let output_ordering = agg.cache.output_ordering();

        let spill_sort_exprs =
            group_schema
                .fields
                .into_iter()
                .enumerate()
                .map(|(idx, field)| {
                    let output_expr = Column::new(field.name().as_str(), idx);

                    // Try to use the sort options from the output ordering, if available.
                    // This ensures that spilled state is sorted in the required order as well.
                    let sort_options = output_ordering
                        .and_then(|o| o.get_sort_options(&output_expr))
                        .unwrap_or_default();

                    PhysicalSortExpr::new(Arc::new(output_expr), sort_options)
                });
        let Some(spill_ordering) = LexOrdering::new(spill_sort_exprs) else {
            return internal_err!("Spill expression is empty");
        };

        let agg_fn_names = aggregate_exprs
            .iter()
            .map(|expr| expr.human_display())
            .collect::<Vec<_>>()
            .join(", ");
        let name = format!("GroupedHashAggregateStream[{partition}] ({agg_fn_names})");
        let group_ordering = GroupOrdering::try_new(&agg.input_order_mode)?;
        let oom_mode = match (agg.mode, &group_ordering) {
            // In partial aggregation mode, always prefer to emit incomplete results early.
            (AggregateMode::Partial, _) => OutOfMemoryMode::EmitEarly,
            // For non-partial aggregation modes, emitting incomplete results is not an option.
            // Instead, use disk spilling to store sorted, incomplete results, and merge them
            // afterwards.
            (_, GroupOrdering::None | GroupOrdering::Partial(_))
                if context.runtime_env().disk_manager.tmp_files_enabled() =>
            {
                OutOfMemoryMode::Spill
            }
            // For `GroupOrdering::Full`, the incoming stream is already sorted. This ensures the
            // number of incomplete groups can be kept small at all times. If we still hit
            // an out-of-memory condition, spilling to disk would not be beneficial since the same
            // situation is likely to reoccur when reading back the spilled data.
            // Therefore, we fall back to simply reporting the error immediately.
            // This mode will also be used if the `DiskManager` is not configured to allow spilling
            // to disk.
            _ => OutOfMemoryMode::ReportError,
        };

        let group_values = new_group_values(group_schema, &group_ordering)?;
        let reservation = MemoryConsumer::new(name)
            // We interpret 'can spill' as 'can handle memory back pressure'.
            // This value needs to be set to true for the default memory pool implementations
            // to ensure fair application of back pressure amongst the memory consumers.
            .with_can_spill(oom_mode != OutOfMemoryMode::ReportError)
            .register(context.memory_pool());
        timer.done();

        let exec_state = ExecutionState::ReadingInput;

        let spill_manager = SpillManager::new(
            context.runtime_env(),
            metrics::SpillMetrics::new(&agg.metrics, partition),
            Arc::clone(&spill_schema),
        )
        .with_compression_type(context.session_config().spill_compression());

        let spill_state = SpillState {
            spills: vec![],
            spill_expr: spill_ordering,
            spill_schema,
            is_stream_merging: false,
            merging_aggregate_arguments,
            merging_group_by: PhysicalGroupBy::new_single(merging_group_by_expr),
            peak_mem_used: MetricBuilder::new(&agg.metrics)
                .gauge("peak_mem_used", partition),
            spill_manager,
        };

        // Skip aggregation is supported if:
        // - aggregation mode is Partial
        // - input is not ordered by GROUP BY expressions,
        //   since Final mode expects unique group values as its input
        // - all accumulators support input batch to intermediate
        //   aggregate state conversion
        // - there is only one GROUP BY expressions set
        let skip_aggregation_probe = if agg.mode == AggregateMode::Partial
            && matches!(group_ordering, GroupOrdering::None)
            && accumulators
                .iter()
                .all(|acc| acc.supports_convert_to_state())
            && agg_group_by.is_single()
        {
            let options = &context.session_config().options().execution;
            let probe_rows_threshold =
                options.skip_partial_aggregation_probe_rows_threshold;
            let probe_ratio_threshold =
                options.skip_partial_aggregation_probe_ratio_threshold;
            let skipped_aggregation_rows = MetricBuilder::new(&agg.metrics)
                .counter("skipped_aggregation_rows", partition);
            Some(SkipAggregationProbe::new(
                probe_rows_threshold,
                probe_ratio_threshold,
                skipped_aggregation_rows,
            ))
        } else {
            None
        };

        let reduction_factor = if agg.mode == AggregateMode::Partial {
            Some(
                MetricBuilder::new(&agg.metrics)
                    .with_type(metrics::MetricType::SUMMARY)
                    .ratio_metrics("reduction_factor", partition),
            )
        } else {
            None
        };

        Ok(GroupedHashAggregateStream {
            schema: agg_schema,
            input,
            mode: agg.mode,
            accumulators,
            aggregate_arguments,
            filter_expressions,
            group_by: agg_group_by,
            reservation,
            oom_mode,
            group_values,
            current_group_indices: Default::default(),
            exec_state,
            baseline_metrics,
            group_by_metrics,
            batch_size,
            group_ordering,
            input_done: false,
            spill_state,
            group_values_soft_limit: agg.limit,
            skip_aggregation_probe,
            reduction_factor,
        })
    }
}

/// Create an accumulator for `agg_expr` -- a [`GroupsAccumulator`] if
/// that is supported by the aggregate, or a
/// [`GroupsAccumulatorAdapter`] if not.
pub(crate) fn create_group_accumulator(
    agg_expr: &Arc<AggregateFunctionExpr>,
) -> Result<Box<dyn GroupsAccumulator>> {
    if agg_expr.groups_accumulator_supported() {
        agg_expr.create_groups_accumulator()
    } else {
        // Note in the log when the slow path is used
        debug!(
            "Creating GroupsAccumulatorAdapter for {}: {agg_expr:?}",
            agg_expr.name()
        );
        let agg_expr_captured = Arc::clone(agg_expr);
        let factory = move || agg_expr_captured.create_accumulator();
        Ok(Box::new(GroupsAccumulatorAdapter::new(factory)))
    }
}

impl Stream for GroupedHashAggregateStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let elapsed_compute = self.baseline_metrics.elapsed_compute().clone();

        loop {
            match &self.exec_state {
                ExecutionState::ReadingInput => 'reading_input: {
                    match ready!(self.input.poll_next_unpin(cx)) {
                        // New batch to aggregate
                        Some(Ok(batch)) => {
                            let timer = elapsed_compute.timer();
                            let input_rows = batch.num_rows();

                            if self.mode == AggregateMode::Partial
                                && let Some(reduction_factor) =
                                    self.reduction_factor.as_ref()
                            {
                                reduction_factor.add_total(input_rows);
                            }

                            // Do the grouping.
                            // `group_aggregate_batch` will _not_ have updated the memory reservation yet.
                            // The rest of the code will first try to reduce memory usage by
                            // already emitting results.
                            self.group_aggregate_batch(&batch)?;

                            assert!(!self.input_done);

                            // If the number of group values equals or exceeds the soft limit,
                            // emit all groups and switch to producing output
                            if self.hit_soft_group_limit() {
                                timer.done();
                                self.set_input_done_and_produce_output()?;
                                // make sure the exec_state just set is not overwritten below
                                break 'reading_input;
                            }

                            // Try to emit completed groups if possible.
                            // If we already started spilling, we can no longer emit since
                            // this might lead to incorrect output ordering
                            if (self.spill_state.spills.is_empty()
                                || self.spill_state.is_stream_merging)
                                && let Some(to_emit) = self.group_ordering.emit_to()
                            {
                                timer.done();
                                if let Some(batch) = self.emit(to_emit, false)? {
                                    self.exec_state =
                                        ExecutionState::ProducingOutput(batch);
                                };
                                // make sure the exec_state just set is not overwritten below
                                break 'reading_input;
                            }

                            if self.mode == AggregateMode::Partial {
                                // Spilling should never be activated in partial aggregation mode.
                                assert!(!self.spill_state.is_stream_merging);

                                // Check if we should switch to skip aggregation mode
                                // It's important that we do this before we early emit since we've
                                // already updated the probe.
                                self.update_skip_aggregation_probe(input_rows);
                                if let Some(new_state) =
                                    self.switch_to_skip_aggregation()?
                                {
                                    timer.done();
                                    self.exec_state = new_state;
                                    break 'reading_input;
                                }
                            }

                            // If we reach this point, try to update the memory reservation
                            // handling out-of-memory conditions as determined by the OOM mode.
                            if let Some(new_state) =
                                self.try_update_memory_reservation()?
                            {
                                timer.done();
                                self.exec_state = new_state;
                                break 'reading_input;
                            }

                            timer.done();
                        }

                        // Found error from input stream
                        Some(Err(e)) => {
                            // inner had error, return to caller
                            return Poll::Ready(Some(Err(e)));
                        }

                        // Found end from input stream
                        None => {
                            // inner is done, emit all rows and switch to producing output
                            self.set_input_done_and_produce_output()?;
                        }
                    }
                }

                ExecutionState::SkippingAggregation => {
                    match ready!(self.input.poll_next_unpin(cx)) {
                        Some(Ok(batch)) => {
                            let _timer = elapsed_compute.timer();
                            if let Some(probe) = self.skip_aggregation_probe.as_mut() {
                                probe.record_skipped(&batch);
                            }
                            let states = self.transform_to_states(&batch)?;
                            return Poll::Ready(Some(Ok(
                                states.record_output(&self.baseline_metrics)
                            )));
                        }
                        Some(Err(e)) => {
                            // inner had error, return to caller
                            return Poll::Ready(Some(Err(e)));
                        }
                        None => {
                            // inner is done, switching to `Done` state
                            // Sanity check: when switching from SkippingAggregation to Done,
                            // all groups should have already been emitted
                            if !self.group_values.is_empty() {
                                return Poll::Ready(Some(internal_err!(
                                    "Switching from SkippingAggregation to Done with {} groups still in hash table. \
                                    This is a bug - all groups should have been emitted before skip aggregation started.",
                                    self.group_values.len()
                                )));
                            }
                            self.exec_state = ExecutionState::Done;
                        }
                    }
                }

                ExecutionState::ProducingOutput(batch) => {
                    // slice off a part of the batch, if needed
                    let output_batch;
                    let size = self.batch_size;
                    (self.exec_state, output_batch) = if batch.num_rows() <= size {
                        (
                            if self.input_done {
                                ExecutionState::Done
                            }
                            // In Partial aggregation, we also need to check
                            // if we should trigger partial skipping
                            else if self.mode == AggregateMode::Partial
                                && self.should_skip_aggregation()
                            {
                                ExecutionState::SkippingAggregation
                            } else {
                                ExecutionState::ReadingInput
                            },
                            batch.clone(),
                        )
                    } else {
                        // output first batch_size rows
                        let size = self.batch_size;
                        let num_remaining = batch.num_rows() - size;
                        let remaining = batch.slice(size, num_remaining);
                        let output = batch.slice(0, size);
                        (ExecutionState::ProducingOutput(remaining), output)
                    };

                    if let Some(reduction_factor) = self.reduction_factor.as_ref() {
                        reduction_factor.add_part(output_batch.num_rows());
                    }

                    // Empty record batches should not be emitted.
                    // They need to be treated as  [`Option<RecordBatch>`]es and handled separately
                    debug_assert!(output_batch.num_rows() > 0);
                    return Poll::Ready(Some(Ok(
                        output_batch.record_output(&self.baseline_metrics)
                    )));
                }

                ExecutionState::Done => {
                    // Sanity check: all groups should have been emitted by now
                    if !self.group_values.is_empty() {
                        return Poll::Ready(Some(internal_err!(
                            "AggregateStream was in Done state with {} groups left in hash table. \
                            This is a bug - all groups should have been emitted before entering Done state.",
                            self.group_values.len()
                        )));
                    }
                    // release the memory reservation since sending back output batch itself needs
                    // some memory reservation, so make some room for it.
                    self.clear_all();
                    let _ = self.update_memory_reservation();
                    return Poll::Ready(None);
                }
            }
        }
    }
}

impl RecordBatchStream for GroupedHashAggregateStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

impl GroupedHashAggregateStream {
    /// Perform group-by aggregation for the given [`RecordBatch`].
    fn group_aggregate_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        // Evaluate the grouping expressions
        let group_by_values = if self.spill_state.is_stream_merging {
            evaluate_group_by(&self.spill_state.merging_group_by, batch)?
        } else {
            evaluate_group_by(&self.group_by, batch)?
        };

        // Only create the timer if there are actual aggregate arguments to evaluate
        let timer = match (
            self.spill_state.is_stream_merging,
            self.spill_state.merging_aggregate_arguments.is_empty(),
            self.aggregate_arguments.is_empty(),
        ) {
            (true, false, _) | (false, _, false) => {
                Some(self.group_by_metrics.aggregate_arguments_time.timer())
            }
            _ => None,
        };

        // Evaluate the aggregation expressions.
        let input_values = if self.spill_state.is_stream_merging {
            evaluate_many(&self.spill_state.merging_aggregate_arguments, batch)?
        } else {
            evaluate_many(&self.aggregate_arguments, batch)?
        };
        drop(timer);

        // Evaluate the filter expressions, if any, against the inputs
        let filter_values = if self.spill_state.is_stream_merging {
            let filter_expressions = vec![None; self.accumulators.len()];
            evaluate_optional(&filter_expressions, batch)?
        } else {
            evaluate_optional(&self.filter_expressions, batch)?
        };

        for group_values in &group_by_values {
            let groups_start_time = Instant::now();

            // calculate the group indices for each input row
            let starting_num_groups = self.group_values.len();
            self.group_values
                .intern(group_values, &mut self.current_group_indices)?;
            let group_indices = &self.current_group_indices;

            // Update ordering information if necessary
            let total_num_groups = self.group_values.len();
            if total_num_groups > starting_num_groups {
                self.group_ordering.new_groups(
                    group_values,
                    group_indices,
                    total_num_groups,
                )?;
            }

            // Use this instant for both measurements to save a syscall
            let agg_start_time = Instant::now();
            self.group_by_metrics
                .time_calculating_group_ids
                .add_duration(agg_start_time - groups_start_time);

            // Gather the inputs to call the actual accumulator
            let t = self
                .accumulators
                .iter_mut()
                .zip(input_values.iter())
                .zip(filter_values.iter());

            for ((acc, values), opt_filter) in t {
                let opt_filter = opt_filter.as_ref().map(|filter| filter.as_boolean());

                // Call the appropriate method on each aggregator with
                // the entire input row and the relevant group indexes
                match self.mode {
                    AggregateMode::Partial
                    | AggregateMode::Single
                    | AggregateMode::SinglePartitioned
                        if !self.spill_state.is_stream_merging =>
                    {
                        acc.update_batch(
                            values,
                            group_indices,
                            opt_filter,
                            total_num_groups,
                        )?;
                    }
                    _ => {
                        assert_or_internal_err!(
                            opt_filter.is_none(),
                            "aggregate filter should be applied in partial stage, there should be no filter in final stage"
                        );

                        // if aggregation is over intermediate states,
                        // use merge
                        acc.merge_batch(values, group_indices, None, total_num_groups)?;
                    }
                }
                self.group_by_metrics
                    .aggregation_time
                    .add_elapsed(agg_start_time);
            }
        }

        Ok(())
    }

    /// Attempts to update the memory reservation. If that fails due to a
    /// [DataFusionError::ResourcesExhausted] error, an attempt will be made to resolve
    /// the out-of-memory condition based on the [out-of-memory handling mode](OutOfMemoryMode).
    ///
    /// If the out-of-memory condition can not be resolved, an `Err` value will be returned
    ///
    /// Returns `Ok(Some(ExecutionState))` if the state should be changed, `Ok(None)` otherwise.
    fn try_update_memory_reservation(&mut self) -> Result<Option<ExecutionState>> {
        let oom = match self.update_memory_reservation() {
            Err(e @ DataFusionError::ResourcesExhausted(_)) => e,
            Err(e) => return Err(e),
            Ok(_) => return Ok(None),
        };

        match self.oom_mode {
            OutOfMemoryMode::Spill if !self.group_values.is_empty() => {
                self.spill()?;
                self.clear_shrink(self.batch_size);
                self.update_memory_reservation()?;
                Ok(None)
            }
            OutOfMemoryMode::EmitEarly if self.group_values.len() > 1 => {
                let n = if self.group_values.len() >= self.batch_size {
                    // Try to emit an integer multiple of batch size if possible
                    self.group_values.len() / self.batch_size * self.batch_size
                } else {
                    // Otherwise emit whatever we can
                    self.group_values.len()
                };

                if let Some(batch) = self.emit(EmitTo::First(n), false)? {
                    Ok(Some(ExecutionState::ProducingOutput(batch)))
                } else {
                    Err(oom)
                }
            }
            _ => Err(oom),
        }
    }

    fn update_memory_reservation(&mut self) -> Result<()> {
        let acc = self.accumulators.iter().map(|x| x.size()).sum::<usize>();
        let new_size = acc
            + self.group_values.size()
            + self.group_ordering.size()
            + self.current_group_indices.allocated_size();
        let reservation_result = self.reservation.try_resize(new_size);

        if reservation_result.is_ok() {
            self.spill_state
                .peak_mem_used
                .set_max(self.reservation.size());
        }

        reservation_result
    }

    /// Create an output RecordBatch with the group keys and
    /// accumulator states/values specified in emit_to
    fn emit(&mut self, emit_to: EmitTo, spilling: bool) -> Result<Option<RecordBatch>> {
        let schema = if spilling {
            Arc::clone(&self.spill_state.spill_schema)
        } else {
            self.schema()
        };
        if self.group_values.is_empty() {
            return Ok(None);
        }

        let timer = self.group_by_metrics.emitting_time.timer();
        let mut output = self.group_values.emit(emit_to)?;
        if let EmitTo::First(n) = emit_to {
            self.group_ordering.remove_groups(n);
        }

        // Next output each aggregate value
        for acc in self.accumulators.iter_mut() {
            match self.mode {
                AggregateMode::Partial => output.extend(acc.state(emit_to)?),
                _ if spilling => {
                    // If spilling, output partial state because the spilled data will be
                    // merged and re-evaluated later.
                    output.extend(acc.state(emit_to)?)
                }
                AggregateMode::Final
                | AggregateMode::FinalPartitioned
                | AggregateMode::Single
                | AggregateMode::SinglePartitioned => output.push(acc.evaluate(emit_to)?),
            }
        }
        drop(timer);

        // emit reduces the memory usage. Ignore Err from update_memory_reservation. Even if it is
        // over the target memory size after emission, we can emit again rather than returning Err.
        let _ = self.update_memory_reservation();
        let batch = RecordBatch::try_new(schema, output)?;
        debug_assert!(batch.num_rows() > 0);

        Ok(Some(batch))
    }

    /// Emit all intermediate aggregation states, sort them, and store them on disk.
    /// This process helps in reducing memory pressure by allowing the data to be
    /// read back with streaming merge.
    fn spill(&mut self) -> Result<()> {
        // Emit and sort intermediate aggregation state
        let Some(emit) = self.emit(EmitTo::All, true)? else {
            return Ok(());
        };
        let sorted = sort_batch(&emit, &self.spill_state.spill_expr, None)?;

        // Spill sorted state to disk
        let spillfile = self
            .spill_state
            .spill_manager
            .spill_record_batch_by_size_and_return_max_batch_memory(
                &sorted,
                "HashAggSpill",
                self.batch_size,
            )?;
        match spillfile {
            Some((spillfile, max_record_batch_memory)) => {
                self.spill_state.spills.push(SortedSpillFile {
                    file: spillfile,
                    max_record_batch_memory,
                })
            }
            None => {
                return internal_err!(
                    "Calling spill with no intermediate batch to spill"
                );
            }
        }

        Ok(())
    }

    /// Clear memory and shirk capacities to the size of the batch.
    fn clear_shrink(&mut self, num_rows: usize) {
        self.group_values.clear_shrink(num_rows);
        self.current_group_indices.clear();
        self.current_group_indices.shrink_to(num_rows);
    }

    /// Clear memory and shirk capacities to zero.
    fn clear_all(&mut self) {
        self.clear_shrink(0);
    }

    /// returns true if there is a soft groups limit and the number of distinct
    /// groups we have seen is over that limit
    fn hit_soft_group_limit(&self) -> bool {
        let Some(group_values_soft_limit) = self.group_values_soft_limit else {
            return false;
        };
        group_values_soft_limit <= self.group_values.len()
    }

    /// Finalizes reading of the input stream and prepares for producing output values.
    ///
    /// This method is called both when the original input stream and,
    /// in case of disk spilling, the SPM stream have been drained.
    fn set_input_done_and_produce_output(&mut self) -> Result<()> {
        self.input_done = true;
        self.group_ordering.input_done();
        let elapsed_compute = self.baseline_metrics.elapsed_compute().clone();
        let timer = elapsed_compute.timer();
        self.exec_state = if self.spill_state.spills.is_empty() {
            // Input has been entirely processed without spilling to disk.

            // Flush any remaining group values.
            let batch = self.emit(EmitTo::All, false)?;

            // If there are none, we're done; otherwise switch to emitting them
            batch.map_or(ExecutionState::Done, ExecutionState::ProducingOutput)
        } else {
            // Spill any remaining data to disk. There is some performance overhead in
            // writing out this last chunk of data and reading it back. The benefit of
            // doing this is that memory usage for this stream is reduced, and the more
            // sophisticated memory handling in `MultiLevelMergeBuilder` can take over
            // instead.
            // Spilling to disk and reading back also ensures batch size is consistent
            // rather than potentially having one significantly larger last batch.
            self.spill()?;

            // Mark that we're switching to stream merging mode.
            self.spill_state.is_stream_merging = true;

            self.input = StreamingMergeBuilder::new()
                .with_schema(Arc::clone(&self.spill_state.spill_schema))
                .with_spill_manager(self.spill_state.spill_manager.clone())
                .with_sorted_spill_files(std::mem::take(&mut self.spill_state.spills))
                .with_expressions(&self.spill_state.spill_expr)
                .with_metrics(self.baseline_metrics.clone())
                .with_batch_size(self.batch_size)
                .with_reservation(self.reservation.new_empty())
                .build()?;
            self.input_done = false;

            // Reset the group values collectors.
            self.clear_all();

            // We can now use `GroupOrdering::Full` since the spill files are sorted
            // on the grouping columns.
            self.group_ordering = GroupOrdering::Full(GroupOrderingFull::new());

            // Use `OutOfMemoryMode::ReportError` from this point on
            // to ensure we don't spill the spilled data to disk again.
            self.oom_mode = OutOfMemoryMode::ReportError;

            self.update_memory_reservation()?;

            ExecutionState::ReadingInput
        };
        timer.done();
        Ok(())
    }

    /// Updates skip aggregation probe state.
    ///
    /// Notice: It should only be called in Partial aggregation
    fn update_skip_aggregation_probe(&mut self, input_rows: usize) {
        if let Some(probe) = self.skip_aggregation_probe.as_mut() {
            // Skip aggregation probe is not supported if stream has any spills,
            // currently spilling is not supported for Partial aggregation
            assert!(self.spill_state.spills.is_empty());
            probe.update_state(input_rows, self.group_values.len());
        };
    }

    /// In case the probe indicates that aggregation may be
    /// skipped, forces stream to produce currently accumulated output.
    ///
    /// Notice: It should only be called in Partial aggregation
    ///
    /// Returns `Some(ExecutionState)` if the state should be changed, None otherwise.
    fn switch_to_skip_aggregation(&mut self) -> Result<Option<ExecutionState>> {
        if let Some(probe) = self.skip_aggregation_probe.as_mut()
            && probe.should_skip()
            && let Some(batch) = self.emit(EmitTo::All, false)?
        {
            return Ok(Some(ExecutionState::ProducingOutput(batch)));
        };

        Ok(None)
    }

    /// Returns true if the aggregation probe indicates that aggregation
    /// should be skipped.
    ///
    /// Notice: It should only be called in Partial aggregation
    fn should_skip_aggregation(&self) -> bool {
        self.skip_aggregation_probe
            .as_ref()
            .is_some_and(|probe| probe.should_skip())
    }

    /// Transforms input batch to intermediate aggregate state, without grouping it
    fn transform_to_states(&self, batch: &RecordBatch) -> Result<RecordBatch> {
        let mut group_values = evaluate_group_by(&self.group_by, batch)?;
        let input_values = evaluate_many(&self.aggregate_arguments, batch)?;
        let filter_values = evaluate_optional(&self.filter_expressions, batch)?;

        assert_eq_or_internal_err!(
            group_values.len(),
            1,
            "group_values expected to have single element"
        );
        let mut output = group_values.swap_remove(0);

        let iter = self
            .accumulators
            .iter()
            .zip(input_values.iter())
            .zip(filter_values.iter());

        for ((acc, values), opt_filter) in iter {
            let opt_filter = opt_filter.as_ref().map(|filter| filter.as_boolean());
            output.extend(acc.convert_to_state(values, opt_filter)?);
        }

        let states_batch = RecordBatch::try_new(self.schema(), output)?;

        Ok(states_batch)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::execution_plan::ExecutionPlan;
    use crate::test::TestMemoryExec;
    use arrow::array::{Int32Array, Int64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use datafusion_execution::TaskContext;
    use datafusion_execution::runtime_env::RuntimeEnvBuilder;
    use datafusion_functions_aggregate::count::count_udaf;
    use datafusion_physical_expr::aggregate::AggregateExprBuilder;
    use datafusion_physical_expr::expressions::col;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_double_emission_race_condition_bug() -> Result<()> {
        // Fix for https://github.com/apache/datafusion/issues/18701
        // This test specifically proves that we have fixed double emission race condition
        // where emit_early_if_necessary() and switch_to_skip_aggregation()
        // both emit in the same loop iteration, causing data loss

        let schema = Arc::new(Schema::new(vec![
            Field::new("group_col", DataType::Int32, false),
            Field::new("value_col", DataType::Int64, false),
        ]));

        // Create data that will trigger BOTH conditions in the same iteration:
        // 1. More groups than batch_size (triggers early emission when memory pressure hits)
        // 2. High cardinality ratio (triggers skip aggregation)
        let batch_size = 1024; // We'll set this in session config
        let num_groups = batch_size + 100; // Slightly more than batch_size (1124 groups)

        // Create exactly 1 row per group = 100% cardinality ratio
        let group_ids: Vec<i32> = (0..num_groups as i32).collect();
        let values: Vec<i64> = vec![1; num_groups];

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(group_ids)),
                Arc::new(Int64Array::from(values)),
            ],
        )?;

        let input_partitions = vec![vec![batch]];

        // Create constrained memory to trigger early emission but not completely fail
        let runtime = RuntimeEnvBuilder::default()
            .with_memory_limit(1024, 1.0) // small enough to start but will trigger pressure
            .build_arc()?;

        let mut task_ctx = TaskContext::default().with_runtime(runtime);

        // Configure to trigger BOTH conditions:
        // 1. Low probe threshold (triggers skip probe after few rows)
        // 2. Low ratio threshold (triggers skip aggregation immediately)
        // 3. Set batch_size to 1024 so our 1124 groups will trigger early emission
        // This creates the race condition where both emit paths are triggered
        let mut session_config = task_ctx.session_config().clone();
        session_config = session_config.set(
            "datafusion.execution.batch_size",
            &datafusion_common::ScalarValue::UInt64(Some(1024)),
        );
        session_config = session_config.set(
            "datafusion.execution.skip_partial_aggregation_probe_rows_threshold",
            &datafusion_common::ScalarValue::UInt64(Some(50)),
        );
        session_config = session_config.set(
            "datafusion.execution.skip_partial_aggregation_probe_ratio_threshold",
            &datafusion_common::ScalarValue::Float64(Some(0.8)),
        );
        task_ctx = task_ctx.with_session_config(session_config);
        let task_ctx = Arc::new(task_ctx);

        // Create aggregate: COUNT(*) GROUP BY group_col
        let group_expr = vec![(col("group_col", &schema)?, "group_col".to_string())];
        let aggr_expr = vec![Arc::new(
            AggregateExprBuilder::new(count_udaf(), vec![col("value_col", &schema)?])
                .schema(Arc::clone(&schema))
                .alias("count_value")
                .build()?,
        )];

        let exec = TestMemoryExec::try_new(&input_partitions, Arc::clone(&schema), None)?;
        let exec = Arc::new(TestMemoryExec::update_cache(&Arc::new(exec)));

        // Use Partial mode where the race condition occurs
        let aggregate_exec = AggregateExec::try_new(
            AggregateMode::Partial,
            PhysicalGroupBy::new_single(group_expr),
            aggr_expr,
            vec![None],
            exec,
            Arc::clone(&schema),
        )?;

        // Execute and collect results
        let mut stream =
            GroupedHashAggregateStream::new(&aggregate_exec, &Arc::clone(&task_ctx), 0)?;
        let mut results = Vec::new();

        while let Some(result) = stream.next().await {
            let batch = result?;
            results.push(batch);
        }

        // Count total groups emitted
        let mut total_output_groups = 0;
        for batch in &results {
            total_output_groups += batch.num_rows();
        }

        assert_eq!(
            total_output_groups, num_groups,
            "Unexpected number of groups",
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_skip_aggregation_probe_not_locked_until_skip() -> Result<()> {
        // Test that the probe is not locked until we actually decide to skip.
        // This allows us to continue evaluating the skip condition across multiple batches.
        //
        // Scenario:
        // - Batch 1: Hits rows threshold but NOT ratio threshold (low cardinality) -> don't skip
        // - Batch 2: Now hits ratio threshold (high cardinality) -> skip
        //
        // Without the fix, the probe would be locked after batch 1, preventing the skip
        // decision from being made on batch 2.

        let schema = Arc::new(Schema::new(vec![
            Field::new("group_col", DataType::Int32, false),
            Field::new("value_col", DataType::Int32, false),
        ]));

        // Configure thresholds:
        // - probe_rows_threshold: 100 rows
        // - probe_ratio_threshold: 0.8 (80%)
        let probe_rows_threshold = 100;
        let probe_ratio_threshold = 0.8;

        // Batch 1: 100 rows with only 10 unique groups
        // Ratio: 10/100 = 0.1 (10%) < 0.8 -> should NOT skip
        // This will hit the rows threshold but not the ratio threshold
        let batch1_rows = 100;
        let batch1_groups = 10;
        let mut group_ids_batch1 = Vec::new();
        for i in 0..batch1_rows {
            group_ids_batch1.push((i % batch1_groups) as i32);
        }
        let values_batch1: Vec<i32> = vec![1; batch1_rows];

        let batch1 = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(group_ids_batch1)),
                Arc::new(Int32Array::from(values_batch1)),
            ],
        )?;

        // Batch 2: 350 rows with 350 unique NEW groups (starting from group 10)
        // After batch 2, total: 450 rows, 360 groups
        // Ratio: 360/450 = 0.8 (80%) >= 0.8 -> SHOULD decide to skip
        let batch2_rows = 350;
        let batch2_groups = 350;
        let group_ids_batch2: Vec<i32> = (batch1_groups..(batch1_groups + batch2_groups))
            .map(|x| x as i32)
            .collect();
        let values_batch2: Vec<i32> = vec![1; batch2_rows];

        let batch2 = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(group_ids_batch2)),
                Arc::new(Int32Array::from(values_batch2)),
            ],
        )?;

        // Batch 3: This batch should be skipped since we decided to skip after batch 2
        // 100 rows with 100 unique groups (continuing from where batch 2 left off)
        let batch3_rows = 100;
        let batch3_groups = 100;
        let batch3_start_group = batch1_groups + batch2_groups;
        let group_ids_batch3: Vec<i32> = (batch3_start_group
            ..(batch3_start_group + batch3_groups))
            .map(|x| x as i32)
            .collect();
        let values_batch3: Vec<i32> = vec![1; batch3_rows];

        let batch3 = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(group_ids_batch3)),
                Arc::new(Int32Array::from(values_batch3)),
            ],
        )?;

        let input_partitions = vec![vec![batch1, batch2, batch3]];

        let runtime = RuntimeEnvBuilder::default().build_arc()?;
        let mut task_ctx = TaskContext::default().with_runtime(runtime);

        // Configure skip aggregation settings
        let mut session_config = task_ctx.session_config().clone();
        session_config = session_config.set(
            "datafusion.execution.skip_partial_aggregation_probe_rows_threshold",
            &datafusion_common::ScalarValue::UInt64(Some(probe_rows_threshold)),
        );
        session_config = session_config.set(
            "datafusion.execution.skip_partial_aggregation_probe_ratio_threshold",
            &datafusion_common::ScalarValue::Float64(Some(probe_ratio_threshold)),
        );
        task_ctx = task_ctx.with_session_config(session_config);
        let task_ctx = Arc::new(task_ctx);

        // Create aggregate: COUNT(*) GROUP BY group_col
        let group_expr = vec![(col("group_col", &schema)?, "group_col".to_string())];
        let aggr_expr = vec![Arc::new(
            AggregateExprBuilder::new(count_udaf(), vec![col("value_col", &schema)?])
                .schema(Arc::clone(&schema))
                .alias("count_value")
                .build()?,
        )];

        let exec = TestMemoryExec::try_new(&input_partitions, Arc::clone(&schema), None)?;
        let exec = Arc::new(TestMemoryExec::update_cache(&Arc::new(exec)));

        // Use Partial mode
        let aggregate_exec = AggregateExec::try_new(
            AggregateMode::Partial,
            PhysicalGroupBy::new_single(group_expr),
            aggr_expr,
            vec![None],
            exec,
            Arc::clone(&schema),
        )?;

        // Execute and collect results
        let mut stream =
            GroupedHashAggregateStream::new(&aggregate_exec, &Arc::clone(&task_ctx), 0)?;
        let mut results = Vec::new();

        while let Some(result) = stream.next().await {
            let batch = result?;
            results.push(batch);
        }

        // Check that skip aggregation actually happened
        // The key metric is skipped_aggregation_rows
        let metrics = aggregate_exec.metrics().unwrap();
        let skipped_rows = metrics
            .sum_by_name("skipped_aggregation_rows")
            .map(|m| m.as_usize())
            .unwrap_or(0);

        // We expect batch 3's rows to be skipped (100 rows)
        assert_eq!(
            skipped_rows, batch3_rows,
            "Expected batch 3's rows ({batch3_rows}) to be skipped",
        );

        Ok(())
    }
}
