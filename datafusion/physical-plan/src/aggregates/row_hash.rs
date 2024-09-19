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
use std::{mem, vec};

use crate::aggregates::group_values::{new_group_values, GroupValuesLike};
use crate::aggregates::order::GroupOrderingFull;
use crate::aggregates::{
    evaluate_group_by, evaluate_many, evaluate_optional, group_schema, AggregateMode,
    PhysicalGroupBy,
};
use crate::metrics::{BaselineMetrics, MetricBuilder, RecordOutput};
use crate::sorts::sort::sort_batch;
use crate::sorts::streaming_merge;
use crate::spill::{read_spill_as_stream, spill_record_batch_by_size};
use crate::stream::RecordBatchStreamAdapter;
use crate::{aggregates, metrics, ExecutionPlan, PhysicalExpr};
use crate::{RecordBatchStream, SendableRecordBatchStream};

use arrow::array::*;
use arrow::datatypes::SchemaRef;
use arrow_schema::SortOptions;
use datafusion_common::utils::get_arrayref_at_indices;
use datafusion_common::{
    arrow_datafusion_err, internal_datafusion_err, DataFusionError, Result,
};
use datafusion_execution::disk_manager::RefCountedTempFile;
use datafusion_execution::memory_pool::proxy::VecAllocExt;
use datafusion_execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion_execution::runtime_env::RuntimeEnv;
use datafusion_execution::TaskContext;
use datafusion_expr::{EmitTo, GroupsAccumulator};
use datafusion_functions_aggregate_common::aggregate::groups_accumulator::get_filter_at_indices;
use datafusion_physical_expr::expressions::Column;
use datafusion_physical_expr::{GroupsAccumulatorAdapter, PhysicalSortExpr};

use datafusion_physical_expr::aggregate::AggregateFunctionExpr;
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

    ProducingPartitionedOutput(PartitionedOutput),
    /// Produce intermediate aggregate state for each input row without
    /// aggregation.
    ///
    /// See "partial aggregation" discussion on [`GroupedHashAggregateStream`]
    SkippingAggregation,
    /// All input has been consumed and all groups have been emitted
    Done,
}

use super::order::GroupOrdering;
use super::AggregateExec;

#[derive(Debug, Clone, Default)]
pub(crate) struct PartitionedOutput {
    partitions: Vec<Option<RecordBatch>>,
    start_idx: usize,
    batch_size: usize,
    num_partitions: usize,
}

impl PartitionedOutput {
    pub fn new(
        src_batches: Vec<RecordBatch>,
        batch_size: usize,
        num_partitions: usize,
    ) -> Self {
        let partitions = src_batches.into_iter().map(|batch| Some(batch)).collect();

        Self {
            partitions,
            start_idx: 0,
            batch_size,
            num_partitions,
        }
    }

    pub fn next_batch(&mut self) -> Option<RecordBatch> {
        if self.partitions.is_empty() {
            return None;
        }

        let mut current_idx = self.start_idx;
        loop {
            // If found a partition having data,
            let batch_opt = if self.partitions[current_idx].is_some() {
                Some(self.extract_batch_from_partition(current_idx))
            } else {
                None
            };

            // Advance the `current_idx`
            current_idx = (current_idx + 1) % self.num_partitions;

            if batch_opt.is_some() {
                // If found batch, we update the `start_idx` and return it
                self.start_idx = current_idx;
                return batch_opt;
            } else if self.start_idx == current_idx {
                // If not found, and has loop to end, we return None
                return batch_opt;
            }
            // Otherwise, we loop to check next partition
        }
    }

    pub fn extract_batch_from_partition(&mut self, part_idx: usize) -> RecordBatch {
        let partition_batch = mem::take(&mut self.partitions[part_idx]).unwrap();
        if partition_batch.num_rows() > self.batch_size {
            // If still the exist rows num > `batch_size`,
            // cut off `batch_size` rows as `output``,
            // and set back `remaining`.
            let size = self.batch_size;
            let num_remaining = partition_batch.num_rows() - size;
            let remaining = partition_batch.slice(size, num_remaining);
            let output = partition_batch.slice(0, size);
            self.partitions[part_idx] = Some(remaining);

            output
        } else {
            // If they are the last rows in `partition_batch`, just return,
            // because `partition_batch` has been set to `None`.
            partition_batch
        }
    }
}

/// This encapsulates the spilling state
struct SpillState {
    // ========================================================================
    // PROPERTIES:
    // These fields are initialized at the start and remain constant throughout
    // the execution.
    // ========================================================================
    /// Sorting expression for spilling batches
    spill_expr: Vec<PhysicalSortExpr>,

    /// Schema for spilling batches
    spill_schema: SchemaRef,

    /// aggregate_arguments for merging spilled data
    merging_aggregate_arguments: Vec<Vec<Arc<dyn PhysicalExpr>>>,

    /// GROUP BY expressions for merging spilled data
    merging_group_by: PhysicalGroupBy,

    // ========================================================================
    // STATES:
    // Fields changes during execution. Can be buffer, or state flags that
    // influence the execution in parent `GroupedHashAggregateStream`
    // ========================================================================
    /// If data has previously been spilled, the locations of the
    /// spill files (in Arrow IPC format)
    spills: Vec<RefCountedTempFile>,

    /// true when streaming merge is in progress
    is_stream_merging: bool,
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
    // influence the exeuction in parent `GroupedHashAggregateStream`
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
            self.is_locked = true;
        }
    }

    fn should_skip(&self) -> bool {
        self.should_skip
    }

    /// Provides an ability to externally set `should_skip` flag
    /// to `false` and prohibit further state updates
    fn forbid_skipping(&mut self) {
        self.should_skip = false;
        self.is_locked = true;
    }

    /// Record the number of rows that were output directly without aggregation
    fn record_skipped(&mut self, batch: &RecordBatch) {
        self.skipped_aggregation_rows.add(batch.num_rows());
    }
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
///
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
    group_values: GroupValuesLike,

    /// scratch space for the current input [`RecordBatch`] being
    /// processed. Reused across batches here to avoid reallocations
    current_group_indices: Vec<Vec<usize>>,

    current_row_indices: Vec<Vec<u32>>,

    /// Accumulators, one for each `AggregateFunctionExpr` in the query
    ///
    /// For example, if the query has aggregates, `SUM(x)`,
    /// `COUNT(y)`, there will be two accumulators, each one
    /// specialized for that particular aggregate and its input types
    accumulators: Vec<Vec<Box<dyn GroupsAccumulator>>>,

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

    /// Execution metrics
    baseline_metrics: BaselineMetrics,

    /// The [`RuntimeEnv`] associated with the [`TaskContext`] argument
    runtime: Arc<RuntimeEnv>,
}

impl GroupedHashAggregateStream {
    /// Create a new GroupedHashAggregateStream
    pub fn new(
        agg: &AggregateExec,
        context: Arc<TaskContext>,
        partition: usize,
    ) -> Result<Self> {
        debug!("Creating GroupedHashAggregateStream");
        let agg_schema = Arc::clone(&agg.schema);
        let agg_group_by = agg.group_by.clone();
        let agg_filter_expr = agg.filter_expr.clone();

        let batch_size = context.session_config().batch_size();
        let input = agg.input.execute(partition, Arc::clone(&context))?;
        let baseline_metrics = BaselineMetrics::new(&agg.metrics, partition);

        let timer = baseline_metrics.elapsed_compute().timer();

        let aggregate_exprs = agg.aggr_expr.clone();

        // arguments for each aggregate, one vec of expressions per
        // aggregate
        let aggregate_arguments = aggregates::aggregate_expressions(
            &agg.aggr_expr,
            &agg.mode,
            agg_group_by.expr.len(),
        )?;
        // arguments for aggregating spilled data is the same as the one for final aggregation
        let merging_aggregate_arguments = aggregates::aggregate_expressions(
            &agg.aggr_expr,
            &AggregateMode::Final,
            agg_group_by.expr.len(),
        )?;

        let filter_expressions = match agg.mode {
            AggregateMode::Partial
            | AggregateMode::Single
            | AggregateMode::SinglePartitioned => agg_filter_expr,
            AggregateMode::Final | AggregateMode::FinalPartitioned => {
                vec![None; agg.aggr_expr.len()]
            }
        };

        let group_schema = group_schema(&agg_schema, agg_group_by.expr.len());

        let spill_expr = group_schema
            .fields
            .into_iter()
            .enumerate()
            .map(|(idx, field)| PhysicalSortExpr {
                expr: Arc::new(Column::new(field.name().as_str(), idx)) as _,
                options: SortOptions::default(),
            })
            .collect();

        let name = format!("GroupedHashAggregateStream[{partition}]");
        let reservation = MemoryConsumer::new(name)
            .with_can_spill(true)
            .register(context.memory_pool());
        let (ordering, _) = agg
            .properties()
            .equivalence_properties()
            .find_longest_permutation(&agg_group_by.output_exprs());
        let group_ordering = GroupOrdering::try_new(
            &group_schema,
            &agg.input_order_mode,
            ordering.as_slice(),
        )?;

        // Instantiate the accumulators and group values
        // Judge should we try to use partitioned hashtable, it will be enabled while:
        //   - It is partial operator
        //   - It is not streaming
        let suggest_num_partitions = context.session_config().target_partitions();
        assert!(suggest_num_partitions > 0);
        let partitioning_group_values = agg.mode == AggregateMode::Partial
            && matches!(group_ordering, GroupOrdering::None);
        let group_values = new_group_values(
            group_schema,
            partitioning_group_values,
            suggest_num_partitions,
        )?;

        // We need to decide how many accumulators partitions should we create according to group values partitions
        let num_partitions = group_values.num_partitions();
        let mut accumulators_partitions = Vec::with_capacity(num_partitions);
        for _ in 0..num_partitions {
            let accumulators: Vec<_> = aggregate_exprs
                .iter()
                .map(create_group_accumulator)
                .collect::<Result<_>>()?;
            accumulators_partitions.push(accumulators);
        }

        let current_group_indices = (0..num_partitions)
            .into_iter()
            .map(|_| Vec::new())
            .collect::<Vec<_>>();
        let current_row_indices = (0..num_partitions)
            .into_iter()
            .map(|_| Vec::new())
            .collect::<Vec<_>>();

        timer.done();

        let exec_state = ExecutionState::ReadingInput;

        let spill_state = SpillState {
            spills: vec![],
            spill_expr,
            spill_schema: Arc::clone(&agg_schema),
            is_stream_merging: false,
            merging_aggregate_arguments,
            merging_group_by: PhysicalGroupBy::new_single(agg_group_by.expr.clone()),
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
            && accumulators_partitions[0]
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

        Ok(GroupedHashAggregateStream {
            schema: agg_schema,
            input,
            mode: agg.mode,
            accumulators: accumulators_partitions,
            aggregate_arguments,
            filter_expressions,
            group_by: agg_group_by,
            reservation,
            group_values,
            current_group_indices,
            current_row_indices,
            exec_state,
            baseline_metrics,
            batch_size,
            group_ordering,
            input_done: false,
            runtime: context.runtime_env(),
            spill_state,
            group_values_soft_limit: agg.limit,
            skip_aggregation_probe,
        })
    }
}

/// Create an accumulator for `agg_expr` -- a [`GroupsAccumulator`] if
/// that is supported by the aggregate, or a
/// [`GroupsAccumulatorAdapter`] if not.
pub(crate) fn create_group_accumulator(
    agg_expr: &AggregateFunctionExpr,
) -> Result<Box<dyn GroupsAccumulator>> {
    if agg_expr.groups_accumulator_supported() {
        agg_expr.create_groups_accumulator()
    } else {
        // Note in the log when the slow path is used
        debug!(
            "Creating GroupsAccumulatorAdapter for {}: {agg_expr:?}",
            agg_expr.name()
        );
        let agg_expr_captured = agg_expr.clone();
        let factory = move || agg_expr_captured.create_accumulator();
        Ok(Box::new(GroupsAccumulatorAdapter::new(factory)))
    }
}

/// Extracts a successful Ok(_) or returns Poll::Ready(Some(Err(e))) with errors
macro_rules! extract_ok {
    ($RES: expr) => {{
        match $RES {
            Ok(v) => v,
            Err(e) => return Poll::Ready(Some(Err(e))),
        }
    }};
}

impl Stream for GroupedHashAggregateStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let elapsed_compute = self.baseline_metrics.elapsed_compute().clone();

        loop {
            match &mut self.exec_state {
                ExecutionState::ReadingInput => 'reading_input: {
                    match ready!(self.input.poll_next_unpin(cx)) {
                        // new batch to aggregate
                        Some(Ok(batch)) => {
                            let timer = elapsed_compute.timer();
                            let input_rows = batch.num_rows();

                            // Make sure we have enough capacity for `batch`, otherwise spill
                            extract_ok!(self.spill_previous_if_necessary(&batch));

                            // Do the grouping
                            extract_ok!(self.group_aggregate_batch(batch));

                            self.update_skip_aggregation_probe(input_rows);

                            // If we can begin emitting rows, do so,
                            // otherwise keep consuming input
                            assert!(!self.input_done);

                            // If the number of group values equals or exceeds the soft limit,
                            // emit all groups and switch to producing output
                            if self.hit_soft_group_limit() {
                                timer.done();
                                extract_ok!(self.set_input_done_and_produce_output());
                                // make sure the exec_state just set is not overwritten below
                                break 'reading_input;
                            }

                            if let Some(to_emit) = self.group_ordering.emit_to() {
                                let mut batch = extract_ok!(self.emit(to_emit, false));
                                assert_eq!(batch.len(), 1);
                                let batch = batch.pop().unwrap();
                                self.exec_state = ExecutionState::ProducingOutput(batch);
                                timer.done();
                                // make sure the exec_state just set is not overwritten below
                                break 'reading_input;
                            }

                            extract_ok!(self.emit_early_if_necessary());

                            extract_ok!(self.switch_to_skip_aggregation());

                            timer.done();
                        }
                        Some(Err(e)) => {
                            // inner had error, return to caller
                            return Poll::Ready(Some(Err(e)));
                        }
                        None => {
                            // inner is done, emit all rows and switch to producing output
                            extract_ok!(self.set_input_done_and_produce_output());
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
                            let states = self.transform_to_states(batch)?;
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
                            self.exec_state = ExecutionState::Done;
                        }
                    }
                }

                ExecutionState::ProducingOutput(batch) => {
                    // slice off a part of the batch, if needed
                    let output_batch;
                    let batch = batch.clone();
                    let size = self.batch_size;
                    (self.exec_state, output_batch) = if batch.num_rows() <= size {
                        (
                            if self.input_done {
                                ExecutionState::Done
                            } else if self.should_skip_aggregation() {
                                ExecutionState::SkippingAggregation
                            } else {
                                ExecutionState::ReadingInput
                            },
                            batch,
                        )
                    } else {
                        // output first batch_size rows
                        let size = self.batch_size;
                        let num_remaining = batch.num_rows() - size;
                        let remaining = batch.slice(size, num_remaining);
                        let output = batch.slice(0, size);
                        (ExecutionState::ProducingOutput(remaining), output)
                    };
                    return Poll::Ready(Some(Ok(
                        output_batch.record_output(&self.baseline_metrics)
                    )));
                }

                ExecutionState::Done => {
                    // release the memory reservation since sending back output batch itself needs
                    // some memory reservation, so make some room for it.
                    self.clear_all();
                    let _ = self.update_memory_reservation();
                    return Poll::Ready(None);
                }

                ExecutionState::ProducingPartitionedOutput(parts) => {
                    // slice off a part of the batch, if needed
                    let batch_opt = parts.next_batch();
                    if let Some(batch) = batch_opt {
                        return Poll::Ready(Some(Ok(
                            batch.record_output(&self.baseline_metrics)
                        )));
                    } else {
                        self.exec_state = if self.input_done {
                            ExecutionState::Done
                        } else if self.should_skip_aggregation() {
                            ExecutionState::SkippingAggregation
                        } else {
                            ExecutionState::ReadingInput
                        };
                    }
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
    fn group_aggregate_batch(&mut self, batch: RecordBatch) -> Result<()> {
        // Evaluate the grouping expressions
        let group_by_values = if self.spill_state.is_stream_merging {
            evaluate_group_by(&self.spill_state.merging_group_by, &batch)?
        } else {
            evaluate_group_by(&self.group_by, &batch)?
        };

        // Evaluate the aggregation expressions.
        let input_values = if self.spill_state.is_stream_merging {
            evaluate_many(&self.spill_state.merging_aggregate_arguments, &batch)?
        } else {
            evaluate_many(&self.aggregate_arguments, &batch)?
        };

        // Evaluate the filter expressions, if any, against the inputs
        let filter_values = if self.spill_state.is_stream_merging {
            let filter_expressions = vec![None; self.accumulators.len()];
            evaluate_optional(&filter_expressions, &batch)?
        } else {
            evaluate_optional(&self.filter_expressions, &batch)?
        };

        if !self.group_values.is_partitioned() {
            self.group_aggregate_batch_single(
                &group_by_values,
                &input_values,
                &filter_values,
            )?;
        } else {
            self.group_aggregate_batch_partitioned(
                &group_by_values,
                &input_values,
                &filter_values,
            )?;
        }

        match self.update_memory_reservation() {
            // Here we can ignore `insufficient_capacity_err` because we will spill later,
            // but at least one batch should fit in the memory
            Err(DataFusionError::ResourcesExhausted(_))
                if self.group_values.len() >= self.batch_size =>
            {
                Ok(())
            }
            other => other,
        }
    }

    fn group_aggregate_batch_partitioned(
        &mut self,
        group_by_values: &[Vec<ArrayRef>],
        acc_values: &[Vec<ArrayRef>],
        acc_opt_filters: &[Option<ArrayRef>],
    ) -> Result<()> {
        assert!(
            self.mode == AggregateMode::Partial
                && matches!(self.group_ordering, GroupOrdering::None)
        );

        let group_values = self.group_values.as_partitioned_mut();

        let mut batch_indices = vec![];
        let mut offsets = vec![];

        for group_cols in group_by_values {
            // 1.Calculate `row_indices` and related `group_indices` for each partition
            group_values.intern(
                group_cols,
                &mut self.current_group_indices,
                &mut self.current_row_indices,
            )?;

            // 2.update the arrays in each partition to their accumulators
            //  - Reorder the arrays to make them sorted by partitions
            //  - Collect the `offsets`, and we can get arrays in partition through `slice`
            batch_indices.clear();
            offsets.clear();
            offsets.push(0);

            let mut offset_so_far = 0;
            for indices in self.current_row_indices.iter() {
                batch_indices.extend_from_slice(indices);
                offset_so_far += indices.len();
                offsets.push(offset_so_far);
            }
            let batch_indices = batch_indices.clone().into();
            let acc_values = acc_values
                .iter()
                .map(|values| get_arrayref_at_indices(values, &batch_indices))
                .collect::<Result<Vec<_>>>()?;
            let acc_opt_filters = acc_opt_filters
                .iter()
                .map(|opt_filter| {
                    let opt_filter = opt_filter.as_ref().map(|f| f.as_boolean());
                    get_filter_at_indices(opt_filter, &batch_indices)
                })
                .collect::<Result<Vec<_>>>()?;

            // Update the accumulators of each partition
            for (part_idx, part_start_end) in offsets.windows(2).enumerate() {
                let (offset, length) =
                    (part_start_end[0], part_start_end[1] - part_start_end[0]);

                // Gather the inputs to call the actual accumulator
                let iter = self.accumulators[part_idx]
                    .iter_mut()
                    .zip(acc_values.iter())
                    .zip(acc_opt_filters.iter());

                for ((acc, values), opt_filter) in iter {
                    let part_values = values
                        .iter()
                        .map(|array| array.slice(offset, length))
                        .collect::<Vec<_>>();

                    let part_opt_filter =
                        opt_filter.as_ref().map(|f| f.slice(offset, length));
                    let part_opt_filter =
                        part_opt_filter.as_ref().map(|filter| filter.as_boolean());

                    let group_indices = &self.current_group_indices[part_idx];
                    let total_num_groups = group_values.partition_len(part_idx);
                    acc.update_batch(
                        &part_values,
                        group_indices,
                        part_opt_filter,
                        total_num_groups,
                    )?;
                }
            }
        }

        Ok(())
    }

    fn group_aggregate_batch_single(
        &mut self,
        group_by_values: &[Vec<ArrayRef>],
        input_values: &[Vec<ArrayRef>],
        filter_values: &[Option<ArrayRef>],
    ) -> Result<()> {
        let group_values = self.group_values.as_single_mut();
        for group_cols in group_by_values {
            // calculate the group indices for each input row
            let starting_num_groups = group_values.len();
            group_values.intern(group_cols, &mut self.current_group_indices[0])?;
            let group_indices = &self.current_group_indices[0];

            // Update ordering information if necessary
            let total_num_groups = group_values.len();
            if total_num_groups > starting_num_groups {
                self.group_ordering.new_groups(
                    group_cols,
                    group_indices,
                    total_num_groups,
                )?;
            }

            // Gather the inputs to call the actual accumulator
            let t = self.accumulators[0]
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
                        // if aggregation is over intermediate states,
                        // use merge
                        acc.merge_batch(
                            values,
                            group_indices,
                            opt_filter,
                            total_num_groups,
                        )?;
                    }
                }
            }
        }

        Ok(())
    }

    fn update_memory_reservation(&mut self) -> Result<()> {
        let acc = self.accumulators[0].iter().map(|x| x.size()).sum::<usize>();
        self.reservation.try_resize(
            acc + self.group_values.size()
                + self.group_ordering.size()
                + self.current_group_indices.allocated_size(),
        )
    }

    /// Create an output RecordBatch with the group keys and
    /// accumulator states/values specified in emit_to
    fn emit(&mut self, emit_to: EmitTo, spilling: bool) -> Result<Vec<RecordBatch>> {
        if !self.group_values.is_partitioned() {
            self.emit_single(emit_to, spilling)
        } else {
            self.emit_partitioned(emit_to)
        }
    }

    /// Create an output RecordBatch with the group keys and
    /// accumulator states/values specified in emit_to
    fn emit_single(
        &mut self,
        emit_to: EmitTo,
        spilling: bool,
    ) -> Result<Vec<RecordBatch>> {
        let schema = if spilling {
            Arc::clone(&self.spill_state.spill_schema)
        } else {
            self.schema()
        };

        if self.group_values.is_empty() {
            return Ok(vec![RecordBatch::new_empty(schema.clone())]);
        }

        let group_values = self.group_values.as_single_mut();
        let mut output = group_values.emit(emit_to)?;
        if let EmitTo::First(n) = emit_to {
            self.group_ordering.remove_groups(n);
        }

        // Next output each aggregate value
        for acc in self.accumulators[0].iter_mut() {
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

        // emit reduces the memory usage. Ignore Err from update_memory_reservation. Even if it is
        // over the target memory size after emission, we can emit again rather than returning Err.
        let _ = self.update_memory_reservation();
        let batch = RecordBatch::try_new(schema, output)?;

        Ok(vec![batch])
    }

    fn emit_partitioned(&mut self, emit_to: EmitTo) -> Result<Vec<RecordBatch>> {
        assert!(
            self.mode == AggregateMode::Partial
                && matches!(self.group_ordering, GroupOrdering::None)
        );

        let schema = self.schema();

        if self.group_values.is_empty() {
            return Ok(Vec::new());
        }

        let group_values = self.group_values.as_partitioned_mut();
        let mut partitioned_outputs = group_values.emit(emit_to)?;

        // Next output each aggregate value
        for (output, accs) in partitioned_outputs
            .iter_mut()
            .zip(self.accumulators.iter_mut())
        {
            for acc in accs.iter_mut() {
                output.extend(acc.state(emit_to)?);
            }
        }

        // emit reduces the memory usage. Ignore Err from update_memory_reservation. Even if it is
        // over the target memory size after emission, we can emit again rather than returning Err.
        let _ = self.update_memory_reservation();

        let batch_parts = partitioned_outputs
            .into_iter()
            .enumerate()
            .map(|(part_idx, part)| {
                let schema_with_metadata = Arc::new(
                    schema.as_ref().clone().with_metadata(
                        [("partition".to_owned(), part_idx.to_string())]
                            .into_iter()
                            .collect(),
                    ),
                );
                RecordBatch::try_new(schema_with_metadata, part)
                    .map_err(|e| arrow_datafusion_err!(e))
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(batch_parts)
    }

    /// Optimistically, [`Self::group_aggregate_batch`] allows to exceed the memory target slightly
    /// (~ 1 [`RecordBatch`]) for simplicity. In such cases, spill the data to disk and clear the
    /// memory. Currently only [`GroupOrdering::None`] is supported for spilling.
    fn spill_previous_if_necessary(&mut self, batch: &RecordBatch) -> Result<()> {
        // TODO: support group_ordering for spilling
        if self.group_values.len() > 0
            && batch.num_rows() > 0
            && matches!(self.group_ordering, GroupOrdering::None)
            && !matches!(self.mode, AggregateMode::Partial)
            && !self.spill_state.is_stream_merging
            && self.update_memory_reservation().is_err()
        {
            // Use input batch (Partial mode) schema for spilling because
            // the spilled data will be merged and re-evaluated later.
            self.spill_state.spill_schema = batch.schema();
            self.spill()?;
            self.clear_shrink(batch);
        }
        Ok(())
    }

    /// Emit all rows, sort them, and store them on disk.
    fn spill(&mut self) -> Result<()> {
        let mut emit = self.emit(EmitTo::All, true)?;
        assert_eq!(emit.len(), 1);
        let emit = emit.pop().unwrap();
        let sorted = sort_batch(&emit, &self.spill_state.spill_expr, None)?;
        let spillfile = self.runtime.disk_manager.create_tmp_file("HashAggSpill")?;
        // TODO: slice large `sorted` and write to multiple files in parallel
        spill_record_batch_by_size(
            &sorted,
            spillfile.path().into(),
            sorted.schema(),
            self.batch_size,
        )?;
        self.spill_state.spills.push(spillfile);
        Ok(())
    }

    /// Clear memory and shirk capacities to the size of the batch.
    fn clear_shrink(&mut self, batch: &RecordBatch) {
        let group_values = self.group_values.as_single_mut();
        group_values.clear_shrink(batch);
    }

    /// Clear memory and shirk capacities to zero.
    fn clear_all(&mut self) {
        let schema = self.schema();
        match &mut self.group_values {
            GroupValuesLike::Single(v) => {
                v.clear_shrink(&RecordBatch::new_empty(schema));
            }
            GroupValuesLike::Partitioned(v) => {
                self.current_group_indices.clear();
                self.current_row_indices.clear();
                v.clear();
            }
        }
    }

    /// Emit if the used memory exceeds the target for partial aggregation.
    /// Currently only [`GroupOrdering::None`] is supported for early emitting.
    /// TODO: support group_ordering for early emitting
    fn emit_early_if_necessary(&mut self) -> Result<()> {
        if self.group_values.len() >= self.batch_size
            && matches!(self.group_ordering, GroupOrdering::None)
            && matches!(self.mode, AggregateMode::Partial)
            && self.update_memory_reservation().is_err()
        {
            if !self.group_values.is_partitioned() {
                let n = self.group_values.len() / self.batch_size * self.batch_size;
                let mut batch = self.emit(EmitTo::First(n), false)?;
                let batch = batch.pop().unwrap();
                self.exec_state = ExecutionState::ProducingOutput(batch);
            } else {
                let batches = self.emit(EmitTo::All, false)?;
                self.exec_state =
                    ExecutionState::ProducingPartitionedOutput(PartitionedOutput::new(
                        batches,
                        self.batch_size,
                        self.group_values.num_partitions(),
                    ));
            }
        }
        Ok(())
    }

    /// At this point, all the inputs are read and there are some spills.
    /// Emit the remaining rows and create a batch.
    /// Conduct a streaming merge sort between the batch and spilled data. Since the stream is fully
    /// sorted, set `self.group_ordering` to Full, then later we can read with [`EmitTo::First`].
    fn update_merged_stream(&mut self) -> Result<()> {
        let mut batch = self.emit(EmitTo::All, true)?;
        assert_eq!(batch.len(), 1);
        let batch = batch.pop().unwrap();
        // clear up memory for streaming_merge
        self.clear_all();
        self.update_memory_reservation()?;
        let mut streams: Vec<SendableRecordBatchStream> = vec![];
        let expr = self.spill_state.spill_expr.clone();
        let schema = batch.schema();
        streams.push(Box::pin(RecordBatchStreamAdapter::new(
            Arc::clone(&schema),
            futures::stream::once(futures::future::lazy(move |_| {
                sort_batch(&batch, &expr, None)
            })),
        )));
        for spill in self.spill_state.spills.drain(..) {
            let stream = read_spill_as_stream(spill, Arc::clone(&schema), 2)?;
            streams.push(stream);
        }
        self.spill_state.is_stream_merging = true;
        self.input = streaming_merge(
            streams,
            schema,
            &self.spill_state.spill_expr,
            self.baseline_metrics.clone(),
            self.batch_size,
            None,
            self.reservation.new_empty(),
        )?;
        self.input_done = false;
        self.group_ordering = GroupOrdering::Full(GroupOrderingFull::new());
        Ok(())
    }

    /// returns true if there is a soft groups limit and the number of distinct
    /// groups we have seen is over that limit
    fn hit_soft_group_limit(&self) -> bool {
        let Some(group_values_soft_limit) = self.group_values_soft_limit else {
            return false;
        };
        group_values_soft_limit <= self.group_values.len()
    }

    /// common function for signalling end of processing of the input stream
    fn set_input_done_and_produce_output(&mut self) -> Result<()> {
        self.input_done = true;
        self.group_ordering.input_done();
        let elapsed_compute = self.baseline_metrics.elapsed_compute().clone();
        let timer = elapsed_compute.timer();
        self.exec_state = if self.spill_state.spills.is_empty() {
            if !self.group_values.is_partitioned() {
                let mut batch = self.emit(EmitTo::All, false)?;
                let batch = batch.pop().unwrap();
                ExecutionState::ProducingOutput(batch)
            } else {
                let batches = self.emit(EmitTo::All, false)?;
                ExecutionState::ProducingPartitionedOutput(PartitionedOutput::new(
                    batches,
                    self.batch_size,
                    self.group_values.num_partitions(),
                ))
            }
        } else {
            // If spill files exist, stream-merge them.
            self.update_merged_stream()?;
            ExecutionState::ReadingInput
        };
        timer.done();
        Ok(())
    }

    /// Updates skip aggregation probe state.
    ///
    /// In case stream has any spills, the probe is forcefully set to
    /// forbid aggregation skipping, and locked, since spilling resets
    /// total number of unique groups.
    ///
    /// Note: currently spilling is not supported for Partial aggregation
    fn update_skip_aggregation_probe(&mut self, input_rows: usize) {
        if let Some(probe) = self.skip_aggregation_probe.as_mut() {
            if !self.spill_state.spills.is_empty() {
                probe.forbid_skipping();
            } else {
                probe.update_state(input_rows, self.group_values.len());
            }
        };
    }

    /// In case the probe indicates that aggregation may be
    /// skipped, forces stream to produce currently accumulated output.
    fn switch_to_skip_aggregation(&mut self) -> Result<()> {
        if let Some(probe) = self.skip_aggregation_probe.as_mut() {
            if probe.should_skip() {
                if !self.group_values.is_partitioned() {
                    let mut batch = self.emit(EmitTo::All, false)?;
                    let batch = batch.pop().unwrap();
                    self.exec_state = ExecutionState::ProducingOutput(batch);
                } else {
                    let batches = self.emit(EmitTo::All, false)?;
                    self.exec_state = ExecutionState::ProducingPartitionedOutput(
                        PartitionedOutput::new(
                            batches,
                            self.batch_size,
                            self.group_values.num_partitions(),
                        ),
                    );
                }
            }
        }

        Ok(())
    }

    /// Returns true if the aggregation probe indicates that aggregation
    /// should be skipped.
    fn should_skip_aggregation(&self) -> bool {
        self.skip_aggregation_probe
            .as_ref()
            .is_some_and(|probe| probe.should_skip())
    }

    /// Transforms input batch to intermediate aggregate state, without grouping it
    fn transform_to_states(&self, batch: RecordBatch) -> Result<RecordBatch> {
        let group_values = evaluate_group_by(&self.group_by, &batch)?;
        let input_values = evaluate_many(&self.aggregate_arguments, &batch)?;
        let filter_values = evaluate_optional(&self.filter_expressions, &batch)?;

        let mut output = group_values.first().cloned().ok_or_else(|| {
            internal_datafusion_err!("group_values expected to have at least one element")
        })?;

        let iter = self.accumulators[0]
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

// /// ```text
// /// ┌─────┐           ┌─────┐            ┌─────┐
// /// │true │           │NULL │            │NULL │
// /// │true │    │      │true │            │true │
// /// │true │ ───┼───   │false│  ────────▶ │false│       filtered_nulls
// /// │false│    │      │NULL │            │false│
// /// │false│           │true │            │false│
// /// └─────┘           └─────┘            └─────┘
// /// array           opt_filter           output
// ///  .nulls()
// ///
// /// false = NULL    true  = pass          false = NULL       Meanings
// /// true  = valid   false = filter        true  = valid
// ///                 NULL  = filter
// /// ```
// ///
// /// [`GroupsAccumulator::convert_to_state`]: datafusion_expr_common::groups_accumulator::GroupsAccumulator
// fn create_partitioned_filter() {
//     BooleanBufferBuilder
// }
