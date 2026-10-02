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
//! The executor given here uses bounded memory (does not maintain all
//! the input data seen so far), which makes it appropriate when processing
//! infinite inputs.

use std::cmp::{Ordering, min};
use std::collections::VecDeque;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use super::utils::create_schema;
use crate::metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet};
use crate::statistics::{ChildStats, StatisticsArgs};
use crate::stream::EmptyRecordBatchStream;
use crate::windows::{
    calc_requirements, get_ordered_partition_by_indices, get_partition_by_sort_exprs,
    window_equivalence_properties,
};
use crate::{
    ChildrenPropertiesMode, ColumnStatistics, DisplayAs, DisplayFormatType, Distribution,
    ExecutionPlan, ExecutionPlanProperties, InputDistributionRequirements,
    InputOrderMode, PlanProperties, RecordBatchStream, ReplaceChildrenOptions,
    SendableRecordBatchStream, Statistics, WindowExpr, validate_child_count,
};

use arrow::compute::take_record_batch;
use arrow::{
    array::{Array, ArrayRef, RecordBatchOptions, UInt32Array, UInt32Builder},
    compute::{concat, concat_batches, sort_to_indices, take_arrays},
    datatypes::SchemaRef,
    record_batch::RecordBatch,
};
use datafusion_common::hash_utils::create_hashes;
use datafusion_common::stats::Precision;
use datafusion_common::tree_node::TreeNodeRecursion;
use datafusion_common::utils::{
    evaluate_partition_ranges, get_at_indices, get_row_at_idx,
};
use datafusion_common::{
    HashMap, Result, ScalarValue, arrow_datafusion_err, exec_datafusion_err, exec_err,
};
use datafusion_execution::TaskContext;
use datafusion_expr::ColumnarValue;
use datafusion_expr::window_state::{PartitionBatchState, WindowAggState};
use datafusion_physical_expr::window::{
    PartitionBatches, PartitionKey, PartitionWindowAggStates, WindowEvalContext,
    WindowState,
};
use datafusion_physical_expr_common::physical_expr::PhysicalExpr;
use datafusion_physical_expr_common::sort_expr::{
    OrderingRequirements, PhysicalSortExpr,
};

use crate::execution_plan::CardinalityEffect;
use datafusion_common::hash_utils::RandomState;
use futures::stream::Stream;
use futures::{StreamExt, ready};
use hashbrown::hash_table::HashTable;
use indexmap::IndexMap;
use log::debug;

/// Callback receiver for per-partition window state.
///
/// `state` is the result of [`Accumulator::state`], which is a `&mut self`
/// call whose trait doc states "this function should not be called twice."
/// Several built-in aggregates (`median`, `percentile_cont`, `string_agg`,
/// `min_max_bytes`/`min_max_struct`) `std::mem::take` their internal
/// buffers to build that state — so `state` is a destructive read, not a
/// snapshot. The exec fires this at most once per group; a callee that
/// needs the value beyond the callback must retain it (e.g. clone into
/// owned storage).
///
/// [`Accumulator::state`]: datafusion_expr::Accumulator::state
pub trait WindowStateObserver: Send + Sync {
    /// Invoked once per (output-partition-index, window-expression,
    /// PARTITION BY tuple) as each PARTITION BY group closes, for every
    /// aggregate window expression on the exec. Non-aggregate window
    /// functions (e.g. `row_number`, `rank`, `lead`/`lag`) do not fire this
    /// callback.
    ///
    /// # Arguments
    ///
    /// * `partition_idx` - Output partition index of the [`BoundedWindowAggExec`]
    ///   stream firing this callback.
    /// * `window_expr` - The window expression whose state just closed.
    /// * `partition_key` - The PARTITION BY tuple that just closed.
    /// * `state` - [`Accumulator::state`] for the closed group of
    ///   `window_expr`. See the trait-level doc for the destructive-read
    ///   contract.
    ///
    /// [`Accumulator::state`]: datafusion_expr::Accumulator::state
    fn finalize_window_aggregate(
        &self,
        partition_idx: usize,
        window_expr: &Arc<dyn WindowExpr>,
        partition_key: &PartitionKey,
        state: Vec<ScalarValue>,
    ) -> Result<()>;
}

/// Window execution plan
#[derive(Clone)]
pub struct BoundedWindowAggExec {
    /// Input plan
    input: Arc<dyn ExecutionPlan>,
    /// Window function expression
    window_expr: Vec<Arc<dyn WindowExpr>>,
    /// Schema after the window is run
    schema: SchemaRef,
    /// Execution metrics
    metrics: ExecutionPlanMetricsSet,
    /// Describes how the input is ordered relative to the partition keys
    pub input_order_mode: InputOrderMode,
    /// Partition by indices that define ordering
    // For example, if input ordering is ORDER BY a, b and window expression
    // contains PARTITION BY b, a; `ordered_partition_by_indices` would be 1, 0.
    // Similarly, if window expression contains PARTITION BY a, b; then
    // `ordered_partition_by_indices` would be 0, 1.
    // See `get_ordered_partition_by_indices` for more details.
    ordered_partition_by_indices: Vec<usize>,
    /// Cache holding plan properties like equivalences, output partitioning etc.
    cache: Arc<PlanProperties>,
    /// If `can_rerepartition` is false, partition_keys is always empty.
    can_repartition: bool,
    /// Invoked at partition-close to publish finalized per-partition window
    /// state. Storage and multi-group handling are the caller's; the exec is
    /// a pure event source.
    state_observer: Option<Arc<dyn WindowStateObserver>>,
}

impl std::fmt::Debug for BoundedWindowAggExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BoundedWindowAggExec")
            .field("input", &self.input)
            .field("window_expr", &self.window_expr)
            .field("schema", &self.schema)
            .field("metrics", &self.metrics)
            .field("input_order_mode", &self.input_order_mode)
            .field(
                "ordered_partition_by_indices",
                &self.ordered_partition_by_indices,
            )
            .field("cache", &self.cache)
            .field("can_repartition", &self.can_repartition)
            .field(
                "state_observer",
                &self.state_observer.as_ref().map(|_| "..."),
            )
            .finish()
    }
}

impl BoundedWindowAggExec {
    /// Create a new execution plan for window aggregates
    pub fn try_new(
        window_expr: Vec<Arc<dyn WindowExpr>>,
        input: Arc<dyn ExecutionPlan>,
        input_order_mode: InputOrderMode,
        can_repartition: bool,
    ) -> Result<Self> {
        let schema = create_schema(&input.schema(), &window_expr)?;
        let schema = Arc::new(schema);
        let partition_by_exprs = window_expr[0].partition_by();
        let ordered_partition_by_indices = match &input_order_mode {
            InputOrderMode::Sorted => {
                let indices = get_ordered_partition_by_indices(
                    window_expr[0].partition_by(),
                    &input,
                )?;
                if indices.len() == partition_by_exprs.len() {
                    indices
                } else {
                    (0..partition_by_exprs.len()).collect::<Vec<_>>()
                }
            }
            InputOrderMode::PartiallySorted(ordered_indices) => ordered_indices.clone(),
            InputOrderMode::Linear => {
                vec![]
            }
        };
        let cache = Self::compute_properties(&input, &schema, &window_expr)?;
        Ok(Self {
            input,
            window_expr,
            schema,
            metrics: ExecutionPlanMetricsSet::new(),
            input_order_mode,
            ordered_partition_by_indices,
            cache: Arc::new(cache),
            can_repartition,
            state_observer: None,
        })
    }

    /// Install (or clear) a [`WindowStateObserver`] that receives each
    /// PARTITION BY group's finalized window state at partition close.
    ///
    /// Errors when `observer` is `Some` and any window expression on this
    /// exec has a non-ever-expanding frame (i.e. its start bound is not
    /// `UNBOUNDED PRECEDING`). Those frames use `SlidingAggregateWindowExpr`
    /// under the hood, whose accumulator calls `retract_batch` — at
    /// partition close the accumulator holds only the last frame's rows,
    /// not the partition aggregate, so the observed state would silently
    /// misrepresent the group.
    pub fn with_state_observer(
        mut self,
        observer: Option<Arc<dyn WindowStateObserver>>,
    ) -> Result<Self> {
        if observer.is_some() {
            for expr in &self.window_expr {
                if !expr.get_window_frame().is_ever_expanding() {
                    return exec_err!(
                        "cannot install WindowStateObserver on BoundedWindowAggExec \
                         with a sliding aggregate window frame (start != \
                         UNBOUNDED PRECEDING) for `{}`; sliding accumulator state \
                         is frame-only, not the partition aggregate",
                        expr.name()
                    );
                }
            }
        }
        self.state_observer = observer;
        Ok(self)
    }

    /// The currently-installed [`WindowStateObserver`], if any. Optimizer
    /// rules that rebuild this exec via
    /// [`crate::windows::get_best_fitting_window`] or a direct `try_new`
    /// call must read this and reinstall it on the new exec, otherwise a
    /// caller-installed observer is silently dropped by the rewrite.
    pub fn state_observer(&self) -> Option<&Arc<dyn WindowStateObserver>> {
        self.state_observer.as_ref()
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

    /// Initializes the appropriate [`PartitionSearcher`] implementation from
    /// the state.
    fn get_search_algo(&self) -> Result<Box<dyn PartitionSearcher>> {
        let partition_by_sort_keys = self.partition_by_sort_keys()?;
        let ordered_partition_by_indices = self.ordered_partition_by_indices.clone();
        let input_schema = self.input().schema();
        Ok(match &self.input_order_mode {
            InputOrderMode::Sorted => {
                // In Sorted mode, all partition by columns should be ordered.
                if self.window_expr()[0].partition_by().len()
                    != ordered_partition_by_indices.len()
                {
                    return exec_err!(
                        "All partition by columns should have an ordering in Sorted mode."
                    );
                }
                Box::new(SortedSearch {
                    partition_by_sort_keys,
                    ordered_partition_by_indices,
                    input_schema,
                })
            }
            InputOrderMode::Linear | InputOrderMode::PartiallySorted(_) => Box::new(
                LinearSearch::new(ordered_partition_by_indices, input_schema),
            ),
        })
    }

    /// This function creates the cache object that stores the plan properties such as schema, equivalence properties, ordering, partitioning, etc.
    fn compute_properties(
        input: &Arc<dyn ExecutionPlan>,
        schema: &SchemaRef,
        window_exprs: &[Arc<dyn WindowExpr>],
    ) -> Result<PlanProperties> {
        // Calculate equivalence properties:
        let eq_properties = window_equivalence_properties(schema, input, window_exprs)?;

        // As we can have repartitioning using the partition keys, this can
        // be either one or more than one, depending on the presence of
        // repartitioning.
        let output_partitioning = input.output_partitioning().clone();

        // Construct properties cache
        Ok(PlanProperties::new(
            eq_properties,
            output_partitioning,
            // TODO: Emission type and boundedness information can be enhanced here
            input.pipeline_behavior(),
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

    fn statistics_helper(&self, statistics: Statistics) -> Result<Statistics> {
        let win_cols = self.window_expr.len();
        let input_cols = self.input.schema().fields().len();
        // TODO stats: some windowing function will maintain invariants such as min, max...
        let mut column_statistics = Vec::with_capacity(win_cols + input_cols);
        // copy stats of the input to the beginning of the schema.
        column_statistics.extend(statistics.column_statistics);
        for _ in 0..win_cols {
            column_statistics.push(ColumnStatistics::new_unknown())
        }
        Ok(Statistics {
            num_rows: statistics.num_rows,
            column_statistics,
            total_byte_size: Precision::Absent,
        })
    }
}

impl DisplayAs for BoundedWindowAggExec {
    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "BoundedWindowAggExec: ")?;
                let g: Vec<String> = self
                    .window_expr
                    .iter()
                    .map(|e| {
                        let field = match e.field() {
                            Ok(f) => f.to_string(),
                            Err(e) => format!("{e:?}"),
                        };
                        format!(
                            "{}: {}, frame: {}",
                            e.name().to_owned(),
                            field,
                            e.get_window_frame()
                        )
                    })
                    .collect();
                let mode = &self.input_order_mode;
                write!(f, "wdw=[{}], mode=[{:?}]", g.join(", "), mode)?;
            }
            DisplayFormatType::TreeRender => {
                let g: Vec<String> = self
                    .window_expr
                    .iter()
                    .map(|e| e.name().to_owned().to_string())
                    .collect();
                writeln!(f, "select_list={}", g.join(", "))?;

                let mode = &self.input_order_mode;
                writeln!(f, "mode={mode:?}")?;
            }
        }
        Ok(())
    }
}

impl ExecutionPlan for BoundedWindowAggExec {
    fn name(&self) -> &'static str {
        "BoundedWindowAggExec"
    }

    /// Return a reference to Any that can be used for downcasting
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.cache
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn apply_expressions(
        &self,
        f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        let expressions = self.window_expr.iter().flat_map(|window_expr| {
            let expressions = window_expr.all_expressions();
            expressions
                .args
                .into_iter()
                .chain(expressions.partition_by_exprs)
                .chain(expressions.order_by_exprs)
        });
        crate::apply_expression_roots(expressions, f)
    }

    fn required_input_ordering(&self) -> Vec<Option<OrderingRequirements>> {
        let partition_bys = self.window_expr()[0].partition_by();
        let order_keys = self.window_expr()[0].order_by();
        let partition_bys = self
            .ordered_partition_by_indices
            .iter()
            .map(|idx| &partition_bys[*idx]);
        vec![calc_requirements(partition_bys, order_keys)]
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        self.input_distribution_requirements().into_per_child()
    }

    fn input_distribution_requirements(&self) -> InputDistributionRequirements {
        if self.partition_keys().is_empty() {
            debug!("No partition defined for BoundedWindowAggExec!!!");
            InputDistributionRequirements::new(vec![Distribution::SinglePartition])
        } else {
            InputDistributionRequirements::new(vec![Distribution::KeyPartitioned(
                self.partition_keys(),
            )])
        }
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }

    fn replace_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
        options: ReplaceChildrenOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        validate_child_count!(self, children);
        match options.children_properties {
            ChildrenPropertiesMode::Keep => Ok(Arc::new(Self {
                input: children.swap_remove(0),
                metrics: ExecutionPlanMetricsSet::new(),
                ..Self::clone(&*self)
            })),
            ChildrenPropertiesMode::Recompute => {
                let new = BoundedWindowAggExec::try_new(
                    self.window_expr.clone(),
                    Arc::clone(&children[0]),
                    self.input_order_mode.clone(),
                    self.can_repartition,
                )?
                .with_state_observer(self.state_observer.clone())?;
                Ok(Arc::new(new))
            }
        }
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.replace_children(
            children,
            ReplaceChildrenOptions::new(ChildrenPropertiesMode::Recompute),
        )
    }

    fn with_new_children_and_same_properties(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        self.replace_children(
            children,
            ReplaceChildrenOptions::new(ChildrenPropertiesMode::Keep),
        )
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let input = self.input.execute(partition, context)?;
        let search_mode = self.get_search_algo()?;
        let stream = Box::pin(BoundedWindowAggStream::new(
            Arc::clone(&self.schema),
            self.window_expr.clone(),
            input,
            BaselineMetrics::new(&self.metrics, partition),
            search_mode,
            partition,
            self.state_observer.clone(),
        )?);
        Ok(stream)
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn child_stats_requests(&self, partition: Option<usize>) -> Vec<ChildStats> {
        vec![ChildStats::At(partition)]
    }

    fn statistics_from_inputs(
        &self,
        input_stats: &[Arc<Statistics>],
        _args: &StatisticsArgs,
    ) -> Result<Arc<Statistics>> {
        let input_stat = input_stats[0].as_ref().clone();
        Ok(Arc::new(self.statistics_helper(input_stat)?))
    }

    fn cardinality_effect(&self) -> CardinalityEffect {
        CardinalityEffect::Equal
    }

    #[cfg(feature = "proto")]
    fn try_to_proto(
        &self,
        ctx: &crate::proto::ExecutionPlanEncodeCtx<'_>,
    ) -> Result<Option<datafusion_proto_models::protobuf::PhysicalPlanNode>> {
        use super::proto::encode_physical_window_expr;
        use datafusion_proto_common::protobuf_common::EmptyMessage;
        use datafusion_proto_models::protobuf;
        use protobuf::window_agg_exec_node::InputOrderMode as ProtoInputOrderMode;

        // Exhaustive destructure: adding a field to `BoundedWindowAggExec`
        // without deciding how it is serialized is a compile error, not a
        // silent round-trip gap.
        let Self {
            input,
            window_expr,
            // Derived at construction by `create_schema` from the input schema
            // and the window expressions.
            schema: _,
            // Runtime execution state, rebuilt empty on decode.
            metrics: _,
            input_order_mode,
            // Derived at construction from `input_order_mode` and the window
            // expressions' PARTITION BY.
            ordered_partition_by_indices: _,
            // Derived at construction by `Self::compute_properties`.
            cache: _,
            // No wire field of its own; it is folded into `partition_keys`
            // below, since `partition_keys()` returns an empty vec when this is
            // false and the decoder recovers it as `!partition_keys.is_empty()`.
            can_repartition: _,
            // Runtime callback installed after planning; not part of the wire
            // format. Any decoder that needs it must reinstall via
            // `with_state_observer`.
            state_observer: _,
        } = self;

        let input = ctx.encode_child(input)?;
        let window_expr = window_expr
            .iter()
            .map(|expr| encode_physical_window_expr(expr, ctx))
            .collect::<Result<Vec<_>>>()?;
        let partition_keys = self
            .partition_keys()
            .iter()
            .map(|expr| ctx.encode_expr(expr))
            .collect::<Result<Vec<_>>>()?;
        // A `Some(input_order_mode)` is what tells the shared `Window` decode
        // arm to rebuild a `BoundedWindowAggExec` rather than a `WindowAggExec`.
        let input_order_mode = match input_order_mode {
            InputOrderMode::Linear => ProtoInputOrderMode::Linear(EmptyMessage {}),
            InputOrderMode::PartiallySorted(columns) => {
                ProtoInputOrderMode::PartiallySorted(
                    protobuf::PartiallySortedInputOrderMode {
                        columns: columns.iter().map(|column| *column as u64).collect(),
                    },
                )
            }
            InputOrderMode::Sorted => ProtoInputOrderMode::Sorted(EmptyMessage {}),
        };

        Ok(Some(protobuf::PhysicalPlanNode {
            physical_plan_type: Some(
                protobuf::physical_plan_node::PhysicalPlanType::Window(Box::new(
                    protobuf::WindowAggExecNode {
                        input: Some(Box::new(input)),
                        window_expr,
                        partition_keys,
                        input_order_mode: Some(input_order_mode),
                    },
                )),
            ),
        }))
    }
}

/// Trait that specifies how we search for (or calculate) partitions. It has two
/// implementations: [`SortedSearch`] and [`LinearSearch`].
trait PartitionSearcher: Send {
    /// This method constructs output columns using the result of each window expression
    /// (each entry in the output vector comes from a window expression).
    /// Executor when producing output concatenates `input_buffer` (corresponding section), and
    /// result of this function to generate output `RecordBatch`. `input_buffer` is used to determine
    /// which sections of the window expression results should be used to generate output.
    /// `partition_buffers` contains corresponding section of the `RecordBatch` for each partition.
    /// `window_agg_states` stores per partition state for each window expression.
    /// None case means that no result is generated
    /// `Some(Vec<ArrayRef>)` is the result of each window expression.
    fn calculate_out_columns(
        &mut self,
        input_buffer: &RecordBatch,
        window_agg_states: &[PartitionWindowAggStates],
        partition_buffers: &mut PartitionBatches,
        window_expr: &[Arc<dyn WindowExpr>],
    ) -> Result<Option<Vec<ArrayRef>>>;

    /// Determine whether `[InputOrderMode]` is `[InputOrderMode::Linear]` or not.
    fn is_mode_linear(&self) -> bool {
        false
    }

    // Constructs corresponding batches for each partition for the record_batch.
    fn evaluate_partition_batches(
        &mut self,
        record_batch: &RecordBatch,
        window_expr: &[Arc<dyn WindowExpr>],
    ) -> Result<Vec<(PartitionKey, RecordBatch)>>;

    /// Prunes the state.
    fn prune(&mut self, _n_out: usize) {}

    /// Marks the partition as done if we are sure that corresponding partition
    /// cannot receive any more values.
    fn mark_partition_end(&self, partition_buffers: &mut PartitionBatches);

    /// Updates `input_buffer` and `partition_buffers` with the new `record_batch`.
    fn update_partition_batch(
        &mut self,
        input_buffer: &mut RecordBatch,
        record_batch: RecordBatch,
        window_expr: &[Arc<dyn WindowExpr>],
        partition_buffers: &mut PartitionBatches,
    ) -> Result<()> {
        if record_batch.num_rows() == 0 {
            return Ok(());
        }
        let partition_batches =
            self.evaluate_partition_batches(&record_batch, window_expr)?;
        for (partition_row, partition_batch) in partition_batches {
            if let Some(partition_batch_state) = partition_buffers.get_mut(&partition_row)
            {
                partition_batch_state.extend(&partition_batch)?
            } else {
                let options = RecordBatchOptions::new()
                    .with_row_count(Some(partition_batch.num_rows()));
                // Use input_schema for the buffer schema, not `record_batch.schema()`
                // as it may not have the "correct" schema in terms of output
                // nullability constraints. For details, see the following issue:
                // https://github.com/apache/datafusion/issues/9320
                let partition_batch = RecordBatch::try_new_with_options(
                    Arc::clone(self.input_schema()),
                    partition_batch.columns().to_vec(),
                    &options,
                )?;
                let partition_batch_state =
                    PartitionBatchState::new_with_batch(partition_batch);
                partition_buffers.insert(partition_row, partition_batch_state);
            }
        }

        self.mark_partition_end(partition_buffers);

        *input_buffer = if input_buffer.num_rows() == 0 {
            record_batch
        } else {
            concat_batches(self.input_schema(), [input_buffer, &record_batch])?
        };

        Ok(())
    }

    fn input_schema(&self) -> &SchemaRef;
}

/// This object encapsulates the algorithm state for a simple linear scan
/// algorithm for computing partitions.
pub struct LinearSearch {
    /// Keeps the hash of input buffer calculated from PARTITION BY columns.
    /// Its length is equal to the `input_buffer` length.
    input_buffer_hashes: VecDeque<u64>,
    /// Used during hash value calculation.
    random_state: RandomState,
    /// Input ordering and partition by key ordering need not be the same, so
    /// this vector stores the mapping between them. For instance, if the input
    /// is ordered by a, b and the window expression contains a PARTITION BY b, a
    /// clause, this attribute stores [1, 0].
    ordered_partition_by_indices: Vec<usize>,
    /// We use this [`HashTable`] to calculate unique partitions for each new
    /// RecordBatch. First entry in the tuple is the hash value, the second
    /// entry is the unique ID for each partition (increments from 0 to n).
    row_map_batch: HashTable<(u64, usize)>,
    /// We use this [`HashTable`] to calculate the output columns that we can
    /// produce at each cycle. First entry in the tuple is the hash value, the
    /// second entry is the unique ID for each partition (increments from 0 to n).
    /// The third entry stores how many new outputs are calculated for the
    /// corresponding partition.
    row_map_out: HashTable<(u64, usize, usize)>,
    input_schema: SchemaRef,
}

impl PartitionSearcher for LinearSearch {
    /// This method constructs output columns using the result of each window expression.
    // Assume input buffer is         |      Partition Buffers would be (Where each partition and its data is separated)
    // a, 2                           |      a, 2
    // b, 2                           |      a, 2
    // a, 2                           |      a, 2
    // b, 2                           |
    // a, 2                           |      b, 2
    // b, 2                           |      b, 2
    // b, 2                           |      b, 2
    //                                |      b, 2
    // Also assume we happen to calculate 2 new values for a, and 3 for b (To be calculate missing values we may need to consider future values).
    // Partition buffers effectively will be
    // a, 2, 1
    // a, 2, 2
    // a, 2, (missing)
    //
    // b, 2, 1
    // b, 2, 2
    // b, 2, 3
    // b, 2, (missing)
    // When partition buffers are mapped back to the original record batch. Result becomes
    // a, 2, 1
    // b, 2, 1
    // a, 2, 2
    // b, 2, 2
    // a, 2, (missing)
    // b, 2, 3
    // b, 2, (missing)
    // This function calculates the column result of window expression(s) (First 4 entry of 3rd column in the above section.)
    // 1
    // 1
    // 2
    // 2
    // Above section corresponds to calculated result which can be emitted without breaking input buffer ordering.
    fn calculate_out_columns(
        &mut self,
        input_buffer: &RecordBatch,
        window_agg_states: &[PartitionWindowAggStates],
        partition_buffers: &mut PartitionBatches,
        window_expr: &[Arc<dyn WindowExpr>],
    ) -> Result<Option<Vec<ArrayRef>>> {
        let partition_output_indices = self.calc_partition_output_indices(
            input_buffer,
            window_agg_states,
            window_expr,
        )?;

        let n_window_col = window_agg_states.len();
        let mut new_columns = vec![vec![]; n_window_col];
        // Size of all_indices can be at most input_buffer.num_rows():
        let mut all_indices = UInt32Builder::with_capacity(input_buffer.num_rows());
        for (row, indices) in partition_output_indices {
            let length = indices.len();
            for (idx, window_agg_state) in window_agg_states.iter().enumerate() {
                let partition = &window_agg_state[&row];
                let values = Arc::clone(&partition.state.out_col.slice(0, length));
                new_columns[idx].push(values);
            }
            let partition_batch_state = &mut partition_buffers[&row];
            // Store how many rows are generated for each partition
            partition_batch_state.n_out_row = length;
            // For each row keep corresponding index in the input record batch
            all_indices.append_slice(&indices);
        }
        let all_indices = all_indices.finish();
        if all_indices.is_empty() {
            // We couldn't generate any new value, return early:
            return Ok(None);
        }

        // Concatenate results for each column by converting `Vec<Vec<ArrayRef>>`
        // to Vec<ArrayRef> where inner `Vec<ArrayRef>`s are converted to `ArrayRef`s.
        let new_columns = new_columns
            .iter()
            .map(|items| {
                concat(&items.iter().map(|e| e.as_ref()).collect::<Vec<_>>())
                    .map_err(|e| arrow_datafusion_err!(e))
            })
            .collect::<Result<Vec<_>>>()?;
        // We should emit columns according to row index ordering.
        let sorted_indices = sort_to_indices(&all_indices, None, None)?;
        // Construct new column according to row ordering. This fixes ordering
        take_arrays(&new_columns, &sorted_indices, None)
            .map(Some)
            .map_err(|e| arrow_datafusion_err!(e))
    }

    fn evaluate_partition_batches(
        &mut self,
        record_batch: &RecordBatch,
        window_expr: &[Arc<dyn WindowExpr>],
    ) -> Result<Vec<(PartitionKey, RecordBatch)>> {
        let partition_bys =
            evaluate_partition_by_column_values(record_batch, window_expr)?;
        // NOTE: In Linear or PartiallySorted modes, we are sure that
        //       `partition_bys` are not empty.
        let (mut keys, permutation, bounds) =
            self.compute_partition_permutation(&partition_bys, record_batch)?;
        if keys.len() == 1 {
            // The batch contains a single partition, so the gather below
            // would be an identity permutation; use the batch as-is.
            let key = keys.remove(0);
            return Ok(vec![(key, record_batch.clone())]);
        }
        // Reorder the batch with a single `take` so that each partition's
        // rows become contiguous, then hand each partition a zero-copy slice
        // of the result. The slices share the gathered batch's buffers;
        // `PartitionBatchState::extend` copies out of them the next time the
        // partition receives rows.
        let gathered = take_record_batch(record_batch, &UInt32Array::from(permutation))?;
        Ok(keys
            .into_iter()
            .zip(bounds.windows(2))
            .map(|(key, bound)| (key, gathered.slice(bound[0], bound[1] - bound[0])))
            .collect())
    }

    fn prune(&mut self, n_out: usize) {
        // Delete hashes for the rows that are outputted.
        self.input_buffer_hashes.drain(0..n_out);
    }

    fn mark_partition_end(&self, partition_buffers: &mut PartitionBatches) {
        // We should be in the `PartiallySorted` case, otherwise we can not
        // tell when we are at the end of a given partition.
        if !self.ordered_partition_by_indices.is_empty()
            && let Some((last_row, _)) = partition_buffers.last()
        {
            let last_sorted_cols = self
                .ordered_partition_by_indices
                .iter()
                .map(|idx| last_row[*idx].clone())
                .collect::<Vec<_>>();
            for (row, partition_batch_state) in partition_buffers.iter_mut() {
                let sorted_cols = self
                    .ordered_partition_by_indices
                    .iter()
                    .map(|idx| &row[*idx]);
                // All the partitions other than `last_sorted_cols` are done.
                // We are sure that we will no longer receive values for these
                // partitions (arrival of a new value would violate ordering).
                partition_batch_state.is_end = !sorted_cols.eq(&last_sorted_cols);
            }
        }
    }

    fn is_mode_linear(&self) -> bool {
        self.ordered_partition_by_indices.is_empty()
    }

    fn input_schema(&self) -> &SchemaRef {
        &self.input_schema
    }
}

impl LinearSearch {
    /// Initialize a new [`LinearSearch`] partition searcher.
    fn new(ordered_partition_by_indices: Vec<usize>, input_schema: SchemaRef) -> Self {
        LinearSearch {
            input_buffer_hashes: VecDeque::new(),
            random_state: Default::default(),
            ordered_partition_by_indices,
            row_map_batch: HashTable::with_capacity(256),
            row_map_out: HashTable::with_capacity(256),
            input_schema,
        }
    }

    /// Splits the rows of `batch` by partition, according to the PARTITION BY
    /// expression results in `columns`. Returns the distinct partition keys
    /// in first-appearance order, a permutation of the row indices of
    /// `batch` that groups each partition's rows together, and the
    /// boundaries of each partition's run of rows within that permutation:
    /// partition `p` occupies `permutation[bounds[p]..bounds[p + 1]]`, and
    /// its indices are in ascending (stream) order.
    fn compute_partition_permutation(
        &mut self,
        columns: &[ArrayRef],
        batch: &RecordBatch,
    ) -> Result<(Vec<PartitionKey>, Vec<u32>, Vec<usize>)> {
        let num_rows = batch.num_rows();
        let mut batch_hashes = vec![0; num_rows];
        create_hashes(columns, &self.random_state, &mut batch_hashes)?;
        self.input_buffer_hashes.extend(&batch_hashes);
        // reset row_map for new calculation
        self.row_map_batch.clear();
        let mut keys: Vec<PartitionKey> = vec![];
        // Partition id of each row, in row order:
        let mut row_partition_ids = Vec::with_capacity(num_rows);
        // Number of rows in each partition:
        let mut counts: Vec<usize> = vec![];
        for (hash, row_idx) in batch_hashes.into_iter().zip(0u32..) {
            let entry = self.row_map_batch.find_mut(hash, |(_, group_idx)| {
                let row = get_row_at_idx(columns, row_idx as usize).unwrap();
                // Handle hash collisions with an equality check:
                row == keys[*group_idx]
            });
            let group_idx = if let Some((_, group_idx)) = entry {
                *group_idx
            } else {
                let group_idx = keys.len();
                self.row_map_batch
                    .insert_unique(hash, (hash, group_idx), |(hash, _)| *hash);
                keys.push(get_row_at_idx(columns, row_idx as usize)?);
                counts.push(0);
                group_idx
            };
            row_partition_ids.push(group_idx);
            counts[group_idx] += 1;
        }
        // A prefix sum over the counts gives each partition's run boundaries
        // in the permutation.
        let mut bounds = Vec::with_capacity(counts.len() + 1);
        let mut total = 0;
        bounds.push(0);
        for count in counts {
            total += count;
            bounds.push(total);
        }
        // Scatter each row's index into its partition's run. Visiting rows
        // in ascending order keeps each run in ascending row order.
        let mut cursors: Vec<usize> = bounds[..bounds.len() - 1].to_vec();
        let mut permutation = vec![0u32; num_rows];
        for (row_idx, group_idx) in row_partition_ids.into_iter().enumerate() {
            permutation[cursors[group_idx]] = row_idx as u32;
            cursors[group_idx] += 1;
        }
        Ok((keys, permutation, bounds))
    }

    /// Calculates partition keys and result indices for each partition.
    /// The return value is a vector of tuples where the first entry stores
    /// the partition key (unique for each partition) and the second entry
    /// stores indices of the rows for which the partition is constructed.
    fn calc_partition_output_indices(
        &mut self,
        input_buffer: &RecordBatch,
        window_agg_states: &[PartitionWindowAggStates],
        window_expr: &[Arc<dyn WindowExpr>],
    ) -> Result<Vec<(PartitionKey, Vec<u32>)>> {
        let partition_by_columns =
            evaluate_partition_by_column_values(input_buffer, window_expr)?;
        // Reset the row_map state:
        self.row_map_out.clear();
        let mut partition_indices: Vec<(PartitionKey, Vec<u32>)> = vec![];
        for (hash, row_idx) in self.input_buffer_hashes.iter().zip(0u32..) {
            let entry = self.row_map_out.find_mut(*hash, |(_, group_idx, _)| {
                let row =
                    get_row_at_idx(&partition_by_columns, row_idx as usize).unwrap();
                row == partition_indices[*group_idx].0
            });
            if let Some((_, group_idx, n_out)) = entry {
                let (_, indices) = &mut partition_indices[*group_idx];
                if indices.len() >= *n_out {
                    break;
                }
                indices.push(row_idx);
            } else {
                let row = get_row_at_idx(&partition_by_columns, row_idx as usize)?;
                let min_out = window_agg_states
                    .iter()
                    .map(|window_agg_state| {
                        window_agg_state
                            .get(&row)
                            .map(|partition| partition.state.out_col.len())
                            .unwrap_or(0)
                    })
                    .min()
                    .unwrap_or(0);
                if min_out == 0 {
                    break;
                }
                self.row_map_out.insert_unique(
                    *hash,
                    (*hash, partition_indices.len(), min_out),
                    |(hash, _, _)| *hash,
                );
                partition_indices.push((row, vec![row_idx]));
            }
        }
        Ok(partition_indices)
    }
}

/// This object encapsulates the algorithm state for sorted searching
/// when computing partitions.
pub struct SortedSearch {
    /// Stores partition by columns and their ordering information
    partition_by_sort_keys: Vec<PhysicalSortExpr>,
    /// Input ordering and partition by key ordering need not be the same, so
    /// this vector stores the mapping between them. For instance, if the input
    /// is ordered by a, b and the window expression contains a PARTITION BY b, a
    /// clause, this attribute stores [1, 0].
    ordered_partition_by_indices: Vec<usize>,
    input_schema: SchemaRef,
}

impl PartitionSearcher for SortedSearch {
    /// This method constructs new output columns using the result of each window expression.
    fn calculate_out_columns(
        &mut self,
        _input_buffer: &RecordBatch,
        window_agg_states: &[PartitionWindowAggStates],
        partition_buffers: &mut PartitionBatches,
        _window_expr: &[Arc<dyn WindowExpr>],
    ) -> Result<Option<Vec<ArrayRef>>> {
        let n_out = self.calculate_n_out_row(window_agg_states, partition_buffers);
        if n_out == 0 {
            Ok(None)
        } else {
            window_agg_states
                .iter()
                .map(|map| get_aggregate_result_out_column(map, n_out).map(Some))
                .collect()
        }
    }

    fn evaluate_partition_batches(
        &mut self,
        record_batch: &RecordBatch,
        _window_expr: &[Arc<dyn WindowExpr>],
    ) -> Result<Vec<(PartitionKey, RecordBatch)>> {
        let num_rows = record_batch.num_rows();
        // Calculate result of partition by column expressions
        let partition_columns = self
            .partition_by_sort_keys
            .iter()
            .map(|elem| elem.evaluate_to_sort_column(record_batch))
            .collect::<Result<Vec<_>>>()?;
        // Reorder `partition_columns` such that its ordering matches input ordering.
        let partition_columns_ordered =
            get_at_indices(&partition_columns, &self.ordered_partition_by_indices)?;
        let partition_points =
            evaluate_partition_ranges(num_rows, &partition_columns_ordered)?;
        let partition_bys = partition_columns
            .into_iter()
            .map(|arr| arr.values)
            .collect::<Vec<ArrayRef>>();

        partition_points
            .iter()
            .map(|range| {
                let row = get_row_at_idx(&partition_bys, range.start)?;
                let len = range.end - range.start;
                let slice = record_batch.slice(range.start, len);
                Ok((row, slice))
            })
            .collect::<Result<Vec<_>>>()
    }

    fn mark_partition_end(&self, partition_buffers: &mut PartitionBatches) {
        // In Sorted case. We can mark all partitions besides last partition as ended.
        // We are sure that those partitions will never receive any values.
        // (Otherwise ordering invariant is violated.)
        let n_partitions = partition_buffers.len();
        for (idx, (_, partition_batch_state)) in partition_buffers.iter_mut().enumerate()
        {
            partition_batch_state.is_end |= idx < n_partitions - 1;
        }
    }

    fn input_schema(&self) -> &SchemaRef {
        &self.input_schema
    }
}

impl SortedSearch {
    /// Calculates how many rows we can output.
    fn calculate_n_out_row(
        &mut self,
        window_agg_states: &[PartitionWindowAggStates],
        partition_buffers: &mut PartitionBatches,
    ) -> usize {
        // Different window aggregators may produce results at different rates.
        // We produce the overall batch result only as fast as the slowest one.
        let mut counts = vec![];
        let out_col_counts = window_agg_states.iter().map(|window_agg_state| {
            // Store how many elements are generated for the current
            // window expression:
            let mut cur_window_expr_out_result_len = 0;
            // We iterate over `window_agg_state`, which is an IndexMap.
            // Iterations follow the insertion order, hence we preserve
            // sorting when partition columns are sorted.
            let mut per_partition_out_results = HashMap::new();
            for (row, WindowState { state, .. }) in window_agg_state.iter() {
                cur_window_expr_out_result_len += state.out_col.len();
                let count = per_partition_out_results.entry(row).or_insert(0);
                if *count < state.out_col.len() {
                    *count = state.out_col.len();
                }
                // If we do not generate all results for the current
                // partition, we do not generate results for next
                // partition --  otherwise we will lose input ordering.
                if state.n_row_result_missing > 0 {
                    break;
                }
            }
            counts.push(per_partition_out_results);
            cur_window_expr_out_result_len
        });
        argmin(out_col_counts).map_or(0, |(min_idx, minima)| {
            let mut slowest_partition = counts.swap_remove(min_idx);
            for (partition_key, partition_batch) in partition_buffers.iter_mut() {
                if let Some(count) = slowest_partition.remove(partition_key) {
                    partition_batch.n_out_row = count;
                }
            }
            minima
        })
    }
}

/// Calculates partition by expression results for each window expression
/// on `record_batch`.
fn evaluate_partition_by_column_values(
    record_batch: &RecordBatch,
    window_expr: &[Arc<dyn WindowExpr>],
) -> Result<Vec<ArrayRef>> {
    window_expr[0]
        .partition_by()
        .iter()
        .map(|item| match item.evaluate(record_batch)? {
            ColumnarValue::Array(array) => Ok(array),
            ColumnarValue::Scalar(scalar) => {
                scalar.to_array_of_size(record_batch.num_rows())
            }
        })
        .collect()
}

/// Stream for the bounded window aggregation plan.
pub struct BoundedWindowAggStream {
    schema: SchemaRef,
    input: SendableRecordBatchStream,
    /// The record batch executor receives as input (i.e. the columns needed
    /// while calculating aggregation results).
    input_buffer: RecordBatch,
    /// Each partition's rows, accumulated across input batches. All window
    /// expressions calculate their results against these shared rows without
    /// copying.
    partition_buffers: PartitionBatches,
    /// An executor can run multiple window expressions if the PARTITION BY
    /// and ORDER BY sections are same. We keep state of the each window
    /// expression inside `window_agg_states`.
    window_agg_states: Vec<PartitionWindowAggStates>,
    finished: bool,
    window_expr: Vec<Arc<dyn WindowExpr>>,
    baseline_metrics: BaselineMetrics,
    /// Search mode for partition columns. This determines the algorithm with
    /// which we group each partition.
    search_mode: Box<dyn PartitionSearcher>,
    /// In `Linear` mode, a single-row batch containing the most recent input
    /// row (whichever partition that row belongs to); `None` in other modes
    /// and before the first non-empty batch arrives. Since in `Linear` mode
    /// the input is sorted by the first ORDER BY column, no future input row
    /// -- in any partition -- can precede this row in that column. Every
    /// partition's evaluation consults this bound to decide whether pending
    /// window frames can be finalized before the partition receives more
    /// data (which in turn allows buffered state to be pruned). Note that
    /// only the first ORDER BY column provides this guarantee. As a counter
    /// example, consider `PARTITION BY b, ORDER BY a, c` when the input is
    /// sorted by `[a, b, c]`: the mode will be `Linear`, but the last row of
    /// the input is the "last" data in terms of `[a, b, c]`, not in terms of
    /// the ordering requirement `[a, c]`. Hence, only column `a` can serve
    /// as a guarantee of the "last" data across partitions. In the `Sorted`
    /// and `PartiallySorted` modes, the leading ordering separates
    /// partitions, so finished partitions are pruned eagerly instead and no
    /// such bound is needed.
    most_recent_row: Option<RecordBatch>,
    /// Output partition index this stream serves; passed as the first
    /// argument to [`WindowStateObserver::finalize_window_aggregate`].
    partition_idx: usize,
    /// If set, invoked from [`Self::publish_finalized_states`] with the
    /// finalized per-window-expression state for every partition key that is
    /// about to be dropped.
    state_observer: Option<Arc<dyn WindowStateObserver>>,
}

impl BoundedWindowAggStream {
    /// Fire `observer` once per (window expression, partition key) for every
    /// group whose [`WindowAggState::is_end`] is true. Always mutates when
    /// called: [`datafusion_expr::Accumulator::state`] requires `&mut`, which
    /// propagates up here. The caller is responsible for deciding whether to
    /// fire (i.e. checking whether an observer is installed).
    ///
    /// Exactly-once per group is enforced by [`WindowState::aggregate_state`],
    /// which errors on second call; the `published` early-skip below avoids reaching the error.
    fn publish_finalized_states(
        &mut self,
        observer: &dyn WindowStateObserver,
    ) -> Result<()> {
        let partition_idx = self.partition_idx;
        for (expr_idx, per_expr) in self.window_agg_states.iter_mut().enumerate() {
            let window_expr = &self.window_expr[expr_idx];
            for (key, ws) in per_expr.iter_mut() {
                if ws.published || !ws.state.is_end {
                    continue;
                }
                if let Some(state) = ws.aggregate_state()? {
                    observer.finalize_window_aggregate(
                        partition_idx,
                        window_expr,
                        key,
                        state,
                    )?;
                }
            }
        }
        Ok(())
    }

    /// Prunes sections of the state that are no longer needed when calculating
    /// results (as determined by window frame boundaries and number of results generated).
    // For instance, if first `n` (not necessarily same with `n_out`) elements are no longer needed to
    // calculate window expression result (outside the window frame boundary) we retract first `n` elements
    // from the corresponding partition's batch in `self.partition_buffers`.
    // For instance, if `n_out` number of rows are calculated, we can remove
    // first `n_out` rows from `self.input_buffer`.
    fn prune_state(&mut self, n_out: usize) -> Result<()> {
        // Prune `self.window_agg_states`:
        self.prune_out_columns();
        // Prune `self.partition_buffers`:
        self.prune_partition_batches();
        // Prune `self.input_buffer`:
        self.prune_input_batch(n_out)?;
        // Prune internal state of search algorithm.
        self.search_mode.prune(n_out);
        Ok(())
    }
}

impl Stream for BoundedWindowAggStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let poll = self.poll_next_inner(cx);
        self.baseline_metrics.record_poll(poll)
    }
}

impl BoundedWindowAggStream {
    /// Create a new BoundedWindowAggStream
    fn new(
        schema: SchemaRef,
        window_expr: Vec<Arc<dyn WindowExpr>>,
        input: SendableRecordBatchStream,
        baseline_metrics: BaselineMetrics,
        search_mode: Box<dyn PartitionSearcher>,
        partition_idx: usize,
        state_observer: Option<Arc<dyn WindowStateObserver>>,
    ) -> Result<Self> {
        let state = window_expr.iter().map(|_| IndexMap::default()).collect();
        let empty_batch = RecordBatch::new_empty(Arc::clone(&schema));
        Ok(Self {
            schema,
            input,
            input_buffer: empty_batch,
            partition_buffers: IndexMap::default(),
            window_agg_states: state,
            finished: false,
            window_expr,
            baseline_metrics,
            search_mode,
            most_recent_row: None,
            partition_idx,
            state_observer,
        })
    }

    fn compute_aggregates(&mut self) -> Result<Option<RecordBatch>> {
        // calculate window cols
        let eval_ctx = WindowEvalContext::default()
            .with_most_recent_row(self.most_recent_row.as_ref());
        for (cur_window_expr, state) in
            self.window_expr.iter().zip(&mut self.window_agg_states)
        {
            cur_window_expr.evaluate_stateful(
                &self.partition_buffers,
                state,
                &eval_ctx,
            )?;
        }

        // Fire before `calculate_out_columns`: on causal frames every row
        // already streamed out, so at EOS that call returns `None` and the
        // prune path is skipped — the final partition would otherwise be
        // dropped unobserved.
        if let Some(observer) = self.state_observer.clone() {
            self.publish_finalized_states(observer.as_ref())?;
        }

        let schema = Arc::clone(&self.schema);
        let window_expr_out = self.search_mode.calculate_out_columns(
            &self.input_buffer,
            &self.window_agg_states,
            &mut self.partition_buffers,
            &self.window_expr,
        )?;
        if let Some(window_expr_out) = window_expr_out {
            let n_out = window_expr_out[0].len();
            // right append new columns to corresponding section in the original input buffer.
            let columns_to_show = self
                .input_buffer
                .columns()
                .iter()
                .map(|elem| elem.slice(0, n_out))
                .chain(window_expr_out)
                .collect::<Vec<_>>();
            let n_generated = columns_to_show[0].len();
            self.prune_state(n_generated)?;
            Ok(Some(RecordBatch::try_new(schema, columns_to_show)?))
        } else {
            Ok(None)
        }
    }

    #[inline]
    fn poll_next_inner(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<RecordBatch>>> {
        if self.finished {
            return Poll::Ready(None);
        }

        let elapsed_compute = self.baseline_metrics.elapsed_compute().clone();
        match ready!(self.input.poll_next_unpin(cx)) {
            Some(Ok(batch)) => {
                // Start the timer for compute time within this operator. It will be
                // stopped when dropped.
                let _timer = elapsed_compute.timer();

                if self.search_mode.is_mode_linear() && batch.num_rows() > 0 {
                    self.most_recent_row = Some(get_last_row_batch(&batch)?);
                }
                self.search_mode.update_partition_batch(
                    &mut self.input_buffer,
                    batch,
                    &self.window_expr,
                    &mut self.partition_buffers,
                )?;
                if let Some(batch) = self.compute_aggregates()? {
                    return Poll::Ready(Some(Ok(batch)));
                }
                self.poll_next_inner(cx)
            }
            Some(Err(e)) => Poll::Ready(Some(Err(e))),
            None => {
                let _timer = elapsed_compute.timer();

                self.finished = true;
                // Release the input pipeline's resources before computing the
                // final aggregates.
                let input_schema = self.input.schema();
                self.input = Box::pin(EmptyRecordBatchStream::new(input_schema));
                for (_, partition_batch_state) in self.partition_buffers.iter_mut() {
                    partition_batch_state.is_end = true;
                }
                if let Some(batch) = self.compute_aggregates()? {
                    return Poll::Ready(Some(Ok(batch)));
                }
                Poll::Ready(None)
            }
        }
    }

    /// Removes partitions that have ended. For the remaining partitions,
    /// drops buffered rows that no window expression will need again.
    fn prune_partition_batches(&mut self) {
        // Check that per-state and per-partition end-flags are consistent;
        // otherwise, the pruning code below might produce inconsistent state.
        #[cfg(debug_assertions)]
        for window_agg_state in self.window_agg_states.iter() {
            for (partition_row, WindowState { state, .. }) in window_agg_state.iter() {
                debug_assert_eq!(
                    state.is_end, self.partition_buffers[partition_row].is_end,
                    "window state's recorded end flag is out of sync with its partition"
                );
            }
        }

        // Remove partitions which we know already ended (is_end flag is true).
        // Since the retain method preserves insertion order, we still have
        // ordering in between partitions after removal.
        self.partition_buffers
            .retain(|_, partition_batch_state| !partition_batch_state.is_end);
        // Likewise, drop per-window-expression state for ended partitions.
        for window_agg_state in self.window_agg_states.iter_mut() {
            window_agg_state.retain(|_, WindowState { state, .. }| !state.is_end);
        }

        // Calculate how many rows to prune from each partition's batch. For a
        // single window expression, rows before min(window_frame_range.start,
        // last_calculated_index) are prunable: their results are already
        // calculated, and frame boundaries never move backwards, so no future
        // frame can include them. All window expressions share the partition
        // batch, so a row can only be pruned once every expression is done with
        // it: the count to prune is the minimum across expressions. A partition
        // missing from the map has nothing to prune.
        let mut n_prune_each_partition = HashMap::new();
        if let Some((first, rest)) = self.window_agg_states.split_first() {
            // First window expression seeds the prune-count map
            for (partition_row, WindowState { state, .. }) in first.iter() {
                let n_prune =
                    min(state.window_frame_range.start, state.last_calculated_index);
                if n_prune > 0 {
                    n_prune_each_partition.insert(partition_row.clone(), n_prune);
                }
            }
            // Take the per-partition min of the prune-count for each
            // additional window expression
            for window_agg_state in rest {
                n_prune_each_partition.retain(|partition_row, current| {
                    let Some(WindowState { state, .. }) =
                        window_agg_state.get(partition_row)
                    else {
                        return false;
                    };
                    let n_prune =
                        min(state.window_frame_range.start, state.last_calculated_index);
                    *current = min(*current, n_prune);
                    *current > 0
                });
            }
        }

        // Drop the prunable prefix of each partition's buffered batch:
        for (partition_row, n_prune) in n_prune_each_partition.iter() {
            debug_assert!(
                *n_prune > 0,
                "prune-count map must only contain positive entries"
            );
            let pb_state = &mut self.partition_buffers[partition_row];

            let batch = &pb_state.record_batch;
            pb_state.record_batch = batch.slice(*n_prune, batch.num_rows() - n_prune);

            // Update state indices since we have pruned some rows from the beginning:
            for window_agg_state in self.window_agg_states.iter_mut() {
                window_agg_state[partition_row].state.prune_state(*n_prune);
            }
        }
    }

    /// Prunes the section of the input batch whose aggregate results
    /// are calculated and emitted.
    fn prune_input_batch(&mut self, n_out: usize) -> Result<()> {
        // Prune first n_out rows from the input_buffer
        let n_to_keep = self.input_buffer.num_rows() - n_out;
        let batch_to_keep = self
            .input_buffer
            .columns()
            .iter()
            .map(|elem| elem.slice(n_out, n_to_keep))
            .collect::<Vec<_>>();
        self.input_buffer = RecordBatch::try_new_with_options(
            self.input_buffer.schema(),
            batch_to_keep,
            &RecordBatchOptions::new().with_row_count(Some(n_to_keep)),
        )?;
        Ok(())
    }

    /// Prunes emitted parts from WindowAggState `out_col` field.
    fn prune_out_columns(&mut self) {
        // We store generated columns for each window expression in the `out_col`
        // field of `WindowAggState`. Given how many rows are emitted, we remove
        // these sections from state.
        for partition_window_agg_states in self.window_agg_states.iter_mut() {
            // If `is_end` is set, directly remove the entry; this shrinks the
            // hash map.
            partition_window_agg_states
                .retain(|_, partition_batch_state| !partition_batch_state.state.is_end);
        }
        // Only partitions that emitted rows since the previous pruning pass
        // have output columns to shrink. Their emitted-row counts are
        // consumed and reset here, so partitions that emitted nothing keep
        // a count of zero and are passed over without any hash lookups.
        for (partition_key, partition_batch) in self.partition_buffers.iter_mut() {
            let n_emitted = partition_batch.n_out_row;
            if n_emitted == 0 {
                continue;
            }
            partition_batch.n_out_row = 0;
            for partition_window_agg_states in self.window_agg_states.iter_mut() {
                if let Some(WindowState { state, .. }) =
                    partition_window_agg_states.get_mut(partition_key)
                {
                    let out_col = &mut state.out_col;
                    let n_to_keep = out_col.len() - n_emitted;
                    *out_col = out_col.slice(n_emitted, n_to_keep);
                }
            }
        }
    }
}

impl RecordBatchStream for BoundedWindowAggStream {
    /// Get the schema
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

// Gets the index of minimum entry, returns None if empty.
fn argmin<T: PartialOrd>(data: impl Iterator<Item = T>) -> Option<(usize, T)> {
    data.enumerate()
        .min_by(|(_, a), (_, b)| a.partial_cmp(b).unwrap_or(Ordering::Equal))
}

/// Calculates the section we can show results for expression
fn get_aggregate_result_out_column(
    partition_window_agg_states: &PartitionWindowAggStates,
    len_to_show: usize,
) -> Result<ArrayRef> {
    let mut result = None;
    let mut running_length = 0;
    let mut batches_to_concat = vec![];
    // We assume that iteration order is according to insertion order
    for (
        _,
        WindowState {
            state: WindowAggState { out_col, .. },
            ..
        },
    ) in partition_window_agg_states
    {
        if running_length < len_to_show {
            let n_to_use = min(len_to_show - running_length, out_col.len());
            let slice_to_use = if n_to_use == out_col.len() {
                // avoid slice when the entire column is used
                Arc::clone(out_col)
            } else {
                out_col.slice(0, n_to_use)
            };
            batches_to_concat.push(slice_to_use);
            running_length += n_to_use;
        } else {
            break;
        }
    }

    if !batches_to_concat.is_empty() {
        let array_refs: Vec<&dyn Array> =
            batches_to_concat.iter().map(|a| a.as_ref()).collect();
        result = Some(concat(&array_refs)?);
    }

    if running_length != len_to_show {
        return exec_err!(
            "Generated row number should be {len_to_show}, it is {running_length}"
        );
    }
    result.ok_or_else(|| exec_datafusion_err!("Should contain something"))
}

/// Constructs a batch from the last row of batch in the argument.
pub(crate) fn get_last_row_batch(batch: &RecordBatch) -> Result<RecordBatch> {
    if batch.num_rows() == 0 {
        return exec_err!("Latest batch should have at least 1 row");
    }
    Ok(batch.slice(batch.num_rows() - 1, 1))
}

#[cfg(test)]
mod tests {
    use std::pin::Pin;
    use std::sync::Arc;
    use std::task::{Context, Poll};
    use std::time::Duration;

    use crate::common::collect;
    use crate::execution_plan::CardinalityEffect;
    use crate::expressions::PhysicalSortExpr;
    use crate::projection::{ProjectionExec, ProjectionExpr};
    use crate::streaming::{PartitionStream, StreamingTableExec};
    use crate::test::TestMemoryExec;
    use crate::windows::bounded_window_agg_exec::WindowStateObserver;
    use crate::windows::{
        BoundedWindowAggExec, InputOrderMode, create_udwf_window_expr, create_window_expr,
    };
    use crate::{ExecutionPlan, WindowExpr, displayable, execute_stream};

    use arrow::array::{
        RecordBatch,
        builder::{Int64Builder, UInt64Builder},
    };
    use arrow::compute::SortOptions;
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
    use datafusion_common::test_util::batches_to_string;
    use datafusion_common::{Result, ScalarValue, exec_datafusion_err};
    use datafusion_execution::config::SessionConfig;
    use datafusion_execution::{
        RecordBatchStream, SendableRecordBatchStream, TaskContext,
    };
    use datafusion_expr::{
        WindowFrame, WindowFrameBound, WindowFrameUnits, WindowFunctionDefinition,
    };
    use datafusion_functions_aggregate::count::count_udaf;
    use datafusion_functions_aggregate::sum::sum_udaf;
    use datafusion_functions_window::lead_lag::lead_udwf;
    use datafusion_functions_window::nth_value::last_value_udwf;
    use datafusion_functions_window::nth_value::nth_value_udwf;
    use datafusion_functions_window::row_number::row_number_udwf;
    use datafusion_physical_expr::expressions::{Column, Literal, col};
    use datafusion_physical_expr::window::{PartitionKey, StandardWindowExpr};
    use datafusion_physical_expr::{LexOrdering, PhysicalExpr};

    use futures::future::Shared;
    use futures::{FutureExt, Stream, StreamExt, pin_mut, ready};
    use insta::assert_snapshot;
    use itertools::Itertools;
    use tokio::time::timeout;

    #[derive(Debug, Clone)]
    struct TestStreamPartition {
        schema: SchemaRef,
        batches: Vec<RecordBatch>,
        idx: usize,
        state: PolingState,
        sleep_duration: Duration,
        send_exit: bool,
    }

    impl PartitionStream for TestStreamPartition {
        fn schema(&self) -> &SchemaRef {
            &self.schema
        }

        fn execute(&self, _ctx: Arc<TaskContext>) -> SendableRecordBatchStream {
            // We create an iterator from the record batches and map them into Ok values,
            // converting the iterator into a futures::stream::Stream
            Box::pin(self.clone())
        }
    }

    impl Stream for TestStreamPartition {
        type Item = Result<RecordBatch>;

        fn poll_next(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
        ) -> Poll<Option<Self::Item>> {
            self.poll_next_inner(cx)
        }
    }

    #[derive(Debug, Clone)]
    enum PolingState {
        Sleep(Shared<futures::future::BoxFuture<'static, ()>>),
        BatchReturn,
    }

    impl TestStreamPartition {
        fn poll_next_inner(
            self: &mut Pin<&mut Self>,
            cx: &mut Context<'_>,
        ) -> Poll<Option<Result<RecordBatch>>> {
            loop {
                match &mut self.state {
                    PolingState::BatchReturn => {
                        // Wait for self.sleep_duration before sending any new data
                        let f = tokio::time::sleep(self.sleep_duration).boxed().shared();
                        self.state = PolingState::Sleep(f);
                        let input_batch = if let Some(batch) =
                            self.batches.clone().get(self.idx)
                        {
                            batch.clone()
                        } else if self.send_exit {
                            // Send None to signal end of data
                            return Poll::Ready(None);
                        } else {
                            // Go to sleep mode
                            let f =
                                tokio::time::sleep(self.sleep_duration).boxed().shared();
                            self.state = PolingState::Sleep(f);
                            continue;
                        };
                        self.idx += 1;
                        return Poll::Ready(Some(Ok(input_batch)));
                    }
                    PolingState::Sleep(future) => {
                        pin_mut!(future);
                        ready!(future.poll_unpin(cx));
                        self.state = PolingState::BatchReturn;
                    }
                }
            }
        }
    }

    impl RecordBatchStream for TestStreamPartition {
        fn schema(&self) -> SchemaRef {
            Arc::clone(&self.schema)
        }
    }

    fn bounded_window_exec_pb_latent_range(
        input: Arc<dyn ExecutionPlan>,
        n_future_range: usize,
        hash: &str,
        order_by: &str,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let schema = input.schema();
        let window_fn = WindowFunctionDefinition::AggregateUDF(count_udaf());
        let col_expr =
            Arc::new(Column::new(schema.fields[0].name(), 0)) as Arc<dyn PhysicalExpr>;
        let args = vec![col_expr];
        let partitionby_exprs = vec![col(hash, &schema)?];
        let orderby_exprs = vec![PhysicalSortExpr {
            expr: col(order_by, &schema)?,
            options: SortOptions::default(),
        }];
        let window_frame = WindowFrame::new_bounds(
            WindowFrameUnits::Range,
            WindowFrameBound::CurrentRow,
            WindowFrameBound::Following(ScalarValue::UInt64(Some(n_future_range as u64))),
        );
        let fn_name = format!(
            "{window_fn}({args:?}) PARTITION BY: [{partitionby_exprs:?}], ORDER BY: [{orderby_exprs:?}]"
        );
        let input_order_mode = InputOrderMode::Linear;
        Ok(Arc::new(BoundedWindowAggExec::try_new(
            vec![create_window_expr(
                &window_fn,
                fn_name,
                &args,
                &partitionby_exprs,
                &orderby_exprs,
                Arc::new(window_frame),
                input.schema(),
                false,
                false,
                None,
            )?],
            input,
            input_order_mode,
            true,
        )?))
    }

    fn projection_exec(input: Arc<dyn ExecutionPlan>) -> Result<Arc<dyn ExecutionPlan>> {
        let schema = input.schema();
        let exprs = input
            .schema()
            .fields
            .iter()
            .enumerate()
            .map(|(idx, field)| {
                let name = if field.name().len() > 20 {
                    format!("col_{idx}")
                } else {
                    field.name().clone()
                };
                let expr = col(field.name(), &schema).unwrap();
                (expr, name)
            })
            .collect::<Vec<_>>();
        let proj_exprs: Vec<ProjectionExpr> = exprs
            .into_iter()
            .map(|(expr, alias)| ProjectionExpr { expr, alias })
            .collect();
        Ok(Arc::new(ProjectionExec::try_new(proj_exprs, input)?))
    }

    fn task_context_helper() -> TaskContext {
        let task_ctx = TaskContext::default();
        // Create session context with config
        let session_config = SessionConfig::new()
            .with_batch_size(1)
            .with_target_partitions(2)
            .with_round_robin_repartition(false);
        task_ctx.with_session_config(session_config)
    }

    fn task_context() -> Arc<TaskContext> {
        Arc::new(task_context_helper())
    }

    pub async fn collect_stream(
        mut stream: SendableRecordBatchStream,
        results: &mut Vec<RecordBatch>,
    ) -> Result<()> {
        while let Some(item) = stream.next().await {
            results.push(item?);
        }
        Ok(())
    }

    /// Execute the [ExecutionPlan] and collect the results in memory
    pub async fn collect_with_timeout(
        plan: Arc<dyn ExecutionPlan>,
        context: Arc<TaskContext>,
        timeout_duration: Duration,
    ) -> Result<Vec<RecordBatch>> {
        let stream = execute_stream(plan, context)?;
        let mut results = vec![];

        // Execute the asynchronous operation with a timeout
        if timeout(timeout_duration, collect_stream(stream, &mut results))
            .await
            .is_ok()
        {
            return Err(exec_datafusion_err!("shouldn't have completed"));
        }

        Ok(results)
    }

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("sn", DataType::UInt64, true),
            Field::new("hash", DataType::Int64, true),
        ]))
    }

    fn schema_orders(schema: &SchemaRef) -> Result<Vec<LexOrdering>> {
        let orderings = vec![
            [PhysicalSortExpr {
                expr: col("sn", schema)?,
                options: SortOptions {
                    descending: false,
                    nulls_first: false,
                },
            }]
            .into(),
        ];
        Ok(orderings)
    }

    fn is_integer_division_safe(lhs: usize, rhs: usize) -> bool {
        let res = lhs / rhs;
        res * rhs == lhs
    }
    fn generate_batches(
        schema: &SchemaRef,
        n_row: usize,
        n_chunk: usize,
    ) -> Result<Vec<RecordBatch>> {
        let mut batches = vec![];
        assert!(n_row > 0);
        assert!(n_chunk > 0);
        assert!(is_integer_division_safe(n_row, n_chunk));
        let hash_replicate = 4;

        let chunks = (0..n_row)
            .chunks(n_chunk)
            .into_iter()
            .map(|elem| elem.into_iter().collect::<Vec<_>>())
            .collect::<Vec<_>>();

        // Send 2 RecordBatches at the source
        for sn_values in chunks {
            let mut sn1_array = UInt64Builder::with_capacity(sn_values.len());
            let mut hash_array = Int64Builder::with_capacity(sn_values.len());

            for sn in sn_values {
                sn1_array.append_value(sn as u64);
                let hash_value = (2 - (sn / hash_replicate)) as i64;
                hash_array.append_value(hash_value);
            }

            let batch = RecordBatch::try_new(
                Arc::clone(schema),
                vec![Arc::new(sn1_array.finish()), Arc::new(hash_array.finish())],
            )?;
            batches.push(batch);
        }
        Ok(batches)
    }

    fn generate_never_ending_source(
        n_rows: usize,
        chunk_length: usize,
        n_partition: usize,
        is_infinite: bool,
        send_exit: bool,
        per_batch_wait_duration_in_millis: u64,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        assert!(n_partition > 0);

        // We use same hash value in the table. This makes sure that
        // After hashing computation will continue in only in one of the output partitions
        // In this case, data flow should still continue
        let schema = test_schema();
        let orderings = schema_orders(&schema)?;

        // Source waits per_batch_wait_duration_in_millis ms before sending other batch
        let per_batch_wait_duration =
            Duration::from_millis(per_batch_wait_duration_in_millis);

        let batches = generate_batches(&schema, n_rows, chunk_length)?;

        // Source has 2 partitions
        let partitions = vec![
            Arc::new(TestStreamPartition {
                schema: Arc::clone(&schema),
                batches,
                idx: 0,
                state: PolingState::BatchReturn,
                sleep_duration: per_batch_wait_duration,
                send_exit,
            }) as _;
            n_partition
        ];
        let source = Arc::new(StreamingTableExec::try_new(
            Arc::clone(&schema),
            partitions,
            None,
            orderings,
            is_infinite,
            None,
        )?) as _;
        Ok(source)
    }

    // Tests NTH_VALUE(negative index) with memoize feature
    // To be able to trigger memoize feature for NTH_VALUE we need to
    // - feed BoundedWindowAggExec with batch stream data.
    // - Window frame should contain UNBOUNDED PRECEDING.
    // It hard to ensure these conditions are met, from the sql query.
    #[tokio::test]
    async fn test_window_nth_value_bounded_memoize() -> Result<()> {
        let config = SessionConfig::new().with_target_partitions(1);
        let task_ctx = Arc::new(TaskContext::default().with_session_config(config));

        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        // Create a new batch of data to insert into the table
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(arrow::array::Int32Array::from(vec![1, 2, 3]))],
        )?;

        let memory_exec = TestMemoryExec::try_new_exec(
            &[vec![batch.clone(), batch.clone(), batch.clone()]],
            Arc::clone(&schema),
            None,
        )?;
        let col_a = col("a", &schema)?;
        let nth_value_func1 = create_udwf_window_expr(
            &nth_value_udwf(),
            &[
                Arc::clone(&col_a),
                Arc::new(Literal::new(ScalarValue::Int32(Some(1)))),
            ],
            &schema,
            "nth_value(-1)".to_string(),
            false,
        )?
        .reverse_expr()
        .unwrap();
        let nth_value_func2 = create_udwf_window_expr(
            &nth_value_udwf(),
            &[
                Arc::clone(&col_a),
                Arc::new(Literal::new(ScalarValue::Int32(Some(2)))),
            ],
            &schema,
            "nth_value(-2)".to_string(),
            false,
        )?
        .reverse_expr()
        .unwrap();

        let last_value_func = create_udwf_window_expr(
            &last_value_udwf(),
            &[Arc::clone(&col_a)],
            &schema,
            "last".to_string(),
            false,
        )?;

        let window_exprs = vec![
            // LAST_VALUE(a)
            Arc::new(StandardWindowExpr::new(
                last_value_func,
                &[],
                &[],
                Arc::new(WindowFrame::new_bounds(
                    WindowFrameUnits::Rows,
                    WindowFrameBound::Preceding(ScalarValue::UInt64(None)),
                    WindowFrameBound::CurrentRow,
                )),
            )) as _,
            // NTH_VALUE(a, -1)
            Arc::new(StandardWindowExpr::new(
                nth_value_func1,
                &[],
                &[],
                Arc::new(WindowFrame::new_bounds(
                    WindowFrameUnits::Rows,
                    WindowFrameBound::Preceding(ScalarValue::UInt64(None)),
                    WindowFrameBound::CurrentRow,
                )),
            )) as _,
            // NTH_VALUE(a, -2)
            Arc::new(StandardWindowExpr::new(
                nth_value_func2,
                &[],
                &[],
                Arc::new(WindowFrame::new_bounds(
                    WindowFrameUnits::Rows,
                    WindowFrameBound::Preceding(ScalarValue::UInt64(None)),
                    WindowFrameBound::CurrentRow,
                )),
            )) as _,
        ];
        let physical_plan = BoundedWindowAggExec::try_new(
            window_exprs,
            memory_exec,
            InputOrderMode::Sorted,
            true,
        )
        .map(|e| Arc::new(e) as Arc<dyn ExecutionPlan>)?;

        let batches = collect(physical_plan.execute(0, task_ctx)?).await?;

        // Get string representation of the plan
        assert_snapshot!(displayable(physical_plan.as_ref()).indent(true), @r#"
        BoundedWindowAggExec: wdw=[last: Field { "last": nullable Int32 }, frame: ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW, nth_value(-1): Field { "nth_value(-1)": nullable Int32 }, frame: ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW, nth_value(-2): Field { "nth_value(-2)": nullable Int32 }, frame: ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW], mode=[Sorted]
          DataSourceExec: partitions=1, partition_sizes=[3]
        "#);

        assert_snapshot!(batches_to_string(&batches), @r"
        +---+------+---------------+---------------+
        | a | last | nth_value(-1) | nth_value(-2) |
        +---+------+---------------+---------------+
        | 1 | 1    | 1             |               |
        | 2 | 2    | 2             | 1             |
        | 3 | 3    | 3             | 2             |
        | 1 | 1    | 1             | 3             |
        | 2 | 2    | 2             | 1             |
        | 3 | 3    | 3             | 2             |
        | 1 | 1    | 1             | 3             |
        | 2 | 2    | 2             | 1             |
        | 3 | 3    | 3             | 2             |
        +---+------+---------------+---------------+
        ");
        Ok(())
    }

    // In `Linear` mode, a partition may receive no new rows for several
    // input batches while other partitions keep growing. Once all of a
    // partition's buffered rows have results, the evaluation sweep skips
    // it until it receives rows again, so this test drives a partition
    // through quiet batches and then resumes it: the results after the
    // gap must continue from the retained accumulator state. Both frames
    // are causal, so results finalize in the batch their row arrives in
    // and the quiet partition is fully calculated while it waits.
    #[tokio::test]
    async fn bounded_window_linear_quiet_partition_resume() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("pk", DataType::UInt64, false),
            Field::new("ts", DataType::UInt64, false),
        ]));
        let make_batch = |rows: &[(u64, u64)]| -> Result<RecordBatch> {
            let mut pk = UInt64Builder::with_capacity(rows.len());
            let mut ts = UInt64Builder::with_capacity(rows.len());
            for (p, t) in rows {
                pk.append_value(*p);
                ts.append_value(*t);
            }
            Ok(RecordBatch::try_new(
                Arc::clone(&schema),
                vec![Arc::new(pk.finish()), Arc::new(ts.finish())],
            )?)
        };
        // `ts` ascends globally; partition 0 is absent from the middle batches.
        let batches = vec![
            make_batch(&[(0, 0), (0, 1), (1, 2)])?,
            make_batch(&[(1, 3), (1, 4)])?,
            make_batch(&[(1, 5)])?,
            make_batch(&[(0, 6), (1, 7)])?,
        ];
        let memory_exec =
            TestMemoryExec::try_new_exec(&[batches], Arc::clone(&schema), None)?;

        let partition_by = vec![col("pk", &schema)?];
        let order_by = [PhysicalSortExpr {
            expr: col("ts", &schema)?,
            options: SortOptions::default(),
        }];
        // A running COUNT (plain aggregate) and a SUM over the previous and
        // current row (sliding aggregate).
        let count_expr = create_window_expr(
            &WindowFunctionDefinition::AggregateUDF(count_udaf()),
            "count".to_string(),
            &[col("ts", &schema)?],
            &partition_by,
            &order_by,
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
        let sum_expr = create_window_expr(
            &WindowFunctionDefinition::AggregateUDF(sum_udaf()),
            "sum".to_string(),
            &[col("ts", &schema)?],
            &partition_by,
            &order_by,
            Arc::new(WindowFrame::new_bounds(
                WindowFrameUnits::Rows,
                WindowFrameBound::Preceding(ScalarValue::UInt64(Some(1))),
                WindowFrameBound::CurrentRow,
            )),
            Arc::clone(&schema),
            false,
            false,
            None,
        )?;
        let physical_plan = BoundedWindowAggExec::try_new(
            vec![count_expr, sum_expr],
            memory_exec,
            InputOrderMode::Linear,
            true,
        )
        .map(|e| Arc::new(e) as Arc<dyn ExecutionPlan>)?;

        let batches = collect(physical_plan.execute(0, task_context())?).await?;

        assert_snapshot!(batches_to_string(&batches), @r"
        +----+----+-------+-----+
        | pk | ts | count | sum |
        +----+----+-------+-----+
        | 0  | 0  | 1     | 0   |
        | 0  | 1  | 2     | 1   |
        | 1  | 2  | 1     | 2   |
        | 1  | 3  | 2     | 5   |
        | 1  | 4  | 3     | 7   |
        | 1  | 5  | 4     | 9   |
        | 0  | 6  | 3     | 7   |
        | 1  | 7  | 5     | 12  |
        +----+----+-------+-----+
        ");
        Ok(())
    }

    // In `Linear` mode, a partition may receive no new rows for several
    // input batches while other partitions keep growing. The evaluation
    // sweep skips a partition whose input is unchanged, so this test
    // drives a partition through quiet batches and then resumes it: the
    // results after the gap must continue from the retained evaluator
    // state. ROW_NUMBER is causal, so the quiet partition is fully
    // calculated while it waits; LEAD is not, so its result for the
    // partition's last buffered row stays pending across the quiet
    // batches and must materialize once the partition receives another
    // row.
    #[tokio::test]
    async fn bounded_window_linear_quiet_partition_resume_standard() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("pk", DataType::UInt64, false),
            Field::new("ts", DataType::UInt64, false),
        ]));
        let make_batch = |rows: &[(u64, u64)]| -> Result<RecordBatch> {
            let mut pk = UInt64Builder::with_capacity(rows.len());
            let mut ts = UInt64Builder::with_capacity(rows.len());
            for (p, t) in rows {
                pk.append_value(*p);
                ts.append_value(*t);
            }
            Ok(RecordBatch::try_new(
                Arc::clone(&schema),
                vec![Arc::new(pk.finish()), Arc::new(ts.finish())],
            )?)
        };
        // `ts` ascends globally; partition 0 is absent from the middle batches.
        let batches = vec![
            make_batch(&[(0, 0), (0, 1), (1, 2)])?,
            make_batch(&[(1, 3), (1, 4)])?,
            make_batch(&[(1, 5)])?,
            make_batch(&[(0, 6), (1, 7)])?,
        ];
        let memory_exec =
            TestMemoryExec::try_new_exec(&[batches], Arc::clone(&schema), None)?;

        let partition_by = vec![col("pk", &schema)?];
        let order_by = [PhysicalSortExpr {
            expr: col("ts", &schema)?,
            options: SortOptions::default(),
        }];
        // Both functions use the default frame of a window with an ORDER BY
        // clause (RANGE UNBOUNDED PRECEDING..CURRENT ROW).
        let row_number_expr = create_window_expr(
            &WindowFunctionDefinition::WindowUDF(row_number_udwf()),
            "row_number".to_string(),
            &[],
            &partition_by,
            &order_by,
            Arc::new(WindowFrame::new(Some(false))),
            Arc::clone(&schema),
            false,
            false,
            None,
        )?;
        let lead_expr = create_window_expr(
            &WindowFunctionDefinition::WindowUDF(lead_udwf()),
            "lead".to_string(),
            &[col("ts", &schema)?],
            &partition_by,
            &order_by,
            Arc::new(WindowFrame::new(Some(false))),
            Arc::clone(&schema),
            false,
            false,
            None,
        )?;
        let physical_plan = BoundedWindowAggExec::try_new(
            vec![row_number_expr, lead_expr],
            memory_exec,
            InputOrderMode::Linear,
            true,
        )
        .map(|e| Arc::new(e) as Arc<dyn ExecutionPlan>)?;

        let batches = collect(physical_plan.execute(0, task_context())?).await?;

        // The skip must not delay results that are ready to be finalized;
        // they stream out as soon as every window expression has produced
        // them. LEAD holds back only the last buffered row of a partition,
        // so one row unblocks right after the first input batch, five more
        // when partition 0 resumes in the last input batch, and the final
        // two (whose LEAD results need the end of the input) in the flush
        // after the input is exhausted.
        assert_eq!(
            batches.iter().map(|b| b.num_rows()).collect::<Vec<_>>(),
            vec![1, 5, 2],
            "expected results to stream as they become final"
        );

        assert_snapshot!(batches_to_string(&batches), @r"
        +----+----+------------+------+
        | pk | ts | row_number | lead |
        +----+----+------------+------+
        | 0  | 0  | 1          | 1    |
        | 0  | 1  | 2          | 6    |
        | 1  | 2  | 1          | 3    |
        | 1  | 3  | 2          | 4    |
        | 1  | 4  | 3          | 5    |
        | 1  | 5  | 4          | 7    |
        | 0  | 6  | 3          |      |
        | 1  | 7  | 5          |      |
        +----+----+------------+------+
        ");
        Ok(())
    }

    // This test, tests whether most recent row guarantee by the input batch of the `BoundedWindowAggExec`
    // helps `BoundedWindowAggExec` to generate low latency result in the `Linear` mode.
    // Input data generated at the source is
    //       "+----+------+",
    //       "| sn | hash |",
    //       "+----+------+",
    //       "| 0  | 2    |",
    //       "| 1  | 2    |",
    //       "| 2  | 2    |",
    //       "| 3  | 2    |",
    //       "| 4  | 1    |",
    //       "| 5  | 1    |",
    //       "| 6  | 1    |",
    //       "| 7  | 1    |",
    //       "| 8  | 0    |",
    //       "| 9  | 0    |",
    //       "+----+------+",
    //
    // Effectively following query is run on this data
    //
    //   SELECT *, count(*) OVER(PARTITION BY duplicated_hash ORDER BY sn RANGE BETWEEN CURRENT ROW AND 1 FOLLOWING)
    //   FROM test;
    //
    // partition `duplicated_hash=2` receives following data from the input
    //
    //       "+----+------+",
    //       "| sn | hash |",
    //       "+----+------+",
    //       "| 0  | 2    |",
    //       "| 1  | 2    |",
    //       "| 2  | 2    |",
    //       "| 3  | 2    |",
    //       "+----+------+",
    // normally `BoundedWindowExec` can only generate following result from the input above
    //
    //       "+----+------+---------+",
    //       "| sn | hash |  count  |",
    //       "+----+------+---------+",
    //       "| 0  | 2    |  2      |",
    //       "| 1  | 2    |  2      |",
    //       "| 2  | 2    |<not yet>|",
    //       "| 3  | 2    |<not yet>|",
    //       "+----+------+---------+",
    // where result of last 2 row is missing. Since window frame end is not may change with future data
    // since window frame end is determined by 1 following (To generate result for row=3[where sn=2] we
    // need to received sn=4 to make sure window frame end bound won't change with future data).
    //
    // With the ability of different partitions to use global ordering at the input (where most up-to date
    //   row is
    //      "| 9  | 0    |",
    //   )
    //
    // `BoundedWindowExec` should be able to generate following result in the test
    //
    //       "+----+------+-------+",
    //       "| sn | hash | col_2 |",
    //       "+----+------+-------+",
    //       "| 0  | 2    | 2     |",
    //       "| 1  | 2    | 2     |",
    //       "| 2  | 2    | 2     |",
    //       "| 3  | 2    | 1     |",
    //       "| 4  | 1    | 2     |",
    //       "| 5  | 1    | 2     |",
    //       "| 6  | 1    | 2     |",
    //       "| 7  | 1    | 1     |",
    //       "+----+------+-------+",
    //
    // where result for all rows except last 2 is calculated (To calculate result for row 9 where sn=8
    //   we need to receive sn=10 value to calculate it result.).
    // In this test, out aim is to test for which portion of the input data `BoundedWindowExec` can generate
    // a result. To test this behaviour, we generated the data at the source infinitely (no `None` signal
    //    is sent to output from source). After, row:
    //
    //       "| 9  | 0    |",
    //
    // is sent. Source stops sending data to output. We collect, result emitted by the `BoundedWindowExec` at the
    // end of the pipeline with a timeout (Since no `None` is sent from source. Collection never ends otherwise).
    #[tokio::test]
    async fn bounded_window_exec_linear_mode_range_information() -> Result<()> {
        let n_rows = 10;
        let chunk_length = 2;
        let n_future_range = 1;

        let timeout_duration = Duration::from_secs(2);

        let source =
            generate_never_ending_source(n_rows, chunk_length, 1, true, false, 5)?;

        let window =
            bounded_window_exec_pb_latent_range(source, n_future_range, "hash", "sn")?;

        let plan = projection_exec(window)?;

        // Get string representation of the plan
        assert_snapshot!(displayable(plan.as_ref()).indent(true), @r#"
        ProjectionExec: expr=[sn@0 as sn, hash@1 as hash, count([Column { name: "sn", index: 0 }]) PARTITION BY: [[Column { name: "hash", index: 1 }]], ORDER BY: [[PhysicalSortExpr { expr: Column { name: "sn", index: 0 }, options: SortOptions { descending: false, nulls_first: true } }]]@2 as col_2]
          BoundedWindowAggExec: wdw=[count([Column { name: "sn", index: 0 }]) PARTITION BY: [[Column { name: "hash", index: 1 }]], ORDER BY: [[PhysicalSortExpr { expr: Column { name: "sn", index: 0 }, options: SortOptions { descending: false, nulls_first: true } }]]: Field { "count([Column { name: \"sn\", index: 0 }]) PARTITION BY: [[Column { name: \"hash\", index: 1 }]], ORDER BY: [[PhysicalSortExpr { expr: Column { name: \"sn\", index: 0 }, options: SortOptions { descending: false, nulls_first: true } }]]": Int64 }, frame: RANGE BETWEEN CURRENT ROW AND 1 FOLLOWING], mode=[Linear]
            StreamingTableExec: partition_sizes=1, projection=[sn, hash], infinite_source=true, output_ordering=[sn@0 ASC NULLS LAST]
        "#);

        let task_ctx = task_context();
        let batches = collect_with_timeout(plan, task_ctx, timeout_duration).await?;

        assert_snapshot!(batches_to_string(&batches), @r"
        +----+------+-------+
        | sn | hash | col_2 |
        +----+------+-------+
        | 0  | 2    | 2     |
        | 1  | 2    | 2     |
        | 2  | 2    | 2     |
        | 3  | 2    | 1     |
        | 4  | 1    | 2     |
        | 5  | 1    | 2     |
        | 6  | 1    | 2     |
        | 7  | 1    | 1     |
        +----+------+-------+
        ");

        Ok(())
    }

    type Observation = (usize, PartitionKey, Vec<ScalarValue>);

    /// Test [`WindowStateObserver`] that records every callback into a shared
    /// `Vec` for later assertion.
    struct RecordingObserver {
        sink: Arc<std::sync::Mutex<Vec<Observation>>>,
    }

    impl WindowStateObserver for RecordingObserver {
        fn finalize_window_aggregate(
            &self,
            partition_idx: usize,
            _window_expr: &Arc<dyn WindowExpr>,
            partition_key: &PartitionKey,
            state: Vec<ScalarValue>,
        ) -> Result<()> {
            self.sink
                .lock()
                .unwrap()
                .push((partition_idx, partition_key.clone(), state));
            Ok(())
        }
    }

    /// Build a `BoundedWindowAggExec` for `count(sn) OVER (PARTITION BY hash
    /// ORDER BY sn <frame>)` over a fixed two-group source (hash=1 × 3,
    /// hash=2 × 3, sorted by (hash, sn)). Returns the plan pre-observer so
    /// callers can decide how to install it.
    fn build_partition_close_plan(frame: WindowFrame) -> Result<BoundedWindowAggExec> {
        let schema = test_schema();

        let mut sn_b = UInt64Builder::with_capacity(6);
        let mut hash_b = Int64Builder::with_capacity(6);
        for (sn, hash) in [(1u64, 1i64), (2, 1), (3, 1), (4, 2), (5, 2), (6, 2)] {
            sn_b.append_value(sn);
            hash_b.append_value(hash);
        }
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(sn_b.finish()), Arc::new(hash_b.finish())],
        )?;
        let ordering: LexOrdering = [
            PhysicalSortExpr {
                expr: col("hash", &schema)?,
                options: SortOptions::default(),
            },
            PhysicalSortExpr {
                expr: col("sn", &schema)?,
                options: SortOptions::default(),
            },
        ]
        .into();
        let source_raw =
            TestMemoryExec::try_new(&[vec![batch]], Arc::clone(&schema), None)?
                .try_with_sort_information(vec![ordering])?;
        let source: Arc<dyn ExecutionPlan> =
            Arc::new(TestMemoryExec::update_cache(&Arc::new(source_raw)));

        let expr = create_window_expr(
            &WindowFunctionDefinition::AggregateUDF(count_udaf()),
            "cnt".to_string(),
            &[col("sn", &schema)?],
            &[col("hash", &schema)?],
            &[PhysicalSortExpr {
                expr: col("sn", &schema)?,
                options: SortOptions::default(),
            }],
            Arc::new(frame),
            source.schema(),
            false,
            false,
            None,
        )?;

        BoundedWindowAggExec::try_new(vec![expr], source, InputOrderMode::Sorted, false)
    }

    // Two PARTITION BY groups: hash=1 [sn=1,2,3] then hash=2 [sn=4,5,6].
    // Input is sorted by (hash, sn) so we can run in Sorted mode; in that
    // mode `mark_partition_end` closes the leading group mid-stream and
    // EOS closes the tail — both fire the observer for an ever-expanding
    // frame. Sliding frames are rejected at install time.

    #[tokio::test]
    async fn test_state_observer_rejects_sliding_frame() -> Result<()> {
        // `CURRENT ROW → UNBOUNDED FOLLOWING` is not ever-expanding, so this
        // maps to `SlidingAggregateWindowExpr` whose accumulator retracts as
        // rows leave the frame — at partition close the accumulator holds
        // only the last frame's rows, not the partition aggregate.
        // `with_state_observer` refuses this configuration.
        use std::sync::Mutex;

        let plan = build_partition_close_plan(WindowFrame::new_bounds(
            WindowFrameUnits::Rows,
            WindowFrameBound::CurrentRow,
            WindowFrameBound::Following(ScalarValue::UInt64(None)),
        ))?;
        let observer: Arc<dyn WindowStateObserver> = Arc::new(RecordingObserver {
            sink: Arc::new(Mutex::new(vec![])),
        });
        let err = plan.with_state_observer(Some(observer)).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("sliding aggregate window frame"),
            "expected sliding-frame rejection, got: {msg}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_finalized_state_observer_fires_on_causal_frame() -> Result<()> {
        // `ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW` — ever-expanding,
        // `PlainAggregateWindowExpr` under the hood. At partition close the
        // accumulator holds the partition aggregate. Both mid-stream close
        // (hash=1 as hash=2 rows arrive) and EOS (hash=2 at drain) fire.
        use std::sync::Mutex;

        let task_ctx = Arc::new(TaskContext::default());
        let plan = build_partition_close_plan(WindowFrame::new_bounds(
            WindowFrameUnits::Rows,
            WindowFrameBound::Preceding(ScalarValue::UInt64(None)),
            WindowFrameBound::CurrentRow,
        ))?;

        let observations: Arc<Mutex<Vec<Observation>>> = Arc::new(Mutex::new(vec![]));
        let observer: Arc<dyn WindowStateObserver> = Arc::new(RecordingObserver {
            sink: Arc::clone(&observations),
        });
        let plan = plan.with_state_observer(Some(observer))?;

        let _ = collect(Arc::new(plan).execute(0, task_ctx)?).await?;

        // count(sn) over each of hash=1 (3 rows) and hash=2 (3 rows), in
        // close order — hash=1 first (mid-stream close), hash=2 second (EOS).
        let observed: Vec<(usize, i64, Vec<ScalarValue>)> = observations
            .lock()
            .unwrap()
            .iter()
            .map(|(idx, key, state)| {
                let hash = match &key[0] {
                    ScalarValue::Int64(Some(v)) => *v,
                    other => panic!("unexpected partition-key element: {other:?}"),
                };
                (*idx, hash, state.clone())
            })
            .collect();
        assert_eq!(
            observed,
            vec![
                (0, 1, vec![ScalarValue::Int64(Some(3))]),
                (0, 2, vec![ScalarValue::Int64(Some(3))]),
            ]
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_finalized_state_observer_fires_exactly_once_across_batches()
    -> Result<()> {
        // Regression guard for the exactly-once observer contract when
        // partition close and pruning happen on different `compute_aggregates`
        // calls.
        //
        // The observer fires from `publish_finalized_states`, called at the
        // top of every `compute_aggregates`. Entries are only cleared by
        // `prune_state`, which runs only when `calculate_out_columns` returns
        // `Some`. Nothing in the type system ties the two together, so a
        // group whose state was published on batch N must not be re-published
        // on batch N+1 or at EOS.
        //
        // Layout: three PARTITION BY groups streamed across two input
        // batches, so each group closes on a distinct `compute_aggregates`
        // call:
        //   batch 1 = [hash=1 × 2]                — no close (single group).
        //   batch 2 = [hash=2 × 2, hash=3 × 2]    — `mark_partition_end`
        //                                            closes hash=1 and hash=2.
        //   EOS                                    — closes hash=3.
        //
        // Assertion: each key appears exactly once across all observations.
        use std::sync::Mutex;

        let task_ctx = Arc::new(TaskContext::default());
        let schema = test_schema();

        // Two batches, same output partition.
        let make_batch = |rows: &[(u64, i64)]| -> Result<RecordBatch> {
            let mut sn_b = UInt64Builder::with_capacity(rows.len());
            let mut hash_b = Int64Builder::with_capacity(rows.len());
            for &(sn, hash) in rows {
                sn_b.append_value(sn);
                hash_b.append_value(hash);
            }
            Ok(RecordBatch::try_new(
                Arc::clone(&schema),
                vec![Arc::new(sn_b.finish()), Arc::new(hash_b.finish())],
            )?)
        };
        let batch1 = make_batch(&[(1, 1), (2, 1)])?;
        let batch2 = make_batch(&[(3, 2), (4, 2), (5, 3), (6, 3)])?;

        let ordering: LexOrdering = [
            PhysicalSortExpr {
                expr: col("hash", &schema)?,
                options: SortOptions::default(),
            },
            PhysicalSortExpr {
                expr: col("sn", &schema)?,
                options: SortOptions::default(),
            },
        ]
        .into();
        let source_raw =
            TestMemoryExec::try_new(&[vec![batch1, batch2]], Arc::clone(&schema), None)?
                .try_with_sort_information(vec![ordering])?;
        let source: Arc<dyn ExecutionPlan> =
            Arc::new(TestMemoryExec::update_cache(&Arc::new(source_raw)));

        let expr = create_window_expr(
            &WindowFunctionDefinition::AggregateUDF(count_udaf()),
            "cnt".to_string(),
            &[col("sn", &schema)?],
            &[col("hash", &schema)?],
            &[PhysicalSortExpr {
                expr: col("sn", &schema)?,
                options: SortOptions::default(),
            }],
            Arc::new(WindowFrame::new_bounds(
                WindowFrameUnits::Rows,
                WindowFrameBound::Preceding(ScalarValue::UInt64(None)),
                WindowFrameBound::CurrentRow,
            )),
            source.schema(),
            false,
            false,
            None,
        )?;

        let observations: Arc<Mutex<Vec<Observation>>> = Arc::new(Mutex::new(vec![]));
        let observer: Arc<dyn WindowStateObserver> = Arc::new(RecordingObserver {
            sink: Arc::clone(&observations),
        });

        let plan = BoundedWindowAggExec::try_new(
            vec![expr],
            source,
            InputOrderMode::Sorted,
            false,
        )?
        .with_state_observer(Some(observer))?;

        let _ = collect(Arc::new(plan).execute(0, task_ctx)?).await?;

        let fired: Vec<i64> = observations
            .lock()
            .unwrap()
            .iter()
            .map(|(_, key, _)| match &key[0] {
                ScalarValue::Int64(Some(v)) => *v,
                other => panic!("unexpected partition-key element: {other:?}"),
            })
            .collect();
        // Each group closes on a distinct `compute_aggregates` call — hash=1
        // and hash=2 on batch 2's `mark_partition_end`, hash=3 at EOS — and
        // each appears exactly once, in close order.
        assert_eq!(fired, vec![1, 2, 3]);
        Ok(())
    }

    /// Run one task's local BWAG for `SUM(sn) OVER (ORDER BY sn ROWS
    /// UNBOUNDED PRECEDING TO CURRENT ROW)` with no PARTITION BY, over
    /// `input` sorted ascending. Returns the per-row output values and the
    /// observed finalized state total (which the caller uses as a carry-in
    /// for the next task).
    async fn run_running_sum_task(
        input: &[u64],
        task_ctx: Arc<TaskContext>,
    ) -> Result<(Vec<u64>, u64)> {
        use arrow::array::UInt64Array;
        use datafusion_functions_aggregate::sum::sum_udaf;
        use std::sync::Mutex;

        /// Observer for `run_running_sum_task`: captures the single running
        /// SUM total published at EOS. Asserts exactly-one fire and rejects
        /// non-empty partition keys (this helper is no-PARTITION-BY only).
        struct RunningSumObserver {
            sink: Arc<Mutex<Option<u64>>>,
        }

        impl WindowStateObserver for RunningSumObserver {
            fn finalize_window_aggregate(
                &self,
                _partition_idx: usize,
                _window_expr: &Arc<dyn WindowExpr>,
                partition_key: &PartitionKey,
                state: Vec<ScalarValue>,
            ) -> Result<()> {
                assert!(
                    partition_key.is_empty(),
                    "empty PartitionKey for no-PARTITION-BY plan"
                );
                let total = match &state[0] {
                    ScalarValue::UInt64(Some(v)) => *v,
                    ScalarValue::Int64(Some(v)) => *v as u64,
                    other => panic!("unexpected sum state element: {other:?}"),
                };
                let prev = self.sink.lock().unwrap().replace(total);
                assert!(prev.is_none(), "observer must fire exactly once per task");
                Ok(())
            }
        }

        let schema = test_schema();
        let mut sn_b = UInt64Builder::with_capacity(input.len());
        let mut hash_b = Int64Builder::with_capacity(input.len());
        for &sn in input {
            sn_b.append_value(sn);
            hash_b.append_value(0);
        }
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(sn_b.finish()), Arc::new(hash_b.finish())],
        )?;
        let ordering: LexOrdering = [PhysicalSortExpr {
            expr: col("sn", &schema)?,
            options: SortOptions::default(),
        }]
        .into();
        let source_raw =
            TestMemoryExec::try_new(&[vec![batch]], Arc::clone(&schema), None)?
                .try_with_sort_information(vec![ordering])?;
        let source: Arc<dyn ExecutionPlan> =
            Arc::new(TestMemoryExec::update_cache(&Arc::new(source_raw)));

        let window_fn = WindowFunctionDefinition::AggregateUDF(sum_udaf());
        let args = vec![col("sn", &schema)?];
        let partition_by: Vec<Arc<dyn PhysicalExpr>> = vec![];
        let order_by = vec![PhysicalSortExpr {
            expr: col("sn", &schema)?,
            options: SortOptions::default(),
        }];
        let frame = WindowFrame::new_bounds(
            WindowFrameUnits::Rows,
            WindowFrameBound::Preceding(ScalarValue::UInt64(None)),
            WindowFrameBound::CurrentRow,
        );
        let expr = create_window_expr(
            &window_fn,
            "running_sum".to_string(),
            &args,
            &partition_by,
            &order_by,
            Arc::new(frame),
            source.schema(),
            false,
            false,
            None,
        )?;

        let total_sink: Arc<Mutex<Option<u64>>> = Arc::new(Mutex::new(None));
        let observer: Arc<dyn WindowStateObserver> = Arc::new(RunningSumObserver {
            sink: Arc::clone(&total_sink),
        });

        let plan = BoundedWindowAggExec::try_new(
            vec![expr],
            source,
            InputOrderMode::Sorted,
            false,
        )?
        .with_state_observer(Some(observer))?;
        let batches = collect(Arc::new(plan).execute(0, task_ctx)?).await?;

        let mut out = Vec::with_capacity(input.len());
        for batch in &batches {
            let col = batch
                .column_by_name("running_sum")
                .expect("running_sum column present");
            let arr = col
                .as_any()
                .downcast_ref::<UInt64Array>()
                .expect("SUM(UInt64) → UInt64Array");
            for i in 0..arr.len() {
                out.push(arr.value(i));
            }
        }
        let total = total_sink
            .lock()
            .unwrap()
            .expect("observer must have fired at EOS");
        Ok((out, total))
    }

    /// Run one task's local BWAG for `approx_distinct(sn) OVER (ORDER BY sn
    /// ROWS UNBOUNDED PRECEDING TO CURRENT ROW)` with no PARTITION BY, and
    /// return the single EOS-observed [`Accumulator::state`] Vec.
    async fn run_approx_distinct_task(
        input: &[u64],
        task_ctx: Arc<TaskContext>,
    ) -> Result<Vec<ScalarValue>> {
        use datafusion_functions_aggregate::approx_distinct::approx_distinct_udaf;
        use std::sync::Mutex;

        /// Observer for `run_approx_distinct_task`: capture the single EOS
        /// state. Asserts exactly-one fire and rejects non-empty partition
        /// keys (helper is no-PARTITION-BY only).
        struct ApproxDistinctObserver {
            sink: Arc<Mutex<Option<Vec<ScalarValue>>>>,
        }

        impl WindowStateObserver for ApproxDistinctObserver {
            fn finalize_window_aggregate(
                &self,
                _partition_idx: usize,
                _window_expr: &Arc<dyn WindowExpr>,
                partition_key: &PartitionKey,
                state: Vec<ScalarValue>,
            ) -> Result<()> {
                assert!(
                    partition_key.is_empty(),
                    "empty PartitionKey for no-PARTITION-BY plan"
                );
                let prev = self.sink.lock().unwrap().replace(state);
                assert!(prev.is_none(), "observer must fire exactly once per task");
                Ok(())
            }
        }

        let schema = test_schema();
        let mut sn_b = UInt64Builder::with_capacity(input.len());
        let mut hash_b = Int64Builder::with_capacity(input.len());
        for &sn in input {
            sn_b.append_value(sn);
            hash_b.append_value(0);
        }
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![Arc::new(sn_b.finish()), Arc::new(hash_b.finish())],
        )?;
        let ordering: LexOrdering = [PhysicalSortExpr {
            expr: col("sn", &schema)?,
            options: SortOptions::default(),
        }]
        .into();
        let source_raw =
            TestMemoryExec::try_new(&[vec![batch]], Arc::clone(&schema), None)?
                .try_with_sort_information(vec![ordering])?;
        let source: Arc<dyn ExecutionPlan> =
            Arc::new(TestMemoryExec::update_cache(&Arc::new(source_raw)));

        let expr = create_window_expr(
            &WindowFunctionDefinition::AggregateUDF(approx_distinct_udaf()),
            "approx_distinct_sn".to_string(),
            &[col("sn", &schema)?],
            &[],
            &[PhysicalSortExpr {
                expr: col("sn", &schema)?,
                options: SortOptions::default(),
            }],
            Arc::new(WindowFrame::new_bounds(
                WindowFrameUnits::Rows,
                WindowFrameBound::Preceding(ScalarValue::UInt64(None)),
                WindowFrameBound::CurrentRow,
            )),
            source.schema(),
            false,
            false,
            None,
        )?;

        let state_sink: Arc<Mutex<Option<Vec<ScalarValue>>>> = Arc::new(Mutex::new(None));
        let observer: Arc<dyn WindowStateObserver> = Arc::new(ApproxDistinctObserver {
            sink: Arc::clone(&state_sink),
        });

        let plan = BoundedWindowAggExec::try_new(
            vec![expr],
            source,
            InputOrderMode::Sorted,
            false,
        )?
        .with_state_observer(Some(observer))?;
        let _ = collect(Arc::new(plan).execute(0, task_ctx)?).await?;

        state_sink
            .lock()
            .unwrap()
            .take()
            .ok_or_else(|| exec_datafusion_err!("observer never fired"))
    }

    #[tokio::test]
    async fn test_prefix_scan_across_tasks_matches_single_bwag() -> Result<()> {
        // Demonstrates the parallel-window shape reviewers asked about:
        // range-shuffle `SUM(sn) OVER (ORDER BY sn UNBOUNDED PRECEDING TO
        // CURRENT ROW)` across two tasks, then prefix-scan each task's
        // finalized state (from the observer) to carry-in the next task's
        // rows. Result must match a single BWAG over the concatenated input.
        let task_ctx = Arc::new(TaskContext::default());

        // Two tasks under range partition on sn:
        let (task1_out, task1_total) =
            run_running_sum_task(&[1, 1, 2, 2, 3, 3, 4, 4], Arc::clone(&task_ctx))
                .await?;
        let (task2_out, task2_total) =
            run_running_sum_task(&[5, 5, 6, 6, 7, 7, 8, 8], Arc::clone(&task_ctx))
                .await?;

        // Local (uncorrected) outputs and totals — first pass.
        assert_eq!(task1_out, vec![1, 2, 4, 6, 9, 12, 16, 20]);
        assert_eq!(task1_total, 20);
        assert_eq!(task2_out, vec![5, 10, 16, 22, 29, 36, 44, 52]);
        assert_eq!(task2_total, 52);

        // Prefix scan over per-task totals → carry-in for each task. Task 0's
        // carry-in is 0; task N's carry-in is the sum of tasks [0, N).
        let carry_ins = [0u64, task1_total];

        // Second pass: shift each task's local values by its carry-in.
        let task1_final: Vec<u64> = task1_out.iter().map(|v| v + carry_ins[0]).collect();
        let task2_final: Vec<u64> = task2_out.iter().map(|v| v + carry_ins[1]).collect();
        let parallel_result: Vec<u64> = task1_final
            .iter()
            .chain(task2_final.iter())
            .copied()
            .collect();

        // Oracle: single BWAG over the full concatenated input.
        let (single_result, single_total) = run_running_sum_task(
            &[1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 6, 6, 7, 7, 8, 8],
            task_ctx,
        )
        .await?;

        assert_eq!(
            parallel_result, single_result,
            "two-task prefix-scan must match single-BWAG oracle"
        );
        // And matches the sequence in the design discussion.
        assert_eq!(
            single_result,
            vec![1, 2, 4, 6, 9, 12, 16, 20, 25, 30, 36, 42, 49, 56, 64, 72]
        );
        assert_eq!(single_total, 72);
        Ok(())
    }

    #[tokio::test]
    async fn test_prefix_merge_across_tasks_approx_distinct() -> Result<()> {
        // Load-bearing contract for the parallel-window use case: the state
        // exposed by `WindowStateObserver::finalize_window_aggregate` must be
        // compatible with `Accumulator::merge_batch` on a fresh accumulator
        // of the same UDAF. This is what allows non-decomposable aggregates
        // like `approx_distinct` (HLL sketch state) to be prefix-merged
        // across shard tasks — the reason we exposed accumulator state at
        // all. If this ever breaks, downstream parallel-window work has to
        // wait for a public API change.
        use arrow::array::{ArrayRef, BinaryArray};
        use arrow::datatypes::FieldRef;
        use datafusion_expr::function::AccumulatorArgs;
        use datafusion_functions_aggregate::approx_distinct::approx_distinct_udaf;

        let task_ctx = Arc::new(TaskContext::default());

        // Two tasks with overlapping inputs; concatenated distinct universe
        // is {1,2,3,4,5}.
        let state1 =
            run_approx_distinct_task(&[1, 1, 2, 3], Arc::clone(&task_ctx)).await?;
        let state2 = run_approx_distinct_task(&[3, 4, 5], Arc::clone(&task_ctx)).await?;
        let state_single =
            run_approx_distinct_task(&[1, 1, 2, 3, 3, 4, 5], Arc::clone(&task_ctx))
                .await?;

        // approx_distinct state is a single serialized-HLL Binary field.
        assert_eq!(state1.len(), 1, "single state field");
        assert_eq!(state2.len(), 1, "single state field");
        assert_eq!(state_single.len(), 1, "single state field");

        // Seed a fresh accumulator with the given serialized HLL states via
        // `merge_batch` and return its distinct-count evaluation.
        fn evaluate_merged(states: &[&ScalarValue]) -> Result<ScalarValue> {
            let udaf = approx_distinct_udaf();
            let input_schema =
                Arc::new(Schema::new(vec![Field::new("sn", DataType::UInt64, true)]));
            let return_field: FieldRef =
                Arc::new(Field::new("approx_distinct_sn", DataType::UInt64, true));
            let expr_field: FieldRef = Arc::new(Field::new("sn", DataType::UInt64, true));
            let physical_col: Arc<dyn PhysicalExpr> = col("sn", &input_schema)?;
            let args = AccumulatorArgs {
                return_field: Arc::clone(&return_field),
                schema: &input_schema,
                ignore_nulls: false,
                order_bys: &[],
                is_reversed: false,
                name: "approx_distinct",
                is_distinct: false,
                exprs: std::slice::from_ref(&physical_col),
                expr_fields: std::slice::from_ref(&expr_field),
            };
            let mut acc = udaf.accumulator(args)?;
            let byte_slices: Vec<&[u8]> = states
                .iter()
                .map(|s| match s {
                    ScalarValue::Binary(Some(v)) => v.as_slice(),
                    other => panic!("expected Binary state, got {other:?}"),
                })
                .collect();
            let bin: ArrayRef = Arc::new(BinaryArray::from_iter_values(byte_slices));
            acc.merge_batch(std::slice::from_ref(&bin))?;
            acc.evaluate()
        }

        let merged = evaluate_merged(&[&state1[0], &state2[0]])?;
        let oracle = evaluate_merged(&[&state_single[0]])?;

        assert_eq!(
            merged, oracle,
            "merged task states must match single-BWAG oracle — parallel prefix-merge contract"
        );
        // HLL is approximate but exact for a 5-element universe.
        assert_eq!(merged, ScalarValue::UInt64(Some(5)));
        Ok(())
    }

    #[test]
    fn test_bounded_window_agg_cardinality_effect() -> Result<()> {
        let schema = test_schema();
        let input: Arc<dyn ExecutionPlan> =
            Arc::new(TestMemoryExec::try_new(&[], Arc::clone(&schema), None)?);
        let plan = bounded_window_exec_pb_latent_range(input, 1, "hash", "sn")?;
        let plan = plan
            .downcast_ref::<BoundedWindowAggExec>()
            .expect("expected BoundedWindowAggExec");

        assert!(matches!(
            plan.cardinality_effect(),
            CardinalityEffect::Equal
        ));
        Ok(())
    }

    /// Checks the per-partition batches that `LinearSearch` splits an input
    /// batch into: partitions appear in first-appearance order, rows within a
    /// partition keep their stream order, NULL keys form their own partition,
    /// and a single-partition batch is passed through without copying.
    #[test]
    fn test_linear_search_evaluate_partition_batches() -> Result<()> {
        use super::{LinearSearch, PartitionSearcher};
        use arrow::array::{Int32Array, Int64Array};

        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, true),
            Field::new("b", DataType::Int64, false),
        ]));
        let window_expr = create_window_expr(
            &WindowFunctionDefinition::AggregateUDF(count_udaf()),
            "count".to_string(),
            &[col("b", &schema)?],
            &[col("a", &schema)?],
            &[],
            Arc::new(WindowFrame::new(None)),
            Arc::clone(&schema),
            false,
            false,
            None,
        )?;
        let mut searcher = LinearSearch::new(vec![], Arc::clone(&schema));

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(vec![
                    Some(1),
                    Some(2),
                    Some(1),
                    None,
                    Some(2),
                    Some(1),
                ])),
                Arc::new(Int64Array::from(vec![10, 20, 11, 30, 21, 12])),
            ],
        )?;
        let result =
            searcher.evaluate_partition_batches(&batch, &[Arc::clone(&window_expr)])?;
        assert_eq!(result.len(), 3);
        let expected = [
            (
                ScalarValue::Int32(Some(1)),
                vec![Some(1); 3],
                vec![10i64, 11, 12],
            ),
            (ScalarValue::Int32(Some(2)), vec![Some(2); 2], vec![20, 21]),
            (ScalarValue::Int32(None), vec![None], vec![30]),
        ];
        for ((key, partition_batch), (exp_key, exp_a, exp_b)) in
            result.iter().zip(expected)
        {
            assert_eq!(key, &vec![exp_key]);
            let exp_batch = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![
                    Arc::new(Int32Array::from(exp_a)),
                    Arc::new(Int64Array::from(exp_b)),
                ],
            )?;
            assert_eq!(partition_batch, &exp_batch);
        }

        let single = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int32Array::from(vec![Some(7), Some(7)])),
                Arc::new(Int64Array::from(vec![70, 71])),
            ],
        )?;
        let result = searcher.evaluate_partition_batches(&single, &[window_expr])?;
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].0, vec![ScalarValue::Int32(Some(7))]);
        assert_eq!(result[0].1, single);
        // The whole batch belongs to one partition, so its columns are reused
        // rather than gathered into a new batch.
        assert!(Arc::ptr_eq(result[0].1.column(0), single.column(0)));
        Ok(())
    }
}
