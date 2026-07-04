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

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{ArrayRef, AsArray, BooleanArray, new_null_array};
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion_common::{Result, assert_eq_or_internal_err, internal_err};
use datafusion_execution::memory_pool::proxy::VecAllocExt;
use datafusion_expr::{EmitTo, GroupsAccumulator};
use datafusion_physical_expr::aggregate::AggregateFunctionExpr;

use crate::PhysicalExpr;
use crate::aggregates::group_values::{GroupByMetrics, GroupValues, new_group_values};
use crate::aggregates::order::GroupOrdering;
use crate::aggregates::row_hash::create_group_accumulator;
use crate::aggregates::{
    AggregateExec, AggregateMode, PhysicalGroupBy, aggregate_expressions,
    evaluate_group_by, group_id_array, max_duplicate_ordinal,
};

/// Semantic mode for the aggregate hash table.
///
/// See [`AggregateHashTable`] comment's 'Mode' section for details.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub(in crate::aggregates) enum AggregateTableMode {
    /// Raw rows -> partial aggregate state rows.
    Partial,
    /// Partial aggregate state rows -> final aggregate value rows.
    Final,
    /// Partial aggregate state rows -> merged partial aggregate state rows.
    PartialReduce,
    /// Raw rows -> final aggregate value rows.
    Single,
}

/// Marker for ordered raw rows -> partial state aggregation.
pub(in crate::aggregates) struct PartialMarker;
/// Marker for ordered partial state -> final value aggregation.
pub(in crate::aggregates) struct FinalMarker;

/// Grouped hash table shared by different hash aggregation modes.
///
/// While building, it consumes input batches and updates group / accumulator
/// state. While outputting, it incrementally drains that state into output
/// batches.
///
/// # Logical and Physical Model
///
/// Logically, this is a hash table that maps { group keys -> accumulator states }
/// For example, `AVG(v) GROUP BY k` stores one entry per `k`, where each
/// entry owns the `sum(v)` and `count(v)` state needed to compute the final
/// average.
///
/// Physically, the group keys and accumulators are backed by [`GroupValues`] and
/// [`GroupsAccumulator`]. Both use columnar storage so aggregation can stay
/// vectorized.
///
/// # Mode
///
/// [`AggregateTableMode`] controls how input batches update accumulator state
/// and how output batches are materialized.
///
/// ```text
/// Example: `AVG(x) GROUP BY k`.
///
/// In `Partial` mode, the table stores partial state:
///     k, sum(x), count(x)
/// The input batch contains raw values:
///     k, x
/// The output batch also contains partial state:
///     k, sum(x), count(x)
/// ```
///
/// So input uses [`GroupsAccumulator::update_batch`], and output uses
/// [`GroupsAccumulator::state`].
///
/// Other modes use different input/output combinations:
/// - `Final`: merge_batch + evaluate
/// - `PartialReduce`: merge_batch + state
/// - `Single`: update_batch + evaluate
pub(in crate::aggregates) struct AggregateHashTable {
    /// Grouping and accumulator-specific timing metrics.
    pub(super) group_by_metrics: GroupByMetrics,

    /// Semantic behavior for this table. See struct comments for details.
    pub(super) mode: AggregateTableMode,

    /// Raw input schema, used to evaluate expressions and synthesize empty
    /// grouping-set rows.
    pub(super) input_schema: SchemaRef,

    /// Output schema: group columns followed by aggregate state or final values.
    pub(super) output_schema: SchemaRef,

    /// Maximum rows per emitted output batch, from config `batch_size`.
    pub(super) batch_size: usize,

    /// Lifecycle-specific state: building stage / outputting stage.
    pub(super) state: AggregateHashTableState,
}

/// Methods shared by all aggregate hash table modes.
impl AggregateHashTable {
    pub(in crate::aggregates) fn new(
        agg: &AggregateExec,
        partition: usize,
        output_schema: SchemaRef,
        batch_size: usize,
    ) -> Result<Self> {
        if batch_size == 0 {
            return internal_err!("AggregateHashTable requires config batch_size >= 1");
        }

        // Infer the internal `AggregateTableMode` based on `AggregateExec`'s mode
        //
        // TODO(simplification): `AggregateMode` seems bloated for aggregate hash
        // table semantics. Consider remove `AggregateMode` and only use `AggregateTableMode`
        // after the refactor has finished.
        //
        // Issue: <https://github.com/apache/datafusion/issues/22710>
        let mode = match agg.mode {
            AggregateMode::Partial => AggregateTableMode::Partial,
            AggregateMode::Final | AggregateMode::FinalPartitioned => {
                AggregateTableMode::Final
            }
            AggregateMode::PartialReduce => AggregateTableMode::PartialReduce,
            AggregateMode::Single | AggregateMode::SinglePartitioned => {
                AggregateTableMode::Single
            }
        };

        let input_schema = agg.input().schema();
        let aggregate_mode = match mode {
            AggregateTableMode::Partial => AggregateMode::Partial,
            AggregateTableMode::Final => AggregateMode::Final,
            AggregateTableMode::PartialReduce => AggregateMode::PartialReduce,
            AggregateTableMode::Single => AggregateMode::Single,
        };
        let aggregate_arguments = aggregate_expressions(
            &agg.aggr_expr,
            &aggregate_mode,
            agg.group_by.num_group_exprs(),
        )?;

        // Filters apply only when the table consumes raw input rows. Final and
        // partial-reduce modes consume partial states, so their filters are not
        // applicable.
        let filters = match mode {
            AggregateTableMode::Partial | AggregateTableMode::Single => {
                agg.filter_expr.iter().cloned().collect()
            }
            AggregateTableMode::Final | AggregateTableMode::PartialReduce => {
                vec![None; agg.aggr_expr.len()]
            }
        };

        let accumulators: Vec<_> = agg
            .aggr_expr
            .iter()
            .zip(aggregate_arguments)
            .zip(filters)
            .map(|((agg_expr, arguments), filter)| {
                let accumulator = create_group_accumulator(agg_expr)?;
                Ok(HashAggregateAccumulator::new(
                    Arc::clone(agg_expr),
                    arguments,
                    filter,
                    accumulator,
                ))
            })
            .collect::<Result<_>>()?;

        let group_schema = agg.group_by.group_schema(&input_schema)?;
        let group_values = new_group_values(group_schema, &GroupOrdering::None)?;

        Ok(Self {
            group_by_metrics: GroupByMetrics::new(&agg.metrics, partition),
            mode,
            input_schema,
            output_schema,
            batch_size,
            state: AggregateHashTableState::Building(AggregateHashTableBuffer {
                group_by: Arc::clone(&agg.group_by),
                group_values,
                batch_group_indices: Default::default(),
                accumulators,
            }),
        })
    }

    pub(in crate::aggregates) fn mode(&self) -> AggregateTableMode {
        self.mode
    }

    /// See comments in [`EvaluatedAggregateBatch`]
    pub(super) fn evaluate_batch(
        &self,
        batch: &RecordBatch,
    ) -> Result<EvaluatedAggregateBatch> {
        let state = self.state.building();
        let timer = self.group_by_metrics.time_calculating_group_ids.timer();
        // outer vec: one per each grouping set
        // inner vec: all group by exprs for the current grouping set
        let grouping_set_args = evaluate_group_by(&state.group_by, batch)?;
        drop(timer);

        let timer = self.group_by_metrics.aggregate_arguments_time.timer();
        // The evaluated args for each accumulator
        let accumulator_args = self
            .state
            .building()
            .accumulators
            .iter()
            .map(|acc| acc.evaluate_acc_args(batch))
            .collect::<Result<Vec<_>>>()?;
        drop(timer);

        Ok(EvaluatedAggregateBatch {
            grouping_set_args,
            accumulator_args,
        })
    }

    pub(in crate::aggregates) fn memory_size(&self) -> usize {
        match &self.state {
            AggregateHashTableState::Building(state)
            | AggregateHashTableState::Outputting(state) => {
                let acc = state
                    .accumulators
                    .iter()
                    .map(|acc| acc.accumulator.size())
                    .sum::<usize>();

                acc + state.group_values.size()
                    + state.batch_group_indices.allocated_size()
            }
            AggregateHashTableState::OutputtingMaterialized(output) => {
                output.memory_size()
            }
            AggregateHashTableState::Done => 0,
        }
    }

    /// Returns the number of distinct groups accumulated so far.
    pub(in crate::aggregates) fn building_group_count(&self) -> usize {
        self.state.building().group_values.len()
    }

    pub(in crate::aggregates) fn can_skip_aggregation(&self) -> bool {
        self.state
            .building()
            .accumulators
            .iter()
            .all(|acc| acc.supports_convert_to_state())
    }

    /// In skip-partial-aggregation optimization, when a decision has been made
    /// to skip partial stage, build a table only for converting raw input rows
    /// into partial aggregate state rows.
    pub(in crate::aggregates) fn partial_skip_table(
        &self,
    ) -> Result<PartialSkipHashTable> {
        let state = self.state.building();
        let group_schema = state.group_by.group_schema(&self.input_schema)?;
        let group_values = new_group_values(group_schema, &GroupOrdering::None)?;
        let accumulators = state
            .accumulators
            .iter()
            .map(HashAggregateAccumulator::empty_like)
            .collect::<Result<Vec<_>>>()?;

        Ok(PartialSkipHashTable {
            table: AggregateHashTable {
                group_by_metrics: self.group_by_metrics.clone(),
                mode: AggregateTableMode::Partial,
                input_schema: Arc::clone(&self.input_schema),
                output_schema: Arc::clone(&self.output_schema),
                batch_size: self.batch_size,
                state: AggregateHashTableState::Building(AggregateHashTableBuffer {
                    group_by: Arc::clone(&state.group_by),
                    group_values,
                    batch_group_indices: Default::default(),
                    accumulators,
                }),
            },
        })
    }

    pub(in crate::aggregates) fn is_building(&self) -> bool {
        matches!(self.state, AggregateHashTableState::Building(_))
    }

    pub(in crate::aggregates) fn is_done(&self) -> bool {
        matches!(self.state, AggregateHashTableState::Done)
    }

    pub(super) fn start_outputting(&mut self) {
        let AggregateHashTableState::Building(mut state) =
            std::mem::replace(&mut self.state, AggregateHashTableState::Done)
        else {
            unreachable!("hash aggregate table is not building")
        };

        state.batch_group_indices = Vec::new();
        self.state = AggregateHashTableState::Outputting(state);
    }

    /// Aggregates one input batch. The behavior depends on the aggregate mode.
    ///
    /// See comments at [`AggregateHashTable`] for details.
    pub(in crate::aggregates) fn aggregate_batch(
        &mut self,
        batch: &RecordBatch,
    ) -> Result<()> {
        let evaluated_batch = self.evaluate_batch(batch)?;
        let mode = self.mode;
        let state = self.state.building_mut();

        let timer = self.group_by_metrics.aggregation_time.timer();
        for group_values in &evaluated_batch.grouping_set_args {
            state
                .group_values
                .intern(group_values, &mut state.batch_group_indices)?;
            let group_indices = &state.batch_group_indices;
            let total_num_groups = state.group_values.len();

            match mode {
                AggregateTableMode::Partial | AggregateTableMode::Single => {
                    for (acc, values) in state
                        .accumulators
                        .iter_mut()
                        .zip(evaluated_batch.accumulator_args.iter())
                    {
                        acc.update_batch(values, group_indices, total_num_groups)?
                    }
                }
                AggregateTableMode::Final | AggregateTableMode::PartialReduce => {
                    for (acc, values) in state
                        .accumulators
                        .iter_mut()
                        .zip(evaluated_batch.accumulator_args.iter())
                    {
                        acc.merge_batch(values, group_indices, total_num_groups)?
                    }
                }
            }
        }
        drop(timer);

        Ok(())
    }

    /// Emits the next output batch according to this table's output semantics.
    ///
    /// `Partial` and `PartialReduce` emit accumulator states for downstream
    /// aggregate stages. `Final` and `Single` evaluate final aggregate values
    /// for the query result.
    pub(in crate::aggregates) fn next_output_batch(
        &mut self,
    ) -> Result<Option<RecordBatch>> {
        let output_schema = Arc::clone(&self.output_schema);
        let batch_size = self.batch_size;
        // Take ownership of the output state. `emit_next_materialized_batch`
        // restores `self.state` to `OutputtingMaterialized` or `Done`.
        match std::mem::replace(&mut self.state, AggregateHashTableState::Done) {
            AggregateHashTableState::Outputting(state) => {
                if state.group_values.is_empty() {
                    return Ok(None);
                }

                let output = self.materialize_output(state, output_schema)?;
                Ok(self.emit_next_materialized_batch(output, batch_size))
            }
            AggregateHashTableState::OutputtingMaterialized(output) => {
                Ok(self.emit_next_materialized_batch(output, batch_size))
            }
            AggregateHashTableState::Done => Ok(None),
            AggregateHashTableState::Building(_) => {
                internal_err!("next_output_batch must be called in the outputting state")
            }
        }
    }
    /// Materialize the aggregated output. The behavior depends on the aggregate mode.
    ///
    /// See comments at [`AggregateHashTable`] for details.
    fn materialize_output(
        &self,
        mut state: AggregateHashTableBuffer,
        output_schema: SchemaRef,
    ) -> Result<MaterializedAggregateOutput> {
        // Final evaluation and partial state emission both consume accumulator
        // state. Materialize all groups once, then slice on subsequent polls.
        let emit_to = EmitTo::All;
        let timer = self.group_by_metrics.emitting_time.timer();
        let mut output = state.group_values.emit(emit_to)?;

        match self.mode {
            AggregateTableMode::Partial | AggregateTableMode::PartialReduce => {
                for acc in state.accumulators.iter_mut() {
                    output.extend(acc.state(emit_to)?)
                }
            }
            AggregateTableMode::Final | AggregateTableMode::Single => {
                for acc in state.accumulators.iter_mut() {
                    output.push(acc.evaluate(emit_to)?)
                }
            }
        }
        drop(timer);

        let batch = RecordBatch::try_new(output_schema, output)?;
        debug_assert!(batch.num_rows() > 0);
        Ok(MaterializedAggregateOutput::new(batch))
    }

    fn emit_next_materialized_batch(
        &mut self,
        mut output: MaterializedAggregateOutput,
        batch_size: usize,
    ) -> Option<RecordBatch> {
        let batch = output.next_batch(batch_size);
        if output.is_exhausted() {
            self.state = AggregateHashTableState::Done;
        } else {
            self.state = AggregateHashTableState::OutputtingMaterialized(output);
        }
        batch
    }

    pub(in crate::aggregates) fn start_output(&mut self) -> Result<()> {
        if matches!(
            self.mode,
            AggregateTableMode::Partial | AggregateTableMode::Single
        ) {
            self.init_empty_grouping_sets()?;
        }
        self.start_outputting();
        Ok(())
    }

    /// Creates the required empty grouping-set rows when the input is empty.
    ///
    /// For example, this query must still produce one grand-total group even if
    /// `t` has no rows:
    ///
    /// ```sql
    /// SELECT COUNT(v)
    /// FROM t
    /// GROUP BY GROUPING SETS (());
    /// ```
    ///
    /// The synthetic row is filtered out before accumulator update so aggregates
    /// see the same state they would see for an empty input, rather than a real
    /// null-valued row.
    pub(super) fn init_empty_grouping_sets(&mut self) -> Result<()> {
        let state = self.state.building_mut();
        if !state.group_by.has_grouping_set() || !state.group_values.is_empty() {
            return Ok(());
        }

        let max_ordinal = max_duplicate_ordinal(state.group_by.groups());
        let mut ordinals: HashMap<&[bool], usize> = HashMap::new();
        let group_schema = state.group_by.group_schema(&self.input_schema)?;
        let n_expr = state.group_by.expr().len();
        let mut any_interned = false;

        for group in state.group_by.groups() {
            let ordinal = {
                let entry = ordinals.entry(group.as_slice()).or_insert(0);
                let ordinal = *entry;
                *entry += 1;
                ordinal
            };

            if !group.iter().all(|&is_null| is_null) {
                continue;
            }

            let mut cols: Vec<ArrayRef> = group_schema
                .fields()
                .iter()
                .take(n_expr)
                .map(|field| new_null_array(field.data_type(), 1))
                .collect();
            cols.push(group_id_array(group, ordinal, max_ordinal, 1)?);

            state
                .group_values
                .intern(&cols, &mut state.batch_group_indices)?;
            any_interned = true;
        }

        if any_interned {
            let total_groups = state.group_values.len();
            let false_filter = BooleanArray::from(vec![false]);
            for acc in state.accumulators.iter_mut() {
                let null_args = acc.null_arguments(&self.input_schema)?;
                let values = EvaluatedAccumulatorArgs {
                    arguments: null_args,
                    filter: Some(Arc::new(false_filter.clone())),
                };
                acc.update_batch(&values, &[0], total_groups)?;
            }
        }

        Ok(())
    }
}

/// Hash table used only for converting raw input rows directly into partial
/// aggregate state rows after partial aggregation has been skipped.
pub(in crate::aggregates) struct PartialSkipHashTable {
    pub(super) table: AggregateHashTable,
}

impl PartialSkipHashTable {
    pub(in crate::aggregates) fn convert_batch_to_state(
        &mut self,
        batch: &RecordBatch,
    ) -> Result<RecordBatch> {
        let evaluated_batch = self.table.evaluate_batch(batch)?;

        assert_eq_or_internal_err!(
            evaluated_batch.grouping_set_args.len(),
            1,
            "group_values expected to have single element"
        );
        let mut output = evaluated_batch
            .grouping_set_args
            .into_iter()
            .next()
            .unwrap_or_default();

        let state = self.table.state.building_mut();
        for (acc, values) in state
            .accumulators
            .iter_mut()
            .zip(evaluated_batch.accumulator_args.iter())
        {
            output.extend(acc.convert_to_state(values)?);
        }

        Ok(RecordBatch::try_new(
            Arc::clone(&self.table.output_schema),
            output,
        )?)
    }
}

/// State and argument information for a single Aggregate
///
/// For example, for `SELECT COUNT(x), SUM(y WHERE z > 10) ...`  there would be two
/// `HashAggregateAccumulator`, one each for `COUNT(x)` and `SUM(y WHERE z > 10)`
pub(super) struct HashAggregateAccumulator {
    /// Aggregate expression used to create a fresh accumulator for related
    /// hash tables, such as the partial-skip table.
    aggregate_expr: Arc<AggregateFunctionExpr>,

    /// Arguments to pass to this accumulator.
    ///
    /// Example: `CORR(x, y)` stores two expressions here, while `SUM(x)` stores one.
    arguments: Vec<Arc<dyn PhysicalExpr>>,

    /// Optional `FILTER` expression for this accumulator.
    ///
    /// Example: `SUM(x) FILTER (WHERE x > 10)` stores the `x > 10` predicate.
    filter: Option<Arc<dyn PhysicalExpr>>,

    /// Accumulator state for all groups for one aggregate expression.
    accumulator: Box<dyn GroupsAccumulator>,
}

pub(super) type AggregateAccumulator = HashAggregateAccumulator;

/// Evaluated aggregate arguments and filter for one input batch.
///
/// For example, `AVG(x + 1) FILTER (WHERE x > 0)` evaluates both `x + 1`
/// and `x > 0`.
///
/// These arrays can be passed directly to [`GroupsAccumulator`].
pub(super) struct EvaluatedAccumulatorArgs {
    /// Evaluated argument arrays. Some aggregate functions take multiple arguments.
    pub(super) arguments: Vec<ArrayRef>,
    /// Evaluated filter array, `Some` if the aggregate has a `FILTER` expression.
    pub(super) filter: Option<ArrayRef>,
}

/// Evaluated all group by keys and accumulator args.
///
/// e.g., `select k+1, sum(v*v) from t group by (k+1)`, this function evaluates
/// `k+1`, `v*v`
pub(super) struct EvaluatedAggregateBatch {
    /// One entry per grouping set; each entry contains all evaluated group key
    /// arrays for the current input batch.
    pub(super) grouping_set_args: Vec<Vec<ArrayRef>>,

    /// Evaluated arguments and filters, one entry per aggregate expression.
    pub(super) accumulator_args: Vec<EvaluatedAccumulatorArgs>,
}

/// Buffer for the aggregate hash table's group keys and accumulator states.
///
/// It accumulates input during aggregation and emits final results during the
/// outputting stage.
///
/// [`GroupValues`] stores the physical group-key layout, while
/// [`GroupsAccumulator`] stores per-group aggregate state.
pub(super) struct AggregateHashTableBuffer {
    /// GROUP BY expressions evaluated for each input batch.
    pub(super) group_by: Arc<PhysicalGroupBy>,

    /// Interned group keys. Accumulator state is stored separately by group index.
    pub(super) group_values: Box<dyn GroupValues>,

    /// Group index for each row in the current input batch.
    ///
    /// Each value indexes into `group_values`, and the same index is used by every
    /// accumulator to update that group's aggregate state.
    pub(super) batch_group_indices: Vec<usize>,

    /// One item per aggregate expression.
    ///
    /// Example: `COUNT(x), SUM(y)` creates two items. Each item owns the input
    /// expressions, optional filter, and accumulator state for all groups.
    pub(super) accumulators: Vec<HashAggregateAccumulator>,
}

pub(super) enum AggregateHashTableState {
    /// Accumulating input rows into group keys and aggregate state.
    Building(AggregateHashTableBuffer),
    /// Emitting results directly from group keys and aggregate state.
    Outputting(AggregateHashTableBuffer),
    /// Materialize all the output results, and then incrementally output in the `OutputtingMaterialized` state.
    ///
    /// Note this is a temporary solution until the `GroupValues` issue is solved:
    /// Issue: <https://github.com/apache/datafusion/issues/23178>
    OutputtingMaterialized(MaterializedAggregateOutput),
    Done,
}

/// Fully evaluated aggregate output and the next row offset to emit.
///
/// Final aggregate evaluation consumes accumulator state, and partial terminal
/// output should not repeatedly renumber group values with `EmitTo::First`.
/// Materialize once and then slice to honor `batch_size` across output polls.
pub(super) struct MaterializedAggregateOutput {
    batch: RecordBatch,
    offset: usize,
}

impl MaterializedAggregateOutput {
    pub(super) fn new(batch: RecordBatch) -> Self {
        Self { batch, offset: 0 }
    }

    pub(super) fn next_batch(&mut self, batch_size: usize) -> Option<RecordBatch> {
        debug_assert!(batch_size > 0);
        if self.is_exhausted() {
            return None;
        }

        let length = batch_size.min(self.batch.num_rows() - self.offset);
        let batch = self.batch.slice(self.offset, length);
        self.offset += length;
        Some(batch)
    }

    pub(super) fn is_exhausted(&self) -> bool {
        self.offset >= self.batch.num_rows()
    }

    pub(super) fn memory_size(&self) -> usize {
        self.batch.get_array_memory_size()
    }
}

impl HashAggregateAccumulator {
    pub(super) fn new(
        aggregate_expr: Arc<AggregateFunctionExpr>,
        arguments: Vec<Arc<dyn PhysicalExpr>>,
        filter: Option<Arc<dyn PhysicalExpr>>,
        accumulator: Box<dyn GroupsAccumulator>,
    ) -> Self {
        Self {
            aggregate_expr,
            arguments,
            filter,
            accumulator,
        }
    }

    /// Construct a new accumulator with the same definition, but with empty internal
    /// state buffers (empty [`GroupsAccumulator`]).
    pub(super) fn empty_like(&self) -> Result<Self> {
        let accumulator = create_group_accumulator(&self.aggregate_expr)?;
        Ok(Self::new(
            Arc::clone(&self.aggregate_expr),
            self.arguments.clone(),
            self.filter.clone(),
            accumulator,
        ))
    }

    /// Evaluate aggregate arguments and filter for one input batch.
    ///
    /// For example, `AVG(x + 1) FILTER (WHERE x > 0)` evaluates both `x + 1`
    /// and `x > 0`.
    ///
    /// These arrays can be passed directly to [`GroupsAccumulator`] next.
    pub(super) fn evaluate_acc_args(
        &self,
        batch: &RecordBatch,
    ) -> Result<EvaluatedAccumulatorArgs> {
        let arguments = self
            .arguments
            .iter()
            .map(|expr| {
                expr.evaluate(batch)
                    .and_then(|value| value.into_array(batch.num_rows()))
            })
            .collect::<Result<_>>()?;

        let filter = self
            .filter
            .as_ref()
            .map(|filter| {
                filter
                    .evaluate(batch)
                    .and_then(|value| value.into_array(batch.num_rows()))
            })
            .transpose()?;

        Ok(EvaluatedAccumulatorArgs { arguments, filter })
    }

    pub(super) fn size(&self) -> usize {
        self.accumulator.size()
    }

    pub(super) fn update_batch(
        &mut self,
        values: &EvaluatedAccumulatorArgs,
        group_indices: &[usize],
        total_num_groups: usize,
    ) -> Result<()> {
        let filter = values.filter.as_ref().map(|filter| filter.as_boolean());
        self.accumulator.update_batch(
            &values.arguments,
            group_indices,
            filter,
            total_num_groups,
        )
    }

    pub(super) fn merge_batch(
        &mut self,
        values: &EvaluatedAccumulatorArgs,
        group_indices: &[usize],
        total_num_groups: usize,
    ) -> Result<()> {
        debug_assert!(values.filter.is_none());
        self.accumulator
            .merge_batch(&values.arguments, group_indices, total_num_groups)
    }

    /// Evaluating final aggregate results according to `EmitTo`, and reset inner
    /// states. (e.g. after `evaluate(EmitTo::All)`, it returns all accumulated groups
    /// , and clear the inner buffers)
    pub(super) fn evaluate(&mut self, emit_to: EmitTo) -> Result<ArrayRef> {
        self.accumulator.evaluate(emit_to)
    }

    /// Evaluating partial aggregate results according to `EmitTo`, and reset inner
    /// states. (e.g. after `state(EmitTo::All)`, it returns all accumulated groups
    /// , and clear the inner buffers)
    pub(super) fn state(&mut self, emit_to: EmitTo) -> Result<Vec<ArrayRef>> {
        self.accumulator.state(emit_to)
    }

    pub(super) fn supports_convert_to_state(&self) -> bool {
        self.accumulator.supports_convert_to_state()
    }

    pub(super) fn convert_to_state(
        &mut self,
        values: &EvaluatedAccumulatorArgs,
    ) -> Result<Vec<ArrayRef>> {
        let opt_filter = values.filter.as_ref().map(|filter| filter.as_boolean());
        self.accumulator
            .convert_to_state(&values.arguments, opt_filter)
    }

    pub(super) fn null_arguments(
        &self,
        input_schema: &SchemaRef,
    ) -> Result<Vec<ArrayRef>> {
        self.arguments
            .iter()
            .map(|expr| {
                let data_type = expr.data_type(input_schema)?;
                Ok(new_null_array(&data_type, 1))
            })
            .collect()
    }
}

impl AggregateHashTableState {
    pub(super) fn building(&self) -> &AggregateHashTableBuffer {
        let Self::Building(state) = self else {
            unreachable!("hash aggregate table is not building")
        };
        state
    }

    pub(super) fn building_mut(&mut self) -> &mut AggregateHashTableBuffer {
        let Self::Building(state) = self else {
            unreachable!("hash aggregate table is not building")
        };
        state
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Array, Int32Array};
    use arrow::datatypes::{DataType, Field, Schema};

    use super::*;

    #[test]
    fn materialized_aggregate_output_slices_batches_until_exhausted() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "group_col",
            DataType::Int32,
            false,
        )]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5]))],
        )?;
        let mut output = MaterializedAggregateOutput::new(batch);

        assert_eq!(int32_values(&output.next_batch(2).unwrap(), 0), vec![1, 2]);
        assert_eq!(int32_values(&output.next_batch(2).unwrap(), 0), vec![3, 4]);
        assert_eq!(int32_values(&output.next_batch(2).unwrap(), 0), vec![5]);
        assert!(output.next_batch(2).is_none());
        assert!(output.is_exhausted());

        Ok(())
    }

    fn int32_values(batch: &RecordBatch, column: usize) -> Vec<i32> {
        let array = batch
            .column(column)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        (0..array.len()).map(|idx| array.value(idx)).collect()
    }
}
