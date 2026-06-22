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
use std::marker::PhantomData;
use std::sync::Arc;

use arrow::array::{ArrayRef, AsArray, BooleanArray, new_null_array};
use arrow::datatypes::SchemaRef;
use arrow::record_batch::RecordBatch;
use datafusion_common::{Result, assert_eq_or_internal_err, internal_err};
use datafusion_execution::memory_pool::proxy::VecAllocExt;
use datafusion_expr::{EmitTo, GroupsAccumulator};
use datafusion_physical_expr::aggregate::AggregateFunctionExpr;

use super::group_values::{GroupByMetrics, GroupValues, new_group_values};
use super::order::GroupOrdering;
use super::row_hash::create_group_accumulator;
use super::{
    AggregateExec, PhysicalGroupBy, aggregate_expressions, evaluate_group_by,
    group_id_array, max_duplicate_ordinal,
};
use crate::PhysicalExpr;

/// Marker for raw rows -> partial state aggregation.
pub(super) struct Partial;
/// Marker for raw rows -> partial state conversion without aggregation.
pub(super) struct PartialSkip;
/// Marker for partial state -> final value aggregation.
pub(super) struct Final;

/// Grouped hash table shared by the partial and final paths.
///
/// While building, it consumes input batches and updates group / accumulator
/// state. While outputting, it incrementally output the materialized batches.
///
/// # Marker Type
/// `AggrMode` selects the aggregate semantics.
///
/// e.g. `AggregateHashTable::<Partial>::new(...)` creates an aggregate hash table
/// for the partial hash aggregate stage, the input schema is raw rows and output
/// schema is intermediate states.
///
/// It is a zero-sized compile-time marker, so each stage keeps its update logic
/// in a separate impl block, to make the behavior difference explicit.
pub(super) struct AggregateHashTable<AggrMode> {
    /// Grouping and accumulator-specific timing metrics.
    group_by_metrics: GroupByMetrics,

    /// Raw input schema, used to evaluate expressions and synthesize empty
    /// grouping-set rows.
    input_schema: SchemaRef,

    /// Output schema: group columns followed by aggregate state or final values.
    output_schema: SchemaRef,

    /// Maximum rows per emitted output batch.
    batch_size: usize,

    /// Lifecycle-specific state: building stage / outputting stage
    state: AggregateHashTableState,

    _mode: PhantomData<AggrMode>,
}

struct HashAggregateAccumulator {
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

struct EvaluatedHashAggregateAccumulator {
    arguments: Vec<ArrayRef>,
    filter: Option<ArrayRef>,
}

/// Evaluated all group by keys and accumulator args.
///
/// e.g., `select k+1, sum(v*v) from t group by (k+1)`, this function evaluates
/// `k+1`, `v*v`
struct EvaluatedAggregateBatch {
    /// One entry per grouping set; each entry contains all evaluated group key
    /// arrays for the current input batch.
    grouping_set_args: Vec<Vec<ArrayRef>>,

    /// Evaluated arguments and filters, one entry per aggregate expression.
    accumulator_args: Vec<EvaluatedHashAggregateAccumulator>,
}

/// Hash table state while grouped aggregation is consuming input.
///
/// This owns the coupled state for:
/// - evaluating group keys,
/// - interning each distinct group,
/// - mapping each input row to its group index,
/// - evaluating aggregate inputs,
/// - updating per-group accumulator state.
struct BuildingHashTableState {
    /// GROUP BY expressions evaluated for each input batch.
    group_by: Arc<PhysicalGroupBy>,

    /// Interned group keys. Accumulator state is stored separately by group index.
    group_values: Box<dyn GroupValues>,

    /// Group index for each row in the current input batch.
    ///
    /// Each value indexes into `group_values`, and the same index is used by every
    /// accumulator to update that group's aggregate state.
    batch_group_indices: Vec<usize>,

    /// One item per aggregate expression.
    ///
    /// Example: `COUNT(x), SUM(y)` creates two items. Each item owns the input
    /// expressions, optional filter, and accumulator state for all groups.
    accumulators: Vec<HashAggregateAccumulator>,
}

enum AggregateHashTableState {
    Building(BuildingHashTableState),
    Outputting {
        output_batch: Option<RecordBatch>,
        output_batch_offset: usize,
    },
    Done,
}

impl HashAggregateAccumulator {
    fn new(
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

    fn empty_like(&self) -> Result<Self> {
        let accumulator = create_group_accumulator(&self.aggregate_expr)?;
        Ok(Self::new(
            Arc::clone(&self.aggregate_expr),
            self.arguments.clone(),
            self.filter.clone(),
            accumulator,
        ))
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<EvaluatedHashAggregateAccumulator> {
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

        Ok(EvaluatedHashAggregateAccumulator { arguments, filter })
    }

    fn update_batch(
        &mut self,
        values: &EvaluatedHashAggregateAccumulator,
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

    fn merge_batch(
        &mut self,
        values: &EvaluatedHashAggregateAccumulator,
        group_indices: &[usize],
        total_num_groups: usize,
    ) -> Result<()> {
        debug_assert!(values.filter.is_none());
        self.accumulator
            .merge_batch(&values.arguments, group_indices, total_num_groups)
    }

    fn evaluate_final(&mut self, emit_to: EmitTo) -> Result<ArrayRef> {
        self.accumulator.evaluate(emit_to)
    }

    fn state(&mut self, emit_to: EmitTo) -> Result<Vec<ArrayRef>> {
        self.accumulator.state(emit_to)
    }

    fn supports_convert_to_state(&self) -> bool {
        self.accumulator.supports_convert_to_state()
    }

    fn convert_to_state(
        &mut self,
        values: &EvaluatedHashAggregateAccumulator,
    ) -> Result<Vec<ArrayRef>> {
        let opt_filter = values.filter.as_ref().map(|filter| filter.as_boolean());
        self.accumulator
            .convert_to_state(&values.arguments, opt_filter)
    }

    fn null_arguments(&self, input_schema: &SchemaRef) -> Result<Vec<ArrayRef>> {
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
    fn building(&self) -> &BuildingHashTableState {
        let Self::Building(state) = self else {
            unreachable!("hash aggregate table is not building")
        };
        state
    }

    fn building_mut(&mut self) -> &mut BuildingHashTableState {
        let Self::Building(state) = self else {
            unreachable!("hash aggregate table is not building")
        };
        state
    }
}

impl<Mode> AggregateHashTable<Mode> {
    fn new_with_filters(
        agg: &AggregateExec,
        partition: usize,
        output_schema: SchemaRef,
        batch_size: usize,
        filters: Vec<Option<Arc<dyn PhysicalExpr>>>,
    ) -> Result<Self> {
        let input_schema = agg.input().schema();
        let aggregate_arguments = aggregate_expressions(
            &agg.aggr_expr,
            &agg.mode,
            agg.group_by.num_group_exprs(),
        )?;
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
            input_schema,
            output_schema,
            batch_size,
            state: AggregateHashTableState::Building(BuildingHashTableState {
                group_by: Arc::clone(&agg.group_by),
                group_values,
                batch_group_indices: Default::default(),
                accumulators,
            }),
            _mode: PhantomData,
        })
    }

    /// See comments in [`EvaluatedAggregateBatch`]
    fn evaluate_batch(&self, batch: &RecordBatch) -> Result<EvaluatedAggregateBatch> {
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
            .map(|acc| acc.evaluate(batch))
            .collect::<Result<Vec<_>>>()?;
        drop(timer);

        Ok(EvaluatedAggregateBatch {
            grouping_set_args,
            accumulator_args,
        })
    }

    pub(super) fn memory_size(&self) -> usize {
        match &self.state {
            AggregateHashTableState::Building(state) => {
                let acc = state
                    .accumulators
                    .iter()
                    .map(|acc| acc.accumulator.size())
                    .sum::<usize>();

                acc + state.group_values.size()
                    + state.batch_group_indices.allocated_size()
            }
            AggregateHashTableState::Outputting { output_batch, .. } => {
                output_batch_memory_size(output_batch)
            }
            AggregateHashTableState::Done => 0,
        }
    }

    /// How many distinct groups has been accumulated now.
    pub(super) fn building_group_count(&self) -> usize {
        self.state.building().group_values.len()
    }

    pub(super) fn is_building(&self) -> bool {
        matches!(self.state, AggregateHashTableState::Building(_))
    }

    pub(super) fn is_done(&self) -> bool {
        matches!(self.state, AggregateHashTableState::Done)
    }

    fn set_output_batch(&mut self, output_batch: Option<RecordBatch>) {
        self.state = AggregateHashTableState::Outputting {
            output_batch,
            output_batch_offset: 0,
        };
    }

    pub(super) fn next_output_batch(&mut self) -> Result<Option<RecordBatch>> {
        match std::mem::replace(&mut self.state, AggregateHashTableState::Done) {
            AggregateHashTableState::Outputting {
                output_batch,
                mut output_batch_offset,
            } => {
                let Some(batch) = output_batch.as_ref() else {
                    return Ok(None);
                };

                let num_rows = batch.num_rows();
                if output_batch_offset >= num_rows {
                    return Ok(None);
                }

                debug_assert!(self.batch_size > 0);
                let output_len =
                    self.batch_size.max(1).min(num_rows - output_batch_offset);
                let output = batch.slice(output_batch_offset, output_len);
                output_batch_offset += output_len;

                if output_batch_offset == num_rows {
                    self.state = AggregateHashTableState::Done;
                } else {
                    self.state = AggregateHashTableState::Outputting {
                        output_batch,
                        output_batch_offset,
                    };
                }

                debug_assert!(output.num_rows() > 0);
                debug_assert!(output.num_rows() <= self.batch_size.max(1));
                Ok(Some(output))
            }
            _ => {
                self.state = AggregateHashTableState::Done;
                internal_err!("next_output_batch must be called in the outputting state")
            }
        }
    }
}

impl AggregateHashTable<Partial> {
    pub(super) fn new(
        agg: &AggregateExec,
        partition: usize,
        output_schema: SchemaRef,
        batch_size: usize,
    ) -> Result<Self> {
        Self::new_with_filters(
            agg,
            partition,
            output_schema,
            batch_size,
            agg.filter_expr.iter().cloned().collect(),
        )
    }

    pub(super) fn can_skip_aggregation(&self) -> bool {
        self.state
            .building()
            .accumulators
            .iter()
            .all(|acc| acc.supports_convert_to_state())
    }

    /// In skip-partial-aggregation optimization, when a decision has made to skip
    /// partial stage, build a typed hash table only for aggregation state conversion
    /// row-by-row.
    pub(super) fn partial_skip_table(&self) -> Result<AggregateHashTable<PartialSkip>> {
        let state = self.state.building();
        let group_schema = state.group_by.group_schema(&self.input_schema)?;
        let group_values = new_group_values(group_schema, &GroupOrdering::None)?;
        let accumulators = state
            .accumulators
            .iter()
            .map(HashAggregateAccumulator::empty_like)
            .collect::<Result<Vec<_>>>()?;

        Ok(AggregateHashTable {
            group_by_metrics: self.group_by_metrics.clone(),
            input_schema: Arc::clone(&self.input_schema),
            output_schema: Arc::clone(&self.output_schema),
            batch_size: self.batch_size,
            state: AggregateHashTableState::Building(BuildingHashTableState {
                group_by: Arc::clone(&state.group_by),
                group_values,
                batch_group_indices: Default::default(),
                accumulators,
            }),
            _mode: PhantomData,
        })
    }

    pub(super) fn aggregate_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        let evaluated_batch = self.evaluate_batch(batch)?;
        let state = self.state.building_mut();

        let timer = self.group_by_metrics.aggregation_time.timer();
        for group_values in &evaluated_batch.grouping_set_args {
            state
                .group_values
                .intern(group_values, &mut state.batch_group_indices)?;
            let group_indices = &state.batch_group_indices;
            let total_num_groups = state.group_values.len();

            for (acc, values) in state
                .accumulators
                .iter_mut()
                .zip(evaluated_batch.accumulator_args.iter())
            {
                acc.update_batch(values, group_indices, total_num_groups)?;
            }
        }
        drop(timer);

        Ok(())
    }

    pub(super) fn start_output(&mut self) -> Result<()> {
        self.init_empty_grouping_sets()?;
        let state = self.state.building_mut();

        let output_batch = if state.group_values.is_empty() {
            None
        } else {
            let timer = self.group_by_metrics.emitting_time.timer();
            let mut output = state.group_values.emit(EmitTo::All)?;

            for acc in state.accumulators.iter_mut() {
                output.extend(acc.state(EmitTo::All)?);
            }

            let batch = RecordBatch::try_new(Arc::clone(&self.output_schema), output)?;
            debug_assert!(batch.num_rows() > 0);
            drop(timer);
            Some(batch)
        };

        self.set_output_batch(output_batch);
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
    fn init_empty_grouping_sets(&mut self) -> Result<()> {
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
                let values = EvaluatedHashAggregateAccumulator {
                    arguments: null_args,
                    filter: Some(Arc::new(false_filter.clone())),
                };
                acc.update_batch(&values, &[0], total_groups)?;
            }
        }

        Ok(())
    }
}

impl AggregateHashTable<PartialSkip> {
    pub(super) fn convert_batch_to_state(
        &mut self,
        batch: &RecordBatch,
    ) -> Result<RecordBatch> {
        let evaluated_batch = self.evaluate_batch(batch)?;

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

        let state = self.state.building_mut();
        for (acc, values) in state
            .accumulators
            .iter_mut()
            .zip(evaluated_batch.accumulator_args.iter())
        {
            output.extend(acc.convert_to_state(values)?);
        }

        Ok(RecordBatch::try_new(
            Arc::clone(&self.output_schema),
            output,
        )?)
    }
}

impl AggregateHashTable<Final> {
    pub(super) fn new(
        agg: &AggregateExec,
        partition: usize,
        output_schema: SchemaRef,
        batch_size: usize,
    ) -> Result<Self> {
        Self::new_with_filters(
            agg,
            partition,
            output_schema,
            batch_size,
            vec![None; agg.aggr_expr.len()],
        )
    }

    pub(super) fn aggregate_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        let evaluated_batch = self.evaluate_batch(batch)?;
        let state = self.state.building_mut();

        let timer = self.group_by_metrics.aggregation_time.timer();
        for group_values in &evaluated_batch.grouping_set_args {
            state
                .group_values
                .intern(group_values, &mut state.batch_group_indices)?;
            let group_indices = &state.batch_group_indices;
            let total_num_groups = state.group_values.len();

            for (acc, values) in state
                .accumulators
                .iter_mut()
                .zip(evaluated_batch.accumulator_args.iter())
            {
                acc.merge_batch(values, group_indices, total_num_groups)?;
            }
        }
        drop(timer);

        Ok(())
    }

    pub(super) fn start_output(&mut self) -> Result<()> {
        let state = self.state.building_mut();
        let output_batch = if state.group_values.is_empty() {
            None
        } else {
            let timer = self.group_by_metrics.emitting_time.timer();
            let mut output = state.group_values.emit(EmitTo::All)?;

            for acc in state.accumulators.iter_mut() {
                output.push(acc.evaluate_final(EmitTo::All)?);
            }

            let batch = RecordBatch::try_new(Arc::clone(&self.output_schema), output)?;
            debug_assert!(batch.num_rows() > 0);
            drop(timer);
            Some(batch)
        };

        self.set_output_batch(output_batch);
        Ok(())
    }
}

fn output_batch_memory_size(output_batch: &Option<RecordBatch>) -> usize {
    output_batch
        .as_ref()
        .map(RecordBatch::get_array_memory_size)
        .unwrap_or_default()
}
