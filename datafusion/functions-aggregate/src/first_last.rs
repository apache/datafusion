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

//! Defines the FIRST_VALUE/LAST_VALUE aggregations.

use std::fmt::Debug;
use std::hash::Hash;
use std::mem::size_of_val;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, AsArray, BooleanArray, BooleanBufferBuilder};
use arrow::buffer::BooleanBuffer;
use arrow::compute::{self, LexicographicalComparator, SortColumn, SortOptions};
use arrow::datatypes::{
    DataType, Date32Type, Date64Type, Decimal32Type, Decimal64Type, Decimal128Type,
    Decimal256Type, Field, FieldRef, Float16Type, Float32Type, Float64Type, Int8Type,
    Int16Type, Int32Type, Int64Type, Time32MillisecondType, Time32SecondType,
    Time64MicrosecondType, Time64NanosecondType, TimeUnit, TimestampMicrosecondType,
    TimestampMillisecondType, TimestampNanosecondType, TimestampSecondType, UInt8Type,
    UInt16Type, UInt32Type, UInt64Type,
};
use datafusion_common::cast::as_boolean_array;
use datafusion_common::utils::{compare_rows, extract_row_at_idx_to_buf, get_row_at_idx};
use datafusion_common::{
    DataFusionError, Result, ScalarValue, arrow_datafusion_err, internal_err,
    not_impl_err,
};
use datafusion_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion_expr::utils::{AggregateOrderSensitivity, format_state_name};
use datafusion_expr::{
    Accumulator, AggregateUDFImpl, Documentation, EmitTo, Expr, ExprFunctionExt,
    GroupsAccumulator, ReversedUDAF, Signature, SortExpr, Volatility,
};
use datafusion_functions_aggregate_common::utils::get_sort_options;
use datafusion_macros::user_doc;
use datafusion_physical_expr_common::sort_expr::LexOrdering;

mod state;

use state::{BytesValueState, GenericValueState, PrimitiveValueState, ValueState};

create_func!(FirstValue, first_value_udaf);
create_func!(LastValue, last_value_udaf);

/// Returns the first value in a group of values.
pub fn first_value(expression: Expr, order_by: Vec<SortExpr>) -> Expr {
    first_value_udaf()
        .call(vec![expression])
        .order_by(order_by)
        .build()
        // guaranteed to be `Expr::AggregateFunction`
        .unwrap()
}

/// Returns the last value in a group of values.
pub fn last_value(expression: Expr, order_by: Vec<SortExpr>) -> Expr {
    last_value_udaf()
        .call(vec![expression])
        .order_by(order_by)
        .build()
        // guaranteed to be `Expr::AggregateFunction`
        .unwrap()
}

fn create_groups_accumulator_helper<S: ValueState + 'static>(
    args: &AccumulatorArgs,
    is_first: bool,
    is_input_pre_ordered: bool,
    state: S,
) -> Result<Box<dyn GroupsAccumulator>> {
    let Some(ordering) = LexOrdering::new(args.order_bys.to_vec()) else {
        return internal_err!("Groups accumulator must have an ordering.");
    };

    let ordering_dtypes = ordering
        .iter()
        .map(|e| e.expr.data_type(args.schema))
        .collect::<Result<Vec<_>>>()?;

    Ok(Box::new(FirstLastGroupsAccumulator::try_new(
        state,
        ordering,
        args.ignore_nulls,
        &ordering_dtypes,
        is_first,
        is_input_pre_ordered,
    )?))
}

fn create_groups_accumulator(
    args: &AccumulatorArgs,
    is_first: bool,
    is_input_pre_ordered: bool,
    function_name: &str,
) -> Result<Box<dyn GroupsAccumulator>> {
    let data_type = args.return_field.data_type();

    macro_rules! instantiate_primitive {
        ($t:ty) => {
            create_groups_accumulator_helper(
                args,
                is_first,
                is_input_pre_ordered,
                PrimitiveValueState::<$t>::new(data_type.clone()),
            )
        };
    }

    match data_type {
        DataType::Int8 => instantiate_primitive!(Int8Type),
        DataType::Int16 => instantiate_primitive!(Int16Type),
        DataType::Int32 => instantiate_primitive!(Int32Type),
        DataType::Int64 => instantiate_primitive!(Int64Type),
        DataType::UInt8 => instantiate_primitive!(UInt8Type),
        DataType::UInt16 => instantiate_primitive!(UInt16Type),
        DataType::UInt32 => instantiate_primitive!(UInt32Type),
        DataType::UInt64 => instantiate_primitive!(UInt64Type),
        DataType::Float16 => instantiate_primitive!(Float16Type),
        DataType::Float32 => instantiate_primitive!(Float32Type),
        DataType::Float64 => instantiate_primitive!(Float64Type),

        DataType::Decimal32(_, _) => instantiate_primitive!(Decimal32Type),
        DataType::Decimal64(_, _) => instantiate_primitive!(Decimal64Type),
        DataType::Decimal128(_, _) => instantiate_primitive!(Decimal128Type),
        DataType::Decimal256(_, _) => instantiate_primitive!(Decimal256Type),

        DataType::Timestamp(TimeUnit::Second, _) => {
            instantiate_primitive!(TimestampSecondType)
        }
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            instantiate_primitive!(TimestampMillisecondType)
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            instantiate_primitive!(TimestampMicrosecondType)
        }
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            instantiate_primitive!(TimestampNanosecondType)
        }

        DataType::Date32 => instantiate_primitive!(Date32Type),
        DataType::Date64 => instantiate_primitive!(Date64Type),
        DataType::Time32(TimeUnit::Second) => instantiate_primitive!(Time32SecondType),
        DataType::Time32(TimeUnit::Millisecond) => {
            instantiate_primitive!(Time32MillisecondType)
        }
        DataType::Time64(TimeUnit::Microsecond) => {
            instantiate_primitive!(Time64MicrosecondType)
        }
        DataType::Time64(TimeUnit::Nanosecond) => {
            instantiate_primitive!(Time64NanosecondType)
        }

        DataType::Utf8
        | DataType::LargeUtf8
        | DataType::Utf8View
        | DataType::Binary
        | DataType::LargeBinary
        | DataType::BinaryView => create_groups_accumulator_helper(
            args,
            is_first,
            is_input_pre_ordered,
            BytesValueState::try_new(data_type.clone())?,
        ),

        // Nested / composite types fall through to a generic ScalarValue-backed
        // state. Slower per-batch than the primitive/bytes fast paths but still
        // avoids the per-row ScalarValue churn of the per-group `Accumulator`
        // path: winner extraction happens once per group per batch, not once
        // per candidate row.
        DataType::List(_)
        | DataType::LargeList(_)
        | DataType::ListView(_)
        | DataType::LargeListView(_)
        | DataType::FixedSizeList(_, _)
        | DataType::Struct(_)
        | DataType::Map(_, _) => create_groups_accumulator_helper(
            args,
            is_first,
            is_input_pre_ordered,
            GenericValueState::new(data_type.clone()),
        ),

        _ => internal_err!(
            "GroupsAccumulator not supported for {}({})",
            function_name,
            data_type
        ),
    }
}

fn groups_accumulator_supported(args: &AccumulatorArgs) -> bool {
    use DataType::*;
    !args.order_bys.is_empty()
        && matches!(
            args.return_field.data_type(),
            Int8 | Int16
                | Int32
                | Int64
                | UInt8
                | UInt16
                | UInt32
                | UInt64
                | Float16
                | Float32
                | Float64
                | Decimal32(_, _)
                | Decimal64(_, _)
                | Decimal128(_, _)
                | Decimal256(_, _)
                | Date32
                | Date64
                | Time32(_)
                | Time64(_)
                | Timestamp(_, _)
                | Utf8
                | LargeUtf8
                | Utf8View
                | Binary
                | LargeBinary
                | BinaryView
                | List(_)
                | LargeList(_)
                | ListView(_)
                | LargeListView(_)
                | FixedSizeList(_, _)
                | Struct(_)
                | Map(_, _)
        )
}

#[user_doc(
    doc_section(label = "General Functions"),
    description = "Returns the first element in an aggregation group according to the requested ordering. If no ordering is given, returns an arbitrary element from the group.",
    syntax_example = "first_value(expression [ORDER BY expression])",
    sql_example = r#"```sql
> SELECT first_value(column_name ORDER BY other_column) FROM table_name;
+------------------------------------------------+
| first_value(column_name ORDER BY other_column) |
+------------------------------------------------+
| first_element                                  |
+------------------------------------------------+
```"#,
    standard_argument(name = "expression",)
)]
#[derive(PartialEq, Eq, Hash, Debug)]
pub struct FirstValue {
    signature: Signature,
    is_input_pre_ordered: bool,
}

impl Default for FirstValue {
    fn default() -> Self {
        Self::new()
    }
}

impl FirstValue {
    pub fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
            is_input_pre_ordered: false,
        }
    }
}

impl AggregateUDFImpl for FirstValue {
    fn name(&self) -> &str {
        "first_value"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        not_impl_err!("Not called because the return_field_from_args is implemented")
    }

    fn return_field(&self, arg_fields: &[FieldRef]) -> Result<FieldRef> {
        // Preserve metadata from the first argument field
        Ok(Arc::new(
            Field::new(
                self.name(),
                arg_fields[0].data_type().clone(),
                true, // always nullable, there may be no rows
            )
            .with_metadata(arg_fields[0].metadata().clone()),
        ))
    }

    fn accumulator(&self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        let Some(ordering) = LexOrdering::new(acc_args.order_bys.to_vec()) else {
            return TrivialFirstValueAccumulator::try_new(
                acc_args.return_field.data_type(),
                acc_args.ignore_nulls,
            )
            .map(|acc| Box::new(acc) as _);
        };
        let ordering_dtypes = ordering
            .iter()
            .map(|e| e.expr.data_type(acc_args.schema))
            .collect::<Result<Vec<_>>>()?;
        Ok(Box::new(FirstValueAccumulator::try_new(
            acc_args.return_field.data_type(),
            &ordering_dtypes,
            ordering,
            self.is_input_pre_ordered,
            acc_args.ignore_nulls,
        )?))
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        let mut fields = vec![
            Field::new(
                format_state_name(args.name, "first_value"),
                args.return_type().clone(),
                true,
            )
            .into(),
        ];
        fields.extend(args.ordering_fields.iter().cloned());
        fields.push(
            Field::new(
                format_state_name(args.name, "first_value_is_set"),
                DataType::Boolean,
                true,
            )
            .into(),
        );
        Ok(fields)
    }

    fn groups_accumulator_supported(&self, args: AccumulatorArgs) -> bool {
        groups_accumulator_supported(&args)
    }

    fn create_groups_accumulator(
        &self,
        args: AccumulatorArgs,
    ) -> Result<Box<dyn GroupsAccumulator>> {
        create_groups_accumulator(&args, true, self.is_input_pre_ordered, self.name())
    }

    fn with_beneficial_ordering(
        self: Arc<Self>,
        beneficial_ordering: bool,
    ) -> Result<Option<Arc<dyn AggregateUDFImpl>>> {
        Ok(Some(Arc::new(Self {
            signature: self.signature.clone(),
            is_input_pre_ordered: beneficial_ordering,
        })))
    }

    fn order_sensitivity(&self) -> AggregateOrderSensitivity {
        AggregateOrderSensitivity::Beneficial
    }

    fn reverse_expr(&self) -> ReversedUDAF {
        ReversedUDAF::Reversed(last_value_udaf())
    }

    fn supports_null_handling_clause(&self) -> bool {
        true
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

struct FirstLastGroupsAccumulator<S: ValueState> {
    // ================ state ===========
    state: S,
    // Stores ordering values, of the aggregator requirement corresponding to first value
    // of the aggregator.
    // The `orderings` are stored row-wise, meaning that `orderings[group_idx]`
    // represents the ordering values corresponding to the `group_idx`-th group.
    orderings: Vec<Vec<ScalarValue>>,
    // At the beginning, `is_sets[group_idx]` is false, which means `first` is not seen yet.
    // Once we see the first value, we set the `is_sets[group_idx]` flag
    is_sets: BooleanBufferBuilder,
    // size of `self.orderings`
    // Calculating the memory usage of `self.orderings` using `ScalarValue::size_of_vec` is quite costly.
    // Therefore, we cache it and compute `size_of` only after each update
    // to avoid calling `ScalarValue::size_of_vec` by Self.size.
    size_of_orderings: usize,

    // Per-batch scoreboard shared by `get_filtered_extreme_of_each_group`
    // and the pre-ordered fast path:
    // extreme_of_each_group_buf.0[group_idx] -> idx_in_val
    // only valid if extreme_of_each_group_buf.1[group_idx] == true
    extreme_of_each_group_buf: (Vec<usize>, BooleanBufferBuilder),
    // Set by `get_filtered_extreme_of_each_group` (merge_batch /
    // convert_to_state), which clears the scoreboard at its start but leaves
    // its winners' bits set on return. `update_batch_pre_ordered` keeps the
    // scoreboard all-false between its own batches, but the two can
    // interleave on one instance (the aggregation stream calls merge_batch on
    // the same accumulators when replaying spilled state), so the fast path
    // does one full reset when this is set.
    extreme_buf_dirty: bool,
    // Batch-local list of groups touched by `update_batch_pre_ordered`,
    // reused across batches. It lets winner collection and the scoreboard
    // reset run in O(groups touched by the batch) instead of
    // O(total_num_groups): with 1M allocated groups and 64 touched per
    // batch, a full-scoreboard sweep per batch dominates the runtime.
    touched_groups_buf: Vec<usize>,

    // =========== option ============

    // Stores the applicable ordering requirement.
    ordering_req: LexOrdering,
    // true: take first element in an aggregation group according to the requested ordering.
    // false: take last element in an aggregation group according to the requested ordering.
    pick_first_in_group: bool,
    // derived from `ordering_req`.
    sort_options: Vec<SortOptions>,
    // Ignore null values.
    ignore_nulls: bool,
    // When `true` (set through `with_beneficial_ordering` by the
    // `OptimizeAggregateOrder` physical-optimizer rule), the optimizer has
    // proven that every group's rows already arrive in `ordering_req` order,
    // and `update_batch` takes the comparison-free fast path.
    is_input_pre_ordered: bool,
    default_orderings: Vec<ScalarValue>,
}

impl<S: ValueState> FirstLastGroupsAccumulator<S> {
    fn try_new(
        state: S,
        ordering_req: LexOrdering,
        ignore_nulls: bool,
        ordering_dtypes: &[DataType],
        pick_first_in_group: bool,
        is_input_pre_ordered: bool,
    ) -> Result<Self> {
        let default_orderings = ordering_dtypes
            .iter()
            .map(ScalarValue::try_from)
            .collect::<Result<_>>()?;

        let sort_options = get_sort_options(&ordering_req);

        Ok(Self {
            ordering_req,
            sort_options,
            ignore_nulls,
            default_orderings,
            state,
            orderings: Vec::new(),
            is_sets: BooleanBufferBuilder::new(0),
            size_of_orderings: 0,
            extreme_of_each_group_buf: (Vec::new(), BooleanBufferBuilder::new(0)),
            extreme_buf_dirty: false,
            touched_groups_buf: Vec::new(),
            pick_first_in_group,
            is_input_pre_ordered,
        })
    }

    fn should_update_state(
        &self,
        group_idx: usize,
        new_ordering_values: &[ScalarValue],
    ) -> Result<bool> {
        if !self.is_sets.get_bit(group_idx) {
            return Ok(true);
        }

        debug_assert_eq!(new_ordering_values.len(), self.ordering_req.len());
        let current_ordering = &self.orderings[group_idx];
        compare_rows(current_ordering, new_ordering_values, &self.sort_options).map(|x| {
            if self.pick_first_in_group {
                x.is_gt()
            } else {
                x.is_lt()
            }
        })
    }

    fn take_orderings(&mut self, emit_to: EmitTo) -> Vec<Vec<ScalarValue>> {
        let result = emit_to.take_needed(&mut self.orderings);

        match emit_to {
            EmitTo::All => self.size_of_orderings = 0,
            EmitTo::First(_) => {
                self.size_of_orderings -=
                    result.iter().map(ScalarValue::size_of_vec).sum::<usize>()
            }
        }

        result
    }

    fn resize_states(&mut self, new_size: usize) {
        self.state.resize(new_size);

        if self.orderings.len() < new_size {
            let current_len = self.orderings.len();

            self.orderings
                .resize(new_size, self.default_orderings.clone());

            self.size_of_orderings += (new_size - current_len)
                * ScalarValue::size_of_vec(
                    // Note: In some cases (such as in the unit test below)
                    // ScalarValue::size_of_vec(&self.default_orderings) != ScalarValue::size_of_vec(&self.default_orderings.clone())
                    // This may be caused by the different vec.capacity() values?
                    self.orderings.last().unwrap(),
                );
        }

        self.is_sets.resize(new_size);

        self.extreme_of_each_group_buf.0.resize(new_size, 0);
        self.extreme_of_each_group_buf.1.resize(new_size);
    }

    fn update_state(
        &mut self,
        group_idx: usize,
        orderings: &[ScalarValue],
        array: &ArrayRef,
        idx: usize,
    ) -> Result<()> {
        self.state.update(group_idx, array, idx)?;
        self.is_sets.set_bit(group_idx, true);

        debug_assert_eq!(orderings.len(), self.ordering_req.len());
        let old_size = ScalarValue::size_of_vec(&self.orderings[group_idx]);
        self.orderings[group_idx].clear();
        self.orderings[group_idx].extend_from_slice(orderings);
        let new_size = ScalarValue::size_of_vec(&self.orderings[group_idx]);
        self.size_of_orderings = self.size_of_orderings - old_size + new_size;
        Ok(())
    }

    fn take_state(
        &mut self,
        emit_to: EmitTo,
    ) -> Result<(ArrayRef, Vec<Vec<ScalarValue>>, BooleanBuffer)> {
        emit_to.take_needed(&mut self.extreme_of_each_group_buf.0);
        self.extreme_of_each_group_buf
            .1
            .truncate(self.extreme_of_each_group_buf.0.len());

        Ok((
            self.state.take(emit_to)?,
            self.take_orderings(emit_to),
            state::take_need(&mut self.is_sets, emit_to),
        ))
    }

    // should be used in test only
    #[cfg(test)]
    fn compute_size_of_orderings(&self) -> usize {
        self.orderings
            .iter()
            .map(ScalarValue::size_of_vec)
            .sum::<usize>()
    }
    /// Returns a vector of tuples `(group_idx, idx_in_val)` representing the index of the
    /// minimum value in `orderings` for each group, using lexicographical comparison.
    /// Values are filtered using `opt_filter` and `is_set_arr` if provided.
    fn get_filtered_extreme_of_each_group(
        &mut self,
        orderings: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        vals: &ArrayRef,
        is_set_arr: Option<&BooleanArray>,
    ) -> Result<Vec<(usize, usize)>> {
        // Set all values in extreme_of_each_group_buf.1 to false.
        self.extreme_of_each_group_buf.1.truncate(0);
        self.extreme_of_each_group_buf
            .1
            .append_n(self.is_sets.len(), false);

        // No need to call `clear` since `self.extreme_of_each_group_buf.0[group_idx]`
        // is only valid when `self.extreme_of_each_group_buf.1[group_idx] == true`.

        let comparator = {
            assert_eq!(orderings.len(), self.ordering_req.len());
            let sort_columns = orderings
                .iter()
                .zip(self.ordering_req.iter())
                .map(|(array, req)| SortColumn {
                    values: Arc::clone(array),
                    options: Some(req.options),
                })
                .collect::<Vec<_>>();

            LexicographicalComparator::try_new(&sort_columns)?
        };

        for (idx_in_val, group_idx) in group_indices.iter().enumerate() {
            let group_idx = *group_idx;

            // A row passes the FILTER clause only when the predicate is
            // `true`; rows whose predicate evaluates to `null` are excluded.
            let passed_filter =
                opt_filter.is_none_or(|x| x.is_valid(idx_in_val) && x.value(idx_in_val));
            // `is_set_arr` carries the user FILTER clause (including its
            // nulls) when the state was produced by `convert_to_state`, so
            // the validity check is required here as well (#22666).
            let is_set =
                is_set_arr.is_none_or(|x| x.is_valid(idx_in_val) && x.value(idx_in_val));

            if !passed_filter || !is_set {
                continue;
            }

            if self.ignore_nulls && vals.is_null(idx_in_val) {
                continue;
            }

            let is_valid = self.extreme_of_each_group_buf.1.get_bit(group_idx);

            if !is_valid {
                self.extreme_of_each_group_buf.1.set_bit(group_idx, true);
                self.extreme_of_each_group_buf.0[group_idx] = idx_in_val;
            } else {
                let ordering = comparator
                    .compare(self.extreme_of_each_group_buf.0[group_idx], idx_in_val);

                if (ordering.is_gt() && self.pick_first_in_group)
                    || (ordering.is_lt() && !self.pick_first_in_group)
                {
                    self.extreme_of_each_group_buf.0[group_idx] = idx_in_val;
                }
            }
        }

        // Winners' bits stay set on return; tell the pre-ordered fast path
        // that the scoreboard needs a reset before it can trust its all-false
        // invariant again.
        self.extreme_buf_dirty = true;

        Ok(self
            .extreme_of_each_group_buf
            .0
            .iter()
            .enumerate()
            .filter(|(group_idx, _)| self.extreme_of_each_group_buf.1.get_bit(*group_idx))
            .map(|(group_idx, idx_in_val)| (group_idx, *idx_in_val))
            .collect::<Vec<_>>())
    }

    /// Comparison-free `update_batch` for pre-ordered input.
    ///
    /// `is_input_pre_ordered` is only set (through `with_beneficial_ordering`,
    /// by the `OptimizeAggregateOrder` physical-optimizer rule) after the
    /// optimizer has proven that the input ordering satisfies the ordered
    /// group-by prefix followed by this aggregate's own ordering requirement.
    /// Under that guarantee each group's rows arrive in `ordering_req` order,
    /// so:
    /// - within a batch, the extreme row of a group is simply its first
    ///   (FIRST_VALUE) or last (LAST_VALUE) qualifying row, and
    /// - across batches, a later batch's winner always beats an earlier one
    ///   for LAST_VALUE, and never does for FIRST_VALUE.
    ///
    /// No lexicographic comparator is built and `compare_rows` never runs.
    /// The winner's ordering values are still materialized into
    /// `self.orderings`: partial state must carry them, because the final
    /// aggregation stage merges states coming from different partitions whose
    /// relative order is not guaranteed, so `merge_batch` keeps comparing.
    ///
    /// Tie handling: among rows whose ordering keys compare equal, this path
    /// picks the physically last qualifying row for LAST_VALUE (and the first
    /// for FIRST_VALUE), matching the single-group pre-ordered accumulator and
    /// `Iterator::max_by`. The tournament path keeps the first-seen row of a
    /// tie instead (its comparisons are strict). Both are valid answers —
    /// which row of a tie wins is unspecified — but results can differ on
    /// tied keys.
    fn update_batch_pre_ordered(
        &mut self,
        values_and_order_cols: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
    ) -> Result<()> {
        let vals = &values_and_order_cols[0];

        // `extreme_of_each_group_buf.1` is sized by `resize_states` and kept
        // all-false between batches: each batch records the groups it touched
        // in `touched_groups_buf` and clears exactly those bits before
        // returning. Neither the reset nor the winner collection may scan
        // `total_num_groups` -- with 1M allocated groups and 64 touched, a
        // full sweep turns a ~13us batch into ~400us and erases the win.
        debug_assert!(self.touched_groups_buf.is_empty());
        if self.extreme_buf_dirty {
            // A merge (spill replay) ran on this instance and left its
            // winners' bits set; restore the all-false invariant once.
            self.extreme_of_each_group_buf.1.truncate(0);
            self.extreme_of_each_group_buf
                .1
                .append_n(self.is_sets.len(), false);
            self.extreme_buf_dirty = false;
        }

        for (idx_in_val, &group_idx) in group_indices.iter().enumerate() {
            // A row passes the FILTER clause only when the predicate is
            // `true`; rows whose predicate evaluates to `null` are excluded.
            let passed_filter =
                opt_filter.is_none_or(|x| x.is_valid(idx_in_val) && x.value(idx_in_val));
            if !passed_filter {
                continue;
            }
            if self.ignore_nulls && vals.is_null(idx_in_val) {
                continue;
            }

            let touched_this_batch = self.extreme_of_each_group_buf.1.get_bit(group_idx);
            if self.pick_first_in_group
                && (self.is_sets.get_bit(group_idx) || touched_this_batch)
            {
                // The first qualifying row wins; groups decided by an earlier
                // batch (or earlier in this batch) never change again.
                continue;
            }
            if !touched_this_batch {
                self.extreme_of_each_group_buf.1.set_bit(group_idx, true);
                self.touched_groups_buf.push(group_idx);
            }
            // For LAST_VALUE, later qualifying rows unconditionally overwrite.
            self.extreme_of_each_group_buf.0[group_idx] = idx_in_val;
        }

        let mut ordering_buf = Vec::with_capacity(self.ordering_req.len());
        // Take the buffer to appease the borrow checker; `update_state`
        // needs `&mut self`.
        let touched = std::mem::take(&mut self.touched_groups_buf);
        for &group_idx in &touched {
            let idx = self.extreme_of_each_group_buf.0[group_idx];
            extract_row_at_idx_to_buf(
                &values_and_order_cols[1..],
                idx,
                &mut ordering_buf,
            )?;
            self.update_state(group_idx, &ordering_buf, vals, idx)?;
        }
        // Restore the all-false invariant by clearing only the touched bits.
        for &group_idx in &touched {
            self.extreme_of_each_group_buf.1.set_bit(group_idx, false);
        }
        self.touched_groups_buf = touched;
        self.touched_groups_buf.clear();

        Ok(())
    }
}

impl<S: ValueState + 'static> GroupsAccumulator for FirstLastGroupsAccumulator<S> {
    fn update_batch(
        &mut self,
        // e.g. first_value(a order by b): values_and_order_cols will be [a, b]
        values_and_order_cols: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        self.resize_states(total_num_groups);

        if self.is_input_pre_ordered {
            return self.update_batch_pre_ordered(
                values_and_order_cols,
                group_indices,
                opt_filter,
            );
        }

        let vals = &values_and_order_cols[0];

        let mut ordering_buf = Vec::with_capacity(self.ordering_req.len());

        // The overhead of calling `extract_row_at_idx_to_buf` is somewhat high, so we need to minimize its calls as much as possible.
        for (group_idx, idx) in self
            .get_filtered_extreme_of_each_group(
                &values_and_order_cols[1..],
                group_indices,
                opt_filter,
                vals,
                None,
            )?
            .into_iter()
        {
            extract_row_at_idx_to_buf(
                &values_and_order_cols[1..],
                idx,
                &mut ordering_buf,
            )?;

            if self.should_update_state(group_idx, &ordering_buf)? {
                self.update_state(group_idx, &ordering_buf, vals, idx)?;
            }
        }

        Ok(())
    }

    fn evaluate(&mut self, emit_to: EmitTo) -> Result<ArrayRef> {
        Ok(self.take_state(emit_to)?.0)
    }

    fn state(&mut self, emit_to: EmitTo) -> Result<Vec<ArrayRef>> {
        let (val_arr, orderings, is_sets) = self.take_state(emit_to)?;
        let mut result = Vec::with_capacity(self.orderings.len() + 2);

        result.push(val_arr);

        let ordering_cols = {
            let mut ordering_cols = Vec::with_capacity(self.ordering_req.len());
            for _ in 0..self.ordering_req.len() {
                ordering_cols.push(Vec::with_capacity(self.orderings.len()));
            }
            for row in orderings.into_iter() {
                debug_assert_eq!(row.len(), self.ordering_req.len());
                for (col_idx, ordering) in row.into_iter().enumerate() {
                    ordering_cols[col_idx].push(ordering);
                }
            }

            ordering_cols
        };
        for ordering_col in ordering_cols {
            result.push(ScalarValue::iter_to_array(ordering_col)?);
        }

        result.push(Arc::new(BooleanArray::new(is_sets, None)));

        Ok(result)
    }

    fn merge_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        total_num_groups: usize,
    ) -> Result<()> {
        self.resize_states(total_num_groups);

        let mut ordering_buf = Vec::with_capacity(self.ordering_req.len());

        let Some((is_set_arr, val_and_order_cols)) = values.split_last() else {
            return internal_err!("Empty row in FIRST_VALUE");
        };

        let is_set_arr = as_boolean_array(is_set_arr)?;

        let vals = &values[0];
        // The overhead of calling `extract_row_at_idx_to_buf` is somewhat high, so we need to minimize its calls as much as possible.
        let groups = self.get_filtered_extreme_of_each_group(
            &val_and_order_cols[1..],
            group_indices,
            None,
            vals,
            Some(is_set_arr),
        )?;

        for (group_idx, idx) in groups.into_iter() {
            extract_row_at_idx_to_buf(&val_and_order_cols[1..], idx, &mut ordering_buf)?;

            if self.should_update_state(group_idx, &ordering_buf)? {
                self.update_state(group_idx, &ordering_buf, vals, idx)?;
            }
        }

        Ok(())
    }

    fn size(&self) -> usize {
        self.state.size()
            + self.is_sets.capacity() / 8 // capacity is in bits, so convert to bytes
            + self.size_of_orderings
            + self.extreme_of_each_group_buf.0.capacity() * size_of::<usize>()
            + self.extreme_of_each_group_buf.1.capacity() / 8
    }
    fn convert_to_state(
        &self,
        values: &[ArrayRef],
        opt_filter: Option<&BooleanArray>,
    ) -> Result<Vec<ArrayRef>> {
        let mut result = values.to_vec();
        match opt_filter {
            Some(f) => {
                result.push(Arc::new(f.clone()));
                Ok(result)
            }
            None => {
                result.push(Arc::new(BooleanArray::from(vec![true; values[0].len()])));
                Ok(result)
            }
        }
    }
}

/// This accumulator is used when there is no ordering specified for the
/// `FIRST_VALUE` aggregation. It simply returns the first value it sees
/// according to the pre-existing ordering of the input data, and provides
/// a fast path for this case without needing to maintain any ordering state.
#[derive(Debug)]
pub struct TrivialFirstValueAccumulator {
    first: ScalarValue,
    // Whether we have seen the first value yet.
    is_set: bool,
    // Ignore null values.
    ignore_nulls: bool,
}

impl TrivialFirstValueAccumulator {
    /// Creates a new `TrivialFirstValueAccumulator` for the given `data_type`.
    pub fn try_new(data_type: &DataType, ignore_nulls: bool) -> Result<Self> {
        ScalarValue::try_from(data_type).map(|first| Self {
            first,
            is_set: false,
            ignore_nulls,
        })
    }
}

impl Accumulator for TrivialFirstValueAccumulator {
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![self.first.clone(), ScalarValue::from(self.is_set)])
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if !self.is_set {
            // Get first entry according to the pre-existing ordering (0th index):
            let value = &values[0];
            let mut first_idx = None;
            if self.ignore_nulls {
                // If ignoring nulls, find the first non-null value.
                for i in 0..value.len() {
                    if !value.is_null(i) {
                        first_idx = Some(i);
                        break;
                    }
                }
            } else if !value.is_empty() {
                // If not ignoring nulls, return the first value if it exists.
                first_idx = Some(0);
            }
            if let Some(first_idx) = first_idx {
                self.first = ScalarValue::try_from_array(&values[0], first_idx)?;
                self.first.compact();
                self.is_set = true;
            }
        }
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        // FIRST_VALUE(first1, first2, first3, ...)
        // Second index contains is_set flag.
        if !self.is_set {
            let flags = states[1].as_boolean();
            validate_is_set_flags(flags, "first_value")?;

            let filtered_states =
                filter_states_according_to_is_set(&states[0..1], flags)?;
            if let Some(first) = filtered_states.first()
                && !first.is_empty()
            {
                self.first = ScalarValue::try_from_array(first, 0)?;
                self.is_set = true;
            }
        }
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        Ok(self.first.clone())
    }

    fn size(&self) -> usize {
        size_of_val(self) - size_of_val(&self.first) + self.first.size()
    }
}

#[derive(Debug)]
pub struct FirstValueAccumulator {
    first: ScalarValue,
    // Whether we have seen the first value yet.
    is_set: bool,
    // Stores values of the ordering columns corresponding to the first value.
    // These values are used during merging of multiple partitions.
    orderings: Vec<ScalarValue>,
    // Stores the applicable ordering requirement.
    ordering_req: LexOrdering,
    // derived from `ordering_req`.
    sort_options: Vec<SortOptions>,
    // Stores whether incoming data already satisfies the ordering requirement.
    is_input_pre_ordered: bool,
    // Ignore null values.
    ignore_nulls: bool,
}

impl FirstValueAccumulator {
    /// Creates a new `FirstValueAccumulator` for the given `data_type`.
    pub fn try_new(
        data_type: &DataType,
        ordering_dtypes: &[DataType],
        ordering_req: LexOrdering,
        is_input_pre_ordered: bool,
        ignore_nulls: bool,
    ) -> Result<Self> {
        let orderings = ordering_dtypes
            .iter()
            .map(ScalarValue::try_from)
            .collect::<Result<_>>()?;
        let sort_options = get_sort_options(&ordering_req);
        ScalarValue::try_from(data_type).map(|first| Self {
            first,
            is_set: false,
            orderings,
            ordering_req,
            sort_options,
            is_input_pre_ordered,
            ignore_nulls,
        })
    }

    // Updates state with the values in the given row.
    fn update_with_new_row(&mut self, mut row: Vec<ScalarValue>) {
        // Ensure any Array based scalars hold have a single value to reduce memory pressure
        for s in row.iter_mut() {
            s.compact();
        }
        self.first = row.remove(0);
        self.orderings = row;
        self.is_set = true;
    }

    fn get_first_idx(&self, values: &[ArrayRef]) -> Result<Option<usize>> {
        let [value, ordering_values @ ..] = values else {
            return internal_err!("Empty row in FIRST_VALUE");
        };
        if self.is_input_pre_ordered {
            // Get first entry according to the pre-existing ordering (0th index):
            if self.ignore_nulls {
                // If ignoring nulls, find the first non-null value.
                for i in 0..value.len() {
                    if !value.is_null(i) {
                        return Ok(Some(i));
                    }
                }
                return Ok(None);
            } else {
                // If not ignoring nulls, return the first value if it exists.
                return Ok((!value.is_empty()).then_some(0));
            }
        }

        let sort_columns = ordering_values
            .iter()
            .zip(self.ordering_req.iter())
            .map(|(values, req)| SortColumn {
                values: Arc::clone(values),
                options: Some(req.options),
            })
            .collect::<Vec<_>>();

        let comparator = LexicographicalComparator::try_new(&sort_columns)?;

        let min_index = if self.ignore_nulls {
            (0..value.len())
                .filter(|&index| !value.is_null(index))
                .min_by(|&a, &b| comparator.compare(a, b))
        } else {
            (0..value.len()).min_by(|&a, &b| comparator.compare(a, b))
        };

        Ok(min_index)
    }
}

impl Accumulator for FirstValueAccumulator {
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        let mut result = vec![self.first.clone()];
        result.extend(self.orderings.iter().cloned());
        result.push(ScalarValue::from(self.is_set));
        Ok(result)
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if let Some(first_idx) = self.get_first_idx(values)? {
            let row = get_row_at_idx(values, first_idx)?;
            if !self.is_set
                || (!self.is_input_pre_ordered
                    && compare_rows(&self.orderings, &row[1..], &self.sort_options)?
                        .is_gt())
            {
                self.update_with_new_row(row);
            }
        }
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        // FIRST_VALUE(first1, first2, first3, ...)
        // last index contains is_set flag.
        let is_set_idx = states.len() - 1;
        let flags = states[is_set_idx].as_boolean();
        validate_is_set_flags(flags, "first_value")?;

        let filtered_states =
            filter_states_according_to_is_set(&states[0..is_set_idx], flags)?;
        // 1..is_set_idx range corresponds to ordering section
        let sort_columns =
            convert_to_sort_cols(&filtered_states[1..is_set_idx], &self.ordering_req);

        let comparator = LexicographicalComparator::try_new(&sort_columns)?;
        let min = (0..filtered_states[0].len()).min_by(|&a, &b| comparator.compare(a, b));

        if let Some(first_idx) = min {
            let mut first_row = get_row_at_idx(&filtered_states, first_idx)?;
            // When collecting orderings, we exclude the is_set flag from the state.
            let first_ordering = &first_row[1..is_set_idx];
            // Either there is no existing value, or there is an earlier version in new data.
            if !self.is_set
                || compare_rows(&self.orderings, first_ordering, &self.sort_options)?
                    .is_gt()
            {
                // Update with first value in the state. Note that we should exclude the
                // is_set flag from the state. Otherwise, we will end up with a state
                // containing two is_set flags.
                assert!(is_set_idx <= first_row.len());
                first_row.resize(is_set_idx, ScalarValue::Null);
                self.update_with_new_row(first_row);
            }
        }
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        Ok(self.first.clone())
    }

    fn size(&self) -> usize {
        size_of_val(self) - size_of_val(&self.first)
            + self.first.size()
            + ScalarValue::size_of_vec(&self.orderings)
            - size_of_val(&self.orderings)
    }
}

#[user_doc(
    doc_section(label = "General Functions"),
    description = "Returns the last element in an aggregation group according to the requested ordering. If no ordering is given, returns an arbitrary element from the group.",
    syntax_example = "last_value(expression [ORDER BY expression])",
    sql_example = r#"```sql
> SELECT last_value(column_name ORDER BY other_column) FROM table_name;
+-----------------------------------------------+
| last_value(column_name ORDER BY other_column) |
+-----------------------------------------------+
| last_element                                  |
+-----------------------------------------------+
```"#,
    standard_argument(name = "expression",)
)]
#[derive(PartialEq, Eq, Hash, Debug)]
pub struct LastValue {
    signature: Signature,
    is_input_pre_ordered: bool,
}

impl Default for LastValue {
    fn default() -> Self {
        Self::new()
    }
}

impl LastValue {
    pub fn new() -> Self {
        Self {
            signature: Signature::any(1, Volatility::Immutable),
            is_input_pre_ordered: false,
        }
    }
}

impl AggregateUDFImpl for LastValue {
    fn name(&self) -> &str {
        "last_value"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        not_impl_err!("Not called because the return_field_from_args is implemented")
    }

    fn return_field(&self, arg_fields: &[FieldRef]) -> Result<FieldRef> {
        // Preserve metadata from the first argument field
        Ok(Arc::new(
            Field::new(
                self.name(),
                arg_fields[0].data_type().clone(),
                true, // always nullable, there may be no rows
            )
            .with_metadata(arg_fields[0].metadata().clone()),
        ))
    }

    fn accumulator(&self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        let Some(ordering) = LexOrdering::new(acc_args.order_bys.to_vec()) else {
            return TrivialLastValueAccumulator::try_new(
                acc_args.return_field.data_type(),
                acc_args.ignore_nulls,
            )
            .map(|acc| Box::new(acc) as _);
        };
        let ordering_dtypes = ordering
            .iter()
            .map(|e| e.expr.data_type(acc_args.schema))
            .collect::<Result<Vec<_>>>()?;
        Ok(Box::new(LastValueAccumulator::try_new(
            acc_args.return_field.data_type(),
            &ordering_dtypes,
            ordering,
            self.is_input_pre_ordered,
            acc_args.ignore_nulls,
        )?))
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        let mut fields = vec![
            Field::new(
                format_state_name(args.name, "last_value"),
                args.return_field.data_type().clone(),
                true,
            )
            .into(),
        ];
        fields.extend(args.ordering_fields.iter().cloned());
        fields.push(
            Field::new(
                format_state_name(args.name, "last_value_is_set"),
                DataType::Boolean,
                true,
            )
            .into(),
        );
        Ok(fields)
    }

    fn with_beneficial_ordering(
        self: Arc<Self>,
        beneficial_ordering: bool,
    ) -> Result<Option<Arc<dyn AggregateUDFImpl>>> {
        Ok(Some(Arc::new(Self {
            signature: self.signature.clone(),
            is_input_pre_ordered: beneficial_ordering,
        })))
    }

    fn order_sensitivity(&self) -> AggregateOrderSensitivity {
        AggregateOrderSensitivity::Beneficial
    }

    fn reverse_expr(&self) -> ReversedUDAF {
        ReversedUDAF::Reversed(first_value_udaf())
    }

    fn supports_null_handling_clause(&self) -> bool {
        true
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }

    fn groups_accumulator_supported(&self, args: AccumulatorArgs) -> bool {
        groups_accumulator_supported(&args)
    }

    fn create_groups_accumulator(
        &self,
        args: AccumulatorArgs,
    ) -> Result<Box<dyn GroupsAccumulator>> {
        create_groups_accumulator(&args, false, self.is_input_pre_ordered, self.name())
    }
}

/// This accumulator is used when there is no ordering specified for the
/// `LAST_VALUE` aggregation. It simply updates the last value it sees
/// according to the pre-existing ordering of the input data, and provides
/// a fast path for this case without needing to maintain any ordering state.
#[derive(Debug)]
pub struct TrivialLastValueAccumulator {
    last: ScalarValue,
    // The `is_set` flag keeps track of whether the last value is finalized.
    // This information is used to discriminate genuine NULLs and NULLS that
    // occur due to empty partitions.
    is_set: bool,
    // Ignore null values.
    ignore_nulls: bool,
}

impl TrivialLastValueAccumulator {
    /// Creates a new `TrivialLastValueAccumulator` for the given `data_type`.
    pub fn try_new(data_type: &DataType, ignore_nulls: bool) -> Result<Self> {
        ScalarValue::try_from(data_type).map(|last| Self {
            last,
            is_set: false,
            ignore_nulls,
        })
    }
}

impl Accumulator for TrivialLastValueAccumulator {
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![self.last.clone(), ScalarValue::from(self.is_set)])
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        // Get last entry according to the pre-existing ordering (0th index):
        let value = &values[0];
        let mut last_idx = None;
        if self.ignore_nulls {
            // If ignoring nulls, find the last non-null value.
            for i in (0..value.len()).rev() {
                if !value.is_null(i) {
                    last_idx = Some(i);
                    break;
                }
            }
        } else if !value.is_empty() {
            // If not ignoring nulls, return the last value if it exists.
            last_idx = Some(value.len() - 1);
        }
        if let Some(last_idx) = last_idx {
            self.last = ScalarValue::try_from_array(&values[0], last_idx)?;
            self.last.compact();
            self.is_set = true;
        }
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        // LAST_VALUE(last1, last2, last3, ...)
        // Second index contains is_set flag.
        let flags = states[1].as_boolean();
        validate_is_set_flags(flags, "last_value")?;

        let filtered_states = filter_states_according_to_is_set(&states[0..1], flags)?;
        if let Some(last) = filtered_states.last()
            && !last.is_empty()
        {
            self.last = ScalarValue::try_from_array(last, last.len() - 1)?;
            self.is_set = true;
        }
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        Ok(self.last.clone())
    }

    fn size(&self) -> usize {
        size_of_val(self) - size_of_val(&self.last) + self.last.size()
    }
}

#[derive(Debug)]
struct LastValueAccumulator {
    last: ScalarValue,
    // The `is_set` flag keeps track of whether the last value is finalized.
    // This information is used to discriminate genuine NULLs and NULLS that
    // occur due to empty partitions.
    is_set: bool,
    // Stores values of the ordering columns corresponding to the first value.
    // These values are used during merging of multiple partitions.
    orderings: Vec<ScalarValue>,
    // Stores the applicable ordering requirement.
    ordering_req: LexOrdering,
    // derived from `ordering_req`.
    sort_options: Vec<SortOptions>,
    // Stores whether incoming data already satisfies the ordering requirement.
    is_input_pre_ordered: bool,
    // Ignore null values.
    ignore_nulls: bool,
}

impl LastValueAccumulator {
    /// Creates a new `LastValueAccumulator` for the given `data_type`.
    pub fn try_new(
        data_type: &DataType,
        ordering_dtypes: &[DataType],
        ordering_req: LexOrdering,
        is_input_pre_ordered: bool,
        ignore_nulls: bool,
    ) -> Result<Self> {
        let orderings = ordering_dtypes
            .iter()
            .map(ScalarValue::try_from)
            .collect::<Result<_>>()?;
        let sort_options = get_sort_options(&ordering_req);
        ScalarValue::try_from(data_type).map(|last| Self {
            last,
            is_set: false,
            orderings,
            ordering_req,
            sort_options,
            is_input_pre_ordered,
            ignore_nulls,
        })
    }

    // Updates state with the values in the given row.
    fn update_with_new_row(&mut self, mut row: Vec<ScalarValue>) {
        // Ensure any Array based scalars hold have a single value to reduce memory pressure
        for s in row.iter_mut() {
            s.compact();
        }
        self.last = row.remove(0);
        self.orderings = row;
        self.is_set = true;
    }

    fn get_last_idx(&self, values: &[ArrayRef]) -> Result<Option<usize>> {
        let [value, ordering_values @ ..] = values else {
            return internal_err!("Empty row in LAST_VALUE");
        };
        if self.is_input_pre_ordered {
            // Get last entry according to the order of data:
            if self.ignore_nulls {
                // If ignoring nulls, find the last non-null value.
                for i in (0..value.len()).rev() {
                    if !value.is_null(i) {
                        return Ok(Some(i));
                    }
                }
                return Ok(None);
            } else {
                return Ok((!value.is_empty()).then_some(value.len() - 1));
            }
        }

        let sort_columns = ordering_values
            .iter()
            .zip(self.ordering_req.iter())
            .map(|(values, req)| SortColumn {
                values: Arc::clone(values),
                options: Some(req.options),
            })
            .collect::<Vec<_>>();

        let comparator = LexicographicalComparator::try_new(&sort_columns)?;
        let max_ind = if self.ignore_nulls {
            (0..value.len())
                .filter(|&index| !(value.is_null(index)))
                .max_by(|&a, &b| comparator.compare(a, b))
        } else {
            (0..value.len()).max_by(|&a, &b| comparator.compare(a, b))
        };

        Ok(max_ind)
    }
}

impl Accumulator for LastValueAccumulator {
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        let mut result = vec![self.last.clone()];
        result.extend(self.orderings.clone());
        result.push(ScalarValue::from(self.is_set));
        Ok(result)
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if let Some(last_idx) = self.get_last_idx(values)? {
            let row = get_row_at_idx(values, last_idx)?;
            let orderings = &row[1..];
            // Update when there is a more recent entry
            if !self.is_set
                || self.is_input_pre_ordered
                || compare_rows(&self.orderings, orderings, &self.sort_options)?.is_lt()
            {
                self.update_with_new_row(row);
            }
        }
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        // LAST_VALUE(last1, last2, last3, ...)
        // last index contains is_set flag.
        let is_set_idx = states.len() - 1;
        let flags = states[is_set_idx].as_boolean();
        validate_is_set_flags(flags, "last_value")?;

        let filtered_states =
            filter_states_according_to_is_set(&states[0..is_set_idx], flags)?;
        // 1..is_set_idx range corresponds to ordering section
        let sort_columns =
            convert_to_sort_cols(&filtered_states[1..is_set_idx], &self.ordering_req);

        let comparator = LexicographicalComparator::try_new(&sort_columns)?;
        let max = (0..filtered_states[0].len()).max_by(|&a, &b| comparator.compare(a, b));

        if let Some(last_idx) = max {
            let mut last_row = get_row_at_idx(&filtered_states, last_idx)?;
            // When collecting orderings, we exclude the is_set flag from the state.
            let last_ordering = &last_row[1..is_set_idx];
            // Either there is no existing value, or there is a newer (latest)
            // version in the new data:
            if !self.is_set
                || self.is_input_pre_ordered
                || compare_rows(&self.orderings, last_ordering, &self.sort_options)?
                    .is_lt()
            {
                // Update with last value in the state. Note that we should exclude the
                // is_set flag from the state. Otherwise, we will end up with a state
                // containing two is_set flags.
                assert!(is_set_idx <= last_row.len());
                last_row.resize(is_set_idx, ScalarValue::Null);
                self.update_with_new_row(last_row);
            }
        }
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        Ok(self.last.clone())
    }

    fn size(&self) -> usize {
        size_of_val(self) - size_of_val(&self.last)
            + self.last.size()
            + ScalarValue::size_of_vec(&self.orderings)
            - size_of_val(&self.orderings)
    }
}

/// Validates that `is_set flags` do not contain NULL values.
fn validate_is_set_flags(flags: &BooleanArray, function_name: &str) -> Result<()> {
    if flags.null_count() > 0 {
        return Err(DataFusionError::Internal(format!(
            "{function_name}: is_set flags contain nulls"
        )));
    }
    Ok(())
}

/// Filters states according to the `is_set` flag at the last column and returns
/// the resulting states.
fn filter_states_according_to_is_set(
    states: &[ArrayRef],
    flags: &BooleanArray,
) -> Result<Vec<ArrayRef>> {
    states
        .iter()
        .map(|state| compute::filter(state, flags).map_err(|e| arrow_datafusion_err!(e)))
        .collect()
}

/// Combines array refs and their corresponding orderings to construct `SortColumn`s.
fn convert_to_sort_cols(arrs: &[ArrayRef], sort_exprs: &LexOrdering) -> Vec<SortColumn> {
    arrs.iter()
        .zip(sort_exprs.iter())
        .map(|(item, sort_expr)| SortColumn {
            values: Arc::clone(item),
            options: Some(sort_expr.options),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use std::iter::repeat_with;

    use arrow::{
        array::{BooleanArray, Int64Array, ListArray, PrimitiveArray, StringArray},
        buffer::NullBuffer,
        compute::SortOptions,
        datatypes::Schema,
    };
    use datafusion_physical_expr::{PhysicalSortExpr, expressions::col};

    use super::*;

    #[test]
    fn test_first_last_value_value() -> Result<()> {
        let mut first_accumulator =
            TrivialFirstValueAccumulator::try_new(&DataType::Int64, false)?;
        let mut last_accumulator =
            TrivialLastValueAccumulator::try_new(&DataType::Int64, false)?;
        // first value in the tuple is start of the range (inclusive),
        // second value in the tuple is end of the range (exclusive)
        let ranges: Vec<(i64, i64)> = vec![(0, 10), (1, 11), (2, 13)];
        // create 3 ArrayRefs between each interval e.g from 0 to 9, 1 to 10, 2 to 12
        let arrs = ranges
            .into_iter()
            .map(|(start, end)| {
                Arc::new(Int64Array::from((start..end).collect::<Vec<_>>())) as ArrayRef
            })
            .collect::<Vec<_>>();
        for arr in arrs {
            // Once first_value is set, accumulator should remember it.
            // It shouldn't update first_value for each new batch
            first_accumulator.update_batch(&[Arc::clone(&arr)])?;
            // last_value should be updated for each new batch.
            last_accumulator.update_batch(&[arr])?;
        }
        // First Value comes from the first value of the first batch which is 0
        assert_eq!(first_accumulator.evaluate()?, ScalarValue::Int64(Some(0)));
        // Last value comes from the last value of the last batch which is 12
        assert_eq!(last_accumulator.evaluate()?, ScalarValue::Int64(Some(12)));
        Ok(())
    }

    #[test]
    fn test_first_last_state_after_merge() -> Result<()> {
        let ranges: Vec<(i64, i64)> = vec![(0, 10), (1, 11), (2, 13)];
        // create 3 ArrayRefs between each interval e.g from 0 to 9, 1 to 10, 2 to 12
        let arrs = ranges
            .into_iter()
            .map(|(start, end)| {
                Arc::new((start..end).collect::<Int64Array>()) as ArrayRef
            })
            .collect::<Vec<_>>();

        // FirstValueAccumulator
        let mut first_accumulator =
            TrivialFirstValueAccumulator::try_new(&DataType::Int64, false)?;

        first_accumulator.update_batch(&[Arc::clone(&arrs[0])])?;
        let state1 = first_accumulator.state()?;

        let mut first_accumulator =
            TrivialFirstValueAccumulator::try_new(&DataType::Int64, false)?;
        first_accumulator.update_batch(&[Arc::clone(&arrs[1])])?;
        let state2 = first_accumulator.state()?;

        assert_eq!(state1.len(), state2.len());

        let mut states = vec![];

        for idx in 0..state1.len() {
            states.push(compute::concat(&[
                &state1[idx].to_array()?,
                &state2[idx].to_array()?,
            ])?);
        }

        let mut first_accumulator =
            TrivialFirstValueAccumulator::try_new(&DataType::Int64, false)?;
        first_accumulator.merge_batch(&states)?;

        let merged_state = first_accumulator.state()?;
        assert_eq!(merged_state.len(), state1.len());

        // LastValueAccumulator
        let mut last_accumulator =
            TrivialLastValueAccumulator::try_new(&DataType::Int64, false)?;

        last_accumulator.update_batch(&[Arc::clone(&arrs[0])])?;
        let state1 = last_accumulator.state()?;

        let mut last_accumulator =
            TrivialLastValueAccumulator::try_new(&DataType::Int64, false)?;
        last_accumulator.update_batch(&[Arc::clone(&arrs[1])])?;
        let state2 = last_accumulator.state()?;

        assert_eq!(state1.len(), state2.len());

        let mut states = vec![];

        for idx in 0..state1.len() {
            states.push(compute::concat(&[
                &state1[idx].to_array()?,
                &state2[idx].to_array()?,
            ])?);
        }

        let mut last_accumulator =
            TrivialLastValueAccumulator::try_new(&DataType::Int64, false)?;
        last_accumulator.merge_batch(&states)?;

        let merged_state = last_accumulator.state()?;
        assert_eq!(merged_state.len(), state1.len());
        assert_eq!(last_accumulator.evaluate()?, ScalarValue::Int64(Some(10)));

        Ok(())
    }

    #[test]
    fn test_trivial_last_value_merge_all_flags_false() -> Result<()> {
        let mut acc = TrivialLastValueAccumulator::try_new(&DataType::Int64, false)?;
        let states: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from(vec![None, None])),
            Arc::new(BooleanArray::from(vec![false, false])),
        ];

        acc.merge_batch(&states)?;
        assert_eq!(acc.evaluate()?, ScalarValue::Int64(None));
        Ok(())
    }

    #[test]
    fn test_first_group_acc() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::Int64, true),
            Field::new("c", DataType::Int64, true),
            Field::new("d", DataType::Int32, true),
            Field::new("e", DataType::Boolean, true),
        ]));

        let sort_keys = [PhysicalSortExpr {
            expr: col("c", &schema).unwrap(),
            options: SortOptions::default(),
        }];

        let mut group_acc = FirstLastGroupsAccumulator::try_new(
            PrimitiveValueState::<Int64Type>::new(DataType::Int64),
            sort_keys.into(),
            true,
            &[DataType::Int64],
            true,
            false,
        )?;

        let mut val_with_orderings = {
            let mut val_with_orderings = Vec::<ArrayRef>::new();

            let vals = Arc::new(Int64Array::from(vec![Some(1), None, Some(3), Some(-6)]));
            let orderings = Arc::new(Int64Array::from(vec![1, -9, 3, -6]));

            val_with_orderings.push(vals);
            val_with_orderings.push(orderings);

            val_with_orderings
        };

        group_acc.update_batch(
            &val_with_orderings,
            &[0, 1, 2, 1],
            Some(&BooleanArray::from(vec![true, true, false, true])),
            3,
        )?;
        assert_eq!(
            group_acc.size_of_orderings,
            group_acc.compute_size_of_orderings()
        );

        let state = group_acc.state(EmitTo::All)?;

        let expected_state: Vec<Arc<dyn Array>> = vec![
            Arc::new(Int64Array::from(vec![Some(1), Some(-6), None])),
            Arc::new(Int64Array::from(vec![Some(1), Some(-6), None])),
            Arc::new(BooleanArray::from(vec![true, true, false])),
        ];
        assert_eq!(state, expected_state);

        assert_eq!(
            group_acc.size_of_orderings,
            group_acc.compute_size_of_orderings()
        );

        group_acc.merge_batch(&state, &[0, 1, 2], 3)?;

        assert_eq!(
            group_acc.size_of_orderings,
            group_acc.compute_size_of_orderings()
        );

        val_with_orderings.clear();
        val_with_orderings.push(Arc::new(Int64Array::from(vec![6, 6])));
        val_with_orderings.push(Arc::new(Int64Array::from(vec![6, 6])));

        group_acc.update_batch(&val_with_orderings, &[1, 2], None, 4)?;

        let binding = group_acc.evaluate(EmitTo::All)?;
        let eval_result = binding.as_any().downcast_ref::<Int64Array>().unwrap();

        // group 0 keeps merged value=1 (ordering=1).
        // group 1 keeps merged value=-6 (ordering=-6 < 6, so -6 is "first").
        // group 2 had no merged value (is_set=false), so update_batch value=6 wins.
        let expect: PrimitiveArray<Int64Type> =
            Int64Array::from(vec![Some(1), Some(-6), Some(6), None]);

        assert_eq!(eval_result, &expect);

        assert_eq!(
            group_acc.size_of_orderings,
            group_acc.compute_size_of_orderings()
        );

        Ok(())
    }

    #[test]
    fn test_group_acc_size_of_ordering() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::Int64, true),
            Field::new("c", DataType::Int64, true),
            Field::new("d", DataType::Int32, true),
            Field::new("e", DataType::Boolean, true),
        ]));

        let sort_keys = [PhysicalSortExpr {
            expr: col("c", &schema).unwrap(),
            options: SortOptions::default(),
        }];

        let mut group_acc = FirstLastGroupsAccumulator::try_new(
            PrimitiveValueState::<Int64Type>::new(DataType::Int64),
            sort_keys.into(),
            true,
            &[DataType::Int64],
            true,
            false,
        )?;

        let val_with_orderings = {
            let mut val_with_orderings = Vec::<ArrayRef>::new();

            let vals = Arc::new(Int64Array::from(vec![Some(1), None, Some(3), Some(-6)]));
            let orderings = Arc::new(Int64Array::from(vec![1, -9, 3, -6]));

            val_with_orderings.push(vals);
            val_with_orderings.push(orderings);

            val_with_orderings
        };

        for _ in 0..10 {
            group_acc.update_batch(
                &val_with_orderings,
                &[0, 1, 2, 1],
                Some(&BooleanArray::from(vec![true, true, false, true])),
                100,
            )?;
            assert_eq!(
                group_acc.size_of_orderings,
                group_acc.compute_size_of_orderings()
            );

            group_acc.state(EmitTo::First(2))?;
            assert_eq!(
                group_acc.size_of_orderings,
                group_acc.compute_size_of_orderings()
            );

            let s = group_acc.state(EmitTo::All)?;
            assert_eq!(
                group_acc.size_of_orderings,
                group_acc.compute_size_of_orderings()
            );

            group_acc.merge_batch(&s, &Vec::from_iter(0..s[0].len()), 100)?;
            assert_eq!(
                group_acc.size_of_orderings,
                group_acc.compute_size_of_orderings()
            );

            group_acc.evaluate(EmitTo::First(2))?;
            assert_eq!(
                group_acc.size_of_orderings,
                group_acc.compute_size_of_orderings()
            );

            group_acc.evaluate(EmitTo::All)?;
            assert_eq!(
                group_acc.size_of_orderings,
                group_acc.compute_size_of_orderings()
            );
        }

        Ok(())
    }

    #[test]
    fn test_last_group_acc() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::Int64, true),
            Field::new("c", DataType::Int64, true),
            Field::new("d", DataType::Int32, true),
            Field::new("e", DataType::Boolean, true),
        ]));

        let sort_keys = [PhysicalSortExpr {
            expr: col("c", &schema).unwrap(),
            options: SortOptions::default(),
        }];

        let mut group_acc = FirstLastGroupsAccumulator::try_new(
            PrimitiveValueState::<Int64Type>::new(DataType::Int64),
            sort_keys.into(),
            true,
            &[DataType::Int64],
            false,
            false,
        )?;

        let mut val_with_orderings = {
            let mut val_with_orderings = Vec::<ArrayRef>::new();

            let vals = Arc::new(Int64Array::from(vec![Some(1), None, Some(3), Some(-6)]));
            let orderings = Arc::new(Int64Array::from(vec![1, -9, 3, -6]));

            val_with_orderings.push(vals);
            val_with_orderings.push(orderings);

            val_with_orderings
        };

        group_acc.update_batch(
            &val_with_orderings,
            &[0, 1, 2, 1],
            Some(&BooleanArray::from(vec![true, true, false, true])),
            3,
        )?;

        let state = group_acc.state(EmitTo::All)?;

        let expected_state: Vec<Arc<dyn Array>> = vec![
            Arc::new(Int64Array::from(vec![Some(1), Some(-6), None])),
            Arc::new(Int64Array::from(vec![Some(1), Some(-6), None])),
            Arc::new(BooleanArray::from(vec![true, true, false])),
        ];
        assert_eq!(state, expected_state);

        group_acc.merge_batch(&state, &[0, 1, 2], 3)?;

        val_with_orderings.clear();
        val_with_orderings.push(Arc::new(Int64Array::from(vec![66, 6])));
        val_with_orderings.push(Arc::new(Int64Array::from(vec![66, 6])));

        group_acc.update_batch(&val_with_orderings, &[1, 2], None, 4)?;

        let binding = group_acc.evaluate(EmitTo::All)?;
        let eval_result = binding.as_any().downcast_ref::<Int64Array>().unwrap();

        // group 0: merged value=1 (ordering=1, is_set=true), update not called.
        // group 1: merged value=-6 (ordering=-6, is_set=true); update ordering=66 > -6
        //          → LAST_VALUE keeps the higher ordering, so group 1 becomes 66.
        // group 2: is_set=false after merge; update_batch sets it to 6.
        let expect: PrimitiveArray<Int64Type> =
            Int64Array::from(vec![Some(1), Some(66), Some(6), None]);

        assert_eq!(eval_result, &expect);

        Ok(())
    }

    /// Rows whose FILTER predicate evaluates to `null` must not pass the
    /// filter, even when the underlying value bit at the null slot is `true`
    /// (#22666).
    #[test]
    fn test_group_acc_filter_null_predicate() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("c", DataType::Int64, true),
        ]));

        let sort_keys = [PhysicalSortExpr {
            expr: col("c", &schema).unwrap(),
            options: SortOptions::default(),
        }];

        let mut group_acc = FirstLastGroupsAccumulator::try_new(
            PrimitiveValueState::<Int64Type>::new(DataType::Int64),
            sort_keys.into(),
            true,
            &[DataType::Int64],
            true,
            false,
        )?;

        let val_with_orderings: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from(vec![10, 20, 30])),
            Arc::new(Int64Array::from(vec![10, 20, 30])),
        ];

        // Row 0: predicate is null (but its value bit is true, as produced by
        // kernels such as `b < 1` when the null slot's underlying value is 0)
        // Row 1: predicate is false
        // Row 2: predicate is true
        let filter = BooleanArray::new(
            BooleanBuffer::from(vec![false, true, false, true]),
            Some(NullBuffer::from(BooleanBuffer::from(vec![
                true, false, true, true,
            ]))),
        )
        .slice(1, 3);
        assert_eq!(filter.offset(), 1);

        group_acc.update_batch(&val_with_orderings, &[0, 0, 1], Some(&filter), 2)?;

        let binding = group_acc.evaluate(EmitTo::All)?;
        let eval_result = binding.as_any().downcast_ref::<Int64Array>().unwrap();

        // Group 0 has no row with a `true` predicate, so it must stay unset.
        // Group 1 takes the only row with a `true` predicate.
        let expect: PrimitiveArray<Int64Type> = Int64Array::from(vec![None, Some(30)]);
        assert_eq!(eval_result, &expect);

        Ok(())
    }

    /// `convert_to_state` stores the user FILTER clause (including its nulls)
    /// in the `is_set` state column, so `merge_batch` must not treat a null
    /// `is_set` entry with a set value bit as "is set" (#22666).
    #[test]
    fn test_group_acc_merge_null_is_set() -> Result<()> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("c", DataType::Int64, true),
        ]));

        let sort_keys = [PhysicalSortExpr {
            expr: col("c", &schema).unwrap(),
            options: SortOptions::default(),
        }];

        let group_acc = FirstLastGroupsAccumulator::try_new(
            PrimitiveValueState::<Int64Type>::new(DataType::Int64),
            sort_keys.clone().into(),
            true,
            &[DataType::Int64],
            true,
            false,
        )?;

        let val_with_orderings: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from(vec![10, 20])),
            Arc::new(Int64Array::from(vec![10, 20])),
        ];

        // Same null-with-set-value-bit filter as above, carried into the state
        let filter = BooleanArray::new(
            BooleanBuffer::from(vec![true, true]),
            Some(NullBuffer::from(BooleanBuffer::from(vec![false, true]))),
        );

        let state = group_acc.convert_to_state(&val_with_orderings, Some(&filter))?;
        assert_eq!(state.len(), 3);

        let mut merging_acc = FirstLastGroupsAccumulator::try_new(
            PrimitiveValueState::<Int64Type>::new(DataType::Int64),
            sort_keys.into(),
            true,
            &[DataType::Int64],
            true,
            false,
        )?;

        merging_acc.merge_batch(&state, &[0, 0], 1)?;

        let binding = merging_acc.evaluate(EmitTo::All)?;
        let eval_result = binding.as_any().downcast_ref::<Int64Array>().unwrap();

        // Only the second row is valid and passes; the null-predicate row must
        // be skipped even though its value bit is true.
        let expect: PrimitiveArray<Int64Type> = Int64Array::from(vec![Some(20)]);
        assert_eq!(eval_result, &expect);

        Ok(())
    }

    #[test]
    fn test_first_list_acc_size() -> Result<()> {
        fn size_after_batch(values: &[ArrayRef]) -> Result<usize> {
            let mut first_accumulator = TrivialFirstValueAccumulator::try_new(
                &DataType::List(Arc::new(Field::new_list_field(DataType::Int64, false))),
                false,
            )?;

            first_accumulator.update_batch(values)?;

            Ok(first_accumulator.size())
        }

        let batch1 = ListArray::from_iter_primitive::<Int32Type, _, _>(
            repeat_with(|| Some(vec![Some(1)])).take(10000),
        );
        let batch2 =
            ListArray::from_iter_primitive::<Int32Type, _, _>([Some(vec![Some(1)])]);

        let size1 = size_after_batch(&[Arc::new(batch1)])?;
        let size2 = size_after_batch(&[Arc::new(batch2)])?;
        assert_eq!(size1, size2);

        Ok(())
    }

    #[test]
    fn test_last_list_acc_size() -> Result<()> {
        fn size_after_batch(values: &[ArrayRef]) -> Result<usize> {
            let mut last_accumulator = TrivialLastValueAccumulator::try_new(
                &DataType::List(Arc::new(Field::new_list_field(DataType::Int64, false))),
                false,
            )?;

            last_accumulator.update_batch(values)?;

            Ok(last_accumulator.size())
        }

        let batch1 = ListArray::from_iter_primitive::<Int32Type, _, _>(
            repeat_with(|| Some(vec![Some(1)])).take(10000),
        );
        let batch2 =
            ListArray::from_iter_primitive::<Int32Type, _, _>([Some(vec![Some(1)])]);

        let size1 = size_after_batch(&[Arc::new(batch1)])?;
        let size2 = size_after_batch(&[Arc::new(batch2)])?;
        assert_eq!(size1, size2);

        Ok(())
    }

    #[test]
    fn test_first_value_merge_with_is_set_nulls() -> Result<()> {
        // Test data with corrupted is_set flag
        let value = Arc::new(StringArray::from(vec![Some("first_string")])) as ArrayRef;
        let corrupted_flag = Arc::new(BooleanArray::from(vec![None])) as ArrayRef;

        // Test TrivialFirstValueAccumulator
        let mut trivial_accumulator =
            TrivialFirstValueAccumulator::try_new(&DataType::Utf8, false)?;
        let trivial_states = vec![Arc::clone(&value), Arc::clone(&corrupted_flag)];
        let result = trivial_accumulator.merge_batch(&trivial_states);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("is_set flags contain nulls")
        );

        // Test FirstValueAccumulator (with ordering)
        let schema = Schema::new(vec![Field::new("ordering", DataType::Int64, false)]);
        let ordering_expr = col("ordering", &schema)?;
        let mut ordered_accumulator = FirstValueAccumulator::try_new(
            &DataType::Utf8,
            &[DataType::Int64],
            LexOrdering::new(vec![PhysicalSortExpr {
                expr: ordering_expr,
                options: SortOptions::default(),
            }])
            .unwrap(),
            false,
            false,
        )?;
        let ordering = Arc::new(Int64Array::from(vec![Some(1)])) as ArrayRef;
        let ordered_states = vec![value, ordering, corrupted_flag];
        let result = ordered_accumulator.merge_batch(&ordered_states);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("is_set flags contain nulls")
        );

        Ok(())
    }

    #[test]
    fn test_last_value_merge_with_is_set_nulls() -> Result<()> {
        // Test data with corrupted is_set flag
        let value = Arc::new(StringArray::from(vec![Some("last_string")])) as ArrayRef;
        let corrupted_flag = Arc::new(BooleanArray::from(vec![None])) as ArrayRef;

        // Test TrivialLastValueAccumulator
        let mut trivial_accumulator =
            TrivialLastValueAccumulator::try_new(&DataType::Utf8, false)?;
        let trivial_states = vec![Arc::clone(&value), Arc::clone(&corrupted_flag)];
        let result = trivial_accumulator.merge_batch(&trivial_states);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("is_set flags contain nulls")
        );

        // Test LastValueAccumulator (with ordering)
        let schema = Schema::new(vec![Field::new("ordering", DataType::Int64, false)]);
        let ordering_expr = col("ordering", &schema)?;
        let mut ordered_accumulator = LastValueAccumulator::try_new(
            &DataType::Utf8,
            &[DataType::Int64],
            LexOrdering::new(vec![PhysicalSortExpr {
                expr: ordering_expr,
                options: SortOptions::default(),
            }])
            .unwrap(),
            false,
            false,
        )?;
        let ordering = Arc::new(Int64Array::from(vec![Some(1)])) as ArrayRef;
        let ordered_states = vec![value, ordering, corrupted_flag];
        let result = ordered_accumulator.merge_batch(&ordered_states);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("is_set flags contain nulls")
        );

        Ok(())
    }

    /// End-to-end integration test for the nested-type support added to
    /// [`FirstLastGroupsAccumulator`]: build the accumulator directly with a
    /// [`GenericValueState`] for `List<Int32>` and verify that winners are
    /// selected correctly across multiple batches.
    ///
    /// Mirrors the shape produced by SQL like:
    /// ```sql
    /// SELECT first_value(list_col ORDER BY o DESC) FROM t GROUP BY p
    /// ```
    /// which previously fell back to the per-group `Accumulator` path and
    /// blew up on wide payloads.
    #[test]
    fn test_first_group_acc_list_int32() -> Result<()> {
        let value_type =
            DataType::List(Arc::new(Field::new("item", DataType::Int32, true)));
        let schema = Arc::new(Schema::new(vec![
            Field::new("val", value_type.clone(), true),
            Field::new("ord", DataType::Int64, true),
        ]));
        let sort_keys = [PhysicalSortExpr {
            expr: col("ord", &schema)?,
            options: SortOptions {
                descending: true,
                nulls_first: false,
            },
        }];

        let mut group_acc = FirstLastGroupsAccumulator::try_new(
            GenericValueState::new(value_type.clone()),
            sort_keys.into(),
            false,
            &[DataType::Int64],
            /* pick_first = */ true,
            false,
        )?;

        // Batch 1: four rows across two groups.
        // Winners (largest ord per group with pick_first=true + DESC):
        //   group 0 -> ord=30 -> [3, 3, 3]
        //   group 1 -> ord=40 -> [4, 4, 4, 4]
        let values_1 = ListArray::from_iter_primitive::<Int32Type, _, _>([
            Some(vec![Some(1)]),
            Some(vec![Some(2), Some(2)]),
            Some(vec![Some(3), Some(3), Some(3)]),
            Some(vec![Some(4), Some(4), Some(4), Some(4)]),
        ]);
        let orderings_1 = Int64Array::from(vec![10, 20, 30, 40]);
        group_acc.update_batch(
            &[
                Arc::new(values_1) as ArrayRef,
                Arc::new(orderings_1) as ArrayRef,
            ],
            &[0, 1, 0, 1],
            None,
            2,
        )?;

        // Batch 2: group 0 gets a new winner ord=50 -> [9, 9]; group 1
        // keeps its previous winner (5 < 40).
        let values_2 = ListArray::from_iter_primitive::<Int32Type, _, _>([
            Some(vec![Some(9), Some(9)]),
            Some(vec![Some(8)]),
        ]);
        let orderings_2 = Int64Array::from(vec![50, 5]);
        group_acc.update_batch(
            &[
                Arc::new(values_2) as ArrayRef,
                Arc::new(orderings_2) as ArrayRef,
            ],
            &[0, 1],
            None,
            2,
        )?;

        let result = group_acc.evaluate(EmitTo::All)?;
        let result = result.as_list::<i32>();
        assert_eq!(result.len(), 2);
        let g0 = result.value(0);
        let g0 = g0.as_primitive::<Int32Type>();
        assert_eq!(g0.len(), 2);
        assert_eq!(g0.value(0), 9);
        assert_eq!(g0.value(1), 9);
        let g1 = result.value(1);
        let g1 = g1.as_primitive::<Int32Type>();
        assert_eq!(g1.len(), 4);
        for i in 0..4 {
            assert_eq!(g1.value(i), 4);
        }
        Ok(())
    }

    /// Regression test for the wide-payload memory blow-up: run the full
    /// aggregate loop over a batch large enough that the per-group
    /// `Accumulator` path would have generated N * batch-worth of state
    /// (via `ScalarValue::List` clones) and verify that the reported
    /// accumulator size stays proportional to `#groups`, not `#rows`.
    #[test]
    fn test_first_group_acc_list_size_bounded_by_groups() -> Result<()> {
        let value_type =
            DataType::List(Arc::new(Field::new("item", DataType::Int32, true)));
        let schema = Arc::new(Schema::new(vec![
            Field::new("val", value_type.clone(), true),
            Field::new("ord", DataType::Int64, true),
        ]));
        let sort_keys = [PhysicalSortExpr {
            expr: col("ord", &schema)?,
            options: SortOptions {
                descending: true,
                nulls_first: false,
            },
        }];
        let mut group_acc = FirstLastGroupsAccumulator::try_new(
            GenericValueState::new(value_type),
            sort_keys.into(),
            false,
            &[DataType::Int64],
            true,
            false,
        )?;

        // 10 groups × 10_000 candidate rows per group (100_000 total). Each
        // list value has ~10 elements. Under the old per-group `Accumulator`
        // + Arc-slice code path this would pin every batch in memory.
        const GROUPS: usize = 10;
        const ROWS_PER_GROUP: usize = 10_000;
        const N: usize = GROUPS * ROWS_PER_GROUP;
        let values = ListArray::from_iter_primitive::<Int32Type, _, _>(
            repeat_with(|| Some(vec![Some(1_i32); 10])).take(N),
        );
        let orderings = Int64Array::from((0..N as i64).collect::<Vec<_>>());
        let group_indices: Vec<usize> = (0..N).map(|i| i % GROUPS).collect();

        group_acc.update_batch(
            &[
                Arc::new(values) as ArrayRef,
                Arc::new(orderings) as ArrayRef,
            ],
            &group_indices,
            None,
            GROUPS,
        )?;

        // Sanity: the retained size must be small — well under what a single
        // input batch worth of list buffers would occupy. The exact number is
        // implementation-dependent, but should be O(GROUPS * per-list), not
        // O(N * per-list).
        let size = group_acc.size();
        assert!(
            size < 100_000,
            "accumulator size {size} bytes is not bounded by #groups (10 groups × ~10 int32 list elements)"
        );

        // Winner per group is the row with the largest ord — with our layout
        // that's the last row assigned to each group.
        let result = group_acc.evaluate(EmitTo::All)?;
        let result = result.as_list::<i32>();
        assert_eq!(result.len(), GROUPS);
        for g in 0..GROUPS {
            let winner = result.value(g);
            let winner = winner.as_primitive::<Int32Type>();
            assert_eq!(winner.len(), 10);
            for i in 0..10 {
                assert_eq!(winner.value(i), 1);
            }
        }
        Ok(())
    }

    /// End-to-end memory-savings regression test.
    ///
    /// Streams many independent batches of wide `List<Int32>` payload through
    /// the accumulator, dropping each source batch immediately after feeding
    /// it in. The test then verifies three things:
    ///
    ///   1. The accumulator still emits the correct winners after every
    ///      source batch has been dropped (proves that stored values are
    ///      owned copies, not `Arc` slices into batches that no longer
    ///      exist).
    ///   2. No buffer of any past source batch is shared by the emitted
    ///      output — the raw data-buffer pointer of every source batch is
    ///      recorded, and the final output's buffers must not alias any of
    ///      them (proves `compact()` copied the winners into owned memory).
    ///   3. The accumulator's reported `size()` stays bounded by
    ///      `#groups * per-group-cost`, independent of `#batches * #rows`.
    ///
    /// This is the regression test for the wide-payload pinning behaviour
    /// that motivated this PR.
    #[test]
    fn test_first_group_acc_list_no_source_batch_pinning() -> Result<()> {
        let value_type =
            DataType::List(Arc::new(Field::new("item", DataType::Int32, true)));
        let schema = Arc::new(Schema::new(vec![
            Field::new("val", value_type.clone(), true),
            Field::new("ord", DataType::Int64, true),
        ]));
        let sort_keys = [PhysicalSortExpr {
            expr: col("ord", &schema)?,
            options: SortOptions {
                descending: true,
                nulls_first: false,
            },
        }];
        let mut group_acc = FirstLastGroupsAccumulator::try_new(
            GenericValueState::new(value_type),
            sort_keys.into(),
            false,
            &[DataType::Int64],
            true,
            false,
        )?;

        const GROUPS: usize = 4;
        const BATCHES: usize = 50;
        const ROWS_PER_BATCH: usize = 256;

        // Record the raw pointer of each source batch's Int32 value-data
        // buffer. If `compact()` did its job, the accumulator's final
        // output must not share any of these pointers — every winner
        // value should have been copied into an owned buffer.
        let mut source_value_ptrs: Vec<*const u8> = Vec::with_capacity(BATCHES);

        // Track the running-max ord we have fed to each group so the test's
        // "expected winner" oracle matches the accumulator's choice.
        let mut expected_ord = [i64::MIN; GROUPS];
        let mut expected_val_repeat = [0_i32; GROUPS];

        for batch in 0..BATCHES {
            // Each batch's list values are `[batch as i32; group_idx + 1]`
            // — a distinct payload per (batch, row) so we can verify the
            // winner by content.
            let values = ListArray::from_iter_primitive::<Int32Type, _, _>(
                (0..ROWS_PER_BATCH).map(|i| {
                    let g = i % GROUPS;
                    Some(vec![Some(batch as i32); g + 1])
                }),
            );
            let orderings = Int64Array::from(
                (0..ROWS_PER_BATCH as i64)
                    .map(|i| batch as i64 * ROWS_PER_BATCH as i64 + i)
                    .collect::<Vec<_>>(),
            );
            let group_indices: Vec<usize> =
                (0..ROWS_PER_BATCH).map(|i| i % GROUPS).collect();

            // Update the oracle: the last row in this batch that hits each
            // group has the largest ord for that group in this batch.
            for i in (0..ROWS_PER_BATCH).rev() {
                let g = i % GROUPS;
                let ord = batch as i64 * ROWS_PER_BATCH as i64 + i as i64;
                if ord > expected_ord[g] {
                    expected_ord[g] = ord;
                    expected_val_repeat[g] = batch as i32;
                }
            }

            // Capture the raw pointer of this batch's Int32 value-data
            // buffer *before* handing ownership to the accumulator. Int32
            // arrays have a single value buffer at index 0.
            source_value_ptrs.push(values.values().to_data().buffers()[0].as_ptr());

            let values_arc: Arc<dyn Array> = Arc::new(values);
            let orderings_arc: Arc<dyn Array> = Arc::new(orderings);

            group_acc.update_batch(
                &[values_arc, orderings_arc],
                &group_indices,
                None,
                GROUPS,
            )?;

            // Drop happens implicitly at end of scope.
        }

        // (2) Size is bounded by #groups. The exact number is
        // implementation-dependent but should be orders of magnitude below
        // `BATCHES * ROWS_PER_BATCH * per-list-cost` (the amount that would
        // be retained under the old Arc-slice pinning bug).
        let size = group_acc.size();
        assert!(
            size < 10_000,
            "accumulator size {size} bytes is not bounded by #groups \
             (expected O({GROUPS}) not O({BATCHES} * {ROWS_PER_BATCH}))"
        );

        // (1) Winners are still readable and match the oracle.
        let result = group_acc.evaluate(EmitTo::All)?;
        let result_list = result.as_list::<i32>();
        assert_eq!(result_list.len(), GROUPS);
        for (g, expected_repeat) in expected_val_repeat.iter().enumerate().take(GROUPS) {
            let winner = result_list.value(g);
            let winner = winner.as_primitive::<Int32Type>();
            assert_eq!(winner.len(), g + 1, "winner list length for group {g}");
            for i in 0..winner.len() {
                assert_eq!(
                    winner.value(i),
                    *expected_repeat,
                    "winner payload mismatch for group {g}"
                );
            }
        }

        // (3) The critical byte-level check: the emitted output's Int32
        // value-data buffer must NOT share a raw pointer with any of the
        // source batches. If `compact()` were omitted, `list_array.value(i)`
        // would yield a slice whose backing buffer points into the source
        // batch — the accumulator would then either pin the batch or emit
        // an output that shares its buffer.
        let result_values_ptr = result_list.values().to_data().buffers()[0].as_ptr();
        for (i, src_ptr) in source_value_ptrs.iter().enumerate() {
            assert_ne!(
                *src_ptr, result_values_ptr,
                "emitted result's Int32 value buffer aliases source batch \
                 {i}'s buffer; compact() is not making an owned copy"
            );
        }
        Ok(())
    }

    // ==================== pre-ordered fast path (#24771) ====================

    use arrow::datatypes::{Field, Int64Type};

    /// Builds a grouped first/last accumulator over Int64 values with a
    /// two-column Int64 ordering requirement `(o1, o2)`.
    #[expect(clippy::fn_params_excessive_bools)]
    fn grouped_acc(
        pick_first: bool,
        ignore_nulls: bool,
        pre_ordered: bool,
        descending: bool,
    ) -> Result<FirstLastGroupsAccumulator<PrimitiveValueState<Int64Type>>> {
        let schema = Schema::new(vec![
            Field::new("v", DataType::Int64, true),
            Field::new("o1", DataType::Int64, true),
            Field::new("o2", DataType::Int64, true),
        ]);
        let options = SortOptions {
            descending,
            nulls_first: false,
        };
        let ordering = LexOrdering::new(vec![
            PhysicalSortExpr::new(col("o1", &schema)?, options),
            PhysicalSortExpr::new(col("o2", &schema)?, options),
        ])
        .unwrap();
        FirstLastGroupsAccumulator::try_new(
            PrimitiveValueState::<Int64Type>::new(DataType::Int64),
            ordering,
            ignore_nulls,
            &[DataType::Int64, DataType::Int64],
            pick_first,
            pre_ordered,
        )
    }

    /// `[value, o1, o2]` columns for one batch.
    fn vo_batch(vals: &[Option<i64>], o1: &[i64], o2: &[i64]) -> Vec<ArrayRef> {
        assert_eq!(vals.len(), o1.len());
        assert_eq!(vals.len(), o2.len());
        vec![
            Arc::new(Int64Array::from(vals.to_vec())) as ArrayRef,
            Arc::new(Int64Array::from(o1.to_vec())) as ArrayRef,
            Arc::new(Int64Array::from(o2.to_vec())) as ArrayRef,
        ]
    }

    type PreOrderedBatch = (Vec<ArrayRef>, Vec<usize>, Option<BooleanArray>, usize);

    /// Feeds identical batches through the tournament path and the pre-ordered
    /// fast path, then asserts that every emitted state column (value,
    /// ordering columns, and the is_set flags) is identical.
    fn assert_matches_tournament(
        pick_first: bool,
        ignore_nulls: bool,
        descending: bool,
        batches: &[PreOrderedBatch],
    ) -> Result<()> {
        let mut slow = grouped_acc(pick_first, ignore_nulls, false, descending)?;
        let mut fast = grouped_acc(pick_first, ignore_nulls, true, descending)?;
        for (cols, group_indices, filter, total_num_groups) in batches {
            slow.update_batch(cols, group_indices, filter.as_ref(), *total_num_groups)?;
            fast.update_batch(cols, group_indices, filter.as_ref(), *total_num_groups)?;
        }
        let slow_state = slow.state(EmitTo::All)?;
        let fast_state = fast.state(EmitTo::All)?;
        assert_eq!(slow_state.len(), fast_state.len());
        for (col_idx, (s, f)) in slow_state.iter().zip(fast_state.iter()).enumerate() {
            assert_eq!(
                s.to_data(),
                f.to_data(),
                "state column {col_idx} differs between tournament and \
                 pre-ordered fast path"
            );
        }
        Ok(())
    }

    fn int64_values(arr: &ArrayRef) -> Vec<Option<i64>> {
        let arr = arr.as_primitive::<Int64Type>();
        (0..arr.len())
            .map(|i| arr.is_valid(i).then(|| arr.value(i)))
            .collect()
    }

    /// LAST_VALUE, ascending, groups interleaved (PartiallySorted-style shape:
    /// each group's own rows are ordered even though groups mix), duplicate
    /// `o1` broken by `o2`, one group appearing only in the second batch.
    #[test]
    fn pre_ordered_last_value_matches_tournament() -> Result<()> {
        let batches: Vec<PreOrderedBatch> = vec![
            (
                vo_batch(
                    &[Some(10), Some(20), Some(11), Some(12), Some(21)],
                    &[1, 1, 2, 2, 3],
                    &[1, 1, 1, 2, 1],
                ),
                vec![0, 1, 0, 0, 1],
                None,
                2,
            ),
            (
                vo_batch(&[Some(13), Some(30), Some(22)], &[4, 1, 5], &[1, 1, 1]),
                vec![0, 2, 1],
                None,
                3,
            ),
        ];
        assert_matches_tournament(false, false, false, &batches)?;

        // Explicit expected values, so both paths being wrong together is
        // caught too: group 0 last row is (o=4,1)->13, group 1 is (5,1)->22,
        // group 2 only saw (1,1)->30.
        let mut fast = grouped_acc(false, false, true, false)?;
        for (cols, gids, filter, total) in &batches {
            fast.update_batch(cols, gids, filter.as_ref(), *total)?;
        }
        let out = fast.evaluate(EmitTo::All)?;
        assert_eq!(int64_values(&out), vec![Some(13), Some(22), Some(30)]);
        Ok(())
    }

    /// FIRST_VALUE, ascending: the first batch decides every group it saw;
    /// later batches must not overwrite.
    #[test]
    fn pre_ordered_first_value_locks_after_first_qualifying_row() -> Result<()> {
        let batches: Vec<PreOrderedBatch> = vec![
            (
                vo_batch(&[Some(10), Some(20)], &[1, 1], &[1, 2]),
                vec![0, 1],
                None,
                2,
            ),
            (
                vo_batch(&[Some(99), Some(98), Some(30)], &[2, 3, 1], &[1, 1, 1]),
                vec![0, 1, 2],
                None,
                3,
            ),
        ];
        assert_matches_tournament(true, false, false, &batches)?;

        let mut fast = grouped_acc(true, false, true, false)?;
        for (cols, gids, filter, total) in &batches {
            fast.update_batch(cols, gids, filter.as_ref(), *total)?;
        }
        let out = fast.evaluate(EmitTo::All)?;
        assert_eq!(int64_values(&out), vec![Some(10), Some(20), Some(30)]);
        Ok(())
    }

    /// Descending ordering requirement with input laid out descending: the
    /// physically-last row is still the requirement's extreme.
    #[test]
    fn pre_ordered_descending_matches_tournament() -> Result<()> {
        let batches: Vec<PreOrderedBatch> = vec![
            (
                vo_batch(
                    &[Some(1), Some(2), Some(3), Some(4)],
                    &[9, 9, 7, 5],
                    &[5, 3, 1, 1],
                ),
                vec![0, 0, 1, 0],
                None,
                2,
            ),
            (
                vo_batch(&[Some(5), Some(6)], &[4, 2], &[9, 9]),
                vec![0, 1],
                None,
                2,
            ),
        ];
        assert_matches_tournament(false, false, true, &batches)?;
        assert_matches_tournament(true, false, true, &batches)?;
        Ok(())
    }

    /// FILTER interaction: a row with a `null` predicate is excluded; a group
    /// whose rows are all filtered in the last batch keeps its earlier winner;
    /// a group filtered everywhere stays unset (emits null + is_set=false).
    #[test]
    fn pre_ordered_respects_filter() -> Result<()> {
        let batches: Vec<PreOrderedBatch> = vec![
            (
                vo_batch(
                    &[Some(10), Some(11), Some(20), Some(30)],
                    &[1, 2, 1, 1],
                    &[1, 1, 1, 1],
                ),
                vec![0, 0, 1, 2],
                Some(BooleanArray::from(vec![
                    Some(true),
                    Some(true),
                    Some(true),
                    Some(false),
                ])),
                3,
            ),
            (
                vo_batch(&[Some(12), Some(21), Some(31)], &[3, 2, 2], &[1, 1, 1]),
                vec![0, 1, 2],
                Some(BooleanArray::from(vec![Some(false), None, Some(false)])),
                3,
            ),
        ];
        assert_matches_tournament(false, false, false, &batches)?;

        let mut fast = grouped_acc(false, false, true, false)?;
        for (cols, gids, filter, total) in &batches {
            fast.update_batch(cols, gids, filter.as_ref(), *total)?;
        }
        let state = fast.state(EmitTo::All)?;
        // value column: group 0 keeps batch-1 winner 11 (batch 2 filtered),
        // group 1 keeps 20 (null predicate excluded), group 2 never set.
        assert_eq!(int64_values(&state[0]), vec![Some(11), Some(20), None]);
        let is_sets = state.last().unwrap().as_boolean();
        assert_eq!(
            (0..3).map(|i| is_sets.value(i)).collect::<Vec<_>>(),
            vec![true, true, false]
        );
        Ok(())
    }

    /// IGNORE NULLS: null values are skipped, so the winner is the last
    /// non-null row; an all-null group stays unset.
    #[test]
    fn pre_ordered_respects_ignore_nulls() -> Result<()> {
        let batches: Vec<PreOrderedBatch> = vec![
            (
                vo_batch(
                    &[Some(10), None, None, Some(20)],
                    &[1, 2, 1, 1],
                    &[1, 1, 1, 2],
                ),
                vec![0, 0, 1, 1],
                None,
                2,
            ),
            (
                vo_batch(&[None, None], &[3, 2], &[1, 1]),
                vec![0, 2],
                None,
                3,
            ),
        ];
        assert_matches_tournament(false, true, false, &batches)?;
        assert_matches_tournament(true, true, false, &batches)?;

        let mut fast = grouped_acc(false, true, true, false)?;
        for (cols, gids, filter, total) in &batches {
            fast.update_batch(cols, gids, filter.as_ref(), *total)?;
        }
        let state = fast.state(EmitTo::All)?;
        assert_eq!(int64_values(&state[0]), vec![Some(10), Some(20), None]);
        Ok(())
    }

    /// Ties: among rows with equal ordering keys the fast path picks the
    /// physically last row for LAST_VALUE / first row for FIRST_VALUE,
    /// matching the single-group pre-ordered accumulator. (The tournament
    /// path keeps the first-seen row of a tie, so the two paths may pick
    /// different — equally valid — rows; this test pins the fast path's
    /// choice rather than asserting equivalence.)
    #[test]
    fn pre_ordered_tie_picks_positional_extreme() -> Result<()> {
        let cols = vo_batch(&[Some(10), Some(11), Some(12)], &[1, 1, 1], &[1, 1, 1]);
        let mut last = grouped_acc(false, false, true, false)?;
        last.update_batch(&cols, &[0, 0, 0], None, 1)?;
        assert_eq!(int64_values(&last.evaluate(EmitTo::All)?), vec![Some(12)]);

        let mut first = grouped_acc(true, false, true, false)?;
        first.update_batch(&cols, &[0, 0, 0], None, 1)?;
        assert_eq!(int64_values(&first.evaluate(EmitTo::All)?), vec![Some(10)]);
        Ok(())
    }

    /// RESPECT NULLS (the default): a null value can itself be the winner;
    /// the fast path must not treat value-nulls specially.
    #[test]
    fn pre_ordered_respect_nulls_null_can_win() -> Result<()> {
        let batches: Vec<PreOrderedBatch> = vec![
            (
                vo_batch(&[Some(10), None], &[1, 2], &[1, 1]),
                vec![0, 0],
                None,
                1,
            ),
            (
                vo_batch(&[None, Some(7)], &[1, 2], &[1, 1]),
                vec![1, 1],
                None,
                2,
            ),
        ];
        assert_matches_tournament(false, false, false, &batches)?;
        assert_matches_tournament(true, false, false, &batches)?;

        let mut fast = grouped_acc(false, false, true, false)?;
        for (cols, gids, filter, total) in &batches {
            fast.update_batch(cols, gids, filter.as_ref(), *total)?;
        }
        // Group 0's last row is the null; it wins under RESPECT NULLS.
        let out = fast.evaluate(EmitTo::All)?;
        assert_eq!(int64_values(&out), vec![None, Some(7)]);
        Ok(())
    }

    /// Draining with `EmitTo::First(n)` mid-stream shifts group indices; the
    /// fast path must stay in lockstep with the tournament path across the
    /// shift.
    #[test]
    fn pre_ordered_survives_partial_emit() -> Result<()> {
        let mut slow = grouped_acc(false, false, false, false)?;
        let mut fast = grouped_acc(false, false, true, false)?;

        let b1 = vo_batch(&[Some(10), Some(20), Some(30)], &[1, 1, 1], &[1, 1, 1]);
        for acc in [&mut slow, &mut fast] {
            acc.update_batch(&b1, &[0, 1, 2], None, 3)?;
        }

        // Emit the first two groups; group 2 shifts down to index 0.
        let s1 = slow.state(EmitTo::First(2))?;
        let f1 = fast.state(EmitTo::First(2))?;
        for (s, f) in s1.iter().zip(f1.iter()) {
            assert_eq!(s.to_data(), f.to_data());
        }
        assert_eq!(int64_values(&f1[0]), vec![Some(10), Some(20)]);

        // Keep feeding the surviving group (now index 0) plus a new group.
        let b2 = vo_batch(&[Some(31), Some(40)], &[2, 1], &[1, 1]);
        for acc in [&mut slow, &mut fast] {
            acc.update_batch(&b2, &[0, 1], None, 2)?;
        }
        let s2 = slow.state(EmitTo::All)?;
        let f2 = fast.state(EmitTo::All)?;
        for (s, f) in s2.iter().zip(f2.iter()) {
            assert_eq!(s.to_data(), f.to_data());
        }
        assert_eq!(int64_values(&f2[0]), vec![Some(31), Some(40)]);
        Ok(())
    }

    /// End-to-end partial→final: states produced by the fast path carry the
    /// winning ordering values, so a (never pre-ordered) final-stage
    /// accumulator merging two partitions in either arrival order picks the
    /// true global extreme.
    #[test]
    fn pre_ordered_partial_states_merge_correctly() -> Result<()> {
        let make_partition_state = |o1: i64, val: i64| -> Result<Vec<ArrayRef>> {
            let mut partial = grouped_acc(false, false, true, false)?;
            partial.update_batch(&vo_batch(&[Some(val)], &[o1], &[1]), &[0], None, 1)?;
            partial.state(EmitTo::All)
        };
        // Partition A saw the later row (o1=9), partition B the earlier one.
        let a = make_partition_state(9, 900)?;
        let b = make_partition_state(3, 300)?;

        for order in [[&a, &b], [&b, &a]] {
            let mut final_acc = grouped_acc(false, false, false, false)?;
            for state in order {
                final_acc.merge_batch(state, &[0], 1)?;
            }
            let out = final_acc.evaluate(EmitTo::All)?;
            assert_eq!(int64_values(&out), vec![Some(900)]);
        }
        Ok(())
    }

    /// Winner collection and scoreboard reset must scale with the groups a
    /// batch touches, not with `total_num_groups`. Correctness side of that:
    /// sparse high group indices against a large total still resolve.
    #[test]
    fn pre_ordered_sparse_groups_large_total() -> Result<()> {
        let total = 100_000;
        let mut fast = grouped_acc(false, false, true, false)?;
        fast.update_batch(
            &vo_batch(&[Some(10), Some(20)], &[1, 1], &[1, 1]),
            &[7, 42_000],
            None,
            total,
        )?;
        fast.update_batch(
            &vo_batch(&[Some(11), Some(30)], &[2, 1], &[1, 1]),
            &[7, 99_999],
            None,
            total,
        )?;
        let out = fast.evaluate(EmitTo::All)?;
        let vals = int64_values(&out);
        assert_eq!(vals.len(), total);
        assert_eq!(vals[7], Some(11));
        assert_eq!(vals[42_000], Some(20));
        assert_eq!(vals[99_999], Some(30));
        assert_eq!(vals[0], None);
        Ok(())
    }

    /// The aggregation stream calls `merge_batch` on the same accumulators
    /// when replaying spilled state, and the tournament helper leaves its
    /// winners' scoreboard bits set. A later pre-ordered `update_batch` must
    /// not mistake those for "touched in this batch", or a merged group's
    /// newer row is dropped from the winner list.
    #[test]
    fn pre_ordered_update_after_merge_interleave() -> Result<()> {
        let merged_state = {
            let mut partial = grouped_acc(false, false, true, false)?;
            partial.update_batch(&vo_batch(&[Some(100)], &[1], &[1]), &[0], None, 1)?;
            partial.state(EmitTo::All)?
        };

        let mut acc = grouped_acc(false, false, true, false)?;
        acc.merge_batch(&merged_state, &[0], 1)?;
        // A newer row (higher ordering key) for the merged group arrives
        // through the fast path afterwards.
        acc.update_batch(&vo_batch(&[Some(500)], &[5], &[1]), &[0], None, 1)?;
        let out = acc.evaluate(EmitTo::All)?;
        assert_eq!(int64_values(&out), vec![Some(500)]);
        Ok(())
    }
}
