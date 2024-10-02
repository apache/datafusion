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

use ahash::RandomState;
use datafusion_common::stats::Precision;
use datafusion_functions_aggregate_common::aggregate::count_distinct::BytesViewDistinctCountAccumulator;
use datafusion_physical_expr::expressions;
use std::collections::HashSet;
use std::ops::BitAnd;
use std::{fmt::Debug, sync::Arc};

use arrow::{
    array::{ArrayRef, AsArray},
    compute,
    datatypes::{
        DataType, Date32Type, Date64Type, Decimal128Type, Decimal256Type, Field,
        Float16Type, Float32Type, Float64Type, Int16Type, Int32Type, Int64Type, Int8Type,
        Time32MillisecondType, Time32SecondType, Time64MicrosecondType,
        Time64NanosecondType, TimeUnit, TimestampMicrosecondType,
        TimestampMillisecondType, TimestampNanosecondType, TimestampSecondType,
        UInt16Type, UInt32Type, UInt64Type, UInt8Type,
    },
};

use arrow::{
    array::{Array, BooleanArray, Int64Array, PrimitiveArray},
    buffer::BooleanBuffer,
};
use datafusion_common::{
    downcast_value, internal_err, not_impl_err, DataFusionError, Result, ScalarValue,
};
use datafusion_expr::function::StateFieldsArgs;
use datafusion_expr::{
    function::AccumulatorArgs, utils::format_state_name, Accumulator, AggregateUDFImpl,
    EmitTo, GroupsAccumulator, Signature, Volatility,
};
use datafusion_expr::{Expr, ReversedUDAF, StatisticsArgs, TypeSignature};
use datafusion_functions_aggregate_common::aggregate::count_distinct::{
    BytesDistinctCountAccumulator, FloatDistinctCountAccumulator,
    PrimitiveDistinctCountAccumulator,
};
use datafusion_functions_aggregate_common::aggregate::groups_accumulator::accumulate::accumulate_indices;
use datafusion_physical_expr_common::binary_map::OutputType;

use datafusion_common::utils::expr::COUNT_STAR_EXPANSION;
make_udaf_expr_and_func!(
    Count,
    count,
    expr,
    "Count the number of non-null values in the column",
    count_udaf
);

pub fn count_distinct(expr: Expr) -> Expr {
    Expr::AggregateFunction(datafusion_expr::expr::AggregateFunction::new_udf(
        count_udaf(),
        vec![expr],
        true,
        None,
        None,
        None,
    ))
}

pub struct Count {
    signature: Signature,
}

impl Debug for Count {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.debug_struct("Count")
            .field("name", &self.name())
            .field("signature", &self.signature)
            .finish()
    }
}

impl Default for Count {
    fn default() -> Self {
        Self::new()
    }
}

impl Count {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                // TypeSignature::Any(0) is required to handle `Count()` with no args
                vec![TypeSignature::VariadicAny, TypeSignature::Any(0)],
                Volatility::Immutable,
            ),
        }
    }
}

impl AggregateUDFImpl for Count {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "count"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int64)
    }

    fn is_nullable(&self) -> bool {
        false
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<Field>> {
        if args.is_distinct {
            Ok(vec![Field::new_list(
                format_state_name(args.name, "count distinct"),
                // See COMMENTS.md to understand why nullable is set to true
                Field::new("item", args.input_types[0].clone(), true),
                false,
            )])
        } else {
            Ok(vec![Field::new(
                format_state_name(args.name, "count"),
                DataType::Int64,
                false,
            )])
        }
    }

    fn accumulator(&self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        if !acc_args.is_distinct {
            return Ok(Box::new(CountAccumulator::new()));
        }

        if acc_args.exprs.len() > 1 {
            return not_impl_err!("COUNT DISTINCT with multiple arguments");
        }

        let data_type = &acc_args.exprs[0].data_type(acc_args.schema)?;
        Ok(match data_type {
            // try and use a specialized accumulator if possible, otherwise fall back to generic accumulator
            DataType::Int8 => Box::new(
                PrimitiveDistinctCountAccumulator::<Int8Type>::new(data_type),
            ),
            DataType::Int16 => Box::new(
                PrimitiveDistinctCountAccumulator::<Int16Type>::new(data_type),
            ),
            DataType::Int32 => Box::new(
                PrimitiveDistinctCountAccumulator::<Int32Type>::new(data_type),
            ),
            DataType::Int64 => Box::new(
                PrimitiveDistinctCountAccumulator::<Int64Type>::new(data_type),
            ),
            DataType::UInt8 => Box::new(
                PrimitiveDistinctCountAccumulator::<UInt8Type>::new(data_type),
            ),
            DataType::UInt16 => Box::new(
                PrimitiveDistinctCountAccumulator::<UInt16Type>::new(data_type),
            ),
            DataType::UInt32 => Box::new(
                PrimitiveDistinctCountAccumulator::<UInt32Type>::new(data_type),
            ),
            DataType::UInt64 => Box::new(
                PrimitiveDistinctCountAccumulator::<UInt64Type>::new(data_type),
            ),
            DataType::Decimal128(_, _) => Box::new(PrimitiveDistinctCountAccumulator::<
                Decimal128Type,
            >::new(data_type)),
            DataType::Decimal256(_, _) => Box::new(PrimitiveDistinctCountAccumulator::<
                Decimal256Type,
            >::new(data_type)),

            DataType::Date32 => Box::new(
                PrimitiveDistinctCountAccumulator::<Date32Type>::new(data_type),
            ),
            DataType::Date64 => Box::new(
                PrimitiveDistinctCountAccumulator::<Date64Type>::new(data_type),
            ),
            DataType::Time32(TimeUnit::Millisecond) => Box::new(
                PrimitiveDistinctCountAccumulator::<Time32MillisecondType>::new(
                    data_type,
                ),
            ),
            DataType::Time32(TimeUnit::Second) => Box::new(
                PrimitiveDistinctCountAccumulator::<Time32SecondType>::new(data_type),
            ),
            DataType::Time64(TimeUnit::Microsecond) => Box::new(
                PrimitiveDistinctCountAccumulator::<Time64MicrosecondType>::new(
                    data_type,
                ),
            ),
            DataType::Time64(TimeUnit::Nanosecond) => Box::new(
                PrimitiveDistinctCountAccumulator::<Time64NanosecondType>::new(data_type),
            ),
            DataType::Timestamp(TimeUnit::Microsecond, _) => Box::new(
                PrimitiveDistinctCountAccumulator::<TimestampMicrosecondType>::new(
                    data_type,
                ),
            ),
            DataType::Timestamp(TimeUnit::Millisecond, _) => Box::new(
                PrimitiveDistinctCountAccumulator::<TimestampMillisecondType>::new(
                    data_type,
                ),
            ),
            DataType::Timestamp(TimeUnit::Nanosecond, _) => Box::new(
                PrimitiveDistinctCountAccumulator::<TimestampNanosecondType>::new(
                    data_type,
                ),
            ),
            DataType::Timestamp(TimeUnit::Second, _) => Box::new(
                PrimitiveDistinctCountAccumulator::<TimestampSecondType>::new(data_type),
            ),

            DataType::Float16 => {
                Box::new(FloatDistinctCountAccumulator::<Float16Type>::new())
            }
            DataType::Float32 => {
                Box::new(FloatDistinctCountAccumulator::<Float32Type>::new())
            }
            DataType::Float64 => {
                Box::new(FloatDistinctCountAccumulator::<Float64Type>::new())
            }

            DataType::Utf8 => {
                Box::new(BytesDistinctCountAccumulator::<i32>::new(OutputType::Utf8))
            }
            DataType::Utf8View => {
                Box::new(BytesViewDistinctCountAccumulator::new(OutputType::Utf8View))
            }
            DataType::LargeUtf8 => {
                Box::new(BytesDistinctCountAccumulator::<i64>::new(OutputType::Utf8))
            }
            DataType::Binary => Box::new(BytesDistinctCountAccumulator::<i32>::new(
                OutputType::Binary,
            )),
            DataType::BinaryView => Box::new(BytesViewDistinctCountAccumulator::new(
                OutputType::BinaryView,
            )),
            DataType::LargeBinary => Box::new(BytesDistinctCountAccumulator::<i64>::new(
                OutputType::Binary,
            )),

            // Use the generic accumulator based on `ScalarValue` for all other types
            _ => Box::new(DistinctCountAccumulator {
                values: HashSet::default(),
                state_data_type: data_type.clone(),
            }),
        })
    }

    fn aliases(&self) -> &[String] {
        &[]
    }

    fn groups_accumulator_supported(&self, args: AccumulatorArgs) -> bool {
        // groups accumulator only supports `COUNT(c1)`, not
        // `COUNT(c1, c2)`, etc
        if args.is_distinct {
            return false;
        }
        args.exprs.len() == 1
    }

    fn create_groups_accumulator(
        &self,
        _args: AccumulatorArgs,
    ) -> Result<Box<dyn GroupsAccumulator>> {
        // instantiate specialized accumulator
        Ok(Box::new(CountGroupsAccumulator::new()))
    }

    fn reverse_expr(&self) -> ReversedUDAF {
        ReversedUDAF::Identical
    }

    fn default_value(&self, _data_type: &DataType) -> Result<ScalarValue> {
        Ok(ScalarValue::Int64(Some(0)))
    }

    fn value_from_stats(&self, statistics_args: &StatisticsArgs) -> Option<ScalarValue> {
        if statistics_args.is_distinct {
            return None;
        }
        if let Precision::Exact(num_rows) = statistics_args.statistics.num_rows {
            if statistics_args.exprs.len() == 1 {
                // TODO optimize with exprs other than Column
                if let Some(col_expr) = statistics_args.exprs[0]
                    .as_any()
                    .downcast_ref::<expressions::Column>()
                {
                    let current_val = &statistics_args.statistics.column_statistics
                        [col_expr.index()]
                    .null_count;
                    if let &Precision::Exact(val) = current_val {
                        return Some(ScalarValue::Int64(Some((num_rows - val) as i64)));
                    }
                } else if let Some(lit_expr) = statistics_args.exprs[0]
                    .as_any()
                    .downcast_ref::<expressions::Literal>()
                {
                    if lit_expr.value() == &COUNT_STAR_EXPANSION {
                        return Some(ScalarValue::Int64(Some(num_rows as i64)));
                    }
                }
            }
        }
        None
    }
}

#[derive(Debug)]
struct CountAccumulator {
    count: i64,
}

impl CountAccumulator {
    /// new count accumulator
    pub fn new() -> Self {
        Self { count: 0 }
    }
}

impl Accumulator for CountAccumulator {
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![ScalarValue::Int64(Some(self.count))])
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let array = &values[0];
        self.count += (array.len() - null_count_for_multiple_cols(values)) as i64;
        Ok(())
    }

    fn retract_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let array = &values[0];
        self.count -= (array.len() - null_count_for_multiple_cols(values)) as i64;
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let counts = downcast_value!(states[0], Int64Array);
        let delta = &arrow::compute::sum(counts);
        if let Some(d) = delta {
            self.count += *d;
        }
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        Ok(ScalarValue::Int64(Some(self.count)))
    }

    fn supports_retract_batch(&self) -> bool {
        true
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self)
    }
}

/// An accumulator to compute the counts of [`PrimitiveArray<T>`].
/// Stores values as native types, and does overflow checking
///
/// Unlike most other accumulators, COUNT never produces NULLs. If no
/// non-null values are seen in any group the output is 0. Thus, this
/// accumulator has no additional null or seen filter tracking.
#[derive(Debug)]
struct CountGroupsAccumulator {
    /// Count per group.
    ///
    /// Note this is an i64 and not a u64 (or usize) because the
    /// output type of count is `DataType::Int64`. Thus by using `i64`
    /// for the counts, the output [`Int64Array`] can be created
    /// without copy.
    counts: Vec<i64>,
}

impl CountGroupsAccumulator {
    pub fn new() -> Self {
        Self { counts: vec![] }
    }
}

impl GroupsAccumulator for CountGroupsAccumulator {
    fn update_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        assert_eq!(values.len(), 1, "single argument to update_batch");
        let values = &values[0];

        // Add one to each group's counter for each non null, non
        // filtered value
        self.counts.resize(total_num_groups, 0);
        accumulate_indices(
            group_indices,
            values.logical_nulls().as_ref(),
            opt_filter,
            |group_index| {
                self.counts[group_index] += 1;
            },
        );

        Ok(())
    }

    fn merge_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        assert_eq!(values.len(), 1, "one argument to merge_batch");
        // first batch is counts, second is partial sums
        let partial_counts = values[0].as_primitive::<Int64Type>();

        // intermediate counts are always created as non null
        assert_eq!(partial_counts.null_count(), 0);
        let partial_counts = partial_counts.values();

        // Adds the counts with the partial counts
        self.counts.resize(total_num_groups, 0);
        match opt_filter {
            Some(filter) => filter
                .iter()
                .zip(group_indices.iter())
                .zip(partial_counts.iter())
                .for_each(|((filter_value, &group_index), partial_count)| {
                    if let Some(true) = filter_value {
                        self.counts[group_index] += partial_count;
                    }
                }),
            None => group_indices.iter().zip(partial_counts.iter()).for_each(
                |(&group_index, partial_count)| {
                    self.counts[group_index] += partial_count;
                },
            ),
        }

        Ok(())
    }

    fn evaluate(&mut self, emit_to: EmitTo) -> Result<ArrayRef> {
        let counts = emit_to.take_needed(&mut self.counts);

        // Count is always non null (null inputs just don't contribute to the overall values)
        let nulls = None;
        let array = PrimitiveArray::<Int64Type>::new(counts.into(), nulls);

        Ok(Arc::new(array))
    }

    // return arrays for counts
    fn state(&mut self, emit_to: EmitTo) -> Result<Vec<ArrayRef>> {
        let counts = emit_to.take_needed(&mut self.counts);
        let counts: PrimitiveArray<Int64Type> = Int64Array::from(counts); // zero copy, no nulls
        Ok(vec![Arc::new(counts) as ArrayRef])
    }

    /// Converts an input batch directly to a state batch
    ///
    /// The state of `COUNT` is always a single Int64Array:
    /// * `1` (for non-null, non filtered values)
    /// * `0` (for null values)
    fn convert_to_state(
        &self,
        values: &[ArrayRef],
        opt_filter: Option<&BooleanArray>,
    ) -> Result<Vec<ArrayRef>> {
        let values = &values[0];

        let state_array = match (values.logical_nulls(), opt_filter) {
            (None, None) => {
                // In case there is no nulls in input and no filter, returning array of 1
                Arc::new(Int64Array::from_value(1, values.len()))
            }
            (Some(nulls), None) => {
                // If there are any nulls in input values -- casting `nulls` (true for values, false for nulls)
                // of input array to Int64
                let nulls = BooleanArray::new(nulls.into_inner(), None);
                compute::cast(&nulls, &DataType::Int64)?
            }
            (None, Some(filter)) => {
                // If there is only filter
                // - applying filter null mask to filter values by bitand filter values and nulls buffers
                //   (using buffers guarantees absence of nulls in result)
                // - casting result of bitand to Int64 array
                let (filter_values, filter_nulls) = filter.clone().into_parts();

                let state_buf = match filter_nulls {
                    Some(filter_nulls) => &filter_values & filter_nulls.inner(),
                    None => filter_values,
                };

                let boolean_state = BooleanArray::new(state_buf, None);
                compute::cast(&boolean_state, &DataType::Int64)?
            }
            (Some(nulls), Some(filter)) => {
                // For both input nulls and filter
                // - applying filter null mask to filter values by bitand filter values and nulls buffers
                //   (using buffers guarantees absence of nulls in result)
                // - applying values null mask to filter buffer by another bitand on filter result and
                //   nulls from input values
                // - casting result to Int64 array
                let (filter_values, filter_nulls) = filter.clone().into_parts();

                let filter_buf = match filter_nulls {
                    Some(filter_nulls) => &filter_values & filter_nulls.inner(),
                    None => filter_values,
                };
                let state_buf = &filter_buf & nulls.inner();

                let boolean_state = BooleanArray::new(state_buf, None);
                compute::cast(&boolean_state, &DataType::Int64)?
            }
        };

        Ok(vec![state_array])
    }

    fn supports_convert_to_state(&self) -> bool {
        true
    }

    fn size(&self) -> usize {
        self.counts.capacity() * std::mem::size_of::<usize>()
    }
}

/// count null values for multiple columns
/// for each row if one column value is null, then null_count + 1
fn null_count_for_multiple_cols(values: &[ArrayRef]) -> usize {
    if values.len() > 1 {
        let result_bool_buf: Option<BooleanBuffer> = values
            .iter()
            .map(|a| a.logical_nulls())
            .fold(None, |acc, b| match (acc, b) {
                (Some(acc), Some(b)) => Some(acc.bitand(b.inner())),
                (Some(acc), None) => Some(acc),
                (None, Some(b)) => Some(b.into_inner()),
                _ => None,
            });
        result_bool_buf.map_or(0, |b| values[0].len() - b.count_set_bits())
    } else {
        values[0]
            .logical_nulls()
            .map_or(0, |nulls| nulls.null_count())
    }
}

/// General purpose distinct accumulator that works for any DataType by using
/// [`ScalarValue`].
///
/// It stores intermediate results as a `ListArray`
///
/// Note that many types have specialized accumulators that are (much)
/// more efficient such as [`PrimitiveDistinctCountAccumulator`] and
/// [`BytesDistinctCountAccumulator`]
#[derive(Debug)]
struct DistinctCountAccumulator {
    values: HashSet<ScalarValue, RandomState>,
    state_data_type: DataType,
}

impl DistinctCountAccumulator {
    // calculating the size for fixed length values, taking first batch size *
    // number of batches This method is faster than .full_size(), however it is
    // not suitable for variable length values like strings or complex types
    fn fixed_size(&self) -> usize {
        std::mem::size_of_val(self)
            + (std::mem::size_of::<ScalarValue>() * self.values.capacity())
            + self
                .values
                .iter()
                .next()
                .map(|vals| ScalarValue::size(vals) - std::mem::size_of_val(vals))
                .unwrap_or(0)
            + std::mem::size_of::<DataType>()
    }

    // calculates the size as accurately as possible. Note that calling this
    // method is expensive
    fn full_size(&self) -> usize {
        std::mem::size_of_val(self)
            + (std::mem::size_of::<ScalarValue>() * self.values.capacity())
            + self
                .values
                .iter()
                .map(|vals| ScalarValue::size(vals) - std::mem::size_of_val(vals))
                .sum::<usize>()
            + std::mem::size_of::<DataType>()
    }
}

impl Accumulator for DistinctCountAccumulator {
    /// Returns the distinct values seen so far as (one element) ListArray.
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        let scalars = self.values.iter().cloned().collect::<Vec<_>>();
        let arr =
            ScalarValue::new_list_nullable(scalars.as_slice(), &self.state_data_type);
        Ok(vec![ScalarValue::List(arr)])
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if values.is_empty() {
            return Ok(());
        }

        let arr = &values[0];
        if arr.data_type() == &DataType::Null {
            return Ok(());
        }

        (0..arr.len()).try_for_each(|index| {
            if !arr.is_null(index) {
                let scalar = ScalarValue::try_from_array(arr, index)?;
                self.values.insert(scalar);
            }
            Ok(())
        })
    }

    /// Merges multiple sets of distinct values into the current set.
    ///
    /// The input to this function is a `ListArray` with **multiple** rows,
    /// where each row contains the values from a partial aggregate's phase (e.g.
    /// the result of calling `Self::state` on multiple accumulators).
    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        if states.is_empty() {
            return Ok(());
        }
        assert_eq!(states.len(), 1, "array_agg states must be singleton!");
        let array = &states[0];
        let list_array = array.as_list::<i32>();
        for inner_array in list_array.iter() {
            let Some(inner_array) = inner_array else {
                return internal_err!(
                    "Intermediate results of COUNT DISTINCT should always be non null"
                );
            };
            self.update_batch(&[inner_array])?;
        }
        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        Ok(ScalarValue::Int64(Some(self.values.len() as i64)))
    }

    fn size(&self) -> usize {
        match &self.state_data_type {
            DataType::Boolean | DataType::Null => self.fixed_size(),
            d if d.is_primitive() => self.fixed_size(),
            _ => self.full_size(),
        }
    }
}
