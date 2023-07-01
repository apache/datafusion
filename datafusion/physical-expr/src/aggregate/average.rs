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

//! Defines physical expressions that can evaluated at runtime during query execution

use arrow::array::AsArray;
use log::debug;

use std::any::Any;
use std::convert::TryFrom;
use std::sync::Arc;

use crate::aggregate::row_accumulator::{
    is_row_accumulator_support_dtype, RowAccumulator,
};
use crate::aggregate::sum;
use crate::aggregate::sum::sum_batch;
use crate::aggregate::utils::calculate_result_decimal_for_avg;
use crate::aggregate::utils::down_cast_any_ref;
use crate::expressions::format_state_name;
use crate::{AggregateExpr, GroupsAccumulator, PhysicalExpr};
use arrow::compute;
use arrow::datatypes::{DataType, Decimal128Type, UInt64Type};
use arrow::{
    array::{ArrayRef, UInt64Array},
    datatypes::Field,
};
use arrow_array::{Array, ArrowNativeTypeOp, ArrowNumericType, PrimitiveArray};
use datafusion_common::{downcast_value, ScalarValue};
use datafusion_common::{DataFusionError, Result};
use datafusion_expr::Accumulator;
use datafusion_row::accessor::RowAccessor;

use super::utils::Decimal128Averager;

/// AVG aggregate expression
#[derive(Debug, Clone)]
pub struct Avg {
    name: String,
    expr: Arc<dyn PhysicalExpr>,
    pub sum_data_type: DataType,
    rt_data_type: DataType,
    pub pre_cast_to_sum_type: bool,
}

impl Avg {
    /// Create a new AVG aggregate function
    pub fn new(
        expr: Arc<dyn PhysicalExpr>,
        name: impl Into<String>,
        sum_data_type: DataType,
    ) -> Self {
        Self::new_with_pre_cast(expr, name, sum_data_type.clone(), sum_data_type, false)
    }

    pub fn new_with_pre_cast(
        expr: Arc<dyn PhysicalExpr>,
        name: impl Into<String>,
        sum_data_type: DataType,
        rt_data_type: DataType,
        cast_to_sum_type: bool,
    ) -> Self {
        // the internal sum data type of avg just support FLOAT64 and Decimal data type.
        assert!(matches!(
            sum_data_type,
            DataType::Float64 | DataType::Decimal128(_, _)
        ));
        // the result of avg just support FLOAT64 and Decimal data type.
        assert!(matches!(
            rt_data_type,
            DataType::Float64 | DataType::Decimal128(_, _)
        ));
        Self {
            name: name.into(),
            expr,
            sum_data_type,
            rt_data_type,
            pre_cast_to_sum_type: cast_to_sum_type,
        }
    }
}

impl AggregateExpr for Avg {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn field(&self) -> Result<Field> {
        Ok(Field::new(&self.name, self.rt_data_type.clone(), true))
    }

    fn create_accumulator(&self) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(AvgAccumulator::try_new(
            // avg is f64 or decimal
            &self.sum_data_type,
            &self.rt_data_type,
        )?))
    }

    fn state_fields(&self) -> Result<Vec<Field>> {
        Ok(vec![
            Field::new(
                format_state_name(&self.name, "count"),
                DataType::UInt64,
                true,
            ),
            Field::new(
                format_state_name(&self.name, "sum"),
                self.sum_data_type.clone(),
                true,
            ),
        ])
    }

    fn expressions(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        vec![self.expr.clone()]
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn row_accumulator_supported(&self) -> bool {
        is_row_accumulator_support_dtype(&self.sum_data_type)
    }

    fn create_row_accumulator(
        &self,
        start_index: usize,
    ) -> Result<Box<dyn RowAccumulator>> {
        Ok(Box::new(AvgRowAccumulator::new(
            start_index,
            &self.sum_data_type,
            &self.rt_data_type,
        )))
    }

    fn reverse_expr(&self) -> Option<Arc<dyn AggregateExpr>> {
        Some(Arc::new(self.clone()))
    }

    fn create_sliding_accumulator(&self) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(AvgAccumulator::try_new(
            &self.sum_data_type,
            &self.rt_data_type,
        )?))
    }

    fn create_groups_accumulator(&self) -> Result<Box<dyn GroupsAccumulator>> {
        // instantiate specialized accumulator
        match (&self.sum_data_type, &self.rt_data_type) {
            (
                DataType::Decimal128(_sum_precision, sum_scale),
                DataType::Decimal128(target_precision, target_scale),
            ) => {
                let decimal_averager = Decimal128Averager::try_new(
                    *sum_scale,
                    *target_precision,
                    *target_scale,
                )?;

                let avg_fn =
                    move |sum: i128, count: u64| decimal_averager.avg(sum, count as i128);

                Ok(Box::new(AvgGroupsAccumulator::<Decimal128Type, _>::new(
                    &self.sum_data_type,
                    &self.rt_data_type,
                    avg_fn,
                )))
            }
            _ => Err(DataFusionError::NotImplemented(format!(
                "AvgGroupsAccumulator for ({} --> {})",
                self.sum_data_type, self.rt_data_type,
            ))),
        }
    }
}

impl PartialEq<dyn Any> for Avg {
    fn eq(&self, other: &dyn Any) -> bool {
        down_cast_any_ref(other)
            .downcast_ref::<Self>()
            .map(|x| {
                self.name == x.name
                    && self.sum_data_type == x.sum_data_type
                    && self.rt_data_type == x.rt_data_type
                    && self.expr.eq(&x.expr)
            })
            .unwrap_or(false)
    }
}

/// An accumulator to compute the average
#[derive(Debug)]
pub struct AvgAccumulator {
    // sum is used for null
    sum: ScalarValue,
    sum_data_type: DataType,
    return_data_type: DataType,
    count: u64,
}

impl AvgAccumulator {
    /// Creates a new `AvgAccumulator`
    pub fn try_new(datatype: &DataType, return_data_type: &DataType) -> Result<Self> {
        Ok(Self {
            sum: ScalarValue::try_from(datatype)?,
            sum_data_type: datatype.clone(),
            return_data_type: return_data_type.clone(),
            count: 0,
        })
    }
}

impl Accumulator for AvgAccumulator {
    fn state(&self) -> Result<Vec<ScalarValue>> {
        Ok(vec![ScalarValue::from(self.count), self.sum.clone()])
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let values = &values[0];

        self.count += (values.len() - values.null_count()) as u64;
        self.sum = self
            .sum
            .add(&sum::sum_batch(values, &self.sum_data_type)?)?;
        Ok(())
    }

    fn retract_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let values = &values[0];
        self.count -= (values.len() - values.null_count()) as u64;
        let delta = sum_batch(values, &self.sum.get_datatype())?;
        self.sum = self.sum.sub(&delta)?;
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let counts = downcast_value!(states[0], UInt64Array);
        // counts are summed
        self.count += compute::sum(counts).unwrap_or(0);

        // sums are summed
        self.sum = self
            .sum
            .add(&sum::sum_batch(&states[1], &self.sum_data_type)?)?;
        Ok(())
    }

    fn evaluate(&self) -> Result<ScalarValue> {
        match self.sum {
            ScalarValue::Float64(e) => {
                Ok(ScalarValue::Float64(e.map(|f| f / self.count as f64)))
            }
            ScalarValue::Decimal128(value, _, scale) => {
                match value {
                    None => match &self.return_data_type {
                        DataType::Decimal128(p, s) => {
                            Ok(ScalarValue::Decimal128(None, *p, *s))
                        }
                        other => Err(DataFusionError::Internal(format!(
                            "Error returned data type in AvgAccumulator {other:?}"
                        ))),
                    },
                    Some(value) => {
                        // now the sum_type and return type is not the same, need to convert the sum type to return type
                        calculate_result_decimal_for_avg(
                            value,
                            self.count as i128,
                            scale,
                            &self.return_data_type,
                        )
                    }
                }
            }
            _ => Err(DataFusionError::Internal(
                "Sum should be f64 or decimal128 on average".to_string(),
            )),
        }
    }
    fn supports_retract_batch(&self) -> bool {
        true
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self) - std::mem::size_of_val(&self.sum) + self.sum.size()
    }
}

#[derive(Debug)]
struct AvgRowAccumulator {
    state_index: usize,
    sum_datatype: DataType,
    return_data_type: DataType,
}

impl AvgRowAccumulator {
    pub fn new(
        start_index: usize,
        sum_datatype: &DataType,
        return_data_type: &DataType,
    ) -> Self {
        Self {
            state_index: start_index,
            sum_datatype: sum_datatype.clone(),
            return_data_type: return_data_type.clone(),
        }
    }
}

impl RowAccumulator for AvgRowAccumulator {
    fn update_batch(
        &mut self,
        values: &[ArrayRef],
        accessor: &mut RowAccessor,
    ) -> Result<()> {
        let values = &values[0];
        // count
        let delta = (values.len() - values.null_count()) as u64;
        accessor.add_u64(self.state_index(), delta);

        // sum
        sum::add_to_row(
            self.state_index() + 1,
            accessor,
            &sum::sum_batch(values, &self.sum_datatype)?,
        )
    }

    fn update_scalar_values(
        &mut self,
        values: &[ScalarValue],
        accessor: &mut RowAccessor,
    ) -> Result<()> {
        let value = &values[0];
        sum::update_avg_to_row(self.state_index(), accessor, value)
    }

    fn update_scalar(
        &mut self,
        value: &ScalarValue,
        accessor: &mut RowAccessor,
    ) -> Result<()> {
        sum::update_avg_to_row(self.state_index(), accessor, value)
    }

    fn merge_batch(
        &mut self,
        states: &[ArrayRef],
        accessor: &mut RowAccessor,
    ) -> Result<()> {
        let counts = downcast_value!(states[0], UInt64Array);
        // count
        let delta = compute::sum(counts).unwrap_or(0);
        accessor.add_u64(self.state_index(), delta);

        // sum
        let difference = sum::sum_batch(&states[1], &self.sum_datatype)?;
        sum::add_to_row(self.state_index() + 1, accessor, &difference)
    }

    fn evaluate(&self, accessor: &RowAccessor) -> Result<ScalarValue> {
        match self.sum_datatype {
            DataType::Decimal128(p, s) => {
                match accessor.get_u64_opt(self.state_index()) {
                    None => Ok(ScalarValue::Decimal128(None, p, s)),
                    Some(0) => Ok(ScalarValue::Decimal128(None, p, s)),
                    Some(n) => {
                        // now the sum_type and return type is not the same, need to convert the sum type to return type
                        accessor.get_i128_opt(self.state_index() + 1).map_or_else(
                            || Ok(ScalarValue::Decimal128(None, p, s)),
                            |f| {
                                calculate_result_decimal_for_avg(
                                    f,
                                    n as i128,
                                    s,
                                    &self.return_data_type,
                                )
                            },
                        )
                    }
                }
            }
            DataType::Float64 => Ok(match accessor.get_u64_opt(self.state_index()) {
                None => ScalarValue::Float64(None),
                Some(0) => ScalarValue::Float64(None),
                Some(n) => ScalarValue::Float64(
                    accessor
                        .get_f64_opt(self.state_index() + 1)
                        .map(|f| f / n as f64),
                ),
            }),
            _ => Err(DataFusionError::Internal(
                "Sum should be f64 or decimal128 on average".to_string(),
            )),
        }
    }

    #[inline(always)]
    fn state_index(&self) -> usize {
        self.state_index
    }
}

/// This function is called to update the accumulator state per row,
/// for a `PrimitiveArray<T>` with no nulls. It is the inner loop for
/// many GroupsAccumulators and thus performance critical.
///
/// I couldn't find any way to combine this with
/// accumulate_all_nullable without having to pass in a is_null on
/// every row.
///
/// * `values`: the input arguments to the accumulator
/// * `group_indices`:  To which groups do the rows in `values` belong, group id)
/// * `opt_filter`: if present, only update aggregate state using values[i] if opt_filter[i] is true
///
/// `F`: The function to invoke for a non null input row to update the
/// accumulator state. Called like `value_fn(group_index, value)
fn accumulate_all<T, F>(
    values: &PrimitiveArray<T>,
    group_indicies: &[usize],
    opt_filter: Option<&arrow_array::BooleanArray>,
    mut value_fn: F,
) where
    T: ArrowNumericType + Send,
    F: FnMut(usize, T::Native) + Send,
{
    assert_eq!(
        values.null_count(), 0,
        "Called accumulate_all with nullable array (call accumulate_all_nullable instead)"
    );

    // AAL TODO handle filter values

    let data: &[T::Native] = values.values();
    let iter = group_indicies.iter().zip(data.iter());
    for (&group_index, &new_value) in iter {
        value_fn(group_index, new_value)
    }
}

/// This function is called to update the accumulator state per row,
/// for a `PrimitiveArray<T>` with no nulls. It is the inner loop for
/// many GroupsAccumulators and thus performance critical.
///
/// * `values`: the input arguments to the accumulator
/// * `group_indices`:  To which groups do the rows in `values` belong, group id)
/// * `opt_filter`: if present, only update aggregate state using values[i] if opt_filter[i] is true
///
/// `F`: The function to invoke for an input row to update the
/// accumulator state. Called like `value_fn(group_index, value,
/// is_valid). NOTE the parameter is true when the value is VALID.
fn accumulate_all_nullable<T, F>(
    values: &PrimitiveArray<T>,
    group_indicies: &[usize],
    opt_filter: Option<&arrow_array::BooleanArray>,
    mut value_fn: F,
) where
    T: ArrowNumericType + Send,
    F: FnMut(usize, T::Native, bool) + Send,
{
    // AAL TODO handle filter values
    // TODO combine the null mask from values and opt_filter
    let valids = values
        .nulls()
        .expect("Called accumulate_all_nullable with non-nullable array (call accumulate_all instead)");

    // This is based on (ahem, COPY/PASTA) arrow::compute::aggregate::sum
    let data: &[T::Native] = values.values();

    let group_indices_chunks = group_indicies.chunks_exact(64);
    let data_chunks = data.chunks_exact(64);
    let bit_chunks = valids.inner().bit_chunks();

    let group_indices_remainder = group_indices_chunks.remainder();
    let data_remainder = data_chunks.remainder();

    group_indices_chunks
        .zip(data_chunks)
        .zip(bit_chunks.iter())
        .for_each(|((group_index_chunk, data_chunk), mask)| {
            // index_mask has value 1 << i in the loop
            let mut index_mask = 1;
            group_index_chunk.iter().zip(data_chunk.iter()).for_each(
                |(&group_index, &new_value)| {
                    // valid bit was set, real vale
                    let is_valid = (mask & index_mask) != 0;
                    value_fn(group_index, new_value, is_valid);
                    index_mask <<= 1;
                },
            )
        });

    // handle any remaining bits (after the intial 64)
    let remainder_bits = bit_chunks.remainder_bits();
    group_indices_remainder
        .iter()
        .zip(data_remainder.iter())
        .enumerate()
        .for_each(|(i, (&group_index, &new_value))| {
            let is_valid = remainder_bits & (1 << i) != 0;
            value_fn(group_index, new_value, is_valid)
        });
}

/// An accumulator to compute the average of PrimitiveArray<T>.
/// Stores values as native types, and does overflow checking
///
/// F: Function that calcuates the average value from a sum of
/// T::Native and a total count
#[derive(Debug)]
struct AvgGroupsAccumulator<T, F>
where
    T: ArrowNumericType + Send,
    F: Fn(T::Native, u64) -> Result<T::Native> + Send,
{
    /// The type of the internal sum
    sum_data_type: DataType,

    /// The type of the returned sum
    return_data_type: DataType,

    /// Count per group (use u64 to make UInt64Array)
    counts: Vec<u64>,

    /// Sums per group, stored as the native type
    sums: Vec<T::Native>,

    /// Function that computes the average (value / count)
    avg_fn: F,
}

impl<T, F> AvgGroupsAccumulator<T, F>
where
    T: ArrowNumericType + Send,
    F: Fn(T::Native, u64) -> Result<T::Native> + Send,
{
    pub fn new(sum_data_type: &DataType, return_data_type: &DataType, avg_fn: F) -> Self {
        debug!(
            "AvgGroupsAccumulator ({}, sum type: {sum_data_type:?}) --> {return_data_type:?}",
            std::any::type_name::<T>()
        );
        Self {
            return_data_type: return_data_type.clone(),
            sum_data_type: sum_data_type.clone(),
            counts: vec![],
            sums: vec![],
            avg_fn,
        }
    }

    /// Adds one to each group's counter
    fn increment_counts(
        &mut self,
        values: &PrimitiveArray<T>,
        group_indicies: &[usize],
        opt_filter: Option<&arrow_array::BooleanArray>,
        total_num_groups: usize,
    ) {
        self.counts.resize(total_num_groups, 0);

        if values.null_count() == 0 {
            accumulate_all(
                values,
                group_indicies,
                opt_filter,
                |group_index, _new_value| {
                    self.counts[group_index] += 1;
                },
            )
        } else {
            accumulate_all_nullable(
                values,
                group_indicies,
                opt_filter,
                |group_index, _new_value, is_valid| {
                    if is_valid {
                        self.counts[group_index] += 1;
                    }
                },
            )
        }
    }

    /// Adds the counts with the partial counts
    fn update_counts_with_partial_counts(
        &mut self,
        partial_counts: &UInt64Array,
        group_indicies: &[usize],
        opt_filter: Option<&arrow_array::BooleanArray>,
        total_num_groups: usize,
    ) {
        self.counts.resize(total_num_groups, 0);

        if partial_counts.null_count() == 0 {
            accumulate_all(
                partial_counts,
                group_indicies,
                opt_filter,
                |group_index, partial_count| {
                    self.counts[group_index] += partial_count;
                },
            )
        } else {
            accumulate_all_nullable(
                partial_counts,
                group_indicies,
                opt_filter,
                |group_index, partial_count, is_valid| {
                    if is_valid {
                        self.counts[group_index] += partial_count;
                    }
                },
            )
        }
    }

    /// Adds the values in `values` to self.sums
    fn update_sums(
        &mut self,
        values: &PrimitiveArray<T>,
        group_indicies: &[usize],
        opt_filter: Option<&arrow_array::BooleanArray>,
        total_num_groups: usize,
    ) {
        self.sums
            .resize_with(total_num_groups, || T::default_value());

        if values.null_count() == 0 {
            accumulate_all(
                values,
                group_indicies,
                opt_filter,
                |group_index, new_value| {
                    let sum = &mut self.sums[group_index];
                    *sum = sum.add_wrapping(new_value);
                },
            )
        } else {
            accumulate_all_nullable(
                values,
                group_indicies,
                opt_filter,
                |group_index, new_value, is_valid| {
                    if is_valid {
                        let sum = &mut self.sums[group_index];
                        *sum = sum.add_wrapping(new_value);
                    }
                },
            )
        }
    }
}

impl<T, F> GroupsAccumulator for AvgGroupsAccumulator<T, F>
where
    T: ArrowNumericType + Send,
    F: Fn(T::Native, u64) -> Result<T::Native> + Send,
{
    fn update_batch(
        &mut self,
        values: &[ArrayRef],
        group_indicies: &[usize],
        opt_filter: Option<&arrow_array::BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        assert_eq!(values.len(), 1, "single argument to update_batch");
        let values = values.get(0).unwrap().as_primitive::<T>();

        self.increment_counts(values, group_indicies, opt_filter, total_num_groups);
        self.update_sums(values, group_indicies, opt_filter, total_num_groups);

        Ok(())
    }

    fn merge_batch(
        &mut self,
        values: &[ArrayRef],
        group_indicies: &[usize],
        opt_filter: Option<&arrow_array::BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        assert_eq!(values.len(), 2, "two arguments to merge_batch");
        // first batch is counts, second is partial sums
        let partial_counts = values.get(0).unwrap().as_primitive::<UInt64Type>();
        let partial_sums = values.get(1).unwrap().as_primitive::<T>();
        self.update_counts_with_partial_counts(
            partial_counts,
            group_indicies,
            opt_filter,
            total_num_groups,
        );
        self.update_sums(partial_sums, group_indicies, opt_filter, total_num_groups);

        Ok(())
    }

    fn evaluate(&mut self) -> Result<ArrayRef> {
        let counts = std::mem::take(&mut self.counts);
        let sums = std::mem::take(&mut self.sums);

        let averages: Vec<T::Native> = sums
            .into_iter()
            .zip(counts.into_iter())
            .map(|(sum, count)| (self.avg_fn)(sum, count))
            .collect::<Result<Vec<_>>>()?;

        // TODO figure out how to do this without the iter / copy
        let array = PrimitiveArray::<T>::from_iter_values(averages);

        // fix up decimal precision and scale for decimals
        let array = adjust_output_array(&self.return_data_type, Arc::new(array))?;

        Ok(array)
    }

    // return arrays for sums and counts
    fn state(&mut self) -> Result<Vec<ArrayRef>> {
        let counts = std::mem::take(&mut self.counts);
        // create array from vec is zero copy
        let counts = UInt64Array::from(counts);

        let sums = std::mem::take(&mut self.sums);
        // create array from vec is zero copy
        // TODO figure out how to do this without the iter / copy
        let sums: PrimitiveArray<T> = PrimitiveArray::from_iter_values(sums);

        // fix up decimal precision and scale for decimals
        let sums = adjust_output_array(&self.sum_data_type, Arc::new(sums))?;

        Ok(vec![
            Arc::new(counts) as ArrayRef,
            Arc::new(sums) as ArrayRef,
        ])
    }

    fn size(&self) -> usize {
        self.counts.capacity() * std::mem::size_of::<usize>()
    }
}

/// Adjust array type metadata if needed
///
/// Decimal128Arrays are are are created from Vec<NativeType> with default
/// precision and scale. This function adjusts them down.
fn adjust_output_array(sum_data_type: &DataType, array: ArrayRef) -> Result<ArrayRef> {
    let array = match sum_data_type {
        DataType::Decimal128(p, s) => Arc::new(
            array
                .as_primitive::<Decimal128Type>()
                .clone()
                .with_precision_and_scale(*p, *s)?,
        ),
        // no adjustment needed for other arrays
        _ => array,
    };
    Ok(array)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expressions::col;
    use crate::expressions::tests::aggregate;
    use crate::generic_test_op;
    use arrow::record_batch::RecordBatch;
    use arrow::{array::*, datatypes::*};
    use datafusion_common::Result;

    #[test]
    fn avg_decimal() -> Result<()> {
        // test agg
        let array: ArrayRef = Arc::new(
            (1..7)
                .map(Some)
                .collect::<Decimal128Array>()
                .with_precision_and_scale(10, 0)?,
        );

        generic_test_op!(
            array,
            DataType::Decimal128(10, 0),
            Avg,
            ScalarValue::Decimal128(Some(35000), 14, 4)
        )
    }

    #[test]
    fn avg_decimal_with_nulls() -> Result<()> {
        let array: ArrayRef = Arc::new(
            (1..6)
                .map(|i| if i == 2 { None } else { Some(i) })
                .collect::<Decimal128Array>()
                .with_precision_and_scale(10, 0)?,
        );
        generic_test_op!(
            array,
            DataType::Decimal128(10, 0),
            Avg,
            ScalarValue::Decimal128(Some(32500), 14, 4)
        )
    }

    #[test]
    fn avg_decimal_all_nulls() -> Result<()> {
        // test agg
        let array: ArrayRef = Arc::new(
            std::iter::repeat::<Option<i128>>(None)
                .take(6)
                .collect::<Decimal128Array>()
                .with_precision_and_scale(10, 0)?,
        );
        generic_test_op!(
            array,
            DataType::Decimal128(10, 0),
            Avg,
            ScalarValue::Decimal128(None, 14, 4)
        )
    }

    #[test]
    fn avg_i32() -> Result<()> {
        let a: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5]));
        generic_test_op!(a, DataType::Int32, Avg, ScalarValue::from(3_f64))
    }

    #[test]
    fn avg_i32_with_nulls() -> Result<()> {
        let a: ArrayRef = Arc::new(Int32Array::from(vec![
            Some(1),
            None,
            Some(3),
            Some(4),
            Some(5),
        ]));
        generic_test_op!(a, DataType::Int32, Avg, ScalarValue::from(3.25f64))
    }

    #[test]
    fn avg_i32_all_nulls() -> Result<()> {
        let a: ArrayRef = Arc::new(Int32Array::from(vec![None, None]));
        generic_test_op!(a, DataType::Int32, Avg, ScalarValue::Float64(None))
    }

    #[test]
    fn avg_u32() -> Result<()> {
        let a: ArrayRef =
            Arc::new(UInt32Array::from(vec![1_u32, 2_u32, 3_u32, 4_u32, 5_u32]));
        generic_test_op!(a, DataType::UInt32, Avg, ScalarValue::from(3.0f64))
    }

    #[test]
    fn avg_f32() -> Result<()> {
        let a: ArrayRef =
            Arc::new(Float32Array::from(vec![1_f32, 2_f32, 3_f32, 4_f32, 5_f32]));
        generic_test_op!(a, DataType::Float32, Avg, ScalarValue::from(3_f64))
    }

    #[test]
    fn avg_f64() -> Result<()> {
        let a: ArrayRef =
            Arc::new(Float64Array::from(vec![1_f64, 2_f64, 3_f64, 4_f64, 5_f64]));
        generic_test_op!(a, DataType::Float64, Avg, ScalarValue::from(3_f64))
    }
}
