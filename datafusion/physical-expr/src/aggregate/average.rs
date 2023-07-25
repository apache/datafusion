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

use arrow::array::{AsArray, PrimitiveBuilder};
use log::debug;

use std::any::Any;
use std::convert::TryFrom;
use std::sync::Arc;

use crate::aggregate::groups_accumulator::accumulate::NullState;
use crate::aggregate::sum;
use crate::aggregate::sum::sum_batch;
use crate::aggregate::utils::calculate_result_decimal_for_avg;
use crate::aggregate::utils::down_cast_any_ref;
use crate::expressions::format_state_name;
use crate::{AggregateExpr, GroupsAccumulator, PhysicalExpr};
use arrow::compute;
use arrow::datatypes::{DataType, Decimal128Type, Float64Type, UInt64Type};
use arrow::{
    array::{ArrayRef, UInt64Array},
    datatypes::Field,
};
use arrow_array::{
    Array, ArrowNativeTypeOp, ArrowNumericType, ArrowPrimitiveType, PrimitiveArray,
};
use datafusion_common::{downcast_value, ScalarValue};
use datafusion_common::{DataFusionError, Result};
use datafusion_expr::Accumulator;

use super::groups_accumulator::EmitTo;
use super::utils::{adjust_output_array, Decimal128Averager};

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
            DataType::Float64 | DataType::Decimal128(_, _) | DataType::Decimal256(_, _)
        ));
        // the result of avg just support FLOAT64 and Decimal data type.
        assert!(matches!(
            rt_data_type,
            DataType::Float64 | DataType::Decimal128(_, _) | DataType::Decimal256(_, _)
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

    fn reverse_expr(&self) -> Option<Arc<dyn AggregateExpr>> {
        Some(Arc::new(self.clone()))
    }

    fn create_sliding_accumulator(&self) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(AvgAccumulator::try_new(
            &self.sum_data_type,
            &self.rt_data_type,
        )?))
    }

    fn groups_accumulator_supported(&self) -> bool {
        use DataType::*;

        matches!(&self.rt_data_type, Float64 | Decimal128(_, _))
    }

    fn create_groups_accumulator(&self) -> Result<Box<dyn GroupsAccumulator>> {
        use DataType::*;
        // instantiate specialized accumulator based for the type
        match (&self.sum_data_type, &self.rt_data_type) {
            (Float64, Float64) => {
                Ok(Box::new(AvgGroupsAccumulator::<Float64Type, _>::new(
                    &self.sum_data_type,
                    &self.rt_data_type,
                    |sum: f64, count: u64| Ok(sum / count as f64),
                )))
            }
            (
                Decimal128(_sum_precision, sum_scale),
                Decimal128(target_precision, target_scale),
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

/// An accumulator to compute the average of `[PrimitiveArray<T>]`.
/// Stores values as native types, and does overflow checking
///
/// F: Function that calculates the average value from a sum of
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

    /// Track nulls in the input / filters
    null_state: NullState,

    /// Function that computes the final average (value / count)
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
            null_state: NullState::new(),
            avg_fn,
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
        group_indices: &[usize],
        opt_filter: Option<&arrow_array::BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        assert_eq!(values.len(), 1, "single argument to update_batch");
        let values = values[0].as_primitive::<T>();

        // increment counts, update sums
        self.counts.resize(total_num_groups, 0);
        self.sums.resize(total_num_groups, T::default_value());
        self.null_state.accumulate(
            group_indices,
            values,
            opt_filter,
            total_num_groups,
            |group_index, new_value| {
                let sum = &mut self.sums[group_index];
                *sum = sum.add_wrapping(new_value);

                self.counts[group_index] += 1;
            },
        );

        Ok(())
    }

    fn merge_batch(
        &mut self,
        values: &[ArrayRef],
        group_indices: &[usize],
        opt_filter: Option<&arrow_array::BooleanArray>,
        total_num_groups: usize,
    ) -> Result<()> {
        assert_eq!(values.len(), 2, "two arguments to merge_batch");
        // first batch is counts, second is partial sums
        let partial_counts = values[0].as_primitive::<UInt64Type>();
        let partial_sums = values[1].as_primitive::<T>();
        // update counts with partial counts
        self.counts.resize(total_num_groups, 0);
        self.null_state.accumulate(
            group_indices,
            partial_counts,
            opt_filter,
            total_num_groups,
            |group_index, partial_count| {
                self.counts[group_index] += partial_count;
            },
        );

        // update sums
        self.sums.resize(total_num_groups, T::default_value());
        self.null_state.accumulate(
            group_indices,
            partial_sums,
            opt_filter,
            total_num_groups,
            |group_index, new_value: <T as ArrowPrimitiveType>::Native| {
                let sum = &mut self.sums[group_index];
                *sum = sum.add_wrapping(new_value);
            },
        );

        Ok(())
    }

    fn evaluate(&mut self, emit_to: EmitTo) -> Result<ArrayRef> {
        let counts = emit_to.take_needed(&mut self.counts);
        let sums = emit_to.take_needed(&mut self.sums);
        let nulls = self.null_state.build(emit_to);

        assert_eq!(nulls.len(), sums.len());
        assert_eq!(counts.len(), sums.len());

        // don't evaluate averages with null inputs to avoid errors on null values

        let array: PrimitiveArray<T> = if nulls.null_count() > 0 {
            let mut builder = PrimitiveBuilder::<T>::with_capacity(nulls.len());
            let iter = sums.into_iter().zip(counts.into_iter()).zip(nulls.iter());

            for ((sum, count), is_valid) in iter {
                if is_valid {
                    builder.append_value((self.avg_fn)(sum, count)?)
                } else {
                    builder.append_null();
                }
            }
            builder.finish()
        } else {
            let averages: Vec<T::Native> = sums
                .into_iter()
                .zip(counts.into_iter())
                .map(|(sum, count)| (self.avg_fn)(sum, count))
                .collect::<Result<Vec<_>>>()?;
            PrimitiveArray::new(averages.into(), Some(nulls)) // no copy
        };

        // fix up decimal precision and scale for decimals
        let array = adjust_output_array(&self.return_data_type, Arc::new(array))?;

        Ok(array)
    }

    // return arrays for sums and counts
    fn state(&mut self, emit_to: EmitTo) -> Result<Vec<ArrayRef>> {
        let nulls = self.null_state.build(emit_to);
        let nulls = Some(nulls);

        let counts = emit_to.take_needed(&mut self.counts);
        let counts = UInt64Array::new(counts.into(), nulls.clone()); // zero copy

        let sums = emit_to.take_needed(&mut self.sums);
        let sums = PrimitiveArray::<T>::new(sums.into(), nulls); // zero copy
        let sums = adjust_output_array(&self.sum_data_type, Arc::new(sums))?;

        Ok(vec![
            Arc::new(counts) as ArrayRef,
            Arc::new(sums) as ArrayRef,
        ])
    }

    fn size(&self) -> usize {
        self.counts.capacity() * std::mem::size_of::<u64>()
            + self.sums.capacity() * std::mem::size_of::<T>()
    }
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
