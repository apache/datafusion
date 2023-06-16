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

use std::any::Any;
use std::convert::TryFrom;
use std::sync::Arc;

use crate::aggregate::row_accumulator::{
    is_row_accumulator_support_dtype, RowAccumulator, RowAccumulatorItem,
};
use crate::aggregate::row_agg_macros::*;
use crate::aggregate::sum;
use crate::aggregate::sum::sum_batch;
use crate::aggregate::utils::down_cast_any_ref;
use crate::aggregate::utils::{apply_filter_on_rows, calculate_result_decimal_for_avg};
use crate::expressions::format_state_name;
use crate::{AggregateExpr, PhysicalExpr};
use arrow::compute;
use arrow::datatypes::DataType;
use arrow::{
    array::{ArrayRef, UInt64Array},
    datatypes::Field,
};
use arrow_array::{
    Array, BooleanArray, Decimal128Array, Float32Array, Float64Array, Int16Array,
    Int32Array, Int64Array, Int8Array, UInt16Array, UInt32Array, UInt8Array,
};
use datafusion_common::{downcast_value, ScalarValue};
use datafusion_common::{DataFusionError, Result};
use datafusion_expr::Accumulator;
use datafusion_row::accessor::{ArrowArrayReader, RowAccessor, RowAccumulatorNativeType};

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

    fn supports_bounded_execution(&self) -> bool {
        true
    }

    fn create_row_accumulator(&self, start_index: usize) -> Result<RowAccumulatorItem> {
        Ok(
            AvgRowAccumulator::new(start_index, &self.sum_data_type, &self.rt_data_type)
                .into(),
        )
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
            ScalarValue::Decimal128(value, precision, scale) => {
                Ok(match value {
                    None => ScalarValue::Decimal128(None, precision, scale),
                    Some(value) => {
                        // now the sum_type and return type is not the same, need to convert the sum type to return type
                        calculate_result_decimal_for_avg(
                            value,
                            self.count as i128,
                            scale,
                            &self.return_data_type,
                        )?
                    }
                })
            }
            _ => Err(DataFusionError::Internal(
                "Sum should be f64 or decimal128 on average".to_string(),
            )),
        }
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self) - std::mem::size_of_val(&self.sum) + self.sum.size()
    }
}

#[derive(Debug)]
pub struct AvgRowAccumulator {
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
        &self,
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

    fn update_row_indices(
        &self,
        values: &[ArrayRef],
        filter: &Option<&BooleanArray>,
        row_indices: &[usize],
        accessor: &mut RowAccessor,
    ) -> Result<()> {
        let array = &values[0];
        let selected_row_idx = apply_filter_on_rows(filter, array, row_indices);
        if !selected_row_idx.is_empty() {
            accessor.add_u64(self.state_index, selected_row_idx.len() as u64);
            let array_dt = array.data_type();
            dispatch_all_supported_data_types! { impl_avg_row_accumulator_update_row_idx_dispatch, array_dt, array, selected_row_idx, accessor, self}
        }

        Ok(())
    }

    #[inline(always)]
    fn update_value<N: RowAccumulatorNativeType>(
        &self,
        native_value: Option<N>,
        accessor: &mut RowAccessor,
    ) {
        if let Some(value) = native_value {
            accessor.add_u64(self.state_index, 1);
            value.add_to_row(self.state_index + 1, accessor);
        }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expressions::col;
    use crate::expressions::tests::aggregate;
    use crate::generic_test_op;
    use arrow::datatypes::*;
    use arrow::record_batch::RecordBatch;
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
