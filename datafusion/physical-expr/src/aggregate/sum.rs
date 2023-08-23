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

//! Defines `SUM` and `SUM DISTINCT` aggregate accumulators

use std::any::Any;
use std::convert::TryFrom;
use std::ops::AddAssign;
use std::sync::Arc;

use super::groups_accumulator::prim_op::PrimitiveGroupsAccumulator;
use crate::aggregate::utils::down_cast_any_ref;
use crate::expressions::format_state_name;
use crate::{AggregateExpr, GroupsAccumulator, PhysicalExpr};
use arrow::array::Array;
use arrow::array::Decimal128Array;
use arrow::array::Decimal256Array;
use arrow::compute;
use arrow::datatypes::DataType;
use arrow::{
    array::{ArrayRef, Float64Array, Int64Array, UInt64Array},
    datatypes::Field,
};
use arrow_array::types::{
    Decimal128Type, Decimal256Type, Float32Type, Float64Type, Int32Type, Int64Type,
    UInt32Type, UInt64Type,
};
use datafusion_common::{
    downcast_value, internal_err, not_impl_err, DataFusionError, Result, ScalarValue,
};
use datafusion_expr::type_coercion::aggregates::sum_return_type;
use datafusion_expr::Accumulator;

/// SUM aggregate expression
#[derive(Debug, Clone)]
pub struct Sum {
    name: String,
    data_type: DataType,
    expr: Arc<dyn PhysicalExpr>,
    nullable: bool,
}

impl Sum {
    /// Create a new SUM aggregate function
    pub fn new(
        expr: Arc<dyn PhysicalExpr>,
        name: impl Into<String>,
        data_type: DataType,
    ) -> Self {
        let data_type = sum_return_type(&data_type).unwrap();
        Self {
            name: name.into(),
            expr,
            data_type,
            nullable: true,
        }
    }
}

/// Creates a [`PrimitiveGroupsAccumulator`] with the specified
/// [`ArrowPrimitiveType`] which applies `$FN` to each element
///
/// [`ArrowPrimitiveType`]: arrow::datatypes::ArrowPrimitiveType
macro_rules! instantiate_primitive_accumulator {
    ($SELF:expr, $PRIMTYPE:ident, $FN:expr) => {{
        Ok(Box::new(PrimitiveGroupsAccumulator::<$PRIMTYPE, _>::new(
            &$SELF.data_type,
            $FN,
        )))
    }};
}

impl AggregateExpr for Sum {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn field(&self) -> Result<Field> {
        Ok(Field::new(
            &self.name,
            self.data_type.clone(),
            self.nullable,
        ))
    }

    fn create_accumulator(&self) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(SumAccumulator::try_new(&self.data_type)?))
    }

    fn state_fields(&self) -> Result<Vec<Field>> {
        Ok(vec![Field::new(
            format_state_name(&self.name, "sum"),
            self.data_type.clone(),
            self.nullable,
        )])
    }

    fn expressions(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        vec![self.expr.clone()]
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn groups_accumulator_supported(&self) -> bool {
        true
    }

    fn create_groups_accumulator(&self) -> Result<Box<dyn GroupsAccumulator>> {
        // instantiate specialized accumulator
        match self.data_type {
            DataType::UInt64 => {
                instantiate_primitive_accumulator!(self, UInt64Type, |x, y| x
                    .add_assign(y))
            }
            DataType::Int64 => {
                instantiate_primitive_accumulator!(self, Int64Type, |x, y| x
                    .add_assign(y))
            }
            DataType::UInt32 => {
                instantiate_primitive_accumulator!(self, UInt32Type, |x, y| x
                    .add_assign(y))
            }
            DataType::Int32 => {
                instantiate_primitive_accumulator!(self, Int32Type, |x, y| x
                    .add_assign(y))
            }
            DataType::Float32 => {
                instantiate_primitive_accumulator!(self, Float32Type, |x, y| x
                    .add_assign(y))
            }
            DataType::Float64 => {
                instantiate_primitive_accumulator!(self, Float64Type, |x, y| x
                    .add_assign(y))
            }
            DataType::Decimal128(_, _) => {
                instantiate_primitive_accumulator!(self, Decimal128Type, |x, y| x
                    .add_assign(y))
            }
            DataType::Decimal256(_, _) => {
                instantiate_primitive_accumulator!(self, Decimal256Type, |x, y| *x =
                    *x + y)
            }
            _ => not_impl_err!(
                "GroupsAccumulator not supported for {}: {}",
                self.name,
                self.data_type
            ),
        }
    }

    fn reverse_expr(&self) -> Option<Arc<dyn AggregateExpr>> {
        Some(Arc::new(self.clone()))
    }

    fn create_sliding_accumulator(&self) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(SlidingSumAccumulator::try_new(&self.data_type)?))
    }
}

impl PartialEq<dyn Any> for Sum {
    fn eq(&self, other: &dyn Any) -> bool {
        down_cast_any_ref(other)
            .downcast_ref::<Self>()
            .map(|x| {
                self.name == x.name
                    && self.data_type == x.data_type
                    && self.nullable == x.nullable
                    && self.expr.eq(&x.expr)
            })
            .unwrap_or(false)
    }
}

/// This accumulator computes SUM incrementally
#[derive(Debug)]
struct SumAccumulator {
    sum: ScalarValue,
}

impl SumAccumulator {
    /// new sum accumulator
    pub fn try_new(data_type: &DataType) -> Result<Self> {
        Ok(Self {
            sum: ScalarValue::try_from(data_type)?,
        })
    }
}

/// This accumulator incrementally computes sums over a sliding window
#[derive(Debug)]
struct SlidingSumAccumulator {
    sum: ScalarValue,
    count: u64,
}

impl SlidingSumAccumulator {
    /// new sum accumulator
    pub fn try_new(data_type: &DataType) -> Result<Self> {
        Ok(Self {
            // start at zero
            sum: ScalarValue::try_from(data_type)?,
            count: 0,
        })
    }
}

/// Sums the contents of the `$VALUES` array using the arrow compute
/// kernel, and return a `ScalarValue::$SCALAR`.
///
/// Handles nullability
macro_rules! typed_sum_delta_batch {
    ($VALUES:expr, $ARRAYTYPE:ident, $SCALAR:ident) => {{
        let array = downcast_value!($VALUES, $ARRAYTYPE);
        let delta = compute::sum(array);
        ScalarValue::$SCALAR(delta)
    }};
}

fn sum_decimal_batch(values: &ArrayRef, precision: u8, scale: i8) -> Result<ScalarValue> {
    let array = downcast_value!(values, Decimal128Array);
    let result = compute::sum(array);
    Ok(ScalarValue::Decimal128(result, precision, scale))
}

fn sum_decimal256_batch(
    values: &ArrayRef,
    precision: u8,
    scale: i8,
) -> Result<ScalarValue> {
    let array = downcast_value!(values, Decimal256Array);
    let result = compute::sum(array);
    Ok(ScalarValue::Decimal256(result, precision, scale))
}

// sums the array and returns a ScalarValue of its corresponding type.
pub(crate) fn sum_batch(values: &ArrayRef) -> Result<ScalarValue> {
    Ok(match values.data_type() {
        DataType::Decimal128(precision, scale) => {
            sum_decimal_batch(values, *precision, *scale)?
        }
        DataType::Decimal256(precision, scale) => {
            sum_decimal256_batch(values, *precision, *scale)?
        }
        DataType::Float64 => typed_sum_delta_batch!(values, Float64Array, Float64),
        DataType::Int64 => typed_sum_delta_batch!(values, Int64Array, Int64),
        DataType::UInt64 => typed_sum_delta_batch!(values, UInt64Array, UInt64),
        e => {
            return internal_err!("Sum is not expected to receive the type {e:?}");
        }
    })
}

impl Accumulator for SumAccumulator {
    fn state(&self) -> Result<Vec<ScalarValue>> {
        Ok(vec![self.sum.clone()])
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let values = &values[0];
        let delta = sum_batch(values)?;
        self.sum = self.sum.add(&delta)?;
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        // sum(sum1, sum2, sum3, ...) = sum1 + sum2 + sum3 + ...
        self.update_batch(states)
    }

    fn evaluate(&self) -> Result<ScalarValue> {
        // TODO: add the checker for overflow
        // For the decimal(precision,_) data type, the absolute of value must be less than 10^precision.
        Ok(self.sum.clone())
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self) - std::mem::size_of_val(&self.sum) + self.sum.size()
    }
}

impl Accumulator for SlidingSumAccumulator {
    fn state(&self) -> Result<Vec<ScalarValue>> {
        Ok(vec![self.sum.clone(), ScalarValue::from(self.count)])
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let values = &values[0];
        self.count += (values.len() - values.null_count()) as u64;
        let delta = sum_batch(values)?;
        self.sum = self.sum.add(&delta)?;
        Ok(())
    }

    fn retract_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let values = &values[0];
        self.count -= (values.len() - values.null_count()) as u64;
        let delta = sum_batch(values)?;
        self.sum = self.sum.sub(&delta)?;
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        // sum(sum1, sum2, sum3, ...) = sum1 + sum2 + sum3 + ...
        self.update_batch(states)
    }

    fn evaluate(&self) -> Result<ScalarValue> {
        // TODO: add the checker for overflow
        // For the decimal(precision,_) data type, the absolute of value must be less than 10^precision.
        if self.count == 0 {
            ScalarValue::try_from(&self.sum.get_datatype())
        } else {
            Ok(self.sum.clone())
        }
    }

    fn supports_retract_batch(&self) -> bool {
        true
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self) - std::mem::size_of_val(&self.sum) + self.sum.size()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expressions::tests::assert_aggregate;
    use arrow_array::{Float32Array, Int32Array, UInt32Array};
    use datafusion_expr::AggregateFunction;

    #[test]
    fn sum_decimal() {
        // test sum batch
        let array: ArrayRef = Arc::new(
            (1..6)
                .map(Some)
                .collect::<Decimal128Array>()
                .with_precision_and_scale(10, 0)
                .unwrap(),
        );
        let result = sum_batch(&array).unwrap();
        assert_eq!(ScalarValue::Decimal128(Some(15), 10, 0), result);

        // test agg
        let array: ArrayRef = Arc::new(
            (1..6)
                .map(Some)
                .collect::<Decimal128Array>()
                .with_precision_and_scale(10, 0)
                .unwrap(),
        );

        assert_aggregate(
            array,
            AggregateFunction::Sum,
            ScalarValue::Decimal128(Some(15), 20, 0),
        );
    }

    #[test]
    fn sum_decimal_with_nulls() {
        // test with batch
        let array: ArrayRef = Arc::new(
            (1..6)
                .map(|i| if i == 2 { None } else { Some(i) })
                .collect::<Decimal128Array>()
                .with_precision_and_scale(10, 0)
                .unwrap(),
        );
        let result = sum_batch(&array).unwrap();
        assert_eq!(ScalarValue::Decimal128(Some(13), 10, 0), result);

        // test agg
        let array: ArrayRef = Arc::new(
            (1..6)
                .map(|i| if i == 2 { None } else { Some(i) })
                .collect::<Decimal128Array>()
                .with_precision_and_scale(35, 0)
                .unwrap(),
        );

        assert_aggregate(
            array,
            AggregateFunction::Sum,
            ScalarValue::Decimal128(Some(13), 38, 0),
        );
    }

    #[test]
    fn sum_decimal_all_nulls() {
        // test with batch
        let array: ArrayRef = Arc::new(
            std::iter::repeat::<Option<i128>>(None)
                .take(6)
                .collect::<Decimal128Array>()
                .with_precision_and_scale(10, 0)
                .unwrap(),
        );
        let result = sum_batch(&array).unwrap();
        assert_eq!(ScalarValue::Decimal128(None, 10, 0), result);

        // test agg
        assert_aggregate(
            array,
            AggregateFunction::Sum,
            ScalarValue::Decimal128(None, 20, 0),
        );
    }

    #[test]
    fn sum_i32() {
        let a: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5]));
        assert_aggregate(a, AggregateFunction::Sum, ScalarValue::from(15i64));
    }

    #[test]
    fn sum_i32_with_nulls() {
        let a: ArrayRef = Arc::new(Int32Array::from(vec![
            Some(1),
            None,
            Some(3),
            Some(4),
            Some(5),
        ]));
        assert_aggregate(a, AggregateFunction::Sum, ScalarValue::from(13i64));
    }

    #[test]
    fn sum_i32_all_nulls() {
        let a: ArrayRef = Arc::new(Int32Array::from(vec![None, None]));
        assert_aggregate(a, AggregateFunction::Sum, ScalarValue::Int64(None));
    }

    #[test]
    fn sum_u32() {
        let a: ArrayRef =
            Arc::new(UInt32Array::from(vec![1_u32, 2_u32, 3_u32, 4_u32, 5_u32]));
        assert_aggregate(a, AggregateFunction::Sum, ScalarValue::from(15u64));
    }

    #[test]
    fn sum_f32() {
        let a: ArrayRef =
            Arc::new(Float32Array::from(vec![1_f32, 2_f32, 3_f32, 4_f32, 5_f32]));
        assert_aggregate(a, AggregateFunction::Sum, ScalarValue::from(15_f64));
    }

    #[test]
    fn sum_f64() {
        let a: ArrayRef =
            Arc::new(Float64Array::from(vec![1_f64, 2_f64, 3_f64, 4_f64, 5_f64]));
        assert_aggregate(a, AggregateFunction::Sum, ScalarValue::from(15_f64));
    }
}
