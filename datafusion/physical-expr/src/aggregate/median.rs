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

//! # Median

use crate::aggregate::utils::down_cast_any_ref;
use crate::expressions::format_state_name;
use crate::{AggregateExpr, PhysicalExpr};
use arrow::array::{Array, ArrayRef};
use arrow::datatypes::{DataType, Field};
use arrow_array::cast::AsArray;
use arrow_array::{downcast_integer, ArrowNativeTypeOp, ArrowNumericType};
use arrow_buffer::ArrowNativeType;
use datafusion_common::{DataFusionError, Result, ScalarValue};
use datafusion_expr::Accumulator;
use std::any::Any;
use std::fmt::Formatter;
use std::sync::Arc;

/// MEDIAN aggregate expression. This uses a lot of memory because all values need to be
/// stored in memory before a result can be computed. If an approximation is sufficient
/// then APPROX_MEDIAN provides a much more efficient solution.
#[derive(Debug)]
pub struct Median {
    name: String,
    expr: Arc<dyn PhysicalExpr>,
    data_type: DataType,
}

impl Median {
    /// Create a new MEDIAN aggregate function
    pub fn new(
        expr: Arc<dyn PhysicalExpr>,
        name: impl Into<String>,
        data_type: DataType,
    ) -> Self {
        Self {
            name: name.into(),
            expr,
            data_type,
        }
    }
}

impl AggregateExpr for Median {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn field(&self) -> Result<Field> {
        Ok(Field::new(&self.name, self.data_type.clone(), true))
    }

    fn create_accumulator(&self) -> Result<Box<dyn Accumulator>> {
        use arrow_array::types::*;
        macro_rules! helper {
            ($t:ty, $dt:expr) => {
                Ok(Box::new(MedianAccumulator::<$t> {
                    data_type: $dt.clone(),
                    all_values: vec![],
                }))
            };
        }
        let dt = &self.data_type;
        downcast_integer! {
            dt => (helper, dt),
            DataType::Float16 => helper!(Float16Type, dt),
            DataType::Float32 => helper!(Float32Type, dt),
            DataType::Float64 => helper!(Float64Type, dt),
            DataType::Decimal128(_, _) => helper!(Decimal128Type, dt),
            DataType::Decimal256(_, _) => helper!(Decimal256Type, dt),
            _ => Err(DataFusionError::NotImplemented(format!(
                "MedianAccumulator not supported for {} with {}",
                self.name(),
                self.data_type
            ))),
        }
    }

    fn state_fields(&self) -> Result<Vec<Field>> {
        //Intermediate state is a list of the elements we have collected so far
        let field = Field::new("item", self.data_type.clone(), true);
        let data_type = DataType::List(Arc::new(field));

        Ok(vec![Field::new(
            format_state_name(&self.name, "median"),
            data_type,
            true,
        )])
    }

    fn expressions(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        vec![self.expr.clone()]
    }

    fn name(&self) -> &str {
        &self.name
    }
}

impl PartialEq<dyn Any> for Median {
    fn eq(&self, other: &dyn Any) -> bool {
        down_cast_any_ref(other)
            .downcast_ref::<Self>()
            .map(|x| {
                self.name == x.name
                    && self.data_type == x.data_type
                    && self.expr.eq(&x.expr)
            })
            .unwrap_or(false)
    }
}

/// The median accumulator accumulates the raw input values
/// as `ScalarValue`s
///
/// The intermediate state is represented as a List of scalar values updated by
/// `merge_batch` and a `Vec` of `ArrayRef` that are converted to scalar values
/// in the final evaluation step so that we avoid expensive conversions and
/// allocations during `update_batch`.
struct MedianAccumulator<T: ArrowNumericType> {
    data_type: DataType,
    all_values: Vec<T::Native>,
}

impl<T: ArrowNumericType> std::fmt::Debug for MedianAccumulator<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "MedianAccumulator({})", self.data_type)
    }
}

impl<T: ArrowNumericType> Accumulator for MedianAccumulator<T> {
    fn state(&self) -> Result<Vec<ScalarValue>> {
        let all_values: Vec<ScalarValue> = self
            .all_values
            .iter()
            .map(|x| ScalarValue::new_primitive::<T>(Some(*x), &self.data_type))
            .collect();

        let arr = ScalarValue::scalars_to_list_array(&all_values, &self.data_type);
        Ok(vec![ScalarValue::List(arr)])
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let values = values[0].as_primitive::<T>();
        self.all_values.reserve(values.len() - values.null_count());
        self.all_values.extend(values.iter().flatten());
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        let array = states[0].as_list::<i32>();
        for v in array.iter().flatten() {
            self.update_batch(&[v])?
        }
        Ok(())
    }

    fn evaluate(&self) -> Result<ScalarValue> {
        // TODO: evaluate could pass &mut self
        let mut d = self.all_values.clone();
        let cmp = |x: &T::Native, y: &T::Native| x.compare(*y);

        let len = d.len();
        let median = if len == 0 {
            None
        } else if len % 2 == 0 {
            let (low, high, _) = d.select_nth_unstable_by(len / 2, cmp);
            let (_, low, _) = low.select_nth_unstable_by(low.len() - 1, cmp);
            let median = low.add_wrapping(*high).div_wrapping(T::Native::usize_as(2));
            Some(median)
        } else {
            let (_, median, _) = d.select_nth_unstable_by(len / 2, cmp);
            Some(*median)
        };
        Ok(ScalarValue::new_primitive::<T>(median, &self.data_type))
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self)
            + self.all_values.capacity() * std::mem::size_of::<T::Native>()
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

    #[test]
    fn median_decimal() -> Result<()> {
        // test median
        let array: ArrayRef = Arc::new(
            (1..7)
                .map(Some)
                .collect::<Decimal128Array>()
                .with_precision_and_scale(10, 4)?,
        );

        generic_test_op!(
            array,
            DataType::Decimal128(10, 4),
            Median,
            ScalarValue::Decimal128(Some(3), 10, 4)
        )
    }

    #[test]
    fn median_decimal_with_nulls() -> Result<()> {
        let array: ArrayRef = Arc::new(
            (1..6)
                .map(|i| if i == 2 { None } else { Some(i) })
                .collect::<Decimal128Array>()
                .with_precision_and_scale(10, 4)?,
        );
        generic_test_op!(
            array,
            DataType::Decimal128(10, 4),
            Median,
            ScalarValue::Decimal128(Some(3), 10, 4)
        )
    }

    #[test]
    fn median_decimal_all_nulls() -> Result<()> {
        // test median
        let array: ArrayRef = Arc::new(
            std::iter::repeat::<Option<i128>>(None)
                .take(6)
                .collect::<Decimal128Array>()
                .with_precision_and_scale(10, 4)?,
        );
        generic_test_op!(
            array,
            DataType::Decimal128(10, 4),
            Median,
            ScalarValue::Decimal128(None, 10, 4)
        )
    }

    #[test]
    fn median_i32_odd() -> Result<()> {
        let a: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5]));
        generic_test_op!(a, DataType::Int32, Median, ScalarValue::from(3_i32))
    }

    #[test]
    fn median_i32_even() -> Result<()> {
        let a: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5, 6]));
        generic_test_op!(a, DataType::Int32, Median, ScalarValue::from(3_i32))
    }

    #[test]
    fn median_i32_with_nulls() -> Result<()> {
        let a: ArrayRef = Arc::new(Int32Array::from(vec![
            Some(1),
            None,
            Some(3),
            Some(4),
            Some(5),
        ]));
        generic_test_op!(a, DataType::Int32, Median, ScalarValue::from(3i32))
    }

    #[test]
    fn median_i32_all_nulls() -> Result<()> {
        let a: ArrayRef = Arc::new(Int32Array::from(vec![None, None]));
        generic_test_op!(a, DataType::Int32, Median, ScalarValue::Int32(None))
    }

    #[test]
    fn median_u32_odd() -> Result<()> {
        let a: ArrayRef =
            Arc::new(UInt32Array::from(vec![1_u32, 2_u32, 3_u32, 4_u32, 5_u32]));
        generic_test_op!(a, DataType::UInt32, Median, ScalarValue::from(3u32))
    }

    #[test]
    fn median_u32_even() -> Result<()> {
        let a: ArrayRef = Arc::new(UInt32Array::from(vec![
            1_u32, 2_u32, 3_u32, 4_u32, 5_u32, 6_u32,
        ]));
        generic_test_op!(a, DataType::UInt32, Median, ScalarValue::from(3u32))
    }

    #[test]
    fn median_f32_odd() -> Result<()> {
        let a: ArrayRef =
            Arc::new(Float32Array::from(vec![1_f32, 2_f32, 3_f32, 4_f32, 5_f32]));
        generic_test_op!(a, DataType::Float32, Median, ScalarValue::from(3_f32))
    }

    #[test]
    fn median_f32_even() -> Result<()> {
        let a: ArrayRef = Arc::new(Float32Array::from(vec![
            1_f32, 2_f32, 3_f32, 4_f32, 5_f32, 6_f32,
        ]));
        generic_test_op!(a, DataType::Float32, Median, ScalarValue::from(3.5_f32))
    }

    #[test]
    fn median_f64_odd() -> Result<()> {
        let a: ArrayRef =
            Arc::new(Float64Array::from(vec![1_f64, 2_f64, 3_f64, 4_f64, 5_f64]));
        generic_test_op!(a, DataType::Float64, Median, ScalarValue::from(3_f64))
    }

    #[test]
    fn median_f64_even() -> Result<()> {
        let a: ArrayRef = Arc::new(Float64Array::from(vec![
            1_f64, 2_f64, 3_f64, 4_f64, 5_f64, 6_f64,
        ]));
        generic_test_op!(a, DataType::Float64, Median, ScalarValue::from(3.5_f64))
    }
}
