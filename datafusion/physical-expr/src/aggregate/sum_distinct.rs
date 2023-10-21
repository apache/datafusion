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

use crate::expressions::format_state_name;
use arrow::datatypes::{DataType, Field};
use std::any::Any;
use std::sync::Arc;

use ahash::RandomState;
use arrow::array::{Array, ArrayRef};
use arrow_array::cast::AsArray;
use arrow_array::types::*;
use arrow_array::{ArrowNativeTypeOp, ArrowPrimitiveType};
use arrow_buffer::{ArrowNativeType, ToByteSlice};
use std::collections::HashSet;

use crate::aggregate::sum::downcast_sum;
use crate::aggregate::utils::down_cast_any_ref;
use crate::{AggregateExpr, PhysicalExpr};
use datafusion_common::{not_impl_err, DataFusionError, Result, ScalarValue};
use datafusion_expr::type_coercion::aggregates::sum_return_type;
use datafusion_expr::Accumulator;

/// Expression for a SUM(DISTINCT) aggregation.
#[derive(Debug)]
pub struct DistinctSum {
    /// Column name
    name: String,
    /// The DataType for the final sum
    data_type: DataType,
    /// The input arguments, only contains 1 item for sum
    exprs: Vec<Arc<dyn PhysicalExpr>>,
}

impl DistinctSum {
    /// Create a SUM(DISTINCT) aggregate function.
    pub fn new(
        exprs: Vec<Arc<dyn PhysicalExpr>>,
        name: String,
        data_type: DataType,
    ) -> Self {
        let data_type = sum_return_type(&data_type).unwrap();
        Self {
            name,
            data_type,
            exprs,
        }
    }
}

impl AggregateExpr for DistinctSum {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn field(&self) -> Result<Field> {
        Ok(Field::new(&self.name, self.data_type.clone(), true))
    }

    fn state_fields(&self) -> Result<Vec<Field>> {
        // State field is a List which stores items to rebuild hash set.
        Ok(vec![Field::new_list(
            format_state_name(&self.name, "sum distinct"),
            Field::new("item", self.data_type.clone(), true),
            false,
        )])
    }

    fn expressions(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        self.exprs.clone()
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn create_accumulator(&self) -> Result<Box<dyn Accumulator>> {
        macro_rules! helper {
            ($t:ty, $dt:expr) => {
                Ok(Box::new(DistinctSumAccumulator::<$t>::try_new(&$dt)?))
            };
        }
        downcast_sum!(self, helper)
    }
}

impl PartialEq<dyn Any> for DistinctSum {
    fn eq(&self, other: &dyn Any) -> bool {
        down_cast_any_ref(other)
            .downcast_ref::<Self>()
            .map(|x| {
                self.name == x.name
                    && self.data_type == x.data_type
                    && self.exprs.len() == x.exprs.len()
                    && self
                        .exprs
                        .iter()
                        .zip(x.exprs.iter())
                        .all(|(this, other)| this.eq(other))
            })
            .unwrap_or(false)
    }
}

/// A wrapper around a type to provide hash for floats
#[derive(Copy, Clone)]
struct Hashable<T>(T);

impl<T: ToByteSlice> std::hash::Hash for Hashable<T> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.0.to_byte_slice().hash(state)
    }
}

impl<T: ArrowNativeTypeOp> PartialEq for Hashable<T> {
    fn eq(&self, other: &Self) -> bool {
        self.0.is_eq(other.0)
    }
}

impl<T: ArrowNativeTypeOp> Eq for Hashable<T> {}

struct DistinctSumAccumulator<T: ArrowPrimitiveType> {
    values: HashSet<Hashable<T::Native>, RandomState>,
    data_type: DataType,
}

impl<T: ArrowPrimitiveType> std::fmt::Debug for DistinctSumAccumulator<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "DistinctSumAccumulator({})", self.data_type)
    }
}

impl<T: ArrowPrimitiveType> DistinctSumAccumulator<T> {
    pub fn try_new(data_type: &DataType) -> Result<Self> {
        Ok(Self {
            values: HashSet::default(),
            data_type: data_type.clone(),
        })
    }
}

impl<T: ArrowPrimitiveType> Accumulator for DistinctSumAccumulator<T> {
    fn state(&self) -> Result<Vec<ScalarValue>> {
        // 1. Stores aggregate state in `ScalarValue::List`
        // 2. Constructs `ScalarValue::List` state from distinct numeric stored in hash set
        let state_out = {
            let distinct_values = self
                .values
                .iter()
                .map(|value| {
                    ScalarValue::new_primitive::<T>(Some(value.0), &self.data_type)
                })
                .collect::<Result<Vec<_>>>()?;

            vec![ScalarValue::List(ScalarValue::new_list(
                &distinct_values,
                &self.data_type,
            ))]
        };
        Ok(state_out)
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if values.is_empty() {
            return Ok(());
        }

        let array = values[0].as_primitive::<T>();
        match array.nulls().filter(|x| x.null_count() > 0) {
            Some(n) => {
                for idx in n.valid_indices() {
                    self.values.insert(Hashable(array.value(idx)));
                }
            }
            None => array.values().iter().for_each(|x| {
                self.values.insert(Hashable(*x));
            }),
        }
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        for x in states[0].as_list::<i32>().iter().flatten() {
            self.update_batch(&[x])?
        }
        Ok(())
    }

    fn evaluate(&self) -> Result<ScalarValue> {
        let mut acc = T::Native::usize_as(0);
        for distinct_value in self.values.iter() {
            acc = acc.add_wrapping(distinct_value.0)
        }
        let v = (!self.values.is_empty()).then_some(acc);
        ScalarValue::new_primitive::<T>(v, &self.data_type)
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self)
            + self.values.capacity() * std::mem::size_of::<T::Native>()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expressions::tests::assert_aggregate;
    use arrow::array::*;
    use datafusion_common::Result;
    use datafusion_expr::AggregateFunction;

    fn run_update_batch(
        return_type: DataType,
        arrays: &[ArrayRef],
    ) -> Result<(Vec<ScalarValue>, ScalarValue)> {
        let agg = DistinctSum::new(vec![], String::from("__col_name__"), return_type);

        let mut accum = agg.create_accumulator()?;
        accum.update_batch(arrays)?;

        Ok((accum.state()?, accum.evaluate()?))
    }

    #[test]
    fn sum_distinct_update_batch() -> Result<()> {
        let array_int64: ArrayRef = Arc::new(Int64Array::from(vec![1, 1, 3]));
        let arrays = vec![array_int64];
        let (states, result) = run_update_batch(DataType::Int64, &arrays)?;

        assert_eq!(states.len(), 1);
        assert_eq!(result, ScalarValue::Int64(Some(4)));

        Ok(())
    }

    #[test]
    fn sum_distinct_i32_with_nulls() {
        let array = Arc::new(Int32Array::from(vec![
            Some(1),
            Some(1),
            None,
            Some(2),
            Some(2),
            Some(3),
        ]));
        assert_aggregate(array, AggregateFunction::Sum, true, 6_i64.into());
    }

    #[test]
    fn sum_distinct_u32_with_nulls() {
        let array: ArrayRef = Arc::new(UInt32Array::from(vec![
            Some(1_u32),
            Some(1_u32),
            Some(3_u32),
            Some(3_u32),
            None,
        ]));
        assert_aggregate(array, AggregateFunction::Sum, true, 4_u64.into());
    }

    #[test]
    fn sum_distinct_f64() {
        let array: ArrayRef =
            Arc::new(Float64Array::from(vec![1_f64, 1_f64, 3_f64, 3_f64, 3_f64]));
        assert_aggregate(array, AggregateFunction::Sum, true, 4_f64.into());
    }

    #[test]
    fn sum_distinct_decimal_with_nulls() {
        let array: ArrayRef = Arc::new(
            (1..6)
                .map(|i| if i == 2 { None } else { Some(i % 2) })
                .collect::<Decimal128Array>()
                .with_precision_and_scale(35, 0)
                .unwrap(),
        );
        assert_aggregate(
            array,
            AggregateFunction::Sum,
            true,
            ScalarValue::Decimal128(Some(1), 38, 0),
        );
    }
}
