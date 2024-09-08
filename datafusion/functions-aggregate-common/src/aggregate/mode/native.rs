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
use std::fmt::Debug;
use std::hash::Hash;

use crate::utils::Hashable;
use arrow::{
    array::{ArrayRef, ArrowPrimitiveType},
    datatypes::DataType,
};
use datafusion_common::{cast::as_primitive_array, Result, ScalarValue};
use datafusion_expr_common::accumulator::Accumulator;

#[derive(Debug)]
pub struct PrimitiveModeAccumulator<T>
where
    T: ArrowPrimitiveType + Send,
    T::Native: Eq + Hash,
{
    value_counts: HashMap<T::Native, i64>,
    data_type: DataType,
}

impl<T> PrimitiveModeAccumulator<T>
where
    T: ArrowPrimitiveType + Send,
    T::Native: Eq + Hash + Clone,
{
    pub fn new(data_type: &DataType) -> Self {
        Self {
            value_counts: HashMap::default(),
            data_type: data_type.clone(),
        }
    }
}

impl<T> Accumulator for PrimitiveModeAccumulator<T>
where
    T: ArrowPrimitiveType + Send + Debug,
    T::Native: Eq + Hash + Clone + PartialOrd + Debug,
{
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if values.is_empty() {
            return Ok(());
        }
        let arr = as_primitive_array::<T>(&values[0])?;

        for value in arr.iter().flatten() {
            let counter = self.value_counts.entry(value).or_insert(0);
            *counter += 1;
        }

        Ok(())
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        let values: Vec<ScalarValue> = self
            .value_counts
            .keys()
            .map(|key| ScalarValue::new_primitive::<T>(Some(*key), &self.data_type))
            .collect::<Result<Vec<_>>>()?;

        let frequencies: Vec<ScalarValue> = self
            .value_counts
            .values()
            .map(|count| ScalarValue::from(*count))
            .collect();

        let values_scalar =
            ScalarValue::new_list_nullable(&values, &self.data_type.clone());
        let frequencies_scalar =
            ScalarValue::new_list_nullable(&frequencies, &DataType::Int64);

        Ok(vec![
            ScalarValue::List(values_scalar),
            ScalarValue::List(frequencies_scalar),
        ])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        if states.is_empty() {
            return Ok(());
        }

        let values_array = as_primitive_array::<T>(&states[0])?;
        let counts_array = as_primitive_array::<arrow::datatypes::Int64Type>(&states[1])?;

        for i in 0..values_array.len() {
            let value = values_array.value(i);
            let count = counts_array.value(i);
            let entry = self.value_counts.entry(value).or_insert(0);
            *entry += count;
        }

        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        let mut max_value: Option<T::Native> = None;
        let mut max_count: i64 = 0;

        self.value_counts.iter().for_each(|(value, &count)| {
            match count.cmp(&max_count) {
                std::cmp::Ordering::Greater => {
                    max_value = Some(*value);
                    max_count = count;
                }
                std::cmp::Ordering::Equal => {
                    max_value = match max_value {
                        Some(ref current_max_value) if value < current_max_value => {
                            Some(*value)
                        }
                        Some(ref current_max_value) => Some(*current_max_value),
                        None => Some(*value),
                    };
                }
                _ => {} // Do nothing if count is less than max_count
            }
        });

        match max_value {
            Some(val) => ScalarValue::new_primitive::<T>(Some(val), &self.data_type),
            None => ScalarValue::new_primitive::<T>(None, &self.data_type),
        }
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(&self.value_counts)
            + self.value_counts.len() * std::mem::size_of::<(T::Native, i64)>()
    }
}

#[derive(Debug)]
pub struct FloatModeAccumulator<T>
where
    T: ArrowPrimitiveType,
{
    value_counts: HashMap<Hashable<T::Native>, i64>,
    data_type: DataType,
}

impl<T> FloatModeAccumulator<T>
where
    T: ArrowPrimitiveType,
{
    pub fn new(data_type: &DataType) -> Self {
        Self {
            value_counts: HashMap::default(),
            data_type: data_type.clone(),
        }
    }
}

impl<T> Accumulator for FloatModeAccumulator<T>
where
    T: ArrowPrimitiveType + Send + Debug,
    T::Native: PartialOrd + Debug + Clone,
{
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if values.is_empty() {
            return Ok(());
        }

        let arr = as_primitive_array::<T>(&values[0])?;

        for value in arr.iter().flatten() {
            let counter = self.value_counts.entry(Hashable(value)).or_insert(0);
            *counter += 1;
        }

        Ok(())
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        let values: Vec<ScalarValue> = self
            .value_counts
            .keys()
            .map(|key| ScalarValue::new_primitive::<T>(Some(key.0), &self.data_type))
            .collect::<Result<Vec<_>>>()?;

        let frequencies: Vec<ScalarValue> = self
            .value_counts
            .values()
            .map(|count| ScalarValue::from(*count))
            .collect();

        let values_scalar =
            ScalarValue::new_list_nullable(&values, &self.data_type.clone());
        let frequencies_scalar =
            ScalarValue::new_list_nullable(&frequencies, &DataType::Int64);

        Ok(vec![
            ScalarValue::List(values_scalar),
            ScalarValue::List(frequencies_scalar),
        ])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        if states.is_empty() {
            return Ok(());
        }

        let values_array = as_primitive_array::<T>(&states[0])?;
        let counts_array = as_primitive_array::<arrow::datatypes::Int64Type>(&states[1])?;

        for i in 0..values_array.len() {
            let count = counts_array.value(i);
            let entry = self
                .value_counts
                .entry(Hashable(values_array.value(i)))
                .or_insert(0);
            *entry += count;
        }

        Ok(())
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        let mut max_value: Option<T::Native> = None;
        let mut max_count: i64 = 0;

        self.value_counts.iter().for_each(|(value, &count)| {
            match count.cmp(&max_count) {
                std::cmp::Ordering::Greater => {
                    max_value = Some(value.0);
                    max_count = count;
                }
                std::cmp::Ordering::Equal => {
                    max_value = match max_value {
                        Some(current_max_value) if value.0 < current_max_value => {
                            Some(value.0)
                        }
                        Some(current_max_value) => Some(current_max_value),
                        None => Some(value.0),
                    };
                }
                _ => {} // Do nothing if count is less than max_count
            }
        });

        match max_value {
            Some(val) => ScalarValue::new_primitive::<T>(Some(val), &self.data_type),
            None => ScalarValue::new_primitive::<T>(None, &self.data_type),
        }
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(&self.value_counts)
            + self.value_counts.len() * std::mem::size_of::<(Hashable<T::Native>, i64)>()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{
        ArrayRef, Date64Array, Float64Array, Int64Array, Time64MicrosecondArray,
    };
    use arrow::datatypes::{
        DataType, Date64Type, Float64Type, Int64Type, Time64MicrosecondType, TimeUnit,
    };
    use datafusion_common::ScalarValue;
    use std::sync::Arc;

    #[test]
    fn test_mode_accumulator_single_mode_int64() -> Result<()> {
        let mut acc = PrimitiveModeAccumulator::<Int64Type>::new(&DataType::Int64);
        let values: ArrayRef = Arc::new(Int64Array::from(vec![1, 2, 2, 3, 3, 3]));
        acc.update_batch(&[values])?;
        let result = acc.evaluate()?;
        assert_eq!(
            result,
            ScalarValue::new_primitive::<Int64Type>(Some(3), &DataType::Int64)?
        );
        Ok(())
    }

    #[test]
    fn test_mode_accumulator_with_nulls_int64() -> Result<()> {
        let mut acc = PrimitiveModeAccumulator::<Int64Type>::new(&DataType::Int64);
        let values: ArrayRef = Arc::new(Int64Array::from(vec![
            None,
            Some(1),
            Some(2),
            Some(2),
            Some(3),
            Some(3),
            Some(3),
        ]));
        acc.update_batch(&[values])?;
        let result = acc.evaluate()?;
        assert_eq!(
            result,
            ScalarValue::new_primitive::<Int64Type>(Some(3), &DataType::Int64)?
        );
        Ok(())
    }

    #[test]
    fn test_mode_accumulator_tie_case_int64() -> Result<()> {
        let mut acc = PrimitiveModeAccumulator::<Int64Type>::new(&DataType::Int64);
        let values: ArrayRef = Arc::new(Int64Array::from(vec![1, 2, 2, 3, 3]));
        acc.update_batch(&[values])?;
        let result = acc.evaluate()?;
        assert_eq!(
            result,
            ScalarValue::new_primitive::<Int64Type>(Some(2), &DataType::Int64)?
        );
        Ok(())
    }

    #[test]
    fn test_mode_accumulator_only_nulls_int64() -> Result<()> {
        let mut acc = PrimitiveModeAccumulator::<Int64Type>::new(&DataType::Int64);
        let values: ArrayRef = Arc::new(Int64Array::from(vec![None, None, None, None]));
        acc.update_batch(&[values])?;
        let result = acc.evaluate()?;
        assert_eq!(
            result,
            ScalarValue::new_primitive::<Int64Type>(None, &DataType::Int64)?
        );
        Ok(())
    }

    #[test]
    fn test_mode_accumulator_single_mode_float64() -> Result<()> {
        let mut acc = FloatModeAccumulator::<Float64Type>::new(&DataType::Float64);
        let values: ArrayRef =
            Arc::new(Float64Array::from(vec![1.0, 2.0, 2.0, 3.0, 3.0, 3.0]));
        acc.update_batch(&[values])?;
        let result = acc.evaluate()?;
        assert_eq!(
            result,
            ScalarValue::new_primitive::<Float64Type>(Some(3.0), &DataType::Float64)?
        );
        Ok(())
    }

    #[test]
    fn test_mode_accumulator_with_nulls_float64() -> Result<()> {
        let mut acc = FloatModeAccumulator::<Float64Type>::new(&DataType::Float64);
        let values: ArrayRef = Arc::new(Float64Array::from(vec![
            None,
            Some(1.0),
            Some(2.0),
            Some(2.0),
            Some(3.0),
            Some(3.0),
            Some(3.0),
        ]));
        acc.update_batch(&[values])?;
        let result = acc.evaluate()?;
        assert_eq!(
            result,
            ScalarValue::new_primitive::<Float64Type>(Some(3.0), &DataType::Float64)?
        );
        Ok(())
    }

    #[test]
    fn test_mode_accumulator_tie_case_float64() -> Result<()> {
        let mut acc = FloatModeAccumulator::<Float64Type>::new(&DataType::Float64);
        let values: ArrayRef =
            Arc::new(Float64Array::from(vec![1.0, 2.0, 2.0, 3.0, 3.0]));
        acc.update_batch(&[values])?;
        let result = acc.evaluate()?;
        assert_eq!(
            result,
            ScalarValue::new_primitive::<Float64Type>(Some(2.0), &DataType::Float64)?
        );
        Ok(())
    }

    #[test]
    fn test_mode_accumulator_only_nulls_float64() -> Result<()> {
        let mut acc = FloatModeAccumulator::<Float64Type>::new(&DataType::Float64);
        let values: ArrayRef = Arc::new(Float64Array::from(vec![None, None, None, None]));
        acc.update_batch(&[values])?;
        let result = acc.evaluate()?;
        assert_eq!(
            result,
            ScalarValue::new_primitive::<Float64Type>(None, &DataType::Float64)?
        );
        Ok(())
    }

    #[test]
    fn test_mode_accumulator_single_mode_date64() -> Result<()> {
        let mut acc = PrimitiveModeAccumulator::<Date64Type>::new(&DataType::Date64);
        let values: ArrayRef = Arc::new(Date64Array::from(vec![
            1609459200000,
            1609545600000,
            1609545600000,
            1609632000000,
            1609632000000,
            1609632000000,
        ]));
        acc.update_batch(&[values])?;
        let result = acc.evaluate()?;
        assert_eq!(
            result,
            ScalarValue::new_primitive::<Date64Type>(
                Some(1609632000000),
                &DataType::Date64
            )?
        );
        Ok(())
    }

    #[test]
    fn test_mode_accumulator_with_nulls_date64() -> Result<()> {
        let mut acc = PrimitiveModeAccumulator::<Date64Type>::new(&DataType::Date64);
        let values: ArrayRef = Arc::new(Date64Array::from(vec![
            None,
            Some(1609459200000),
            Some(1609545600000),
            Some(1609545600000),
            Some(1609632000000),
            Some(1609632000000),
            Some(1609632000000),
        ]));
        acc.update_batch(&[values])?;
        let result = acc.evaluate()?;
        assert_eq!(
            result,
            ScalarValue::new_primitive::<Date64Type>(
                Some(1609632000000),
                &DataType::Date64
            )?
        );
        Ok(())
    }

    #[test]
    fn test_mode_accumulator_tie_case_date64() -> Result<()> {
        let mut acc = PrimitiveModeAccumulator::<Date64Type>::new(&DataType::Date64);
        let values: ArrayRef = Arc::new(Date64Array::from(vec![
            1609459200000,
            1609545600000,
            1609545600000,
            1609632000000,
            1609632000000,
        ]));
        acc.update_batch(&[values])?;
        let result = acc.evaluate()?;
        assert_eq!(
            result,
            ScalarValue::new_primitive::<Date64Type>(
                Some(1609545600000),
                &DataType::Date64
            )?
        );
        Ok(())
    }

    #[test]
    fn test_mode_accumulator_only_nulls_date64() -> Result<()> {
        let mut acc = PrimitiveModeAccumulator::<Date64Type>::new(&DataType::Date64);
        let values: ArrayRef = Arc::new(Date64Array::from(vec![None, None, None, None]));
        acc.update_batch(&[values])?;
        let result = acc.evaluate()?;
        assert_eq!(
            result,
            ScalarValue::new_primitive::<Date64Type>(None, &DataType::Date64)?
        );
        Ok(())
    }

    #[test]
    fn test_mode_accumulator_single_mode_time64() -> Result<()> {
        let mut acc = PrimitiveModeAccumulator::<Time64MicrosecondType>::new(
            &DataType::Time64(TimeUnit::Microsecond),
        );
        let values: ArrayRef = Arc::new(Time64MicrosecondArray::from(vec![
            3600000000,
            7200000000,
            7200000000,
            10800000000,
            10800000000,
            10800000000,
        ]));
        acc.update_batch(&[values])?;
        let result = acc.evaluate()?;
        assert_eq!(
            result,
            ScalarValue::new_primitive::<Time64MicrosecondType>(
                Some(10800000000),
                &DataType::Time64(TimeUnit::Microsecond)
            )?
        );
        Ok(())
    }

    #[test]
    fn test_mode_accumulator_with_nulls_time64() -> Result<()> {
        let mut acc = PrimitiveModeAccumulator::<Time64MicrosecondType>::new(
            &DataType::Time64(TimeUnit::Microsecond),
        );
        let values: ArrayRef = Arc::new(Time64MicrosecondArray::from(vec![
            None,
            Some(3600000000),
            Some(7200000000),
            Some(7200000000),
            Some(10800000000),
            Some(10800000000),
            Some(10800000000),
        ]));
        acc.update_batch(&[values])?;
        let result = acc.evaluate()?;
        assert_eq!(
            result,
            ScalarValue::new_primitive::<Time64MicrosecondType>(
                Some(10800000000),
                &DataType::Time64(TimeUnit::Microsecond)
            )?
        );
        Ok(())
    }

    #[test]
    fn test_mode_accumulator_tie_case_time64() -> Result<()> {
        let mut acc = PrimitiveModeAccumulator::<Time64MicrosecondType>::new(
            &DataType::Time64(TimeUnit::Microsecond),
        );
        let values: ArrayRef = Arc::new(Time64MicrosecondArray::from(vec![
            3600000000,
            7200000000,
            7200000000,
            10800000000,
            10800000000,
        ]));
        acc.update_batch(&[values])?;
        let result = acc.evaluate()?;
        assert_eq!(
            result,
            ScalarValue::new_primitive::<Time64MicrosecondType>(
                Some(7200000000),
                &DataType::Time64(TimeUnit::Microsecond)
            )?
        );
        Ok(())
    }

    #[test]
    fn test_mode_accumulator_only_nulls_time64() -> Result<()> {
        let mut acc = PrimitiveModeAccumulator::<Time64MicrosecondType>::new(
            &DataType::Time64(TimeUnit::Microsecond),
        );
        let values: ArrayRef =
            Arc::new(Time64MicrosecondArray::from(vec![None, None, None, None]));
        acc.update_batch(&[values])?;
        let result = acc.evaluate()?;
        assert_eq!(
            result,
            ScalarValue::new_primitive::<Time64MicrosecondType>(
                None,
                &DataType::Time64(TimeUnit::Microsecond)
            )?
        );
        Ok(())
    }
}
