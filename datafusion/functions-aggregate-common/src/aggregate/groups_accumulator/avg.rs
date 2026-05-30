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

//! Shared helpers for average group accumulator state handling.

use arrow::array::{ArrowNumericType, BooleanArray, PrimitiveArray};

use super::nulls::{filtered_null_mask, set_nulls};

/// Converts an AVG input value array into nullable per-row state arrays.
///
/// The returned arrays are `(sum_state, count_state)`. Callers keep control of
/// their aggregate-specific state field order when wrapping these arrays in the
/// final state vector.
///
/// Rows with NULL input values, `false` filters, or NULL filters are marked NULL
/// in both output arrays so later merge steps can ignore them consistently.
pub fn convert_to_avg_state<SumType, CountType>(
    sums: PrimitiveArray<SumType>,
    count_value: CountType::Native,
    opt_filter: Option<&BooleanArray>,
) -> (PrimitiveArray<SumType>, PrimitiveArray<CountType>)
where
    SumType: ArrowNumericType + Send,
    CountType: ArrowNumericType + Send,
{
    let counts = PrimitiveArray::<CountType>::from_value(count_value, sums.len());
    let nulls = filtered_null_mask(opt_filter, &sums);
    let counts = set_nulls(counts, nulls.clone());
    let sums = set_nulls(sums, nulls);

    (sums, counts)
}

#[cfg(test)]
mod tests {
    use arrow::array::{Array, BooleanArray, Float64Array};
    use arrow::datatypes::{DataType, Float64Type, Int64Type};

    use super::convert_to_avg_state;

    type CountType = Int64Type;

    fn assert_validity(array: &dyn Array, expected: &[bool]) {
        assert_eq!(array.len(), expected.len());
        for (idx, expected_valid) in expected.iter().copied().enumerate() {
            assert_eq!(!array.is_null(idx), expected_valid, "validity at row {idx}");
        }
    }

    #[test]
    fn convert_to_avg_state_applies_input_nulls_to_sum_and_count() {
        let sums = Float64Array::from(vec![Some(1.0), None, Some(3.0)]);

        let (sums, counts) =
            convert_to_avg_state::<Float64Type, CountType>(sums, 1, None);

        assert_validity(&sums, &[true, false, true]);
        assert_validity(&counts, &[true, false, true]);
        assert_eq!(counts.values().as_ref(), &[1, 1, 1]);
    }

    #[test]
    fn convert_to_avg_state_applies_filter_nulls_to_sum_and_count() {
        let sums = Float64Array::from(vec![Some(1.0), Some(2.0), Some(3.0), Some(4.0)]);
        let filter = BooleanArray::from(vec![Some(true), Some(false), None, Some(true)]);

        let (sums, counts) =
            convert_to_avg_state::<Float64Type, CountType>(sums, 1, Some(&filter));

        assert_eq!(sums.null_count(), 2);
        assert_validity(&sums, &[true, false, false, true]);

        assert_eq!(counts.null_count(), 2);
        assert_validity(&counts, &[true, false, false, true]);
        assert_eq!(counts.values().as_ref(), &[1, 1, 1, 1]);
    }

    #[test]
    fn convert_to_avg_state_preserves_sum_data_type() {
        let sums = Float64Array::from(vec![1.0, 2.0]).with_data_type(DataType::Float64);

        let (sums, _counts) =
            convert_to_avg_state::<Float64Type, CountType>(sums, 1, None);

        assert_eq!(sums.data_type(), &DataType::Float64);
    }
}
