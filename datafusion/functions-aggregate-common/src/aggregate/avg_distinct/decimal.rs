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

use arrow::{
    array::{ArrayRef, ArrowNumericType},
    datatypes::{
        Decimal32Type, Decimal64Type, Decimal128Type, Decimal256Type, DecimalType, i256,
    },
};
use datafusion_common::{Result, ScalarValue};
use datafusion_expr_common::accumulator::Accumulator;
use std::fmt::Debug;
use std::mem::size_of_val;

use crate::aggregate::sum_distinct::DistinctSumAccumulator;
use crate::utils::DecimalAverager;

/// Generic implementation of `AVG DISTINCT` for Decimal types.
/// Handles both all Arrow decimal types (32, 64, 128 and 256 bits).
#[derive(Debug)]
pub struct DecimalDistinctAvgAccumulator<T: DecimalType + Debug> {
    sum_accumulator: DistinctSumAccumulator<T>,
    sum_scale: i8,
    target_precision: u8,
    target_scale: i8,
}

impl<T: DecimalType + Debug> DecimalDistinctAvgAccumulator<T> {
    pub fn with_decimal_params(
        sum_scale: i8,
        target_precision: u8,
        target_scale: i8,
    ) -> Self {
        let data_type = T::TYPE_CONSTRUCTOR(T::MAX_PRECISION, sum_scale);

        Self {
            sum_accumulator: DistinctSumAccumulator::new(&data_type),
            sum_scale,
            target_precision,
            target_scale,
        }
    }
}

impl<T: DecimalType + ArrowNumericType + Debug> Accumulator
    for DecimalDistinctAvgAccumulator<T>
{
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        self.sum_accumulator.state()
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        self.sum_accumulator.update_batch(values)
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        self.sum_accumulator.merge_batch(states)
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        if self.sum_accumulator.distinct_count() == 0 {
            return ScalarValue::new_primitive::<T>(
                None,
                &T::TYPE_CONSTRUCTOR(self.target_precision, self.target_scale),
            );
        }

        let sum_scalar = self.sum_accumulator.evaluate()?;

        match sum_scalar {
            ScalarValue::Decimal32(Some(sum), _, _) => {
                let decimal_averager = DecimalAverager::<Decimal32Type>::try_new(
                    self.sum_scale,
                    self.target_precision,
                    self.target_scale,
                )?;
                let avg = decimal_averager
                    .avg(sum, self.sum_accumulator.distinct_count() as i32)?;
                Ok(ScalarValue::Decimal32(
                    Some(avg),
                    self.target_precision,
                    self.target_scale,
                ))
            }
            ScalarValue::Decimal64(Some(sum), _, _) => {
                let decimal_averager = DecimalAverager::<Decimal64Type>::try_new(
                    self.sum_scale,
                    self.target_precision,
                    self.target_scale,
                )?;
                let avg = decimal_averager
                    .avg(sum, self.sum_accumulator.distinct_count() as i64)?;
                Ok(ScalarValue::Decimal64(
                    Some(avg),
                    self.target_precision,
                    self.target_scale,
                ))
            }
            ScalarValue::Decimal128(Some(sum), _, _) => {
                let decimal_averager = DecimalAverager::<Decimal128Type>::try_new(
                    self.sum_scale,
                    self.target_precision,
                    self.target_scale,
                )?;
                let avg = decimal_averager
                    .avg(sum, self.sum_accumulator.distinct_count() as i128)?;
                Ok(ScalarValue::Decimal128(
                    Some(avg),
                    self.target_precision,
                    self.target_scale,
                ))
            }
            ScalarValue::Decimal256(Some(sum), _, _) => {
                let decimal_averager = DecimalAverager::<Decimal256Type>::try_new(
                    self.sum_scale,
                    self.target_precision,
                    self.target_scale,
                )?;
                // `distinct_count` returns `u64`, but `avg` expects `i256`
                // first convert `u64` to `i128`, then convert `i128` to `i256` to avoid overflow
                let distinct_cnt: i128 = self.sum_accumulator.distinct_count() as i128;
                let count: i256 = i256::from_i128(distinct_cnt);
                let avg = decimal_averager.avg(sum, count)?;
                Ok(ScalarValue::Decimal256(
                    Some(avg),
                    self.target_precision,
                    self.target_scale,
                ))
            }

            _ => unreachable!("Unsupported decimal type: {:?}", sum_scalar),
        }
    }

    fn size(&self) -> usize {
        let fixed_size = size_of_val(self);

        // Account for the size of the sum_accumulator with its contained values
        fixed_size + self.sum_accumulator.size()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{
        Decimal32Array, Decimal64Array, Decimal128Array, Decimal256Array,
    };
    use std::sync::Arc;

    #[test]
    fn test_decimal32_distinct_avg_accumulator() -> Result<()> {
        let precision = 5_u8;
        let scale = 2_i8;
        let array = Decimal32Array::from(vec![
            Some(10_00),
            Some(12_50),
            Some(17_50),
            Some(20_00),
            Some(20_00),
            Some(30_00),
            None,
            None,
        ])
        .with_precision_and_scale(precision, scale)?;

        let mut accumulator =
            DecimalDistinctAvgAccumulator::<Decimal32Type>::with_decimal_params(
                scale, 9, 6,
            );
        accumulator.update_batch(&[Arc::new(array)])?;

        let result = accumulator.evaluate()?;
        let expected_result = ScalarValue::Decimal32(Some(18000000), 9, 6);
        assert_eq!(result, expected_result);

        Ok(())
    }

    #[test]
    fn test_decimal64_distinct_avg_accumulator() -> Result<()> {
        let precision = 10_u8;
        let scale = 4_i8;
        let array = Decimal64Array::from(vec![
            Some(100_0000),
            Some(125_0000),
            Some(175_0000),
            Some(200_0000),
            Some(200_0000),
            Some(300_0000),
            None,
            None,
        ])
        .with_precision_and_scale(precision, scale)?;

        let mut accumulator =
            DecimalDistinctAvgAccumulator::<Decimal64Type>::with_decimal_params(
                scale, 14, 8,
            );
        accumulator.update_batch(&[Arc::new(array)])?;

        let result = accumulator.evaluate()?;
        let expected_result = ScalarValue::Decimal64(Some(180_00000000), 14, 8);
        assert_eq!(result, expected_result);

        Ok(())
    }

    #[test]
    fn test_decimal128_distinct_avg_accumulator() -> Result<()> {
        let precision = 10_u8;
        let scale = 4_i8;
        let array = Decimal128Array::from(vec![
            Some(100_0000),
            Some(125_0000),
            Some(175_0000),
            Some(200_0000),
            Some(200_0000),
            Some(300_0000),
            None,
            None,
        ])
        .with_precision_and_scale(precision, scale)?;

        let mut accumulator =
            DecimalDistinctAvgAccumulator::<Decimal128Type>::with_decimal_params(
                scale, 14, 8,
            );
        accumulator.update_batch(&[Arc::new(array)])?;

        let result = accumulator.evaluate()?;
        let expected_result = ScalarValue::Decimal128(Some(180_00000000), 14, 8);
        assert_eq!(result, expected_result);

        Ok(())
    }

    #[test]
    fn test_decimal256_distinct_avg_accumulator() -> Result<()> {
        let precision = 50_u8;
        let scale = 2_i8;

        let array = Decimal256Array::from(vec![
            Some(i256::from_i128(10_000)),
            Some(i256::from_i128(12_500)),
            Some(i256::from_i128(17_500)),
            Some(i256::from_i128(20_000)),
            Some(i256::from_i128(20_000)),
            Some(i256::from_i128(30_000)),
            None,
            None,
        ])
        .with_precision_and_scale(precision, scale)?;

        let mut accumulator =
            DecimalDistinctAvgAccumulator::<Decimal256Type>::with_decimal_params(
                scale, 54, 6,
            );
        accumulator.update_batch(&[Arc::new(array)])?;

        let result = accumulator.evaluate()?;
        let expected_result =
            ScalarValue::Decimal256(Some(i256::from_i128(180_000000)), 54, 6);
        assert_eq!(result, expected_result);

        Ok(())
    }
}
