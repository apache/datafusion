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
    array::{ArrayRef, ArrowNativeTypeOp, ArrowNumericType},
    compute::DecimalCast,
    datatypes::{ArrowNativeType, DecimalType},
};
use datafusion_common::{Result, ScalarValue, exec_datafusion_err, exec_err};
use datafusion_expr_common::accumulator::Accumulator;
use std::fmt::Debug;
use std::marker::PhantomData;
use std::mem::size_of_val;

use crate::aggregate::sum_distinct::DistinctSumAccumulator;
use crate::utils::DecimalAverager;

/// Generic implementation of `AVG DISTINCT` for Decimal types.
/// Handles both all Arrow decimal types (32, 64, 128 and 256 bits).
///
/// The distinct values are stored in the input type `I`; only the intermediate
/// sum is computed in the (never narrower) sum type `S` so it cannot overflow
/// `I`'s native type.
#[derive(Debug)]
pub struct DecimalDistinctAvgAccumulator<
    I: DecimalType + Debug,
    S: DecimalType + Debug = I,
> {
    sum_accumulator: DistinctSumAccumulator<I>,
    sum_scale: i8,
    target_precision: u8,
    target_scale: i8,
    _sum_type: PhantomData<S>,
}

impl<I: DecimalType + Debug, S: DecimalType + Debug> DecimalDistinctAvgAccumulator<I, S> {
    pub fn with_decimal_params(
        sum_scale: i8,
        target_precision: u8,
        target_scale: i8,
    ) -> Self {
        let data_type = I::TYPE_CONSTRUCTOR(I::MAX_PRECISION, sum_scale);

        Self {
            sum_accumulator: DistinctSumAccumulator::new(&data_type),
            sum_scale,
            target_precision,
            target_scale,
            _sum_type: PhantomData,
        }
    }
}

/// Adds a distinct input value to AVG's widened intermediate sum.
/// Wrapping is intentional because the caller selects `S` with the same
/// `avg_sum_data_type` headroom contract as the non-distinct AVG path.
#[inline]
fn add_avg_distinct_sum<I, S>(sum: S::Native, value: I::Native) -> S::Native
where
    I: ArrowNumericType,
    S: ArrowNumericType,
    I::Native: Into<S::Native>,
{
    sum.add_wrapping(value.into())
}

impl<I, S> Accumulator for DecimalDistinctAvgAccumulator<I, S>
where
    I: DecimalType + ArrowNumericType + Debug,
    S: DecimalType + ArrowNumericType + Debug,
    I::Native: Into<S::Native> + DecimalCast,
    S::Native: DecimalCast,
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
        let out_type = I::TYPE_CONSTRUCTOR(self.target_precision, self.target_scale);
        let count = self.sum_accumulator.distinct_count();
        if count == 0 {
            return ScalarValue::new_primitive::<I>(None, &out_type);
        }

        // Sum the distinct input values in the wider `S` so the total cannot
        // overflow the input's native width (mirrors the non-distinct path).
        let mut sum = S::Native::usize_as(0);
        for value in self.sum_accumulator.distinct_values() {
            sum = add_avg_distinct_sum::<I, S>(sum, value);
        }

        let Some(count) = S::Native::from_usize(count) else {
            return exec_err!(
                "Arithmetic overflow in avg: the distinct count {count} cannot \
                 be represented in the sum type"
            );
        };

        let averager = DecimalAverager::<S>::try_new(
            self.sum_scale,
            self.target_precision,
            self.target_scale,
        )?;
        // Narrowing the average back to the (never wider) output type cannot
        // fail in practice: `DecimalAverager::avg` validates the average
        // against the output precision, whose bound fits the output's native
        // type by construction
        let avg =
            I::Native::from_decimal(averager.avg(sum, count)?).ok_or_else(|| {
                exec_datafusion_err!(
                    "Arithmetic overflow in avg: the computed average does not fit \
                 the output type"
                )
            })?;
        ScalarValue::new_primitive::<I>(Some(avg), &out_type)
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
    use arrow::datatypes::{
        Decimal32Type, Decimal64Type, Decimal128Type, Decimal256Type, i256,
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

    // The overflow regression tests below use odd-count ranges symmetric
    // around a center value, so the exact sum is `count * center` and the
    // average is exactly `center`.

    #[test]
    fn test_decimal32_distinct_avg_widens_to_decimal64() -> Result<()> {
        // 42951 distinct values centered on 50000:
        // sum = 42951 * 50000 = 2,147,550,000 > i32::MAX
        let array = Decimal32Array::from_iter_values(28525..=71475)
            .with_precision_and_scale(5, 0)?;

        let mut accumulator = DecimalDistinctAvgAccumulator::<
            Decimal32Type,
            Decimal64Type,
        >::with_decimal_params(0, 9, 4);
        accumulator.update_batch(&[Arc::new(array)])?;

        assert_eq!(
            accumulator.evaluate()?,
            ScalarValue::Decimal32(Some(500_000_000), 9, 4)
        );

        Ok(())
    }

    #[test]
    fn test_decimal32_distinct_avg_widens_to_decimal128() -> Result<()> {
        // 21477 distinct values centered on 99999:
        // sum = 21477 * 99999 = 2,147,678,523 > i32::MAX
        let array = Decimal32Array::from_iter_values(89261..=110737)
            .with_precision_and_scale(9, 0)?;

        let mut accumulator = DecimalDistinctAvgAccumulator::<
            Decimal32Type,
            Decimal128Type,
        >::with_decimal_params(0, 9, 4);
        accumulator.update_batch(&[Arc::new(array)])?;

        assert_eq!(
            accumulator.evaluate()?,
            ScalarValue::Decimal32(Some(999_990_000), 9, 4)
        );

        Ok(())
    }

    #[test]
    fn test_decimal64_distinct_avg_widens_to_decimal128() -> Result<()> {
        // 92235 distinct values centered on 10^14 - 1:
        // sum = 92235 * (10^14 - 1) ~= 9.22e18 > i64::MAX
        let center: i64 = 100_000_000_000_000 - 1;
        let array = Decimal64Array::from_iter_values(center - 46117..=center + 46117)
            .with_precision_and_scale(18, 0)?;

        let mut accumulator = DecimalDistinctAvgAccumulator::<
            Decimal64Type,
            Decimal128Type,
        >::with_decimal_params(0, 18, 4);
        accumulator.update_batch(&[Arc::new(array)])?;

        assert_eq!(
            accumulator.evaluate()?,
            ScalarValue::Decimal64(Some(999_999_999_999_990_000), 18, 4)
        );

        Ok(())
    }

    #[test]
    fn test_decimal128_distinct_avg_widens_to_decimal256() -> Result<()> {
        // 21477 distinct values ending at 10^34 - 1, centered on 10^34 - 10739:
        // sum = 21477 * (10^34 - 10739) ~= 2.15e38 > i128::MAX
        let center: i128 = 10_i128.pow(34) - 10739;
        let array = Decimal128Array::from_iter_values(center - 10738..=center + 10738)
            .with_precision_and_scale(34, 0)?;

        let mut accumulator = DecimalDistinctAvgAccumulator::<
            Decimal128Type,
            Decimal256Type,
        >::with_decimal_params(0, 38, 4);
        accumulator.update_batch(&[Arc::new(array)])?;

        assert_eq!(
            accumulator.evaluate()?,
            ScalarValue::Decimal128(Some(center * 10_000), 38, 4)
        );

        Ok(())
    }
}
