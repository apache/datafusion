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

//! Utilities used in aggregates

use crate::{AggregateExpr, PhysicalSortExpr};
use arrow::array::ArrayRef;
use arrow::datatypes::{MAX_DECIMAL_FOR_EACH_PRECISION, MIN_DECIMAL_FOR_EACH_PRECISION};
use arrow_schema::{DataType, Field};
use datafusion_common::{DataFusionError, Result, ScalarValue};
use datafusion_expr::Accumulator;
use std::any::Any;
use std::sync::Arc;

/// Convert scalar values from an accumulator into arrays.
pub fn get_accum_scalar_values_as_arrays(
    accum: &dyn Accumulator,
) -> Result<Vec<ArrayRef>> {
    Ok(accum
        .state()?
        .iter()
        .map(|s| s.to_array_of_size(1))
        .collect::<Vec<_>>())
}

/// Computes averages for `Decimal128` values, checking for overflow
///
/// This is needed because different precisions for Decimal128 can
/// store different ranges of values and thus sum/count may not fit in
/// the target type.
///
/// For example, the precision is 3, the max of value is `999` and the min
/// value is `-999`
pub(crate) struct Decimal128Averager {
    /// scale factor for sum values (10^sum_scale)
    sum_mul: i128,
    /// scale factor for target (10^target_scale)
    target_mul: i128,
    /// The minimum output value possible to represent with the target precision
    target_min: i128,
    /// The maximum output value possible to represent with the target precision
    target_max: i128,
}

impl Decimal128Averager {
    /// Create a new `Decimal128Averager`:
    ///
    /// * sum_scale: the scale of `sum` values passed to [`Self::avg`]
    /// * target_precision: the output precision
    /// * target_precision: the output scale
    ///
    /// Errors if the resulting data can not be stored
    pub fn try_new(
        sum_scale: i8,
        target_precision: u8,
        target_scale: i8,
    ) -> Result<Self> {
        let sum_mul = 10_i128.pow(sum_scale as u32);
        let target_mul = 10_i128.pow(target_scale as u32);
        let target_min = MIN_DECIMAL_FOR_EACH_PRECISION[target_precision as usize - 1];
        let target_max = MAX_DECIMAL_FOR_EACH_PRECISION[target_precision as usize - 1];

        if target_mul >= sum_mul {
            Ok(Self {
                sum_mul,
                target_mul,
                target_min,
                target_max,
            })
        } else {
            // can't convert the lit decimal to the returned data type
            Err(DataFusionError::Execution(
                "Arithmetic Overflow in AvgAccumulator".to_string(),
            ))
        }
    }

    /// Returns the `sum`/`count` as a i128 Decimal128 with
    /// target_scale and target_precision and reporting overflow.
    ///
    /// * sum: The total sum value stored as Decimal128 with sum_scale
    /// (passed to `Self::try_new`)
    /// * count: total count, stored as a i128 (*NOT* a Decimal128 value)
    #[inline(always)]
    pub fn avg(&self, sum: i128, count: i128) -> Result<i128> {
        if let Some(value) = sum.checked_mul(self.target_mul / self.sum_mul) {
            let new_value = value / count;
            if new_value >= self.target_min && new_value <= self.target_max {
                Ok(new_value)
            } else {
                Err(DataFusionError::Execution(
                    "Arithmetic Overflow in AvgAccumulator".to_string(),
                ))
            }
        } else {
            // can't convert the lit decimal to the returned data type
            Err(DataFusionError::Execution(
                "Arithmetic Overflow in AvgAccumulator".to_string(),
            ))
        }
    }
}

/// Returns `sum`/`count` for decimal values, detecting and reporting overflow.
///
/// * sum:  stored as Decimal128 with `sum_scale` scale
/// * count: stored as a i128 (*NOT* a Decimal128 value)
/// * sum_scale: the scale of `sum`
/// * target_type: the output decimal type
pub fn calculate_result_decimal_for_avg(
    sum: i128,
    count: i128,
    sum_scale: i8,
    target_type: &DataType,
) -> Result<ScalarValue> {
    match target_type {
        DataType::Decimal128(target_precision, target_scale) => {
            let new_value =
                Decimal128Averager::try_new(sum_scale, *target_precision, *target_scale)?
                    .avg(sum, count)?;

            Ok(ScalarValue::Decimal128(
                Some(new_value),
                *target_precision,
                *target_scale,
            ))
        }
        other => Err(DataFusionError::Internal(format!(
            "Invalid target type in AvgAccumulator {other:?}"
        ))),
    }
}

/// Downcast a `Box<dyn AggregateExpr>` or `Arc<dyn AggregateExpr>`
/// and return the inner trait object as [`Any`](std::any::Any) so
/// that it can be downcast to a specific implementation.
///
/// This method is used when implementing the `PartialEq<dyn Any>`
/// for [`AggregateExpr`] aggregation expressions and allows comparing the equality
/// between the trait objects.
pub fn down_cast_any_ref(any: &dyn Any) -> &dyn Any {
    if any.is::<Arc<dyn AggregateExpr>>() {
        any.downcast_ref::<Arc<dyn AggregateExpr>>()
            .unwrap()
            .as_any()
    } else if any.is::<Box<dyn AggregateExpr>>() {
        any.downcast_ref::<Box<dyn AggregateExpr>>()
            .unwrap()
            .as_any()
    } else {
        any
    }
}

/// Construct corresponding fields for lexicographical ordering requirement expression
pub(crate) fn ordering_fields(
    ordering_req: &[PhysicalSortExpr],
    // Data type of each expression in the ordering requirement
    data_types: &[DataType],
) -> Vec<Field> {
    ordering_req
        .iter()
        .zip(data_types.iter())
        .map(|(expr, dtype)| {
            Field::new(
                expr.to_string().as_str(),
                dtype.clone(),
                // Multi partitions may be empty hence field should be nullable.
                true,
            )
        })
        .collect()
}
