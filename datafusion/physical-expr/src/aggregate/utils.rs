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

pub fn calculate_result_decimal_for_avg(
    lit_value: i128,
    count: i128,
    scale: i8,
    target_type: &DataType,
) -> Result<ScalarValue> {
    match target_type {
        DataType::Decimal128(p, s) => {
            // Different precision for decimal128 can store different range of value.
            // For example, the precision is 3, the max of value is `999` and the min
            // value is `-999`
            let (target_mul, target_min, target_max) = (
                10_i128.pow(*s as u32),
                MIN_DECIMAL_FOR_EACH_PRECISION[*p as usize - 1],
                MAX_DECIMAL_FOR_EACH_PRECISION[*p as usize - 1],
            );
            let lit_scale_mul = 10_i128.pow(scale as u32);
            if target_mul >= lit_scale_mul {
                if let Some(value) = lit_value.checked_mul(target_mul / lit_scale_mul) {
                    let new_value = value / count;
                    if new_value >= target_min && new_value <= target_max {
                        Ok(ScalarValue::Decimal128(Some(new_value), *p, *s))
                    } else {
                        Err(DataFusionError::Internal(
                            "Arithmetic Overflow in AvgAccumulator".to_string(),
                        ))
                    }
                } else {
                    // can't convert the lit decimal to the returned data type
                    Err(DataFusionError::Internal(
                        "Arithmetic Overflow in AvgAccumulator".to_string(),
                    ))
                }
            } else {
                // can't convert the lit decimal to the returned data type
                Err(DataFusionError::Internal(
                    "Arithmetic Overflow in AvgAccumulator".to_string(),
                ))
            }
        }
        other => Err(DataFusionError::Internal(format!(
            "Error returned data type in AvgAccumulator {other:?}"
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

pub(crate) fn ordering_fields(
    ordering_req: &[PhysicalSortExpr],
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
