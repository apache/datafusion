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

//! # Continuous Percentile

use crate::aggregate::approx_percentile_cont::validate_input_percentile_expr;
use crate::aggregate::utils::down_cast_any_ref;
use crate::expressions::format_state_name;
use crate::{AggregateExpr, PhysicalExpr};
use arrow::array::{Array, ArrayRef};
use arrow::compute::sort_to_indices;
use arrow::datatypes::{DataType, Field};
use arrow_array::UInt32Array;
use bigdecimal::{BigDecimal, FromPrimitive, ToPrimitive};
use datafusion_common::{DataFusionError, Result, ScalarValue};
use datafusion_expr::Accumulator;
use std::any::Any;
use std::sync::Arc;

/// PERCENTILE_CONT aggregate expression. This uses a lot of memory because all values need to be
/// stored in memory before a result can be computed. If an approximation is sufficient
/// then APPROX_PERCENTILE_CONT provides a much more efficient solution.
#[derive(Debug)]
pub struct PercentileCont {
    name: String,
    expr: Vec<Arc<dyn PhysicalExpr>>,
    percentile: f64,
    data_type: DataType,
}

impl PercentileCont {
    /// Create a new PERCENTILE_CONT aggregate function
    pub fn try_new(
        expr: Vec<Arc<dyn PhysicalExpr>>,
        name: impl Into<String>,
        data_type: DataType,
    ) -> Result<Self> {
        // Arguments should be [ColumnExpr, DesiredPercentileLiteral]
        debug_assert_eq!(expr.len(), 2);

        let percentile = validate_input_percentile_expr(&expr[1])?;
        Ok(Self {
            name: name.into(),
            expr,
            percentile,
            data_type,
        })
    }
}

impl AggregateExpr for PercentileCont {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn field(&self) -> Result<Field> {
        Ok(Field::new(&self.name, self.data_type.clone(), true))
    }

    fn create_accumulator(&self) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(PercentileContAccumulator {
            data_type: self.data_type.clone(),
            all_values: vec![],
            percentile: self.percentile,
        }))
    }

    fn state_fields(&self) -> Result<Vec<Field>> {
        //Intermediate state is a list of the elements we have collected so far
        let field = Field::new("item", self.data_type.clone(), true);
        let data_type = DataType::List(Arc::new(field));

        Ok(vec![Field::new(
            format_state_name(&self.name, "percentile_cont"),
            data_type,
            true,
        )])
    }

    fn expressions(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        self.expr.clone()
    }

    fn name(&self) -> &str {
        &self.name
    }
}

impl PartialEq<dyn Any> for PercentileCont {
    fn eq(&self, other: &dyn Any) -> bool {
        down_cast_any_ref(other)
            .downcast_ref::<Self>()
            .map(|x| {
                self.name == x.name
                    && self.percentile == x.percentile
                    && self.data_type == x.data_type
                    && self
                        .expr
                        .iter()
                        .zip(x.expr.iter())
                        .all(|(this, other)| this.eq(other))
            })
            .unwrap_or(false)
    }
}

#[derive(Debug)]
/// The percentile_cont accumulator accumulates the raw input values
/// as `ScalarValue`s
///
/// The intermediate state is represented as a List of those scalars
pub(crate) struct PercentileContAccumulator {
    pub data_type: DataType,
    pub all_values: Vec<ScalarValue>,
    pub percentile: f64,
}

impl Accumulator for PercentileContAccumulator {
    fn state(&self) -> Result<Vec<ScalarValue>> {
        let state =
            ScalarValue::new_list(Some(self.all_values.clone()), self.data_type.clone());
        Ok(vec![state])
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        let array = &values[0];

        assert_eq!(array.data_type(), &self.data_type);
        self.all_values.reserve(array.len());
        for index in 0..array.len() {
            self.all_values
                .push(ScalarValue::try_from_array(array, index)?);
        }

        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        assert_eq!(states.len(), 1);

        let array = &states[0];
        assert!(matches!(array.data_type(), DataType::List(_)));
        for index in 0..array.len() {
            match ScalarValue::try_from_array(array, index)? {
                ScalarValue::List(Some(mut values), _) => {
                    self.all_values.append(&mut values);
                }
                ScalarValue::List(None, _) => {} // skip empty state
                v => {
                    return Err(DataFusionError::Internal(format!(
                        "unexpected state in percentile_cont. Expected DataType::List, got {v:?}"
                    )))
                }
            }
        }
        Ok(())
    }

    fn evaluate(&self) -> Result<ScalarValue> {
        if !self.all_values.iter().any(|v| !v.is_null()) {
            return ScalarValue::try_from(&self.data_type);
        }

        // Create an array of all the non null values and find the
        // sorted indexes
        let array = ScalarValue::iter_to_array(
            self.all_values
                .iter()
                // ignore null values
                .filter(|v| !v.is_null())
                .cloned(),
        )?;

        // find the mid point
        let len = array.len();
        let r = (len - 1) as f64 * self.percentile;

        let limit = Some(r.ceil() as usize + 1);
        let options = None;
        let indices = sort_to_indices(&array, options, limit)?;

        let r_lower = r.floor() as usize;
        let r_upper = r.ceil() as usize;

        let result = if r_lower == r_upper {
            // Exact value found, pick that one
            scalar_at_index(&array, &indices, r_lower)?
        } else {
            // Interpolate between upper and lower values
            let s_lower = scalar_at_index(&array, &indices, r_lower)?;
            let s_upper = scalar_at_index(&array, &indices, r_upper)?;

            // Convert lower/upper values and percentile to BigDecimal
            let big_lower = scalar_to_bigdecimal(&s_lower);
            let big_upper = scalar_to_bigdecimal(&s_upper);
            let big_percentile = BigDecimal::from_f64(self.percentile);

            if let (Some(big_s1), Some(big_s2), Some(big_percentile)) =
                (big_lower, big_upper, big_percentile)
            {
                // Perform percentile calculation with BigDecimal values to preserve precision
                let big_result = big_s1.clone() + (big_s2 - big_s1) * big_percentile;

                // Convert result back to column type
                match array.data_type() {
                    DataType::Int8 => ScalarValue::Int8(big_result.to_i8()),
                    DataType::Int16 => ScalarValue::Int16(big_result.to_i16()),
                    DataType::Int32 => ScalarValue::Int32(big_result.to_i32()),
                    DataType::Int64 => ScalarValue::Int64(big_result.to_i64()),
                    DataType::UInt8 => ScalarValue::UInt8(big_result.to_u8()),
                    DataType::UInt16 => ScalarValue::UInt16(big_result.to_u16()),
                    DataType::UInt32 => ScalarValue::UInt32(big_result.to_u32()),
                    DataType::UInt64 => ScalarValue::UInt64(big_result.to_u64()),
                    DataType::Float16 => ScalarValue::Float32(big_result.to_f32()),
                    DataType::Float32 => ScalarValue::Float32(big_result.to_f32()),
                    DataType::Float64 => ScalarValue::Float64(big_result.to_f64()),
                    _ => ScalarValue::try_from(array.data_type())?,
                }
            } else if scalar_is_non_finite(&s_lower) || scalar_is_non_finite(&s_upper) {
                // If upper or lower value is a non-finite float, use NaN instead of NULL
                match array.data_type() {
                    DataType::Float32 => ScalarValue::Float32(Some(f32::NAN)),
                    DataType::Float64 => ScalarValue::Float64(Some(f64::NAN)),
                    _ => ScalarValue::try_from(array.data_type())?,
                }
            } else {
                // Otherwise, NULL
                ScalarValue::try_from(array.data_type())?
            }
        };

        Ok(result)
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self) + ScalarValue::size_of_vec(&self.all_values)
            - std::mem::size_of_val(&self.all_values)
            + self.data_type.size()
            - std::mem::size_of_val(&self.data_type)
    }
}

fn scalar_to_bigdecimal(v: &ScalarValue) -> Option<BigDecimal> {
    match v {
        ScalarValue::Int8(Some(v)) => Some(BigDecimal::from(*v)),
        ScalarValue::Int16(Some(v)) => Some(BigDecimal::from(*v)),
        ScalarValue::Int32(Some(v)) => Some(BigDecimal::from(*v)),
        ScalarValue::Int64(Some(v)) => Some(BigDecimal::from(*v)),
        ScalarValue::UInt8(Some(v)) => Some(BigDecimal::from(*v)),
        ScalarValue::UInt16(Some(v)) => Some(BigDecimal::from(*v)),
        ScalarValue::UInt32(Some(v)) => Some(BigDecimal::from(*v)),
        ScalarValue::UInt64(Some(v)) => Some(BigDecimal::from(*v)),
        ScalarValue::Float32(Some(v)) => BigDecimal::from_f32(*v),
        ScalarValue::Float64(Some(v)) => BigDecimal::from_f64(*v),
        _ => None,
    }
}

fn scalar_is_non_finite(v: &ScalarValue) -> bool {
    match v {
        ScalarValue::Float32(Some(v)) => !v.is_finite(),
        ScalarValue::Float64(Some(v)) => !v.is_finite(),
        _ => false,
    }
}

/// Given a returns `array[indicies[indicie_index]]` as a `ScalarValue`
fn scalar_at_index(
    array: &dyn Array,
    indices: &UInt32Array,
    indicies_index: usize,
) -> Result<ScalarValue> {
    let array_index = indices
        .value(indicies_index)
        .try_into()
        .expect("Convert uint32 to usize");
    ScalarValue::try_from_array(array, array_index)
}
