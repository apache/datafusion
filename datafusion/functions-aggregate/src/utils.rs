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

use std::sync::Arc;

use arrow::array::{Array, ArrayRef, Float32Array, Float64Array, RecordBatch};
use arrow::compute::{cast, max, min};
use arrow::datatypes::{DataType, Schema};
use datafusion_common::{
    Result, ScalarValue, downcast_value, internal_datafusion_err, internal_err,
    plan_datafusion_err, plan_err,
};
use datafusion_expr::ColumnarValue;
use datafusion_physical_expr_common::physical_expr::PhysicalExpr;

/// Evaluates a physical expression to extract its scalar value.
///
/// This is used to extract constant values from expressions (like percentile parameters)
/// by evaluating them against an empty record batch.
pub(crate) fn get_scalar_value(expr: &Arc<dyn PhysicalExpr>) -> Result<ScalarValue> {
    let empty_schema = Arc::new(Schema::empty());
    let batch = RecordBatch::new_empty(Arc::clone(&empty_schema));
    if let ColumnarValue::Scalar(s) = expr.evaluate(&batch)? {
        Ok(s)
    } else {
        internal_err!("Didn't expect ColumnarValue::Array")
    }
}

/// Validates that a percentile scalar is a Float32/Float64 value between 0.0 and 1.0.
fn scalar_to_percentile(scalar_value: ScalarValue, fn_name: &str) -> Result<f64> {
    let percentile = match scalar_value {
        ScalarValue::Float32(Some(value)) => value as f64,
        ScalarValue::Float64(Some(value)) => value,
        ScalarValue::Float32(None) | ScalarValue::Float64(None) => {
            return plan_err!(
                "Percentile value for '{fn_name}' must be Float32 or Float64 (got null)"
            );
        }
        sv => {
            return plan_err!(
                "Percentile value for '{fn_name}' must be Float32 or Float64 (got data type {})",
                sv.data_type()
            );
        }
    };

    check_percentile_range(percentile)
}

/// Ensures the percentile is between 0 and 1.
fn check_percentile_range(percentile: f64) -> Result<f64> {
    if !(0.0..=1.0).contains(&percentile) {
        return plan_err!(
            "Percentile value must be between 0.0 and 1.0 inclusive, {percentile} is invalid"
        );
    }
    Ok(percentile)
}

/// State of the PercentileParam resolution.
/// Either already resolved or still requiring a non-empty record batch.
#[derive(Debug, Clone)]
pub(crate) enum PercentileParamState {
    Resolved(f64),
    Pending,
}

/// Percentile argument for `aggregate_fn_name` and its state.
#[derive(Debug)]
pub struct PercentileParam {
    pub aggregate_fn_name: String,
    pub(crate) state: PercentileParamState,
    pub(crate) is_desc: bool,
}

impl PercentileParam {
    /// Try to resolve the percentile eagerly. If the expression can't be
    /// evaluated without row data (i.e. it references a column), defer
    /// resolution to the first batch instead of erroring here.
    pub(crate) fn try_new(
        expr: &Arc<dyn PhysicalExpr>,
        fn_name: &str,
        is_desc: bool,
    ) -> Result<Self> {
        match get_scalar_value(expr) {
            Ok(scalar_value) => Ok(PercentileParam {
                aggregate_fn_name: fn_name.to_string(),
                state: PercentileParamState::Resolved(scalar_to_percentile(
                    scalar_value,
                    fn_name,
                )?),
                is_desc,
            }),
            Err(_) => Ok(PercentileParam {
                aggregate_fn_name: fn_name.to_string(),
                state: PercentileParamState::Pending,
                is_desc,
            }),
        }
    }

    /// Resolve using the current batch if `Pending`
    /// and validate that the argument is constant across all batches.
    pub(crate) fn resolve(&mut self, array: &ArrayRef) -> Result<()> {
        if array.null_count() >= array.len() {
            return Ok(());
        }

        let agg_fn_name = self.aggregate_fn_name.clone();
        let (batch_min, batch_max) = match array.data_type() {
            DataType::Float64 => {
                let float_array = downcast_value!(array, Float64Array);
                (min(float_array), max(float_array))
            }
            DataType::Float32 => {
                let float_array = downcast_value!(array, Float32Array);
                (
                    min(float_array).map(|v| v as f64),
                    max(float_array).map(|v| v as f64),
                )
            }
            data_type => {
                return plan_err!(
                    "Percentile value for {agg_fn_name} must be Float32 or Float64 (got {data_type})"
                );
            }
        };
        let batch_min = batch_min.ok_or_else(|| {
            internal_datafusion_err!("expected a non-null percentile value")
        })?;
        let batch_max = batch_max.ok_or_else(|| {
            internal_datafusion_err!("expected a non-null percentile value")
        })?;
        if batch_min != batch_max {
            return plan_err!(
                "Percentile value for '{agg_fn_name}' must be constant across the aggregation, found differing values"
            );
        }

        match self.state {
            PercentileParamState::Resolved(resolved) => {
                if batch_min != resolved {
                    return plan_err!(
                        "Percentile value for '{agg_fn_name}' must be constant across the aggregation, found differing values"
                    );
                }
            }
            PercentileParamState::Pending => {
                let resolved = scalar_to_percentile(
                    ScalarValue::Float64(Some(batch_min)),
                    &agg_fn_name,
                )?;
                self.state = PercentileParamState::Resolved(resolved);
            }
        }

        Ok(())
    }

    /// Returns the resolved percentile.
    /// Errors if it has not been yet resolved.
    pub(crate) fn get(&self) -> Result<f64> {
        match self.state {
            PercentileParamState::Resolved(value) => Ok(value),
            PercentileParamState::Pending => {
                let aggregate_fn_name = self.aggregate_fn_name.clone();
                plan_err!(
                    "Percentile value for '{aggregate_fn_name}' could not be determined: no non-null percentile value was seen"
                )
            }
        }
    }

    /// The percentile to use, applying the `1.0 - p` flip for descending
    /// `WITHIN GROUP (ORDER BY ... DESC)`.
    pub(crate) fn effective_percentile(&self) -> Result<f64> {
        Ok(self.apply_order(self.get()?))
    }

    /// Applies the `1.0 - p` flip for descending `WITHIN GROUP (ORDER BY ... DESC)`.
    pub(crate) fn apply_order(&self, percentile: f64) -> f64 {
        if self.is_desc {
            1.0 - percentile
        } else {
            percentile
        }
    }

    /// Returns `true` if the percentile must be read from the input rows,
    /// i.e. it was given as a column reference rather than a literal.
    pub(crate) fn is_pending(&self) -> bool {
        matches!(self.state, PercentileParamState::Pending)
    }

    /// Casts a percentile argument array to `Float64`.
    pub(crate) fn to_float64_array(&self, array: &ArrayRef) -> Result<Float64Array> {
        match array.data_type() {
            DataType::Float64 | DataType::Float32 => {
                let array = cast(array, &DataType::Float64)?;
                Ok(downcast_value!(array, Float64Array).clone())
            }
            data_type => {
                let agg_fn_name = &self.aggregate_fn_name;
                plan_err!(
                    "Percentile value for {agg_fn_name} must be Float32 or Float64 (got {data_type})"
                )
            }
        }
    }

    /// Validates `value` as the percentile of a single group: it must be in
    /// range and agree with the group's `current` percentile, if any.
    pub(crate) fn resolve_group_value(
        &self,
        current: Option<f64>,
        value: f64,
    ) -> Result<f64> {
        match current {
            Some(current) if current == value => Ok(current),
            Some(_) => {
                let agg_fn_name = &self.aggregate_fn_name;
                plan_err!(
                    "Percentile value for '{agg_fn_name}' must be constant across the aggregation, found differing values"
                )
            }
            None => check_percentile_range(value),
        }
    }

    /// Returns the resolved percentile of a group.
    /// Errors if no percentile was seen for it.
    pub(crate) fn get_group_value(&self, value: Option<f64>) -> Result<f64> {
        value.ok_or_else(|| {
            let aggregate_fn_name = &self.aggregate_fn_name;
            plan_datafusion_err!(
                "Percentile value for '{aggregate_fn_name}' could not be determined: no non-null percentile value was seen"
            )
        })
    }
}
