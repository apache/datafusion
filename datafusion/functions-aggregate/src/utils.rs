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

use arrow::array::RecordBatch;
use arrow::datatypes::Schema;
use datafusion_common::{DataFusionError, Result, ScalarValue, internal_err, plan_err};
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

/// Validates that a percentile expression is a literal float value between 0.0 and 1.0.
///
/// Used by both `percentile_cont` and `approx_percentile_cont` to validate their
/// percentile parameters.
pub(crate) fn validate_percentile_expr(
    expr: &Arc<dyn PhysicalExpr>,
    fn_name: &str,
) -> Result<f64> {
    let scalar_value = get_scalar_value(expr).map_err(|_e| {
        DataFusionError::Plan(format!(
            "Percentile value for '{fn_name}' must be a literal"
        ))
    })?;

    let percentile = match scalar_value {
        ScalarValue::Float32(Some(value)) => value as f64,
        ScalarValue::Float64(Some(value)) => value,
        sv => {
            return plan_err!(
                "Percentile value for '{fn_name}' must be Float32 or Float64 literal (got data type {})",
                sv.data_type()
            );
        }
    };

    // Ensure the percentile is between 0 and 1.
    if !(0.0..=1.0).contains(&percentile) {
        return plan_err!(
            "Percentile value must be between 0.0 and 1.0 inclusive, {percentile} is invalid"
        );
    }
    Ok(percentile)
}
