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

use arrow::array::{Array, MapArray, RecordBatch, StructArray};
use arrow::datatypes::Schema;
use datafusion_common::{
    DataFusionError, Result, ScalarValue, internal_datafusion_err, internal_err, plan_err,
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
        ScalarValue::Float32(None) | ScalarValue::Float64(None) => {
            return plan_err!(
                "Percentile value for '{fn_name}' must be Float32 or Float64 literal (got null)"
            );
        }
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

/// Reads the key/value scalars out of one row of a `MapArray`.
pub(crate) fn map_row_to_scalars(
    map_array: &MapArray,
    row: usize,
) -> Result<(Vec<ScalarValue>, Vec<ScalarValue>)> {
    let entries = map_array.value(row);
    let entries = entries
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| internal_datafusion_err!("map entries must be a StructArray"))?;
    let key_col = entries.column(0);
    let val_col = entries.column(1);

    let mut keys = Vec::with_capacity(key_col.len());
    let mut values = Vec::with_capacity(val_col.len());
    for i in 0..key_col.len() {
        keys.push(ScalarValue::try_from_array(key_col, i)?);
        values.push(ScalarValue::try_from_array(val_col, i)?);
    }
    Ok((keys, values))
}

/// Converts a `StructArray` into per-row scalar tuples: `rows[i][c]` is column
/// `c` of row `i`.
pub(crate) fn struct_to_rows(s: &StructArray) -> Result<Vec<Vec<ScalarValue>>> {
    (0..s.len())
        .map(|i| {
            s.columns()
                .iter()
                .map(|col| ScalarValue::try_from_array(col, i))
                .collect()
        })
        .collect()
}
