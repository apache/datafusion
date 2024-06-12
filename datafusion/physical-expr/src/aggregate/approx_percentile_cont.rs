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

use std::{any::Any, sync::Arc};

use arrow::datatypes::{DataType, Field};
use arrow_array::RecordBatch;
use arrow_schema::Schema;

use datafusion_common::{not_impl_err, plan_err, DataFusionError, Result, ScalarValue};
use datafusion_expr::{Accumulator, ColumnarValue};
use datafusion_functions_aggregate::approx_percentile_cont::ApproxPercentileAccumulator;

use crate::aggregate::utils::down_cast_any_ref;
use crate::expressions::format_state_name;
use crate::{AggregateExpr, PhysicalExpr};

/// APPROX_PERCENTILE_CONT aggregate expression
#[derive(Debug)]
pub struct ApproxPercentileCont {
    name: String,
    input_data_type: DataType,
    expr: Vec<Arc<dyn PhysicalExpr>>,
    percentile: f64,
    tdigest_max_size: Option<usize>,
}

impl ApproxPercentileCont {
    /// Create a new [`ApproxPercentileCont`] aggregate function.
    pub fn new(
        expr: Vec<Arc<dyn PhysicalExpr>>,
        name: impl Into<String>,
        input_data_type: DataType,
    ) -> Result<Self> {
        // Arguments should be [ColumnExpr, DesiredPercentileLiteral]
        debug_assert_eq!(expr.len(), 2);

        let percentile = validate_input_percentile_expr(&expr[1])?;

        Ok(Self {
            name: name.into(),
            input_data_type,
            // The physical expr to evaluate during accumulation
            expr,
            percentile,
            tdigest_max_size: None,
        })
    }

    /// Create a new [`ApproxPercentileCont`] aggregate function.
    pub fn new_with_max_size(
        expr: Vec<Arc<dyn PhysicalExpr>>,
        name: impl Into<String>,
        input_data_type: DataType,
    ) -> Result<Self> {
        // Arguments should be [ColumnExpr, DesiredPercentileLiteral, TDigestMaxSize]
        debug_assert_eq!(expr.len(), 3);
        let percentile = validate_input_percentile_expr(&expr[1])?;
        let max_size = validate_input_max_size_expr(&expr[2])?;
        Ok(Self {
            name: name.into(),
            input_data_type,
            // The physical expr to evaluate during accumulation
            expr,
            percentile,
            tdigest_max_size: Some(max_size),
        })
    }

    pub(crate) fn create_plain_accumulator(&self) -> Result<ApproxPercentileAccumulator> {
        let accumulator: ApproxPercentileAccumulator = match &self.input_data_type {
            t @ (DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::Float32
            | DataType::Float64) => {
                if let Some(max_size) = self.tdigest_max_size {
                    ApproxPercentileAccumulator::new_with_max_size(self.percentile, t.clone(), max_size)

                }else{
                    ApproxPercentileAccumulator::new(self.percentile, t.clone())

                }
            }
            other => {
                return not_impl_err!(
                    "Support for 'APPROX_PERCENTILE_CONT' for data type {other} is not implemented"
                )
            }
        };
        Ok(accumulator)
    }
}

impl PartialEq for ApproxPercentileCont {
    fn eq(&self, other: &ApproxPercentileCont) -> bool {
        self.name == other.name
            && self.input_data_type == other.input_data_type
            && self.percentile == other.percentile
            && self.tdigest_max_size == other.tdigest_max_size
            && self.expr.len() == other.expr.len()
            && self
                .expr
                .iter()
                .zip(other.expr.iter())
                .all(|(this, other)| this.eq(other))
    }
}

fn get_lit_value(expr: &Arc<dyn PhysicalExpr>) -> Result<ScalarValue> {
    let empty_schema = Schema::empty();
    let empty_batch = RecordBatch::new_empty(Arc::new(empty_schema));
    let result = expr.evaluate(&empty_batch)?;
    match result {
        ColumnarValue::Array(_) => Err(DataFusionError::Internal(format!(
            "The expr {:?} can't be evaluated to scalar value",
            expr
        ))),
        ColumnarValue::Scalar(scalar_value) => Ok(scalar_value),
    }
}

fn validate_input_percentile_expr(expr: &Arc<dyn PhysicalExpr>) -> Result<f64> {
    let lit = get_lit_value(expr)?;
    let percentile = match &lit {
        ScalarValue::Float32(Some(q)) => *q as f64,
        ScalarValue::Float64(Some(q)) => *q,
        got => return not_impl_err!(
            "Percentile value for 'APPROX_PERCENTILE_CONT' must be Float32 or Float64 literal (got data type {})",
            got.data_type()
        )
    };

    // Ensure the percentile is between 0 and 1.
    if !(0.0..=1.0).contains(&percentile) {
        return plan_err!(
            "Percentile value must be between 0.0 and 1.0 inclusive, {percentile} is invalid"
        );
    }
    Ok(percentile)
}

fn validate_input_max_size_expr(expr: &Arc<dyn PhysicalExpr>) -> Result<usize> {
    let lit = get_lit_value(expr)?;
    let max_size = match &lit {
        ScalarValue::UInt8(Some(q)) => *q as usize,
        ScalarValue::UInt16(Some(q)) => *q as usize,
        ScalarValue::UInt32(Some(q)) => *q as usize,
        ScalarValue::UInt64(Some(q)) => *q as usize,
        ScalarValue::Int32(Some(q)) if *q > 0 => *q as usize,
        ScalarValue::Int64(Some(q)) if *q > 0 => *q as usize,
        ScalarValue::Int16(Some(q)) if *q > 0 => *q as usize,
        ScalarValue::Int8(Some(q)) if *q > 0 => *q as usize,
        got => return not_impl_err!(
            "Tdigest max_size value for 'APPROX_PERCENTILE_CONT' must be UInt > 0 literal (got data type {}).",
            got.data_type()
        )
    };
    Ok(max_size)
}

impl AggregateExpr for ApproxPercentileCont {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn field(&self) -> Result<Field> {
        Ok(Field::new(&self.name, self.input_data_type.clone(), false))
    }

    #[allow(rustdoc::private_intra_doc_links)]
    /// See [`datafusion_physical_expr_common::aggregate::tdigest::TDigest::to_scalar_state()`] for a description of the serialised
    /// state.
    fn state_fields(&self) -> Result<Vec<Field>> {
        Ok(vec![
            Field::new(
                format_state_name(&self.name, "max_size"),
                DataType::UInt64,
                false,
            ),
            Field::new(
                format_state_name(&self.name, "sum"),
                DataType::Float64,
                false,
            ),
            Field::new(
                format_state_name(&self.name, "count"),
                DataType::Float64,
                false,
            ),
            Field::new(
                format_state_name(&self.name, "max"),
                DataType::Float64,
                false,
            ),
            Field::new(
                format_state_name(&self.name, "min"),
                DataType::Float64,
                false,
            ),
            Field::new_list(
                format_state_name(&self.name, "centroids"),
                Field::new("item", DataType::Float64, true),
                false,
            ),
        ])
    }

    fn expressions(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        self.expr.clone()
    }

    fn create_accumulator(&self) -> Result<Box<dyn Accumulator>> {
        let accumulator = self.create_plain_accumulator()?;
        Ok(Box::new(accumulator))
    }

    fn name(&self) -> &str {
        &self.name
    }
}

impl PartialEq<dyn Any> for ApproxPercentileCont {
    fn eq(&self, other: &dyn Any) -> bool {
        down_cast_any_ref(other)
            .downcast_ref::<Self>()
            .map(|x| self.eq(x))
            .unwrap_or(false)
    }
}
