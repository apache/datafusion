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

//! LIKE and NOT LIKE expression

use arrow::array::*;
use arrow::compute::kernels::comparison::*; // TODO import specific items
use arrow::{
    datatypes::{DataType, Schema},
    record_batch::RecordBatch,
};
use std::{any::Any, sync::Arc};

use crate::PhysicalExpr;
use datafusion_common::DataFusionError;
use datafusion_common::Result;
use datafusion_common::ScalarValue;
use datafusion_expr::ColumnarValue;

/// IS NULL expression
#[derive(Debug)]
pub struct LikeExpr {
    /// negated
    negated: bool,
    /// Input expression
    expr: Arc<dyn PhysicalExpr>,
    /// Regex pattern
    pattern: Arc<dyn PhysicalExpr>,
}

impl LikeExpr {
    /// Create new not expression
    pub fn new(
        negated: bool,
        expr: Arc<dyn PhysicalExpr>,
        pattern: Arc<dyn PhysicalExpr>,
    ) -> Self {
        Self {
            negated,
            expr,
            pattern,
        }
    }

    /// Get the input expression
    pub fn expr(&self) -> &Arc<dyn PhysicalExpr> {
        &self.expr
    }

    /// Get the pattern expression
    pub fn pattern(&self) -> &Arc<dyn PhysicalExpr> {
        &self.pattern
    }
}

impl std::fmt::Display for LikeExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        if self.negated {
            write!(f, "{} NOT LIKE", self.expr)
        } else {
            write!(f, "{} LIKE", self.expr)
        }
    }
}

macro_rules! binary_string_array_op_scalar {
    ($LEFT:expr, $RIGHT:expr, $OP:ident, $OP_TYPE:expr) => {{
        let result: Result<Arc<dyn Array>> = match $LEFT.data_type() {
            DataType::Utf8 => compute_utf8_op_scalar!($LEFT, $RIGHT, $OP, StringArray, $OP_TYPE),
            other => Err(DataFusionError::Internal(format!(
                "Data type {:?} not supported for scalar operation '{}' on string array",
                other, stringify!($OP)
            ))),
        };
        Some(result)
    }};
}

impl PhysicalExpr for LikeExpr {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self, _input_schema: &Schema) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn nullable(&self, _input_schema: &Schema) -> Result<bool> {
        self.expr.nullable(_input_schema)
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        println!("LIKE evaluate");

        let inputs = self.expr.evaluate(batch)?;
        let pattern = self.pattern.evaluate(batch)?;
        match inputs {
            ColumnarValue::Array(array) => match pattern {
                ColumnarValue::Array(_) => Err(DataFusionError::NotImplemented(
                    "LIKE: non-scalar pattern not supported".to_string(),
                )),
                ColumnarValue::Scalar(scalar) => foo(array, scalar, self.negated),
            },
            ColumnarValue::Scalar(data) => match pattern {
                ColumnarValue::Array(_) => Err(DataFusionError::NotImplemented(
                    "LIKE: non-scalar pattern not supported".to_string(),
                )),
                ColumnarValue::Scalar(pattern) => {
                    foo(data.to_array(), pattern, self.negated)
                }
            },
        }
    }
}

//TODO escape_char
fn foo(data: ArrayRef, pattern: ScalarValue, negated: bool) -> Result<ColumnarValue> {
    let arc: Arc<dyn Array> = Arc::new(if negated {
        binary_string_array_op_scalar!(data, pattern, nlike, &DataType::Boolean)
            //TODO unwraps
            .unwrap()
            .unwrap()
    } else {
        binary_string_array_op_scalar!(
            data,
            pattern,
            like, //TODO what aboue like_utf8_scalar, nlike_utf8_scalar,  ?
            &DataType::Boolean
        )
        //TODO unwraps
        .unwrap()
        .unwrap()
    });
    Ok(ColumnarValue::Array(arc))
}
/// Create a LIKE expression
pub fn like(
    expr: Arc<dyn PhysicalExpr>,
    pattern: Arc<dyn PhysicalExpr>,
) -> Result<Arc<dyn PhysicalExpr>> {
    Ok(Arc::new(LikeExpr::new(false, expr, pattern)))
}

/// Create a NOT LIKE expression
pub fn not_like(
    expr: Arc<dyn PhysicalExpr>,
    pattern: Arc<dyn PhysicalExpr>,
) -> Result<Arc<dyn PhysicalExpr>> {
    Ok(Arc::new(LikeExpr::new(true, expr, pattern)))
}

// TODO unit tests
