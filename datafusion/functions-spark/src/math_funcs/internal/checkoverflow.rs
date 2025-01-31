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
    array::{as_primitive_array, Array, ArrayRef, Decimal128Array},
    datatypes::{Decimal128Type, DecimalType},
    record_batch::RecordBatch,
};
use arrow_schema::{DataType, Schema};
use datafusion::logical_expr::ColumnarValue;
use datafusion_common::{DataFusionError, ScalarValue};
use datafusion_physical_expr::PhysicalExpr;
use std::hash::Hash;
use std::{
    any::Any,
    fmt::{Display, Formatter},
    sync::Arc,
};

/// This is from Spark `CheckOverflow` expression. Spark `CheckOverflow` expression rounds decimals
/// to given scale and check if the decimals can fit in given precision. As `cast` kernel rounds
/// decimals already, Comet `CheckOverflow` expression only checks if the decimals can fit in the
/// precision.
#[derive(Debug, Eq)]
pub struct CheckOverflow {
    pub child: Arc<dyn PhysicalExpr>,
    pub data_type: DataType,
    pub fail_on_error: bool,
}

impl Hash for CheckOverflow {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.child.hash(state);
        self.data_type.hash(state);
        self.fail_on_error.hash(state);
    }
}

impl PartialEq for CheckOverflow {
    fn eq(&self, other: &Self) -> bool {
        self.child.eq(&other.child)
            && self.data_type.eq(&other.data_type)
            && self.fail_on_error.eq(&other.fail_on_error)
    }
}

impl CheckOverflow {
    pub fn new(child: Arc<dyn PhysicalExpr>, data_type: DataType, fail_on_error: bool) -> Self {
        Self {
            child,
            data_type,
            fail_on_error,
        }
    }
}

impl Display for CheckOverflow {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "CheckOverflow [datatype: {}, fail_on_error: {}, child: {}]",
            self.data_type, self.fail_on_error, self.child
        )
    }
}

impl PhysicalExpr for CheckOverflow {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self, _: &Schema) -> datafusion_common::Result<DataType> {
        Ok(self.data_type.clone())
    }

    fn nullable(&self, _: &Schema) -> datafusion_common::Result<bool> {
        Ok(true)
    }

    fn evaluate(&self, batch: &RecordBatch) -> datafusion_common::Result<ColumnarValue> {
        let arg = self.child.evaluate(batch)?;
        match arg {
            ColumnarValue::Array(array)
                if matches!(array.data_type(), DataType::Decimal128(_, _)) =>
            {
                let (precision, scale) = match &self.data_type {
                    DataType::Decimal128(p, s) => (p, s),
                    dt => {
                        return Err(DataFusionError::Execution(format!(
                            "CheckOverflow expects only Decimal128, but got {:?}",
                            dt
                        )))
                    }
                };

                let decimal_array = as_primitive_array::<Decimal128Type>(&array);

                let casted_array = if self.fail_on_error {
                    // Returning error if overflow
                    decimal_array.validate_decimal_precision(*precision)?;
                    decimal_array
                } else {
                    // Overflowing gets null value
                    &decimal_array.null_if_overflow_precision(*precision)
                };

                let new_array = Decimal128Array::from(casted_array.into_data())
                    .with_precision_and_scale(*precision, *scale)
                    .map(|a| Arc::new(a) as ArrayRef)?;

                Ok(ColumnarValue::Array(new_array))
            }
            ColumnarValue::Scalar(ScalarValue::Decimal128(v, precision, scale)) => {
                // `fail_on_error` is only true when ANSI is enabled, which we don't support yet
                // (Java side will simply fallback to Spark when it is enabled)
                assert!(
                    !self.fail_on_error,
                    "fail_on_error (ANSI mode) is not supported yet"
                );

                let new_v: Option<i128> = v.and_then(|v| {
                    Decimal128Type::validate_decimal_precision(v, precision)
                        .map(|_| v)
                        .ok()
                });

                Ok(ColumnarValue::Scalar(ScalarValue::Decimal128(
                    new_v, precision, scale,
                )))
            }
            v => Err(DataFusionError::Execution(format!(
                "CheckOverflow's child expression should be decimal array, but found {:?}",
                v
            ))),
        }
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![&self.child]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> datafusion_common::Result<Arc<dyn PhysicalExpr>> {
        Ok(Arc::new(CheckOverflow::new(
            Arc::clone(&children[0]),
            self.data_type.clone(),
            self.fail_on_error,
        )))
    }
}
