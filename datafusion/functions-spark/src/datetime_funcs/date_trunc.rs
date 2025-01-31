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

use arrow::record_batch::RecordBatch;
use arrow_schema::{DataType, Schema};
use datafusion::logical_expr::ColumnarValue;
use datafusion_common::{DataFusionError, ScalarValue::Utf8};
use datafusion_physical_expr::PhysicalExpr;
use std::hash::Hash;
use std::{
    any::Any,
    fmt::{Debug, Display, Formatter},
    sync::Arc,
};

use crate::kernels::temporal::{date_trunc_array_fmt_dyn, date_trunc_dyn};

#[derive(Debug, Eq)]
pub struct DateTruncExpr {
    /// An array with DataType::Date32
    child: Arc<dyn PhysicalExpr>,
    /// Scalar UTF8 string matching the valid values in Spark SQL: https://spark.apache.org/docs/latest/api/sql/index.html#trunc
    format: Arc<dyn PhysicalExpr>,
}

impl Hash for DateTruncExpr {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.child.hash(state);
        self.format.hash(state);
    }
}
impl PartialEq for DateTruncExpr {
    fn eq(&self, other: &Self) -> bool {
        self.child.eq(&other.child) && self.format.eq(&other.format)
    }
}

impl DateTruncExpr {
    pub fn new(child: Arc<dyn PhysicalExpr>, format: Arc<dyn PhysicalExpr>) -> Self {
        DateTruncExpr { child, format }
    }
}

impl Display for DateTruncExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "DateTrunc [child:{}, format: {}]",
            self.child, self.format
        )
    }
}

impl PhysicalExpr for DateTruncExpr {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self, input_schema: &Schema) -> datafusion_common::Result<DataType> {
        self.child.data_type(input_schema)
    }

    fn nullable(&self, _: &Schema) -> datafusion_common::Result<bool> {
        Ok(true)
    }

    fn evaluate(&self, batch: &RecordBatch) -> datafusion_common::Result<ColumnarValue> {
        let date = self.child.evaluate(batch)?;
        let format = self.format.evaluate(batch)?;
        match (date, format) {
            (ColumnarValue::Array(date), ColumnarValue::Scalar(Utf8(Some(format)))) => {
                let result = date_trunc_dyn(&date, format)?;
                Ok(ColumnarValue::Array(result))
            }
            (ColumnarValue::Array(date), ColumnarValue::Array(formats)) => {
                let result = date_trunc_array_fmt_dyn(&date, &formats)?;
                Ok(ColumnarValue::Array(result))
            }
            _ => Err(DataFusionError::Execution(
                "Invalid input to function DateTrunc. Expected (PrimitiveArray<Date32>, Scalar) or \
                    (PrimitiveArray<Date32>, StringArray)".to_string(),
            )),
        }
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![&self.child]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>, DataFusionError> {
        Ok(Arc::new(DateTruncExpr::new(
            Arc::clone(&children[0]),
            Arc::clone(&self.format),
        )))
    }
}
