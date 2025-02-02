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

use crate::utils::array_with_timezone;
use arrow::{
    compute::{date_part, DatePart},
    record_batch::RecordBatch,
};
use arrow_schema::{DataType, Schema, TimeUnit::Microsecond};
use datafusion::logical_expr::ColumnarValue;
use datafusion_common::DataFusionError;
use datafusion_physical_expr::PhysicalExpr;
use std::hash::Hash;
use std::{
    any::Any,
    fmt::{Debug, Display, Formatter},
    sync::Arc,
};

#[derive(Debug, Eq)]
pub struct SecondExpr {
    /// An array with DataType::Timestamp(TimeUnit::Microsecond, None)
    child: Arc<dyn PhysicalExpr>,
    timezone: String,
}

impl Hash for SecondExpr {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.child.hash(state);
        self.timezone.hash(state);
    }
}
impl PartialEq for SecondExpr {
    fn eq(&self, other: &Self) -> bool {
        self.child.eq(&other.child) && self.timezone.eq(&other.timezone)
    }
}

impl SecondExpr {
    pub fn new(child: Arc<dyn PhysicalExpr>, timezone: String) -> Self {
        SecondExpr { child, timezone }
    }
}

impl Display for SecondExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Second (timezone:{}, child: {}]",
            self.timezone, self.child
        )
    }
}

impl PhysicalExpr for SecondExpr {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self, input_schema: &Schema) -> datafusion_common::Result<DataType> {
        match self.child.data_type(input_schema).unwrap() {
            DataType::Dictionary(key_type, _) => {
                Ok(DataType::Dictionary(key_type, Box::new(DataType::Int32)))
            }
            _ => Ok(DataType::Int32),
        }
    }

    fn nullable(&self, _: &Schema) -> datafusion_common::Result<bool> {
        Ok(true)
    }

    fn evaluate(&self, batch: &RecordBatch) -> datafusion_common::Result<ColumnarValue> {
        let arg = self.child.evaluate(batch)?;
        match arg {
            ColumnarValue::Array(array) => {
                let array = array_with_timezone(
                    array,
                    self.timezone.clone(),
                    Some(&DataType::Timestamp(
                        Microsecond,
                        Some(self.timezone.clone().into()),
                    )),
                )?;
                let result = date_part(&array, DatePart::Second)?;

                Ok(ColumnarValue::Array(result))
            }
            _ => Err(DataFusionError::Execution(
                "Second(scalar) should be fold in Spark JVM side.".to_string(),
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
        Ok(Arc::new(SecondExpr::new(
            Arc::clone(&children[0]),
            self.timezone.clone(),
        )))
    }
}
