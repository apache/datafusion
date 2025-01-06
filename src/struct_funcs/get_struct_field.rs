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
use arrow_array::{Array, StructArray};
use arrow_schema::{DataType, Field, Schema};
use datafusion::logical_expr::ColumnarValue;
use datafusion_common::{DataFusionError, Result as DataFusionResult, ScalarValue};
use datafusion_physical_expr::PhysicalExpr;
use std::{
    any::Any,
    fmt::{Display, Formatter},
    hash::Hash,
    sync::Arc,
};

#[derive(Debug, Eq)]
pub struct GetStructField {
    child: Arc<dyn PhysicalExpr>,
    ordinal: usize,
}

impl Hash for GetStructField {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.child.hash(state);
        self.ordinal.hash(state);
    }
}
impl PartialEq for GetStructField {
    fn eq(&self, other: &Self) -> bool {
        self.child.eq(&other.child) && self.ordinal.eq(&other.ordinal)
    }
}

impl GetStructField {
    pub fn new(child: Arc<dyn PhysicalExpr>, ordinal: usize) -> Self {
        Self { child, ordinal }
    }

    fn child_field(&self, input_schema: &Schema) -> DataFusionResult<Arc<Field>> {
        match self.child.data_type(input_schema)? {
            DataType::Struct(fields) => Ok(Arc::clone(&fields[self.ordinal])),
            data_type => Err(DataFusionError::Plan(format!(
                "Expect struct field, got {:?}",
                data_type
            ))),
        }
    }
}

impl PhysicalExpr for GetStructField {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self, input_schema: &Schema) -> DataFusionResult<DataType> {
        Ok(self.child_field(input_schema)?.data_type().clone())
    }

    fn nullable(&self, input_schema: &Schema) -> DataFusionResult<bool> {
        Ok(self.child_field(input_schema)?.is_nullable())
    }

    fn evaluate(&self, batch: &RecordBatch) -> DataFusionResult<ColumnarValue> {
        let child_value = self.child.evaluate(batch)?;

        match child_value {
            ColumnarValue::Array(array) => {
                let struct_array = array
                    .as_any()
                    .downcast_ref::<StructArray>()
                    .expect("A struct is expected");

                Ok(ColumnarValue::Array(Arc::clone(
                    struct_array.column(self.ordinal),
                )))
            }
            ColumnarValue::Scalar(ScalarValue::Struct(struct_array)) => Ok(ColumnarValue::Array(
                Arc::clone(struct_array.column(self.ordinal)),
            )),
            value => Err(DataFusionError::Execution(format!(
                "Expected a struct array, got {:?}",
                value
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
        Ok(Arc::new(GetStructField::new(
            Arc::clone(&children[0]),
            self.ordinal,
        )))
    }
}

impl Display for GetStructField {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "GetStructField [child: {:?}, ordinal: {:?}]",
            self.child, self.ordinal
        )
    }
}
