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

//! get field of a struct array

use std::{any::Any, sync::Arc};

use arrow::{
    array::StructArray,
    datatypes::{DataType, Schema},
    record_batch::RecordBatch,
};

use crate::{
    error::DataFusionError,
    error::Result,
    physical_plan::{ColumnarValue, PhysicalExpr},
    utils::get_field as get_data_type_field,
};

/// expression to get a field of a struct array.
#[derive(Debug)]
pub struct GetFieldExpr {
    arg: Arc<dyn PhysicalExpr>,
    name: String,
}

impl GetFieldExpr {
    /// Create new get field expression
    pub fn new(arg: Arc<dyn PhysicalExpr>, name: String) -> Self {
        Self { arg, name }
    }

    /// Get the input expression
    pub fn arg(&self) -> &Arc<dyn PhysicalExpr> {
        &self.arg
    }
}

impl std::fmt::Display for GetFieldExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "({}).{}", self.arg, self.name)
    }
}

impl PhysicalExpr for GetFieldExpr {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self, input_schema: &Schema) -> Result<DataType> {
        let data_type = self.arg.data_type(input_schema)?;
        get_data_type_field(&data_type, &self.name).map(|f| f.data_type().clone())
    }

    fn nullable(&self, input_schema: &Schema) -> Result<bool> {
        let data_type = self.arg.data_type(input_schema)?;
        get_data_type_field(&data_type, &self.name).map(|f| f.is_nullable())
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        let arg = self.arg.evaluate(batch)?;
        match arg {
            ColumnarValue::Array(array) => Ok(ColumnarValue::Array(
                array
                    .as_any()
                    .downcast_ref::<StructArray>()
                    .unwrap()
                    .column_by_name(&self.name)
                    .unwrap()
                    .clone(),
            )),
            ColumnarValue::Scalar(_) => Err(DataFusionError::NotImplemented(
                "field is not yet implemented for scalar values".to_string(),
            )),
        }
    }
}

/// Create an `.field` expression
pub fn get_field(
    arg: Arc<dyn PhysicalExpr>,
    name: String,
) -> Result<Arc<dyn PhysicalExpr>> {
    Ok(Arc::new(GetFieldExpr::new(arg, name)))
}
