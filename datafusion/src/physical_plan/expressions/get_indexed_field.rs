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
    datatypes::{DataType, Schema},
    record_batch::RecordBatch,
};

use crate::{
    error::DataFusionError,
    error::Result,
    physical_plan::{ColumnarValue, PhysicalExpr},
    utils::get_indexed_field as get_data_type_field,
};
use arrow::array::ListArray;

/// expression to get a field of a struct array.
#[derive(Debug)]
pub struct GetIndexedFieldExpr {
    arg: Arc<dyn PhysicalExpr>,
    key: String,
}

impl GetIndexedFieldExpr {
    /// Create new get field expression
    pub fn new(arg: Arc<dyn PhysicalExpr>, key: String) -> Self {
        Self { arg, key }
    }

    /// Get the input expression
    pub fn arg(&self) -> &Arc<dyn PhysicalExpr> {
        &self.arg
    }
}

impl std::fmt::Display for GetIndexedFieldExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "({}).[{}]", self.arg, self.key)
    }
}

impl PhysicalExpr for GetIndexedFieldExpr {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self, input_schema: &Schema) -> Result<DataType> {
        let data_type = self.arg.data_type(input_schema)?;
        get_data_type_field(&data_type, &self.key).map(|f| f.data_type().clone())
    }

    fn nullable(&self, input_schema: &Schema) -> Result<bool> {
        let data_type = self.arg.data_type(input_schema)?;
        get_data_type_field(&data_type, &self.key).map(|f| f.is_nullable())
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        let arg = self.arg.evaluate(batch)?;
        match arg {
            ColumnarValue::Array(array) => {
                if let Some(la) = array.as_any().downcast_ref::<ListArray>() {
                    Ok(ColumnarValue::Array(
                        la.value(self.key.parse::<usize>().unwrap()),
                    ))
                }
                /*else if let Some(da) =
                    array.as_any().downcast_ref::<DictionaryArray<_>>()
                {
                    if let Some(index) = da.lookup_key::<Utf8>(&self.key) {
                        Ok(ColumnarValue::Array(Arc::new(
                            Int32Array::builder(0).finish(),
                        )))
                    } else {
                        Err(DataFusionError::NotImplemented(format!(
                            "key not found in dictionnary : {}",
                            self.key
                        )))
                    }
                }*/
                else {
                    Err(DataFusionError::NotImplemented(
                        "get indexed field is only possible on dictionnary and list"
                            .to_string(),
                    ))
                }
            }
            ColumnarValue::Scalar(_) => Err(DataFusionError::NotImplemented(
                "field is not yet implemented for scalar values".to_string(),
            )),
        }
    }
}

/// Create a `.[field]` expression
pub fn get_indexed_field(
    arg: Arc<dyn PhysicalExpr>,
    key: String,
) -> Result<Arc<dyn PhysicalExpr>> {
    Ok(Arc::new(GetIndexedFieldExpr::new(arg, key)))
}
