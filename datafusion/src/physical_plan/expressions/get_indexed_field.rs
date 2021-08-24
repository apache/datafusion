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

use crate::arrow::array::Array;
use crate::arrow::compute::concat;
use crate::scalar::ScalarValue;
use crate::{
    error::DataFusionError,
    error::Result,
    physical_plan::{ColumnarValue, PhysicalExpr},
    utils::get_indexed_field as get_data_type_field,
};
use arrow::array::{DictionaryArray, ListArray};
use arrow::datatypes::Int8Type;
use std::fmt::Debug;

/// expression to get a field of a struct array.
#[derive(Debug)]
pub struct GetIndexedFieldExpr {
    arg: Arc<dyn PhysicalExpr>,
    key: ScalarValue,
}

impl GetIndexedFieldExpr {
    /// Create new get field expression
    pub fn new(arg: Arc<dyn PhysicalExpr>, key: ScalarValue) -> Self {
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
            ColumnarValue::Array(array) => match (array.data_type(), &self.key) {
                (DataType::List(_), ScalarValue::Int64(Some(i))) => {
                    let as_list_array =
                        array.as_any().downcast_ref::<ListArray>().unwrap();
                    let x: Vec<Arc<dyn Array>> = as_list_array
                        .iter()
                        .filter_map(|o| o.map(|list| list.slice(*i as usize, 1).clone()))
                        .collect();
                    let vec = x.iter().map(|a| a.as_ref()).collect::<Vec<&dyn Array>>();
                    let iter = concat(vec.as_slice()).unwrap();
                    Ok(ColumnarValue::Array(iter))
                }
                (DataType::Dictionary(_, _), ScalarValue::Utf8(Some(s))) => {
                    let as_dict_array = array
                        .as_any()
                        .downcast_ref::<DictionaryArray<Int8Type>>()
                        .unwrap();
                    if let Some(index) = as_dict_array.lookup_key(s) {
                        Ok(ColumnarValue::Array(as_dict_array.slice(index as usize, 1)))
                    } else {
                        Err(DataFusionError::NotImplemented(format!(
                            "key not found in dictionnary : {}",
                            self.key
                        )))
                    }
                }
                _ => Err(DataFusionError::NotImplemented(
                    "get indexed field is only possible on dictionary and list"
                        .to_string(),
                )),
            },
            ColumnarValue::Scalar(_) => Err(DataFusionError::NotImplemented(
                "field is not yet implemented for scalar values".to_string(),
            )),
        }
    }
}

/// Create a `.[field]` expression
pub fn get_indexed_field(
    arg: Arc<dyn PhysicalExpr>,
    key: ScalarValue,
) -> Result<Arc<dyn PhysicalExpr>> {
    Ok(Arc::new(GetIndexedFieldExpr::new(arg, key)))
}
