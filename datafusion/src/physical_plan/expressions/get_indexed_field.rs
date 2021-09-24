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
use crate::arrow::datatypes::{
    Int16Type, Int32Type, Int64Type, UInt16Type, UInt32Type, UInt64Type, UInt8Type,
};
use crate::scalar::ScalarValue;
use crate::{
    error::DataFusionError,
    error::Result,
    field_util::get_indexed_field as get_data_type_field,
    physical_plan::{ColumnarValue, PhysicalExpr},
};
use arrow::array::{ArrayRef, DictionaryArray, ListArray};
use arrow::datatypes::{ArrowPrimitiveType, Int8Type};
use num_traits::ToPrimitive;
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
                        .filter_map(|o| o.map(|list| list.slice(*i as usize, 1)))
                        .collect();
                    let vec = x.iter().map(|a| a.as_ref()).collect::<Vec<&dyn Array>>();
                    let iter = concat(vec.as_slice()).unwrap();
                    Ok(ColumnarValue::Array(iter))
                }
                (DataType::Dictionary(ref kt, _), ScalarValue::Utf8(Some(s))) => {
                    match **kt {
                        DataType::Int8 => dict_lookup::<Int8Type>(array, s),
                        DataType::Int16 => dict_lookup::<Int16Type>(array, s),
                        DataType::Int32 => dict_lookup::<Int32Type>(array, s),
                        DataType::Int64 => dict_lookup::<Int64Type>(array, s),
                        DataType::UInt8 => dict_lookup::<UInt8Type>(array, s),
                        DataType::UInt16 => dict_lookup::<UInt16Type>(array, s),
                        DataType::UInt32 => dict_lookup::<UInt32Type>(array, s),
                        DataType::UInt64 => dict_lookup::<UInt64Type>(array, s),
                        _ => Err(DataFusionError::NotImplemented(
                            "dictionary lookup only available for numeric keys"
                                .to_string(),
                        )),
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

fn dict_lookup<T: ArrowPrimitiveType>(
    array: ArrayRef,
    lookup: &str,
) -> Result<ColumnarValue>
where
    T::Native: num_traits::cast::ToPrimitive,
{
    let as_dict_array = array.as_any().downcast_ref::<DictionaryArray<T>>().unwrap();
    if let Some(index) = as_dict_array.lookup_key(lookup) {
        Ok(ColumnarValue::Array(
            as_dict_array
                .keys()
                .slice(ToPrimitive::to_usize(&index).unwrap(), 1),
        ))
    } else {
        Err(DataFusionError::NotImplemented(format!(
            "key not found in dictionary for : {}",
            lookup
        )))
    }
}
