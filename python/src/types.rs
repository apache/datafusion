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

use datafusion::arrow::datatypes::DataType;
use pyo3::{FromPyObject, PyAny, PyResult};

use crate::errors;

/// utility struct to convert PyObj to native DataType
#[derive(Debug, Clone)]
pub struct PyDataType {
    pub data_type: DataType,
}

impl<'source> FromPyObject<'source> for PyDataType {
    fn extract(ob: &'source PyAny) -> PyResult<Self> {
        let id = ob.getattr("id")?.extract::<i32>()?;
        let data_type = data_type_id(&id)?;
        Ok(PyDataType { data_type })
    }
}

fn data_type_id(id: &i32) -> Result<DataType, errors::DataFusionError> {
    // see https://github.com/apache/arrow/blob/3694794bdfd0677b95b8c95681e392512f1c9237/python/pyarrow/includes/libarrow.pxd
    // this is not ideal as it does not generalize for non-basic types
    // Find a way to get a unique name from the pyarrow.DataType
    Ok(match id {
        1 => DataType::Boolean,
        2 => DataType::UInt8,
        3 => DataType::Int8,
        4 => DataType::UInt16,
        5 => DataType::Int16,
        6 => DataType::UInt32,
        7 => DataType::Int32,
        8 => DataType::UInt64,
        9 => DataType::Int64,
        10 => DataType::Float16,
        11 => DataType::Float32,
        12 => DataType::Float64,
        13 => DataType::Utf8,
        14 => DataType::Binary,
        34 => DataType::LargeUtf8,
        35 => DataType::LargeBinary,
        other => {
            return Err(errors::DataFusionError::Common(format!(
                "The type {} is not valid",
                other
            )))
        }
    })
}
