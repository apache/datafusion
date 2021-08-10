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

use crate::pyarrow::PyArrowConvert;

/// utility struct to convert PyObj to native DataType
#[derive(Debug, Clone)]
pub struct PyDataType {
    pub data_type: DataType,
}

impl<'source> FromPyObject<'source> for PyDataType {
    fn extract(ob: &'source PyAny) -> PyResult<Self> {
        let dtype = DataType::from_pyarrow(ob)?;
        Ok(PyDataType { data_type: dtype })
    }
}
