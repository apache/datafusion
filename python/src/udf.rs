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

use pyo3::{prelude::*, types::PyTuple};

use datafusion::{arrow::array, physical_plan::functions::make_scalar_function};

use datafusion::error::DataFusionError;
use datafusion::physical_plan::functions::ScalarFunctionImplementation;

use crate::to_py::to_py_array;
use crate::to_rust::to_rust;

/// creates a DataFusion's UDF implementation from a python function that expects pyarrow arrays
/// This is more efficient as it performs a zero-copy of the contents.
pub fn array_udf(func: PyObject) -> ScalarFunctionImplementation {
    make_scalar_function(
        move |args: &[array::ArrayRef]| -> Result<array::ArrayRef, DataFusionError> {
            Python::with_gil(|py| {
                // 1. cast args to Pyarrow arrays
                // 2. call function
                // 3. cast to arrow::array::Array

                // 1.
                let py_args = args
                    .iter()
                    .map(|arg| {
                        // remove unwrap
                        to_py_array(arg, py).unwrap()
                    })
                    .collect::<Vec<_>>();
                let py_args = PyTuple::new(py, py_args);

                // 2.
                let value = func.as_ref(py).call(py_args, None);
                let value = match value {
                    Ok(n) => Ok(n),
                    Err(error) => Err(DataFusionError::Execution(format!("{:?}", error))),
                }?;

                let array = to_rust(value).unwrap();
                Ok(array)
            })
        },
    )
}
