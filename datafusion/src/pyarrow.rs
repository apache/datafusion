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

use arrow::array::Array;
use arrow::error::ArrowError;
use pyo3::exceptions::{PyException, PyNotImplementedError};
use pyo3::ffi::Py_uintptr_t;
use pyo3::prelude::*;
use pyo3::types::PyList;
use pyo3::PyNativeType;
use std::sync::Arc;

use crate::error::DataFusionError;
use crate::scalar::ScalarValue;

impl From<DataFusionError> for PyErr {
    fn from(err: DataFusionError) -> PyErr {
        PyException::new_err(err.to_string())
    }
}

impl From<PyO3ArrowError> for PyErr {
    fn from(err: PyO3ArrowError) -> PyErr {
        PyException::new_err(format!("{:?}", err))
    }
}

#[derive(Debug)]
enum PyO3ArrowError {
    ArrowError(ArrowError),
}

fn to_rust_array(ob: PyObject, py: Python) -> PyResult<Arc<dyn Array>> {
    // prepare a pointer to receive the Array struct
    let array = Box::new(arrow::ffi::Ffi_ArrowArray::empty());
    let schema = Box::new(arrow::ffi::Ffi_ArrowSchema::empty());

    let array_ptr = &*array as *const arrow::ffi::Ffi_ArrowArray;
    let schema_ptr = &*schema as *const arrow::ffi::Ffi_ArrowSchema;

    // make the conversion through PyArrow's private API
    // this changes the pointer's memory and is thus unsafe. In particular, `_export_to_c` can go out of bounds
    ob.call_method1(
        py,
        "_export_to_c",
        (array_ptr as Py_uintptr_t, schema_ptr as Py_uintptr_t),
    )?;

    let field = unsafe {
        arrow::ffi::import_field_from_c(schema.as_ref())
            .map_err(PyO3ArrowError::ArrowError)?
    };
    let array = unsafe {
        arrow::ffi::import_array_from_c(array, &field)
            .map_err(PyO3ArrowError::ArrowError)?
    };

    Ok(array.into())
}
impl<'source> FromPyObject<'source> for ScalarValue {
    fn extract(value: &'source PyAny) -> PyResult<Self> {
        let py = value.py();
        let typ = value.getattr("type")?;
        let val = value.call_method0("as_py")?;

        // construct pyarrow array from the python value and pyarrow type
        let factory = py.import("pyarrow")?.getattr("array")?;
        let args = PyList::new(py, &[val]);
        let array = factory.call1((args, typ))?;

        // convert the pyarrow array to rust array using C data interface]
        let array = to_rust_array(array.to_object(py), py)?;
        let scalar = ScalarValue::try_from_array(&array, 0)?;

        Ok(scalar)
    }
}

impl<'a> IntoPy<PyObject> for ScalarValue {
    fn into_py(self, _py: Python) -> PyObject {
        Err(PyNotImplementedError::new_err("Not implemented")).unwrap()
    }
}
