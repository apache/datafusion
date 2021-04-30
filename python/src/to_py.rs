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

use pyo3::prelude::*;
use pyo3::{libc::uintptr_t, PyErr};

use std::convert::From;

use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::record_batch::RecordBatch;

use crate::errors;

pub fn to_py_array(array: &ArrayRef, py: Python) -> PyResult<PyObject> {
    let (array_pointer, schema_pointer) =
        array.to_raw().map_err(errors::DataFusionError::from)?;

    let pa = py.import("pyarrow")?;

    let array = pa.getattr("Array")?.call_method1(
        "_import_from_c",
        (array_pointer as uintptr_t, schema_pointer as uintptr_t),
    )?;
    Ok(array.to_object(py))
}

fn to_py_batch<'a>(
    batch: &RecordBatch,
    py: Python,
    pyarrow: &'a PyModule,
) -> Result<PyObject, PyErr> {
    let mut py_arrays = vec![];
    let mut py_names = vec![];

    let schema = batch.schema();
    for (array, field) in batch.columns().iter().zip(schema.fields().iter()) {
        let array = to_py_array(array, py)?;

        py_arrays.push(array);
        py_names.push(field.name());
    }

    let record = pyarrow
        .getattr("RecordBatch")?
        .call_method1("from_arrays", (py_arrays, py_names))?;

    Ok(PyObject::from(record))
}

/// Converts a &[RecordBatch] into a Vec<RecordBatch> represented in PyArrow
pub fn to_py(batches: &[RecordBatch]) -> PyResult<PyObject> {
    let gil = pyo3::Python::acquire_gil();
    let py = gil.python();
    let pyarrow = PyModule::import(py, "pyarrow")?;
    let builtins = PyModule::import(py, "builtins")?;

    let mut py_batches = vec![];
    for batch in batches {
        py_batches.push(to_py_batch(batch, py, pyarrow)?);
    }
    let result = builtins.call1("list", (py_batches,))?;
    Ok(PyObject::from(result))
}
