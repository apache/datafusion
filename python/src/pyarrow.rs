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

use std::convert::TryFrom;
use std::error;

use libc::uintptr_t;
use pyo3::prelude::*;
use pyo3::wrap_pyfunction;

use datafusion::arrow::array::{make_array_from_raw, ArrayRef};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::ffi;
use datafusion::arrow::ffi::FFI_ArrowSchema;
use datafusion::arrow::record_batch::RecordBatch;

use crate::errors::DataFusionError;

pub trait PyArrowConvert: Sized {
    fn from_pyarrow(value: &PyAny) -> PyResult<Self>;
    fn to_pyarrow(&self, py: Python) -> PyResult<PyObject>;
}

impl PyArrowConvert for DataType {
    fn from_pyarrow(value: &PyAny) -> PyResult<Self> {
        let c_schema = FFI_ArrowSchema::empty();
        let c_schema_ptr = &c_schema as *const FFI_ArrowSchema;
        value.call_method1("_export_to_c", (c_schema_ptr as uintptr_t,))?;
        let dtype = DataType::try_from(&c_schema).map_err(DataFusionError::from)?;
        Ok(dtype)
    }

    fn to_pyarrow(&self, py: Python) -> PyResult<PyObject> {
        let c_schema = FFI_ArrowSchema::try_from(self).map_err(DataFusionError::from)?;
        let c_schema_ptr = &c_schema as *const FFI_ArrowSchema;
        let cls = py.import("pyarrow.DataType")?;
        let dtype = cls.call_method1("_import_from_c", (c_schema_ptr as uintptr_t,))?;
        Ok(dtype.into())
    }
}

impl PyArrowConvert for Field {
    fn from_pyarrow(value: &PyAny) -> PyResult<Self> {
        let c_schema = FFI_ArrowSchema::empty();
        let c_schema_ptr = &c_schema as *const FFI_ArrowSchema;
        value.call_method1("_export_to_c", (c_schema_ptr as uintptr_t,))?;
        let field = Field::try_from(&c_schema).map_err(DataFusionError::from)?;
        Ok(field)
    }

    fn to_pyarrow(&self, py: Python) -> PyResult<PyObject> {
        let c_schema = FFI_ArrowSchema::try_from(self).map_err(DataFusionError::from)?;
        let c_schema_ptr = &c_schema as *const FFI_ArrowSchema;
        let cls = py.import("pyarrow.Field")?;
        let dtype = cls.call_method1("_import_from_c", (c_schema_ptr as uintptr_t,))?;
        Ok(dtype.into())
    }
}

impl PyArrowConvert for Schema {
    fn from_pyarrow(value: &PyAny) -> PyResult<Self> {
        let c_schema = FFI_ArrowSchema::empty();
        let c_schema_ptr = &c_schema as *const FFI_ArrowSchema;
        value.call_method1("_export_to_c", (c_schema_ptr as uintptr_t,))?;
        let schema = Schema::try_from(&c_schema).map_err(DataFusionError::from)?;
        Ok(schema)
    }

    fn to_pyarrow(&self, py: Python) -> PyResult<PyObject> {
        let c_schema = FFI_ArrowSchema::try_from(self).map_err(DataFusionError::from)?;
        let c_schema_ptr = &c_schema as *const FFI_ArrowSchema;
        let cls = py.import("pyarrow.Schema")?;
        let schema = cls.call_method1("_import_from_c", (c_schema_ptr as uintptr_t,))?;
        Ok(schema.into())
    }
}

impl PyArrowConvert for ArrayRef {
    fn from_pyarrow(value: &PyAny) -> PyResult<Self> {
        // prepare a pointer to receive the Array struct
        let (array_pointer, schema_pointer) =
            ffi::ArrowArray::into_raw(unsafe { ffi::ArrowArray::empty() });

        // make the conversion through PyArrow's private API
        // this changes the pointer's memory and is thus unsafe.
        // In particular, `_export_to_c` can go out of bounds
        value.call_method1(
            "_export_to_c",
            (array_pointer as uintptr_t, schema_pointer as uintptr_t),
        )?;

        let array = unsafe { make_array_from_raw(array_pointer, schema_pointer) }
            .map_err(DataFusionError::from)?;
        Ok(array)
    }

    fn to_pyarrow(&self, py: Python) -> PyResult<PyObject> {
        let (array_pointer, schema_pointer) =
            self.to_raw().map_err(DataFusionError::from)?;

        let cls = py.import("pyarrow.Array")?;
        let array = cls.call_method1(
            "_import_from_c",
            (array_pointer as uintptr_t, schema_pointer as uintptr_t),
        )?;
        Ok(array.to_object(py))
    }
}

impl PyArrowConvert for RecordBatch {
    fn from_pyarrow(value: &PyAny) -> PyResult<Self> {
        // TODO(kszucs): implement the FFI conversions in arrow-rs for RecordBatches
        let schema = value.getattr("schema")?;
        let arrays = value.getattr("columns")?;

        // let names = schema.getattr("names")?.extract::<Vec<String>>()?;

        // let fields = names
        //     .iter()
        //     .enumerate()
        //     .map(|(i, name)| {
        //         let field = schema.call_method1("field", (i,))?;
        //         let nullable = field.getattr("nullable")?.extract::<bool>()?;
        //         let py_data_type = field.getattr("type")?;
        //         let data_type = py_data_type.extract::<PyDataType>()?.data_type;
        //         Ok(Field::new(name, data_type, nullable))
        //     })
        //     .collect::<PyResult<_>>()?;

        let schema = Schema::from_pyarrow(schema)?;

        let arrays = (0..names.len())
            .map(|i| {
                let array = batch.call_method1("column", (i,))?;
                to_rust(array)
            })
            .collect::<PyResult<_>>()?;

        let batch =
            RecordBatch::try_new(schema, arrays).map_err(DataFusionError::from)?;
        Ok(batch)
    }

    fn to_pyarrow(&self, py: Python) -> PyResult<PyObject> {
        let mut py_arrays = vec![];
        let mut py_names = vec![];

        let columns = self.columns().iter();
        let fields = self.schema().fields().iter();

        for (array, field) in columns.zip(fields) {
            py_arrays.push(array.to_pyarrow(py)?);
            py_names.push(field.name());
        }

        let cls = py.import("pyarrow.RecordBatch")?;
        let record = cls.call_method1("from_arrays", (py_arrays, py_names))?;

        Ok(PyObject::from(record))
    }

    // fn to_pyarrow(batches: &[RecordBatch]) -> PyResult<PyObject> {
    //     Python::with_gil(|py| {
    //         let pyarrow = PyModule::import(py, "pyarrow")?;
    //         let mut py_batches = vec![];
    //         for batch in batches {
    //             py_batches.push(to_py_batch(batch, py, pyarrow)?);
    //         }
    //         let list = PyList::new(py, py_batches);
    //         Ok(PyObject::from(list))
    //     })
    // }
}
