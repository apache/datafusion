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

//! Conversions between PyArrow and DataFusion types

use arrow::array::ArrayData;
use arrow::pyarrow::{FromPyArrow, ToPyArrow};
use arrow_array::Array;
use pyo3::exceptions::PyException;
use pyo3::prelude::PyErr;
use pyo3::types::{PyAnyMethods, PyList};
use pyo3::{Bound, FromPyObject, IntoPy, PyAny, PyObject, PyResult, Python};

use crate::{DataFusionError, ScalarValue};

impl From<DataFusionError> for PyErr {
    fn from(err: DataFusionError) -> PyErr {
        PyException::new_err(err.to_string())
    }
}

impl FromPyArrow for ScalarValue {
    fn from_pyarrow_bound(value: &Bound<'_, PyAny>) -> PyResult<Self> {
        let py = value.py();
        let typ = value.getattr("type")?;
        let val = value.call_method0("as_py")?;

        // construct pyarrow array from the python value and pyarrow type
        let factory = py.import_bound("pyarrow")?.getattr("array")?;
        let args = PyList::new_bound(py, [val]);
        let array = factory.call1((args, typ))?;

        // convert the pyarrow array to rust array using C data interface
        let array = arrow::array::make_array(ArrayData::from_pyarrow_bound(&array)?);
        let scalar = ScalarValue::try_from_array(&array, 0)?;

        Ok(scalar)
    }
}

impl ToPyArrow for ScalarValue {
    fn to_pyarrow(&self, py: Python) -> PyResult<PyObject> {
        let array = self.to_array()?;
        // convert to pyarrow array using C data interface
        let pyarray = array.to_data().to_pyarrow(py)?;
        let pyscalar = pyarray.call_method1(py, "__getitem__", (0,))?;

        Ok(pyscalar)
    }
}

impl<'source> FromPyObject<'source> for ScalarValue {
    fn extract_bound(value: &Bound<'source, PyAny>) -> PyResult<Self> {
        Self::from_pyarrow_bound(value)
    }
}

impl IntoPy<PyObject> for ScalarValue {
    fn into_py(self, py: Python) -> PyObject {
        self.to_pyarrow(py).unwrap()
    }
}

#[cfg(test)]
mod tests {
    use pyo3::prepare_freethreaded_python;
    use pyo3::py_run;
    use pyo3::types::PyDict;

    use super::*;

    fn init_python() {
        prepare_freethreaded_python();
        Python::with_gil(|py| {
            if py.run_bound("import pyarrow", None, None).is_err() {
                let locals = PyDict::new_bound(py);
                py.run_bound(
                    "import sys; executable = sys.executable; python_path = sys.path",
                    None,
                    Some(&locals),
                )
                .expect("Couldn't get python info");
                let executable = locals.get_item("executable").unwrap();
                let executable: String = executable.extract().unwrap();

                let python_path = locals.get_item("python_path").unwrap();
                let python_path: Vec<String> = python_path.extract().unwrap();

                panic!("pyarrow not found\nExecutable: {executable}\nPython path: {python_path:?}\n\
                         HINT: try `pip install pyarrow`\n\
                         NOTE: On Mac OS, you must compile against a Framework Python \
                         (default in python.org installers and brew, but not pyenv)\n\
                         NOTE: On Mac OS, PYO3 might point to incorrect Python library \
                         path when using virtual environments. Try \
                         `export PYTHONPATH=$(python -c \"import sys; print(sys.path[-1])\")`\n")
            }
        })
    }

    #[test]
    fn test_roundtrip() {
        init_python();

        let example_scalars = vec![
            ScalarValue::Boolean(Some(true)),
            ScalarValue::Int32(Some(23)),
            ScalarValue::Float64(Some(12.34)),
            ScalarValue::from("Hello!"),
            ScalarValue::Date32(Some(1234)),
        ];

        Python::with_gil(|py| {
            for scalar in example_scalars.iter() {
                let result = ScalarValue::from_pyarrow_bound(
                    scalar.to_pyarrow(py).unwrap().bind(py),
                )
                .unwrap();
                assert_eq!(scalar, &result);
            }
        });
    }

    #[test]
    fn test_py_scalar() {
        init_python();

        Python::with_gil(|py| {
            let scalar_float = ScalarValue::Float64(Some(12.34));
            let py_float = scalar_float.into_py(py).call_method0(py, "as_py").unwrap();
            py_run!(py, py_float, "assert py_float == 12.34");

            let scalar_string = ScalarValue::Utf8(Some("Hello!".to_string()));
            let py_string = scalar_string.into_py(py).call_method0(py, "as_py").unwrap();
            py_run!(py, py_string, "assert py_string == 'Hello!'");
        });
    }
}
