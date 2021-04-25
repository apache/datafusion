use std::sync::Arc;

use pyo3::{prelude::*, types::PyTuple};

use arrow::array;

use datafusion::error::DataFusionError;
use datafusion::physical_plan::functions::ScalarFunctionImplementation;

use crate::to_py::to_py_array;
use crate::to_rust::to_rust;

/// creates a DataFusion's UDF implementation from a python function that expects pyarrow arrays
/// This is more efficient as it performs a zero-copy of the contents.
pub fn array_udf(func: PyObject) -> ScalarFunctionImplementation {
    Arc::new(
        move |args: &[array::ArrayRef]| -> Result<array::ArrayRef, DataFusionError> {
            // get GIL
            let gil = pyo3::Python::acquire_gil();
            let py = gil.python();

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
                Err(error) => Err(DataFusionError::Execution(
                    format!("{:?}", error).to_owned(),
                )),
            }?;

            let array = to_rust(value).unwrap();
            Ok(array)
        },
    )
}
