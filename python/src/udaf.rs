use std::sync::Arc;

use pyo3::{prelude::*, types::PyTuple};

use arrow::array;

use datafusion::error::Result;
use datafusion::{error::DataFusionError as InnerDataFusionError, physical_plan::Accumulator};

use crate::scalar::Scalar;
use crate::to_py::to_py_array;
use crate::to_rust::to_rust_scalar;

#[derive(Debug)]
struct PyAccumulator {
    accum: PyObject,
}

impl PyAccumulator {
    fn new(accum: PyObject) -> Self {
        Self { accum }
    }
}

impl Accumulator for PyAccumulator {
    fn state(&self) -> Result<Vec<datafusion::scalar::ScalarValue>> {
        let gil = pyo3::Python::acquire_gil();
        let py = gil.python();

        let state = self
            .accum
            .as_ref(py)
            .call_method0("to_scalars")
            .map_err(|e| InnerDataFusionError::Execution(format!("{}", e)))?
            .extract::<Vec<Scalar>>()
            .map_err(|e| InnerDataFusionError::Execution(format!("{}", e)))?;

        Ok(state.into_iter().map(|v| v.scalar).collect::<Vec<_>>())
    }

    fn update(&mut self, _values: &Vec<datafusion::scalar::ScalarValue>) -> Result<()> {
        // no need to implement as datafusion does not use it
        todo!()
    }

    fn merge(&mut self, _states: &Vec<datafusion::scalar::ScalarValue>) -> Result<()> {
        // no need to implement as datafusion does not use it
        todo!()
    }

    fn evaluate(&self) -> Result<datafusion::scalar::ScalarValue> {
        // get GIL
        let gil = pyo3::Python::acquire_gil();
        let py = gil.python();

        let value = self
            .accum
            .as_ref(py)
            .call_method0("evaluate")
            .map_err(|e| InnerDataFusionError::Execution(format!("{}", e)))?;

        to_rust_scalar(value).map_err(|e| InnerDataFusionError::Execution(format!("{}", e)))
    }

    fn update_batch(&mut self, values: &Vec<array::ArrayRef>) -> Result<()> {
        // get GIL
        let gil = pyo3::Python::acquire_gil();
        let py = gil.python();

        // 1. cast args to Pyarrow array
        // 2. call function

        // 1.
        let py_args = values
            .iter()
            .map(|arg| {
                // remove unwrap
                to_py_array(arg, py).unwrap()
            })
            .collect::<Vec<_>>();
        let py_args = PyTuple::new(py, py_args);

        // update accumulator
        self.accum
            .as_ref(py)
            .call_method1("update", py_args)
            .map_err(|e| InnerDataFusionError::Execution(format!("{}", e)))?;

        Ok(())
    }

    fn merge_batch(&mut self, states: &Vec<array::ArrayRef>) -> Result<()> {
        // get GIL
        let gil = pyo3::Python::acquire_gil();
        let py = gil.python();

        // 1. cast states to Pyarrow array
        // 2. merge
        let state = &states[0];

        let state = to_py_array(state, py)
            .map_err(|e| InnerDataFusionError::Execution(format!("{}", e)))?;

        // 2.
        self.accum
            .as_ref(py)
            .call_method1("merge", (state,))
            .map_err(|e| InnerDataFusionError::Execution(format!("{}", e)))?;

        Ok(())
    }
}

pub fn array_udaf(
    accumulator: PyObject,
) -> Arc<dyn Fn() -> Result<Box<dyn Accumulator>> + Send + Sync> {
    let accumulator = accumulator.clone();
    Arc::new(move || -> Result<Box<dyn Accumulator>> {
        let gil = pyo3::Python::acquire_gil();
        let py = gil.python();

        let accumulator = accumulator
            .call0(py)
            .map_err(|e| InnerDataFusionError::Execution(format!("{}", e)))?;
        Ok(Box::new(PyAccumulator::new(accumulator)))
    })
}
