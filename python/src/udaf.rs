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

use std::sync::Arc;

use pyo3::{prelude::*, types::PyTuple};

use datafusion::arrow::array::ArrayRef;

use datafusion::error::Result;
use datafusion::{
    error::DataFusionError as InnerDataFusionError, physical_plan::Accumulator,
    scalar::ScalarValue,
};

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
        Python::with_gil(|py| {
            let state = self
                .accum
                .as_ref(py)
                .call_method0("to_scalars")
                .map_err(|e| InnerDataFusionError::Execution(format!("{}", e)))?
                .extract::<Vec<Scalar>>()
                .map_err(|e| InnerDataFusionError::Execution(format!("{}", e)))?;

            Ok(state.into_iter().map(|v| v.scalar).collect::<Vec<_>>())
        })
    }

    fn update(&mut self, _values: &[ScalarValue]) -> Result<()> {
        // no need to implement as datafusion does not use it
        todo!()
    }

    fn merge(&mut self, _states: &[ScalarValue]) -> Result<()> {
        // no need to implement as datafusion does not use it
        todo!()
    }

    fn evaluate(&self) -> Result<datafusion::scalar::ScalarValue> {
        Python::with_gil(|py| {
            let value = self
                .accum
                .as_ref(py)
                .call_method0("evaluate")
                .map_err(|e| InnerDataFusionError::Execution(format!("{}", e)))?;

            to_rust_scalar(value)
                .map_err(|e| InnerDataFusionError::Execution(format!("{}", e)))
        })
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        Python::with_gil(|py| {
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
        })
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        Python::with_gil(|py| {
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
        })
    }
}

pub fn array_udaf(
    accumulator: PyObject,
) -> Arc<dyn Fn() -> Result<Box<dyn Accumulator>> + Send + Sync> {
    Arc::new(move || -> Result<Box<dyn Accumulator>> {
        let accumulator = Python::with_gil(|py| {
            accumulator
                .call0(py)
                .map_err(|e| InnerDataFusionError::Execution(format!("{}", e)))
        })?;
        Ok(Box::new(PyAccumulator::new(accumulator)))
    })
}
