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
use datafusion::arrow::datatypes::DataType;
use datafusion::arrow::pyarrow::PyArrowConvert;
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_plan;
use datafusion::physical_plan::aggregates::AccumulatorFunctionImplementation;
use datafusion::physical_plan::udaf::AggregateUDF;
use datafusion::physical_plan::Accumulator;
use datafusion::scalar::ScalarValue;

use crate::expression::PyExpr;
use crate::utils::parse_volatility;

#[derive(Debug)]
struct RustAccumulator {
    accum: PyObject,
}

impl RustAccumulator {
    fn new(accum: PyObject) -> Self {
        Self { accum }
    }
}

impl Accumulator for RustAccumulator {
    fn state(&self) -> Result<Vec<ScalarValue>> {
        Python::with_gil(|py| self.accum.as_ref(py).call_method0("state")?.extract())
            .map_err(|e| DataFusionError::Execution(format!("{}", e)))
    }

    fn update(&mut self, _values: &[ScalarValue]) -> Result<()> {
        // no need to implement as datafusion does not use it
        todo!()
    }

    fn merge(&mut self, _states: &[ScalarValue]) -> Result<()> {
        // no need to implement as datafusion does not use it
        todo!()
    }

    fn evaluate(&self) -> Result<ScalarValue> {
        Python::with_gil(|py| self.accum.as_ref(py).call_method0("evaluate")?.extract())
            .map_err(|e| DataFusionError::Execution(format!("{}", e)))
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        Python::with_gil(|py| {
            // 1. cast args to Pyarrow array
            let py_args = values
                .iter()
                .map(|arg| arg.data().to_owned().to_pyarrow(py).unwrap())
                .collect::<Vec<_>>();
            let py_args = PyTuple::new(py, py_args);

            // 2. call function
            self.accum
                .as_ref(py)
                .call_method1("update", py_args)
                .map_err(|e| DataFusionError::Execution(format!("{}", e)))?;

            Ok(())
        })
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        Python::with_gil(|py| {
            let state = &states[0];

            // 1. cast states to Pyarrow array
            let state = state
                .to_pyarrow(py)
                .map_err(|e| DataFusionError::Execution(format!("{}", e)))?;

            // 2. call merge
            self.accum
                .as_ref(py)
                .call_method1("merge", (state,))
                .map_err(|e| DataFusionError::Execution(format!("{}", e)))?;

            Ok(())
        })
    }
}

pub fn to_rust_accumulator(accum: PyObject) -> AccumulatorFunctionImplementation {
    Arc::new(move || -> Result<Box<dyn Accumulator>> {
        let accum = Python::with_gil(|py| {
            accum
                .call0(py)
                .map_err(|e| DataFusionError::Execution(format!("{}", e)))
        })?;
        Ok(Box::new(RustAccumulator::new(accum)))
    })
}

/// Represents a AggregateUDF
#[pyclass(name = "AggregateUDF", module = "datafusion", subclass)]
#[derive(Debug, Clone)]
pub struct PyAggregateUDF {
    pub(crate) function: AggregateUDF,
}

#[pymethods]
impl PyAggregateUDF {
    #[new(name, accumulator, input_type, return_type, state_type, volatility)]
    fn new(
        name: &str,
        accumulator: PyObject,
        input_type: DataType,
        return_type: DataType,
        state_type: Vec<DataType>,
        volatility: &str,
    ) -> PyResult<Self> {
        let function = logical_plan::create_udaf(
            &name,
            input_type,
            Arc::new(return_type),
            parse_volatility(volatility)?,
            to_rust_accumulator(accumulator),
            Arc::new(state_type),
        );
        Ok(Self { function })
    }

    /// creates a new PyExpr with the call of the udf
    #[call]
    #[args(args = "*")]
    fn __call__(&self, args: Vec<PyExpr>) -> PyResult<PyExpr> {
        let args = args.iter().map(|e| e.expr.clone()).collect();
        Ok(self.function.call(args).into())
    }
}
