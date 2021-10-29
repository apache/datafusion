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
use datafusion::error::DataFusionError;
use datafusion::logical_plan;
use datafusion::physical_plan::functions::ScalarFunctionImplementation;
use datafusion::physical_plan::{
    functions::Volatility, udaf::AggregateUDF, udf::ScalarUDF,
};
use datafusion::{arrow::array, physical_plan::functions::make_scalar_function};

use crate::errors;
use crate::expression::PyExpr;

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
                    .map(|arg| arg.data().to_owned().to_pyarrow(py).unwrap())
                    .collect::<Vec<_>>();
                let py_args = PyTuple::new(py, py_args);

                // 2.
                let value = func.as_ref(py).call(py_args, None);
                let value = match value {
                    Ok(n) => Ok(n),
                    Err(error) => Err(DataFusionError::Execution(format!("{:?}", error))),
                }?;

                let array = ArrayRef::from_pyarrow(value).unwrap();
                Ok(array)
            })
        },
    )
}

/// Represents a PyScalarUDF
#[pyclass]
#[derive(Debug, Clone)]
pub struct PyScalarUDF {
    pub(crate) function: ScalarUDF,
}

#[pymethods]
impl PyScalarUDF {
    //#args(args)
    fn new(
        fun: PyObject,
        input_types: Vec<DataType>,
        return_type: DataType,
        volatility: &str,
        name: &str,
    ) -> PyResult<Self> {
        let volatility = match volatility {
            "immutable" => Volatility::Immutable,
            "stable" => Volatility::Stable,
            "volatile" => Volatility::Volatile,
            value => {
                return Err(errors::DataFusionError::Common(format!(
                    "Unsupportad volatility type: `{}`, supported values are: immutable, stable and volatile.",
                    value
                )).into())
            }
        };
        let function = logical_plan::create_udf(
            name,
            input_types,
            Arc::new(return_type),
            volatility,
            array_udf(fun),
        );
        Ok(PyScalarUDF { function })
    }

    /// creates a new PyExpr with the call of the udf
    #[call]
    #[args(args = "*")]
    fn __call__(&self, args: Vec<PyExpr>) -> PyResult<PyExpr> {
        let args = args.iter().map(|e| e.expr.clone()).collect();
        Ok(self.function.call(args).into())
    }
}
