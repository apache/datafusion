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

use datafusion::arrow::datatypes::DataType;
use pyo3::{prelude::*, wrap_pyfunction};

use datafusion::logical_plan;

use crate::udaf;
use crate::udf;
use crate::{expression, types::PyDataType};

/// Expression representing a column on the existing plan.
#[pyfunction]
#[text_signature = "(name)"]
fn col(name: &str) -> expression::Expression {
    expression::Expression {
        expr: logical_plan::col(name),
    }
}

/// Expression representing a constant value
#[pyfunction]
#[text_signature = "(value)"]
fn lit(value: i32) -> expression::Expression {
    expression::Expression {
        expr: logical_plan::lit(value),
    }
}

#[pyfunction]
fn sum(value: expression::Expression) -> expression::Expression {
    expression::Expression {
        expr: logical_plan::sum(value.expr),
    }
}

#[pyfunction]
fn avg(value: expression::Expression) -> expression::Expression {
    expression::Expression {
        expr: logical_plan::avg(value.expr),
    }
}

#[pyfunction]
fn min(value: expression::Expression) -> expression::Expression {
    expression::Expression {
        expr: logical_plan::min(value.expr),
    }
}

#[pyfunction]
fn max(value: expression::Expression) -> expression::Expression {
    expression::Expression {
        expr: logical_plan::max(value.expr),
    }
}

#[pyfunction]
fn count(value: expression::Expression) -> expression::Expression {
    expression::Expression {
        expr: logical_plan::count(value.expr),
    }
}

/*
#[pyfunction]
fn concat(value: Vec<expression::Expression>) -> expression::Expression {
    expression::Expression {
        expr: logical_plan::concat(value.into_iter().map(|e| e.expr)),
    }
}
 */

pub(crate) fn create_udf(
    fun: PyObject,
    input_types: Vec<PyDataType>,
    return_type: PyDataType,
    name: &str,
) -> expression::ScalarUDF {
    let input_types: Vec<DataType> =
        input_types.iter().map(|d| d.data_type.clone()).collect();
    let return_type = Arc::new(return_type.data_type);

    expression::ScalarUDF {
        function: logical_plan::create_udf(
            name,
            input_types,
            return_type,
            udf::array_udf(fun),
        ),
    }
}

/// Creates a new udf.
#[pyfunction]
fn udf(
    fun: PyObject,
    input_types: Vec<PyDataType>,
    return_type: PyDataType,
    py: Python,
) -> PyResult<expression::ScalarUDF> {
    let name = fun.getattr(py, "__qualname__")?.extract::<String>(py)?;

    Ok(create_udf(fun, input_types, return_type, &name))
}

/// Creates a new udf.
#[pyfunction]
fn udaf(
    accumulator: PyObject,
    input_type: PyDataType,
    return_type: PyDataType,
    state_type: Vec<PyDataType>,
    py: Python,
) -> PyResult<expression::AggregateUDF> {
    let name = accumulator
        .getattr(py, "__qualname__")?
        .extract::<String>(py)?;

    let input_type = input_type.data_type;
    let return_type = Arc::new(return_type.data_type);
    let state_type = Arc::new(state_type.into_iter().map(|t| t.data_type).collect());

    Ok(expression::AggregateUDF {
        function: logical_plan::create_udaf(
            &name,
            input_type,
            return_type,
            udaf::array_udaf(accumulator),
            state_type,
        ),
    })
}

pub fn init(module: &PyModule) -> PyResult<()> {
    module.add_function(wrap_pyfunction!(col, module)?)?;
    module.add_function(wrap_pyfunction!(lit, module)?)?;
    // see https://github.com/apache/arrow-datafusion/issues/226
    //module.add_function(wrap_pyfunction!(concat, module)?)?;
    module.add_function(wrap_pyfunction!(udf, module)?)?;
    module.add_function(wrap_pyfunction!(sum, module)?)?;
    module.add_function(wrap_pyfunction!(count, module)?)?;
    module.add_function(wrap_pyfunction!(min, module)?)?;
    module.add_function(wrap_pyfunction!(max, module)?)?;
    module.add_function(wrap_pyfunction!(avg, module)?)?;
    module.add_function(wrap_pyfunction!(udaf, module)?)?;
    Ok(())
}
