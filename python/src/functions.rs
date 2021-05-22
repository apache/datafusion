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
fn array(value: Vec<expression::Expression>) -> expression::Expression {
    expression::Expression {
        expr: logical_plan::array(value.into_iter().map(|x| x.expr).collect::<Vec<_>>()),
    }
}

#[pyfunction]
fn ascii(value: expression::Expression) -> expression::Expression {
    expression::Expression {
        expr: logical_plan::ascii(value.expr),
    }
}

#[pyfunction]
fn sum(value: expression::Expression) -> expression::Expression {
    expression::Expression {
        expr: logical_plan::sum(value.expr),
    }
}

#[pyfunction]
fn bit_length(value: expression::Expression) -> expression::Expression {
    expression::Expression {
        expr: logical_plan::bit_length(value.expr),
    }
}

#[pyfunction]
fn btrim(value: expression::Expression) -> expression::Expression {
    expression::Expression {
        expr: logical_plan::btrim(value.expr),
    }
}

#[pyfunction]
fn character_length(value: expression::Expression) -> expression::Expression {
    expression::Expression {
        expr: logical_plan::character_length(value.expr),
    }
}

#[pyfunction]
fn chr(value: expression::Expression) -> expression::Expression {
    expression::Expression {
        expr: logical_plan::chr(value.expr),
    }
}

#[pyfunction]
fn concat_ws(value: expression::Expression) -> expression::Expression {
    expression::Expression {
        expr: logical_plan::concat_ws(value.expr),
    }
}

#[pyfunction]
fn in_list(expr: expression::Expression, value: Vec<expression::Expression>, negated: bool) -> expression::Expression {
    expression::Expression {
        expr: logical_plan::in_list(expr.expr, value.into_iter().map(|x| x.expr).collect::<Vec<_>>(), negated),
    }
}

#[pyfunction]
fn initcap(value: expression::Expression) -> expression::Expression {
    expression::Expression {
        expr: logical_plan::initcap(value.expr),
    }
}

#[pyfunction]
fn left(value: expression::Expression) -> expression::Expression {
    expression::Expression {
        expr: logical_plan::left(value.expr),
    }
}

#[pyfunction]
fn lower(value: expression::Expression) -> expression::Expression {
    expression::Expression {
        expr: logical_plan::lower(value.expr),
    }
}

#[pyfunction]
fn lpad(value: expression::Expression) -> expression::Expression {
    expression::Expression {
        expr: logical_plan::lpad(value.expr),
    }
}

#[pyfunction]
fn ltrim(value: expression::Expression) -> expression::Expression {
    expression::Expression {
        expr: logical_plan::ltrim(value.expr),
    }
}

#[pyfunction]
fn md5(value: expression::Expression) -> expression::Expression {
    expression::Expression {
        expr: logical_plan::md5(value.expr),
    }
}

#[pyfunction]
fn octet_length(value: expression::Expression) -> expression::Expression {
    expression::Expression {
        expr: logical_plan::octet_length(value.expr),
    }
}

#[pyfunction]
fn regexp_replace(value: expression::Expression) -> expression::Expression {
    expression::Expression {
        expr: logical_plan::regexp_replace(value.expr),
    }
}

#[pyfunction]
fn repeat(value: expression::Expression) -> expression::Expression {
    expression::Expression {
        expr: logical_plan::repeat(value.expr),
    }
}

#[pyfunction]
fn replace(value: expression::Expression) -> expression::Expression {
    expression::Expression {
        expr: logical_plan::replace(value.expr),
    }
}

#[pyfunction]
fn reverse(value: expression::Expression) -> expression::Expression {
    expression::Expression {
        expr: logical_plan::reverse(value.expr),
    }
}

#[pyfunction]
fn right(value: expression::Expression) -> expression::Expression {
    expression::Expression {
        expr: logical_plan::right(value.expr),
    }
}

#[pyfunction]
fn rpad(value: expression::Expression) -> expression::Expression {
    expression::Expression {
        expr: logical_plan::rpad(value.expr),
    }
}

#[pyfunction]
fn rtrim(value: expression::Expression) -> expression::Expression {
    expression::Expression {
        expr: logical_plan::rtrim(value.expr),
    }
}

#[pyfunction]
fn sha224(value: expression::Expression) -> expression::Expression {
    expression::Expression {
        expr: logical_plan::sha224(value.expr),
    }
}

#[pyfunction]
fn sha256(value: expression::Expression) -> expression::Expression {
    expression::Expression {
        expr: logical_plan::sha256(value.expr),
    }
}

#[pyfunction]
fn sha384(value: expression::Expression) -> expression::Expression {
    expression::Expression {
        expr: logical_plan::sha384(value.expr),
    }
}

#[pyfunction]
fn sha512(value: expression::Expression) -> expression::Expression {
    expression::Expression {
        expr: logical_plan::sha512(value.expr),
    }
}

#[pyfunction]
fn split_part(value: expression::Expression) -> expression::Expression {
    expression::Expression {
        expr: logical_plan::split_part(value.expr),
    }
}

#[pyfunction]
fn starts_with(value: expression::Expression) -> expression::Expression {
    expression::Expression {
        expr: logical_plan::starts_with(value.expr),
    }
}

#[pyfunction]
fn strpos(value: expression::Expression) -> expression::Expression {
    expression::Expression {
        expr: logical_plan::strpos(value.expr),
    }
}

#[pyfunction]
fn substr(value: expression::Expression) -> expression::Expression {
    expression::Expression {
        expr: logical_plan::substr(value.expr),
    }
}

#[pyfunction]
fn to_hex(value: expression::Expression) -> expression::Expression {
    expression::Expression {
        expr: logical_plan::to_hex(value.expr),
    }
}

#[pyfunction]
fn translate(value: expression::Expression) -> expression::Expression {
    expression::Expression {
        expr: logical_plan::translate(value.expr),
    }
}

#[pyfunction]
fn trim(value: expression::Expression) -> expression::Expression {
    expression::Expression {
        expr: logical_plan::trim(value.expr),
    }
}

#[pyfunction]
fn upper(value: expression::Expression) -> expression::Expression {
    expression::Expression {
        expr: logical_plan::upper(value.expr),
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
    module.add_function(wrap_pyfunction!(array, module)?)?;
    module.add_function(wrap_pyfunction!(ascii, module)?)?;
    module.add_function(wrap_pyfunction!(bit_length, module)?)?;
    module.add_function(wrap_pyfunction!(character_length, module)?)?;
    module.add_function(wrap_pyfunction!(chr, module)?)?;
    module.add_function(wrap_pyfunction!(btrim, module)?)?;
    module.add_function(wrap_pyfunction!(concat_ws, module)?)?;
    module.add_function(wrap_pyfunction!(in_list, module)?)?;
    module.add_function(wrap_pyfunction!(initcap, module)?)?;
    module.add_function(wrap_pyfunction!(left, module)?)?;
    module.add_function(wrap_pyfunction!(lower, module)?)?;
    module.add_function(wrap_pyfunction!(lpad, module)?)?;
    module.add_function(wrap_pyfunction!(md5, module)?)?;
    module.add_function(wrap_pyfunction!(ltrim, module)?)?;
    module.add_function(wrap_pyfunction!(octet_length, module)?)?;
    module.add_function(wrap_pyfunction!(regexp_replace, module)?)?;
    module.add_function(wrap_pyfunction!(repeat, module)?)?;
    module.add_function(wrap_pyfunction!(replace, module)?)?;
    module.add_function(wrap_pyfunction!(reverse, module)?)?;
    module.add_function(wrap_pyfunction!(right, module)?)?;
    module.add_function(wrap_pyfunction!(rpad, module)?)?;
    module.add_function(wrap_pyfunction!(rtrim, module)?)?;
    module.add_function(wrap_pyfunction!(sha224, module)?)?;
    module.add_function(wrap_pyfunction!(sha256, module)?)?;
    module.add_function(wrap_pyfunction!(sha384, module)?)?;
    module.add_function(wrap_pyfunction!(sha512, module)?)?;
    module.add_function(wrap_pyfunction!(split_part, module)?)?;
    module.add_function(wrap_pyfunction!(starts_with, module)?)?;
    module.add_function(wrap_pyfunction!(strpos, module)?)?;
    module.add_function(wrap_pyfunction!(substr, module)?)?;
    module.add_function(wrap_pyfunction!(to_hex, module)?)?;
    module.add_function(wrap_pyfunction!(translate, module)?)?;
    module.add_function(wrap_pyfunction!(trim, module)?)?;
    module.add_function(wrap_pyfunction!(upper, module)?)?;
    module.add_function(wrap_pyfunction!(sum, module)?)?;
    module.add_function(wrap_pyfunction!(count, module)?)?;
    module.add_function(wrap_pyfunction!(min, module)?)?;
    module.add_function(wrap_pyfunction!(max, module)?)?;
    module.add_function(wrap_pyfunction!(avg, module)?)?;
    module.add_function(wrap_pyfunction!(udaf, module)?)?;
    Ok(())
}
