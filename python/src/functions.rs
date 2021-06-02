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

use crate::udaf;
use crate::udf;
use crate::{expression, types::PyDataType};
use datafusion::arrow::datatypes::DataType;
use datafusion::logical_plan;
use pyo3::{prelude::*, wrap_pyfunction};
use std::sync::Arc;

#[pyfunction]
fn array(value: Vec<expression::Expression>) -> expression::Expression {
    expression::Expression {
        expr: logical_plan::array(value.into_iter().map(|x| x.expr).collect::<Vec<_>>()),
    }
}

#[pyfunction]
fn in_list(
    expr: expression::Expression,
    value: Vec<expression::Expression>,
    negated: bool,
) -> expression::Expression {
    expression::Expression {
        expr: logical_plan::in_list(
            expr.expr,
            value.into_iter().map(|x| x.expr).collect::<Vec<_>>(),
            negated,
        ),
    }
}

macro_rules! define_function {
    ($NAME: ident) => {{
        /// Expression representing a $NAME function
        #[pyfunction]
        fn $NAME(value: expression::Expression) -> expression::Expression {
            expression::Expression {
                expr: logical_plan::$NAME(value.expr),
            }
        }
    }};
    ($NAME: ident, $SIGNATURE: expr) => {{
        /// Expression representing a $NAME function
        #[pyfunction]
        #[text_signature = $SIGNATURE]
        fn $NAME(value: expression::Expression) -> expression::Expression {
            expression::Expression {
                expr: logical_plan::$NAME(value.expr),
            }
        }
    }};
}

define_function!(col, "(name)");
define_function!(lit, "(value)");
define_function!(ascii);
define_function!(sum);
define_function!(bit_length);
define_function!(btrim);
define_function!(character_length);
define_function!(chr);
define_function!(concat_ws);
define_function!(initcap);
define_function!(left);
define_function!(lower);
define_function!(lpad);
define_function!(ltrim);
define_function!(md5);
define_function!(now);
define_function!(octet_length);
define_function!(random);
define_function!(replace);
define_function!(repeat);
define_function!(regexp_replace);
define_function!(reverse);
define_function!(right);
define_function!(rpad);
define_function!(rtrim);
define_function!(sha224);
define_function!(sha256);
define_function!(sha384);
define_function!(sha512);
define_function!(split_part);
define_function!(starts_with);
define_function!(strpos);
define_function!(substr);
define_function!(to_hex);
define_function!(translate);
define_function!(trim);
define_function!(upper);
define_function!(avg);
define_function!(min);
define_function!(max);
define_function!(count);

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
    module.add_function(wrap_pyfunction!(now, module)?)?;
    module.add_function(wrap_pyfunction!(ltrim, module)?)?;
    module.add_function(wrap_pyfunction!(octet_length, module)?)?;
    module.add_function(wrap_pyfunction!(random, module)?)?;
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
