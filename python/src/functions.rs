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
use pyo3::{prelude::*, types::PyTuple, wrap_pyfunction};
use std::sync::Arc;

/// Expression representing a column on the existing plan.
#[pyfunction]
#[pyo3(text_signature = "(name)")]
fn col(name: &str) -> expression::Expression {
    expression::Expression {
        expr: logical_plan::col(name),
    }
}

/// Expression representing a constant value
#[pyfunction]
#[pyo3(text_signature = "(value)")]
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

/// Current date and time
#[pyfunction]
fn now() -> expression::Expression {
    expression::Expression {
        // here lit(0) is a stub for conform to arity
        expr: logical_plan::now(logical_plan::lit(0)),
    }
}

/// Returns a random value in the range 0.0 <= x < 1.0
#[pyfunction]
fn random() -> expression::Expression {
    expression::Expression {
        expr: logical_plan::random(),
    }
}

/// Concatenates the text representations of all the arguments.
/// NULL arguments are ignored.
#[pyfunction(args = "*")]
fn concat(args: &PyTuple) -> PyResult<expression::Expression> {
    let expressions = expression::from_tuple(args)?;
    let args = expressions.into_iter().map(|e| e.expr).collect::<Vec<_>>();
    Ok(expression::Expression {
        expr: logical_plan::concat(&args),
    })
}

/// Concatenates all but the first argument, with separators.
/// The first argument is used as the separator string, and should not be NULL.
/// Other NULL arguments are ignored.
#[pyfunction(sep, args = "*")]
fn concat_ws(sep: String, args: &PyTuple) -> PyResult<expression::Expression> {
    let expressions = expression::from_tuple(args)?;
    let args = expressions.into_iter().map(|e| e.expr).collect::<Vec<_>>();
    Ok(expression::Expression {
        expr: logical_plan::concat_ws(sep, &args),
    })
}

macro_rules! define_unary_function {
    ($NAME: ident) => {
        #[doc = "This function is not documented yet"]
        #[pyfunction]
        fn $NAME(value: expression::Expression) -> expression::Expression {
            expression::Expression {
                expr: logical_plan::$NAME(value.expr),
            }
        }
    };
    ($NAME: ident, $DOC: expr) => {
        #[doc = $DOC]
        #[pyfunction]
        fn $NAME(value: expression::Expression) -> expression::Expression {
            expression::Expression {
                expr: logical_plan::$NAME(value.expr),
            }
        }
    };
}

define_unary_function!(sqrt, "sqrt");
define_unary_function!(sin, "sin");
define_unary_function!(cos, "cos");
define_unary_function!(tan, "tan");
define_unary_function!(asin, "asin");
define_unary_function!(acos, "acos");
define_unary_function!(atan, "atan");
define_unary_function!(floor, "floor");
define_unary_function!(ceil, "ceil");
define_unary_function!(round, "round");
define_unary_function!(trunc, "trunc");
define_unary_function!(abs, "abs");
define_unary_function!(signum, "signum");
define_unary_function!(exp, "exp");
define_unary_function!(ln, "ln");
define_unary_function!(log2, "log2");
define_unary_function!(log10, "log10");

define_unary_function!(ascii, "Returns the numeric code of the first character of the argument. In UTF8 encoding, returns the Unicode code point of the character. In other multibyte encodings, the argument must be an ASCII character.");
define_unary_function!(sum);
define_unary_function!(
    bit_length,
    "Returns number of bits in the string (8 times the octet_length)."
);
define_unary_function!(btrim, "Removes the longest string containing only characters in characters (a space by default) from the start and end of string.");
define_unary_function!(
    character_length,
    "Returns number of characters in the string."
);
define_unary_function!(chr, "Returns the character with the given code.");
define_unary_function!(initcap, "Converts the first letter of each word to upper case and the rest to lower case. Words are sequences of alphanumeric characters separated by non-alphanumeric characters.");
define_unary_function!(left, "Returns first n characters in the string, or when n is negative, returns all but last |n| characters.");
define_unary_function!(lower, "Converts the string to all lower case");
define_unary_function!(lpad, "Extends the string to length length by prepending the characters fill (a space by default). If the string is already longer than length then it is truncated (on the right).");
define_unary_function!(ltrim, "Removes the longest string containing only characters in characters (a space by default) from the start of string.");
define_unary_function!(
    md5,
    "Computes the MD5 hash of the argument, with the result written in hexadecimal."
);
define_unary_function!(octet_length, "Returns number of bytes in the string. Since this version of the function accepts type character directly, it will not strip trailing spaces.");
define_unary_function!(
    replace,
    "Replaces all occurrences in string of substring from with substring to."
);
define_unary_function!(repeat, "Repeats string the specified number of times.");
define_unary_function!(
    regexp_replace,
    "Replaces substring(s) matching a POSIX regular expression"
);
define_unary_function!(
    reverse,
    "Reverses the order of the characters in the string."
);
define_unary_function!(right, "Returns last n characters in the string, or when n is negative, returns all but first |n| characters.");
define_unary_function!(rpad, "Extends the string to length length by appending the characters fill (a space by default). If the string is already longer than length then it is truncated.");
define_unary_function!(rtrim, "Removes the longest string containing only characters in characters (a space by default) from the end of string.");
define_unary_function!(sha224);
define_unary_function!(sha256);
define_unary_function!(sha384);
define_unary_function!(sha512);
define_unary_function!(split_part, "Splits string at occurrences of delimiter and returns the n'th field (counting from one).");
define_unary_function!(starts_with, "Returns true if string starts with prefix.");
define_unary_function!(strpos,"Returns starting index of specified substring within string, or zero if it's not present. (Same as position(substring in string), but note the reversed argument order.)");
define_unary_function!(substr);
define_unary_function!(
    to_hex,
    "Converts the number to its equivalent hexadecimal representation."
);
define_unary_function!(translate, "Replaces each character in string that matches a character in the from set with the corresponding character in the to set. If from is longer than to, occurrences of the extra characters in from are deleted.");
define_unary_function!(trim, "Removes the longest string containing only characters in characters (a space by default) from the start, end, or both ends (BOTH is the default) of string.");
define_unary_function!(upper, "Converts the string to all upper case.");
define_unary_function!(avg);
define_unary_function!(min);
define_unary_function!(max);
define_unary_function!(count);

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
    module.add_function(wrap_pyfunction!(abs, module)?)?;
    module.add_function(wrap_pyfunction!(acos, module)?)?;
    module.add_function(wrap_pyfunction!(array, module)?)?;
    module.add_function(wrap_pyfunction!(ascii, module)?)?;
    module.add_function(wrap_pyfunction!(asin, module)?)?;
    module.add_function(wrap_pyfunction!(atan, module)?)?;
    module.add_function(wrap_pyfunction!(avg, module)?)?;
    module.add_function(wrap_pyfunction!(bit_length, module)?)?;
    module.add_function(wrap_pyfunction!(btrim, module)?)?;
    module.add_function(wrap_pyfunction!(ceil, module)?)?;
    module.add_function(wrap_pyfunction!(character_length, module)?)?;
    module.add_function(wrap_pyfunction!(chr, module)?)?;
    module.add_function(wrap_pyfunction!(col, module)?)?;
    module.add_function(wrap_pyfunction!(concat_ws, module)?)?;
    module.add_function(wrap_pyfunction!(concat, module)?)?;
    module.add_function(wrap_pyfunction!(cos, module)?)?;
    module.add_function(wrap_pyfunction!(count, module)?)?;
    module.add_function(wrap_pyfunction!(exp, module)?)?;
    module.add_function(wrap_pyfunction!(floor, module)?)?;
    module.add_function(wrap_pyfunction!(in_list, module)?)?;
    module.add_function(wrap_pyfunction!(initcap, module)?)?;
    module.add_function(wrap_pyfunction!(left, module)?)?;
    module.add_function(wrap_pyfunction!(lit, module)?)?;
    module.add_function(wrap_pyfunction!(ln, module)?)?;
    module.add_function(wrap_pyfunction!(log10, module)?)?;
    module.add_function(wrap_pyfunction!(log2, module)?)?;
    module.add_function(wrap_pyfunction!(lower, module)?)?;
    module.add_function(wrap_pyfunction!(lpad, module)?)?;
    module.add_function(wrap_pyfunction!(ltrim, module)?)?;
    module.add_function(wrap_pyfunction!(max, module)?)?;
    module.add_function(wrap_pyfunction!(md5, module)?)?;
    module.add_function(wrap_pyfunction!(min, module)?)?;
    module.add_function(wrap_pyfunction!(now, module)?)?;
    module.add_function(wrap_pyfunction!(octet_length, module)?)?;
    module.add_function(wrap_pyfunction!(random, module)?)?;
    module.add_function(wrap_pyfunction!(regexp_replace, module)?)?;
    module.add_function(wrap_pyfunction!(repeat, module)?)?;
    module.add_function(wrap_pyfunction!(replace, module)?)?;
    module.add_function(wrap_pyfunction!(reverse, module)?)?;
    module.add_function(wrap_pyfunction!(right, module)?)?;
    module.add_function(wrap_pyfunction!(round, module)?)?;
    module.add_function(wrap_pyfunction!(rpad, module)?)?;
    module.add_function(wrap_pyfunction!(rtrim, module)?)?;
    module.add_function(wrap_pyfunction!(sha224, module)?)?;
    module.add_function(wrap_pyfunction!(sha256, module)?)?;
    module.add_function(wrap_pyfunction!(sha384, module)?)?;
    module.add_function(wrap_pyfunction!(sha512, module)?)?;
    module.add_function(wrap_pyfunction!(signum, module)?)?;
    module.add_function(wrap_pyfunction!(sin, module)?)?;
    module.add_function(wrap_pyfunction!(split_part, module)?)?;
    module.add_function(wrap_pyfunction!(sqrt, module)?)?;
    module.add_function(wrap_pyfunction!(starts_with, module)?)?;
    module.add_function(wrap_pyfunction!(strpos, module)?)?;
    module.add_function(wrap_pyfunction!(substr, module)?)?;
    module.add_function(wrap_pyfunction!(sum, module)?)?;
    module.add_function(wrap_pyfunction!(tan, module)?)?;
    module.add_function(wrap_pyfunction!(to_hex, module)?)?;
    module.add_function(wrap_pyfunction!(translate, module)?)?;
    module.add_function(wrap_pyfunction!(trim, module)?)?;
    module.add_function(wrap_pyfunction!(trunc, module)?)?;
    module.add_function(wrap_pyfunction!(udaf, module)?)?;
    module.add_function(wrap_pyfunction!(udf, module)?)?;
    module.add_function(wrap_pyfunction!(upper, module)?)?;

    Ok(())
}
