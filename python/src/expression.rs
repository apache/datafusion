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

use pyo3::{
    basic::CompareOp, prelude::*, types::PyTuple, PyNumberProtocol, PyObjectProtocol,
};

use datafusion::logical_plan::Expr;
use datafusion::physical_plan::{udaf::AggregateUDF, udf::ScalarUDF};

/// An PyExpr that can be used on a DataFrame
#[pyclass]
#[derive(Debug, Clone)]
pub(crate) struct PyExpr {
    pub(crate) expr: Expr,
}

/// converts a tuple of expressions into a vector of Expressions
pub(crate) fn from_tuple(value: &PyTuple) -> PyResult<Vec<PyExpr>> {
    value
        .iter()
        .map(|e| e.extract::<PyExpr>())
        .collect::<PyResult<_>>()
}

#[pyproto]
impl PyNumberProtocol for PyExpr {
    fn __add__(lhs: PyExpr, rhs: PyExpr) -> PyResult<PyExpr> {
        Ok(PyExpr {
            expr: lhs.expr + rhs.expr,
        })
    }

    fn __sub__(lhs: PyExpr, rhs: PyExpr) -> PyResult<PyExpr> {
        Ok(PyExpr {
            expr: lhs.expr - rhs.expr,
        })
    }

    fn __truediv__(lhs: PyExpr, rhs: PyExpr) -> PyResult<PyExpr> {
        Ok(PyExpr {
            expr: lhs.expr / rhs.expr,
        })
    }

    fn __mul__(lhs: PyExpr, rhs: PyExpr) -> PyResult<PyExpr> {
        Ok(PyExpr {
            expr: lhs.expr * rhs.expr,
        })
    }

    fn __and__(lhs: PyExpr, rhs: PyExpr) -> PyResult<PyExpr> {
        Ok(PyExpr {
            expr: lhs.expr.and(rhs.expr),
        })
    }

    fn __or__(lhs: PyExpr, rhs: PyExpr) -> PyResult<PyExpr> {
        Ok(PyExpr {
            expr: lhs.expr.or(rhs.expr),
        })
    }

    fn __invert__(&self) -> PyResult<PyExpr> {
        Ok(PyExpr {
            expr: self.expr.clone().not(),
        })
    }
}

#[pyproto]
impl PyObjectProtocol for PyExpr {
    fn __richcmp__(&self, other: PyExpr, op: CompareOp) -> PyExpr {
        match op {
            CompareOp::Lt => PyExpr {
                expr: self.expr.clone().lt(other.expr),
            },
            CompareOp::Le => PyExpr {
                expr: self.expr.clone().lt_eq(other.expr),
            },
            CompareOp::Eq => PyExpr {
                expr: self.expr.clone().eq(other.expr),
            },
            CompareOp::Ne => PyExpr {
                expr: self.expr.clone().not_eq(other.expr),
            },
            CompareOp::Gt => PyExpr {
                expr: self.expr.clone().gt(other.expr),
            },
            CompareOp::Ge => PyExpr {
                expr: self.expr.clone().gt_eq(other.expr),
            },
        }
    }
}

#[pymethods]
impl PyExpr {
    /// assign a name to the PyExpr
    pub fn alias(&self, name: &str) -> PyResult<PyExpr> {
        Ok(PyExpr {
            expr: self.expr.clone().alias(name),
        })
    }

    /// Create a sort PyExpr from an existing PyExpr.
    #[args(ascending = true, nulls_first = true)]
    pub fn sort(&self, ascending: bool, nulls_first: bool) -> PyResult<PyExpr> {
        Ok(PyExpr {
            expr: self.expr.clone().sort(ascending, nulls_first),
        })
    }
}

/// Represents a PyScalarUDF
#[pyclass]
#[derive(Debug, Clone)]
pub struct PyScalarUDF {
    pub(crate) function: ScalarUDF,
}

#[pymethods]
impl PyScalarUDF {
    /// creates a new PyExpr with the call of the udf
    #[call]
    #[args(args = "*")]
    fn __call__(&self, args: &PyTuple) -> PyResult<PyExpr> {
        let args = from_tuple(args)?.iter().map(|e| e.expr.clone()).collect();

        Ok(PyExpr {
            expr: self.function.call(args),
        })
    }
}

/// Represents a AggregateUDF
#[pyclass]
#[derive(Debug, Clone)]
pub struct PyAggregateUDF {
    pub(crate) function: AggregateUDF,
}

#[pymethods]
impl PyAggregateUDF {
    /// creates a new PyExpr with the call of the udf
    #[call]
    #[args(args = "*")]
    fn __call__(&self, args: &PyTuple) -> PyResult<PyExpr> {
        let args = from_tuple(args)?.iter().map(|e| e.expr.clone()).collect();

        Ok(PyExpr {
            expr: self.function.call(args),
        })
    }
}
