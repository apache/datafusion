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

use datafusion::logical_plan::Expr as _Expr;
use datafusion::physical_plan::udaf::AggregateUDF as _AggregateUDF;
use datafusion::physical_plan::udf::ScalarUDF as _ScalarUDF;

/// An expression that can be used on a DataFrame
#[pyclass]
#[derive(Debug, Clone)]
pub(crate) struct Expression {
    pub(crate) expr: _Expr,
}

/// converts a tuple of expressions into a vector of Expressions
pub(crate) fn from_tuple(value: &PyTuple) -> PyResult<Vec<Expression>> {
    value
        .iter()
        .map(|e| e.extract::<Expression>())
        .collect::<PyResult<_>>()
}

#[pyproto]
impl PyNumberProtocol for Expression {
    fn __add__(lhs: Expression, rhs: Expression) -> PyResult<Expression> {
        Ok(Expression {
            expr: lhs.expr + rhs.expr,
        })
    }

    fn __sub__(lhs: Expression, rhs: Expression) -> PyResult<Expression> {
        Ok(Expression {
            expr: lhs.expr - rhs.expr,
        })
    }

    fn __truediv__(lhs: Expression, rhs: Expression) -> PyResult<Expression> {
        Ok(Expression {
            expr: lhs.expr / rhs.expr,
        })
    }

    fn __mul__(lhs: Expression, rhs: Expression) -> PyResult<Expression> {
        Ok(Expression {
            expr: lhs.expr * rhs.expr,
        })
    }

    fn __and__(lhs: Expression, rhs: Expression) -> PyResult<Expression> {
        Ok(Expression {
            expr: lhs.expr.and(rhs.expr),
        })
    }

    fn __or__(lhs: Expression, rhs: Expression) -> PyResult<Expression> {
        Ok(Expression {
            expr: lhs.expr.or(rhs.expr),
        })
    }

    fn __invert__(&self) -> PyResult<Expression> {
        Ok(Expression {
            expr: self.expr.clone().not(),
        })
    }
}

#[pyproto]
impl PyObjectProtocol for Expression {
    fn __richcmp__(&self, other: Expression, op: CompareOp) -> Expression {
        match op {
            CompareOp::Lt => Expression {
                expr: self.expr.clone().lt(other.expr),
            },
            CompareOp::Le => Expression {
                expr: self.expr.clone().lt_eq(other.expr),
            },
            CompareOp::Eq => Expression {
                expr: self.expr.clone().eq(other.expr),
            },
            CompareOp::Ne => Expression {
                expr: self.expr.clone().not_eq(other.expr),
            },
            CompareOp::Gt => Expression {
                expr: self.expr.clone().gt(other.expr),
            },
            CompareOp::Ge => Expression {
                expr: self.expr.clone().gt_eq(other.expr),
            },
        }
    }
}

#[pymethods]
impl Expression {
    /// assign a name to the expression
    pub fn alias(&self, name: &str) -> PyResult<Expression> {
        Ok(Expression {
            expr: self.expr.clone().alias(name),
        })
    }

    /// Create a sort expression from an existing expression.
    #[args(ascending = true, nulls_first = true)]
    pub fn sort(&self, ascending: bool, nulls_first: bool) -> PyResult<Expression> {
        Ok(Expression {
            expr: self.expr.clone().sort(ascending, nulls_first),
        })
    }
}

/// Represents a ScalarUDF
#[pyclass]
#[derive(Debug, Clone)]
pub struct ScalarUDF {
    pub(crate) function: _ScalarUDF,
}

#[pymethods]
impl ScalarUDF {
    /// creates a new expression with the call of the udf
    #[call]
    #[args(args = "*")]
    fn __call__(&self, args: &PyTuple) -> PyResult<Expression> {
        let args = from_tuple(args)?.iter().map(|e| e.expr.clone()).collect();

        Ok(Expression {
            expr: self.function.call(args),
        })
    }
}

/// Represents a AggregateUDF
#[pyclass]
#[derive(Debug, Clone)]
pub struct AggregateUDF {
    pub(crate) function: _AggregateUDF,
}

#[pymethods]
impl AggregateUDF {
    /// creates a new expression with the call of the udf
    #[call]
    #[args(args = "*")]
    fn __call__(&self, args: &PyTuple) -> PyResult<Expression> {
        let args = from_tuple(args)?.iter().map(|e| e.expr.clone()).collect();

        Ok(Expression {
            expr: self.function.call(args),
        })
    }
}
