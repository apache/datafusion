use pyo3::{basic::CompareOp, prelude::*, types::PyTuple, PyNumberProtocol, PyObjectProtocol};

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
            expr: self.expr.not(),
        })
    }
}

#[pyproto]
impl PyObjectProtocol for Expression {
    fn __richcmp__(&self, other: Expression, op: CompareOp) -> Expression {
        match op {
            CompareOp::Lt => Expression {
                expr: self.expr.lt(other.expr),
            },
            CompareOp::Le => Expression {
                expr: self.expr.lt_eq(other.expr),
            },
            CompareOp::Eq => Expression {
                expr: self.expr.eq(other.expr),
            },
            CompareOp::Ne => Expression {
                expr: self.expr.not_eq(other.expr),
            },
            CompareOp::Gt => Expression {
                expr: self.expr.gt(other.expr),
            },
            CompareOp::Ge => Expression {
                expr: self.expr.gt_eq(other.expr),
            },
        }
    }
}

#[pymethods]
impl Expression {
    /// assign a name to the expression
    pub fn alias(&self, name: &str) -> PyResult<Expression> {
        Ok(Expression {
            expr: self.expr.alias(name),
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
