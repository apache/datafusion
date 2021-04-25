use pyo3::prelude::*;

use datafusion::scalar::ScalarValue as _Scalar;

use crate::to_rust::to_rust_scalar;

/// An expression that can be used on a DataFrame
#[derive(Debug, Clone)]
pub(crate) struct Scalar {
    pub(crate) scalar: _Scalar,
}

impl<'source> FromPyObject<'source> for Scalar {
    fn extract(ob: &'source PyAny) -> PyResult<Self> {
        Ok(Self {
            scalar: to_rust_scalar(ob)?,
        })
    }
}
