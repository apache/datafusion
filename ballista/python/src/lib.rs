pub mod context;

use pyo3::prelude::*;


#[pymodule]
fn ballista(py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<BallistaContext>();

    crate::functions::init(m);
    Ok(())
}

