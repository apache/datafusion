pub mod context;

use pyo3::prelude::*;


#[pymodule]
fn ballista(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<context::PyBallistaContext>()?;

    Ok(())
}

