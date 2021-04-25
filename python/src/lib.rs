use pyo3::prelude::*;

mod context;
mod dataframe;
mod errors;
mod expression;
mod functions;
mod scalar;
mod to_py;
mod to_rust;
mod types;
mod udaf;
mod udf;

/// DataFusion.
#[pymodule]
fn datafusion(py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<context::ExecutionContext>()?;
    m.add_class::<dataframe::DataFrame>()?;
    m.add_class::<expression::Expression>()?;

    let functions = PyModule::new(py, "functions")?;
    functions::init(functions)?;
    m.add_submodule(functions)?;

    Ok(())
}
