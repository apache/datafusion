
use pyo3::types::PyTuple;
use pyo3::{exceptions::PyException, prelude::*};

use std::collections::HashMap;
use ballista_core::config::BallistaConfig;


#[pyclass(unsendable, name = "BallistaContext", module = "ballista")]
pub(crate) struct PyBallistaContext {
    ctx: ballista::context::BallistaContext,
}

#[pymethods]
impl PyBallistaContext {
    #[new]
    #[args(host = "\"localhost\"", port = "50051", config = "()", is_standalone = "false")]
    pub fn new(scheduler_host: &str, scheduler_port: u16, config: &pyo3::types::PyDict, is_standalone: bool) -> PyResult<Self> {
        let setting = config.iter().map(|(k, v)| {
            Ok((k.to_string(), v.to_string()))
        }).collect::<Result<HashMap<String, String>, PyErr>>()?;

        let conf = BallistaConfig {
            settings: setting
         };

        match is_standalone {
            true => Ok(PyBallistaContext {
                ctx: ballista::context::BallistaContext::standalone(&conf, 1)
            }),
            false => Ok(PyBallistaContext {
                ctx: ballista::context::BallistaContext::remote(scheduler_host, scheduler_port, &conf)
            })    
        }

    }
}

