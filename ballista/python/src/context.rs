
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
    #[args(host = "\"localhost\"", port = "50051",  is_standalone = "false", kwargs = "**")]
    pub fn new(scheduler_host: &str, scheduler_port: u16, is_standalone: bool, kwargs: Option<&pyo3::types::PyDict>) -> PyResult<Self> {
        let setting = match kwargs {
            Some(args) => args
                .iter().map(|(k, v)| {
                    Ok((k.to_string(), v.to_string()))
                }).collect::<Result<HashMap<String, String>, PyErr>>()?,
            None => HashMap::new(),    
        };

        let mut config_builder = BallistaConfig::builder();
        for (k, v) in setting {
            config_builder = config_builder.set(&k, &v);
        }
        let config = config_builder.build().unwrap();

        Ok(PyBallistaContext {
            ctx: ballista::context::BallistaContext::remote(scheduler_host, scheduler_port, &config)
        })    
        /*
        match is_standalone {
            true => Ok(PyBallistaContext {
                ctx: ballista::context::BallistaContext::standalone(&conf, 1)
            }),
            false => Ok(PyBallistaContext {
                ctx: ballista::context::BallistaContext::remote(scheduler_host, scheduler_port, &conf)
            })    
        }
        */
    }
}

