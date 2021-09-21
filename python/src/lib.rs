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

use pyo3::prelude::*;

mod catalog;
mod context;
mod dataframe;
mod errors;
mod expression;
mod functions;
mod udaf;
mod udf;

// taken from https://github.com/PyO3/pyo3/issues/471
fn register_module_package(py: Python, package_name: &str, module: &PyModule) {
    py.import("sys")
        .expect("failed to import python sys module")
        .dict()
        .get_item("modules")
        .expect("failed to get python modules dictionary")
        .downcast::<pyo3::types::PyDict>()
        .expect("failed to turn sys.modules into a PyDict")
        .set_item(package_name, module)
        .expect("failed to inject module");
}

/// DataFusion.
#[pymodule]
fn internals(py: Python, m: &PyModule) -> PyResult<()> {
    expression::init(m)?;

    let functions = PyModule::new(py, "functions")?;
    functions::init(functions)?;
    register_module_package(py, "datafusion.functions", functions);
    m.add_submodule(functions)?;

    m.add_class::<catalog::PyCatalog>()?;
    m.add_class::<catalog::PyDatabase>()?;
    m.add_class::<catalog::PyTable>()?;
    m.add_class::<context::PyExecutionContext>()?;
    m.add_class::<dataframe::PyDataFrame>()?;

    Ok(())
}
