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

use std::{collections::HashSet, sync::Arc};

use rand::distributions::Alphanumeric;
use rand::Rng;

use pyo3::prelude::*;

use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::execution::context::ExecutionContext as _ExecutionContext;

use crate::dataframe;
use crate::errors;
use crate::functions;
use crate::to_rust;
use crate::types::PyDataType;

/// `ExecutionContext` is able to plan and execute DataFusion plans.
/// It has a powerful optimizer, a physical planner for local execution, and a
/// multi-threaded execution engine to perform the execution.
#[pyclass(unsendable)]
pub(crate) struct ExecutionContext {
    ctx: _ExecutionContext,
}

#[pymethods]
impl ExecutionContext {
    #[new]
    fn new() -> Self {
        ExecutionContext {
            ctx: _ExecutionContext::new(),
        }
    }

    /// Returns a DataFrame whose plan corresponds to the SQL statement.
    fn sql(&mut self, query: &str) -> PyResult<dataframe::DataFrame> {
        let df = self
            .ctx
            .sql(query)
            .map_err(|e| -> errors::DataFusionError { e.into() })?;
        Ok(dataframe::DataFrame::new(
            self.ctx.state.clone(),
            df.to_logical_plan(),
        ))
    }

    fn create_dataframe(
        &mut self,
        partitions: Vec<Vec<PyObject>>,
        py: Python,
    ) -> PyResult<dataframe::DataFrame> {
        let partitions: Vec<Vec<RecordBatch>> = partitions
            .iter()
            .map(|batches| {
                batches
                    .iter()
                    .map(|batch| to_rust::to_rust_batch(batch.as_ref(py)))
                    .collect()
            })
            .collect::<PyResult<_>>()?;

        let table =
            errors::wrap(MemTable::try_new(partitions[0][0].schema(), partitions))?;

        // generate a random (unique) name for this table
        let name = rand::thread_rng()
            .sample_iter(&Alphanumeric)
            .take(10)
            .collect::<String>();

        errors::wrap(self.ctx.register_table(&*name, Arc::new(table)))?;
        Ok(dataframe::DataFrame::new(
            self.ctx.state.clone(),
            errors::wrap(self.ctx.table(&*name))?.to_logical_plan(),
        ))
    }

    fn register_parquet(&mut self, name: &str, path: &str) -> PyResult<()> {
        errors::wrap(self.ctx.register_parquet(name, path))?;
        Ok(())
    }

    fn register_udf(
        &mut self,
        name: &str,
        func: PyObject,
        args_types: Vec<PyDataType>,
        return_type: PyDataType,
    ) {
        let function = functions::create_udf(func, args_types, return_type, name);

        self.ctx.register_udf(function.function);
    }

    fn tables(&self) -> HashSet<String> {
        self.ctx.tables().unwrap()
    }
}
