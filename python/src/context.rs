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

use std::path::PathBuf;
use std::{collections::HashSet, sync::Arc};

use rand::distributions::Alphanumeric;
use rand::Rng;

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

use datafusion::arrow::datatypes::Schema;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::execution::context::ExecutionContext;
use datafusion::prelude::CsvReadOptions;

use crate::dataframe::PyDataFrame;
use crate::errors::DataFusionError;
use crate::functions;
use crate::pyarrow::PyArrowConvert;

/// `PyExecutionContext` is able to plan and execute DataFusion plans.
/// It has a powerful optimizer, a physical planner for local execution, and a
/// multi-threaded execution engine to perform the execution.
#[pyclass(unsendable)]
pub(crate) struct PyExecutionContext {
    ctx: ExecutionContext,
}

#[pymethods]
impl PyExecutionContext {
    #[new]
    fn new() -> Self {
        PyExecutionContext {
            ctx: ExecutionContext::new(),
        }
    }

    /// Returns a PyDataFrame whose plan corresponds to the SQL statement.
    fn sql(&mut self, query: &str) -> PyResult<PyDataFrame> {
        let df = self.ctx.sql(query).map_err(DataFusionError::from)?;
        Ok(PyDataFrame::new(
            self.ctx.state.clone(),
            df.to_logical_plan(),
        ))
    }

    fn create_dataframe(
        &mut self,
        partitions: Vec<Vec<&PyAny>>,
    ) -> PyResult<PyDataFrame> {
        let partitions: Vec<Vec<RecordBatch>> = partitions
            .into_iter()
            .map(|batches| {
                batches
                    .into_iter()
                    .map(RecordBatch::from_pyarrow)
                    .collect::<PyResult<_>>()
            })
            .collect::<PyResult<_>>()?;

        let table = MemTable::try_new(partitions[0][0].schema(), partitions)
            .map_err(DataFusionError::from)?;

        // generate a random (unique) name for this table
        let name = rand::thread_rng()
            .sample_iter(&Alphanumeric)
            .take(10)
            .collect::<String>();

        self.ctx
            .register_table(&*name, Arc::new(table))
            .map_err(DataFusionError::from)?;
        let table = self.ctx.table(&*name).map_err(DataFusionError::from)?;

        let df = PyDataFrame::new(self.ctx.state.clone(), table.to_logical_plan());
        Ok(df)
    }

    fn register_parquet(&mut self, name: &str, path: &str) -> PyResult<()> {
        self.ctx
            .register_parquet(name, path)
            .map_err(DataFusionError::from)?;
        Ok(())
    }

    #[args(
        schema = "None",
        has_header = "true",
        delimiter = "\",\"",
        schema_infer_max_records = "1000",
        file_extension = "\".csv\""
    )]
    fn register_csv(
        &mut self,
        name: &str,
        path: PathBuf,
        schema: Option<&PyAny>,
        has_header: bool,
        delimiter: &str,
        schema_infer_max_records: usize,
        file_extension: &str,
    ) -> PyResult<()> {
        let path = path
            .to_str()
            .ok_or(PyValueError::new_err("Unable to convert path to a string"))?;
        let schema = match schema {
            Some(s) => Some(Schema::from_pyarrow(s)?),
            None => None,
        };
        let delimiter = delimiter.as_bytes();
        if delimiter.len() != 1 {
            return Err(PyValueError::new_err(
                "Delimiter must be a single character",
            ));
        }

        let mut options = CsvReadOptions::new()
            .has_header(has_header)
            .delimiter(delimiter[0])
            .schema_infer_max_records(schema_infer_max_records)
            .file_extension(file_extension);
        options.schema = schema.as_ref();

        self.ctx
            .register_csv(name, path, options)
            .map_err(DataFusionError::from)?;
        Ok(())
    }

    fn register_udf(
        &mut self,
        name: &str,
        func: PyObject,
        args_types: Vec<&PyAny>,
        return_type: &PyAny,
    ) -> PyResult<()> {
        let function = functions::create_udf(func, args_types, return_type, name)?;
        self.ctx.register_udf(function.function);
        Ok(())
    }

    fn tables(&self) -> HashSet<String> {
        self.ctx.tables().unwrap()
    }
}
