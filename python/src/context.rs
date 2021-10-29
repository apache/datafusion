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

use uuid::Uuid;

use pyo3::exceptions::{PyKeyError, PyValueError};
use pyo3::prelude::*;

use datafusion::arrow::datatypes::Schema;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::execution::context::ExecutionContext;
use datafusion::prelude::CsvReadOptions;

use crate::catalog::PyCatalog;
use crate::dataframe::PyDataFrame;
use crate::errors::DataFusionError;
use crate::udf::PyScalarUDF;
use crate::utils::wait_for_future;

/// `PyExecutionContext` is able to plan and execute DataFusion plans.
/// It has a powerful optimizer, a physical planner for local execution, and a
/// multi-threaded execution engine to perform the execution.
#[pyclass(name = "ExecutionContext", module = "datafusion", subclass, unsendable)]
pub(crate) struct PyExecutionContext {
    ctx: ExecutionContext,
}

#[pymethods]
impl PyExecutionContext {
    // TODO(kszucs): should expose the configuration options as keyword arguments
    #[new]
    fn new() -> Self {
        PyExecutionContext {
            ctx: ExecutionContext::new(),
        }
    }

    /// Returns a PyDataFrame whose plan corresponds to the SQL statement.
    fn sql(&mut self, query: &str, py: Python) -> PyResult<PyDataFrame> {
        let result = self.ctx.sql(query);
        let df = wait_for_future(py, result).map_err(DataFusionError::from)?;
        Ok(PyDataFrame::new(df))
    }

    fn create_dataframe(
        &mut self,
        partitions: Vec<Vec<RecordBatch>>,
    ) -> PyResult<PyDataFrame> {
        let table = MemTable::try_new(partitions[0][0].schema(), partitions)
            .map_err(DataFusionError::from)?;

        // generate a random (unique) name for this table
        // table name cannot start with numeric digit
        let name = "c".to_owned()
            + &Uuid::new_v4()
                .to_simple()
                .encode_lower(&mut Uuid::encode_buffer());

        self.ctx
            .register_table(&*name, Arc::new(table))
            .map_err(DataFusionError::from)?;
        let table = self.ctx.table(&*name).map_err(DataFusionError::from)?;

        let df = PyDataFrame::new(table);
        Ok(df)
    }

    fn register_record_batches(
        &mut self,
        name: &str,
        partitions: Vec<Vec<RecordBatch>>,
    ) -> PyResult<()> {
        let schema = partitions[0][0].schema();
        let table = MemTable::try_new(schema, partitions)?;
        self.ctx
            .register_table(name, Arc::new(table))
            .map_err(DataFusionError::from)?;
        Ok(())
    }

    fn register_parquet(&mut self, name: &str, path: &str, py: Python) -> PyResult<()> {
        let result = self.ctx.register_parquet(name, path);
        wait_for_future(py, result).map_err(DataFusionError::from)?;
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
        schema: Option<Schema>,
        has_header: bool,
        delimiter: &str,
        schema_infer_max_records: usize,
        file_extension: &str,
        py: Python,
    ) -> PyResult<()> {
        let path = path
            .to_str()
            .ok_or(PyValueError::new_err("Unable to convert path to a string"))?;
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

        let result = self.ctx.register_csv(name, path, options);
        wait_for_future(py, result).map_err(DataFusionError::from)?;

        Ok(())
    }

    fn register_udf(&mut self, udf: PyScalarUDF) -> PyResult<()> {
        self.ctx.register_udf(udf.function);
        Ok(())
    }

    #[args(name = "\"datafusion\"")]
    fn catalog(&self, name: &str) -> PyResult<PyCatalog> {
        match self.ctx.catalog(name) {
            Some(catalog) => Ok(PyCatalog::new(catalog)),
            None => Err(PyKeyError::new_err(format!(
                "Catalog with name {} doesn't exist.",
                &name
            ))),
        }
    }

    fn tables(&self) -> HashSet<String> {
        self.ctx.tables().unwrap()
    }

    fn table(&self, name: &str) -> PyResult<PyDataFrame> {
        Ok(PyDataFrame::new(self.ctx.table(name)?))
    }

    fn empty_table(&self) -> PyResult<PyDataFrame> {
        Ok(PyDataFrame::new(self.ctx.read_empty()?))
    }
}
