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

use std::sync::Arc;

use pyo3::prelude::*;

use datafusion::arrow::datatypes::Schema;
use datafusion::arrow::pyarrow::PyArrowConvert;
use datafusion::arrow::util::pretty;
use datafusion::dataframe::DataFrame;
use datafusion::logical_plan::JoinType;

use crate::utils::wait_for_future;
use crate::{errors::DataFusionError, expression::PyExpr};

/// A PyDataFrame is a representation of a logical plan and an API to compose statements.
/// Use it to build a plan and `.collect()` to execute the plan and collect the result.
/// The actual execution of a plan runs natively on Rust and Arrow on a multi-threaded environment.
#[pyclass(name = "DataFrame", module = "datafusion", subclass)]
#[derive(Clone)]
pub(crate) struct PyDataFrame {
    df: Arc<dyn DataFrame>,
}

impl PyDataFrame {
    /// creates a new PyDataFrame
    pub fn new(df: Arc<dyn DataFrame>) -> Self {
        Self { df }
    }
}

#[pymethods]
impl PyDataFrame {
    /// Returns the schema from the logical plan
    fn schema(&self) -> Schema {
        self.df.schema().into()
    }

    #[args(args = "*")]
    fn select(&self, args: Vec<PyExpr>) -> PyResult<Self> {
        let expr = args.into_iter().map(|e| e.into()).collect();
        let df = self.df.select(expr)?;
        Ok(Self::new(df))
    }

    fn filter(&self, predicate: PyExpr) -> PyResult<Self> {
        let df = self.df.filter(predicate.into())?;
        Ok(Self::new(df))
    }

    fn aggregate(&self, group_by: Vec<PyExpr>, aggs: Vec<PyExpr>) -> PyResult<Self> {
        let group_by = group_by.into_iter().map(|e| e.into()).collect();
        let aggs = aggs.into_iter().map(|e| e.into()).collect();
        let df = self.df.aggregate(group_by, aggs)?;
        Ok(Self::new(df))
    }

    #[args(exprs = "*")]
    fn sort(&self, exprs: Vec<PyExpr>) -> PyResult<Self> {
        let exprs = exprs.into_iter().map(|e| e.into()).collect();
        let df = self.df.sort(exprs)?;
        Ok(Self::new(df))
    }

    fn limit(&self, count: usize) -> PyResult<Self> {
        let df = self.df.limit(count)?;
        Ok(Self::new(df))
    }

    /// Executes the plan, returning a list of `RecordBatch`es.
    /// Unless some order is specified in the plan, there is no
    /// guarantee of the order of the result.
    fn collect(&self, py: Python) -> PyResult<Vec<PyObject>> {
        let batches = wait_for_future(py, self.df.collect())?;
        // cannot use PyResult<Vec<RecordBatch>> return type due to
        // https://github.com/PyO3/pyo3/issues/1813
        batches.into_iter().map(|rb| rb.to_pyarrow(py)).collect()
    }

    /// Print the result, 20 lines by default
    #[args(num = "20")]
    fn show(&self, py: Python, num: usize) -> PyResult<()> {
        let df = self.df.limit(num)?;
        let batches = wait_for_future(py, df.collect())?;
        Ok(pretty::print_batches(&batches)?)
    }

    fn join(
        &self,
        right: PyDataFrame,
        join_keys: (Vec<&str>, Vec<&str>),
        how: &str,
    ) -> PyResult<Self> {
        let join_type = match how {
            "inner" => JoinType::Inner,
            "left" => JoinType::Left,
            "right" => JoinType::Right,
            "full" => JoinType::Full,
            "semi" => JoinType::Semi,
            "anti" => JoinType::Anti,
            how => {
                return Err(DataFusionError::Common(format!(
                    "The join type {} does not exist or is not implemented",
                    how
                ))
                .into())
            }
        };

        let df = self
            .df
            .join(right.df, join_type, &join_keys.0, &join_keys.1)?;
        Ok(Self::new(df))
    }
}
