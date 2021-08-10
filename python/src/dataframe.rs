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

use std::sync::{Arc, Mutex};

use pyo3::{
    prelude::*,
    types::{PyList, PyTuple},
};
use tokio::runtime::Runtime;

use datafusion::execution::context::{ExecutionContext, ExecutionContextState};
use datafusion::logical_plan::{JoinType, LogicalPlan, LogicalPlanBuilder};
use datafusion::physical_plan::collect;

use crate::{
    errors, errors::DataFusionError, expression, expression::PyExpr,
    pyarrow::PyArrowConvert,
};

/// A PyDataFrame is a representation of a logical plan and an API to compose statements.
/// Use it to build a plan and `.collect()` to execute the plan and collect the result.
/// The actual execution of a plan runs natively on Rust and Arrow on a multi-threaded environment.
#[pyclass]
pub(crate) struct PyDataFrame {
    ctx_state: Arc<Mutex<ExecutionContextState>>,
    plan: LogicalPlan,
}

impl PyDataFrame {
    /// creates a new PyDataFrame
    pub fn new(ctx_state: Arc<Mutex<ExecutionContextState>>, plan: LogicalPlan) -> Self {
        Self { ctx_state, plan }
    }
}

#[pymethods]
impl PyDataFrame {
    /// Select `expressions` from the existing PyDataFrame.
    #[args(args = "*")]
    fn select(&self, args: &PyTuple) -> PyResult<Self> {
        let expressions = expression::from_tuple(args)?;
        let builder = LogicalPlanBuilder::from(self.plan.clone());
        let builder =
            errors::wrap(builder.project(expressions.into_iter().map(|e| e.expr)))?;
        let plan = errors::wrap(builder.build())?;

        Ok(PyDataFrame {
            ctx_state: self.ctx_state.clone(),
            plan,
        })
    }

    /// Filter according to the `predicate` expression
    fn filter(&self, predicate: PyExpr) -> PyResult<Self> {
        let builder = LogicalPlanBuilder::from(self.plan.clone());
        let builder = errors::wrap(builder.filter(predicate.expr))?;
        let plan = errors::wrap(builder.build())?;

        Ok(PyDataFrame {
            ctx_state: self.ctx_state.clone(),
            plan,
        })
    }

    /// Aggregates using expressions
    fn aggregate(&self, group_by: Vec<PyExpr>, aggs: Vec<PyExpr>) -> PyResult<Self> {
        let builder = LogicalPlanBuilder::from(self.plan.clone());
        let builder = errors::wrap(builder.aggregate(
            group_by.into_iter().map(|e| e.expr),
            aggs.into_iter().map(|e| e.expr),
        ))?;
        let plan = errors::wrap(builder.build())?;

        Ok(PyDataFrame {
            ctx_state: self.ctx_state.clone(),
            plan,
        })
    }

    /// Sort by specified sorting expressions
    fn sort(&self, exprs: Vec<PyExpr>) -> PyResult<Self> {
        let exprs = exprs.into_iter().map(|e| e.expr);
        let builder = LogicalPlanBuilder::from(self.plan.clone());
        let builder = errors::wrap(builder.sort(exprs))?;
        let plan = errors::wrap(builder.build())?;
        Ok(PyDataFrame {
            ctx_state: self.ctx_state.clone(),
            plan,
        })
    }

    /// Limits the plan to return at most `count` rows
    fn limit(&self, count: usize) -> PyResult<Self> {
        let builder = LogicalPlanBuilder::from(self.plan.clone());
        let builder = errors::wrap(builder.limit(count))?;
        let plan = errors::wrap(builder.build())?;

        Ok(PyDataFrame {
            ctx_state: self.ctx_state.clone(),
            plan,
        })
    }

    /// Executes the plan, returning a list of `RecordBatch`es.
    /// Unless some order is specified in the plan, there is no guarantee of the order of the result
    fn collect(&self, py: Python) -> PyResult<PyObject> {
        let ctx = ExecutionContext::from(self.ctx_state.clone());
        let plan = ctx.optimize(&self.plan).map_err(DataFusionError::from)?;
        let plan = ctx
            .create_physical_plan(&plan)
            .map_err(DataFusionError::from)?;

        let rt = Runtime::new().unwrap();
        let batches = py.allow_threads(|| {
            rt.block_on(async { collect(plan).await.map_err(DataFusionError::from) })
        })?;

        let mut py_batches = vec![];
        for batch in batches {
            py_batches.push(batch.to_pyarrow(py)?);
        }
        let py_list = PyList::new(py, py_batches);
        Ok(PyObject::from(py_list))
    }

    /// Returns the join of two PyDataFrames `on`.
    fn join(&self, right: &PyDataFrame, on: Vec<&str>, how: &str) -> PyResult<Self> {
        let builder = LogicalPlanBuilder::from(self.plan.clone());

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

        let builder = errors::wrap(builder.join(&right.plan, join_type, on.clone(), on))?;

        let plan = errors::wrap(builder.build())?;

        Ok(PyDataFrame {
            ctx_state: self.ctx_state.clone(),
            plan,
        })
    }
}
