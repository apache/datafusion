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

//! Defines physical expressions for APPROX_MEDIAN that can be evaluated MEDIAN at runtime during query execution

use crate::expressions::{lit, ApproxPercentileCont};
use crate::{AggregateExpr, PhysicalExpr};
use arrow::{datatypes::DataType, datatypes::Field};
use datafusion_common::Result;
use datafusion_expr::Accumulator;
use std::any::Any;
use std::sync::Arc;

/// MEDIAN aggregate expression
#[derive(Debug)]
pub struct ApproxMedian {
    name: String,
    expr: Arc<dyn PhysicalExpr>,
    data_type: DataType,
    approx_percentile: ApproxPercentileCont,
}

impl ApproxMedian {
    /// Create a new APPROX_MEDIAN aggregate function
    pub fn try_new(
        expr: Arc<dyn PhysicalExpr>,
        name: impl Into<String>,
        data_type: DataType,
    ) -> Result<Self> {
        let name: String = name.into();
        let approx_percentile = ApproxPercentileCont::new(
            vec![expr.clone(), lit(0.5_f64)],
            name.clone(),
            data_type.clone(),
        )?;
        Ok(Self {
            name,
            expr,
            data_type,
            approx_percentile,
        })
    }
}

impl AggregateExpr for ApproxMedian {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn field(&self) -> Result<Field> {
        Ok(Field::new(&self.name, self.data_type.clone(), true))
    }

    fn create_accumulator(&self) -> Result<Box<dyn Accumulator>> {
        self.approx_percentile.create_accumulator()
    }

    fn state_fields(&self) -> Result<Vec<Field>> {
        self.approx_percentile.state_fields()
    }

    fn expressions(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        vec![self.expr.clone()]
    }

    fn name(&self) -> &str {
        &self.name
    }
}
