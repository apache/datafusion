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

//! NoOp placeholder for physical operations

use std::any::Any;
use std::hash::Hash;
use std::sync::Arc;

use arrow::{
    datatypes::{DataType, Schema},
    record_batch::RecordBatch,
};

use crate::PhysicalExpr;
use datafusion_common::{internal_err, Result};
use datafusion_expr::ColumnarValue;

/// A place holder expression, can not be evaluated.
///
/// Used in some cases where an `Arc<dyn PhysicalExpr>` is needed, such as `children()`
#[derive(Debug, PartialEq, Eq, Default, Hash)]
pub struct NoOp {}

impl NoOp {
    /// Create a NoOp expression
    pub fn new() -> Self {
        Self {}
    }
}

impl std::fmt::Display for NoOp {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "NoOp")
    }
}

impl PhysicalExpr for NoOp {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self, _input_schema: &Schema) -> Result<DataType> {
        Ok(DataType::Null)
    }

    fn nullable(&self, _input_schema: &Schema) -> Result<bool> {
        Ok(true)
    }

    fn evaluate(&self, _batch: &RecordBatch) -> Result<ColumnarValue> {
        internal_err!("NoOp::evaluate() should not be called")
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        Ok(self)
    }

    fn fmt_sql(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self, f)
    }
}
