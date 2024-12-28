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

use arrow_array::RecordBatch;
use arrow_schema::{DataType, Schema};
use datafusion::physical_plan::ColumnarValue;
use datafusion_common::{internal_err, Result};
use datafusion_physical_expr::PhysicalExpr;
use std::{hash::Hash, sync::Arc};

/// This is similar to `UnKnownColumn` in DataFusion, but it has data type.
/// This is only used when the column is not bound to a schema, for example, the
/// inputs to aggregation functions in final aggregation. In the case, we cannot
/// bind the aggregation functions to the input schema which is grouping columns
/// and aggregate buffer attributes in Spark (DataFusion has different design).
/// But when creating certain aggregation functions, we need to know its input
/// data types. As `UnKnownColumn` doesn't have data type, we implement this
/// `UnboundColumn` to carry the data type.
#[derive(Debug, Hash, PartialEq, Eq, Clone)]
pub struct UnboundColumn {
    name: String,
    datatype: DataType,
}

impl UnboundColumn {
    /// Create a new unbound column expression
    pub fn new(name: &str, datatype: DataType) -> Self {
        Self {
            name: name.to_owned(),
            datatype,
        }
    }

    /// Get the column name
    pub fn name(&self) -> &str {
        &self.name
    }
}

impl std::fmt::Display for UnboundColumn {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}, datatype: {}", self.name, self.datatype)
    }
}

impl PhysicalExpr for UnboundColumn {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    /// Get the data type of this expression, given the schema of the input
    fn data_type(&self, _input_schema: &Schema) -> Result<DataType> {
        Ok(self.datatype.clone())
    }

    /// Decide whether this expression is nullable, given the schema of the input
    fn nullable(&self, _input_schema: &Schema) -> Result<bool> {
        Ok(true)
    }

    /// Evaluate the expression
    fn evaluate(&self, _batch: &RecordBatch) -> Result<ColumnarValue> {
        internal_err!("UnboundColumn::evaluate() should not be called")
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
}
