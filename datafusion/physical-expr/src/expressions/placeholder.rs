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

//! Placeholder expression.

use std::{
    any::Any,
    fmt::{self, Formatter},
    sync::Arc,
};

use arrow::{
    array::RecordBatch,
    datatypes::{DataType, Field, FieldRef, Schema},
};
use datafusion_common::{
    DataFusionError, Result, exec_datafusion_err, tree_node::TreeNode,
};
use datafusion_expr::ColumnarValue;
use datafusion_physical_expr_common::physical_expr::PhysicalExpr;
use std::hash::Hash;

/// Physical expression representing a placeholder parameter (e.g., $1, $2, or named parameters) in
/// the physical plan.
///
/// This expression serves as a placeholder that will be resolved to a literal value during
/// execution. It should not be evaluated directly.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct PlaceholderExpr {
    /// Placeholder id, e.g. $1 or $a.
    pub id: String,
    /// Derived from expression where placeholder is met.
    pub field: Option<FieldRef>,
}

impl PlaceholderExpr {
    /// Create a new placeholder expression.
    pub fn new(id: String) -> Self {
        Self { id, field: None }
    }

    /// Create a new placeholders expression with a field.
    pub fn new_with_field(id: String, field: FieldRef) -> Self {
        Self {
            id,
            field: Some(field),
        }
    }

    /// Create a new placeholder expression with a data type.
    pub fn new_with_datatype(id: String, data_type: DataType) -> Self {
        let field = Arc::new(Field::new("", data_type, true));
        Self::new_with_field(id, field)
    }

    fn not_provided(&self, what: &str) -> DataFusionError {
        exec_datafusion_err!("Placeholder '{}' was not provided {}.", self.id, what)
    }
}

/// Create a placeholder expression.
pub fn placeholder<I: Into<String>>(id: I, data_type: DataType) -> Arc<dyn PhysicalExpr> {
    Arc::new(PlaceholderExpr::new_with_datatype(id.into(), data_type))
}

/// Returns `true` if expression has placeholders.
pub fn has_placeholders(expr: &Arc<dyn PhysicalExpr>) -> bool {
    expr.exists(|e| Ok(e.as_any().is::<PlaceholderExpr>()))
        .expect("do not return errors")
}

impl fmt::Display for PlaceholderExpr {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        write!(f, "{}", self.id)
    }
}

impl PhysicalExpr for PlaceholderExpr {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn return_field(&self, _input_schema: &Schema) -> Result<FieldRef> {
        self.field
            .as_ref()
            .map(Arc::clone)
            .ok_or_else(|| self.not_provided("a field"))
    }

    fn data_type(&self, _input_schema: &Schema) -> Result<DataType> {
        self.field
            .as_ref()
            .map(|f| f.data_type().clone())
            .ok_or_else(|| self.not_provided("a data type"))
    }

    fn evaluate(&self, _batch: &RecordBatch) -> Result<ColumnarValue> {
        Err(self.not_provided("a value for execution"))
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        assert!(children.is_empty());
        Ok(self)
    }

    fn fmt_sql(&self, f: &mut Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}
