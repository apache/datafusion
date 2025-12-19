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

//! Physical lambda column reference: [`LambdaVariable`]

use std::any::Any;
use std::hash::Hash;
use std::sync::Arc;

use crate::physical_expr::PhysicalExpr;
use arrow::array::ArrayRef;
use arrow::datatypes::FieldRef;
use arrow::{
    datatypes::{DataType, Schema},
    record_batch::RecordBatch,
};
use datafusion_common::{Result, exec_datafusion_err};
use datafusion_expr::ColumnarValue;

/// Represents the lambda column with a given name and field
#[derive(Debug, Clone)]
pub struct LambdaVariable {
    name: String,
    field: FieldRef,
    value: Option<ColumnarValue>,
}

impl Eq for LambdaVariable {}

impl PartialEq for LambdaVariable {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name && self.field == other.field
    }
}

impl Hash for LambdaVariable {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.name.hash(state);
        self.field.hash(state);
    }
}

impl LambdaVariable {
    /// Create a new lambda column expression
    pub fn new(name: &str, field: FieldRef) -> Self {
        Self {
            name: name.to_owned(),
            field,
            value: None,
        }
    }

    /// Get the column's name
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Get the column's field
    pub fn field(&self) -> &FieldRef {
        &self.field
    }
    
    pub fn with_value(self, value: ArrayRef) -> Self {
        Self {
            name: self.name,
            field: self.field,
            value: Some(ColumnarValue::Array(value)),
        }
    }
}

impl std::fmt::Display for LambdaVariable {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}@-1", self.name)
    }
}

impl PhysicalExpr for LambdaVariable {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    /// Get the data type of this expression, given the schema of the input
    fn data_type(&self, _input_schema: &Schema) -> Result<DataType> {
        Ok(self.field.data_type().clone())
    }

    /// Decide whether this expression is nullable, given the schema of the input
    fn nullable(&self, _input_schema: &Schema) -> Result<bool> {
        Ok(self.field.is_nullable())
    }

    /// Evaluate the expression
    fn evaluate(&self, _batch: &RecordBatch) -> Result<ColumnarValue> {
        self.value.clone().ok_or_else(|| exec_datafusion_err!("Physical LambdaVariable {} missing value", self.name))
    }

    fn return_field(&self, _input_schema: &Schema) -> Result<FieldRef> {
        Ok(Arc::clone(&self.field))
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
        write!(f, "{}", self.name)
    }
}

/// Create a lambda variable expression
pub fn lambda_variable(name: &str, field: FieldRef) -> Result<Arc<dyn PhysicalExpr>> {
    Ok(Arc::new(LambdaVariable::new(name, field)))
}
