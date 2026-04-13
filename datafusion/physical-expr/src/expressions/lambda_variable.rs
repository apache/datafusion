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

//! Physical lambda variable reference: [`LambdaVariable`]

use std::any::Any;
use std::hash::Hash;
use std::sync::Arc;

use crate::physical_expr::PhysicalExpr;
use arrow::datatypes::FieldRef;
use arrow::{
    datatypes::{DataType, Schema},
    record_batch::RecordBatch,
};

use datafusion_common::{Result, internal_err, plan_err};
use datafusion_expr::ColumnarValue;

/// Represents the lambda variable with a given name and field
#[derive(Debug, Clone)]
pub struct LambdaVariable {
    name: String,
    index: usize,
    field: FieldRef,
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
    /// Create a new lambda variable expression
    pub fn new(name: String, index: usize, field: FieldRef) -> Self {
        Self { name, index, field }
    }

    /// Get the variable's name
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Get the variable's index
    pub fn index(&self) -> usize {
        self.index
    }

    /// Get the variable's field
    pub fn field(&self) -> &FieldRef {
        &self.field
    }
}

impl std::fmt::Display for LambdaVariable {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}@{}", self.name(), self.index)
    }
}

impl PhysicalExpr for LambdaVariable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self, _input_schema: &Schema) -> Result<DataType> {
        Ok(self.field.data_type().clone())
    }

    fn nullable(&self, _input_schema: &Schema) -> Result<bool> {
        Ok(self.field.is_nullable())
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        if self.index >= batch.num_columns() {
            return internal_err!(
                "PhysicalExpr LambdaVariable references column '{}' at index {} (zero-based) but batch only has {} columns: {:?}",
                self.name,
                self.index,
                batch.num_columns(),
                batch
                    .schema_ref()
                    .fields()
                    .iter()
                    .map(|f| f.name())
                    .collect::<Vec<_>>()
            );
        }

        Ok(ColumnarValue::Array(Arc::clone(batch.column(self.index))))
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
        write!(f, "{}@{}", self.name(), self.index)
    }
}

/// Create a lambda variable expression
pub fn lambda_variable(
    name: impl Into<String>,
    field: FieldRef,
    schema: &Schema,
) -> Result<Arc<dyn PhysicalExpr>> {
    let name = name.into();
    let index = schema.index_of(&name)?;

    let schema_field = schema.field(index);

    if field.as_ref() != schema_field {
        return plan_err!(
            "LambdaVariable owned field differ from schema field {field} != {schema_field}"
        );
    }

    Ok(Arc::new(LambdaVariable::new(name, index, field)))
}
