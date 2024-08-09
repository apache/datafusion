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

//! Column expression

use std::any::Any;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use arrow::{
    datatypes::{DataType, Schema},
    record_batch::RecordBatch,
};
use arrow_schema::SchemaRef;
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_common::{internal_err, plan_err, Result};
use datafusion_expr::ColumnarValue;

use crate::physical_expr::{down_cast_any_ref, PhysicalExpr};

/// Represents the column at a given index in a RecordBatch
#[derive(Debug, Hash, PartialEq, Eq, Clone)]
pub struct Column {
    name: String,
    index: usize,
}

impl Column {
    /// Create a new column expression
    pub fn new(name: &str, index: usize) -> Self {
        Self {
            name: name.to_owned(),
            index,
        }
    }

    /// Create a new column expression based on column name and schema
    pub fn new_with_schema(name: &str, schema: &Schema) -> Result<Self> {
        Ok(Column::new(name, schema.index_of(name)?))
    }

    /// Get the column name
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Get the column index
    pub fn index(&self) -> usize {
        self.index
    }
}

impl std::fmt::Display for Column {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}@{}", self.name, self.index)
    }
}

impl PhysicalExpr for Column {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    /// Get the data type of this expression, given the schema of the input
    fn data_type(&self, input_schema: &Schema) -> Result<DataType> {
        self.bounds_check(input_schema)?;
        Ok(input_schema.field(self.index).data_type().clone())
    }

    /// Decide whether this expression is nullable, given the schema of the input
    fn nullable(&self, input_schema: &Schema) -> Result<bool> {
        self.bounds_check(input_schema)?;
        Ok(input_schema.field(self.index).is_nullable())
    }

    /// Evaluate the expression
    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        self.bounds_check(batch.schema().as_ref())?;
        Ok(ColumnarValue::Array(Arc::clone(batch.column(self.index))))
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

    fn dyn_hash(&self, state: &mut dyn Hasher) {
        let mut s = state;
        self.hash(&mut s);
    }
}

impl PartialEq<dyn Any> for Column {
    fn eq(&self, other: &dyn Any) -> bool {
        down_cast_any_ref(other)
            .downcast_ref::<Self>()
            .map(|x| self == x)
            .unwrap_or(false)
    }
}

impl Column {
    fn bounds_check(&self, input_schema: &Schema) -> Result<()> {
        if self.index < input_schema.fields.len() {
            Ok(())
        } else {
            internal_err!(
                "PhysicalExpr Column references column '{}' at index {} (zero-based) but input schema only has {} columns: {:?}",
                self.name,
                self.index, input_schema.fields.len(), input_schema.fields().iter().map(|f| f.name().clone()).collect::<Vec<String>>())
        }
    }
}

/// Create a column expression
pub fn col(name: &str, schema: &Schema) -> Result<Arc<dyn PhysicalExpr>> {
    Ok(Arc::new(Column::new_with_schema(name, schema)?))
}

/// Rewrites an expression according to new schema; i.e. changes the columns it
/// refers to with the column at corresponding index in the new schema. Returns
/// an error if the given schema has fewer columns than the original schema.
/// Note that the resulting expression may not be valid if data types in the
/// new schema is incompatible with expression nodes.
pub fn with_new_schema(
    expr: Arc<dyn PhysicalExpr>,
    schema: &SchemaRef,
) -> Result<Arc<dyn PhysicalExpr>> {
    Ok(expr
        .transform_up(|expr| {
            if let Some(col) = expr.as_any().downcast_ref::<Column>() {
                let idx = col.index();
                let Some(field) = schema.fields().get(idx) else {
                    return plan_err!(
                        "New schema has fewer columns than original schema"
                    );
                };
                let new_col = Column::new(field.name(), idx);
                Ok(Transformed::yes(Arc::new(new_col) as _))
            } else {
                Ok(Transformed::no(expr))
            }
        })?
        .data)
}

#[cfg(test)]
mod test {
    use super::Column;
    use crate::physical_expr::PhysicalExpr;

    use arrow::array::StringArray;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use datafusion_common::Result;

    use std::sync::Arc;

    #[test]
    fn out_of_bounds_data_type() {
        let schema = Schema::new(vec![Field::new("foo", DataType::Utf8, true)]);
        let col = Column::new("id", 9);
        let error = col.data_type(&schema).expect_err("error").strip_backtrace();
        assert!("Internal error: PhysicalExpr Column references column 'id' at index 9 (zero-based) \
            but input schema only has 1 columns: [\"foo\"].\nThis was likely caused by a bug in \
            DataFusion's code and we would welcome that you file an bug report in our issue tracker".starts_with(&error))
    }

    #[test]
    fn out_of_bounds_nullable() {
        let schema = Schema::new(vec![Field::new("foo", DataType::Utf8, true)]);
        let col = Column::new("id", 9);
        let error = col.nullable(&schema).expect_err("error").strip_backtrace();
        assert!("Internal error: PhysicalExpr Column references column 'id' at index 9 (zero-based) \
            but input schema only has 1 columns: [\"foo\"].\nThis was likely caused by a bug in \
            DataFusion's code and we would welcome that you file an bug report in our issue tracker".starts_with(&error))
    }

    #[test]
    fn out_of_bounds_evaluate() -> Result<()> {
        let schema = Schema::new(vec![Field::new("foo", DataType::Utf8, true)]);
        let data: StringArray = vec!["data"].into();
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(data)])?;
        let col = Column::new("id", 9);
        let error = col.evaluate(&batch).expect_err("error").strip_backtrace();
        assert!("Internal error: PhysicalExpr Column references column 'id' at index 9 (zero-based) \
            but input schema only has 1 columns: [\"foo\"].\nThis was likely caused by a bug in \
            DataFusion's code and we would welcome that you file an bug report in our issue tracker".starts_with(&error));
        Ok(())
    }
}
