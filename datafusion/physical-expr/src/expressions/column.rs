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

use crate::physical_expr::down_cast_any_ref;
use crate::PhysicalExpr;

use arrow::{
    datatypes::{DataType, Schema},
    record_batch::RecordBatch,
};
use datafusion_common::{internal_err, Result};
use datafusion_expr::ColumnarValue;

#[derive(Debug, Hash, PartialEq, Eq, Clone)]
pub struct UnKnownColumn {
    name: String,
}

impl UnKnownColumn {
    /// Create a new unknown column expression
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_owned(),
        }
    }

    /// Get the column name
    pub fn name(&self) -> &str {
        &self.name
    }
}

impl std::fmt::Display for UnKnownColumn {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.name)
    }
}

impl PhysicalExpr for UnKnownColumn {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    /// Get the data type of this expression, given the schema of the input
    fn data_type(&self, _input_schema: &Schema) -> Result<DataType> {
        Ok(DataType::Null)
    }

    /// Decide whehter this expression is nullable, given the schema of the input
    fn nullable(&self, _input_schema: &Schema) -> Result<bool> {
        Ok(true)
    }

    /// Evaluate the expression
    fn evaluate(&self, _batch: &RecordBatch) -> Result<ColumnarValue> {
        internal_err!("UnKnownColumn::evaluate() should not be called")
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

impl PartialEq<dyn Any> for UnKnownColumn {
    fn eq(&self, other: &dyn Any) -> bool {
        down_cast_any_ref(other)
            .downcast_ref::<Self>()
            .map(|x| self == x)
            .unwrap_or(false)
    }
}

#[cfg(test)]
mod test {
    use crate::expressions::Column;
    use crate::PhysicalExpr;

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
