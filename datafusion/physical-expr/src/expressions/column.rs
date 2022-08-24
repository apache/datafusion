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

use std::sync::Arc;

use arrow::{
    datatypes::{DataType, Schema},
    record_batch::RecordBatch,
};

use crate::PhysicalExpr;
use datafusion_common::{DataFusionError, Result};
use datafusion_expr::ColumnarValue;

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

    /// Decide whehter this expression is nullable, given the schema of the input
    fn nullable(&self, input_schema: &Schema) -> Result<bool> {
        self.bounds_check(input_schema)?;
        Ok(input_schema.field(self.index).is_nullable())
    }

    /// Evaluate the expression
    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        self.bounds_check(batch.schema().as_ref())?;
        Ok(ColumnarValue::Array(batch.column(self.index).clone()))
    }
}

impl Column {
    fn bounds_check(&self, input_schema: &Schema) -> Result<()> {
        if self.index < input_schema.fields.len() {
            Ok(())
        } else {
            Err(DataFusionError::Internal(format!(
                "PhysicalExpr Column references column '{}' at index {} (zero-based) but input schema only has {} columns: {:?}",
                self.name,
                self.index, input_schema.fields.len(), input_schema.fields().iter().map(|f| f.name().clone()).collect::<Vec<String>>())))
        }
    }
}

/// Create a column expression
pub fn col(name: &str, schema: &Schema) -> Result<Arc<dyn PhysicalExpr>> {
    Ok(Arc::new(Column::new_with_schema(name, schema)?))
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
        let error = col.data_type(&schema).expect_err("error");
        assert_eq!("Internal error: PhysicalExpr Column references column 'id' at index 9 (zero-based) \
            but input schema only has 1 columns: [\"foo\"]. This was likely caused by a bug in \
            DataFusion's code and we would welcome that you file an bug report in our issue tracker",
           &format!("{}", error))
    }

    #[test]
    fn out_of_bounds_nullable() {
        let schema = Schema::new(vec![Field::new("foo", DataType::Utf8, true)]);
        let col = Column::new("id", 9);
        let error = col.nullable(&schema).expect_err("error");
        assert_eq!("Internal error: PhysicalExpr Column references column 'id' at index 9 (zero-based) \
            but input schema only has 1 columns: [\"foo\"]. This was likely caused by a bug in \
            DataFusion's code and we would welcome that you file an bug report in our issue tracker",
                   &format!("{}", error))
    }

    #[test]
    fn out_of_bounds_evaluate() -> Result<()> {
        let schema = Schema::new(vec![Field::new("foo", DataType::Utf8, true)]);
        let data: StringArray = vec!["data"].into();
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(data)])?;
        let col = Column::new("id", 9);
        let error = col.evaluate(&batch).expect_err("error");
        assert_eq!("Internal error: PhysicalExpr Column references column 'id' at index 9 (zero-based) \
            but input schema only has 1 columns: [\"foo\"]. This was likely caused by a bug in \
            DataFusion's code and we would welcome that you file an bug report in our issue tracker",
                   &format!("{}", error));
        Ok(())
    }
}
