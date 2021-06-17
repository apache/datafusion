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

//! Defines physical expression for `row_number` that can evaluated at runtime during query execution

use crate::error::Result;
use crate::physical_plan::{window_functions::BuiltInWindowFunctionExpr, PhysicalExpr};
use crate::scalar::ScalarValue;
use arrow::array::ArrayRef;
use arrow::datatypes::{DataType, Field};
use std::any::Any;
use std::sync::Arc;

/// row_number expression
#[derive(Debug)]
pub struct RowNumber {
    name: String,
}

impl RowNumber {
    /// Create a new ROW_NUMBER function
    pub fn new(name: String) -> Self {
        Self { name }
    }
}

impl BuiltInWindowFunctionExpr for RowNumber {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn field(&self) -> Result<Field> {
        let nullable = false;
        let data_type = DataType::UInt64;
        Ok(Field::new(self.name(), data_type, nullable))
    }

    fn expressions(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        vec![]
    }

    fn name(&self) -> &str {
        self.name.as_str()
    }

    fn evaluate(
        &self,
        num_rows: usize,
        _values: &[ArrayRef],
    ) -> Result<Box<dyn Iterator<Item = ScalarValue>>> {
        Ok(Box::new((1..(num_rows as u64) + 1).map(|i| i.into())))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::Result;
    use arrow::record_batch::RecordBatch;
    use arrow::{array::*, datatypes::*};

    #[test]
    fn row_number_all_null() -> Result<()> {
        let arr: ArrayRef = Arc::new(BooleanArray::from(vec![
            None, None, None, None, None, None, None, None,
        ]));
        let schema = Schema::new(vec![Field::new("arr", DataType::Boolean, false)]);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![arr])?;
        let row_number = RowNumber::new("row_number".to_owned());
        let result =
            ScalarValue::iter_to_array(row_number.evaluate(batch.num_rows(), &[])?)?;
        let result = result.as_any().downcast_ref::<UInt64Array>().unwrap();
        let result = result.values();
        assert_eq!(vec![1, 2, 3, 4, 5, 6, 7, 8], result);
        Ok(())
    }

    #[test]
    fn row_number_all_values() -> Result<()> {
        let arr: ArrayRef = Arc::new(BooleanArray::from(vec![
            true, false, true, false, false, true, false, true,
        ]));
        let schema = Schema::new(vec![Field::new("arr", DataType::Boolean, false)]);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![arr])?;
        let row_number = RowNumber::new("row_number".to_owned());
        let result =
            ScalarValue::iter_to_array(row_number.evaluate(batch.num_rows(), &[])?)?;
        let result = result.as_any().downcast_ref::<UInt64Array>().unwrap();
        let result = result.values();
        assert_eq!(vec![1, 2, 3, 4, 5, 6, 7, 8], result);
        Ok(())
    }
}
