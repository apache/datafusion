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

use crate::window::partition_evaluator::PartitionEvaluator;
use crate::window::BuiltInWindowFunctionExpr;
use crate::PhysicalExpr;
use arrow::array::{ArrayRef, UInt64Array};
use arrow::datatypes::{DataType, Field};
use arrow::record_batch::RecordBatch;
use datafusion_common::Result;
use std::any::Any;
use std::ops::Range;
use std::sync::Arc;

/// row_number expression
#[derive(Debug)]
pub struct RowNumber {
    name: String,
}

impl RowNumber {
    /// Create a new ROW_NUMBER function
    pub fn new(name: impl Into<String>) -> Self {
        Self { name: name.into() }
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
        &self.name
    }

    fn create_evaluator(
        &self,
        _batch: &RecordBatch,
    ) -> Result<Box<dyn PartitionEvaluator>> {
        Ok(Box::new(NumRowsEvaluator::default()))
    }
}

#[derive(Default)]
pub(crate) struct NumRowsEvaluator {}

impl PartitionEvaluator for NumRowsEvaluator {
    fn evaluate_partition(&self, partition: Range<usize>) -> Result<ArrayRef> {
        let num_rows = partition.end - partition.start;
        Ok(Arc::new(UInt64Array::from_iter_values(
            1..(num_rows as u64) + 1,
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::record_batch::RecordBatch;
    use arrow::{array::*, datatypes::*};
    use datafusion_common::Result;

    #[test]
    fn row_number_all_null() -> Result<()> {
        let arr: ArrayRef = Arc::new(BooleanArray::from(vec![
            None, None, None, None, None, None, None, None,
        ]));
        let schema = Schema::new(vec![Field::new("arr", DataType::Boolean, true)]);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![arr])?;
        let row_number = RowNumber::new("row_number".to_owned());
        let result = row_number.create_evaluator(&batch)?.evaluate(vec![0..8])?;
        assert_eq!(1, result.len());
        let result = result[0].as_any().downcast_ref::<UInt64Array>().unwrap();
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
        let result = row_number.create_evaluator(&batch)?.evaluate(vec![0..8])?;
        assert_eq!(1, result.len());
        let result = result[0].as_any().downcast_ref::<UInt64Array>().unwrap();
        let result = result.values();
        assert_eq!(vec![1, 2, 3, 4, 5, 6, 7, 8], result);
        Ok(())
    }
}
