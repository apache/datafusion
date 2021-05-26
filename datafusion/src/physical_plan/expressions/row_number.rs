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
use crate::physical_plan::{
    window_functions::BuiltInWindowFunctionExpr, PhysicalExpr, WindowAccumulator,
};
use crate::scalar::ScalarValue;
use arrow::array::{ArrayRef, UInt64Array};
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
        Ok(Field::new(&self.name(), data_type, nullable))
    }

    fn expressions(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        vec![]
    }

    fn name(&self) -> &str {
        self.name.as_str()
    }

    fn create_accumulator(&self) -> Result<Box<dyn WindowAccumulator>> {
        Ok(Box::new(RowNumberAccumulator::new()))
    }
}

#[derive(Debug)]
struct RowNumberAccumulator {
    row_number: u64,
}

impl RowNumberAccumulator {
    /// new row_number accumulator
    pub fn new() -> Self {
        // row number is 1 based
        Self { row_number: 1 }
    }
}

impl WindowAccumulator for RowNumberAccumulator {
    fn scan(&mut self, _values: &[ScalarValue]) -> Result<Option<ScalarValue>> {
        let result = Some(ScalarValue::UInt64(Some(self.row_number)));
        self.row_number += 1;
        Ok(result)
    }

    fn scan_batch(
        &mut self,
        num_rows: usize,
        _values: &[ArrayRef],
    ) -> Result<Option<ArrayRef>> {
        let new_row_number = self.row_number + (num_rows as u64);
        // TODO: probably would be nice to have a (optimized) kernel for this at some point to
        // generate an array like this.
        let result = UInt64Array::from_iter_values(self.row_number..new_row_number);
        self.row_number = new_row_number;
        Ok(Some(Arc::new(result)))
    }

    fn evaluate(&self) -> Result<Option<ScalarValue>> {
        Ok(None)
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

        let row_number = Arc::new(RowNumber::new("row_number".to_owned()));

        let mut acc = row_number.create_accumulator()?;
        let expr = row_number.expressions();
        let values = expr
            .iter()
            .map(|e| e.evaluate(&batch))
            .map(|r| r.map(|v| v.into_array(batch.num_rows())))
            .collect::<Result<Vec<_>>>()?;

        let result = acc.scan_batch(batch.num_rows(), &values)?;
        assert_eq!(true, result.is_some());

        let result = result.unwrap();
        let result = result.as_any().downcast_ref::<UInt64Array>().unwrap();
        let result = result.values();
        assert_eq!(vec![1, 2, 3, 4, 5, 6, 7, 8], result);

        let result = acc.evaluate()?;
        assert_eq!(false, result.is_some());
        Ok(())
    }

    #[test]
    fn row_number_all_values() -> Result<()> {
        let arr: ArrayRef = Arc::new(BooleanArray::from(vec![
            true, false, true, false, false, true, false, true,
        ]));
        let schema = Schema::new(vec![Field::new("arr", DataType::Boolean, false)]);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![arr])?;

        let row_number = Arc::new(RowNumber::new("row_number".to_owned()));

        let mut acc = row_number.create_accumulator()?;
        let expr = row_number.expressions();
        let values = expr
            .iter()
            .map(|e| e.evaluate(&batch))
            .map(|r| r.map(|v| v.into_array(batch.num_rows())))
            .collect::<Result<Vec<_>>>()?;

        let result = acc.scan_batch(batch.num_rows(), &values)?;
        assert_eq!(true, result.is_some());

        let result = result.unwrap();
        let result = result.as_any().downcast_ref::<UInt64Array>().unwrap();
        let result = result.values();
        assert_eq!(vec![1, 2, 3, 4, 5, 6, 7, 8], result);

        let result = acc.evaluate()?;
        assert_eq!(false, result.is_some());
        Ok(())
    }
}
