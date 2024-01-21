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

use crate::expressions::Column;
use crate::window::window_expr::NumRowsState;
use crate::window::BuiltInWindowFunctionExpr;
use crate::{PhysicalExpr, PhysicalSortExpr};

use arrow::array::{ArrayRef, UInt64Array};
use arrow::datatypes::{DataType, Field};
use arrow_schema::{SchemaRef, SortOptions};
use datafusion_common::{Result, ScalarValue};
use datafusion_expr::PartitionEvaluator;

use std::any::Any;
use std::ops::Range;
use std::sync::Arc;

/// row_number expression
#[derive(Debug)]
pub struct RowNumber {
    name: String,
    /// Output data type
    data_type: DataType,
}

impl RowNumber {
    /// Create a new ROW_NUMBER function
    pub fn new(name: impl Into<String>, data_type: &DataType) -> Self {
        Self {
            name: name.into(),
            data_type: data_type.clone(),
        }
    }
}

impl BuiltInWindowFunctionExpr for RowNumber {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn field(&self) -> Result<Field> {
        let nullable = false;
        Ok(Field::new(self.name(), self.data_type.clone(), nullable))
    }

    fn expressions(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        vec![]
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn get_result_ordering(&self, schema: &SchemaRef) -> Option<PhysicalSortExpr> {
        // The built-in ROW_NUMBER window function introduces a new ordering:
        schema.column_with_name(self.name()).map(|(idx, field)| {
            let expr = Arc::new(Column::new(field.name(), idx));
            let options = SortOptions {
                descending: false,
                nulls_first: false,
            }; // ASC, NULLS LAST
            PhysicalSortExpr { expr, options }
        })
    }

    fn create_evaluator(&self) -> Result<Box<dyn PartitionEvaluator>> {
        Ok(Box::<NumRowsEvaluator>::default())
    }
}

#[derive(Default, Debug)]
pub(crate) struct NumRowsEvaluator {
    state: NumRowsState,
}

impl PartitionEvaluator for NumRowsEvaluator {
    /// evaluate window function result inside given range
    fn evaluate(
        &mut self,
        _values: &[ArrayRef],
        _range: &Range<usize>,
    ) -> Result<ScalarValue> {
        self.state.n_rows += 1;
        Ok(ScalarValue::UInt64(Some(self.state.n_rows as u64)))
    }

    fn evaluate_all(
        &mut self,
        _values: &[ArrayRef],
        num_rows: usize,
    ) -> Result<ArrayRef> {
        Ok(Arc::new(UInt64Array::from_iter_values(
            1..(num_rows as u64) + 1,
        )))
    }

    fn supports_bounded_execution(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::record_batch::RecordBatch;
    use arrow::{array::*, datatypes::*};
    use datafusion_common::{cast::as_uint64_array, Result};

    #[test]
    fn row_number_all_null() -> Result<()> {
        let arr: ArrayRef = Arc::new(BooleanArray::from(vec![
            None, None, None, None, None, None, None, None,
        ]));
        let schema = Schema::new(vec![Field::new("arr", DataType::Boolean, true)]);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![arr])?;
        let row_number = RowNumber::new("row_number".to_owned(), &DataType::UInt64);
        let values = row_number.evaluate_args(&batch)?;
        let result = row_number
            .create_evaluator()?
            .evaluate_all(&values, batch.num_rows())?;
        let result = as_uint64_array(&result)?;
        let result = result.values();
        assert_eq!(vec![1, 2, 3, 4, 5, 6, 7, 8], *result);
        Ok(())
    }

    #[test]
    fn row_number_all_values() -> Result<()> {
        let arr: ArrayRef = Arc::new(BooleanArray::from(vec![
            true, false, true, false, false, true, false, true,
        ]));
        let schema = Schema::new(vec![Field::new("arr", DataType::Boolean, false)]);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![arr])?;
        let row_number = RowNumber::new("row_number".to_owned(), &DataType::UInt64);
        let values = row_number.evaluate_args(&batch)?;
        let result = row_number
            .create_evaluator()?
            .evaluate_all(&values, batch.num_rows())?;
        let result = as_uint64_array(&result)?;
        let result = result.values();
        assert_eq!(vec![1, 2, 3, 4, 5, 6, 7, 8], *result);
        Ok(())
    }
}
