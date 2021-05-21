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

//! Defines physical expressions that can evaluated at runtime during query execution

use crate::error::Result;
use crate::physical_plan::{BuiltInWindowFunctionExpr, PhysicalExpr, WindowAccumulator};
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
    /// Create a new MAX aggregate function
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
        Ok(Field::new(&self.name, data_type, nullable))
    }

    fn expressions(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        vec![]
    }

    fn name(&self) -> &str {
        &self.name
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
    /// new count accumulator
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
    ) -> Result<Option<Vec<ScalarValue>>> {
        let new_row_number = self.row_number + (num_rows as u64);
        let result = (self.row_number..new_row_number)
            .map(|i| ScalarValue::UInt64(Some(i)))
            .collect();
        self.row_number = new_row_number;
        Ok(Some(result))
    }

    fn evaluate(&self) -> Result<Option<ScalarValue>> {
        Ok(None)
    }
}
