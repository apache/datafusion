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

//! Defines physical expression for `ntile` that can evaluated
//! at runtime during query execution

use crate::window::partition_evaluator::PartitionEvaluator;
use crate::window::BuiltInWindowFunctionExpr;
use crate::PhysicalExpr;
use arrow::array::{ArrayRef, UInt64Array};
use arrow::datatypes::Field;
use arrow_schema::DataType;
use datafusion_common::Result;
use std::any::Any;
use std::sync::Arc;

#[derive(Debug)]
pub struct Ntile {
    name: String,
    n: u64,
}

impl Ntile {
    pub fn new(name: String, n: u64) -> Self {
        Self { name, n }
    }
}

impl BuiltInWindowFunctionExpr for Ntile {
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

    fn create_evaluator(&self) -> Result<Box<dyn PartitionEvaluator>> {
        Ok(Box::new(NtileEvaluator { n: self.n }))
    }
}

#[derive(Debug)]
pub(crate) struct NtileEvaluator {
    n: u64,
}

impl PartitionEvaluator for NtileEvaluator {
    fn evaluate(&self, _values: &[ArrayRef], num_rows: usize) -> Result<ArrayRef> {
        let num_rows = num_rows as u64;
        let mut vec: Vec<u64> = Vec::new();
        for i in 0..num_rows {
            let res = i * self.n / num_rows;
            vec.push(res + 1)
        }
        Ok(Arc::new(UInt64Array::from_iter_values(vec)))
    }
}
