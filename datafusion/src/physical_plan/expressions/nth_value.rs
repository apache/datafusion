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

use crate::error::{DataFusionError, Result};
use crate::physical_plan::{BuiltInWindowFunctionExpr, PhysicalExpr, WindowAccumulator};
use crate::scalar::ScalarValue;
use arrow::datatypes::{DataType, Field};
use std::any::Any;
use std::convert::TryFrom;
use std::sync::Arc;

/// first_value expression
#[derive(Debug)]
pub struct FirstValue {
    name: String,
    data_type: DataType,
    expr: Arc<dyn PhysicalExpr>,
}

impl FirstValue {
    /// Create a new FIRST_VALUE window aggregate function
    pub fn new(expr: Arc<dyn PhysicalExpr>, name: String, data_type: DataType) -> Self {
        Self {
            name,
            data_type,
            expr,
        }
    }
}

impl BuiltInWindowFunctionExpr for FirstValue {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn field(&self) -> Result<Field> {
        let nullable = true;
        Ok(Field::new(&self.name, self.data_type.clone(), nullable))
    }

    fn expressions(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        vec![self.expr.clone()]
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn create_accumulator(&self) -> Result<Box<dyn WindowAccumulator>> {
        Ok(Box::new(NthValueAccumulator::try_new(
            1,
            self.data_type.clone(),
        )?))
    }
}

// sql values start with 1, so we can use 0 to indicate the special last value behavior
const SPECIAL_SIZE_VALUE_FOR_LAST: u32 = 0;

/// last_value expression
#[derive(Debug)]
pub struct LastValue {
    name: String,
    data_type: DataType,
    expr: Arc<dyn PhysicalExpr>,
}

impl LastValue {
    /// Create a new FIRST_VALUE window aggregate function
    pub fn new(expr: Arc<dyn PhysicalExpr>, name: String, data_type: DataType) -> Self {
        Self {
            name,
            data_type,
            expr,
        }
    }
}

impl BuiltInWindowFunctionExpr for LastValue {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn field(&self) -> Result<Field> {
        let nullable = true;
        Ok(Field::new(&self.name, self.data_type.clone(), nullable))
    }

    fn expressions(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        vec![self.expr.clone()]
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn create_accumulator(&self) -> Result<Box<dyn WindowAccumulator>> {
        Ok(Box::new(NthValueAccumulator::try_new(
            SPECIAL_SIZE_VALUE_FOR_LAST,
            self.data_type.clone(),
        )?))
    }
}

/// nth_value expression
#[derive(Debug)]
pub struct NthValue {
    name: String,
    n: u32,
    data_type: DataType,
    expr: Arc<dyn PhysicalExpr>,
}

impl NthValue {
    /// Create a new NTH_VALUE window aggregate function
    pub fn try_new(
        expr: Arc<dyn PhysicalExpr>,
        name: String,
        n: u32,
        data_type: DataType,
    ) -> Result<Self> {
        if n == SPECIAL_SIZE_VALUE_FOR_LAST {
            Err(DataFusionError::Execution(
                "nth_value expect n to be > 0".to_owned(),
            ))
        } else {
            Ok(Self {
                name,
                n,
                data_type,
                expr,
            })
        }
    }
}

impl BuiltInWindowFunctionExpr for NthValue {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn field(&self) -> Result<Field> {
        let nullable = true;
        Ok(Field::new(&self.name, self.data_type.clone(), nullable))
    }

    fn expressions(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        vec![self.expr.clone()]
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn create_accumulator(&self) -> Result<Box<dyn WindowAccumulator>> {
        Ok(Box::new(NthValueAccumulator::try_new(
            self.n,
            self.data_type.clone(),
        )?))
    }
}

#[derive(Debug)]
struct NthValueAccumulator {
    // n the target nth_value, however we'll reuse it for last_value acc, so when n == 0 it specifically
    // means last; also note that it is totally valid for n to be larger than the number of rows input
    // in which case all the values shall be null
    n: u32,
    offset: u32,
    value: ScalarValue,
}

impl NthValueAccumulator {
    /// new count accumulator
    pub fn try_new(n: u32, data_type: DataType) -> Result<Self> {
        Ok(Self {
            n,
            offset: 0,
            // null value of that data_type by default
            value: ScalarValue::try_from(&data_type)?,
        })
    }
}

impl WindowAccumulator for NthValueAccumulator {
    fn scan(&mut self, values: &[ScalarValue]) -> Result<Option<ScalarValue>> {
        if self.n == SPECIAL_SIZE_VALUE_FOR_LAST {
            // for last_value function
            self.value = values[0].clone();
        } else if self.offset < self.n {
            self.offset += 1;
            if self.offset == self.n {
                self.value = values[0].clone();
            }
        }
        Ok(None)
    }

    fn evaluate(&self) -> Result<Option<ScalarValue>> {
        Ok(Some(self.value.clone()))
    }
}
