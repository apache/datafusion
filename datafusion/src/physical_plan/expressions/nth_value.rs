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
use crate::physical_plan::{
    window_functions::BuiltInWindowFunctionExpr, PhysicalExpr, WindowAccumulator,
};
use crate::scalar::ScalarValue;
use arrow::datatypes::{DataType, Field};
use std::any::Any;
use std::convert::TryFrom;
use std::sync::Arc;

/// nth_value kind
#[derive(Debug, Copy, Clone)]
enum NthValueKind {
    First,
    Last,
    Nth(u32),
}

/// nth_value expression
#[derive(Debug)]
pub struct NthValue {
    name: String,
    expr: Arc<dyn PhysicalExpr>,
    data_type: DataType,
    kind: NthValueKind,
}

impl NthValue {
    /// Create a new FIRST_VALUE window aggregate function
    pub fn first_value(
        name: String,
        expr: Arc<dyn PhysicalExpr>,
        data_type: DataType,
    ) -> Self {
        Self {
            name,
            expr,
            data_type,
            kind: NthValueKind::First,
        }
    }

    /// Create a new LAST_VALUE window aggregate function
    pub fn last_value(
        name: String,
        expr: Arc<dyn PhysicalExpr>,
        data_type: DataType,
    ) -> Self {
        Self {
            name,
            expr,
            data_type,
            kind: NthValueKind::Last,
        }
    }

    /// Create a new NTH_VALUE window aggregate function
    pub fn nth_value(
        name: String,
        expr: Arc<dyn PhysicalExpr>,
        data_type: DataType,
        n: u32,
    ) -> Result<Self> {
        match n {
            0 => Err(DataFusionError::Execution(
                "nth_value expect n to be > 0".to_owned(),
            )),
            _ => Ok(Self {
                name,
                expr,
                data_type,
                kind: NthValueKind::Nth(n),
            }),
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
            self.kind,
            self.data_type.clone(),
        )?))
    }
}

#[derive(Debug)]
struct NthValueAccumulator {
    kind: NthValueKind,
    offset: u32,
    value: ScalarValue,
}

impl NthValueAccumulator {
    /// new count accumulator
    pub fn try_new(kind: NthValueKind, data_type: DataType) -> Result<Self> {
        Ok(Self {
            kind,
            offset: 0,
            // null value of that data_type by default
            value: ScalarValue::try_from(&data_type)?,
        })
    }
}

impl WindowAccumulator for NthValueAccumulator {
    fn scan(&mut self, values: &[ScalarValue]) -> Result<Option<ScalarValue>> {
        self.offset += 1;
        match self.kind {
            NthValueKind::Last => {
                self.value = values[0].clone();
            }
            NthValueKind::First if self.offset == 1 => {
                self.value = values[0].clone();
            }
            NthValueKind::Nth(n) if self.offset == n => {
                self.value = values[0].clone();
            }
            _ => {}
        }

        Ok(None)
    }

    fn evaluate(&self) -> Result<Option<ScalarValue>> {
        Ok(Some(self.value.clone()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::Result;
    use crate::physical_plan::expressions::col;
    use arrow::record_batch::RecordBatch;
    use arrow::{array::*, datatypes::*};

    fn test_i32_result(expr: Arc<NthValue>, expected: i32) -> Result<()> {
        let arr: ArrayRef = Arc::new(Int32Array::from(vec![1, -2, 3, -4, 5, -6, 7, 8]));
        let schema = Schema::new(vec![Field::new("arr", DataType::Int32, false)]);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![arr])?;

        let mut acc = expr.create_accumulator()?;
        let expr = expr.expressions();
        let values = expr
            .iter()
            .map(|e| e.evaluate(&batch))
            .map(|r| r.map(|v| v.into_array(batch.num_rows())))
            .collect::<Result<Vec<_>>>()?;
        let result = acc.scan_batch(batch.num_rows(), &values)?;
        assert_eq!(false, result.is_some());
        let result = acc.evaluate()?;
        assert_eq!(Some(ScalarValue::Int32(Some(expected))), result);
        Ok(())
    }

    #[test]
    fn first_value() -> Result<()> {
        let first_value = Arc::new(NthValue::first_value(
            "first_value".to_owned(),
            col("arr"),
            DataType::Int32,
        ));
        test_i32_result(first_value, 1)?;
        Ok(())
    }

    #[test]
    fn last_value() -> Result<()> {
        let last_value = Arc::new(NthValue::last_value(
            "last_value".to_owned(),
            col("arr"),
            DataType::Int32,
        ));
        test_i32_result(last_value, 8)?;
        Ok(())
    }

    #[test]
    fn nth_value_1() -> Result<()> {
        let nth_value = Arc::new(NthValue::nth_value(
            "nth_value".to_owned(),
            col("arr"),
            DataType::Int32,
            1,
        )?);
        test_i32_result(nth_value, 1)?;
        Ok(())
    }

    #[test]
    fn nth_value_2() -> Result<()> {
        let nth_value = Arc::new(NthValue::nth_value(
            "nth_value".to_owned(),
            col("arr"),
            DataType::Int32,
            2,
        )?);
        test_i32_result(nth_value, -2)?;
        Ok(())
    }
}
