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

use super::format_state_name;
use crate::error::Result;
use crate::physical_plan::{Accumulator, AggregateExpr, PhysicalExpr};
use crate::scalar::ScalarValue;
use arrow::datatypes::{DataType, Field};
use std::any::Any;
use std::sync::Arc;

/// ARRAY_AGG aggregate expression
#[derive(Debug)]
pub struct ArrayAgg {
    name: String,
    input_data_type: DataType,
    expr: Arc<dyn PhysicalExpr>,
}

impl ArrayAgg {
    /// Create a new ArrayAgg aggregate function
    pub fn new(
        expr: Arc<dyn PhysicalExpr>,
        name: impl Into<String>,
        data_type: DataType,
    ) -> Self {
        Self {
            name: name.into(),
            expr,
            input_data_type: data_type,
        }
    }
}

impl AggregateExpr for ArrayAgg {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn field(&self) -> Result<Field> {
        Ok(Field::new(
            &self.name,
            DataType::List(Box::new(Field::new(
                "item",
                self.input_data_type.clone(),
                true,
            ))),
            false,
        ))
    }

    fn create_accumulator(&self) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(ArrayAggAccumulator::try_new(
            &self.input_data_type,
        )?))
    }

    fn state_fields(&self) -> Result<Vec<Field>> {
        Ok(vec![Field::new(
            &format_state_name(&self.name, "array_agg"),
            DataType::List(Box::new(Field::new(
                "item",
                self.input_data_type.clone(),
                true,
            ))),
            false,
        )])
    }

    fn expressions(&self) -> Vec<Arc<dyn PhysicalExpr>> {
        vec![self.expr.clone()]
    }

    fn name(&self) -> &str {
        &self.name
    }
}

#[derive(Debug)]
pub(crate) struct ArrayAggAccumulator {
    array: Vec<ScalarValue>,
    datatype: DataType,
}

impl ArrayAggAccumulator {
    /// new array_agg accumulator based on given item data type
    pub fn try_new(datatype: &DataType) -> Result<Self> {
        Ok(Self {
            array: vec![],
            datatype: datatype.clone(),
        })
    }
}

impl Accumulator for ArrayAggAccumulator {
    fn state(&self) -> Result<Vec<ScalarValue>> {
        Ok(vec![ScalarValue::List(
            Some(Box::new(self.array.clone())),
            Box::new(self.datatype.clone()),
        )])
    }

    fn update(&mut self, values: &[ScalarValue]) -> Result<()> {
        let value = &values[0];
        self.array.push(value.clone());

        Ok(())
    }

    fn merge(&mut self, states: &[ScalarValue]) -> Result<()> {
        if states.is_empty() {
            return Ok(());
        };

        assert!(states.len() == 1, "states length should be 1!");
        match &states[0] {
            ScalarValue::List(Some(array), _) => {
                self.array.extend((&**array).clone());
            }
            _ => unreachable!(),
        }
        Ok(())
    }

    fn evaluate(&self) -> Result<ScalarValue> {
        Ok(ScalarValue::List(
            Some(Box::new(self.array.clone())),
            Box::new(self.datatype.clone()),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::physical_plan::expressions::col;
    use crate::physical_plan::expressions::tests::aggregate;
    use crate::{error::Result, generic_test_op};
    use arrow::array::ArrayRef;
    use arrow::array::Int32Array;
    use arrow::datatypes::*;
    use arrow::record_batch::RecordBatch;

    #[test]
    fn array_agg_i32() -> Result<()> {
        let a: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5]));

        let list = ScalarValue::List(
            Some(Box::new(vec![
                ScalarValue::Int32(Some(1)),
                ScalarValue::Int32(Some(2)),
                ScalarValue::Int32(Some(3)),
                ScalarValue::Int32(Some(4)),
                ScalarValue::Int32(Some(5)),
            ])),
            Box::new(DataType::Int32),
        );

        generic_test_op!(a, DataType::Int32, ArrayAgg, list, DataType::Int32)
    }

    #[test]
    fn array_agg_nested() -> Result<()> {
        let l1 = ScalarValue::List(
            Some(Box::new(vec![
                ScalarValue::List(
                    Some(Box::new(vec![
                        ScalarValue::from(1i32),
                        ScalarValue::from(2i32),
                        ScalarValue::from(3i32),
                    ])),
                    Box::new(DataType::Int32),
                ),
                ScalarValue::List(
                    Some(Box::new(vec![
                        ScalarValue::from(4i32),
                        ScalarValue::from(5i32),
                    ])),
                    Box::new(DataType::Int32),
                ),
            ])),
            Box::new(DataType::List(Box::new(Field::new(
                "item",
                DataType::Int32,
                true,
            )))),
        );

        let l2 = ScalarValue::List(
            Some(Box::new(vec![
                ScalarValue::List(
                    Some(Box::new(vec![ScalarValue::from(6i32)])),
                    Box::new(DataType::Int32),
                ),
                ScalarValue::List(
                    Some(Box::new(vec![
                        ScalarValue::from(7i32),
                        ScalarValue::from(8i32),
                    ])),
                    Box::new(DataType::Int32),
                ),
            ])),
            Box::new(DataType::List(Box::new(Field::new(
                "item",
                DataType::Int32,
                true,
            )))),
        );

        let l3 = ScalarValue::List(
            Some(Box::new(vec![ScalarValue::List(
                Some(Box::new(vec![ScalarValue::from(9i32)])),
                Box::new(DataType::Int32),
            )])),
            Box::new(DataType::List(Box::new(Field::new(
                "item",
                DataType::Int32,
                true,
            )))),
        );

        let list = ScalarValue::List(
            Some(Box::new(vec![l1.clone(), l2.clone(), l3.clone()])),
            Box::new(DataType::List(Box::new(Field::new(
                "item",
                DataType::Int32,
                true,
            )))),
        );

        let array = ScalarValue::iter_to_array(vec![l1, l2, l3]).unwrap();

        generic_test_op!(
            array,
            DataType::List(Box::new(Field::new(
                "item",
                DataType::List(Box::new(Field::new("item", DataType::Int32, true,))),
                true,
            ))),
            ArrayAgg,
            list,
            DataType::List(Box::new(Field::new("item", DataType::Int32, true,)))
        )
    }
}
