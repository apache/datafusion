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

//! Defines physical expression for `set_agg` which aggregates unique values into an array

use super::format_state_name;
use crate::error::Result;
use crate::physical_plan::{Accumulator, AggregateExpr, PhysicalExpr};
use crate::scalar::ScalarValue;
use arrow::datatypes::{DataType, Field};
use hashbrown::HashSet;
use std::any::Any;
use std::sync::Arc;

/// SET_AGG aggregate expression
#[derive(Debug)]
pub struct SetAgg {
    name: String,
    input_data_type: DataType,
    expr: Arc<dyn PhysicalExpr>,
}

impl SetAgg {
    /// Create a new SetAgg aggregate function
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

impl AggregateExpr for SetAgg {
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
        Ok(Box::new(SetAggAccumulator::try_new(&self.input_data_type)?))
    }

    fn state_fields(&self) -> Result<Vec<Field>> {
        Ok(vec![Field::new(
            &format_state_name(&self.name, "set_agg"),
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
pub(crate) struct SetAggAccumulator {
    set: HashSet<ScalarValue>,
    datatype: DataType,
}

impl SetAggAccumulator {
    /// new set_agg accumulator based on given item data type
    pub fn try_new(datatype: &DataType) -> Result<Self> {
        Ok(Self {
            set: HashSet::new(),
            datatype: datatype.clone(),
        })
    }
}

impl Accumulator for SetAggAccumulator {
    fn state(&self) -> Result<Vec<ScalarValue>> {
        Ok(vec![ScalarValue::List(
            Some(Box::new(Vec::from_iter(self.set.clone()))),
            Box::new(self.datatype.clone()),
        )])
    }

    fn update(&mut self, values: &[ScalarValue]) -> Result<()> {
        let value = &values[0];
        self.set.insert(value.clone());

        Ok(())
    }

    fn merge(&mut self, states: &[ScalarValue]) -> Result<()> {
        if states.is_empty() {
            return Ok(());
        };

        assert!(states.len() == 1, "states length should be 1!");
        match &states[0] {
            ScalarValue::List(Some(array), _) => {
                for v in (&**array).iter() {
                    self.set.insert(v.clone());
                }
            }
            _ => unreachable!(),
        }
        Ok(())
    }

    fn evaluate(&self) -> Result<ScalarValue> {
        Ok(ScalarValue::List(
            Some(Box::new(Vec::from_iter(self.set.clone().into_iter()))),
            Box::new(self.datatype.clone()),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::Result;
    use crate::physical_plan::expressions::col;
    use crate::physical_plan::expressions::tests::aggregate;
    use arrow::array::ArrayRef;
    use arrow::array::Int32Array;
    use arrow::datatypes::*;
    use arrow::record_batch::RecordBatch;

    // When converting HashSet to Vec, ordering is unpredictable, so we are unable to use the
    // generic_test_op macro. This function is similar to generic_test_op except it checks for
    // the correct set_agg semantics by confirming the following:
    //   1. `expected` and `actual` have the same number of elements.
    //   2. `expected` contains no duplicates.
    //   3. `expected` and `actual` contain the same unique elements.
    fn check_set_agg(
        input: ArrayRef,
        expected: ScalarValue,
        datatype: DataType,
    ) -> Result<()> {
        let schema = Schema::new(vec![Field::new("a", datatype.clone(), false)]);
        let batch = RecordBatch::try_new(Arc::new(schema.clone()), vec![input])?;

        let agg = Arc::new(<SetAgg>::new(
            col("a", &schema)?,
            "bla".to_string(),
            datatype,
        ));
        let actual = aggregate(&batch, agg)?;

        match (expected, actual) {
            (ScalarValue::List(Some(e), _), ScalarValue::List(Some(a), _)) => {
                // Check that the inputs are the same length.
                assert_eq!(e.len(), a.len());

                let h1: HashSet<ScalarValue> = HashSet::from_iter(e.clone().into_iter());
                let h2: HashSet<ScalarValue> = HashSet::from_iter(a.into_iter());

                // Check that e's elements are unique.
                assert_eq!(h1.len(), e.len());

                // Check that a contains the same unique elements as e.
                assert_eq!(h1, h2);
            }
            _ => {
                unreachable!()
            }
        }

        Ok(())
    }

    #[test]
    fn set_agg_i32() -> Result<()> {
        let col: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 7, 4, 5, 2]));

        let out = ScalarValue::List(
            Some(Box::new(vec![
                ScalarValue::Int32(Some(1)),
                ScalarValue::Int32(Some(2)),
                ScalarValue::Int32(Some(7)),
                ScalarValue::Int32(Some(4)),
                ScalarValue::Int32(Some(5)),
            ])),
            Box::new(DataType::Int32),
        );

        check_set_agg(col, out, DataType::Int32)
    }

    #[test]
    fn set_agg_nested() -> Result<()> {
        // [[1, 2, 3], [4, 5]]
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

        // [[6], [7, 8]]
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

        // [[9]]
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

        // Duplicate l1 in the input array and check that it is deduped in the output.
        let array = ScalarValue::iter_to_array(vec![l1.clone(), l2, l3, l1]).unwrap();

        check_set_agg(
            array,
            list,
            DataType::List(Box::new(Field::new(
                "item",
                DataType::List(Box::new(Field::new("item", DataType::Int32, true))),
                true,
            ))),
        )
    }
}
