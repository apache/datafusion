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

//! Implementations for DISTINCT expressions, e.g. `COUNT(DISTINCT c)`

use arrow::datatypes::{DataType, Field};
use std::any::Any;
use std::fmt::Debug;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef};
use std::collections::HashSet;

use crate::aggregate::utils::down_cast_any_ref;
use crate::expressions::format_state_name;
use crate::{AggregateExpr, PhysicalExpr};
use datafusion_common::Result;
use datafusion_common::ScalarValue;
use datafusion_expr::Accumulator;

/// Expression for a ARRAY_AGG(DISTINCT) aggregation.
#[derive(Debug)]
pub struct DistinctArrayAgg {
    /// Column name
    name: String,
    /// The DataType for the input expression
    input_data_type: DataType,
    /// The input expression
    expr: Arc<dyn PhysicalExpr>,
}

impl DistinctArrayAgg {
    /// Create a new DistinctArrayAgg aggregate function
    pub fn new(
        expr: Arc<dyn PhysicalExpr>,
        name: impl Into<String>,
        input_data_type: DataType,
    ) -> Self {
        let name = name.into();
        Self {
            name,
            expr,
            input_data_type,
        }
    }
}

impl AggregateExpr for DistinctArrayAgg {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn field(&self) -> Result<Field> {
        Ok(Field::new_list(
            &self.name,
            Field::new("item", self.input_data_type.clone(), true),
            false,
        ))
    }

    fn create_accumulator(&self) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(DistinctArrayAggAccumulator::try_new(
            &self.input_data_type,
        )?))
    }

    fn state_fields(&self) -> Result<Vec<Field>> {
        Ok(vec![Field::new_list(
            format_state_name(&self.name, "distinct_array_agg"),
            Field::new("item", self.input_data_type.clone(), true),
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

impl PartialEq<dyn Any> for DistinctArrayAgg {
    fn eq(&self, other: &dyn Any) -> bool {
        down_cast_any_ref(other)
            .downcast_ref::<Self>()
            .map(|x| {
                self.name == x.name
                    && self.input_data_type == x.input_data_type
                    && self.expr.eq(&x.expr)
            })
            .unwrap_or(false)
    }
}

#[derive(Debug)]
struct DistinctArrayAggAccumulator {
    values: HashSet<ScalarValue>,
    datatype: DataType,
}

impl DistinctArrayAggAccumulator {
    pub fn try_new(datatype: &DataType) -> Result<Self> {
        Ok(Self {
            values: HashSet::new(),
            datatype: datatype.clone(),
        })
    }
}

impl Accumulator for DistinctArrayAggAccumulator {
    fn state(&self) -> Result<Vec<ScalarValue>> {
        Ok(vec![ScalarValue::new_list(
            Some(self.values.clone().into_iter().collect()),
            self.datatype.clone(),
        )])
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        assert_eq!(values.len(), 1, "batch input should only include 1 column!");

        let arr = &values[0];
        for i in 0..arr.len() {
            self.values.insert(ScalarValue::try_from_array(arr, i)?);
        }
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        if states.is_empty() {
            return Ok(());
        }

        for array in states {
            for j in 0..array.len() {
                self.values.insert(ScalarValue::try_from_array(array, j)?);
            }
        }

        Ok(())
    }

    fn evaluate(&self) -> Result<ScalarValue> {
        Ok(ScalarValue::new_list(
            Some(self.values.clone().into_iter().collect()),
            self.datatype.clone(),
        ))
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self) + ScalarValue::size_of_hashset(&self.values)
            - std::mem::size_of_val(&self.values)
            + self.datatype.size()
            - std::mem::size_of_val(&self.datatype)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expressions::col;
    use crate::expressions::tests::aggregate;
    use arrow::array::{ArrayRef, Int32Array};
    use arrow::datatypes::{DataType, Schema};
    use arrow::record_batch::RecordBatch;

    fn check_distinct_array_agg(
        input: ArrayRef,
        expected: ScalarValue,
        datatype: DataType,
    ) -> Result<()> {
        let schema = Schema::new(vec![Field::new("a", datatype.clone(), false)]);
        let batch = RecordBatch::try_new(Arc::new(schema.clone()), vec![input])?;

        let agg = Arc::new(DistinctArrayAgg::new(
            col("a", &schema)?,
            "bla".to_string(),
            datatype,
        ));
        let actual = aggregate(&batch, agg)?;

        match (expected, actual) {
            (ScalarValue::List(Some(mut e), _), ScalarValue::List(Some(mut a), _)) => {
                // workaround lack of Ord of ScalarValue
                let cmp = |a: &ScalarValue, b: &ScalarValue| {
                    a.partial_cmp(b).expect("Can compare ScalarValues")
                };

                e.sort_by(cmp);
                a.sort_by(cmp);
                // Check that the inputs are the same
                assert_eq!(e, a);
            }
            _ => {
                unreachable!()
            }
        }

        Ok(())
    }

    #[test]
    fn distinct_array_agg_i32() -> Result<()> {
        let col: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 7, 4, 5, 2]));

        let out = ScalarValue::new_list(
            Some(vec![
                ScalarValue::Int32(Some(1)),
                ScalarValue::Int32(Some(2)),
                ScalarValue::Int32(Some(7)),
                ScalarValue::Int32(Some(4)),
                ScalarValue::Int32(Some(5)),
            ]),
            DataType::Int32,
        );

        check_distinct_array_agg(col, out, DataType::Int32)
    }

    #[test]
    fn distinct_array_agg_nested() -> Result<()> {
        // [[1, 2, 3], [4, 5]]
        let l1 = ScalarValue::new_list(
            Some(vec![
                ScalarValue::new_list(
                    Some(vec![
                        ScalarValue::from(1i32),
                        ScalarValue::from(2i32),
                        ScalarValue::from(3i32),
                    ]),
                    DataType::Int32,
                ),
                ScalarValue::new_list(
                    Some(vec![ScalarValue::from(4i32), ScalarValue::from(5i32)]),
                    DataType::Int32,
                ),
            ]),
            DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
        );

        // [[6], [7, 8]]
        let l2 = ScalarValue::new_list(
            Some(vec![
                ScalarValue::new_list(
                    Some(vec![ScalarValue::from(6i32)]),
                    DataType::Int32,
                ),
                ScalarValue::new_list(
                    Some(vec![ScalarValue::from(7i32), ScalarValue::from(8i32)]),
                    DataType::Int32,
                ),
            ]),
            DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
        );

        // [[9]]
        let l3 = ScalarValue::new_list(
            Some(vec![ScalarValue::new_list(
                Some(vec![ScalarValue::from(9i32)]),
                DataType::Int32,
            )]),
            DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
        );

        let list = ScalarValue::new_list(
            Some(vec![l1.clone(), l2.clone(), l3.clone()]),
            DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
        );

        // Duplicate l1 in the input array and check that it is deduped in the output.
        let array = ScalarValue::iter_to_array(vec![l1.clone(), l2, l3, l1]).unwrap();

        check_distinct_array_agg(
            array,
            list,
            DataType::List(Arc::new(Field::new_list(
                "item",
                Field::new("item", DataType::Int32, true),
                true,
            ))),
        )
    }
}
