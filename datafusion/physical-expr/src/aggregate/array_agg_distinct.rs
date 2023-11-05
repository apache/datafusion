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

use arrow::array::ArrayRef;
use std::collections::HashSet;

use crate::aggregate::utils::down_cast_any_ref;
use crate::expressions::format_state_name;
use crate::{AggregateExpr, PhysicalExpr};
use datafusion_common::{Result, ScalarValue};
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
    /// Whether the input expression can produce NULL values
    is_expr_nullable: bool,
}

impl DistinctArrayAgg {
    /// Create a new DistinctArrayAgg aggregate function
    pub fn new(
        expr: Arc<dyn PhysicalExpr>,
        name: impl Into<String>,
        input_data_type: DataType,
        is_expr_nullable: bool,
    ) -> Self {
        let name = name.into();
        Self {
            name,
            expr,
            input_data_type,
            is_expr_nullable,
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
            // This should be the same as return type of AggregateFunction::ArrayAgg
            Field::new("item", self.input_data_type.clone(), true),
            self.is_expr_nullable,
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
            self.is_expr_nullable,
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
        Ok(vec![self.evaluate()?])
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        assert_eq!(values.len(), 1, "batch input should only include 1 column!");

        let array = &values[0];
        let scalars = ScalarValue::convert_array_to_scalar_vec(array)?;
        for scalar in scalars {
            self.values.extend(scalar)
        }
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        if states.is_empty() {
            return Ok(());
        }

        assert_eq!(
            states.len(),
            1,
            "array_agg_distinct states must contain single array"
        );

        let scalar_vec = ScalarValue::convert_array_to_scalar_vec(&states[0])?;
        for scalars in scalar_vec {
            self.values.extend(scalars)
        }

        Ok(())
    }

    fn evaluate(&self) -> Result<ScalarValue> {
        let values: Vec<ScalarValue> = self.values.iter().cloned().collect();
        let arr = ScalarValue::new_list(&values, &self.datatype);
        Ok(ScalarValue::List(arr))
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
    use arrow_array::cast::as_list_array;
    use arrow_array::types::Int32Type;
    use arrow_array::{Array, ListArray};
    use arrow_buffer::OffsetBuffer;
    use datafusion_common::utils::array_into_list_array;
    use datafusion_common::{internal_err, DataFusionError};

    // arrow::compute::sort cann't sort ListArray directly, so we need to sort the inner primitive array and wrap it back into ListArray.
    fn sort_list_inner(arr: ScalarValue) -> ScalarValue {
        let arr = match arr {
            ScalarValue::List(arr) => {
                let list_arr = as_list_array(&arr);
                list_arr.value(0)
            }
            _ => {
                panic!("Expected ScalarValue::List, got {:?}", arr)
            }
        };

        let arr = arrow::compute::sort(&arr, None).unwrap();
        let list_arr = array_into_list_array(arr);
        ScalarValue::List(Arc::new(list_arr))
    }

    fn compare_list_contents(expected: ScalarValue, actual: ScalarValue) -> Result<()> {
        let actual = sort_list_inner(actual);

        match (&expected, &actual) {
            (ScalarValue::List(arr1), ScalarValue::List(arr2)) => {
                if arr1.eq(arr2) {
                    Ok(())
                } else {
                    internal_err!(
                        "Actual value {:?} not found in expected values {:?}",
                        actual,
                        expected
                    )
                }
            }
            _ => {
                internal_err!("Expected scalar lists as inputs")
            }
        }
    }

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
            true,
        ));
        let actual = aggregate(&batch, agg)?;

        compare_list_contents(expected, actual)
    }

    fn check_merge_distinct_array_agg(
        input1: ArrayRef,
        input2: ArrayRef,
        expected: ScalarValue,
        datatype: DataType,
    ) -> Result<()> {
        let schema = Schema::new(vec![Field::new("a", datatype.clone(), false)]);
        let agg = Arc::new(DistinctArrayAgg::new(
            col("a", &schema)?,
            "bla".to_string(),
            datatype,
            true,
        ));

        let mut accum1 = agg.create_accumulator()?;
        let mut accum2 = agg.create_accumulator()?;

        accum1.update_batch(&[input1])?;
        accum2.update_batch(&[input2])?;

        let array = accum2.state()?[0].raw_data()?;
        accum1.merge_batch(&[array])?;

        let actual = accum1.evaluate()?;

        compare_list_contents(expected, actual)
    }

    #[test]
    fn distinct_array_agg_i32() -> Result<()> {
        let col: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 7, 4, 5, 2]));
        let expected =
            ScalarValue::List(Arc::new(
                ListArray::from_iter_primitive::<Int32Type, _, _>(vec![Some(vec![
                    Some(1),
                    Some(2),
                    Some(4),
                    Some(5),
                    Some(7),
                ])]),
            ));

        check_distinct_array_agg(col, expected, DataType::Int32)
    }

    #[test]
    fn merge_distinct_array_agg_i32() -> Result<()> {
        let col1: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 7, 4, 5, 2]));
        let col2: ArrayRef = Arc::new(Int32Array::from(vec![1, 3, 7, 8, 4]));

        let expected =
            ScalarValue::List(Arc::new(
                ListArray::from_iter_primitive::<Int32Type, _, _>(vec![Some(vec![
                    Some(1),
                    Some(2),
                    Some(3),
                    Some(4),
                    Some(5),
                    Some(7),
                    Some(8),
                ])]),
            ));

        check_merge_distinct_array_agg(col1, col2, expected, DataType::Int32)
    }

    #[test]
    fn distinct_array_agg_nested() -> Result<()> {
        // [[1, 2, 3], [4, 5]]
        let a1 = ListArray::from_iter_primitive::<Int32Type, _, _>(vec![Some(vec![
            Some(1),
            Some(2),
            Some(3),
        ])]);
        let a2 = ListArray::from_iter_primitive::<Int32Type, _, _>(vec![Some(vec![
            Some(4),
            Some(5),
        ])]);
        let l1 = ListArray::new(
            Arc::new(Field::new("item", a1.data_type().to_owned(), true)),
            OffsetBuffer::from_lengths([2]),
            arrow::compute::concat(&[&a1, &a2]).unwrap(),
            None,
        );

        // [[6], [7, 8]]
        let a1 =
            ListArray::from_iter_primitive::<Int32Type, _, _>(vec![Some(vec![Some(6)])]);
        let a2 = ListArray::from_iter_primitive::<Int32Type, _, _>(vec![Some(vec![
            Some(7),
            Some(8),
        ])]);
        let l2 = ListArray::new(
            Arc::new(Field::new("item", a1.data_type().to_owned(), true)),
            OffsetBuffer::from_lengths([2]),
            arrow::compute::concat(&[&a1, &a2]).unwrap(),
            None,
        );

        // [[9]]
        let a1 =
            ListArray::from_iter_primitive::<Int32Type, _, _>(vec![Some(vec![Some(9)])]);
        let l3 = ListArray::new(
            Arc::new(Field::new("item", a1.data_type().to_owned(), true)),
            OffsetBuffer::from_lengths([1]),
            Arc::new(a1),
            None,
        );

        let l1 = ScalarValue::List(Arc::new(l1));
        let l2 = ScalarValue::List(Arc::new(l2));
        let l3 = ScalarValue::List(Arc::new(l3));

        // Duplicate l1 in the input array and check that it is deduped in the output.
        let array = ScalarValue::iter_to_array(vec![l1.clone(), l2, l3, l1]).unwrap();

        let expected =
            ScalarValue::List(Arc::new(
                ListArray::from_iter_primitive::<Int32Type, _, _>(vec![Some(vec![
                    Some(1),
                    Some(2),
                    Some(3),
                    Some(4),
                    Some(5),
                    Some(6),
                    Some(7),
                    Some(8),
                    Some(9),
                ])]),
            ));

        check_distinct_array_agg(
            array,
            expected,
            DataType::List(Arc::new(Field::new_list(
                "item",
                Field::new("item", DataType::Int32, true),
                true,
            ))),
        )
    }

    #[test]
    fn merge_distinct_array_agg_nested() -> Result<()> {
        // [[1, 2], [3, 4]]
        let a1 = ListArray::from_iter_primitive::<Int32Type, _, _>(vec![Some(vec![
            Some(1),
            Some(2),
        ])]);
        let a2 = ListArray::from_iter_primitive::<Int32Type, _, _>(vec![Some(vec![
            Some(3),
            Some(4),
        ])]);
        let l1 = ListArray::new(
            Arc::new(Field::new("item", a1.data_type().to_owned(), true)),
            OffsetBuffer::from_lengths([2]),
            arrow::compute::concat(&[&a1, &a2]).unwrap(),
            None,
        );

        let a1 =
            ListArray::from_iter_primitive::<Int32Type, _, _>(vec![Some(vec![Some(5)])]);
        let l2 = ListArray::new(
            Arc::new(Field::new("item", a1.data_type().to_owned(), true)),
            OffsetBuffer::from_lengths([1]),
            Arc::new(a1),
            None,
        );

        // [[6, 7], [8]]
        let a1 = ListArray::from_iter_primitive::<Int32Type, _, _>(vec![Some(vec![
            Some(6),
            Some(7),
        ])]);
        let a2 =
            ListArray::from_iter_primitive::<Int32Type, _, _>(vec![Some(vec![Some(8)])]);
        let l3 = ListArray::new(
            Arc::new(Field::new("item", a1.data_type().to_owned(), true)),
            OffsetBuffer::from_lengths([2]),
            arrow::compute::concat(&[&a1, &a2]).unwrap(),
            None,
        );

        let l1 = ScalarValue::List(Arc::new(l1));
        let l2 = ScalarValue::List(Arc::new(l2));
        let l3 = ScalarValue::List(Arc::new(l3));

        // Duplicate l1 in the input array and check that it is deduped in the output.
        let input1 = ScalarValue::iter_to_array(vec![l1.clone(), l2]).unwrap();
        let input2 = ScalarValue::iter_to_array(vec![l1, l3]).unwrap();

        let expected =
            ScalarValue::List(Arc::new(
                ListArray::from_iter_primitive::<Int32Type, _, _>(vec![Some(vec![
                    Some(1),
                    Some(2),
                    Some(3),
                    Some(4),
                    Some(5),
                    Some(6),
                    Some(7),
                    Some(8),
                ])]),
            ));

        check_merge_distinct_array_agg(input1, input2, expected, DataType::Int32)
    }
}
