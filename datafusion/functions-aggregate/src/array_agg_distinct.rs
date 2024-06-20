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

use std::collections::HashSet;
use std::fmt::Debug;

use arrow::array::ArrayRef;
use arrow::datatypes::DataType;
use arrow_array::cast::AsArray;

use datafusion_common::{Result, ScalarValue};
use datafusion_expr::Accumulator;

#[derive(Debug)]
pub(crate) struct DistinctArrayAggAccumulator {
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
    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![self.evaluate()?])
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        assert_eq!(values.len(), 1, "batch input should only include 1 column!");

        let array = &values[0];

        for i in 0..array.len() {
            let scalar = ScalarValue::try_from_array(&array, i)?;
            self.values.insert(scalar);
        }

        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        if states.is_empty() {
            return Ok(());
        }

        states[0]
            .as_list::<i32>()
            .iter()
            .flatten()
            .try_for_each(|val| self.update_batch(&[val]))
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
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

    use std::sync::Arc;

    use super::*;
    use crate::array_agg::array_agg_udaf;
    use arrow::array::Int32Array;
    use arrow::datatypes::Schema;
    use arrow::record_batch::RecordBatch;
    use arrow_array::types::Int32Type;
    use arrow_array::Array;
    use arrow_array::ListArray;
    use arrow_buffer::OffsetBuffer;
    use arrow_schema::Field;
    use datafusion_common::internal_err;
    use datafusion_physical_expr_common::aggregate::create_aggregate_expr;
    use datafusion_physical_expr_common::expressions::column::col;

    // arrow::compute::sort can't sort nested ListArray directly, so we compare the scalar values pair-wise.
    fn compare_list_contents(
        expected: Vec<ScalarValue>,
        actual: ScalarValue,
    ) -> Result<()> {
        let array = actual.to_array()?;
        let list_array = array.as_list::<i32>();
        let inner_array = list_array.value(0);
        let mut actual_scalars = vec![];
        for index in 0..inner_array.len() {
            let sv = ScalarValue::try_from_array(&inner_array, index)?;
            actual_scalars.push(sv);
        }

        if actual_scalars.len() != expected.len() {
            return internal_err!(
                "Expected and actual list lengths differ: expected={}, actual={}",
                expected.len(),
                actual_scalars.len()
            );
        }

        let mut seen = vec![false; expected.len()];
        for v in expected {
            let mut found = false;
            for (i, sv) in actual_scalars.iter().enumerate() {
                if sv == &v {
                    seen[i] = true;
                    found = true;
                    break;
                }
            }
            if !found {
                return internal_err!(
                    "Expected value {:?} not found in actual values {:?}",
                    v,
                    actual_scalars
                );
            }
        }

        Ok(())
    }

    fn check_distinct_array_agg(
        input: ArrayRef,
        expected: Vec<ScalarValue>,
        datatype: DataType,
    ) -> Result<()> {
        let schema = Schema::new(vec![Field::new("a", datatype.clone(), false)]);
        let batch = RecordBatch::try_new(Arc::new(schema.clone()), vec![input])?;

        let agg = create_aggregate_expr(
            &array_agg_udaf(),
            &vec![col("a", &schema)?],
            &[],
            &[],
            &[],
            &schema,
            "array_agg_distinct",
            false,
            true,
        )?;

        let actual = {
            let mut accum = agg.create_accumulator()?;
            let expr = agg.expressions();
            let values = expr
                .iter()
                .map(|e| {
                    e.evaluate(&batch)
                        .and_then(|v| v.into_array(batch.num_rows()))
                })
                .collect::<Result<Vec<_>>>()?;
            accum.update_batch(&values)?;
            accum.evaluate()?
        };
        compare_list_contents(expected, actual)
    }

    fn check_merge_distinct_array_agg(
        input1: ArrayRef,
        input2: ArrayRef,
        expected: Vec<ScalarValue>,
        datatype: DataType,
    ) -> Result<()> {
        let schema = Schema::new(vec![Field::new("a", datatype.clone(), false)]);

        let agg = create_aggregate_expr(
            &array_agg_udaf(),
            &vec![col("a", &schema)?],
            &[],
            &[],
            &[],
            &schema,
            "array_agg_distinct",
            false,
            true,
        )?;

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

        let expected = vec![
            ScalarValue::Int32(Some(1)),
            ScalarValue::Int32(Some(2)),
            ScalarValue::Int32(Some(4)),
            ScalarValue::Int32(Some(5)),
            ScalarValue::Int32(Some(7)),
        ];

        check_distinct_array_agg(col, expected, DataType::Int32)
    }

    #[test]
    fn merge_distinct_array_agg_i32() -> Result<()> {
        let col1: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 7, 4, 5, 2]));
        let col2: ArrayRef = Arc::new(Int32Array::from(vec![1, 3, 7, 8, 4]));

        let expected = vec![
            ScalarValue::Int32(Some(1)),
            ScalarValue::Int32(Some(2)),
            ScalarValue::Int32(Some(3)),
            ScalarValue::Int32(Some(4)),
            ScalarValue::Int32(Some(5)),
            ScalarValue::Int32(Some(7)),
            ScalarValue::Int32(Some(8)),
        ];

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

        // Duplicate l1 and l3 in the input array and check that it is deduped in the output.
        let array = ScalarValue::iter_to_array(vec![
            l1.clone(),
            l2.clone(),
            l3.clone(),
            l3.clone(),
            l1.clone(),
        ])
        .unwrap();
        let expected = vec![l1, l2, l3];

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
        let input1 = ScalarValue::iter_to_array(vec![l1.clone(), l2.clone()]).unwrap();
        let input2 = ScalarValue::iter_to_array(vec![l1.clone(), l3.clone()]).unwrap();

        let expected = vec![l1, l2, l3];

        check_merge_distinct_array_agg(input1, input2, expected, DataType::Int32)
    }
}
