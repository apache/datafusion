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

use crate::aggregate::utils::down_cast_any_ref;
use crate::expressions::format_state_name;
use crate::{AggregateExpr, PhysicalExpr};
use arrow::array::ArrayRef;
use arrow::datatypes::{DataType, Field};
use arrow_array::Array;
use datafusion_common::cast::as_list_array;
use datafusion_common::utils::array_into_list_array;
use datafusion_common::Result;
use datafusion_common::ScalarValue;
use datafusion_expr::Accumulator;
use std::any::Any;
use std::sync::Arc;

/// ARRAY_AGG aggregate expression
#[derive(Debug)]
pub struct ArrayAgg {
    name: String,
    input_data_type: DataType,
    expr: Arc<dyn PhysicalExpr>,
    is_expr_nullable: bool,
}

impl ArrayAgg {
    /// Create a new ArrayAgg aggregate function
    pub fn new(
        expr: Arc<dyn PhysicalExpr>,
        name: impl Into<String>,
        data_type: DataType,
        is_expr_nullable: bool,
    ) -> Self {
        Self {
            name: name.into(),
            expr,
            input_data_type: data_type,
            is_expr_nullable,
        }
    }
}

impl AggregateExpr for ArrayAgg {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn field(&self) -> Result<Field> {
        Ok(Field::new_list(
            &self.name,
            Field::new("item", self.input_data_type.clone(), self.is_expr_nullable),
            false,
        ))
    }

    fn create_accumulator(&self) -> Result<Box<dyn Accumulator>> {
        Ok(Box::new(ArrayAggAccumulator::try_new(
            &self.input_data_type,
        )?))
    }

    fn state_fields(&self) -> Result<Vec<Field>> {
        Ok(vec![Field::new_list(
            format_state_name(&self.name, "array_agg"),
            Field::new("item", self.input_data_type.clone(), self.is_expr_nullable),
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

impl PartialEq<dyn Any> for ArrayAgg {
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
pub(crate) struct ArrayAggAccumulator {
    values: Vec<ArrayRef>,
    datatype: DataType,
}

impl ArrayAggAccumulator {
    /// new array_agg accumulator based on given item data type
    pub fn try_new(datatype: &DataType) -> Result<Self> {
        Ok(Self {
            values: vec![],
            datatype: datatype.clone(),
        })
    }
}

impl Accumulator for ArrayAggAccumulator {
    // Append value like Int64Array(1,2,3)
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if values.is_empty() {
            return Ok(());
        }
        assert!(values.len() == 1, "array_agg can only take 1 param!");
        let val = values[0].clone();
        self.values.push(val);
        Ok(())
    }

    // Append value like ListArray(Int64Array(1,2,3), Int64Array(4,5,6))
    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        if states.is_empty() {
            return Ok(());
        }
        assert!(states.len() == 1, "array_agg states must be singleton!");

        let list_arr = as_list_array(&states[0])?;
        for arr in list_arr.iter().flatten() {
            self.values.push(arr);
        }
        Ok(())
    }

    fn state(&self) -> Result<Vec<ScalarValue>> {
        Ok(vec![self.evaluate()?])
    }

    fn evaluate(&self) -> Result<ScalarValue> {
        // Transform Vec<ListArr> to ListArr

        let element_arrays: Vec<&dyn Array> =
            self.values.iter().map(|a| a.as_ref()).collect();

        if element_arrays.is_empty() {
            let arr = ScalarValue::new_list(&[], &self.datatype);
            return Ok(ScalarValue::List(arr));
        }

        let concated_array = arrow::compute::concat(&element_arrays)?;
        let list_array = array_into_list_array(concated_array);

        Ok(ScalarValue::List(Arc::new(list_array)))
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(self)
            + (std::mem::size_of::<ArrayRef>() * self.values.capacity())
            + self
                .values
                .iter()
                .map(|arr| arr.get_array_memory_size())
                .sum::<usize>()
            + self.datatype.size()
            - std::mem::size_of_val(&self.datatype)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expressions::col;
    use crate::expressions::tests::aggregate;
    use arrow::array::ArrayRef;
    use arrow::array::Int32Array;
    use arrow::datatypes::*;
    use arrow::record_batch::RecordBatch;
    use arrow_array::Array;
    use arrow_array::ListArray;
    use arrow_buffer::OffsetBuffer;
    use datafusion_common::DataFusionError;
    use datafusion_common::Result;

    macro_rules! test_op {
        ($ARRAY:expr, $DATATYPE:expr, $OP:ident, $EXPECTED:expr) => {
            test_op!($ARRAY, $DATATYPE, $OP, $EXPECTED, $EXPECTED.data_type())
        };
        ($ARRAY:expr, $DATATYPE:expr, $OP:ident, $EXPECTED:expr, $EXPECTED_DATATYPE:expr) => {{
            let schema = Schema::new(vec![Field::new("a", $DATATYPE, true)]);

            let batch = RecordBatch::try_new(Arc::new(schema.clone()), vec![$ARRAY])?;

            let agg = Arc::new(<$OP>::new(
                col("a", &schema)?,
                "bla".to_string(),
                $EXPECTED_DATATYPE,
                true,
            ));
            let actual = aggregate(&batch, agg)?;
            let expected = ScalarValue::from($EXPECTED);

            assert_eq!(expected, actual);

            Ok(()) as Result<(), DataFusionError>
        }};
    }

    #[test]
    fn array_agg_i32() -> Result<()> {
        let a: ArrayRef = Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5]));

        let list = ListArray::from_iter_primitive::<Int32Type, _, _>(vec![Some(vec![
            Some(1),
            Some(2),
            Some(3),
            Some(4),
            Some(5),
        ])]);
        let list = ScalarValue::List(Arc::new(list));

        test_op!(a, DataType::Int32, ArrayAgg, list, DataType::Int32)
    }

    #[test]
    fn array_agg_nested() -> Result<()> {
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
            OffsetBuffer::from_lengths([a1.len() + a2.len()]),
            arrow::compute::concat(&[&a1, &a2])?,
            None,
        );

        let a1 =
            ListArray::from_iter_primitive::<Int32Type, _, _>(vec![Some(vec![Some(6)])]);
        let a2 = ListArray::from_iter_primitive::<Int32Type, _, _>(vec![Some(vec![
            Some(7),
            Some(8),
        ])]);
        let l2 = ListArray::new(
            Arc::new(Field::new("item", a1.data_type().to_owned(), true)),
            OffsetBuffer::from_lengths([a1.len() + a2.len()]),
            arrow::compute::concat(&[&a1, &a2])?,
            None,
        );

        let a1 =
            ListArray::from_iter_primitive::<Int32Type, _, _>(vec![Some(vec![Some(9)])]);
        let l3 = ListArray::new(
            Arc::new(Field::new("item", a1.data_type().to_owned(), true)),
            OffsetBuffer::from_lengths([a1.len()]),
            arrow::compute::concat(&[&a1])?,
            None,
        );

        let list = ListArray::new(
            Arc::new(Field::new("item", l1.data_type().to_owned(), true)),
            OffsetBuffer::from_lengths([l1.len() + l2.len() + l3.len()]),
            arrow::compute::concat(&[&l1, &l2, &l3])?,
            None,
        );
        let list = ScalarValue::List(Arc::new(list));
        let l1 = ScalarValue::List(Arc::new(l1));
        let l2 = ScalarValue::List(Arc::new(l2));
        let l3 = ScalarValue::List(Arc::new(l3));

        let array = ScalarValue::iter_to_array(vec![l1, l2, l3]).unwrap();

        test_op!(
            array,
            DataType::List(Arc::new(Field::new_list(
                "item",
                Field::new("item", DataType::Int32, true),
                true,
            ))),
            ArrayAgg,
            list,
            DataType::List(Arc::new(Field::new("item", DataType::Int32, true,)))
        )
    }
}
