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

//! IS NULL expression

use std::hash::{Hash, Hasher};
use std::{any::Any, sync::Arc};

use arrow::compute;
use arrow::{
    datatypes::{DataType, Schema},
    record_batch::RecordBatch,
};
use arrow_array::{Array, ArrayRef, BooleanArray, Int8Array, UnionArray};
use arrow_buffer::{BooleanBuffer, ScalarBuffer};
use arrow_ord::cmp;

use crate::physical_expr::down_cast_any_ref;
use crate::PhysicalExpr;
use datafusion_common::Result;
use datafusion_common::ScalarValue;
use datafusion_expr::ColumnarValue;

/// IS NULL expression
#[derive(Debug, Hash)]
pub struct IsNullExpr {
    /// Input expression
    arg: Arc<dyn PhysicalExpr>,
}

impl IsNullExpr {
    /// Create new not expression
    pub fn new(arg: Arc<dyn PhysicalExpr>) -> Self {
        Self { arg }
    }

    /// Get the input expression
    pub fn arg(&self) -> &Arc<dyn PhysicalExpr> {
        &self.arg
    }
}

impl std::fmt::Display for IsNullExpr {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{} IS NULL", self.arg)
    }
}

impl PhysicalExpr for IsNullExpr {
    /// Return a reference to Any that can be used for downcasting
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self, _input_schema: &Schema) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn nullable(&self, _input_schema: &Schema) -> Result<bool> {
        Ok(false)
    }

    fn evaluate(&self, batch: &RecordBatch) -> Result<ColumnarValue> {
        let arg = self.arg.evaluate(batch)?;
        match arg {
            ColumnarValue::Array(array) => {
                Ok(ColumnarValue::Array(Arc::new(compute_is_null(array)?)))
            }
            ColumnarValue::Scalar(scalar) => Ok(ColumnarValue::Scalar(
                ScalarValue::Boolean(Some(scalar.is_null())),
            )),
        }
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![&self.arg]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> Result<Arc<dyn PhysicalExpr>> {
        Ok(Arc::new(IsNullExpr::new(Arc::clone(&children[0]))))
    }

    fn dyn_hash(&self, state: &mut dyn Hasher) {
        let mut s = state;
        self.hash(&mut s);
    }
}

/// workaround <https://github.com/apache/arrow-rs/issues/6017>,
/// this can be replaced with a direct call to `arrow::compute::is_null` once it's fixed.
pub(crate) fn compute_is_null(array: ArrayRef) -> Result<BooleanArray> {
    if let Some(union_array) = array.as_any().downcast_ref::<UnionArray>() {
        if let Some(offsets) = union_array.offsets() {
            dense_union_is_null(union_array, offsets)
        } else {
            sparse_union_is_null(union_array)
        }
    } else {
        compute::is_null(array.as_ref()).map_err(Into::into)
    }
}

/// workaround <https://github.com/apache/arrow-rs/issues/6017>,
/// this can be replaced with a direct call to `arrow::compute::is_not_null` once it's fixed.
pub(crate) fn compute_is_not_null(array: ArrayRef) -> Result<BooleanArray> {
    if array.as_any().is::<UnionArray>() {
        compute::not(&compute_is_null(array)?).map_err(Into::into)
    } else {
        compute::is_not_null(array.as_ref()).map_err(Into::into)
    }
}

fn dense_union_is_null(
    union_array: &UnionArray,
    offsets: &ScalarBuffer<i32>,
) -> Result<BooleanArray> {
    let child_arrays = (0..union_array.type_names().len())
        .map(|type_id| {
            compute::is_null(&union_array.child(type_id as i8)).map_err(Into::into)
        })
        .collect::<Result<Vec<BooleanArray>>>()?;

    let buffer: BooleanBuffer = offsets
        .iter()
        .zip(union_array.type_ids())
        .map(|(offset, type_id)| child_arrays[*type_id as usize].value(*offset as usize))
        .collect();

    Ok(BooleanArray::new(buffer, None))
}

fn sparse_union_is_null(union_array: &UnionArray) -> Result<BooleanArray> {
    let type_ids = Int8Array::new(union_array.type_ids().clone(), None);

    let mut union_is_null =
        BooleanArray::new(BooleanBuffer::new_unset(union_array.len()), None);
    for type_id in 0..union_array.type_names().len() {
        let type_id = type_id as i8;
        let union_is_child = cmp::eq(&type_ids, &Int8Array::new_scalar(type_id))?;
        let child = union_array.child(type_id);
        let child_array_is_null = compute::is_null(&child)?;
        let child_is_null = compute::and(&union_is_child, &child_array_is_null)?;
        union_is_null = compute::or(&union_is_null, &child_is_null)?;
    }
    Ok(union_is_null)
}

impl PartialEq<dyn Any> for IsNullExpr {
    fn eq(&self, other: &dyn Any) -> bool {
        down_cast_any_ref(other)
            .downcast_ref::<Self>()
            .map(|x| self.arg.eq(&x.arg))
            .unwrap_or(false)
    }
}

/// Create an IS NULL expression
pub fn is_null(arg: Arc<dyn PhysicalExpr>) -> Result<Arc<dyn PhysicalExpr>> {
    Ok(Arc::new(IsNullExpr::new(arg)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::expressions::col;
    use arrow::{
        array::{BooleanArray, StringArray},
        datatypes::*,
    };
    use arrow_array::{Float64Array, Int32Array};
    use arrow_buffer::ScalarBuffer;
    use datafusion_common::cast::as_boolean_array;

    #[test]
    fn is_null_op() -> Result<()> {
        let schema = Schema::new(vec![Field::new("a", DataType::Utf8, true)]);
        let a = StringArray::from(vec![Some("foo"), None]);

        // expression: "a is null"
        let expr = is_null(col("a", &schema)?).unwrap();
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(a)])?;

        let result = expr
            .evaluate(&batch)?
            .into_array(batch.num_rows())
            .expect("Failed to convert to array");
        let result =
            as_boolean_array(&result).expect("failed to downcast to BooleanArray");

        let expected = &BooleanArray::from(vec![false, true]);

        assert_eq!(expected, result);

        Ok(())
    }

    fn union_fields() -> UnionFields {
        [
            (0, Arc::new(Field::new("A", DataType::Int32, true))),
            (1, Arc::new(Field::new("B", DataType::Float64, true))),
            (2, Arc::new(Field::new("C", DataType::Utf8, true))),
        ]
        .into_iter()
        .collect()
    }

    #[test]
    fn sparse_union_is_null() {
        // union of [{A=1}, {A=}, {B=1.1}, {B=1.2}, {B=}, {C=}, {C="a"}]
        let int_array =
            Int32Array::from(vec![Some(1), None, None, None, None, None, None]);
        let float_array =
            Float64Array::from(vec![None, None, Some(1.1), Some(1.2), None, None, None]);
        let str_array =
            StringArray::from(vec![None, None, None, None, None, None, Some("a")]);
        let type_ids = [0, 0, 1, 1, 1, 2, 2]
            .into_iter()
            .collect::<ScalarBuffer<i8>>();

        let children = vec![
            Arc::new(int_array) as Arc<dyn Array>,
            Arc::new(float_array),
            Arc::new(str_array),
        ];

        let array =
            UnionArray::try_new(union_fields(), type_ids, None, children).unwrap();

        let array_ref = Arc::new(array) as ArrayRef;
        let result = compute_is_null(array_ref).unwrap();

        let expected =
            &BooleanArray::from(vec![false, true, false, false, true, true, false]);
        assert_eq!(expected, &result);
    }

    #[test]
    fn dense_union_is_null() {
        // union of [{A=1}, {A=}, {B=3.2}, {B=}, {C="a"}, {C=}]
        let int_array = Int32Array::from(vec![Some(1), None]);
        let float_array = Float64Array::from(vec![Some(3.2), None]);
        let str_array = StringArray::from(vec![Some("a"), None]);
        let type_ids = [0, 0, 1, 1, 2, 2].into_iter().collect::<ScalarBuffer<i8>>();
        let offsets = [0, 1, 0, 1, 0, 1]
            .into_iter()
            .collect::<ScalarBuffer<i32>>();

        let children = vec![
            Arc::new(int_array) as Arc<dyn Array>,
            Arc::new(float_array),
            Arc::new(str_array),
        ];

        let array =
            UnionArray::try_new(union_fields(), type_ids, Some(offsets), children)
                .unwrap();

        let array_ref = Arc::new(array) as ArrayRef;
        let result = compute_is_null(array_ref).unwrap();

        let expected = &BooleanArray::from(vec![false, true, false, true, false, true]);
        assert_eq!(expected, &result);
    }
}
