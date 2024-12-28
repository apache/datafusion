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

use arrow::{
    array::{as_primitive_array, Capacities, MutableArrayData},
    buffer::{NullBuffer, OffsetBuffer},
    datatypes::ArrowNativeType,
    record_batch::RecordBatch,
};
use arrow_array::{
    make_array, Array, ArrayRef, GenericListArray, Int32Array, OffsetSizeTrait, StructArray,
};
use arrow_schema::{DataType, Field, FieldRef, Schema};
use datafusion::logical_expr::ColumnarValue;
use datafusion_common::{
    cast::{as_int32_array, as_large_list_array, as_list_array},
    internal_err, DataFusionError, Result as DataFusionResult, ScalarValue,
};
use datafusion_physical_expr::PhysicalExpr;
use std::hash::Hash;
use std::{
    any::Any,
    fmt::{Debug, Display, Formatter},
    sync::Arc,
};

// 2147483632 == java.lang.Integer.MAX_VALUE - 15
// It is a value of ByteArrayUtils.MAX_ROUNDED_ARRAY_LENGTH
// https://github.com/apache/spark/blob/master/common/utils/src/main/java/org/apache/spark/unsafe/array/ByteArrayUtils.java
const MAX_ROUNDED_ARRAY_LENGTH: usize = 2147483632;

#[derive(Debug, Eq)]
pub struct ListExtract {
    child: Arc<dyn PhysicalExpr>,
    ordinal: Arc<dyn PhysicalExpr>,
    default_value: Option<Arc<dyn PhysicalExpr>>,
    one_based: bool,
    fail_on_error: bool,
}

impl Hash for ListExtract {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.child.hash(state);
        self.ordinal.hash(state);
        self.default_value.hash(state);
        self.one_based.hash(state);
        self.fail_on_error.hash(state);
    }
}
impl PartialEq for ListExtract {
    fn eq(&self, other: &Self) -> bool {
        self.child.eq(&other.child)
            && self.ordinal.eq(&other.ordinal)
            && self.default_value.eq(&other.default_value)
            && self.one_based.eq(&other.one_based)
            && self.fail_on_error.eq(&other.fail_on_error)
    }
}

impl ListExtract {
    pub fn new(
        child: Arc<dyn PhysicalExpr>,
        ordinal: Arc<dyn PhysicalExpr>,
        default_value: Option<Arc<dyn PhysicalExpr>>,
        one_based: bool,
        fail_on_error: bool,
    ) -> Self {
        Self {
            child,
            ordinal,
            default_value,
            one_based,
            fail_on_error,
        }
    }

    fn child_field(&self, input_schema: &Schema) -> DataFusionResult<FieldRef> {
        match self.child.data_type(input_schema)? {
            DataType::List(field) | DataType::LargeList(field) => Ok(field),
            data_type => Err(DataFusionError::Internal(format!(
                "Unexpected data type in ListExtract: {:?}",
                data_type
            ))),
        }
    }
}

impl PhysicalExpr for ListExtract {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self, input_schema: &Schema) -> DataFusionResult<DataType> {
        Ok(self.child_field(input_schema)?.data_type().clone())
    }

    fn nullable(&self, input_schema: &Schema) -> DataFusionResult<bool> {
        // Only non-nullable if fail_on_error is enabled and the element is non-nullable
        Ok(!self.fail_on_error || self.child_field(input_schema)?.is_nullable())
    }

    fn evaluate(&self, batch: &RecordBatch) -> DataFusionResult<ColumnarValue> {
        let child_value = self.child.evaluate(batch)?.into_array(batch.num_rows())?;
        let ordinal_value = self.ordinal.evaluate(batch)?.into_array(batch.num_rows())?;

        let default_value = self
            .default_value
            .as_ref()
            .map(|d| {
                d.evaluate(batch).map(|value| match value {
                    ColumnarValue::Scalar(scalar)
                        if !scalar.data_type().equals_datatype(child_value.data_type()) =>
                    {
                        scalar.cast_to(child_value.data_type())
                    }
                    ColumnarValue::Scalar(scalar) => Ok(scalar),
                    v => Err(DataFusionError::Execution(format!(
                        "Expected scalar default value for ListExtract, got {:?}",
                        v
                    ))),
                })
            })
            .transpose()?
            .unwrap_or(self.data_type(&batch.schema())?.try_into())?;

        let adjust_index = if self.one_based {
            one_based_index
        } else {
            zero_based_index
        };

        match child_value.data_type() {
            DataType::List(_) => {
                let list_array = as_list_array(&child_value)?;
                let index_array = as_int32_array(&ordinal_value)?;

                list_extract(
                    list_array,
                    index_array,
                    &default_value,
                    self.fail_on_error,
                    adjust_index,
                )
            }
            DataType::LargeList(_) => {
                let list_array = as_large_list_array(&child_value)?;
                let index_array = as_int32_array(&ordinal_value)?;

                list_extract(
                    list_array,
                    index_array,
                    &default_value,
                    self.fail_on_error,
                    adjust_index,
                )
            }
            data_type => Err(DataFusionError::Internal(format!(
                "Unexpected child type for ListExtract: {:?}",
                data_type
            ))),
        }
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![&self.child, &self.ordinal]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> datafusion_common::Result<Arc<dyn PhysicalExpr>> {
        match children.len() {
            2 => Ok(Arc::new(ListExtract::new(
                Arc::clone(&children[0]),
                Arc::clone(&children[1]),
                self.default_value.clone(),
                self.one_based,
                self.fail_on_error,
            ))),
            _ => internal_err!("ListExtract should have exactly two children"),
        }
    }
}

fn one_based_index(index: i32, len: usize) -> DataFusionResult<Option<usize>> {
    if index == 0 {
        return Err(DataFusionError::Execution(
            "Invalid index of 0 for one-based ListExtract".to_string(),
        ));
    }

    let abs_index = index.abs().as_usize();
    if abs_index <= len {
        if index > 0 {
            Ok(Some(abs_index - 1))
        } else {
            Ok(Some(len - abs_index))
        }
    } else {
        Ok(None)
    }
}

fn zero_based_index(index: i32, len: usize) -> DataFusionResult<Option<usize>> {
    if index < 0 {
        Ok(None)
    } else {
        let positive_index = index.as_usize();
        if positive_index < len {
            Ok(Some(positive_index))
        } else {
            Ok(None)
        }
    }
}

fn list_extract<O: OffsetSizeTrait>(
    list_array: &GenericListArray<O>,
    index_array: &Int32Array,
    default_value: &ScalarValue,
    fail_on_error: bool,
    adjust_index: impl Fn(i32, usize) -> DataFusionResult<Option<usize>>,
) -> DataFusionResult<ColumnarValue> {
    let values = list_array.values();
    let offsets = list_array.offsets();

    let data = values.to_data();

    let default_data = default_value.to_array()?.to_data();

    let mut mutable = MutableArrayData::new(vec![&data, &default_data], true, index_array.len());

    for (row, (offset_window, index)) in offsets.windows(2).zip(index_array.values()).enumerate() {
        let start = offset_window[0].as_usize();
        let len = offset_window[1].as_usize() - start;

        if let Some(i) = adjust_index(*index, len)? {
            mutable.extend(0, start + i, start + i + 1);
        } else if list_array.is_null(row) {
            mutable.extend_nulls(1);
        } else if fail_on_error {
            return Err(DataFusionError::Execution(
                "Index out of bounds for array".to_string(),
            ));
        } else {
            mutable.extend(1, 0, 1);
        }
    }

    let data = mutable.freeze();
    Ok(ColumnarValue::Array(arrow::array::make_array(data)))
}

impl Display for ListExtract {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ListExtract [child: {:?}, ordinal: {:?}, default_value: {:?}, one_based: {:?}, fail_on_error: {:?}]",
            self.child, self.ordinal,  self.default_value, self.one_based, self.fail_on_error
        )
    }
}

#[derive(Debug, Eq)]
pub struct GetArrayStructFields {
    child: Arc<dyn PhysicalExpr>,
    ordinal: usize,
}

impl Hash for GetArrayStructFields {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.child.hash(state);
        self.ordinal.hash(state);
    }
}
impl PartialEq for GetArrayStructFields {
    fn eq(&self, other: &Self) -> bool {
        self.child.eq(&other.child) && self.ordinal.eq(&other.ordinal)
    }
}

impl GetArrayStructFields {
    pub fn new(child: Arc<dyn PhysicalExpr>, ordinal: usize) -> Self {
        Self { child, ordinal }
    }

    fn list_field(&self, input_schema: &Schema) -> DataFusionResult<FieldRef> {
        match self.child.data_type(input_schema)? {
            DataType::List(field) | DataType::LargeList(field) => Ok(field),
            data_type => Err(DataFusionError::Internal(format!(
                "Unexpected data type in GetArrayStructFields: {:?}",
                data_type
            ))),
        }
    }

    fn child_field(&self, input_schema: &Schema) -> DataFusionResult<FieldRef> {
        match self.list_field(input_schema)?.data_type() {
            DataType::Struct(fields) => Ok(Arc::clone(&fields[self.ordinal])),
            data_type => Err(DataFusionError::Internal(format!(
                "Unexpected data type in GetArrayStructFields: {:?}",
                data_type
            ))),
        }
    }
}

impl PhysicalExpr for GetArrayStructFields {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self, input_schema: &Schema) -> DataFusionResult<DataType> {
        let struct_field = self.child_field(input_schema)?;
        match self.child.data_type(input_schema)? {
            DataType::List(_) => Ok(DataType::List(struct_field)),
            DataType::LargeList(_) => Ok(DataType::LargeList(struct_field)),
            data_type => Err(DataFusionError::Internal(format!(
                "Unexpected data type in GetArrayStructFields: {:?}",
                data_type
            ))),
        }
    }

    fn nullable(&self, input_schema: &Schema) -> DataFusionResult<bool> {
        Ok(self.list_field(input_schema)?.is_nullable()
            || self.child_field(input_schema)?.is_nullable())
    }

    fn evaluate(&self, batch: &RecordBatch) -> DataFusionResult<ColumnarValue> {
        let child_value = self.child.evaluate(batch)?.into_array(batch.num_rows())?;

        match child_value.data_type() {
            DataType::List(_) => {
                let list_array = as_list_array(&child_value)?;

                get_array_struct_fields(list_array, self.ordinal)
            }
            DataType::LargeList(_) => {
                let list_array = as_large_list_array(&child_value)?;

                get_array_struct_fields(list_array, self.ordinal)
            }
            data_type => Err(DataFusionError::Internal(format!(
                "Unexpected child type for ListExtract: {:?}",
                data_type
            ))),
        }
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![&self.child]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> datafusion_common::Result<Arc<dyn PhysicalExpr>> {
        match children.len() {
            1 => Ok(Arc::new(GetArrayStructFields::new(
                Arc::clone(&children[0]),
                self.ordinal,
            ))),
            _ => internal_err!("GetArrayStructFields should have exactly one child"),
        }
    }
}

fn get_array_struct_fields<O: OffsetSizeTrait>(
    list_array: &GenericListArray<O>,
    ordinal: usize,
) -> DataFusionResult<ColumnarValue> {
    let values = list_array
        .values()
        .as_any()
        .downcast_ref::<StructArray>()
        .expect("A struct is expected");

    let column = Arc::clone(values.column(ordinal));
    let field = Arc::clone(&values.fields()[ordinal]);

    let offsets = list_array.offsets();
    let array = GenericListArray::new(field, offsets.clone(), column, list_array.nulls().cloned());

    Ok(ColumnarValue::Array(Arc::new(array)))
}

impl Display for GetArrayStructFields {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "GetArrayStructFields [child: {:?}, ordinal: {:?}]",
            self.child, self.ordinal
        )
    }
}

#[derive(Debug, Eq)]
pub struct ArrayInsert {
    src_array_expr: Arc<dyn PhysicalExpr>,
    pos_expr: Arc<dyn PhysicalExpr>,
    item_expr: Arc<dyn PhysicalExpr>,
    legacy_negative_index: bool,
}

impl Hash for ArrayInsert {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.src_array_expr.hash(state);
        self.pos_expr.hash(state);
        self.item_expr.hash(state);
        self.legacy_negative_index.hash(state);
    }
}
impl PartialEq for ArrayInsert {
    fn eq(&self, other: &Self) -> bool {
        self.src_array_expr.eq(&other.src_array_expr)
            && self.pos_expr.eq(&other.pos_expr)
            && self.item_expr.eq(&other.item_expr)
            && self.legacy_negative_index.eq(&other.legacy_negative_index)
    }
}

impl ArrayInsert {
    pub fn new(
        src_array_expr: Arc<dyn PhysicalExpr>,
        pos_expr: Arc<dyn PhysicalExpr>,
        item_expr: Arc<dyn PhysicalExpr>,
        legacy_negative_index: bool,
    ) -> Self {
        Self {
            src_array_expr,
            pos_expr,
            item_expr,
            legacy_negative_index,
        }
    }

    pub fn array_type(&self, data_type: &DataType) -> DataFusionResult<DataType> {
        match data_type {
            DataType::List(field) => Ok(DataType::List(Arc::clone(field))),
            DataType::LargeList(field) => Ok(DataType::LargeList(Arc::clone(field))),
            data_type => Err(DataFusionError::Internal(format!(
                "Unexpected src array type in ArrayInsert: {:?}",
                data_type
            ))),
        }
    }
}

impl PhysicalExpr for ArrayInsert {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn data_type(&self, input_schema: &Schema) -> DataFusionResult<DataType> {
        self.array_type(&self.src_array_expr.data_type(input_schema)?)
    }

    fn nullable(&self, input_schema: &Schema) -> DataFusionResult<bool> {
        self.src_array_expr.nullable(input_schema)
    }

    fn evaluate(&self, batch: &RecordBatch) -> DataFusionResult<ColumnarValue> {
        let pos_value = self
            .pos_expr
            .evaluate(batch)?
            .into_array(batch.num_rows())?;

        // Spark supports only IntegerType (Int32):
        // https://github.com/apache/spark/blob/branch-3.5/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/collectionOperations.scala#L4737
        if !matches!(pos_value.data_type(), DataType::Int32) {
            return Err(DataFusionError::Internal(format!(
                "Unexpected index data type in ArrayInsert: {:?}, expected type is Int32",
                pos_value.data_type()
            )));
        }

        // Check that src array is actually an array and get it's value type
        let src_value = self
            .src_array_expr
            .evaluate(batch)?
            .into_array(batch.num_rows())?;

        let src_element_type = match self.array_type(src_value.data_type())? {
            DataType::List(field) => &field.data_type().clone(),
            DataType::LargeList(field) => &field.data_type().clone(),
            _ => unreachable!(),
        };

        // Check that inserted value has the same type as an array
        let item_value = self
            .item_expr
            .evaluate(batch)?
            .into_array(batch.num_rows())?;
        if item_value.data_type() != src_element_type {
            return Err(DataFusionError::Internal(format!(
                "Type mismatch in ArrayInsert: array type is {:?} but item type is {:?}",
                src_element_type,
                item_value.data_type()
            )));
        }

        match src_value.data_type() {
            DataType::List(_) => {
                let list_array = as_list_array(&src_value)?;
                array_insert(
                    list_array,
                    &item_value,
                    &pos_value,
                    self.legacy_negative_index,
                )
            }
            DataType::LargeList(_) => {
                let list_array = as_large_list_array(&src_value)?;
                array_insert(
                    list_array,
                    &item_value,
                    &pos_value,
                    self.legacy_negative_index,
                )
            }
            _ => unreachable!(), // This case is checked already
        }
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![&self.src_array_expr, &self.pos_expr, &self.item_expr]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> DataFusionResult<Arc<dyn PhysicalExpr>> {
        match children.len() {
            3 => Ok(Arc::new(ArrayInsert::new(
                Arc::clone(&children[0]),
                Arc::clone(&children[1]),
                Arc::clone(&children[2]),
                self.legacy_negative_index,
            ))),
            _ => internal_err!("ArrayInsert should have exactly three childrens"),
        }
    }
}

fn array_insert<O: OffsetSizeTrait>(
    list_array: &GenericListArray<O>,
    items_array: &ArrayRef,
    pos_array: &ArrayRef,
    legacy_mode: bool,
) -> DataFusionResult<ColumnarValue> {
    // The code is based on the implementation of the array_append from the Apache DataFusion
    // https://github.com/apache/datafusion/blob/main/datafusion/functions-nested/src/concat.rs#L513
    //
    // This code is also based on the implementation of the array_insert from the Apache Spark
    // https://github.com/apache/spark/blob/branch-3.5/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/collectionOperations.scala#L4713

    let values = list_array.values();
    let offsets = list_array.offsets();
    let values_data = values.to_data();
    let item_data = items_array.to_data();
    let new_capacity = Capacities::Array(values_data.len() + item_data.len());

    let mut mutable_values =
        MutableArrayData::with_capacities(vec![&values_data, &item_data], true, new_capacity);

    let mut new_offsets = vec![O::usize_as(0)];
    let mut new_nulls = Vec::<bool>::with_capacity(list_array.len());

    let pos_data: &Int32Array = as_primitive_array(&pos_array); // Spark supports only i32 for positions

    for (row_index, offset_window) in offsets.windows(2).enumerate() {
        let pos = pos_data.values()[row_index];
        let start = offset_window[0].as_usize();
        let end = offset_window[1].as_usize();
        let is_item_null = items_array.is_null(row_index);

        if list_array.is_null(row_index) {
            // In Spark if value of the array is NULL than nothing happens
            mutable_values.extend_nulls(1);
            new_offsets.push(new_offsets[row_index] + O::one());
            new_nulls.push(false);
            continue;
        }

        if pos == 0 {
            return Err(DataFusionError::Internal(
                "Position for array_insert should be greter or less than zero".to_string(),
            ));
        }

        if (pos > 0) || ((-pos).as_usize() < (end - start + 1)) {
            let corrected_pos = if pos > 0 {
                (pos - 1).as_usize()
            } else {
                end - start - (-pos).as_usize() + if legacy_mode { 0 } else { 1 }
            };
            let new_array_len = std::cmp::max(end - start + 1, corrected_pos);
            if new_array_len > MAX_ROUNDED_ARRAY_LENGTH {
                return Err(DataFusionError::Internal(format!(
                    "Max array length in Spark is {:?}, but got {:?}",
                    MAX_ROUNDED_ARRAY_LENGTH, new_array_len
                )));
            }

            if (start + corrected_pos) <= end {
                mutable_values.extend(0, start, start + corrected_pos);
                mutable_values.extend(1, row_index, row_index + 1);
                mutable_values.extend(0, start + corrected_pos, end);
                new_offsets.push(new_offsets[row_index] + O::usize_as(new_array_len));
            } else {
                mutable_values.extend(0, start, end);
                mutable_values.extend_nulls(new_array_len - (end - start));
                mutable_values.extend(1, row_index, row_index + 1);
                // In that case spark actualy makes array longer than expected;
                // For example, if pos is equal to 5, len is eq to 3, than resulted len will be 5
                new_offsets.push(new_offsets[row_index] + O::usize_as(new_array_len) + O::one());
            }
        } else {
            // This comment is takes from the Apache Spark source code as is:
            // special case- if the new position is negative but larger than the current array size
            // place the new item at start of array, place the current array contents at the end
            // and fill the newly created array elements inbetween with a null
            let base_offset = if legacy_mode { 1 } else { 0 };
            let new_array_len = (-pos + base_offset).as_usize();
            if new_array_len > MAX_ROUNDED_ARRAY_LENGTH {
                return Err(DataFusionError::Internal(format!(
                    "Max array length in Spark is {:?}, but got {:?}",
                    MAX_ROUNDED_ARRAY_LENGTH, new_array_len
                )));
            }
            mutable_values.extend(1, row_index, row_index + 1);
            mutable_values.extend_nulls(new_array_len - (end - start + 1));
            mutable_values.extend(0, start, end);
            new_offsets.push(new_offsets[row_index] + O::usize_as(new_array_len));
        }
        if is_item_null {
            if (start == end) || (values.is_null(row_index)) {
                new_nulls.push(false)
            } else {
                new_nulls.push(true)
            }
        } else {
            new_nulls.push(true)
        }
    }

    let data = make_array(mutable_values.freeze());
    let data_type = match list_array.data_type() {
        DataType::List(field) => field.data_type(),
        DataType::LargeList(field) => field.data_type(),
        _ => unreachable!(),
    };
    let new_array = GenericListArray::<O>::try_new(
        Arc::new(Field::new("item", data_type.clone(), true)),
        OffsetBuffer::new(new_offsets.into()),
        data,
        Some(NullBuffer::new(new_nulls.into())),
    )?;

    Ok(ColumnarValue::Array(Arc::new(new_array)))
}

impl Display for ArrayInsert {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ArrayInsert [array: {:?}, pos: {:?}, item: {:?}]",
            self.src_array_expr, self.pos_expr, self.item_expr
        )
    }
}

#[cfg(test)]
mod test {
    use crate::list::{array_insert, list_extract, zero_based_index};

    use arrow::datatypes::Int32Type;
    use arrow_array::{Array, ArrayRef, Int32Array, ListArray};
    use datafusion_common::{Result, ScalarValue};
    use datafusion_expr::ColumnarValue;
    use std::sync::Arc;

    #[test]
    fn test_list_extract_default_value() -> Result<()> {
        let list = ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
            Some(vec![Some(1)]),
            None,
            Some(vec![]),
        ]);
        let indices = Int32Array::from(vec![0, 0, 0]);

        let null_default = ScalarValue::Int32(None);

        let ColumnarValue::Array(result) =
            list_extract(&list, &indices, &null_default, false, zero_based_index)?
        else {
            unreachable!()
        };

        assert_eq!(
            &result.to_data(),
            &Int32Array::from(vec![Some(1), None, None]).to_data()
        );

        let zero_default = ScalarValue::Int32(Some(0));

        let ColumnarValue::Array(result) =
            list_extract(&list, &indices, &zero_default, false, zero_based_index)?
        else {
            unreachable!()
        };

        assert_eq!(
            &result.to_data(),
            &Int32Array::from(vec![Some(1), None, Some(0)]).to_data()
        );
        Ok(())
    }

    #[test]
    fn test_array_insert() -> Result<()> {
        // Test inserting an item into a list array
        // Inputs and expected values are taken from the Spark results
        let list = ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
            Some(vec![Some(1), Some(2), Some(3)]),
            Some(vec![Some(4), Some(5)]),
            Some(vec![None]),
            Some(vec![Some(1), Some(2), Some(3)]),
            Some(vec![Some(1), Some(2), Some(3)]),
            None,
        ]);

        let positions = Int32Array::from(vec![2, 1, 1, 5, 6, 1]);
        let items = Int32Array::from(vec![
            Some(10),
            Some(20),
            Some(30),
            Some(100),
            Some(100),
            Some(40),
        ]);

        let ColumnarValue::Array(result) = array_insert(
            &list,
            &(Arc::new(items) as ArrayRef),
            &(Arc::new(positions) as ArrayRef),
            false,
        )?
        else {
            unreachable!()
        };

        let expected = ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
            Some(vec![Some(1), Some(10), Some(2), Some(3)]),
            Some(vec![Some(20), Some(4), Some(5)]),
            Some(vec![Some(30), None]),
            Some(vec![Some(1), Some(2), Some(3), None, Some(100)]),
            Some(vec![Some(1), Some(2), Some(3), None, None, Some(100)]),
            None,
        ]);

        assert_eq!(&result.to_data(), &expected.to_data());

        Ok(())
    }

    #[test]
    fn test_array_insert_negative_index() -> Result<()> {
        // Test insert with negative index
        // Inputs and expected values are taken from the Spark results
        let list = ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
            Some(vec![Some(1), Some(2), Some(3)]),
            Some(vec![Some(4), Some(5)]),
            Some(vec![Some(1)]),
            None,
        ]);

        let positions = Int32Array::from(vec![-2, -1, -3, -1]);
        let items = Int32Array::from(vec![Some(10), Some(20), Some(100), Some(30)]);

        let ColumnarValue::Array(result) = array_insert(
            &list,
            &(Arc::new(items) as ArrayRef),
            &(Arc::new(positions) as ArrayRef),
            false,
        )?
        else {
            unreachable!()
        };

        let expected = ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
            Some(vec![Some(1), Some(2), Some(10), Some(3)]),
            Some(vec![Some(4), Some(5), Some(20)]),
            Some(vec![Some(100), None, Some(1)]),
            None,
        ]);

        assert_eq!(&result.to_data(), &expected.to_data());

        Ok(())
    }

    #[test]
    fn test_array_insert_legacy_mode() -> Result<()> {
        // Test the so-called "legacy" mode exisiting in the Spark
        let list = ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
            Some(vec![Some(1), Some(2), Some(3)]),
            Some(vec![Some(4), Some(5)]),
            None,
        ]);

        let positions = Int32Array::from(vec![-1, -1, -1]);
        let items = Int32Array::from(vec![Some(10), Some(20), Some(30)]);

        let ColumnarValue::Array(result) = array_insert(
            &list,
            &(Arc::new(items) as ArrayRef),
            &(Arc::new(positions) as ArrayRef),
            true,
        )?
        else {
            unreachable!()
        };

        let expected = ListArray::from_iter_primitive::<Int32Type, _, _>(vec![
            Some(vec![Some(1), Some(2), Some(10), Some(3)]),
            Some(vec![Some(4), Some(20), Some(5)]),
            None,
        ]);

        assert_eq!(&result.to_data(), &expected.to_data());

        Ok(())
    }
}
