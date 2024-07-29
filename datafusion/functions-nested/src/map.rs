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

use std::any::Any;
use std::collections::VecDeque;
use std::sync::Arc;

use arrow::array::{ArrayData, Capacities, MutableArrayData};
use arrow_array::{Array, ArrayRef, GenericListArray, MapArray, StructArray};
use arrow_buffer::{Buffer, OffsetBuffer, ToByteSlice};
use arrow_schema::{DataType, Field, SchemaBuilder};

use datafusion_common::cast::as_list_array;
use datafusion_common::{exec_err, ScalarValue};
use datafusion_expr::expr::ScalarFunction;
use datafusion_expr::{ColumnarValue, Expr, ScalarUDFImpl, Signature, Volatility};

use crate::make_array::make_array;

/// Returns a map created from a key list and a value list
pub fn map(keys: Vec<Expr>, values: Vec<Expr>) -> Expr {
    let keys = make_array(keys);
    let values = make_array(values);
    Expr::ScalarFunction(ScalarFunction::new_udf(map_udf(), vec![keys, values]))
}

create_func!(MapFunc, map_udf);

/// Check if we can evaluate the expr to constant directly.
///
/// # Example
/// ```sql
/// SELECT make_map('type', 'test') from test
/// ```
/// We can evaluate the result of `make_map` directly.
fn can_evaluate_to_const(args: &[ColumnarValue]) -> bool {
    args.iter()
        .all(|arg| matches!(arg, ColumnarValue::Scalar(_)))
}

fn make_map_batch(args: &[ColumnarValue]) -> datafusion_common::Result<ColumnarValue> {
    if args.len() != 2 {
        return exec_err!(
            "make_map requires exactly 2 arguments, got {} instead",
            args.len()
        );
    }

    let can_evaluate_to_const = can_evaluate_to_const(args);

    let key = get_first_array_ref(&args[0])?;
    let value = get_first_array_ref(&args[1])?;
    make_map_batch_internal(key, value, can_evaluate_to_const)
}

fn get_first_array_ref(
    columnar_value: &ColumnarValue,
) -> datafusion_common::Result<ArrayRef> {
    match columnar_value {
        ColumnarValue::Scalar(value) => match value {
            ScalarValue::List(array) => Ok(array.value(0)),
            ScalarValue::LargeList(array) => Ok(array.value(0)),
            ScalarValue::FixedSizeList(array) => Ok(array.value(0)),
            _ => exec_err!("Expected array, got {:?}", value),
        },
        ColumnarValue::Array(array) => {
            // return ListArray
            // First element in array has all keys in first row,
            // Same for values
            Ok(array.to_owned())
        }
    }
}

fn make_map_batch_internal(
    keys: ArrayRef,
    values: ArrayRef,
    can_evaluate_to_const: bool,
) -> datafusion_common::Result<ColumnarValue> {
    if keys.null_count() > 0 {
        return exec_err!("map key cannot be null");
    }

    if keys.len() != values.len() {
        return exec_err!("map requires key and value lists to have the same length");
    }

    if keys.data_type().is_nested() && values.data_type().is_nested() {
        return handle_array(keys, values);
    }

    let key_field = Arc::new(Field::new("key", keys.data_type().clone(), false));
    let value_field = Arc::new(Field::new("value", values.data_type().clone(), true));
    let mut entry_struct_buffer: VecDeque<(Arc<Field>, ArrayRef)> = VecDeque::new();
    let mut entry_offsets_buffer = VecDeque::new();
    entry_offsets_buffer.push_back(0);

    entry_struct_buffer.push_back((Arc::clone(&key_field), Arc::clone(&keys)));
    entry_struct_buffer.push_back((Arc::clone(&value_field), Arc::clone(&values)));
    entry_offsets_buffer.push_back(keys.len() as u32);

    let entry_struct: Vec<(Arc<Field>, ArrayRef)> = entry_struct_buffer.into();
    let entry_struct = StructArray::from(entry_struct);

    let map_data_type = DataType::Map(
        Arc::new(Field::new(
            "entries",
            entry_struct.data_type().clone(),
            false,
        )),
        false,
    );

    let entry_offsets: Vec<u32> = entry_offsets_buffer.into();
    let entry_offsets_buffer = Buffer::from(entry_offsets.to_byte_slice());

    let map_data = ArrayData::builder(map_data_type)
        .len(entry_offsets.len() - 1)
        .add_buffer(entry_offsets_buffer)
        .add_child_data(entry_struct.to_data())
        .build()?;
    let map_array = Arc::new(MapArray::from(map_data));

    Ok(if can_evaluate_to_const {
        ColumnarValue::Scalar(ScalarValue::try_from_array(map_array.as_ref(), 0)?)
    } else {
        ColumnarValue::Array(map_array)
    })
}

fn create_map(keys: ArrayRef, values: ArrayRef) -> ArrayData {
    let key_field = Arc::new(Field::new("key", keys.data_type().clone(), false));
    let value_field = Arc::new(Field::new("value", values.data_type().clone(), true));
    let mut entry_struct_buffer: VecDeque<(Arc<Field>, ArrayRef)> = VecDeque::new();
    let mut entry_offsets_buffer = VecDeque::new();
    entry_offsets_buffer.push_back(0);

    entry_struct_buffer.push_back((Arc::clone(&key_field), Arc::clone(&keys)));
    entry_struct_buffer.push_back((Arc::clone(&value_field), Arc::clone(&values)));
    entry_offsets_buffer.push_back(keys.len() as u32);

    let entry_struct: Vec<(Arc<Field>, ArrayRef)> = entry_struct_buffer.into();
    let entry_struct = StructArray::from(entry_struct);

    let map_data_type = DataType::Map(
        Arc::new(Field::new(
            "entries",
            entry_struct.data_type().clone(),
            false,
        )),
        false,
    );

    let entry_offsets: Vec<u32> = entry_offsets_buffer.into();
    let entry_offsets_buffer = Buffer::from(entry_offsets.to_byte_slice());

    let map_data = ArrayData::builder(map_data_type)
        .len(entry_offsets.len() - 1)
        .add_buffer(entry_offsets_buffer)
        .add_child_data(entry_struct.to_data())
        .build()
        .unwrap();
    map_data
}

fn handle_array(
    keys: ArrayRef,
    values: ArrayRef,
) -> datafusion_common::Result<ColumnarValue> {
    let keys = as_list_array(keys.as_ref())?;
    let values = as_list_array(values.as_ref())?;

    let mut map_data_vec = vec![];
    for (k, v) in keys.iter().zip(values.iter()) {
        map_data_vec.push(create_map(k.unwrap(), v.unwrap()));
    }

    let total_len = map_data_vec.len();
    let mut offsets: Vec<i32> = Vec::with_capacity(total_len);
    offsets.push(0);

    let capacity = Capacities::Array(total_len);
    let map_data_vec_ref = map_data_vec.iter().collect::<Vec<_>>();
    let mut mutable = MutableArrayData::with_capacities(map_data_vec_ref, true, capacity);

    // let num_rows = args[0].len();

    for (arr_idx, arg) in map_data_vec.iter().enumerate() {
        if !arg.is_null(0) {
            mutable.extend(arr_idx, 0, arg.len());
        } else {
            mutable.extend_nulls(1);
        }
        offsets.push(mutable.len() as i32);
    }
    let data = mutable.freeze();

    let data_type = map_data_vec[0].data_type().to_owned();

    let array = Arc::new(GenericListArray::<i32>::try_new(
        Arc::new(Field::new("item", data_type, true)),
        OffsetBuffer::new(offsets.into()),
        arrow_array::make_array(data),
        None,
    )?);
    Ok(ColumnarValue::Array(array))
}

#[derive(Debug)]
pub struct MapFunc {
    signature: Signature,
}

impl Default for MapFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl MapFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for MapFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "map"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> datafusion_common::Result<DataType> {
        if arg_types.len() % 2 != 0 {
            return exec_err!(
                "map requires an even number of arguments, got {} instead",
                arg_types.len()
            );
        }
        let mut builder = SchemaBuilder::new();
        builder.push(Field::new(
            "key",
            get_element_type(&arg_types[0])?.clone(),
            false,
        ));
        builder.push(Field::new(
            "value",
            get_element_type(&arg_types[1])?.clone(),
            true,
        ));
        let fields = builder.finish().fields;
        let map_dt = DataType::Map(
            Arc::new(Field::new("entries", DataType::Struct(fields), false)),
            false,
        );
        if !arg_types[0].is_nested() {
            return Ok(map_dt);
        } else {
            Ok(DataType::List(Arc::new(Field::new("item", map_dt, true))))
        }
    }

    fn invoke(&self, args: &[ColumnarValue]) -> datafusion_common::Result<ColumnarValue> {
        make_map_batch(args)
    }
}

fn get_element_type(data_type: &DataType) -> datafusion_common::Result<&DataType> {
    match data_type {
        DataType::List(element) => Ok(element.data_type()),
        DataType::LargeList(element) => Ok(element.data_type()),
        DataType::FixedSizeList(element, _) => Ok(element.data_type()),
        _ => exec_err!(
            "Expected list, large_list or fixed_size_list, got {:?}",
            data_type
        ),
    }
}
