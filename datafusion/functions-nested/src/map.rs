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

use arrow::array::ArrayData;
use arrow::compute::concat;
use arrow_array::cast::AsArray;
use arrow_array::{Array, ArrayRef, MapArray, OffsetSizeTrait, StructArray};
use arrow_buffer::{Buffer, ToByteSlice};
use arrow_schema::{DataType, Field, SchemaBuilder};

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

    match args[0] {
        ColumnarValue::Scalar(_) => {
            let can_evaluate_to_const = can_evaluate_to_const(args);
            let key = get_first_array_ref(&args[0])?;
            let value = get_first_array_ref(&args[1])?;
            make_map_batch_internal(key, value, can_evaluate_to_const)
        }
        _ => {
            let key = get_first_array_ref(&args[0])?;
            let value = get_first_array_ref(&args[1])?;
            return handle_array::<i32>(key, value);
        }
    }
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

fn handle_array<O: OffsetSizeTrait>(
    keys: ArrayRef,
    values: ArrayRef,
) -> datafusion_common::Result<ColumnarValue> {
    let key_count = keys.len();
    let keys = keys.as_list::<O>();
    let values = values.as_list::<O>();

    let mut keys_ref = vec![];
    let mut values_ref = vec![];
    let mut offsets = vec![];
    offsets.push(O::usize_as(0));
    let keys = keys.iter().map(|x| x.unwrap()).collect::<Vec<_>>();
    let values = values.iter().map(|x| x.unwrap()).collect::<Vec<_>>();

    let mut current_offset: O = O::usize_as(0);
    for (k, v) in keys.iter().zip(values.iter()) {
        current_offset = current_offset.add(O::usize_as(k.len()));
        offsets.push(current_offset);
        keys_ref.push(k.as_ref());
        values_ref.push(v.as_ref());
    }

    let flattened_keys = concat(&keys_ref).unwrap();
    let flattened_values = concat(&values_ref).unwrap();

    let fields = vec![
        Field::new("key", keys_ref[0].data_type().clone(), false),
        Field::new("value", values_ref[0].data_type().clone(), true),
    ];

    let struct_data = ArrayData::builder(DataType::Struct(fields.clone().into()))
        .len(flattened_keys.len())
        .offset(0)
        .add_child_data(flattened_keys.to_data())
        .add_child_data(flattened_values.to_data())
        .build()
        .unwrap();

    let map_data = ArrayData::builder(DataType::Map(
        Arc::new(Field::new(
            "entries",
            DataType::Struct(fields.clone().into()),
            false,
        )),
        false,
    ))
    .len(key_count)
    .offset(0)
    .add_buffer(Buffer::from_slice_ref(offsets.as_slice()))
    .add_child_data(struct_data.clone())
    .build()
    .unwrap();

    Ok(ColumnarValue::Array(Arc::new(MapArray::from(map_data))))
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
        Ok(DataType::Map(
            Arc::new(Field::new("entries", DataType::Struct(fields), false)),
            false,
        ))
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
