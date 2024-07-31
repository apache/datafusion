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
        ColumnarValue::Array(_) => {
            let key = get_first_array_ref(&args[0])?;
            let value = get_first_array_ref(&args[1])?;
            make_map_array_internal::<i32>(key, value)
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

fn make_map_array_internal<O: OffsetSizeTrait>(
    keys: ArrayRef,
    values: ArrayRef,
) -> datafusion_common::Result<ColumnarValue> {
    let mut offset_buffer = vec![O::usize_as(0)];
    let mut running_offset = O::usize_as(0);
    // FIXME: Can we use ColumnarValue::values_to_arrays() over here
    let keys = keys
        .as_list::<O>()
        .iter()
        .flatten()
        .map(|x| match x.data_type() {
            DataType::List(_) => x.as_list::<i32>().value(0),
            _ => x,
        })
        .collect::<Vec<_>>();
    // each item is an array
    let values = values
        .as_list::<O>()
        .iter()
        .flatten()
        .map(|x| match x.data_type() {
            DataType::List(_) => x.as_list::<i32>().value(0),
            _ => x,
        })
        .collect::<Vec<_>>();
    assert_eq!(keys.len(), values.len());

    let mut key_array_vec = vec![];
    let mut value_array_vec = vec![];
    for (k, v) in keys.iter().zip(values.iter()) {
        running_offset = running_offset.add(O::usize_as(k.len()));
        offset_buffer.push(running_offset);
        key_array_vec.push(k.as_ref());
        value_array_vec.push(v.as_ref());
    }

    // concatenate all the arrays in t
    let flattened_keys = arrow::compute::concat(key_array_vec.as_ref()).unwrap();
    let flattened_values = arrow::compute::concat(value_array_vec.as_ref()).unwrap();

    let fields = vec![
        Arc::new(Field::new("key", flattened_keys.data_type().clone(), false)),
        Arc::new(Field::new(
            "value",
            flattened_values.data_type().clone(),
            true,
        )),
    ];
    let struct_data = ArrayData::builder(DataType::Struct(fields.into()))
        .len(flattened_keys.len())
        .add_child_data(flattened_keys.to_data())
        .add_child_data(flattened_values.to_data())
        .build()
        .unwrap();
    // Data should be struct array
    // offer should be partition of the struct array
    let data = ArrayData::builder(DataType::Map(
        Arc::new(Field::new(
            "entries",
            struct_data.data_type().clone(),
            false,
        )),
        false,
    ))
    .len(keys.len())
    .add_child_data(struct_data)
    .add_buffer(Buffer::from_slice_ref(offset_buffer.as_slice()))
    .build()
    .unwrap();
    Ok(ColumnarValue::Array(Arc::new(MapArray::from(data))))
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
            get_element_type(&arg_types[0], 0)?.clone(),
            false,
        ));
        builder.push(Field::new(
            "value",
            get_element_type(&arg_types[1], 0)?.clone(),
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

fn get_element_type(
    data_type: &DataType,
    level: u8,
) -> datafusion_common::Result<&DataType> {
    match data_type {
        DataType::List(element) => Ok(get_element_type(element.data_type(), level + 1)?),
        DataType::LargeList(element) => {
            Ok(get_element_type(element.data_type(), level + 1)?)
        }
        DataType::FixedSizeList(element, _) => {
            Ok(get_element_type(element.data_type(), level + 1)?)
        }
        t => {
            if level <= 2 {
                Ok(t)
            } else {
                exec_err!(
                    "Expected list, large_list or fixed_size_list, got {:?}",
                    data_type
                )
            }
        }
    }
}
