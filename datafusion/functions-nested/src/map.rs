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
use std::collections::{HashSet, VecDeque};
use std::sync::Arc;

use arrow::array::ArrayData;
use arrow_array::{Array, ArrayRef, MapArray, OffsetSizeTrait, StructArray};
use arrow_buffer::{Buffer, ToByteSlice};
use arrow_schema::{DataType, Field, SchemaBuilder};

use datafusion_common::utils::{fixed_size_list_to_arrays, list_to_arrays};
use datafusion_common::{exec_err, Result, ScalarValue};
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

fn make_map_batch(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    if args.len() != 2 {
        return exec_err!(
            "make_map requires exactly 2 arguments, got {} instead",
            args.len()
        );
    }

    let can_evaluate_to_const = can_evaluate_to_const(args);

    // check the keys array is unique
    let keys = get_first_array_ref(&args[0])?;
    if keys.null_count() > 0 {
        return exec_err!("map key cannot be null");
    }
    let key_array = keys.as_ref();

    match &args[0] {
        ColumnarValue::Array(_) => {
            let row_keys = match key_array.data_type() {
                DataType::List(_) => list_to_arrays::<i32>(&keys),
                DataType::LargeList(_) => list_to_arrays::<i64>(&keys),
                DataType::FixedSizeList(_, _) => fixed_size_list_to_arrays(&keys),
                data_type => {
                    return exec_err!(
                        "Expected list, large_list or fixed_size_list, got {:?}",
                        data_type
                    );
                }
            };

            row_keys
                .iter()
                .try_for_each(|key| check_unique_keys(key.as_ref()))?;
        }
        ColumnarValue::Scalar(_) => {
            check_unique_keys(key_array)?;
        }
    }

    let values = get_first_array_ref(&args[1])?;
    make_map_batch_internal(keys, values, can_evaluate_to_const, args[0].data_type())
}

fn check_unique_keys(array: &dyn Array) -> Result<()> {
    let mut seen_keys = HashSet::with_capacity(array.len());

    for i in 0..array.len() {
        let key = ScalarValue::try_from_array(array, i)?;
        if seen_keys.contains(&key) {
            return exec_err!("map key must be unique, duplicate key found: {}", key);
        }
        seen_keys.insert(key);
    }
    Ok(())
}

fn get_first_array_ref(columnar_value: &ColumnarValue) -> Result<ArrayRef> {
    match columnar_value {
        ColumnarValue::Scalar(value) => match value {
            ScalarValue::List(array) => Ok(array.value(0)),
            ScalarValue::LargeList(array) => Ok(array.value(0)),
            ScalarValue::FixedSizeList(array) => Ok(array.value(0)),
            _ => exec_err!("Expected array, got {:?}", value),
        },
        ColumnarValue::Array(array) => Ok(array.to_owned()),
    }
}

fn make_map_batch_internal(
    keys: ArrayRef,
    values: ArrayRef,
    can_evaluate_to_const: bool,
    data_type: DataType,
) -> Result<ColumnarValue> {
    if keys.len() != values.len() {
        return exec_err!("map requires key and value lists to have the same length");
    }

    if !can_evaluate_to_const {
        return if let DataType::LargeList(..) = data_type {
            make_map_array_internal::<i64>(keys, values)
        } else {
            make_map_array_internal::<i32>(keys, values)
        };
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

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
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

    fn invoke(&self, args: &[ColumnarValue]) -> Result<ColumnarValue> {
        make_map_batch(args)
    }
}
fn get_element_type(data_type: &DataType) -> Result<&DataType> {
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

/// Helper function to create MapArray from array of values to support arrays for Map scalar function
///
/// ``` text
/// Format of input KEYS and VALUES column
///         keys                        values
/// +---------------------+       +---------------------+
/// | +-----------------+ |       | +-----------------+ |
/// | | [k11, k12, k13] | |       | | [v11, v12, v13] | |
/// | +-----------------+ |       | +-----------------+ |
/// |                     |       |                     |
/// | +-----------------+ |       | +-----------------+ |
/// | | [k21, k22, k23] | |       | | [v21, v22, v23] | |
/// | +-----------------+ |       | +-----------------+ |
/// |                     |       |                     |
/// | +-----------------+ |       | +-----------------+ |
/// | |[k31, k32, k33]  | |       | |[v31, v32, v33]  | |
/// | +-----------------+ |       | +-----------------+ |
/// +---------------------+       +---------------------+
/// ```
/// Flattened keys and values array to user create `StructArray`,
/// which serves as inner child for `MapArray`
///
/// ``` text
/// Flattened           Flattened
/// Keys                Values
/// +-----------+      +-----------+
/// | +-------+ |      | +-------+ |
/// | |  k11  | |      | |  v11  | |
/// | +-------+ |      | +-------+ |
/// | +-------+ |      | +-------+ |
/// | |  k12  | |      | |  v12  | |
/// | +-------+ |      | +-------+ |
/// | +-------+ |      | +-------+ |
/// | |  k13  | |      | |  v13  | |
/// | +-------+ |      | +-------+ |
/// | +-------+ |      | +-------+ |
/// | |  k21  | |      | |  v21  | |
/// | +-------+ |      | +-------+ |
/// | +-------+ |      | +-------+ |
/// | |  k22  | |      | |  v22  | |
/// | +-------+ |      | +-------+ |
/// | +-------+ |      | +-------+ |
/// | |  k23  | |      | |  v23  | |
/// | +-------+ |      | +-------+ |
/// | +-------+ |      | +-------+ |
/// | |  k31  | |      | |  v31  | |
/// | +-------+ |      | +-------+ |
/// | +-------+ |      | +-------+ |
/// | |  k32  | |      | |  v32  | |
/// | +-------+ |      | +-------+ |
/// | +-------+ |      | +-------+ |
/// | |  k33  | |      | |  v33  | |
/// | +-------+ |      | +-------+ |
/// +-----------+      +-----------+
/// ```text

fn make_map_array_internal<O: OffsetSizeTrait>(
    keys: ArrayRef,
    values: ArrayRef,
) -> Result<ColumnarValue> {
    let mut offset_buffer = vec![O::zero()];
    let mut running_offset = O::zero();

    let keys = list_to_arrays::<O>(&keys);
    let values = list_to_arrays::<O>(&values);

    let mut key_array_vec = vec![];
    let mut value_array_vec = vec![];
    for (k, v) in keys.iter().zip(values.iter()) {
        running_offset = running_offset.add(O::usize_as(k.len()));
        offset_buffer.push(running_offset);
        key_array_vec.push(k.as_ref());
        value_array_vec.push(v.as_ref());
    }

    // concatenate all the arrays
    let flattened_keys = arrow::compute::concat(key_array_vec.as_ref())?;
    if flattened_keys.null_count() > 0 {
        return exec_err!("keys cannot be null");
    }
    let flattened_values = arrow::compute::concat(value_array_vec.as_ref())?;

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
        .build()?;

    let map_data = ArrayData::builder(DataType::Map(
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
    .build()?;
    Ok(ColumnarValue::Array(Arc::new(MapArray::from(map_data))))
}
