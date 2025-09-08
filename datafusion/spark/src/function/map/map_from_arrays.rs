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
use std::borrow::Cow;
use std::collections::HashSet;
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, AsArray, BooleanBuilder, MapArray, StructArray};
use arrow::buffer::OffsetBuffer;
use arrow::compute::filter;
use arrow::datatypes::{DataType, Field, Fields};
use datafusion_common::utils::take_function_args;
use datafusion_common::{exec_err, internal_err, Result, ScalarValue};
use datafusion_expr::{ColumnarValue, ScalarUDFImpl, Signature, Volatility};
use datafusion_functions::utils::make_scalar_function;

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct MapFromArrays {
    signature: Signature,
}

impl Default for MapFromArrays {
    fn default() -> Self {
        Self::new()
    }
}

impl MapFromArrays {
    pub fn new() -> Self {
        Self {
            signature: Signature::any(2, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for MapFromArrays {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "map_from_arrays"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        let [key_type, value_type] = take_function_args("map_from_arrays", arg_types)?;
        Ok(return_type_from_key_value_types(
            get_element_type(key_type)?,
            get_element_type(value_type)?,
        ))
    }

    fn invoke_with_args(
        &self,
        args: datafusion_expr::ScalarFunctionArgs,
    ) -> Result<ColumnarValue> {
        make_scalar_function(map_from_arrays_inner, vec![])(&args.args)
    }
}

fn get_list_field(data_type: &DataType) -> Result<&Arc<Field>> {
    match data_type {
        DataType::List(element)
        | DataType::LargeList(element)
        | DataType::FixedSizeList(element, _) => Ok(element),
        _ => exec_err!(
            "map_from_arrays expects 2 listarrays for keys and values as arguments, got {data_type:?}"
        ),
    }
}

fn get_element_type(data_type: &DataType) -> Result<&DataType> {
    get_list_field(data_type).map(|field| field.data_type())
}

pub fn return_type_from_key_value_types(
    key_type: &DataType,
    value_type: &DataType,
) -> DataType {
    DataType::Map(
        Arc::new(Field::new(
            "entries",
            DataType::Struct(Fields::from(vec![
                // the key must not be nullable
                Field::new("key", key_type.clone(), false),
                Field::new("value", value_type.clone(), true),
            ])),
            false, // the entry is not nullable
        )),
        false, // the keys are not sorted
    )
}

fn get_list_values(array: &ArrayRef) -> Result<&ArrayRef> {
    match array.data_type() {
        DataType::List(_) => Ok(array.as_list::<i32>().values()),
        DataType::LargeList(_) => Ok(array.as_list::<i64>().values()),
        DataType::FixedSizeList(..) => Ok(array.as_fixed_size_list().values()),
        wrong_type => internal_err!(
            "get_list_values expects List/LargeList/FixedSizeList as argument, got {wrong_type:?}"
        ),
    }
}

fn map_from_arrays_inner(args: &[ArrayRef]) -> Result<ArrayRef> {
    let [keys, values] = take_function_args("map_from_arrays", args)?;

    let flat_keys = get_list_values(keys)?;
    let flat_values = get_list_values(values)?;

    let offsets: Cow<[i32]> = match keys.data_type() {
        DataType::List(_) => Ok(Cow::Borrowed(keys.as_list::<i32>().offsets().as_ref())),
        DataType::LargeList(_) => Ok(Cow::Owned(
            keys.as_list::<i64>()
                .offsets()
                .iter()
                .map(|i| *i as i32)
                .collect::<Vec<_>>(),
        )),
        DataType::FixedSizeList(_, size) => Ok(Cow::Owned(
             (0..=keys.len() as i32).map(|i| size * i).collect()
        )),
        wrong_type => internal_err!(
            "map_from_arrays expects List/LargeList/FixedSizeList as first argument, got {wrong_type:?}"
        ),
    }?;

    map_from_keys_values_offsets(flat_keys, flat_values, &offsets)
}

pub fn map_from_keys_values_offsets(
    keys: &ArrayRef,
    values: &ArrayRef,
    offsets: &[i32],
) -> Result<ArrayRef> {
    let (keys, values, offsets) = map_deduplicate_keys(keys, values, offsets)?;

    let fields = Fields::from(vec![
        Field::new("key", keys.data_type().clone(), false),
        Field::new("value", values.data_type().clone(), true),
    ]);
    let entries = StructArray::try_new(fields.clone(), vec![keys, values], None)?;
    let field = Arc::new(Field::new("entries", DataType::Struct(fields), false));
    Ok(Arc::new(MapArray::try_new(
        field, offsets, entries, None, false,
    )?))
}

fn map_deduplicate_keys(
    keys: &ArrayRef,
    values: &ArrayRef,
    offsets: &[i32],
) -> Result<(ArrayRef, ArrayRef, OffsetBuffer<i32>)> {
    let offsets_len = offsets.len();
    let mut new_offsets = Vec::with_capacity(offsets_len);

    let mut cur_offset = 0;
    let mut new_last_offset = 0;
    new_offsets.push(new_last_offset);

    let mut needed_rows_builder = BooleanBuilder::new();
    for next_offset in offsets.iter().skip(1) {
        let num_entries = *next_offset as usize - cur_offset;
        let mut seen_keys = HashSet::new();
        let mut needed_rows_one = [false].repeat(num_entries);
        for cur_entry_idx in (0..num_entries).rev() {
            let key = ScalarValue::try_from_array(&keys, cur_offset + cur_entry_idx)?
                .compacted();
            if seen_keys.contains(&key) {
                // TODO: implement configuration and logic for spark.sql.mapKeyDedupPolicy=EXCEPTION (this is default spark-config)
                // exec_err!("invalid argument: duplicate keys in map")
                // https://github.com/apache/spark/blob/cf3a34e19dfcf70e2d679217ff1ba21302212472/sql/catalyst/src/main/scala/org/apache/spark/sql/internal/SQLConf.scala#L4961
            } else {
                // This code implements deduplication logic for spark.sql.mapKeyDedupPolicy=LAST_WIN (this is NOT default spark-config)
                needed_rows_one[cur_entry_idx] = true;
                seen_keys.insert(key);
                new_last_offset += 1;
            }
        }
        needed_rows_builder.append_array(&needed_rows_one.into());
        new_offsets.push(new_last_offset);
        cur_offset += num_entries;
    }
    let needed_rows = needed_rows_builder.finish();
    let needed_keys = filter(&keys, &needed_rows)?;
    let needed_values = filter(&values, &needed_rows)?;
    let offsets = OffsetBuffer::new(new_offsets.into());
    Ok((needed_keys, needed_values, offsets))
}
