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
use std::sync::Arc;

use arrow::array::{ArrayRef, MapArray, StructArray};
use arrow::buffer::OffsetBuffer;
use arrow::compute::interleave;
use arrow::datatypes::{DataType, Field, Fields};
use datafusion_common::{exec_err, Result};
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};

trait KeyValue<T>
where
    T: 'static,
{
    fn keys(&self) -> impl Iterator<Item = &T>;
    fn values(&self) -> impl Iterator<Item = &T>;
}

impl<T> KeyValue<T> for &[T]
where
    T: 'static,
{
    fn keys(&self) -> impl Iterator<Item = &T> {
        self.iter().step_by(2)
    }

    fn values(&self) -> impl Iterator<Item = &T> {
        self.iter().skip(1).step_by(2)
    }
}

fn to_map_array(args: &[ArrayRef]) -> Result<ArrayRef> {
    if !args.len().is_multiple_of(2) {
        return exec_err!("map requires an even number of arguments");
    }
    let num_entries = args.len() / 2;
    let num_rows = args.first().map(|a| a.len()).unwrap_or(0);
    if args.iter().any(|a| a.len() != num_rows) {
        return exec_err!("map requires all arrays to have the same length");
    }
    let key_type = args
        .first()
        .map(|a| a.data_type())
        .unwrap_or(&DataType::Null);
    let value_type = args
        .get(1)
        .map(|a| a.data_type())
        .unwrap_or(&DataType::Null);
    let keys = args.keys().map(|a| a.as_ref()).collect::<Vec<_>>();
    let values = args.values().map(|a| a.as_ref()).collect::<Vec<_>>();
    if keys.iter().any(|a| a.data_type() != key_type) {
        return exec_err!("map requires all key types to be the same");
    }
    if values.iter().any(|a| a.data_type() != value_type) {
        return exec_err!("map requires all value types to be the same");
    }
    // TODO: avoid materializing the indices
    let indices = (0..num_rows)
        .flat_map(|i| (0..num_entries).map(move |j| (j, i)))
        .collect::<Vec<_>>();
    let keys = interleave(keys.as_slice(), indices.as_slice())?;
    let values = interleave(values.as_slice(), indices.as_slice())?;
    let offsets = (0..num_rows + 1)
        .map(|i| i as i32 * num_entries as i32)
        .collect::<Vec<_>>();
    let offsets = OffsetBuffer::new(offsets.into());
    let fields = Fields::from(vec![
        Field::new("key", key_type.clone(), false),
        Field::new("value", value_type.clone(), true),
    ]);
    let entries = StructArray::try_new(fields.clone(), vec![keys, values], None)?;
    let field = Arc::new(Field::new("entries", DataType::Struct(fields), false));
    Ok(Arc::new(MapArray::try_new(
        field, offsets, entries, None, false,
    )?))
}

#[derive(Debug, Clone)]
pub struct MapFunction {
    signature: Signature,
}

impl Default for MapFunction {
    fn default() -> Self {
        Self::new()
    }
}

impl MapFunction {
    pub fn new() -> Self {
        Self {
            signature: Signature::variadic_any(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for MapFunction {
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
        if !arg_types.len().is_multiple_of(2) {
            return exec_err!("map requires an even number of arguments");
        }
        let key_type = arg_types.first().unwrap_or(&DataType::Null);
        let value_type = arg_types.get(1).unwrap_or(&DataType::Null);
        // TODO: support type coercion
        if arg_types.keys().any(|dt| dt != key_type) {
            return exec_err!("map requires all key types to be the same");
        }
        if arg_types.values().any(|dt| dt != value_type) {
            return exec_err!("map requires all value types to be the same");
        }
        Ok(DataType::Map(
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
        ))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;
        let arrays = ColumnarValue::values_to_arrays(&args)?;
        Ok(ColumnarValue::Array(to_map_array(arrays.as_slice())?))
    }
}
