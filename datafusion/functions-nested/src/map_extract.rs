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

//! [`ScalarUDFImpl`] definitions for map_extract functions.

use crate::utils::{get_map_entry_field, make_scalar_function};
use arrow::array::{
    Array, ArrayRef, ListArray, MapArray, MutableArrayData, make_array, new_empty_array,
};
use arrow::buffer::OffsetBuffer;
use arrow::compute::SortOptions;
use arrow::datatypes::{DataType, Field};
use arrow_ord::ord::make_comparator;
use datafusion_common::utils::take_function_args;
use datafusion_common::{Result, cast::as_map_array, exec_err};
use datafusion_expr::{
    ColumnarValue, Documentation, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    Volatility,
};
use datafusion_macros::user_doc;
use std::sync::Arc;

// Create static instances of ScalarUDFs for each function
make_udf_expr_and_func!(
    MapExtract,
    map_extract,
    map key,
    "Return a list containing the value for a given key or an empty list if the key is not contained in the map.",
    map_extract_udf
);

#[user_doc(
    doc_section(label = "Map Functions"),
    description = "Returns a list containing the value for the given key or an empty list if the key is not present in the map.",
    syntax_example = "map_extract(map, key)",
    sql_example = r#"```sql
SELECT map_extract(MAP {'a': 1, 'b': NULL, 'c': 3}, 'a');
----
[1]

SELECT map_extract(MAP {1: 'one', 2: 'two'}, 2);
----
['two']

SELECT map_extract(MAP {'x': 10, 'y': NULL, 'z': 30}, 'y');
----
[NULL]

-- non-existing key
SELECT map_extract(MAP {'x': 10, 'y': NULL, 'z': 30}, 'a');
----
[]
```"#,
    argument(
        name = "map",
        description = "Map expression. Can be a constant, column, or function, and any combination of map operators."
    ),
    argument(
        name = "key",
        description = "Key to extract from the map. Can be a constant, column, or function, any combination of arithmetic or string operators, or a named expression of the previously listed."
    )
)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct MapExtract {
    signature: Signature,
    aliases: Vec<String>,
}

impl Default for MapExtract {
    fn default() -> Self {
        Self::new()
    }
}

impl MapExtract {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
            aliases: vec![String::from("element_at")],
        }
    }
}

impl ScalarUDFImpl for MapExtract {
    fn name(&self) -> &str {
        "map_extract"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        let [map_type, _] = take_function_args(self.name(), arg_types)?;

        if map_type.is_null() {
            return Ok(DataType::Null);
        }

        let map_fields = get_map_entry_field(map_type)?;
        Ok(DataType::List(Arc::new(Field::new_list_field(
            map_fields.last().unwrap().data_type().clone(),
            true,
        ))))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        make_scalar_function(map_extract_inner)(&args.args)
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        let [map_type, _] = take_function_args(self.name(), arg_types)?;

        if map_type.is_null() {
            return Ok(arg_types.to_vec());
        }

        let field = get_map_entry_field(map_type)?;
        Ok(vec![
            map_type.clone(),
            field.first().unwrap().data_type().clone(),
        ])
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

fn general_map_extract_inner(
    map_array: &MapArray,
    query_keys_array: &dyn Array,
) -> Result<ArrayRef> {
    let keys = map_array.keys();
    let values = map_array.values();
    let field = Arc::new(Field::new_list_field(map_array.value_type().clone(), true));
    let map_offsets = map_array.value_offsets();
    if map_offsets.first() == map_offsets.last() {
        return Ok(Arc::new(ListArray::new(
            field,
            OffsetBuffer::new_zeroed(map_array.len()),
            new_empty_array(values.data_type()),
            map_array.nulls().cloned(),
        )));
    }

    // Compare keys by index using a single comparator for the batch.
    let compare =
        make_comparator(keys.as_ref(), query_keys_array, SortOptions::default())?;
    let mut offsets = Vec::with_capacity(map_array.len() + 1);
    offsets.push(0_i32);

    let original_data = values.to_data();
    // There is at most one output value per map row.
    let mut mutable = MutableArrayData::new(
        vec![&original_data],
        false,
        map_array.len().min(values.len()),
    );

    for (row_index, offset_window) in map_offsets.windows(2).enumerate() {
        let start = offset_window[0] as usize;
        let end = offset_window[1] as usize;
        let mut offset = offsets[row_index];

        if map_array.is_valid(row_index)
            && let Some(index) = (start..end).find(|&i| compare(i, row_index).is_eq())
        {
            mutable.try_extend(0, index, index + 1)?;
            offset += 1;
        }

        // A missing key results in an empty list.
        offsets.push(offset);
    }

    let data = mutable.freeze();

    Ok(Arc::new(ListArray::new(
        field,
        OffsetBuffer::<i32>::new(offsets.into()),
        make_array(data),
        map_array.nulls().cloned(),
    )))
}

fn map_extract_inner(args: &[ArrayRef]) -> Result<ArrayRef> {
    let [map_arg, key_arg] = take_function_args("map_extract", args)?;

    let map_array = match map_arg.data_type() {
        DataType::Map(_, _) => as_map_array(&map_arg)?,
        DataType::Null => return Ok(Arc::clone(map_arg)),
        _ => return exec_err!("The first argument in map_extract must be a map"),
    };

    let key_type = map_array.key_type();

    if key_type != key_arg.data_type() {
        return exec_err!(
            "The key type {} does not match the map key type {}",
            key_arg.data_type(),
            key_type
        );
    }

    general_map_extract_inner(map_array, key_arg)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Float64Array, Int32Array, StructArray};
    use arrow::buffer::NullBuffer;
    use arrow::datatypes::Int32Type;

    fn make_map(
        keys: ArrayRef,
        values: Vec<i32>,
        offsets: Vec<i32>,
        nulls: Option<NullBuffer>,
    ) -> MapArray {
        let entries = StructArray::from(vec![
            (
                Arc::new(Field::new("key", keys.data_type().clone(), false)),
                keys,
            ),
            (
                Arc::new(Field::new("value", DataType::Int32, true)),
                Arc::new(Int32Array::from(values)) as ArrayRef,
            ),
        ]);
        MapArray::new(
            Arc::new(Field::new("entries", entries.data_type().clone(), false)),
            OffsetBuffer::new(offsets.into()),
            entries,
            nulls,
            false,
        )
    }

    #[test]
    fn map_extract_sliced_maps() -> Result<()> {
        let map = make_map(
            Arc::new(Int32Array::from(vec![0, 1, 2, 3])),
            vec![0, 10, 20, 30],
            vec![0, 1, 3, 4],
            None,
        );
        let query_keys = Int32Array::from(vec![0, 2, 9]);

        // Map offsets address the original entries; query indices address the slice.
        let result =
            general_map_extract_inner(&map.slice(1, 2), &query_keys.slice(1, 2))?;
        let expected = ListArray::from_iter_primitive::<Int32Type, _, _>([
            Some(vec![Some(20)]),
            Some(vec![]),
        ]);
        assert_eq!(result.as_ref(), &expected);

        // Empty slices may retain the original nonempty keys and values buffers.
        let result =
            general_map_extract_inner(&map.slice(1, 0), &query_keys.slice(1, 0))?;
        assert_eq!(result.len(), 0);
        Ok(())
    }

    #[test]
    fn map_extract_all_empty_maps_with_nulls() -> Result<()> {
        // No entries exist to scan, but the null map must still produce NULL
        // rather than an empty list.
        let map = make_map(
            Arc::new(Int32Array::from(Vec::<i32>::new())),
            vec![],
            vec![0, 0, 0],
            Some(NullBuffer::from(vec![true, false])),
        );
        let result = general_map_extract_inner(&map, &Int32Array::from(vec![1, 1]))?;
        let expected =
            ListArray::from_iter_primitive::<Int32Type, _, _>([Some(vec![]), None]);
        assert_eq!(result.as_ref(), &expected);
        Ok(())
    }

    #[test]
    fn map_extract_float_keys() -> Result<()> {
        let nan = f64::NAN;
        let other_nan = f64::from_bits(nan.to_bits() + 1);
        let map = make_map(
            Arc::new(Float64Array::from(vec![-0.0, 0.0, nan, other_nan])),
            vec![1, 2, 3, 4],
            vec![0, 4],
            None,
        );

        // Signed zeros and distinct NaN payloads identify different keys.
        for (query, expected) in [(-0.0, 1), (0.0, 2), (nan, 3), (other_nan, 4)] {
            let result =
                general_map_extract_inner(&map, &Float64Array::from(vec![query]))?;
            let expected = ListArray::from_iter_primitive::<Int32Type, _, _>([Some(
                vec![Some(expected)],
            )]);
            assert_eq!(result.as_ref(), &expected);
        }
        Ok(())
    }
}
