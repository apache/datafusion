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

//! `MAP_AGG` aggregate implementation: [`MapAgg`]
//!
//! Aggregates key/value pairs into a `Map`, analogous to how `array_agg`
//! aggregates values into a `List`.

use std::collections::VecDeque;
use std::mem::{size_of, size_of_val};
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, MapArray, StructArray};
use arrow::buffer::{OffsetBuffer, ScalarBuffer};
use arrow::datatypes::{DataType, Field, FieldRef, Fields};

use datafusion_common::cast::as_map_array;
use datafusion_common::utils::take_function_args;
use datafusion_common::{
    Result, ScalarValue, assert_eq_or_internal_err, exec_err, internal_err,
};
use datafusion_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion_expr::utils::format_state_name;
use datafusion_expr::{
    Accumulator, AggregateUDFImpl, Documentation, Signature, Volatility,
};
use datafusion_macros::user_doc;

make_udaf_expr_and_func!(
    MapAgg,
    map_agg,
    key value,
    "Aggregates keys and values into a map",
    map_agg_udaf
);

#[user_doc(
    doc_section(label = "General Functions"),
    description = "Returns a map created from the key and value expression elements. \
For each row, the key expression becomes a map key and the value expression becomes the corresponding map value.",
    syntax_example = "map_agg(key, value)",
    sql_example = r#"```sql
> SELECT map_agg(column_key, column_value) FROM table_name;
+-------------------------------------+
| map_agg(column_key, column_value)   |
+-------------------------------------+
| {key1: value1, key2: value2, ...}    |
+-------------------------------------+
```"#,
    argument(
        name = "key",
        description = "Expression used as the map key. Can be a column or any valid expression."
    ),
    argument(
        name = "value",
        description = "Expression used as the map value. Can be a column or any valid expression."
    )
)]
#[derive(Debug, PartialEq, Eq, Hash)]
/// MAP_AGG aggregate expression
pub struct MapAgg {
    signature: Signature,
}

impl Default for MapAgg {
    fn default() -> Self {
        Self {
            signature: Signature::any(2, Volatility::Immutable),
        }
    }
}

impl MapAgg {
    /// Create a new MAP_AGG aggregate function
    pub fn new() -> Self {
        Self::default()
    }

    /// Build the Arrow `Map` data type for `map_agg(key, value)`
    fn map_data_type(key_type: &DataType, value_type: &DataType) -> DataType {
        DataType::Map(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(Fields::from(vec![
                    Field::new("key", key_type.clone(), false),
                    Field::new("value", value_type.clone(), true),
                ])),
                false,
            )),
            false,
        )
    }
}

impl AggregateUDFImpl for MapAgg {
    fn name(&self) -> &str {
        "map_agg"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        let [key_type, value_type] = take_function_args(self.name(), arg_types)?;
        Ok(Self::map_data_type(key_type, value_type))
    }

    fn state_fields(&self, args: StateFieldsArgs) -> Result<Vec<FieldRef>> {
        let map_type = Self::map_data_type(
            args.input_fields[0].data_type(),
            args.input_fields[1].data_type(),
        );

        Ok(vec![
            Field::new(
                format_state_name(args.name, "map_agg"),
                // Nullable so empty groups can produce a NULL map
                map_type,
                true,
            )
            .into(),
        ])
    }

    fn accumulator(&self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        MapAggAccumulator::try_new(acc_args.return_field.data_type().clone())
            .map(|acc| Box::new(acc) as _)
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

/// Accumulates key/value pairs for [`MapAgg`].
///
/// Collected batches are concatenated when the map is built in [`Self::evaluate`];
/// partial states are single-row [`MapArray`] scalars that are split back into
/// batches in [`Self::merge_batch`].
#[derive(Debug)]
pub struct MapAggAccumulator {
    keys: VecDeque<ArrayRef>, // mirrors ArrayAggAccumulator::values
    values: VecDeque<ArrayRef>,
    datatype: DataType, // DataType::Map, from return_type
}

impl MapAggAccumulator {
    /// Create a new map_agg accumulator for the given map data type
    pub fn try_new(datatype: DataType) -> Result<Self> {
        Ok(Self {
            keys: VecDeque::new(),
            values: VecDeque::new(),
            datatype,
        })
    }
}

impl Accumulator for MapAggAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        assert_eq_or_internal_err!(values.len(), 2, "map_agg expects (key, value)");

        let keys = &values[0];
        let vals = &values[1];
        assert_eq_or_internal_err!(keys.len(), vals.len(), "key/value length mismatch");

        if keys.is_empty() {
            return Ok(());
        }

        self.keys.push_back(Arc::clone(keys));
        self.values.push_back(Arc::clone(vals));
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        if states.is_empty() {
            return Ok(());
        }

        assert_eq_or_internal_err!(states.len(), 1, "expects single state");

        let map_arr = as_map_array(states[0].as_ref())?;
        let offsets = map_arr.value_offsets();

        for row in 0..map_arr.len() {
            // Partial states are nullable so empty groups can produce a NULL map
            if map_arr.is_null(row) {
                continue;
            }
            let start = offsets[row] as usize;
            let end = offsets[row + 1] as usize;
            if start == end {
                continue;
            }
            self.keys
                .push_back(map_arr.keys().slice(start, end - start));
            self.values
                .push_back(map_arr.values().slice(start, end - start));
        }

        Ok(())
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![self.evaluate()?])
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        if self.keys.is_empty() {
            return ScalarValue::try_new_null(&self.datatype);
        }

        let key_refs: Vec<&dyn Array> = self.keys.iter().map(|a| a.as_ref()).collect();
        let value_refs: Vec<&dyn Array> =
            self.values.iter().map(|a| a.as_ref()).collect();

        let keys = arrow::compute::concat(&key_refs)?;
        let values = arrow::compute::concat(&value_refs)?;

        if keys.is_empty() {
            return ScalarValue::try_new_null(&self.datatype);
        }

        // Arrow maps cannot represent null keys (same rule as the `map` function)
        if keys.logical_null_count() > 0 {
            return exec_err!("map key cannot be null");
        }

        let DataType::Map(entries_field, ordered) = &self.datatype else {
            return internal_err!("map_agg expected Map type, got {}", self.datatype);
        };
        let DataType::Struct(entry_fields) = entries_field.data_type() else {
            return internal_err!(
                "map_agg expected struct entries, got {}",
                entries_field.data_type()
            );
        };

        let entries =
            StructArray::try_new(entry_fields.clone(), vec![keys, values], None)?;

        let Ok(num_entries) = i32::try_from(entries.len()) else {
            return internal_err!(
                "map_agg produced {} entries which exceeds the map offset range",
                entries.len()
            );
        };
        let offsets = OffsetBuffer::new(ScalarBuffer::from(vec![0_i32, num_entries]));
        let map_array = MapArray::try_new(
            Arc::clone(entries_field),
            offsets,
            entries,
            None,
            *ordered,
        )?;

        Ok(ScalarValue::Map(Arc::new(map_array)))
    }

    fn size(&self) -> usize {
        size_of_val(self)
            + (size_of::<ArrayRef>() * (self.keys.capacity() + self.values.capacity()))
            + self
                .keys
                .iter()
                .chain(self.values.iter())
                // See ArrayAggAccumulator::size: this approximates the memory each
                // ArrayRef would occupy if fully owned by this accumulator.
                .map(|arr| arr.to_data().get_slice_memory_size().unwrap_or_default())
                .sum::<usize>()
            + self.datatype.size()
            - size_of_val(&self.datatype)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use arrow::array::{Int32Array, StringArray};

    fn map_agg_accumulator() -> Result<MapAggAccumulator> {
        let datatype =
            MapAgg::default().return_type(&[DataType::Utf8, DataType::Int32])?;
        MapAggAccumulator::try_new(datatype)
    }

    #[test]
    fn map_agg_builds_map_from_batches() -> Result<()> {
        let mut acc = map_agg_accumulator()?;

        acc.update_batch(&[
            Arc::new(StringArray::from(vec!["a", "b"])),
            Arc::new(Int32Array::from(vec![Some(1), None])),
        ])?;
        acc.update_batch(&[
            Arc::new(StringArray::from(vec!["c"])),
            Arc::new(Int32Array::from(vec![Some(3)])),
        ])?;

        let ScalarValue::Map(map) = acc.evaluate()? else {
            panic!("expected map scalar");
        };
        assert_eq!(map.len(), 1);
        assert!(map.is_valid(0));
        assert_eq!(map.value_offsets(), &[0, 3]);

        let map_keys = map.keys().as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(map_keys, &StringArray::from(vec!["a", "b", "c"]));

        let map_values = map.values().as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(map_values, &Int32Array::from(vec![Some(1), None, Some(3)]));

        Ok(())
    }

    #[test]
    fn map_agg_empty_group_produces_null_map() -> Result<()> {
        let mut acc = map_agg_accumulator()?;
        let map_type =
            MapAgg::default().return_type(&[DataType::Utf8, DataType::Int32])?;

        acc.update_batch(&[
            Arc::new(StringArray::from(Vec::<&str>::new())),
            Arc::new(Int32Array::from(Vec::<i32>::new())),
        ])?;

        assert_eq!(acc.evaluate()?, ScalarValue::try_new_null(&map_type)?);
        Ok(())
    }

    #[test]
    fn map_agg_null_key_errors() -> Result<()> {
        let mut acc = map_agg_accumulator()?;
        acc.update_batch(&[
            Arc::new(StringArray::from(vec![Some("a"), None])),
            Arc::new(Int32Array::from(vec![Some(1), Some(2)])),
        ])?;

        let err = acc.evaluate().unwrap_err();
        assert!(
            err.to_string().contains("map key cannot be null"),
            "unexpected error: {err}"
        );
        Ok(())
    }

    #[test]
    fn map_agg_state_merge_roundtrip() -> Result<()> {
        let mut acc = map_agg_accumulator()?;
        acc.update_batch(&[
            Arc::new(StringArray::from(vec!["a", "b"])),
            Arc::new(Int32Array::from(vec![1, 2])),
        ])?;
        acc.update_batch(&[
            Arc::new(StringArray::from(vec!["c"])),
            Arc::new(Int32Array::from(vec![3])),
        ])?;

        let expected = acc.evaluate()?;
        let state = acc.state()?;
        assert_eq!(state.len(), 1);
        assert_eq!(state[0], expected);

        let state_arrays: Vec<ArrayRef> =
            state.iter().map(|s| s.to_array()).collect::<Result<_>>()?;
        let mut merged = map_agg_accumulator()?;
        merged.merge_batch(&state_arrays)?;

        // Merging state back in must not change the accumulated map
        assert_eq!(merged.evaluate()?, expected);
        Ok(())
    }

    #[test]
    fn map_agg_update_batch_rejects_mismatched_lengths() {
        let mut acc = map_agg_accumulator().unwrap();
        let err = acc
            .update_batch(&[
                Arc::new(StringArray::from(vec!["a", "b"])),
                Arc::new(Int32Array::from(vec![1])),
            ])
            .unwrap_err();
        assert!(
            err.to_string().contains("key/value length mismatch"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn map_agg_name() {
        assert_eq!(MapAgg::default().name(), "map_agg");
    }

    #[test]
    fn map_agg_return_type_builds_map() {
        let map_type = MapAgg::default()
            .return_type(&[DataType::Utf8, DataType::Int32])
            .unwrap();

        let DataType::Map(entries, false) = map_type else {
            panic!("expected Map, got {map_type:?}");
        };
        let DataType::Struct(fields) = entries.data_type() else {
            panic!("expected entries Struct, got {:?}", entries.data_type());
        };
        assert_eq!(fields.len(), 2);
        assert_eq!(fields[0].name(), "key");
        assert_eq!(fields[0].data_type(), &DataType::Utf8);
        assert!(!fields[0].is_nullable());
        assert_eq!(fields[1].name(), "value");
        assert_eq!(fields[1].data_type(), &DataType::Int32);
        assert!(fields[1].is_nullable());
    }

    #[test]
    fn map_agg_udaf_is_registered_singleton() {
        let udaf = map_agg_udaf();
        assert_eq!(udaf.name(), "map_agg");
        assert!(Arc::ptr_eq(&udaf, &map_agg_udaf()));
    }
}
