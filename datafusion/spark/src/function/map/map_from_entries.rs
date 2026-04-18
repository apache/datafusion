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

use std::sync::Arc;

use crate::function::map::utils::{
    MapKeyDedupPolicy, get_list_offsets, get_list_values,
    map_from_keys_values_offsets_nulls, map_type_from_key_value_types,
};
use arrow::array::{Array, ArrayRef, StructArray};
use arrow::datatypes::{DataType, Field, FieldRef};
use datafusion_common::utils::take_function_args;
use datafusion_common::{Result, exec_err, internal_err};
use datafusion_expr::{
    ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    Volatility,
};
use datafusion_functions::utils::make_scalar_function;

/// Spark-compatible `map_from_entries` expression
/// <https://spark.apache.org/docs/latest/api/sql/index.html#map_from_entries>
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct MapFromEntries {
    signature: Signature,
}

impl Default for MapFromEntries {
    fn default() -> Self {
        Self::new()
    }
}

impl MapFromEntries {
    pub fn new() -> Self {
        Self {
            signature: Signature::array(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for MapFromEntries {
    fn name(&self) -> &str {
        "map_from_entries"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        internal_err!("return_field_from_args should be used instead")
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let [entries_field] = args.arg_fields else {
            return exec_err!("map_from_entries: expected one argument");
        };

        let (entries_element_field, entries_element_type) =
            match entries_field.data_type() {
                DataType::List(field)
                | DataType::LargeList(field)
                | DataType::FixedSizeList(field, _) => {
                    Ok((field.as_ref(), field.data_type()))
                }
                wrong_type => exec_err!(
                    "map_from_entries: expected array<struct<key, value>>, got {:?}",
                    wrong_type
                ),
            }?;

        let (keys_type, values_type) = match entries_element_type {
            DataType::Struct(fields) if fields.len() == 2 => {
                Ok((fields[0].data_type(), fields[1].data_type()))
            }
            wrong_type => exec_err!(
                "map_from_entries: expected array<struct<key, value>>, got {:?}",
                wrong_type
            ),
        }?;

        let map_type = map_type_from_key_value_types(keys_type, values_type);
        let nullable = entries_field.is_nullable() || entries_element_field.is_nullable();

        Ok(Arc::new(Field::new(self.name(), map_type, nullable)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let dedup_policy = MapKeyDedupPolicy::from_config_str(
            &args.config_options.execution.map_key_dedup_policy,
        );
        make_scalar_function(
            move |arrays: &[ArrayRef]| map_from_entries_inner(arrays, dedup_policy),
            vec![],
        )(&args.args)
    }
}

fn map_from_entries_inner(
    args: &[ArrayRef],
    dedup_policy: MapKeyDedupPolicy,
) -> Result<ArrayRef> {
    let [entries] = take_function_args("map_from_entries", args)?;
    let entries_offsets = get_list_offsets(entries)?;
    let entries_values = get_list_values(entries)?;

    let (flat_keys, flat_values) =
        match entries_values.as_any().downcast_ref::<StructArray>() {
            Some(a) => Ok((a.column(0), a.column(1))),
            None => exec_err!(
                "map_from_entries: expected array<struct<key, value>>, got {:?}",
                entries_values.data_type()
            ),
        }?;

    // Spark throws on:
    //   * a null struct entry inside a non-null list row — Spark error class `NULL_MAP_KEY`
    //     (see `QueryExecutionErrors.nullAsMapKeyNotAllowedError`)
    //   * a null key inside a non-null struct entry — Spark error class `NULL_MAP_KEY`
    // A null outer list row is valid and propagates to a null output row.
    let outer_nulls = entries.nulls();
    let struct_nulls = entries_values.nulls();
    let key_nulls = flat_keys.nulls();

    if struct_nulls.is_some() || key_nulls.is_some() {
        let start = entries_offsets
            .first()
            .map(|offset| *offset as usize)
            .unwrap_or(0);
        let mut cur_offset = start;
        for (row_idx, next_offset) in entries_offsets.iter().skip(1).enumerate() {
            let next = *next_offset as usize;
            let row_is_null = outer_nulls.is_some_and(|n| n.is_null(row_idx));
            if !row_is_null {
                for i in cur_offset..next {
                    if struct_nulls.is_some_and(|n| n.is_null(i))
                        || key_nulls.is_some_and(|n| n.is_null(i))
                    {
                        return exec_err!("[NULL_MAP_KEY] Cannot use null as map key.");
                    }
                }
            }
            cur_offset = next;
        }
    }

    map_from_keys_values_offsets_nulls(
        flat_keys,
        flat_values,
        &entries_offsets,
        &entries_offsets,
        None,
        outer_nulls,
        dedup_policy,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{
        Int32Array, Int32Builder, ListArray, MapArray, StringArray, StringBuilder,
        StructArray,
    };
    use arrow::buffer::{NullBuffer, OffsetBuffer};
    use arrow::datatypes::Fields;

    fn make_entries_field(array_nullable: bool, element_nullable: bool) -> FieldRef {
        let struct_type = DataType::Struct(Fields::from(vec![
            Field::new("key", DataType::Int32, false),
            Field::new("value", DataType::Utf8, true),
        ]));
        Arc::new(Field::new(
            "entries",
            DataType::List(Arc::new(Field::new("item", struct_type, element_nullable))),
            array_nullable,
        ))
    }

    #[test]
    fn test_map_from_entries_nullability_matches_input() {
        let func = MapFromEntries::new();
        let expected_type =
            map_type_from_key_value_types(&DataType::Int32, &DataType::Utf8);

        // Non-nullable array and elements => non-nullable result
        let non_nullable_field = make_entries_field(false, false);
        let result = func
            .return_field_from_args(ReturnFieldArgs {
                arg_fields: &[Arc::clone(&non_nullable_field)],
                scalar_arguments: &[None],
            })
            .expect("should infer field");
        assert!(!result.is_nullable());
        assert_eq!(result.data_type(), &expected_type);

        // Nullable elements should make result nullable even if array is non-nullable
        let element_nullable_field = make_entries_field(false, true);
        let result = func
            .return_field_from_args(ReturnFieldArgs {
                arg_fields: &[Arc::clone(&element_nullable_field)],
                scalar_arguments: &[None],
            })
            .expect("should infer field");
        assert!(result.is_nullable());
        assert_eq!(result.data_type(), &expected_type);

        // Nullable array should also yield nullable result
        let array_nullable_field = make_entries_field(true, false);
        let result = func
            .return_field_from_args(ReturnFieldArgs {
                arg_fields: &[Arc::clone(&array_nullable_field)],
                scalar_arguments: &[None],
            })
            .expect("should infer field");
        assert!(result.is_nullable());
        assert_eq!(result.data_type(), &expected_type);
    }

    fn struct_fields() -> Fields {
        Fields::from(vec![
            Field::new("key", DataType::Int32, false),
            Field::new("value", DataType::Utf8, true),
        ])
    }

    type TestRow<'a> = Option<Vec<(i32, Option<&'a str>)>>;

    /// Build a `List<Struct<key: Int32 not null, value: Utf8>>` from per-row entries.
    /// `rows` is a list of rows; `None` means the outer list row is null; a row is a vector
    /// of `(key, value)` pairs where `key` is always present and `value` may be `None`.
    fn build_list(rows: Vec<TestRow>) -> ArrayRef {
        let fields = struct_fields();
        let mut key_builder = Int32Builder::new();
        let mut val_builder = StringBuilder::new();
        let mut offsets: Vec<i32> = vec![0];
        let mut nulls = vec![];
        let mut cur: i32 = 0;
        for row in rows {
            match row {
                Some(entries) => {
                    for (k, v) in entries {
                        key_builder.append_value(k);
                        match v {
                            Some(s) => val_builder.append_value(s),
                            None => val_builder.append_null(),
                        }
                        cur += 1;
                    }
                    nulls.push(true);
                }
                None => nulls.push(false),
            }
            offsets.push(cur);
        }
        let keys: ArrayRef = Arc::new(key_builder.finish());
        let values: ArrayRef = Arc::new(val_builder.finish());
        let entries = StructArray::try_new(fields.clone(), vec![keys, values], None)
            .expect("struct array");
        let list_field = Arc::new(Field::new("item", DataType::Struct(fields), false));
        let list = ListArray::try_new(
            list_field,
            OffsetBuffer::new(offsets.into()),
            Arc::new(entries),
            Some(NullBuffer::from(nulls)),
        )
        .expect("list array");
        Arc::new(list)
    }

    #[test]
    fn test_map_from_entries_happy_path() {
        let input = build_list(vec![
            Some(vec![(1, Some("a")), (2, Some("b"))]),
            Some(vec![]),
            None,
        ]);
        let out = map_from_entries_inner(&[input], MapKeyDedupPolicy::Exception).unwrap();
        let map = out.as_any().downcast_ref::<MapArray>().unwrap();
        assert_eq!(map.len(), 3);
        assert!(!map.is_null(0));
        assert!(!map.is_null(1));
        assert!(map.is_null(2));
        let row0 = map.value(0);
        let row0 = row0.as_any().downcast_ref::<StructArray>().unwrap();
        let keys = row0
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let values = row0
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(keys.values(), &[1, 2]);
        assert_eq!(values.value(0), "a");
        assert_eq!(values.value(1), "b");
    }

    #[test]
    fn test_map_from_entries_duplicate_keys_exception() {
        let input = build_list(vec![Some(vec![(1, Some("a")), (1, Some("b"))])]);
        let err = map_from_entries_inner(&[input], MapKeyDedupPolicy::Exception)
            .expect_err("should error on duplicate key under Exception policy");
        assert!(
            err.to_string().contains("[DUPLICATED_MAP_KEY]"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_map_from_entries_duplicate_keys_last_win() {
        let input = build_list(vec![Some(vec![(1, Some("a")), (1, Some("b"))])]);
        let out = map_from_entries_inner(&[input], MapKeyDedupPolicy::LastWin).unwrap();
        let map = out.as_any().downcast_ref::<MapArray>().unwrap();
        assert_eq!(map.len(), 1);
        let row0 = map.value(0);
        let row0 = row0.as_any().downcast_ref::<StructArray>().unwrap();
        let keys = row0
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let values = row0
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(keys.len(), 1);
        assert_eq!(keys.value(0), 1);
        assert_eq!(values.value(0), "b");
    }

    #[test]
    fn test_map_from_entries_null_struct_entry_throws() {
        // Build List<Struct> where the struct element has a null at position 1
        // inside a non-null list row.
        let fields = struct_fields();
        let keys: ArrayRef = Arc::new(Int32Array::from(vec![1, 0]));
        let values: ArrayRef = Arc::new(StringArray::from(vec![Some("a"), Some("x")]));
        let struct_nulls = NullBuffer::from(vec![true, false]);
        let entries =
            StructArray::try_new(fields.clone(), vec![keys, values], Some(struct_nulls))
                .unwrap();
        let list_field = Arc::new(Field::new("item", DataType::Struct(fields), true));
        let list = ListArray::try_new(
            list_field,
            OffsetBuffer::new(vec![0, 2].into()),
            Arc::new(entries),
            None,
        )
        .unwrap();
        let input: ArrayRef = Arc::new(list);
        let err = map_from_entries_inner(&[input], MapKeyDedupPolicy::Exception)
            .expect_err("should error on null struct entry");
        assert!(
            err.to_string().contains("[NULL_MAP_KEY]"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_map_from_entries_null_key_throws() {
        // Build List<Struct> where the struct itself is non-null but the key column has a null.
        let fields = Fields::from(vec![
            Field::new("key", DataType::Int32, true),
            Field::new("value", DataType::Utf8, true),
        ]);
        let keys: ArrayRef = Arc::new(Int32Array::from(vec![Some(1), None]));
        let values: ArrayRef = Arc::new(StringArray::from(vec![Some("a"), Some("b")]));
        let entries =
            StructArray::try_new(fields.clone(), vec![keys, values], None).unwrap();
        let list_field = Arc::new(Field::new("item", DataType::Struct(fields), false));
        let list = ListArray::try_new(
            list_field,
            OffsetBuffer::new(vec![0, 2].into()),
            Arc::new(entries),
            None,
        )
        .unwrap();
        let input: ArrayRef = Arc::new(list);
        let err = map_from_entries_inner(&[input], MapKeyDedupPolicy::Exception)
            .expect_err("should error on null key");
        assert!(
            err.to_string().contains("[NULL_MAP_KEY]"),
            "unexpected error: {err}"
        );
    }
}
