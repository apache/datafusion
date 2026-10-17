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
//!
//! # Accumulators
//!
//! Two accumulators implement the function, selected in
//! [`MapAgg::accumulator`] by whether the aggregate carries an `ORDER BY`:
//!
//! * [`MapAggAccumulator`] keeps the pairs in input order and serializes its
//!   state as a single `Map` column.
//! * [`OrderSensitiveMapAggAccumulator`] additionally records the values of the
//!   ordering expressions for every pair. Its state therefore has two columns,
//!   the `Map` plus a `List<Struct<ordering...>>` that lets partial states from
//!   every partition be merged on the ordering by `merge_batch`, so the final
//!   entry order stays deterministic under parallelism.
//!
//! [`MapAgg::order_sensitivity`] is [`AggregateOrderSensitivity::SoftRequirement`],
//! so when a plan's input already satisfies the ordering the accumulator is
//! marked through [`AggregateUDFImpl::with_beneficial_ordering`] and the pairs
//! are not sorted again.
//!
//! # De-duplication
//!
//! Arrow maps hold at most one value per key, so duplicate keys are removed
//! when the accumulator evaluates. The *first* pair wins, where "first" means
//! first in input order for [`MapAggAccumulator`] and first in the sorted
//! order for [`OrderSensitiveMapAggAccumulator`].
//!
//! # Nulls
//!
//! Arrow maps cannot represent `NULL` keys: [`MapAggAccumulator`] fails with
//! `map key cannot be null` when one is evaluated, while
//! [`OrderSensitiveMapAggAccumulator`] drops such pairs as it collects input.
//! `NULL` values are ordinary map values and are kept. An empty group
//! evaluates to a `NULL` map, matching `array_agg`.

use std::collections::{HashSet, VecDeque};
use std::mem::{size_of, size_of_val, take};
use std::sync::Arc;

use arrow::array::{Array, ArrayRef, AsArray, MapArray, StructArray};
use arrow::buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow::compute::SortOptions;
use arrow::datatypes::{DataType, Field, FieldRef, Fields};

use datafusion_common::cast::as_map_array;
use datafusion_common::utils::{
    SingleRowListArrayBuilder, compare_rows, get_row_at_idx, take_function_args,
};
use datafusion_common::{
    Result, ScalarValue, assert_eq_or_internal_err, exec_err, internal_err,
};
use datafusion_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion_expr::utils::format_state_name;
use datafusion_expr::{
    Accumulator, AggregateUDFImpl, Documentation, Signature, Volatility,
};
use datafusion_functions_aggregate_common::merge_arrays::merge_ordered_arrays;
use datafusion_functions_aggregate_common::order::AggregateOrderSensitivity;
use datafusion_functions_aggregate_common::utils::ordering_fields;
use datafusion_macros::user_doc;
use datafusion_physical_expr_common::sort_expr::LexOrdering;

use crate::utils::{map_row_to_scalars, struct_to_rows};

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
For each row, the key expression becomes a map key and the value expression becomes the corresponding map value. \
Entries appear in input order, or in the order given by the optional `ORDER BY`. \
When a key repeats, only the first entry for that key is kept.",
    syntax_example = "map_agg(key, value [ORDER BY expression])",
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
    /// Accepts two arguments of any type: the key and the value.
    signature: Signature,
    /// Whether the input is known to arrive already ordered by the `ORDER BY`
    /// inside the aggregate.
    is_input_pre_ordered: bool,
}

impl Default for MapAgg {
    fn default() -> Self {
        Self {
            signature: Signature::any(2, Volatility::Immutable),
            is_input_pre_ordered: false,
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

        let mut fields = vec![
            Field::new(
                format_state_name(args.name, "map_agg"),
                // Nullable so empty groups can produce a NULL map
                map_type,
                true,
            )
            .into(),
        ];

        if args.ordering_fields.is_empty() {
            return Ok(fields);
        }

        let orderings = args.ordering_fields.to_vec();
        fields.push(
            Field::new_list(
                format_state_name(args.name, "map_agg_orderings"),
                Field::new_list_field(DataType::Struct(Fields::from(orderings)), true),
                false,
            )
            .into(),
        );

        Ok(fields)
    }

    fn accumulator(&self, acc_args: AccumulatorArgs) -> Result<Box<dyn Accumulator>> {
        let [key_field, value_field] =
            take_function_args(self.name(), acc_args.expr_fields)?;
        let key_type = key_field.data_type().clone();
        let value_type = value_field.data_type().clone();

        let Some(ordering) = LexOrdering::new(acc_args.order_bys.to_vec()) else {
            return MapAggAccumulator::try_new(key_type, value_type)
                .map(|acc| Box::new(acc) as _);
        };

        let ordering_dtypes = ordering
            .iter()
            .map(|e| e.expr.data_type(acc_args.schema))
            .collect::<Result<Vec<_>>>()?;

        Ok(Box::new(OrderSensitiveMapAggAccumulator::new(
            key_type,
            value_type,
            ordering_dtypes,
            ordering,
            self.is_input_pre_ordered,
        )))
    }

    fn order_sensitivity(&self) -> AggregateOrderSensitivity {
        AggregateOrderSensitivity::SoftRequirement
    }

    fn with_beneficial_ordering(
        self: Arc<Self>,
        beneficial_ordering: bool,
    ) -> Result<Option<Arc<dyn AggregateUDFImpl>>> {
        Ok(Some(Arc::new(Self {
            signature: self.signature.clone(),
            is_input_pre_ordered: beneficial_ordering,
        })))
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

/// Accumulates key/value pairs for [`MapAgg`].
///
/// Input rows are kept as parallel [`ScalarValue`] lists and concatenated into
/// the map in [`Self::evaluate`]; partial states are single-row [`MapArray`]
/// scalars that are split back into rows in [`Self::merge_batch`].
#[derive(Debug)]
pub struct MapAggAccumulator {
    /// Data type of the map keys.
    key_type: DataType,
    /// Data type of the map values.
    value_type: DataType,
    /// Input keys in arrival order, parallel to `values`.
    keys: Vec<ScalarValue>,
    /// Input values in arrival order, parallel to `keys`.
    values: Vec<ScalarValue>,
}

impl MapAggAccumulator {
    /// Create a new map_agg accumulator for the given key and value types
    pub fn try_new(key_type: DataType, value_type: DataType) -> Result<Self> {
        Ok(Self {
            key_type,
            value_type,
            keys: vec![],
            values: vec![],
        })
    }
    /// Keeps only the first occurrence of each key, preserving input order.
    ///
    /// Returns the surviving keys and values as two aligned vectors.
    #[allow(clippy::allow_attributes, clippy::mutable_key_type)] // ScalarValue has interior mutability but is intentionally used as hash key
    fn dedup_first_wins(
        keys: Vec<ScalarValue>,
        values: Vec<ScalarValue>,
    ) -> (Vec<ScalarValue>, Vec<ScalarValue>) {
        // First pass: mark each position that is the first occurrence of its key.
        let mut seen = HashSet::with_capacity(keys.len());
        let keep: Vec<bool> = keys.iter().map(|k| seen.insert(k.clone())).collect();

        // Second pass: keep only the first-occurrence positions.
        let out_keys = keys
            .into_iter()
            .zip(&keep)
            .filter_map(|(k, &keep)| keep.then_some(k))
            .collect();
        let out_values = values
            .into_iter()
            .zip(&keep)
            .filter_map(|(v, &keep)| keep.then_some(v))
            .collect();
        (out_keys, out_values)
    }
    /// The `DataType::Map` this accumulator evaluates to
    fn map_type(&self) -> DataType {
        MapAgg::map_data_type(&self.key_type, &self.value_type)
    }
}

impl Accumulator for MapAggAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        assert_eq_or_internal_err!(values.len(), 2, "map_agg expects (key, value)");

        let keys = &values[0];
        let vals = &values[1];
        assert_eq_or_internal_err!(keys.len(), vals.len(), "key/value length mismatch");

        for row in 0..keys.len() {
            self.keys
                .push(ScalarValue::try_from_array(keys, row)?.compacted());
            self.values
                .push(ScalarValue::try_from_array(vals, row)?.compacted());
        }

        Ok(())
    }
    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        if states.is_empty() {
            return Ok(());
        }

        assert_eq_or_internal_err!(states.len(), 1, "expects single state");

        let map_arr = as_map_array(states[0].as_ref())?;
        let offsets = map_arr.value_offsets();
        let map_keys = map_arr.keys();
        let map_values = map_arr.values();

        for row in 0..map_arr.len() {
            // Partial states are nullable so empty groups can produce a NULL map
            if map_arr.is_null(row) {
                continue;
            }
            for idx in offsets[row] as usize..offsets[row + 1] as usize {
                self.keys
                    .push(ScalarValue::try_from_array(map_keys, idx)?.compacted());
                self.values
                    .push(ScalarValue::try_from_array(map_values, idx)?.compacted());
            }
        }

        Ok(())
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![self.evaluate()?])
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        let map_type = self.map_type();

        if self.keys.is_empty() {
            return ScalarValue::try_new_null(&map_type);
        }

        // Arrow maps cannot represent null keys (same rule as the `map` function)
        if self.keys.iter().any(ScalarValue::is_null) {
            return exec_err!("map key cannot be null");
        }

        let (keys, values) =
            Self::dedup_first_wins(self.keys.clone(), self.values.clone());

        let keys = ScalarValue::iter_to_array(keys)?;
        let values = ScalarValue::iter_to_array(values)?;

        let DataType::Map(entries_field, ordered) = &map_type else {
            return internal_err!("map_agg expected Map type, got {map_type}");
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
        // ScalarValue::size already counts the enum itself, so spare capacity is
        // accounted for separately to avoid double counting the held values
        size_of_val(self)
            + (size_of::<ScalarValue>() * (self.keys.capacity() + self.values.capacity()))
            + self
                .keys
                .iter()
                .chain(self.values.iter())
                .map(|s| s.size() - size_of::<ScalarValue>())
                .sum::<usize>()
    }
}

/// Accumulator used when `map_agg` has an `ORDER BY`. Stores the ordering column
/// values alongside each pair so the input can be globally sorted (across
/// partitions).
#[derive(Debug)]
pub struct OrderSensitiveMapAggAccumulator {
    /// Data type of the map keys.
    key_type: DataType,
    /// Data type of the map values.
    value_type: DataType,
    keys: Vec<ScalarValue>,
    values: Vec<ScalarValue>,
    /// Ordering-expression values for each pair, parallel to `keys`/`values`.
    ordering_values: Vec<Vec<ScalarValue>>,
    /// Data types of the ordering expressions, used to build the state field.
    ordering_dtypes: Vec<DataType>,
    /// The `ORDER BY` requirement inside the aggregate.
    ordering_req: LexOrdering,
    /// When true the input already satisfies `ordering_req`, so the pairs do
    /// not have to be sorted again before de-duplication.
    is_input_pre_ordered: bool,
}

impl OrderSensitiveMapAggAccumulator {
    /// Create a new accumulator for `map_agg(key, value ORDER BY ...)`.
    ///
    /// `ordering_dtypes` must hold one data type per entry of `ordering_req`.
    pub fn new(
        key_type: DataType,
        value_type: DataType,
        ordering_dtypes: Vec<DataType>,
        ordering_req: LexOrdering,
        is_input_pre_ordered: bool,
    ) -> Self {
        Self {
            key_type,
            value_type,
            keys: Vec::new(),
            values: Vec::new(),
            ordering_values: Vec::new(),
            ordering_dtypes,
            ordering_req,
            is_input_pre_ordered,
        }
    }

    /// Sort options of each ordering expression, used when sorting the pairs
    /// and when merging partial states.
    fn sort_options(&self) -> Vec<SortOptions> {
        self.ordering_req.iter().map(|s| s.options).collect()
    }

    /// Sorts the accumulated pairs by their ordering values, then applies
    /// first-wins de-duplication. Returns the surviving keys, values, and
    /// ordering values, all aligned so they describe the same rows.
    #[allow(clippy::allow_attributes, clippy::mutable_key_type)] // ScalarValue has interior mutability but is intentionally used as hash key
    fn sorted_deduped(&self) -> Result<OrderSensitiveMapAggRows> {
        let mut rows: Vec<usize> = (0..self.keys.len()).collect();

        if !self.is_input_pre_ordered {
            let sort_options = self.sort_options();
            let mut cmp_err = Ok(());
            rows.sort_by(|&a, &b| {
                compare_rows(
                    &self.ordering_values[a],
                    &self.ordering_values[b],
                    &sort_options,
                )
                .unwrap_or_else(|e| {
                    cmp_err = Err(e);
                    std::cmp::Ordering::Equal
                })
            });
            cmp_err?;
        }

        // Keep the first occurrence of each key in sorted order, and project the
        // keys, values, and ordering values through the same surviving indices
        // so all three stay aligned.
        let mut seen = HashSet::with_capacity(rows.len());
        let mut keys = Vec::new();
        let mut values = Vec::new();
        let mut ordering_values = Vec::new();
        for &i in &rows {
            if seen.insert(&self.keys[i]) {
                keys.push(self.keys[i].clone());
                values.push(self.values[i].clone());
                ordering_values.push(self.ordering_values[i].clone());
            }
        }

        Ok(OrderSensitiveMapAggRows {
            keys,
            values,
            ordering_values,
        })
    }

    /// Builds the `List<Struct<ordering...>>` state column from the given
    /// ordering values. These must be the de-duplicated ordering values that
    /// align with the map state, so both pieces of state describe the same rows.
    fn evaluate_orderings(
        &self,
        ordering_values: &[Vec<ScalarValue>],
    ) -> Result<ScalarValue> {
        let fields = ordering_fields(&self.ordering_req, &self.ordering_dtypes);
        let struct_field = Fields::from(fields.clone());

        let mut column_wise: Vec<ArrayRef> = Vec::with_capacity(fields.len());
        for (col_idx, field) in fields.iter().enumerate() {
            if ordering_values.is_empty() {
                column_wise.push(arrow::array::new_empty_array(field.data_type()));
            } else {
                let col_vals = ordering_values.iter().map(|row| row[col_idx].clone());
                column_wise.push(ScalarValue::iter_to_array(col_vals)?);
            }
        }

        let struct_array = StructArray::try_new(struct_field, column_wise, None)?;
        Ok(SingleRowListArrayBuilder::new(Arc::new(struct_array)).build_list_scalar())
    }
}

/// Keys, values, and ordering values of the rows that survive sorting and
/// first-wins de-duplication. The three vectors are parallel.
#[derive(Debug)]
struct OrderSensitiveMapAggRows {
    keys: Vec<ScalarValue>,
    values: Vec<ScalarValue>,
    ordering_values: Vec<Vec<ScalarValue>>,
}

impl Accumulator for OrderSensitiveMapAggAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        if values.len() < 2 {
            return exec_err!("map_agg expects at least 2 columns, got {}", values.len());
        }
        let keys = &values[0];
        let vals = &values[1];
        let ordering_cols = &values[2..];

        for i in 0..keys.len() {
            // NULL keys cannot exist in a map; skip the whole pair.
            if keys.is_null(i) {
                continue;
            }
            self.keys
                .push(ScalarValue::try_from_array(keys, i)?.compacted());
            self.values
                .push(ScalarValue::try_from_array(vals, i)?.compacted());
            self.ordering_values.push(
                get_row_at_idx(ordering_cols, i)?
                    .into_iter()
                    .map(|v| v.compacted())
                    .collect(),
            );
        }
        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        if states.is_empty() {
            return Ok(());
        }
        if states.len() != 2 {
            return exec_err!(
                "map_agg ordered merge expects 2 state columns, got {}",
                states.len()
            );
        }

        let map_array = states[0].as_map();
        let orderings = states[1].as_list::<i32>();

        // Each partition contributes one map row plus a parallel list of
        // ordering-value structs. Collect them, then merge by ordering.
        let mut partition_keys: Vec<VecDeque<ScalarValue>> =
            vec![take(&mut self.keys).into()];
        let mut partition_orderings: Vec<VecDeque<Vec<ScalarValue>>> =
            vec![take(&mut self.ordering_values).into()];
        let mut partition_values: Vec<VecDeque<ScalarValue>> =
            vec![take(&mut self.values).into()];

        // Push keys and values from each partition's state into the merge buffers.
        for row in 0..map_array.len() {
            if map_array.is_null(row) {
                continue;
            }
            let (keys, values) = map_row_to_scalars(map_array, row)?;
            let ord_vals = struct_to_rows(orderings.value(row).as_struct())?;

            partition_keys.push(keys.into());
            partition_values.push(values.into());
            partition_orderings.push(ord_vals.into());
        }

        // Merge keys and values along the ordering. `merge_ordered_arrays`
        // merges a single value stream; run it once for keys and once for
        // values using the same ordering inputs so they stay aligned.
        let sort_options = self.sort_options();

        let (merged_keys, merged_orderings) = merge_ordered_arrays(
            &mut partition_keys,
            &mut partition_orderings.clone(),
            &sort_options,
        )?;
        let (merged_values, _) = merge_ordered_arrays(
            &mut partition_values,
            &mut partition_orderings,
            &sort_options,
        )?;

        self.keys = merged_keys;
        self.values = merged_values;
        self.ordering_values = merged_orderings;
        Ok(())
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        let rows = self.sorted_deduped()?;
        let orderings = self.evaluate_orderings(&rows.ordering_values)?;
        let map_array =
            build_single_map(rows.keys, rows.values, &self.key_type, &self.value_type)?;
        Ok(vec![ScalarValue::try_from_array(&map_array, 0)?, orderings])
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        let rows = self.sorted_deduped()?;
        let map_array =
            build_single_map(rows.keys, rows.values, &self.key_type, &self.value_type)?;
        ScalarValue::try_from_array(&map_array, 0)
    }

    fn size(&self) -> usize {
        let mut total = size_of_val(self) + ScalarValue::size_of_vec(&self.keys)
            - size_of_val(&self.keys)
            + ScalarValue::size_of_vec(&self.values)
            - size_of_val(&self.values)
            + self.key_type.size()
            - size_of_val(&self.key_type)
            + self.value_type.size()
            - size_of_val(&self.value_type);

        // ordering_values: Vec spine plus the heap owned by each row's scalars.
        total += size_of::<Vec<ScalarValue>>() * self.ordering_values.capacity();
        for row in &self.ordering_values {
            total += ScalarValue::size_of_vec(row) - size_of_val(row);
        }

        // ordering_dtypes: Vec spine plus the heap owned by each DataType.
        total += size_of::<DataType>() * self.ordering_dtypes.capacity();
        for dtype in &self.ordering_dtypes {
            total += dtype.size() - size_of_val(dtype);
        }

        total
    }
}

/// Builds a single-row `Map` array holding `keys` and `values`.
///
/// An empty input yields a one-row array that is NULL, so empty groups
/// evaluate to a NULL map just like [`MapAggAccumulator`] does.
fn build_single_map(
    keys: Vec<ScalarValue>,
    values: Vec<ScalarValue>,
    key_type: &DataType,
    value_type: &DataType,
) -> Result<ArrayRef> {
    let map_type = MapAgg::map_data_type(key_type, value_type);
    let DataType::Map(entries_field, ordered) = &map_type else {
        return internal_err!("map_agg expected Map type, got {map_type}");
    };
    let DataType::Struct(entry_fields) = entries_field.data_type() else {
        return internal_err!(
            "map_agg expected struct entries, got {}",
            entries_field.data_type()
        );
    };

    let (keys, vals) = if keys.is_empty() {
        (
            arrow::array::new_empty_array(entry_fields[0].data_type()),
            arrow::array::new_empty_array(entry_fields[1].data_type()),
        )
    } else {
        (
            ScalarValue::iter_to_array(keys)?,
            ScalarValue::iter_to_array(values)?,
        )
    };

    let entries = StructArray::try_new(entry_fields.clone(), vec![keys, vals], None)?;

    let Ok(num_entries) = i32::try_from(entries.len()) else {
        return internal_err!(
            "map_agg produced {} entries which exceeds the map offset range",
            entries.len()
        );
    };
    let offsets = OffsetBuffer::new(ScalarBuffer::from(vec![0_i32, num_entries]));
    let nulls = entries.is_empty().then(|| NullBuffer::from(vec![false]));

    Ok(Arc::new(MapArray::try_new(
        Arc::clone(entries_field),
        offsets,
        entries,
        nulls,
        *ordered,
    )?))
}

#[cfg(test)]
mod tests {
    use super::*;

    use arrow::array::{Int32Array, StringArray};

    fn map_agg_accumulator() -> Result<MapAggAccumulator> {
        MapAggAccumulator::try_new(DataType::Utf8, DataType::Int32)
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
    fn duplicate_key_handling() -> Result<()> {
        let mut acc = map_agg_accumulator()?;
        acc.update_batch(&[
            Arc::new(StringArray::from(vec!["a", "b"])),
            Arc::new(Int32Array::from(vec![Some(1), Some(23)])),
        ])?;
        acc.update_batch(&[
            Arc::new(StringArray::from(vec!["a"])),
            Arc::new(Int32Array::from(vec![Some(3)])),
        ])?;

        let ScalarValue::Map(map) = acc.evaluate()? else {
            panic!("expected map scalar");
        };
        // De-duplication is first-wins: the later `a` pair is dropped and the
        // surviving entries keep their input order.
        assert_eq!(map.len(), 1);
        assert!(map.is_valid(0));
        assert_eq!(map.value_offsets(), &[0, 2]);

        let map_keys = map.keys().as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(map_keys, &StringArray::from(vec!["a", "b"]));

        let map_values = map.values().as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(map_values, &Int32Array::from(vec![Some(1), Some(23)]));

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

    /// Accumulator ordered by an `Int32` `ord` column (ascending, nulls first).
    fn ordered_accumulator(
        is_input_pre_ordered: bool,
    ) -> Result<OrderSensitiveMapAggAccumulator> {
        use arrow::datatypes::Schema;
        use datafusion_physical_expr::expressions::Column;
        use datafusion_physical_expr_common::physical_expr::PhysicalExpr;
        use datafusion_physical_expr_common::sort_expr::PhysicalSortExpr;

        let schema = Schema::new(vec![
            Field::new("key", DataType::Utf8, true),
            Field::new("value", DataType::Int32, true),
            Field::new("ord", DataType::Int32, true),
        ]);
        let ord_expr = Arc::new(
            Column::new_with_schema("ord", &schema).expect("column not in schema"),
        ) as Arc<dyn PhysicalExpr>;
        let ordering = LexOrdering::new(vec![PhysicalSortExpr::new(
            ord_expr,
            SortOptions::new(false, false),
        )])
        .unwrap();

        Ok(OrderSensitiveMapAggAccumulator::new(
            DataType::Utf8,
            DataType::Int32,
            vec![DataType::Int32],
            ordering,
            is_input_pre_ordered,
        ))
    }

    /// `(key, value, ord)` input batch for [`ordered_accumulator`].
    fn ordered_batch(keys: &[&str], values: &[i32], ord: &[i32]) -> Vec<ArrayRef> {
        vec![
            Arc::new(StringArray::from(keys.to_vec())),
            Arc::new(Int32Array::from(values.to_vec())),
            Arc::new(Int32Array::from(ord.to_vec())),
        ]
    }

    #[test]
    fn ordered_map_agg_sorts_and_keeps_first_pair_per_key() -> Result<()> {
        let mut acc = ordered_accumulator(false)?;
        acc.update_batch(&ordered_batch(&["a", "b", "a"], &[10, 20, 30], &[3, 1, 2]))?;

        // State carries the map plus the parallel ordering column.
        assert_eq!(acc.state()?.len(), 2);

        let ScalarValue::Map(map) = acc.evaluate()? else {
            panic!("expected map scalar");
        };
        // Ordered by `ord`, so `b` (ord 1) comes first; of the two `a` pairs the
        // one with the smaller `ord` wins the de-duplication.
        assert_eq!(map.value_offsets(), &[0, 2]);
        let map_keys = map.keys().as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(map_keys, &StringArray::from(vec!["b", "a"]));
        let map_values = map.values().as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(map_values, &Int32Array::from(vec![20, 30]));

        Ok(())
    }

    #[test]
    fn ordered_map_agg_merges_partition_states_in_ordering() -> Result<()> {
        let mut lhs = ordered_accumulator(false)?;
        lhs.update_batch(&ordered_batch(&["a", "c"], &[1, 3], &[1, 3]))?;

        let mut rhs = ordered_accumulator(false)?;
        rhs.update_batch(&ordered_batch(&["b", "a"], &[2, 4], &[2, 4]))?;
        let state = rhs
            .state()?
            .iter()
            .map(ScalarValue::to_array)
            .collect::<Result<Vec<_>>>()?;
        lhs.merge_batch(&state)?;

        let ScalarValue::Map(map) = lhs.evaluate()? else {
            panic!("expected map scalar");
        };
        // Both partitions interleave on `ord`: a(1), b(2), c(3), a(4) -- and
        // the trailing `a` loses the de-duplication to the earlier one.
        assert_eq!(map.value_offsets(), &[0, 3]);
        let map_keys = map.keys().as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(map_keys, &StringArray::from(vec!["a", "b", "c"]));
        let map_values = map.values().as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(map_values, &Int32Array::from(vec![1, 2, 3]));

        Ok(())
    }
}
