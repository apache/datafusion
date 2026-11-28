// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

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
//!   every partition be sorted together at output, so the final
//!   entry order stays deterministic under parallelism.
//!
//! [`MapAgg::order_sensitivity`] is [`AggregateOrderSensitivity::SoftRequirement`],
//! so when a plan's input already satisfies the ordering the accumulator is
//! marked through [`AggregateUDFImpl::with_beneficial_ordering`] and the pairs
//! are not sorted again unless partial states are merged.
//!
//! # De-duplication
//!
//! Duplicate keys are removed during unordered updates or after sorting ordered
//! input. Without `ORDER BY`, neither entry order nor the duplicate winner is
//! guaranteed. With `ORDER BY`, the first pair in the requested ordering wins;
//! ties require additional ordering expressions for deterministic selection.
//!
//! # Nulls
//!
//! Both accumulators reject `NULL` keys in `update_batch` with
//! `map key cannot be null`.
//! `NULL` values are ordinary map values and are kept. An empty group
//! evaluates to a `NULL` map, matching `array_agg`.

use std::collections::HashSet;
use std::mem::{size_of, size_of_val};
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, AsArray, MapArray, StructArray, UInt64Array, make_array,
    new_empty_array,
};
use arrow::buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow::compute::{SortOptions, cast, concat, take};
use arrow::datatypes::{DataType, Field, FieldRef, Fields};
use arrow::row::{RowConverter, SortField};

use crate::array_agg::compact_payload;
use datafusion_common::cast::as_map_array;
use datafusion_common::scalar::copy_array_data;
use datafusion_common::utils::{SingleRowListArrayBuilder, take_function_args};
use datafusion_common::{
    Result, ScalarValue, assert_eq_or_internal_err, exec_err, internal_err, not_impl_err,
};
use datafusion_expr::function::{AccumulatorArgs, StateFieldsArgs};
use datafusion_expr::utils::format_state_name;
use datafusion_expr::{
    Accumulator, AggregateUDFImpl, Documentation, Signature, Volatility,
};
use datafusion_functions_aggregate_common::order::AggregateOrderSensitivity;
use datafusion_functions_aggregate_common::utils::ordering_fields;
use datafusion_macros::user_doc;
use datafusion_physical_expr_common::sort_expr::LexOrdering;
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
Without `ORDER BY`, neither entry order nor the value retained for a duplicate key is guaranteed. \
With `ORDER BY`, the first entry for each key in the requested ordering is kept; ties are not deterministic. \
NULL keys cause an error, NULL values are retained, and empty input returns NULL. \
DISTINCT is not supported.",
    syntax_example = "map_agg(key, value [ORDER BY expression])",
    sql_example = r#"```sql
> SELECT map_agg(k, v ORDER BY k) AS m FROM (VALUES ('a', 1)) t(k, v);
+--------+
| m      |
+--------+
| {a: 1} |
+--------+
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
        if acc_args.is_distinct {
            return not_impl_err!("map_agg does not support DISTINCT");
        }
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
            acc_args.is_reversed,
        )))
    }

    fn order_sensitivity(&self) -> AggregateOrderSensitivity {
        AggregateOrderSensitivity::SoftRequirement
    }

    fn reverse_expr(&self) -> datafusion_expr::ReversedUDAF {
        // Not Supported because order matters for aggregate first-wins semantics for duplicate keys
        datafusion_expr::ReversedUDAF::NotSupported
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
/// Only the first pair for each key is retained, in compact array batches.
#[derive(Debug)]
pub struct MapAggAccumulator {
    /// Data type of the map keys.
    key_type: DataType,
    /// Data type of the map values.
    value_type: DataType,
    batches: Vec<Vec<ArrayRef>>,
    seen: HashSet<ScalarValue>,
}

impl MapAggAccumulator {
    /// Create a new map_agg accumulator for the given key and value types
    pub fn try_new(key_type: DataType, value_type: DataType) -> Result<Self> {
        Ok(Self {
            key_type,
            value_type,
            batches: vec![],
            seen: HashSet::new(),
        })
    }
}

impl Accumulator for MapAggAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        assert_eq_or_internal_err!(values.len(), 2, "map_agg expects (key, value)");

        let keys = cast(&values[0], &self.key_type)?;
        let vals = cast(&values[1], &self.value_type)?;

        assert_eq_or_internal_err!(keys.len(), vals.len(), "key/value length mismatch");
        if keys.logical_null_count() != 0 {
            return exec_err!("map key cannot be null");
        }
        let mut indices = Vec::new();
        for row in 0..keys.len() {
            if self
                .seen
                .insert(ScalarValue::try_from_array(&keys, row)?.compacted())
            {
                indices.push(row as u64);
            }
        }
        if !indices.is_empty() {
            let indices = UInt64Array::from(indices);
            self.batches.push(
                [keys, vals]
                    .into_iter()
                    .map(|array| compact_array(take(array.as_ref(), &indices, None)?))
                    .collect::<Result<_>>()?,
            );
        }

        Ok(())
    }
    fn merge_batch(&mut self, states: &[ArrayRef]) -> Result<()> {
        if states.is_empty() {
            return Ok(());
        }

        assert_eq_or_internal_err!(states.len(), 1, "expects single state");

        let map_arr = as_map_array(states[0].as_ref())?;
        for row in 0..map_arr.len() {
            // Partial states are nullable so empty groups can produce a NULL map
            if map_arr.is_null(row) {
                continue;
            }
            self.update_batch(&map_row_to_arrays(map_arr, row))?;
        }

        Ok(())
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        Ok(vec![self.evaluate()?])
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        let keys = concat_column(&self.batches, 0, &self.key_type)?;
        let values = concat_column(&self.batches, 1, &self.value_type)?;
        build_single_map(keys, values)
    }

    fn size(&self) -> usize {
        // HashSet capacity excludes the empty buckets reserved for probing.
        let buckets = if self.seen.capacity() == 0 {
            0
        } else {
            self.seen.capacity().next_power_of_two()
        };
        size_of_val(self) + batches_size(&self.batches) + self.key_type.size()
            - size_of_val(&self.key_type)
            + self.value_type.size()
            - size_of_val(&self.value_type)
            + buckets * (size_of::<ScalarValue>() + 1)
            + self
                .seen
                .iter()
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
    /// Key, value, and ordering columns, retained as compact batches.
    batches: Vec<Vec<ArrayRef>>,
    /// Data types of the ordering expressions, used to build the state field.
    ordering_dtypes: Vec<DataType>,
    /// The `ORDER BY` requirement inside the aggregate.
    ordering_req: LexOrdering,
    /// When true the input already satisfies `ordering_req`, so the pairs do
    /// not have to be sorted again before de-duplication.
    is_input_pre_ordered: bool,
    /// Whether to restore the output order after sorting by a reversed requirement.
    reverse: bool,
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
        reverse: bool,
    ) -> Self {
        Self {
            key_type,
            value_type,
            batches: Vec::new(),
            ordering_dtypes,
            ordering_req,
            is_input_pre_ordered,
            reverse,
        }
    }

    /// Sort options of each ordering expression, used when sorting the pairs
    /// and when merging partial states.
    ///
    /// For a reversed accumulator these are the already-reversed options;
    /// the requested output order is restored separately by
    /// reversing the rows in [`Self::sorted_deduped`].
    fn sort_options(&self) -> Vec<SortOptions> {
        self.ordering_req.iter().map(|s| s.options).collect()
    }

    /// Sorts and de-duplicates all columns using the same row indices.
    #[allow(clippy::allow_attributes, clippy::mutable_key_type)] // ScalarValue has interior mutability but is intentionally used as hash key
    fn sorted_deduped(&self) -> Result<Vec<ArrayRef>> {
        let columns = std::iter::once(&self.key_type)
            .chain(std::iter::once(&self.value_type))
            .chain(self.ordering_dtypes.iter())
            .enumerate()
            .map(|(i, dtype)| concat_column(&self.batches, i, dtype))
            .collect::<Result<Vec<_>>>()?;
        let mut rows: Vec<usize> = (0..columns[0].len()).collect();
        if !self.is_input_pre_ordered {
            let converter = RowConverter::new(
                self.ordering_dtypes
                    .iter()
                    .zip(self.sort_options())
                    .map(|(dtype, options)| {
                        SortField::new_with_options(dtype.clone(), options)
                    })
                    .collect(),
            )?;
            let ordering_rows = converter.convert_columns(&columns[2..])?;
            rows.sort_by(|&a, &b| ordering_rows.row(a).cmp(&ordering_rows.row(b)));
        }
        if self.reverse {
            rows.reverse();
        }
        let mut seen = HashSet::new();
        let mut indices = Vec::new();
        for i in rows {
            if seen.insert(ScalarValue::try_from_array(&columns[0], i)?) {
                indices.push(i as u64);
            }
        }
        let indices = UInt64Array::from(indices);
        columns
            .iter()
            .map(|array| Ok(take(array.as_ref(), &indices, None)?))
            .collect()
    }

    /// Builds the `List<Struct<ordering...>>` state column from the given
    /// ordering values. These must be the de-duplicated ordering values that
    /// align with the map state, so both pieces of state describe the same rows.
    fn evaluate_orderings(&self, ordering_values: &[ArrayRef]) -> Result<ScalarValue> {
        let fields = ordering_fields(&self.ordering_req, &self.ordering_dtypes);
        let struct_array =
            StructArray::try_new(Fields::from(fields), ordering_values.to_vec(), None)?;
        Ok(SingleRowListArrayBuilder::new(Arc::new(struct_array)).build_list_scalar())
    }
}

impl Accumulator for OrderSensitiveMapAggAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> Result<()> {
        assert_eq_or_internal_err!(
            values.len(),
            2 + self.ordering_dtypes.len(),
            "map_agg column count mismatch"
        );
        let keys = cast(&values[0], &self.key_type)?;
        let vals = cast(values[1].as_ref(), &self.value_type)?;

        for array in &values[1..] {
            assert_eq_or_internal_err!(
                keys.len(),
                array.len(),
                "map_agg column length mismatch"
            );
        }
        if keys.logical_null_count() != 0 {
            return exec_err!("map key cannot be null");
        }
        if !keys.is_empty() {
            // Compaction alone leaves ordinary slices sharing the source buffers.
            let copy_and_compact = |array: ArrayRef| {
                compact_payload(make_array(copy_array_data(&array.to_data())))
            };
            let mut batch = Vec::with_capacity(values.len());
            batch.push(copy_and_compact(keys)?);
            batch.push(copy_and_compact(vals)?);
            for (array, dtype) in values[2..].iter().zip(&self.ordering_dtypes) {
                batch.push(copy_and_compact(cast(array, dtype)?)?);
            }
            self.batches.push(batch);
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

        // Individually ordered partitions are not globally ordered. Sort once
        // at output, including when the raw input was marked pre-ordered.
        self.is_input_pre_ordered = false;
        for row in 0..map_array.len() {
            if map_array.is_null(row) {
                continue;
            }
            let mut columns = map_row_to_arrays(map_array, row).to_vec();
            columns.extend_from_slice(orderings.value(row).as_struct().columns());
            self.update_batch(&columns)?;
        }
        Ok(())
    }

    fn state(&mut self) -> Result<Vec<ScalarValue>> {
        let rows = self.sorted_deduped()?;
        let orderings = self.evaluate_orderings(&rows[2..])?;
        let map = build_single_map(Arc::clone(&rows[0]), Arc::clone(&rows[1]))?;
        Ok(vec![map, orderings])
    }

    fn evaluate(&mut self) -> Result<ScalarValue> {
        let rows = self.sorted_deduped()?;
        build_single_map(Arc::clone(&rows[0]), Arc::clone(&rows[1]))
    }

    fn size(&self) -> usize {
        let mut total =
            size_of_val(self) + batches_size(&self.batches) + self.key_type.size()
                - size_of_val(&self.key_type)
                + self.value_type.size()
                - size_of_val(&self.value_type);

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
fn build_single_map(keys: ArrayRef, values: ArrayRef) -> Result<ScalarValue> {
    let map_type = MapAgg::map_data_type(keys.data_type(), values.data_type());
    let DataType::Map(entries_field, ordered) = &map_type else {
        return internal_err!("map_agg expected Map type, got {map_type}");
    };
    let DataType::Struct(entry_fields) = entries_field.data_type() else {
        return internal_err!(
            "map_agg expected struct entries, got {}",
            entries_field.data_type()
        );
    };

    let entries = StructArray::try_new(entry_fields.clone(), vec![keys, values], None)?;

    let Ok(num_entries) = i32::try_from(entries.len()) else {
        return internal_err!(
            "map_agg produced {} entries which exceeds the map offset range",
            entries.len()
        );
    };
    let offsets = OffsetBuffer::new(ScalarBuffer::from(vec![0_i32, num_entries]));
    let nulls = entries.is_empty().then(|| NullBuffer::from(vec![false]));

    Ok(ScalarValue::Map(Arc::new(MapArray::try_new(
        Arc::clone(entries_field),
        offsets,
        entries,
        nulls,
        *ordered,
    )?)))
}

fn map_row_to_arrays(map: &MapArray, row: usize) -> [ArrayRef; 2] {
    let entries = map.value(row);
    [Arc::clone(entries.column(0)), Arc::clone(entries.column(1))]
}

fn compact_array(array: ArrayRef) -> Result<ArrayRef> {
    compact_payload(array)
}

fn concat_column(
    batches: &[Vec<ArrayRef>],
    column: usize,
    dtype: &DataType,
) -> Result<ArrayRef> {
    if batches.is_empty() {
        return Ok(new_empty_array(dtype));
    }
    if batches.len() == 1 {
        return Ok(Arc::clone(&batches[0][column]));
    }
    let arrays = batches
        .iter()
        .map(|batch| batch[column].as_ref())
        .collect::<Vec<_>>();
    Ok(concat(&arrays)?)
}

fn batches_size(batches: &Vec<Vec<ArrayRef>>) -> usize {
    batches.capacity() * size_of::<Vec<ArrayRef>>()
        + batches
            .iter()
            .map(|batch| {
                batch.capacity() * size_of::<ArrayRef>()
                    + batch
                        .iter()
                        .map(|array| array.get_array_memory_size())
                        .sum::<usize>()
            })
            .sum::<usize>()
}

#[cfg(test)]
mod tests {
    use super::*;

    use arrow::array::{Int32Array, RecordBatch, StringArray};
    use arrow::datatypes::Schema;

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
    fn map_agg_output_and_state_match_declared_schema() -> Result<()> {
        let key_type = DataType::Utf8;
        let value_type =
            DataType::Struct(Fields::from(vec![Field::new("n", DataType::Int32, true)]));
        let agg = MapAgg::default();
        let return_field = Arc::new(Field::new(
            "map_agg",
            agg.return_type(&[key_type.clone(), value_type.clone()])?,
            true,
        ));
        let state_fields = agg.state_fields(StateFieldsArgs {
            name: agg.name(),
            input_fields: &[
                Arc::new(Field::new("key", key_type.clone(), false)),
                Arc::new(Field::new("value", value_type.clone(), true)),
            ],
            return_field: Arc::clone(&return_field),
            ordering_fields: &[],
            is_distinct: false,
        })?;
        let mut acc = MapAggAccumulator::try_new(key_type, value_type)?;
        let values = StructArray::from(vec![(
            Arc::new(Field::new("n", DataType::Int32, false)),
            Arc::new(Int32Array::from(vec![1])) as ArrayRef,
        )]);
        acc.update_batch(&[Arc::new(StringArray::from(vec!["a"])), Arc::new(values)])?;

        RecordBatch::try_new(
            Arc::new(Schema::new(vec![return_field])),
            vec![acc.evaluate()?.to_array()?],
        )?;
        let state = acc
            .state()?
            .iter()
            .map(ScalarValue::to_array)
            .collect::<Result<Vec<_>>>()?;
        RecordBatch::try_new(Arc::new(Schema::new(state_fields)), state)?;
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
        let err = acc
            .update_batch(&[
                Arc::new(StringArray::from(vec![Some("a"), None])),
                Arc::new(Int32Array::from(vec![Some(1), Some(2)])),
            ])
            .unwrap_err();
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

    /// Accumulator ordered by an `Int32` `ord` column (ascending, nulls last).
    fn ordered_accumulator(
        is_input_pre_ordered: bool,
        reverse: bool,
    ) -> Result<OrderSensitiveMapAggAccumulator> {
        ordered_accumulator_with_options(
            is_input_pre_ordered,
            reverse,
            SortOptions::new(false, false),
        )
    }

    /// Accumulator ordered by an `Int32` `ord` column with the given options.
    fn ordered_accumulator_with_options(
        is_input_pre_ordered: bool,
        reverse: bool,
        options: SortOptions,
    ) -> Result<OrderSensitiveMapAggAccumulator> {
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
        let ordering =
            LexOrdering::new(vec![PhysicalSortExpr::new(ord_expr, options)]).unwrap();

        Ok(OrderSensitiveMapAggAccumulator::new(
            DataType::Utf8,
            DataType::Int32,
            vec![DataType::Int32],
            ordering,
            is_input_pre_ordered,
            reverse,
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
        let mut acc = ordered_accumulator(false, false)?;
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

    fn assert_ordered_map_agg_normalizes_ordering_type(
        is_input_pre_ordered: bool,
    ) -> Result<()> {
        let mut acc = ordered_accumulator(is_input_pre_ordered, false)?;
        let declared_field = Arc::new(Field::new("n", DataType::Int32, true));
        acc.ordering_dtypes[0] =
            DataType::Struct(Fields::from(vec![declared_field.clone()]));
        let (keys, values, order) = if is_input_pre_ordered {
            (["a", "b"], [10, 20], [1, 2])
        } else {
            (["b", "a"], [20, 10], [2, 1])
        };
        let order = StructArray::from(vec![(
            Arc::new(Field::new("n", DataType::Int32, false)),
            Arc::new(Int32Array::from(order.to_vec())) as ArrayRef,
        )]);
        acc.update_batch(&[
            Arc::new(StringArray::from(keys.to_vec())),
            Arc::new(Int32Array::from(values.to_vec())),
            Arc::new(order),
        ])?;

        let result = acc.evaluate()?;
        let ScalarValue::Map(map) = &result else {
            panic!("expected map scalar");
        };
        assert_eq!(
            map.keys().as_string::<i32>(),
            &StringArray::from(vec!["a", "b"])
        );
        assert_eq!(map.values().as_primitive(), &Int32Array::from(vec![10, 20]));

        let state = acc.state()?;
        assert_eq!(state[0], result);
        let orderings = state[1].to_array()?;
        let entries = orderings.as_list::<i32>().value(0);
        let expected = StructArray::from(vec![(
            declared_field,
            Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef,
        )]);
        assert_eq!(entries.as_struct().column(0).as_struct(), &expected);
        Ok(())
    }

    #[test]
    fn ordered_map_agg_normalizes_presorted_ordering_type() -> Result<()> {
        assert_ordered_map_agg_normalizes_ordering_type(true)
    }

    #[test]
    fn ordered_map_agg_normalizes_unsorted_ordering_type() -> Result<()> {
        assert_ordered_map_agg_normalizes_ordering_type(false)
    }

    #[test]
    fn ordered_map_agg_detaches_input_slices() -> Result<()> {
        let keys = StringArray::from_iter_values((0..10_000).map(|i| format!("key-{i}")));
        let values = Int32Array::from_iter_values(0..10_000);
        let orderings = Int32Array::from_iter_values((0..10_000).rev());
        let mut acc = ordered_accumulator(false, false)?;
        acc.update_batch(&[
            Arc::new(keys.slice(3, 1)),
            Arc::new(values.slice(3, 1)),
            Arc::new(orderings.slice(3, 1)),
        ])?;
        let size = acc.size();
        assert!(size < 4_000, "one retained row uses {size} bytes");

        let result = acc.evaluate()?;
        let ScalarValue::Map(map) = &result else {
            panic!("expected map scalar");
        };
        assert_eq!(
            map.keys().as_string::<i32>(),
            &StringArray::from(vec!["key-3"])
        );
        assert_eq!(map.values().as_primitive(), &Int32Array::from(vec![3]));

        let state = acc.state()?;
        assert_eq!(state[0], result);
        let orderings = state[1].to_array()?;
        let entries = orderings.as_list::<i32>().value(0);
        assert_eq!(
            entries.as_struct().column(0).as_primitive(),
            &Int32Array::from(vec![9996])
        );
        Ok(())
    }

    #[test]
    fn ordered_map_agg_merges_partition_states_in_ordering() -> Result<()> {
        let mut lhs = ordered_accumulator(true, false)?;
        lhs.update_batch(&ordered_batch(&["a", "c"], &[1, 3], &[1, 3]))?;

        let mut rhs = ordered_accumulator(false, false)?;
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

    #[test]
    fn ordered_map_agg_reverses_preordered_input_before_deduplication() -> Result<()> {
        // Reversed accumulator arguments carry reversed ordering options.
        let mut acc =
            ordered_accumulator_with_options(true, true, SortOptions::new(true, false))?;
        acc.update_batch(&ordered_batch(&["a", "a", "b"], &[20, 10, 5], &[20, 10, 5]))?;

        let ScalarValue::Map(map) = acc.evaluate()? else {
            panic!("expected map scalar");
        };
        let map_keys = map.keys().as_any().downcast_ref::<StringArray>().unwrap();
        let map_values = map.values().as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(map_keys, &StringArray::from(vec!["b", "a"]));
        assert_eq!(map_values, &Int32Array::from(vec![5, 10]));

        Ok(())
    }

    #[test]
    fn ordered_map_agg_reverses_unordered_input_before_deduplication() -> Result<()> {
        // A reversed aggregate can receive the reversed ordering options
        // without pre-ordered input, for example in a Final aggregate.
        let mut acc =
            ordered_accumulator_with_options(false, true, SortOptions::new(true, false))?;
        acc.update_batch(&ordered_batch(&["a", "b", "a"], &[20, 5, 10], &[20, 5, 10]))?;

        let ScalarValue::Map(map) = acc.evaluate()? else {
            panic!("expected map scalar");
        };
        // The requested order is ascending, so `b` (5) comes first and the
        // earlier `a` pair (10) wins over the later one (20).
        let map_keys = map.keys().as_any().downcast_ref::<StringArray>().unwrap();
        let map_values = map.values().as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(map_keys, &StringArray::from(vec!["b", "a"]));
        assert_eq!(map_values, &Int32Array::from(vec![5, 10]));

        Ok(())
    }

    #[test]
    fn ordered_map_agg_reversed_partial_states_merge_before_deduplication() -> Result<()>
    {
        let options = SortOptions::new(true, false);

        let mut lhs = ordered_accumulator_with_options(true, true, options)?;
        lhs.update_batch(&ordered_batch(&["a"], &[20], &[20]))?;
        let lhs_state = lhs
            .state()?
            .iter()
            .map(ScalarValue::to_array)
            .collect::<Result<Vec<_>>>()?;

        let mut rhs = ordered_accumulator_with_options(true, true, options)?;
        rhs.update_batch(&ordered_batch(&["a", "b"], &[10, 5], &[10, 5]))?;
        let rhs_state = rhs
            .state()?
            .iter()
            .map(ScalarValue::to_array)
            .collect::<Result<Vec<_>>>()?;

        // The final stage is reversed but not pre-ordered.
        let mut final_acc = ordered_accumulator_with_options(false, true, options)?;
        final_acc.merge_batch(&lhs_state)?;
        final_acc.merge_batch(&rhs_state)?;

        let ScalarValue::Map(map) = final_acc.evaluate()? else {
            panic!("expected map scalar");
        };
        let map_keys = map.keys().as_any().downcast_ref::<StringArray>().unwrap();
        let map_values = map.values().as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(map_keys, &StringArray::from(vec!["b", "a"]));
        assert_eq!(map_values, &Int32Array::from(vec![5, 10]));

        Ok(())
    }

    #[test]
    fn ordered_map_agg_null_key_errors() -> Result<()> {
        let mut acc = ordered_accumulator(false, false)?;
        let err = acc
            .update_batch(&[
                Arc::new(StringArray::from(vec![Some("a"), None])),
                Arc::new(Int32Array::from(vec![1, 2])),
                Arc::new(Int32Array::from(vec![1, 2])),
            ])
            .unwrap_err();
        assert!(err.to_string().contains("map key cannot be null"));
        assert!(acc.evaluate()?.is_null());
        Ok(())
    }

    #[test]
    fn map_agg_dictionary_null_keys() -> Result<()> {
        use arrow::array::DictionaryArray;
        use arrow::datatypes::Int32Type;

        let keys: ArrayRef = Arc::new(DictionaryArray::<Int32Type>::try_new(
            Int32Array::from(vec![0, 1]),
            Arc::new(StringArray::from(vec![Some("a"), None])),
        )?);
        assert_eq!(keys.null_count(), 0);
        for ordered in [false, true] {
            let mut acc: Box<dyn Accumulator> = if ordered {
                let mut acc = ordered_accumulator(false, false)?;
                acc.key_type = keys.data_type().clone();
                Box::new(acc)
            } else {
                Box::new(MapAggAccumulator::try_new(
                    keys.data_type().clone(),
                    DataType::Int32,
                )?)
            };
            let mut values = vec![
                Arc::clone(&keys),
                Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef,
            ];
            if ordered {
                values.push(Arc::clone(&values[1]));
            }
            let err = acc.update_batch(&values).unwrap_err();
            assert!(err.to_string().contains("map key cannot be null"));
            assert!(acc.evaluate()?.is_null());
        }
        Ok(())
    }

    #[test]
    fn map_agg_duplicate_payloads_are_not_retained() -> Result<()> {
        let mut acc = map_agg_accumulator()?;
        let batch = [
            Arc::new(StringArray::from(vec!["a"])) as ArrayRef,
            Arc::new(Int32Array::from(vec![1])) as ArrayRef,
        ];
        acc.update_batch(&batch)?;
        let size = acc.size();
        let expected = acc.evaluate()?;
        for _ in 0..10 {
            acc.update_batch(&batch)?;
            acc.merge_batch(&[expected.to_array()?])?;
            assert_eq!(acc.evaluate()?, expected);
            assert_eq!(acc.size(), size);
        }
        Ok(())
    }

    #[test]
    fn ordered_map_agg_empty_state_roundtrip() -> Result<()> {
        let mut acc = ordered_accumulator(false, false)?;
        let expected = acc.evaluate()?;
        assert_eq!(
            expected,
            ScalarValue::try_new_null(&MapAgg::map_data_type(
                &DataType::Utf8,
                &DataType::Int32,
            ))?
        );
        let state = acc
            .state()?
            .iter()
            .map(ScalarValue::to_array)
            .collect::<Result<Vec<_>>>()?;
        acc.merge_batch(&state)?;
        assert_eq!(acc.evaluate()?, expected);
        acc.update_batch(&ordered_batch(&["a"], &[1], &[1]))?;
        acc.merge_batch(&state)?;
        assert_eq!(acc.evaluate()?, acc.state()?[0]);
        Ok(())
    }

    #[test]
    fn map_agg_compacts_dictionary_payloads() -> Result<()> {
        use arrow::array::{DictionaryArray, StringArray};
        use arrow::datatypes::Int32Type;

        for n in [10, 10_000] {
            let values = StringArray::from_iter_values(
                (0..n).map(|i| format!("unused_value_{i:05}")),
            );
            let dictionary: ArrayRef = Arc::new(DictionaryArray::<Int32Type>::try_new(
                Int32Array::from_iter_values(0..n as i32),
                Arc::new(values),
            )?);
            // Retain only the last row so compaction must remap its key and
            // drop the rest of the dictionary.
            let value = dictionary.slice(n - 1, 1);
            let keys = Arc::new(Int32Array::from(vec![42])) as ArrayRef;

            let mut accumulator = MapAggAccumulator::try_new(
                DataType::Int32,
                dictionary.data_type().clone(),
            )?;
            accumulator.update_batch(&[keys, Arc::clone(&value)])?;

            let size = accumulator.size();
            assert!(
                size < 2_000,
                "dictionary compaction retained unused values: \
                 {size} bytes for {n} entries"
            );

            let ScalarValue::Map(map) = accumulator.evaluate()? else {
                panic!("expected map scalar");
            };
            assert_eq!(
                ScalarValue::try_from_array(map.values(), 0)?,
                ScalarValue::try_from_array(&value, 0)?
            );
        }

        Ok(())
    }
}
