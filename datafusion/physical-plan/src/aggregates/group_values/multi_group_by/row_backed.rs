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

//! A generic [`GroupColumn`] backed by the arrow row format.
//!
//! Unlike the type-specialized builders in this module (primitive, byte,
//! boolean, ...), [`RowsGroupColumn`] works for *any* data type that arrow's
//! [`RowConverter`] can encode — including nested types such as `Struct`,
//! `List`, `LargeList` and `FixedSizeList`. It stores one group value per row
//! in a single-column [`Rows`] buffer and compares group keys by their encoded
//! bytes.
//!
//! # Why this exists
//!
//! [`GroupValuesColumn`] can only be used when *every* column of the group-by
//! key has a [`GroupColumn`] implementation; otherwise the whole aggregation
//! falls back to the row-wise [`GroupValuesRows`], which is materially slower
//! and heavier for the columns that *would* have qualified for the column-wise
//! fast path. By providing a generic fallback `GroupColumn`, a schema like
//! `GROUP BY int_col, struct_col` keeps `int_col` on its fast native builder
//! and only pays the row-encoding cost on `struct_col`, instead of dragging both
//! columns onto `GroupValuesRows`.
//!
//! # Relationship to hashing
//!
//! This column does not hash anything itself: [`GroupValuesColumn`] hashes the
//! raw input columns via `create_hashes`, which already supports nested types.
//! Equality is decided here by comparing arrow-row bytes. For the two to agree
//! on group identity, values that this column considers equal must hash equal —
//! see the float `-0.0` / `NaN` note on [`RowsGroupColumn`].
//!
//! [`GroupValuesColumn`]: crate::aggregates::group_values::multi_group_by::GroupValuesColumn
//! [`GroupValuesRows`]: crate::aggregates::group_values::GroupValuesRows

use crate::aggregates::group_values::multi_group_by::GroupColumn;
use crate::aggregates::group_values::row::encode_array_if_necessary;

use arrow::array::{Array, ArrayRef, BooleanBufferBuilder};
use arrow::datatypes::DataType;
use arrow::row::{RowConverter, Rows, SortField};
use datafusion_common::{DataFusionError, Result};

/// A [`GroupColumn`] that stores group values for a single column in the arrow
/// [row format], backed by a single-field [`RowConverter`].
///
/// # NULL semantics
///
/// The [`GroupColumn`] contract treats two NULLs as equal. The row format
/// encodes NULL with a distinct sentinel, so `null`-row bytes compare equal to
/// each other and unequal to any non-null row — matching the contract without
/// special-casing.
///
/// # Float `-0.0` / `NaN`
///
/// Equality here is byte equality under arrow's IEEE-754 *totalOrder* row
/// encoding, which treats `-0.0` and `+0.0` as distinct and canonicalizes
/// `NaN`. Because hashing is performed separately (on the raw input array), a
/// caller must ensure the two agree — e.g. by normalizing `-0.0 → +0.0` on the
/// input columns before hashing when a float leaf is present (as
/// [`GroupValuesRows`] does). See the module docs.
///
/// [row format]: arrow::row
/// [`GroupValuesRows`]: crate::aggregates::group_values::GroupValuesRows
pub struct RowsGroupColumn {
    /// Single-field row converter for this column's data type.
    row_converter: RowConverter,
    /// Accumulated group values in row format; `group_values.row(i)` is the
    /// group value for group index `i`.
    group_values: Rows,
    /// The column's expected output type. The row format decodes dictionary /
    /// run-end encoded values to their plain value type, so emitted arrays are
    /// re-encoded to this type in `build` / `take_n` (mirroring
    /// `GroupValuesRows::emit`).
    output_type: DataType,
}

/// Walk `data_type`'s subtree and return `true` if it contains a
/// [`DataType::FixedSizeList`] whose descendant tree includes any
/// [`DataType::Dictionary`].
///
/// Two-state recursion: once we cross a `FixedSizeList`, `inside_fsl`
/// stays true for every descendant, so a `Dictionary` anywhere below
/// counts. Above that boundary, encountering a `Dictionary` is fine —
/// only nested containers propagate the risk.
fn contains_fsl_with_dictionary(data_type: &DataType) -> bool {
    fn walk(dt: &DataType, inside_fsl: bool) -> bool {
        match dt {
            DataType::Dictionary(_, _) => inside_fsl,
            DataType::FixedSizeList(f, _) => walk(f.data_type(), true),
            DataType::List(f)
            | DataType::LargeList(f)
            | DataType::ListView(f)
            | DataType::LargeListView(f) => walk(f.data_type(), inside_fsl),
            DataType::Map(f, _) => walk(f.data_type(), inside_fsl),
            DataType::Struct(fs) => fs.iter().any(|f| walk(f.data_type(), inside_fsl)),
            DataType::RunEndEncoded(_, values) => walk(values.data_type(), inside_fsl),
            DataType::Union(fs, _) => {
                fs.iter().any(|(_, f)| walk(f.data_type(), inside_fsl))
            }
            _ => false,
        }
    }
    walk(data_type, false)
}

/// Return `true` if `data_type` contains a [`DataType::Union`] or
/// [`DataType::RunEndEncoded`] anywhere in its subtree.
///
/// These two nested variants can round-trip through `RowConverter` in
/// principle, but their arrow-row decoders have not been validated by
/// this crate's test matrix against the full range of leaf types (dict,
/// nested, etc.). Before this PR both were handled by `GroupValuesRows`
/// (they were not `is_nested`-eligible for `GroupValuesColumn`), so
/// reject them here to preserve the pre-PR routing rather than route
/// untested shapes through `RowsGroupColumn`. When we grow explicit
/// round-trip tests for these types, this blacklist can be removed.
fn contains_union_or_run_end_encoded(data_type: &DataType) -> bool {
    match data_type {
        DataType::Union(_, _) | DataType::RunEndEncoded(_, _) => true,
        DataType::List(f)
        | DataType::LargeList(f)
        | DataType::ListView(f)
        | DataType::LargeListView(f)
        | DataType::FixedSizeList(f, _) => {
            contains_union_or_run_end_encoded(f.data_type())
        }
        DataType::Map(f, _) => contains_union_or_run_end_encoded(f.data_type()),
        DataType::Struct(fs) => fs
            .iter()
            .any(|f| contains_union_or_run_end_encoded(f.data_type())),
        _ => false,
    }
}

impl RowsGroupColumn {
    /// Returns whether `data_type` can be handled by this generic column.
    ///
    /// This is stricter than [`RowConverter::supports_fields`]: the row
    /// format also has to survive the `build` / `take_n` reverse trip
    /// through [`RowConverter::convert_rows`], and arrow's
    /// `decode_fixed_size_list` (arrow-row 59.1.0) skips the
    /// dictionary-flatten correction that the other list-like decoders
    /// apply, so any `FixedSizeList` containing a `Dictionary` leaf
    /// panics on emit with `"FixedSizeListArray expected data type
    /// Dictionary(...) got <flattened> for \"item\""`.
    ///
    /// Reject those shapes here so `make_group_column` falls back to
    /// `GroupValuesRows`. The other list-likes (`List`, `LargeList`,
    /// `ListView`, `LargeListView`, `Map`) do carry the correction and
    /// round-trip cleanly on the same arrow-row version.
    ///
    /// Additionally, `Union` and `RunEndEncoded` are rejected because
    /// they were routed to `GroupValuesRows` before this column existed
    /// and their arrow-row round-trip has not been covered by this
    /// crate's tests yet. Keeping them on the pre-PR path avoids
    /// introducing an untested code path for those types.
    pub fn supports_type(data_type: &DataType) -> bool {
        if contains_fsl_with_dictionary(data_type) {
            return false;
        }
        if contains_union_or_run_end_encoded(data_type) {
            return false;
        }
        RowConverter::supports_fields(&[SortField::new(data_type.clone())])
    }

    /// Create an empty [`RowsGroupColumn`] for `data_type`.
    pub fn try_new(data_type: DataType) -> Result<Self> {
        let row_converter = RowConverter::new(vec![SortField::new(data_type.clone())])?;
        let group_values = row_converter.empty_rows(0, 0);
        Ok(Self {
            row_converter,
            group_values,
            output_type: data_type,
        })
    }

    /// Materialize `rows` into a single array of `self.output_type`, re-applying
    /// dictionary / run-end encoding the row format strips on decode.
    fn rows_to_array<'a>(
        &self,
        rows: impl IntoIterator<Item = arrow::row::Row<'a>>,
    ) -> ArrayRef {
        let mut arrays = self
            .row_converter
            .convert_rows(rows)
            .expect("row conversion during emit");
        debug_assert_eq!(arrays.len(), 1, "single-field row converter");
        let array = arrays.swap_remove(0);
        encode_array_if_necessary(&array, &self.output_type)
            .expect("dictionary re-encode during emit")
    }

    /// Encode a whole incoming column into the row format.
    fn convert(&self, array: &ArrayRef) -> Result<Rows> {
        self.row_converter
            .convert_columns(std::slice::from_ref(array))
            .map_err(DataFusionError::from)
    }
}

impl GroupColumn for RowsGroupColumn {
    fn equal_to(&self, lhs_row: usize, array: &ArrayRef, rhs_row: usize) -> bool {
        // Scalar path (hash-collision remainder / streaming). Encode just the
        // single incoming row rather than the whole column. The vectorized
        // methods below encode the batch once; this path is expected to be rare.
        let incoming = self
            .convert(&array.slice(rhs_row, 1))
            .expect("row conversion during equal_to");
        self.group_values.row(lhs_row) == incoming.row(0)
    }

    fn append_val(&mut self, array: &ArrayRef, row: usize) -> Result<()> {
        let incoming = self.convert(&array.slice(row, 1))?;
        self.group_values.push(incoming.row(0));
        Ok(())
    }

    fn vectorized_equal_to(
        &self,
        lhs_rows: &[usize],
        array: &ArrayRef,
        rhs_rows: &[usize],
        equal_to_results: &mut BooleanBufferBuilder,
    ) {
        // Encode the incoming column once for the whole batch.
        let incoming = self
            .convert(array)
            .expect("row conversion during vectorized_equal_to");
        for (idx, (&lhs_row, &rhs_row)) in
            lhs_rows.iter().zip(rhs_rows.iter()).enumerate()
        {
            // Preserve the AND-accumulate contract: skip rows already false.
            if !equal_to_results.get_bit(idx) {
                continue;
            }
            if self.group_values.row(lhs_row) != incoming.row(rhs_row) {
                equal_to_results.set_bit(idx, false);
            }
        }
    }

    fn vectorized_append(&mut self, array: &ArrayRef, rows: &[usize]) -> Result<()> {
        // Encode the incoming column once, then push the selected rows.
        let incoming = self.convert(array)?;
        for &row in rows {
            self.group_values.push(incoming.row(row));
        }
        Ok(())
    }

    fn len(&self) -> usize {
        self.group_values.num_rows()
    }

    fn size(&self) -> usize {
        self.row_converter.size() + self.group_values.size()
    }

    fn build(self: Box<Self>) -> ArrayRef {
        self.rows_to_array(&self.group_values)
    }

    fn take_n(&mut self, n: usize) -> ArrayRef {
        debug_assert!(n <= self.group_values.num_rows());

        // Materialize the first `n` group rows.
        let output = self.rows_to_array(self.group_values.iter().take(n));

        // Shift the remaining rows to the front by rebuilding the buffer.
        // TODO: mirror the arrow-rs efficiency TODO in `GroupValuesRows::emit`.
        let mut remaining = self.row_converter.empty_rows(0, 0);
        for row in self.group_values.iter().skip(n) {
            remaining.push(row);
        }
        self.group_values = remaining;

        output
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use arrow::array::{Array, ArrayRef, FixedSizeListArray, Int32Array, StructArray};
    use arrow::datatypes::{DataType, Field, Int32Type};
    use std::sync::Arc;

    fn fsl_i32(data: Vec<Option<Vec<Option<i32>>>>, list_len: i32) -> ArrayRef {
        Arc::new(FixedSizeListArray::from_iter_primitive::<Int32Type, _, _>(
            data, list_len,
        ))
    }

    /// The generic column must agree with a per-row reference for equality,
    /// including inner-null and outer-null rows, on a `FixedSizeList<Int32>`.
    #[test]
    fn fsl_append_equal_to_build_roundtrip() {
        let dt = DataType::FixedSizeList(
            Arc::new(Field::new("item", DataType::Int32, true)),
            2,
        );
        let mut col = Box::new(RowsGroupColumn::try_new(dt).unwrap());

        // group values: [1,2], null-outer, [3, null-inner]
        let input = fsl_i32(
            vec![
                Some(vec![Some(1), Some(2)]),
                None,
                Some(vec![Some(3), None]),
            ],
            2,
        );

        col.vectorized_append(&input, &[0, 1, 2]).unwrap();
        assert_eq!(col.len(), 3);

        // Probe with a fresh batch: row0 == group0, row1 (null) == group1,
        // row2 differs from group0, row3 (inner null) == group2.
        let probe = fsl_i32(
            vec![
                Some(vec![Some(1), Some(2)]), // == g0
                None,                         // == g1
                Some(vec![Some(9), Some(9)]), // != g0
                Some(vec![Some(3), None]),    // == g2
            ],
            2,
        );

        assert!(col.equal_to(0, &probe, 0));
        assert!(col.equal_to(1, &probe, 1));
        assert!(!col.equal_to(0, &probe, 2));
        assert!(col.equal_to(2, &probe, 3));

        // Vectorized equal_to should match the scalar reference.
        let mut results = BooleanBufferBuilder::new(3);
        results.append_n(3, true);
        col.vectorized_equal_to(&[0, 1, 2], &probe, &[0, 1, 3], &mut results);
        assert!(results.get_bit(0));
        assert!(results.get_bit(1));
        assert!(results.get_bit(2));

        // build() must reproduce the original group values.
        let out = col.build();
        let out = out.as_any().downcast_ref::<FixedSizeListArray>().unwrap();
        assert_eq!(out.len(), 3);
        assert!(out.is_null(1));
        assert!(!out.is_null(0));
    }

    /// `take_n` must emit the first `n` rows and shift the rest to the front.
    #[test]
    fn fsl_take_n_shifts_remaining() {
        let dt = DataType::FixedSizeList(
            Arc::new(Field::new("item", DataType::Int32, true)),
            1,
        );
        let mut col = RowsGroupColumn::try_new(dt).unwrap();

        let input = fsl_i32(
            vec![
                Some(vec![Some(10)]),
                Some(vec![Some(20)]),
                Some(vec![Some(30)]),
            ],
            1,
        );
        col.vectorized_append(&input, &[0, 1, 2]).unwrap();

        let first = col.take_n(1);
        let first = first.as_any().downcast_ref::<FixedSizeListArray>().unwrap();
        let first_vals = first
            .value(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .clone();
        assert_eq!(first_vals.value(0), 10);
        assert_eq!(col.len(), 2);

        // Remaining 20, 30 should now be at indices 0, 1.
        let rest = Box::new(col).build();
        let rest = rest.as_any().downcast_ref::<FixedSizeListArray>().unwrap();
        assert_eq!(rest.len(), 2);
        let g0 = rest
            .value(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
            .value(0);
        assert_eq!(g0, 20);
    }

    /// Works for `Struct<a: Int32>` too — proves the column is type-generic.
    #[test]
    fn struct_roundtrip() {
        let dt = DataType::Struct(vec![Field::new("a", DataType::Int32, true)].into());
        let mut col = RowsGroupColumn::try_new(dt).unwrap();

        let a: ArrayRef = Arc::new(Int32Array::from(vec![Some(1), Some(2)]));
        let input: ArrayRef = Arc::new(StructArray::new(
            vec![Field::new("a", DataType::Int32, true)].into(),
            vec![a],
            None,
        ));
        col.vectorized_append(&input, &[0, 1]).unwrap();
        assert_eq!(col.len(), 2);
        assert!(col.equal_to(0, &input, 0));
        assert!(!col.equal_to(0, &input, 1));
    }

    #[test]
    fn supports_type_matches_row_converter_impl() {
        assert!(RowsGroupColumn::supports_type(&DataType::FixedSizeList(
            Arc::new(Field::new("item", DataType::Int32, true)),
            3
        )));
        assert!(RowsGroupColumn::supports_type(&DataType::Struct(
            vec![Field::new("a", DataType::Int32, true)].into()
        )));
        // Whether Map is encodable depends on the arrow-rs version.
        // Just assert that our `supports_type` agrees with arrow's
        // `RowConverter::supports_fields` — either both accept it or both
        // reject it. Both are correct wrt the invariant.
        let map_field = Arc::new(Field::new(
            "entries",
            DataType::Struct(
                vec![
                    Field::new("keys", DataType::Int32, false),
                    Field::new("values", DataType::Int32, true),
                ]
                .into(),
            ),
            false,
        ));
        let map_dt = DataType::Map(map_field, false);
        let arrow_supports =
            RowConverter::supports_fields(&[SortField::new(map_dt.clone())]);
        assert_eq!(RowsGroupColumn::supports_type(&map_dt), arrow_supports);
    }

    /// Regression test for the nested-container recursion in
    /// [`crate::aggregates::group_values::row::encode_array_if_necessary`].
    /// `RowConverter` flattens dictionary values on the way in, so a
    /// `List<Dict<Int32, Utf8>>` schema round-trips with `Utf8` values
    /// unless the helper re-encodes the leaf. Without that recursion,
    /// `build()` would emit an array whose data type does not match the
    /// group column's declared type.
    #[test]
    fn build_preserves_list_of_dictionary_schema() {
        use arrow::array::{DictionaryArray, ListArray, StringArray};
        use arrow::buffer::OffsetBuffer;
        use arrow::datatypes::Int32Type;

        let dict_dt =
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8));
        let item_field = Arc::new(Field::new("item", dict_dt.clone(), true));
        let outer_dt = DataType::List(Arc::clone(&item_field));

        // Skip if this arrow-rs version rejects the nesting — the invariant we
        // care about is `output().data_type() == declared type` conditional on
        // supports_type saying yes.
        if !RowsGroupColumn::supports_type(&outer_dt) {
            return;
        }

        let mut col = Box::new(RowsGroupColumn::try_new(outer_dt.clone()).unwrap());

        // Build List<Dict<Int32,Utf8>> of one row = ["a", "b"].
        let values = Arc::new(StringArray::from(vec!["a", "b"]));
        let keys = Int32Array::from(vec![0, 1]);
        let dict = DictionaryArray::<Int32Type>::try_new(keys, values).unwrap();
        let offsets = OffsetBuffer::from_lengths([2]);
        let list =
            ListArray::try_new(Arc::clone(&item_field), offsets, Arc::new(dict), None)
                .unwrap();
        let input: ArrayRef = Arc::new(list);

        col.vectorized_append(&input, &[0]).unwrap();
        let built = col.build();
        assert_eq!(
            built.data_type(),
            &outer_dt,
            "build() must return the declared List<Dict> data type, \
             not the RowConverter-flattened List<Utf8>",
        );
    }

    // ---- FSL<Dict> rejection ----------------------------------------
    //
    // arrow-row 59.1.0's `decode_fixed_size_list` skips the
    // dict-flatten correction that the generic `decode<L>` path applies
    // to `List` / `LargeList` / `ListView` / `LargeListView` / `Map`,
    // so any `FixedSizeList` containing a `Dictionary` leaf panics on
    // emit. `supports_type` must reject those shapes so
    // `GroupValuesRows` fallback handles them instead. These tests pin
    // the current shape of that black-list.

    fn dict_utf8() -> DataType {
        DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8))
    }

    fn fsl_of(inner: DataType) -> DataType {
        DataType::FixedSizeList(Arc::new(Field::new("item", inner, true)), 2)
    }

    #[test]
    fn supports_type_rejects_fixed_size_list_of_dict() {
        // Direct case: `FixedSizeList<Dict<Int32, Utf8>>`.
        assert!(!RowsGroupColumn::supports_type(&fsl_of(dict_utf8())));
    }

    #[test]
    fn supports_type_rejects_fsl_with_dict_nested_in_struct() {
        // The dict is one level deep under a struct that is itself the
        // FSL element. arrow-row still panics because `convert_raw`
        // returns the struct with a decoded (Utf8) field while the
        // FSL builder expects the declared struct-with-dict shape.
        let struct_dt = DataType::Struct(vec![Field::new("d", dict_utf8(), true)].into());
        assert!(!RowsGroupColumn::supports_type(&fsl_of(struct_dt)));
    }

    #[test]
    fn supports_type_rejects_fsl_with_dict_nested_in_list() {
        // `FixedSizeList<List<Dict>>` — the inner `List<Dict>` handles
        // dicts correctly on its own, but the outer FSL wrapper still
        // panics with the mismatched declared child type.
        let list_of_dict =
            DataType::List(Arc::new(Field::new("item", dict_utf8(), true)));
        assert!(!RowsGroupColumn::supports_type(&fsl_of(list_of_dict)));
    }

    #[test]
    fn supports_type_rejects_fsl_hidden_under_outer_list() {
        // Sibling positioning: the outer container is a `List` (which is
        // fine on its own), but its child is a `FixedSizeList<Dict>`.
        // The panic surface is at the inner FSL layer regardless of what
        // wraps it, so this must still be rejected.
        let outer =
            DataType::List(Arc::new(Field::new("item", fsl_of(dict_utf8()), true)));
        assert!(!RowsGroupColumn::supports_type(&outer));
    }

    #[test]
    fn supports_type_rejects_fsl_hidden_under_outer_struct() {
        // Same, but the outer wrapper is a struct.
        let outer =
            DataType::Struct(vec![Field::new("f", fsl_of(dict_utf8()), true)].into());
        assert!(!RowsGroupColumn::supports_type(&outer));
    }

    // ---- FSL without dicts is still fine ----------------------------

    #[test]
    fn supports_type_accepts_fsl_of_primitive() {
        // Sanity: a plain FSL<Int32> must not get caught by the
        // dict-under-FSL blacklist.
        assert!(RowsGroupColumn::supports_type(&fsl_of(DataType::Int32)));
    }

    #[test]
    fn supports_type_accepts_fsl_of_struct_without_dict() {
        // FSL of struct where the struct's fields are all primitives.
        let struct_dt =
            DataType::Struct(vec![Field::new("a", DataType::Int32, true)].into());
        assert!(RowsGroupColumn::supports_type(&fsl_of(struct_dt)));
    }

    // ---- Positive round-trip tests for non-FSL list-likes -----------
    //
    // The other list-like decoders in arrow-row 59.1.0
    // (`GenericListArrayOrMap` path) apply the corrected_type fix, so
    // `List<Dict>`, `LargeList<Dict>`, `ListView<Dict>`, `LargeListView<Dict>`
    // and `Map<..., Dict>` all round-trip cleanly. These tests pin
    // that they are (a) accepted by `supports_type` and (b) actually
    // survive `vectorized_append` + `build()` without panicking, so a
    // future arrow-rs regression there is caught here rather than in
    // production.

    #[test]
    fn supports_type_accepts_large_list_of_dict() {
        let dt = DataType::LargeList(Arc::new(Field::new("item", dict_utf8(), true)));
        assert!(RowsGroupColumn::supports_type(&dt));
    }

    #[test]
    fn supports_type_accepts_list_view_of_dict() {
        let dt = DataType::ListView(Arc::new(Field::new("item", dict_utf8(), true)));
        assert!(RowsGroupColumn::supports_type(&dt));
    }

    #[test]
    fn supports_type_accepts_large_list_view_of_dict() {
        let dt = DataType::LargeListView(Arc::new(Field::new("item", dict_utf8(), true)));
        assert!(RowsGroupColumn::supports_type(&dt));
    }

    #[test]
    fn supports_type_map_agrees_with_row_converter() {
        // Map<Int32, Dict<Int32, Utf8>>. Whether arrow-row supports Map
        // depends on the version; either way, our `supports_type` must
        // agree with `RowConverter::supports_fields` — otherwise we'd
        // pick a strategy the converter can't back.
        let entries = Arc::new(Field::new(
            "entries",
            DataType::Struct(
                vec![
                    Field::new("keys", DataType::Int32, false),
                    Field::new("values", dict_utf8(), true),
                ]
                .into(),
            ),
            false,
        ));
        let map_dt = DataType::Map(entries, false);
        let arrow_supports =
            RowConverter::supports_fields(&[SortField::new(map_dt.clone())]);
        assert_eq!(RowsGroupColumn::supports_type(&map_dt), arrow_supports);
    }

    /// End-to-end regression: `LargeList<Dict<Int32, Utf8>>` must
    /// actually survive `vectorized_append` + `build()` on the current
    /// arrow-rs version, not just be accepted by `supports_type`.
    #[test]
    fn build_preserves_large_list_of_dictionary_schema() {
        use arrow::array::{DictionaryArray, LargeListArray, StringArray};
        use arrow::buffer::OffsetBuffer;

        let item_field = Arc::new(Field::new("item", dict_utf8(), true));
        let outer_dt = DataType::LargeList(Arc::clone(&item_field));

        // Skip if this arrow-rs version rejects the nesting (defensive:
        // the invariant we care about is `output().data_type() == declared`
        // conditional on `supports_type` saying yes).
        if !RowsGroupColumn::supports_type(&outer_dt) {
            return;
        }

        let mut col = Box::new(RowsGroupColumn::try_new(outer_dt.clone()).unwrap());

        let values = Arc::new(StringArray::from(vec!["a", "b"]));
        let keys = Int32Array::from(vec![0, 1]);
        let dict = DictionaryArray::<Int32Type>::try_new(keys, values).unwrap();
        let offsets = OffsetBuffer::<i64>::from_lengths([2]);
        let list = LargeListArray::try_new(
            Arc::clone(&item_field),
            offsets,
            Arc::new(dict),
            None,
        )
        .unwrap();

        col.vectorized_append(&(Arc::new(list) as ArrayRef), &[0])
            .unwrap();
        let built = col.build();
        assert_eq!(
            built.data_type(),
            &outer_dt,
            "LargeList<Dict>: build() must preserve the declared type",
        );
    }

    /// End-to-end regression for `Map<Int32, Dict<Int32, Utf8>>` when
    /// arrow-row supports it. Same intent as the LargeList test.
    #[test]
    fn build_preserves_map_of_dictionary_schema() {
        use arrow::array::{
            DictionaryArray, Int32Array, MapArray, StringArray, StructArray,
        };
        use arrow::buffer::OffsetBuffer;

        let key_field = Arc::new(Field::new("keys", DataType::Int32, false));
        let value_field = Arc::new(Field::new("values", dict_utf8(), true));
        let entries_field = Arc::new(Field::new(
            "entries",
            DataType::Struct(vec![(*key_field).clone(), (*value_field).clone()].into()),
            false,
        ));
        let outer_dt = DataType::Map(Arc::clone(&entries_field), false);

        if !RowsGroupColumn::supports_type(&outer_dt) {
            return;
        }

        let mut col = Box::new(RowsGroupColumn::try_new(outer_dt.clone()).unwrap());

        // One map entry: {1 -> "a"}.
        let keys = Arc::new(Int32Array::from(vec![1])) as ArrayRef;
        let values_arr = Arc::new(StringArray::from(vec!["a"]));
        let value_keys = Int32Array::from(vec![0]);
        let value_dict =
            DictionaryArray::<Int32Type>::try_new(value_keys, values_arr).unwrap();
        let entries = StructArray::try_new(
            vec![(*key_field).clone(), (*value_field).clone()].into(),
            vec![keys, Arc::new(value_dict)],
            None,
        )
        .unwrap();
        let offsets = OffsetBuffer::<i32>::from_lengths([1]);
        let map =
            MapArray::try_new(Arc::clone(&entries_field), offsets, entries, None, false)
                .unwrap();

        col.vectorized_append(&(Arc::new(map) as ArrayRef), &[0])
            .unwrap();
        let built = col.build();
        assert_eq!(
            built.data_type(),
            &outer_dt,
            "Map<..., Dict>: build() must preserve the declared type",
        );
    }

    // ---- Union / RunEndEncoded defensive rejection -----------------
    //
    // Before this PR both types were routed to `GroupValuesRows`
    // (`group_column_supported_type` didn't have a nested branch). This
    // PR added `is_nested`-based dispatch to `RowsGroupColumn`, which
    // would opt them in — but the arrow-row round-trip for these two
    // families hasn't been covered by our tests. Reject them here so
    // the pre-PR routing is preserved; drop the blacklist when the
    // round-trip matrix grows to include them.

    #[test]
    fn supports_type_rejects_union() {
        use arrow::datatypes::UnionFields;

        let fields = UnionFields::try_new(
            vec![0_i8, 1_i8],
            vec![
                Field::new("a", DataType::Int32, true),
                Field::new("b", DataType::Utf8, true),
            ],
        )
        .unwrap();
        let dt = DataType::Union(fields, arrow::datatypes::UnionMode::Dense);
        assert!(
            !RowsGroupColumn::supports_type(&dt),
            "Union must fall back to GroupValuesRows until arrow-row \
             round-trip is covered by our tests",
        );
    }

    #[test]
    fn supports_type_rejects_run_end_encoded_with_nested_values() {
        // REE with `is_nested() = true` (nested values) is what this PR
        // could otherwise opt into RowsGroupColumn; keep it on
        // GroupValuesRows.
        let list_of_i32 =
            DataType::List(Arc::new(Field::new("item", DataType::Int32, true)));
        let dt = DataType::RunEndEncoded(
            Arc::new(Field::new("run_ends", DataType::Int32, false)),
            Arc::new(Field::new("values", list_of_i32, true)),
        );
        assert!(!RowsGroupColumn::supports_type(&dt));
    }

    #[test]
    fn supports_type_rejects_run_end_encoded_with_scalar_values() {
        // REE with scalar values is `is_nested() == false`, so
        // `group_column_supported_type` never routes it to us via the
        // nested branch anyway — but pin the invariant explicitly so a
        // future refactor doesn't accidentally opt it in.
        let dt = DataType::RunEndEncoded(
            Arc::new(Field::new("run_ends", DataType::Int32, false)),
            Arc::new(Field::new("values", DataType::Utf8, true)),
        );
        assert!(!RowsGroupColumn::supports_type(&dt));
    }

    #[test]
    fn supports_type_rejects_ree_hidden_under_outer_wrapper() {
        // REE buried under a struct or list: still rejected because
        // the wrapper's decoder recurses through the REE branch we
        // haven't validated.
        let ree = DataType::RunEndEncoded(
            Arc::new(Field::new("run_ends", DataType::Int32, false)),
            Arc::new(Field::new("values", DataType::Utf8, true)),
        );
        let outer = DataType::Struct(vec![Field::new("f", ree, true)].into());
        assert!(!RowsGroupColumn::supports_type(&outer));
    }

    #[test]
    fn supports_type_accepts_plain_list_and_struct_still() {
        // Sanity: the defensive Union/REE blacklist must not accidentally
        // catch the well-tested list-likes / structs that this column
        // exists to serve.
        let list_of_int =
            DataType::List(Arc::new(Field::new("item", DataType::Int32, true)));
        assert!(RowsGroupColumn::supports_type(&list_of_int));

        let struct_of_prims = DataType::Struct(
            vec![
                Field::new("a", DataType::Int32, true),
                Field::new("b", DataType::Utf8, true),
            ]
            .into(),
        );
        assert!(RowsGroupColumn::supports_type(&struct_of_prims));
    }
}
