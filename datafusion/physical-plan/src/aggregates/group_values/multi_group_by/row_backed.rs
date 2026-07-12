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
//! and only pays the row-encoding cost on `struct_col`, instead of dragging the
//! entire key onto `GroupValuesRows`.
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
use crate::aggregates::group_values::row::dictionary_encode_if_necessary;

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

impl RowsGroupColumn {
    /// Returns whether `data_type` can be handled by this generic column, i.e.
    /// whether arrow's [`RowConverter`] can encode it.
    pub fn supports_type(data_type: &DataType) -> bool {
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
        dictionary_encode_if_necessary(&array, &self.output_type)
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
}
