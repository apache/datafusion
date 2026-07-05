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

//! [`GroupColumn`] implementation for `FixedSizeList<primitive>`.
//!
//! Storage: outer null bitmap + a child [`PrimitiveGroupValueBuilder`] that
//! holds every element flat (length = outer_len * list_len). The j-th element
//! of the i-th outer row lives at child index `i * list_len + j`.

use crate::aggregates::group_values::HashValue;
use crate::aggregates::group_values::multi_group_by::primitive::PrimitiveGroupValueBuilder;
use crate::aggregates::group_values::multi_group_by::{GroupColumn, nulls_equal_to};
use crate::aggregates::group_values::null_builder::MaybeNullBufferBuilder;

use arrow::array::{
    Array, ArrayRef, ArrowPrimitiveType, BooleanBufferBuilder, FixedSizeListArray,
};
use arrow::datatypes::{DataType, FieldRef};
use datafusion_common::Result;
use std::sync::Arc;

/// A [`GroupColumn`] for `FixedSizeList<T>` where `T` is a primitive type.
pub struct FixedSizeListGroupValueBuilder<T: ArrowPrimitiveType> {
    /// Child field, cached for `build` / `take_n`.
    field: FieldRef,
    /// List length per outer row.
    list_len: i32,
    /// Outer-level null bitmap.
    outer_nulls: MaybeNullBufferBuilder,
    /// Number of outer rows accumulated.
    outer_len: usize,
    /// Flat storage for child elements; always treated as nullable so it can
    /// hold child nulls regardless of the outer row's nullability.
    child: PrimitiveGroupValueBuilder<T, true>,
}

impl<T> FixedSizeListGroupValueBuilder<T>
where
    T: ArrowPrimitiveType,
    T::Native: HashValue,
{
    pub fn new(data_type: &DataType) -> Self {
        let (field, list_len) = match data_type {
            DataType::FixedSizeList(f, n) => (Arc::clone(f), *n),
            other => unreachable!(
                "FixedSizeListGroupValueBuilder built with non-FixedSizeList type {other:?}"
            ),
        };
        assert!(
            list_len >= 0,
            "FixedSizeListGroupValueBuilder requires non-negative list size (allow-list / dispatcher should have rejected earlier), got {list_len}"
        );
        let child = PrimitiveGroupValueBuilder::<T, true>::new(field.data_type().clone());
        Self {
            field,
            list_len,
            outer_nulls: MaybeNullBufferBuilder::new(),
            outer_len: 0,
            child,
        }
    }

    fn list_len_usize(&self) -> usize {
        // `try_from` (vs `as usize`) so any future invariant break
        // surfaces as a panic instead of a silent wrap.
        usize::try_from(self.list_len)
            .expect("list_len validated >= 0 in `new`; conversion to usize cannot fail")
    }
}

impl<T> GroupColumn for FixedSizeListGroupValueBuilder<T>
where
    T: ArrowPrimitiveType,
    T::Native: HashValue,
{
    fn equal_to(&self, lhs_row: usize, array: &ArrayRef, rhs_row: usize) -> bool {
        let lhs_null = self.outer_nulls.is_null(lhs_row);
        let rhs_null = array.is_null(rhs_row);
        if let Some(result) = nulls_equal_to(lhs_null, rhs_null) {
            return result;
        }

        let list_array = array
            .as_any()
            .downcast_ref::<FixedSizeListArray>()
            .expect("FixedSizeListGroupValueBuilder called with non-FixedSizeList array");
        // Use the borrowed child array + `value_offset` (same approach as
        // `append_val` below) instead of `list_array.value(rhs_row)`,
        // which would allocate a fresh sliced `ArrayRef` on every
        // equality check inside grouping's hot path.
        let child_array = list_array.values();
        let list_len = self.list_len_usize();
        let lhs_base = lhs_row * list_len;
        let rhs_base = list_array.value_offset(rhs_row) as usize;
        for j in 0..list_len {
            if !self.child.equal_to(lhs_base + j, child_array, rhs_base + j) {
                return false;
            }
        }
        true
    }

    fn append_val(&mut self, array: &ArrayRef, row: usize) -> Result<()> {
        let list_array = array
            .as_any()
            .downcast_ref::<FixedSizeListArray>()
            .expect("FixedSizeListGroupValueBuilder called with non-FixedSizeList array");
        self.outer_nulls.append(list_array.is_null(row));
        self.outer_len += 1;
        let child_array = list_array.values();
        let list_len = self.list_len_usize();
        // Use the array's own `value_offset` rather than computing `(offset
        // + row) * list_len` ourselves. For sliced FixedSizeListArrays the
        // `values()` slice is already advanced, so doing the arithmetic
        // manually risks double-applying any future offset behavior.
        let start = list_array.value_offset(row) as usize;
        for j in 0..list_len {
            self.child.append_val(child_array, start + j)?;
        }
        Ok(())
    }

    fn vectorized_equal_to(
        &self,
        lhs_rows: &[usize],
        array: &ArrayRef,
        rhs_rows: &[usize],
        equal_to_results: &mut BooleanBufferBuilder,
    ) {
        for (idx, (&lhs_row, &rhs_row)) in
            lhs_rows.iter().zip(rhs_rows.iter()).enumerate()
        {
            if !equal_to_results.get_bit(idx) {
                continue;
            }
            if !self.equal_to(lhs_row, array, rhs_row) {
                equal_to_results.set_bit(idx, false);
            }
        }
    }

    fn vectorized_append(&mut self, array: &ArrayRef, rows: &[usize]) -> Result<()> {
        for &row in rows {
            self.append_val(array, row)?;
        }
        Ok(())
    }

    fn len(&self) -> usize {
        self.outer_len
    }

    fn size(&self) -> usize {
        self.outer_nulls.allocated_size() + self.child.size()
    }

    fn build(self: Box<Self>) -> ArrayRef {
        let Self {
            field,
            list_len,
            mut outer_nulls,
            outer_len: _,
            child,
        } = *self;
        let outer_nulls = outer_nulls_take_build(&mut outer_nulls);
        let child_array = Box::new(child).build();
        Arc::new(FixedSizeListArray::new(
            field,
            list_len,
            child_array,
            outer_nulls,
        ))
    }

    fn take_n(&mut self, n: usize) -> ArrayRef {
        let list_len = self.list_len_usize();
        let first_n_outer_nulls = self.outer_nulls.take_n(n);
        // Drain a `PrimitiveGroupValueBuilder` cleanly through its
        // `GroupColumn` impl; this also shifts the remaining elements down.
        let first_n_child: ArrayRef =
            <PrimitiveGroupValueBuilder<T, true> as GroupColumn>::take_n(
                &mut self.child,
                n * list_len,
            );
        self.outer_len -= n;
        Arc::new(FixedSizeListArray::new(
            Arc::clone(&self.field),
            self.list_len,
            first_n_child,
            first_n_outer_nulls,
        ))
    }
}

/// Helper: consume the inner builder once on `build` to release its buffer.
fn outer_nulls_take_build(
    nulls: &mut MaybeNullBufferBuilder,
) -> Option<arrow::buffer::NullBuffer> {
    std::mem::replace(nulls, MaybeNullBufferBuilder::new()).build()
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{
        ArrayRef, FixedSizeListArray, Int32Array, builder::FixedSizeListBuilder,
        builder::Int32Builder,
    };
    use arrow::datatypes::Int32Type;
    use std::sync::Arc;

    /// Build a FixedSizeList<Int32, size=2> array from a sequence of optional rows.
    /// `None` outer rows are null lists. Within a non-null row, inner values come
    /// from the given `[Option<i32>; 2]`.
    fn fsl_array(rows: &[Option<[Option<i32>; 2]>]) -> ArrayRef {
        let inner = Int32Builder::new();
        let mut builder = FixedSizeListBuilder::new(inner, 2);
        for row in rows {
            match row {
                None => {
                    // Null outer row: push placeholder child values so the
                    // child array has the right length, then mark the outer
                    // row null via `append(false)`.
                    builder.values().append_value(0);
                    builder.values().append_value(0);
                    builder.append(false);
                }
                Some([a, b]) => {
                    match a {
                        Some(v) => builder.values().append_value(*v),
                        None => builder.values().append_null(),
                    }
                    match b {
                        Some(v) => builder.values().append_value(*v),
                        None => builder.values().append_null(),
                    }
                    builder.append(true);
                }
            }
        }
        Arc::new(builder.finish())
    }

    fn data_type(child_nullable: bool) -> DataType {
        DataType::FixedSizeList(
            Arc::new(arrow::datatypes::Field::new(
                "item",
                DataType::Int32,
                child_nullable,
            )),
            2,
        )
    }

    #[test]
    fn append_and_build_round_trips_values() {
        let input = fsl_array(&[
            Some([Some(1), Some(2)]),
            None,
            Some([Some(3), Some(4)]),
            Some([Some(1), Some(2)]),
        ]);
        let mut builder: FixedSizeListGroupValueBuilder<Int32Type> =
            FixedSizeListGroupValueBuilder::new(&data_type(true));

        for i in 0..input.len() {
            builder.append_val(&input, i).unwrap();
        }
        assert_eq!(builder.len(), 4);

        let out = Box::new(builder).build();
        let out_fsl = out.as_any().downcast_ref::<FixedSizeListArray>().unwrap();
        assert_eq!(out_fsl.len(), 4);
        assert!(out_fsl.is_null(1));
        assert!(!out_fsl.is_null(0));
        assert!(!out_fsl.is_null(2));
        assert!(!out_fsl.is_null(3));

        let values = out_fsl
            .values()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        // Row 0: [1, 2]
        assert_eq!(values.value(0), 1);
        assert_eq!(values.value(1), 2);
        // Row 2: [3, 4]  (row 1 is null, but child slot positions are 2..4)
        assert_eq!(values.value(4), 3);
        assert_eq!(values.value(5), 4);
        // Row 3: [1, 2]
        assert_eq!(values.value(6), 1);
        assert_eq!(values.value(7), 2);
    }

    #[test]
    fn equal_to_matches_identical_rows_and_rejects_different() {
        let stored = fsl_array(&[Some([Some(1), Some(2)]), Some([Some(3), Some(4)])]);
        let mut builder: FixedSizeListGroupValueBuilder<Int32Type> =
            FixedSizeListGroupValueBuilder::new(&data_type(true));
        builder.append_val(&stored, 0).unwrap();
        builder.append_val(&stored, 1).unwrap();

        let probe = fsl_array(&[
            Some([Some(1), Some(2)]), // == stored row 0
            Some([Some(3), Some(5)]), // != stored row 1 (last element differs)
            Some([Some(3), Some(4)]), // == stored row 1
        ]);

        assert!(builder.equal_to(0, &probe, 0), "[1,2] == [1,2]");
        assert!(
            !builder.equal_to(1, &probe, 1),
            "[3,4] != [3,5] (one element differs)"
        );
        assert!(builder.equal_to(1, &probe, 2), "[3,4] == [3,4]");
    }

    #[test]
    fn equal_to_handles_outer_nulls() {
        let stored = fsl_array(&[None, Some([Some(1), Some(2)])]);
        let mut builder: FixedSizeListGroupValueBuilder<Int32Type> =
            FixedSizeListGroupValueBuilder::new(&data_type(true));
        builder.append_val(&stored, 0).unwrap();
        builder.append_val(&stored, 1).unwrap();

        let probe =
            fsl_array(&[None, Some([Some(1), Some(2)]), Some([Some(0), Some(0)])]);

        assert!(builder.equal_to(0, &probe, 0), "null == null");
        assert!(!builder.equal_to(0, &probe, 1), "null != [1,2]");
        assert!(!builder.equal_to(1, &probe, 0), "[1,2] != null");
        assert!(builder.equal_to(1, &probe, 1), "[1,2] == [1,2]");
        assert!(!builder.equal_to(1, &probe, 2), "[1,2] != [0,0]");
    }

    #[test]
    fn equal_to_handles_inner_nulls() {
        let stored = fsl_array(&[Some([Some(1), None]), Some([None, None])]);
        let mut builder: FixedSizeListGroupValueBuilder<Int32Type> =
            FixedSizeListGroupValueBuilder::new(&data_type(true));
        builder.append_val(&stored, 0).unwrap();
        builder.append_val(&stored, 1).unwrap();

        let probe = fsl_array(&[
            Some([Some(1), None]),
            Some([Some(1), Some(2)]),
            Some([None, None]),
        ]);

        assert!(builder.equal_to(0, &probe, 0), "[1, null] == [1, null]");
        assert!(
            !builder.equal_to(0, &probe, 1),
            "[1, null] != [1, 2] (null vs value)"
        );
        assert!(
            builder.equal_to(1, &probe, 2),
            "[null, null] == [null, null]"
        );
    }

    #[test]
    fn dispatcher_routes_fixed_size_list_to_group_values_column() {
        // End-to-end: feed a schema with a FixedSizeList<Int32, size=2> column
        // through `new_group_values` and prove that `GroupValuesColumn` (not
        // `GroupValuesRows`) handles the intern. The behavioral signal is that
        // dedup works correctly across batches.
        use crate::aggregates::group_values::new_group_values;
        use crate::aggregates::order::GroupOrdering;
        use arrow::datatypes::{Field, Schema};
        use datafusion_expr::EmitTo;

        let schema =
            Arc::new(Schema::new(vec![Field::new("tags", data_type(true), true)]));

        let mut gv = new_group_values(schema, &GroupOrdering::None).unwrap();

        // Batch 1: three rows, two distinct values.
        let batch1: ArrayRef = fsl_array(&[
            Some([Some(1), Some(2)]),
            Some([Some(3), Some(4)]),
            Some([Some(1), Some(2)]),
        ]);
        let mut groups = Vec::new();
        gv.intern(&[batch1], &mut groups).unwrap();
        assert_eq!(
            groups,
            vec![0, 1, 0],
            "first batch: dedup [1,2] across rows"
        );

        // Batch 2: revisit existing groups + introduce a new one.
        let batch2: ArrayRef = fsl_array(&[
            Some([Some(3), Some(4)]), // existing group 1
            Some([Some(5), Some(6)]), // new group 2
            Some([Some(1), Some(2)]), // existing group 0
        ]);
        let mut groups = Vec::new();
        gv.intern(&[batch2], &mut groups).unwrap();
        assert_eq!(
            groups,
            vec![1, 2, 0],
            "second batch: dedup hits across batches"
        );

        assert_eq!(gv.len(), 3, "exactly three distinct group keys retained");

        // Emit and verify the materialized group keys.
        let arrays = gv.emit(EmitTo::All).unwrap();
        assert_eq!(arrays.len(), 1);
        let out = arrays[0]
            .as_any()
            .downcast_ref::<FixedSizeListArray>()
            .unwrap();
        assert_eq!(out.len(), 3);
        let values = out.values().as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(values.values(), &[1, 2, 3, 4, 5, 6]);
    }

    /// `FixedSizeList` carries its size as `i32`; the enum lets callers
    /// construct a negative size programmatically even though Arrow
    /// considers it invalid. The supported-type allow-list must reject
    /// such a type so it never reaches the builder path.
    #[test]
    fn negative_list_size_rejected_by_allow_list() {
        use crate::aggregates::group_values::multi_group_by::group_column_supported_type;
        use arrow::datatypes::Field;

        let invalid = DataType::FixedSizeList(
            Arc::new(Field::new("item", DataType::Int32, true)),
            -1,
        );

        assert!(
            !group_column_supported_type(&invalid),
            "allow-list must reject FixedSizeList(_, -1)"
        );
    }

    /// The dispatcher is a belt-and-suspenders guard for direct callers
    /// that bypass the allow-list. A negative `list_size` must panic
    /// there rather than wrap in the builder.
    #[test]
    #[should_panic(expected = "FixedSizeList requires non-negative size")]
    fn negative_list_size_panics_in_dispatcher() {
        use crate::aggregates::group_values::multi_group_by::make_group_column;
        use arrow::datatypes::Field;

        let invalid = DataType::FixedSizeList(
            Arc::new(Field::new("item", DataType::Int32, true)),
            -1,
        );
        let field = Field::new("col", invalid, true);
        let _ = make_group_column(&field);
    }

    #[test]
    fn take_n_returns_prefix_and_shifts_remainder() {
        let input = fsl_array(&[
            Some([Some(1), Some(2)]),
            Some([Some(3), Some(4)]),
            Some([Some(5), Some(6)]),
        ]);
        let mut builder: FixedSizeListGroupValueBuilder<Int32Type> =
            FixedSizeListGroupValueBuilder::new(&data_type(true));
        for i in 0..input.len() {
            builder.append_val(&input, i).unwrap();
        }
        assert_eq!(builder.len(), 3);

        let first_two = builder.take_n(2);
        let first_two_fsl = first_two
            .as_any()
            .downcast_ref::<FixedSizeListArray>()
            .unwrap();
        assert_eq!(first_two_fsl.len(), 2);
        let v = first_two_fsl
            .values()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(v.values(), &[1, 2, 3, 4]);

        // Remaining builder should now contain only [5, 6].
        assert_eq!(builder.len(), 1);
        let rest = Box::new(builder).build();
        let rest_fsl = rest.as_any().downcast_ref::<FixedSizeListArray>().unwrap();
        assert_eq!(rest_fsl.len(), 1);
        let v = rest_fsl
            .values()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(v.values(), &[5, 6]);
    }

    #[test]
    fn handles_sliced_input_array() {
        // Build a 5-row array, slice off the first 2 rows, and verify
        // append_val + equal_to operate on the correct logical positions.
        let full = fsl_array(&[
            Some([Some(99), Some(99)]), // sliced off
            Some([Some(98), Some(98)]), // sliced off
            Some([Some(1), Some(2)]),
            None,
            Some([Some(3), Some(4)]),
        ]);
        let sliced = full.slice(2, 3);

        let mut builder: FixedSizeListGroupValueBuilder<Int32Type> =
            FixedSizeListGroupValueBuilder::new(&data_type(true));
        for i in 0..sliced.len() {
            builder.append_val(&sliced, i).unwrap();
        }

        // Probe with an UNSLICED equivalent.
        let probe = fsl_array(&[
            Some([Some(1), Some(2)]),
            None,
            Some([Some(3), Some(4)]),
            Some([Some(99), Some(99)]),
        ]);
        assert!(builder.equal_to(0, &probe, 0), "sliced row 0 == [1,2]");
        assert!(builder.equal_to(1, &probe, 1), "sliced row 1 == null");
        assert!(builder.equal_to(2, &probe, 2), "sliced row 2 == [3,4]");
        assert!(
            !builder.equal_to(0, &probe, 3),
            "sliced row 0 ([1,2]) != [99,99]"
        );

        // Probe with the SAME sliced array to make sure equal_to with sliced
        // rhs also works.
        for i in 0..sliced.len() {
            assert!(
                builder.equal_to(i, &sliced, i),
                "row {i} of sliced array must equal itself"
            );
        }

        let out = Box::new(builder).build();
        let out = out.as_any().downcast_ref::<FixedSizeListArray>().unwrap();
        assert_eq!(out.len(), 3);
        assert!(out.is_null(1));
        let v = out.values().as_any().downcast_ref::<Int32Array>().unwrap();
        // Output stores: [1,2], placeholder for null, [3,4]. Placeholder is
        // whatever the source had — for our null builder we pushed actual
        // values 0,0 from the test fixture, so the child layout is [1,2,0,0,3,4].
        assert_eq!(v.value(0), 1);
        assert_eq!(v.value(1), 2);
        assert_eq!(v.value(4), 3);
        assert_eq!(v.value(5), 4);
    }

    #[test]
    fn take_n_zero_and_full() {
        let input = fsl_array(&[Some([Some(1), Some(2)]), Some([Some(3), Some(4)])]);
        let mut builder: FixedSizeListGroupValueBuilder<Int32Type> =
            FixedSizeListGroupValueBuilder::new(&data_type(true));
        for i in 0..input.len() {
            builder.append_val(&input, i).unwrap();
        }

        // take_n(0): empty prefix, remainder unchanged.
        let none = builder.take_n(0);
        let none_fsl = none.as_any().downcast_ref::<FixedSizeListArray>().unwrap();
        assert_eq!(none_fsl.len(), 0);
        assert_eq!(builder.len(), 2);

        // take_n(len): full prefix, empty remainder.
        let all = builder.take_n(2);
        let all_fsl = all.as_any().downcast_ref::<FixedSizeListArray>().unwrap();
        assert_eq!(all_fsl.len(), 2);
        let v = all_fsl
            .values()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(v.values(), &[1, 2, 3, 4]);
        assert_eq!(builder.len(), 0, "builder drained after take_n(len)");
    }

    #[test]
    fn take_n_with_null_outer_in_prefix() {
        let input =
            fsl_array(&[Some([Some(1), Some(2)]), None, Some([Some(3), Some(4)])]);
        let mut builder: FixedSizeListGroupValueBuilder<Int32Type> =
            FixedSizeListGroupValueBuilder::new(&data_type(true));
        for i in 0..input.len() {
            builder.append_val(&input, i).unwrap();
        }

        let first_two = builder.take_n(2);
        let first_two = first_two
            .as_any()
            .downcast_ref::<FixedSizeListArray>()
            .unwrap();
        assert_eq!(first_two.len(), 2);
        assert!(!first_two.is_null(0));
        assert!(first_two.is_null(1), "null row preserved in take_n prefix");

        // Remaining = row 2 only.
        let rest = Box::new(builder).build();
        let rest = rest.as_any().downcast_ref::<FixedSizeListArray>().unwrap();
        assert_eq!(rest.len(), 1);
        assert!(!rest.is_null(0));
        let v = rest.values().as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(v.values(), &[3, 4]);
    }

    #[test]
    fn vectorized_methods_match_per_row() {
        // Build two builders, one via per-row append/equal, one via vectorized
        // append/equal, and prove they produce the same output and decisions.
        let input = fsl_array(&[
            Some([Some(1), Some(2)]),
            None,
            Some([Some(3), Some(4)]),
            Some([Some(1), Some(2)]),
        ]);

        let mut per_row: FixedSizeListGroupValueBuilder<Int32Type> =
            FixedSizeListGroupValueBuilder::new(&data_type(true));
        for i in 0..input.len() {
            per_row.append_val(&input, i).unwrap();
        }

        let mut vec_b: FixedSizeListGroupValueBuilder<Int32Type> =
            FixedSizeListGroupValueBuilder::new(&data_type(true));
        vec_b.vectorized_append(&input, &[0, 1, 2, 3]).unwrap();
        assert_eq!(vec_b.len(), per_row.len());

        // Vectorized equal_to should produce the same per-row decisions.
        let lhs = vec![0usize, 1, 2, 3];
        let rhs = vec![0usize, 1, 2, 0];
        let mut bb = BooleanBufferBuilder::new(4);
        bb.append_n(4, true);
        vec_b.vectorized_equal_to(&lhs, &input, &rhs, &mut bb);
        for idx in 0..4 {
            let expected = per_row.equal_to(lhs[idx], &input, rhs[idx]);
            assert_eq!(
                bb.get_bit(idx),
                expected,
                "row {idx}: vectorized={} per-row={expected}",
                bb.get_bit(idx),
            );
        }

        // A pre-set false entry must NOT be flipped back to true.
        let mut bb = BooleanBufferBuilder::new(4);
        bb.append_n(4, true);
        bb.set_bit(0, false);
        vec_b.vectorized_equal_to(&lhs, &input, &rhs, &mut bb);
        assert!(!bb.get_bit(0), "pre-set false must stay false");
    }

    #[test]
    fn size_grows_with_appends() {
        let input = fsl_array(&[Some([Some(1), Some(2)])]);
        let mut builder: FixedSizeListGroupValueBuilder<Int32Type> =
            FixedSizeListGroupValueBuilder::new(&data_type(true));
        let s0 = builder.size();
        for _ in 0..16 {
            builder.append_val(&input, 0).unwrap();
        }
        let s1 = builder.size();
        assert!(s1 > s0, "size should grow after appends ({s0} -> {s1})");
    }

    #[test]
    fn build_empty_builder_returns_empty_array() {
        let builder: FixedSizeListGroupValueBuilder<Int32Type> =
            FixedSizeListGroupValueBuilder::new(&data_type(true));
        let out = Box::new(builder).build();
        let out = out.as_any().downcast_ref::<FixedSizeListArray>().unwrap();
        assert_eq!(out.len(), 0);
    }
}
