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

//! [`GroupColumn`] implementation for `List<T>` and `LargeList<T>` group keys.
//!
//! Storage:
//! - `offsets: Vec<O>` of length `outer_len + 1`, with `offsets[0] = 0`. The
//!   children of outer row `i` live at child positions
//!   `[offsets[i] .. offsets[i+1])`.
//! - `child: Box<dyn GroupColumn>` for the element type.
//! - `outer_nulls`: outer-row null bitmap.
//!
//! Null outer rows are stored with a zero-length range (i.e.
//! `offsets[i+1] == offsets[i]`). No child slots are pushed for null rows.

use crate::aggregates::group_values::multi_group_by::{GroupColumn, nulls_equal_to};
use crate::aggregates::group_values::null_builder::MaybeNullBufferBuilder;

use arrow::array::{
    Array, ArrayRef, BooleanBufferBuilder, GenericListArray, OffsetSizeTrait,
};
use arrow::buffer::{OffsetBuffer, ScalarBuffer};
use arrow::datatypes::FieldRef;
use datafusion_common::{Result, internal_datafusion_err};
use std::mem::size_of_val;
use std::sync::Arc;

/// A [`GroupColumn`] for `List<T>` (`O = i32`) and `LargeList<T>` (`O = i64`).
pub struct ListGroupValueBuilder<O: OffsetSizeTrait> {
    field: FieldRef,
    offsets: Vec<O>,
    child: Box<dyn GroupColumn>,
    outer_nulls: MaybeNullBufferBuilder,
    outer_len: usize,
}

impl<O: OffsetSizeTrait> ListGroupValueBuilder<O> {
    pub fn new(field: FieldRef, child: Box<dyn GroupColumn>) -> Self {
        Self {
            field,
            offsets: vec![O::usize_as(0)],
            child,
            outer_nulls: MaybeNullBufferBuilder::new(),
            outer_len: 0,
        }
    }

    fn list_array<'a>(array: &'a ArrayRef) -> &'a GenericListArray<O> {
        array
            .as_any()
            .downcast_ref::<GenericListArray<O>>()
            .expect("ListGroupValueBuilder called with non-List/LargeList array")
    }

    fn current_end(&self) -> O {
        *self.offsets.last().expect("offsets is never empty")
    }

    fn push_offset(&mut self, additional: usize) -> Result<()> {
        let next = self.current_end().as_usize() + additional;
        let next_o = O::from_usize(next)
            .ok_or_else(|| internal_datafusion_err!("List offset overflows {}", next))?;
        self.offsets.push(next_o);
        Ok(())
    }
}

impl<O: OffsetSizeTrait> GroupColumn for ListGroupValueBuilder<O> {
    fn equal_to(&self, lhs_row: usize, array: &ArrayRef, rhs_row: usize) -> bool {
        let lhs_null = self.outer_nulls.is_null(lhs_row);
        let rhs_null = array.is_null(rhs_row);
        if let Some(result) = nulls_equal_to(lhs_null, rhs_null) {
            return result;
        }

        let l = Self::list_array(array);
        // sliced child covering exactly the rhs row's elements
        let rhs_sublist: ArrayRef = l.value(rhs_row);
        let lhs_start = self.offsets[lhs_row].as_usize();
        let lhs_end = self.offsets[lhs_row + 1].as_usize();
        let lhs_len = lhs_end - lhs_start;
        if lhs_len != rhs_sublist.len() {
            return false;
        }
        for j in 0..lhs_len {
            if !self.child.equal_to(lhs_start + j, &rhs_sublist, j) {
                return false;
            }
        }
        true
    }

    fn append_val(&mut self, array: &ArrayRef, row: usize) -> Result<()> {
        let l = Self::list_array(array);
        if l.is_null(row) {
            self.outer_nulls.append(true);
            // Zero-length range for null outer rows: do not push any child
            // elements, and the offset for the next row stays the same.
            let end = self.current_end();
            self.offsets.push(end);
        } else {
            self.outer_nulls.append(false);
            let sublist: ArrayRef = l.value(row);
            let n = sublist.len();
            for j in 0..n {
                self.child.append_val(&sublist, j)?;
            }
            self.push_offset(n)?;
        }
        self.outer_len += 1;
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
        size_of_val(self.offsets.as_slice())
            + self.outer_nulls.allocated_size()
            + self.child.size()
    }

    fn build(self: Box<Self>) -> ArrayRef {
        let Self {
            field,
            offsets,
            child,
            mut outer_nulls,
            outer_len: _,
        } = *self;
        let outer_nulls =
            std::mem::replace(&mut outer_nulls, MaybeNullBufferBuilder::new()).build();
        let child_array = child.build();
        // SAFETY: offsets are constructed monotonically by `push_offset` /
        // initial `[0]`, and child_array length matches the final offset.
        let offset_buffer = OffsetBuffer::<O>::new(ScalarBuffer::<O>::from(offsets));
        Arc::new(GenericListArray::<O>::new(
            field,
            offset_buffer,
            child_array,
            outer_nulls,
        ))
    }

    fn take_n(&mut self, n: usize) -> ArrayRef {
        // Number of child elements consumed by the first n outer rows.
        let cut = self.offsets[n].as_usize();
        let cut_offset = self.offsets[n];

        // First-n offsets: 0, off[1], ..., off[n].
        let first_n_offsets: Vec<O> = self.offsets[..=n].to_vec();

        // Remaining offsets shifted so that what was offsets[n] becomes 0.
        let mut remaining = Vec::with_capacity(self.offsets.len() - n);
        for i in n..self.offsets.len() {
            remaining.push(self.offsets[i] - cut_offset);
        }
        self.offsets = remaining;

        let first_n_outer_nulls = self.outer_nulls.take_n(n);
        let first_n_child = self.child.take_n(cut);
        self.outer_len -= n;

        let offset_buffer =
            OffsetBuffer::<O>::new(ScalarBuffer::<O>::from(first_n_offsets));
        Arc::new(GenericListArray::<O>::new(
            Arc::clone(&self.field),
            offset_buffer,
            first_n_child,
            first_n_outer_nulls,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::aggregates::group_values::multi_group_by::primitive::PrimitiveGroupValueBuilder;
    use arrow::array::{
        Int32Array, LargeListArray, ListArray, builder::Int32Builder,
        builder::LargeListBuilder, builder::ListBuilder,
    };
    use arrow::datatypes::{DataType, Field, Int32Type};

    fn child_field() -> FieldRef {
        Arc::new(Field::new("item", DataType::Int32, true))
    }

    fn list_array(rows: &[Option<Vec<Option<i32>>>]) -> ArrayRef {
        let mut b = ListBuilder::new(Int32Builder::new());
        for row in rows {
            match row {
                None => b.append(false),
                Some(items) => {
                    for v in items {
                        b.values().append_option(*v);
                    }
                    b.append(true);
                }
            }
        }
        Arc::new(b.finish())
    }

    fn large_list_array(rows: &[Option<Vec<Option<i32>>>]) -> ArrayRef {
        let mut b = LargeListBuilder::new(Int32Builder::new());
        for row in rows {
            match row {
                None => b.append(false),
                Some(items) => {
                    for v in items {
                        b.values().append_option(*v);
                    }
                    b.append(true);
                }
            }
        }
        Arc::new(b.finish())
    }

    fn make_child() -> Box<dyn GroupColumn> {
        Box::new(PrimitiveGroupValueBuilder::<Int32Type, true>::new(
            DataType::Int32,
        ))
    }

    #[test]
    fn list_append_equal_build_round_trip() {
        let input = list_array(&[
            Some(vec![Some(1), Some(2)]),
            Some(vec![Some(3)]),
            None,
            Some(vec![Some(1), Some(2)]),
            Some(vec![]),
        ]);
        let mut builder = ListGroupValueBuilder::<i32>::new(child_field(), make_child());
        for i in 0..input.len() {
            builder.append_val(&input, i).unwrap();
        }
        assert_eq!(builder.len(), 5);

        // Verify equality matrix.
        let probe = list_array(&[
            Some(vec![Some(1), Some(2)]),
            Some(vec![Some(1), Some(3)]),
            None,
            Some(vec![]),
        ]);
        assert!(builder.equal_to(0, &probe, 0), "[1,2] == [1,2]");
        assert!(!builder.equal_to(0, &probe, 1), "[1,2] != [1,3]");
        assert!(builder.equal_to(2, &probe, 2), "null == null");
        assert!(!builder.equal_to(0, &probe, 2), "[1,2] != null");
        assert!(builder.equal_to(4, &probe, 3), "[] == []");
        assert!(!builder.equal_to(0, &probe, 3), "[1,2] != []");

        let out = Box::new(builder).build();
        let out_list = out.as_any().downcast_ref::<ListArray>().unwrap();
        assert_eq!(out_list.len(), 5);
        assert!(out_list.is_null(2));
        // Verify values for non-null rows.
        let v0 = out_list.value(0);
        let v0 = v0.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(v0.values(), &[1, 2]);
        let v1 = out_list.value(1);
        let v1 = v1.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(v1.values(), &[3]);
        let v3 = out_list.value(3);
        let v3 = v3.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(v3.values(), &[1, 2]);
        assert_eq!(out_list.value_length(4), 0);
    }

    #[test]
    fn large_list_round_trip() {
        let input = large_list_array(&[
            Some(vec![Some(7), Some(8), Some(9)]),
            None,
            Some(vec![Some(10)]),
        ]);
        let mut builder = ListGroupValueBuilder::<i64>::new(child_field(), make_child());
        for i in 0..input.len() {
            builder.append_val(&input, i).unwrap();
        }
        let out = Box::new(builder).build();
        let out_list = out.as_any().downcast_ref::<LargeListArray>().unwrap();
        assert_eq!(out_list.len(), 3);
        assert!(out_list.is_null(1));
        let v0 = out_list.value(0);
        let v0 = v0.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(v0.values(), &[7, 8, 9]);
        let v2 = out_list.value(2);
        let v2 = v2.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(v2.values(), &[10]);
    }

    #[test]
    fn list_take_n_splits_offsets_and_child() {
        let input = list_array(&[
            Some(vec![Some(1), Some(2)]),
            Some(vec![Some(3)]),
            Some(vec![Some(4), Some(5), Some(6)]),
        ]);
        let mut builder = ListGroupValueBuilder::<i32>::new(child_field(), make_child());
        for i in 0..input.len() {
            builder.append_val(&input, i).unwrap();
        }
        let first = builder.take_n(2);
        let first_list = first.as_any().downcast_ref::<ListArray>().unwrap();
        assert_eq!(first_list.len(), 2);
        let v0 = first_list.value(0);
        let v0 = v0.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(v0.values(), &[1, 2]);
        let v1 = first_list.value(1);
        let v1 = v1.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(v1.values(), &[3]);

        // Remaining = row 2 only.
        assert_eq!(builder.len(), 1);
        let rest = Box::new(builder).build();
        let rest_list = rest.as_any().downcast_ref::<ListArray>().unwrap();
        assert_eq!(rest_list.len(), 1);
        let v = rest_list.value(0);
        let v = v.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(v.values(), &[4, 5, 6]);
    }

    #[test]
    fn large_list_of_struct_round_trip_through_new_group_values() {
        // Composition test: LargeList<Struct<id: Utf8, n: Int32>>, the same
        // shape as a SEC Form 4 `footnotes` column (`LargeList<Struct<id:
        // Utf8, description: Utf8>>`). Proves the recursive factory wires
        // List/Struct together correctly and that intern/emit round-trips
        // through `GroupValuesColumn` rather than `GroupValuesRows`.
        use crate::aggregates::group_values::new_group_values;
        use crate::aggregates::order::GroupOrdering;
        use arrow::array::{
            Int32Array, LargeListArray, StringArray, StructArray, builder::Int32Builder,
            builder::LargeListBuilder, builder::StringBuilder, builder::StructBuilder,
        };
        use arrow::datatypes::{Fields, Schema};
        use datafusion_expr::EmitTo;

        let struct_fields = Fields::from(vec![
            Field::new("id", DataType::Utf8, true),
            Field::new("n", DataType::Int32, true),
        ]);
        let element_field = Arc::new(Field::new(
            "element",
            DataType::Struct(struct_fields.clone()),
            true,
        ));
        let column_field = Field::new(
            "notes",
            DataType::LargeList(Arc::clone(&element_field)),
            true,
        );
        let schema = Arc::new(Schema::new(vec![column_field]));

        // Build LargeList<Struct<id: Utf8, n: Int32>> arrays.
        //
        // notes_v(rows) where each row is Option<Vec<(Option<&str>, Option<i32>)>>:
        //   None       -> null outer list
        //   Some(vec![]) -> empty list
        //   Some(vec![(id, n), ...]) -> list with struct entries
        let notes_v = |rows: &[Option<Vec<(Option<&str>, Option<i32>)>>]| -> ArrayRef {
            let struct_builder = StructBuilder::new(
                struct_fields.clone(),
                vec![
                    Box::new(StringBuilder::new()),
                    Box::new(Int32Builder::new()),
                ],
            );
            let mut list_builder = LargeListBuilder::new(struct_builder)
                .with_field(Arc::clone(&element_field));
            for row in rows {
                match row {
                    None => list_builder.append(false),
                    Some(items) => {
                        for (id, n) in items {
                            let s = list_builder.values();
                            s.field_builder::<StringBuilder>(0)
                                .unwrap()
                                .append_option(id.map(|x| x.to_string()));
                            s.field_builder::<Int32Builder>(1)
                                .unwrap()
                                .append_option(*n);
                            s.append(true);
                        }
                        list_builder.append(true);
                    }
                }
            }
            Arc::new(list_builder.finish())
        };

        let mut gv = new_group_values(schema, &GroupOrdering::None).unwrap();

        // Batch 1: a mix of duplicate / distinct / null lists.
        let batch1 = notes_v(&[
            Some(vec![(Some("a"), Some(1))]), // 0
            Some(vec![(Some("a"), Some(1))]), // dup of 0
            Some(vec![(Some("a"), Some(2))]), // distinct (n differs)
            None,                             // null
            Some(vec![(Some("a"), Some(1)), (Some("b"), Some(2))]), // distinct (length differs)
            None,                                                   // dup of null
            Some(vec![]),                                           // empty list
        ]);
        let mut groups = Vec::new();
        gv.intern(&[batch1], &mut groups).unwrap();
        assert_eq!(
            groups,
            vec![0, 0, 1, 2, 3, 2, 4],
            "composition dedup: same struct list -> same group; null and empty are distinct"
        );

        // Batch 2: cross-batch dedup including a brand-new struct value.
        let batch2 = notes_v(&[
            Some(vec![(Some("a"), Some(1))]), // existing 0
            Some(vec![(Some("a"), Some(1)), (Some("b"), Some(2))]), // existing 3
            Some(vec![(Some("z"), Some(99))]), // new 5
            None,                             // existing 2
        ]);
        let mut groups = Vec::new();
        gv.intern(&[batch2], &mut groups).unwrap();
        assert_eq!(groups, vec![0, 3, 5, 2]);
        assert_eq!(gv.len(), 6, "five distinct lists + one null group");

        // Emit and sanity-check the materialized shape.
        let out = gv.emit(EmitTo::All).unwrap();
        let ll = out[0].as_any().downcast_ref::<LargeListArray>().unwrap();
        assert_eq!(ll.len(), 6);
        assert!(ll.is_null(2), "row 2 was the null-list group");
        // row 0 == [(a, 1)]
        let row0 = ll.value(0);
        let row0 = row0.as_any().downcast_ref::<StructArray>().unwrap();
        assert_eq!(row0.len(), 1);
        let ids = row0
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let ns = row0
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(ids.value(0), "a");
        assert_eq!(ns.value(0), 1);
        // row 3 == [(a, 1), (b, 2)]
        let row3 = ll.value(3);
        let row3 = row3.as_any().downcast_ref::<StructArray>().unwrap();
        assert_eq!(row3.len(), 2);
        // row 5 == [(z, 99)]  (the new value from batch 2)
        let row5 = ll.value(5);
        let row5 = row5.as_any().downcast_ref::<StructArray>().unwrap();
        let ids = row5
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let ns = row5
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(ids.value(0), "z");
        assert_eq!(ns.value(0), 99);
    }

    #[test]
    fn list_dispatcher_round_trip_through_new_group_values() {
        use crate::aggregates::group_values::new_group_values;
        use crate::aggregates::order::GroupOrdering;
        use arrow::datatypes::Schema;
        use datafusion_expr::EmitTo;

        let schema = Arc::new(Schema::new(vec![Field::new(
            "tags",
            DataType::List(child_field()),
            true,
        )]));
        let mut gv = new_group_values(schema, &GroupOrdering::None).unwrap();

        // Batch 1.
        let batch1: ArrayRef = list_array(&[
            Some(vec![Some(1), Some(2)]),
            None,
            Some(vec![Some(1), Some(2)]),
            Some(vec![Some(3)]),
            None,
        ]);
        let mut groups = Vec::new();
        gv.intern(&[batch1], &mut groups).unwrap();
        assert_eq!(
            groups,
            vec![0, 1, 0, 2, 1],
            "list dedup: [1,2] -> 0, null -> 1, [3] -> 2"
        );

        // Batch 2 hits existing keys + adds one new.
        let batch2: ArrayRef =
            list_array(&[Some(vec![Some(3)]), Some(vec![Some(4), Some(5)])]);
        let mut groups = Vec::new();
        gv.intern(&[batch2], &mut groups).unwrap();
        assert_eq!(groups, vec![2, 3]);

        assert_eq!(gv.len(), 4);

        let out = gv.emit(EmitTo::All).unwrap();
        let out_list = out[0].as_any().downcast_ref::<ListArray>().unwrap();
        assert_eq!(out_list.len(), 4);
        assert!(out_list.is_null(1));
    }

    #[test]
    fn handles_sliced_input_list_array() {
        // Build 5 rows then slice off the first 2. append_val/equal_to must
        // operate on logical positions of the slice, not the underlying array.
        let full = list_array(&[
            Some(vec![Some(99), Some(99)]), // sliced off
            Some(vec![Some(98)]),           // sliced off
            Some(vec![Some(1), Some(2)]),
            None,
            Some(vec![Some(3)]),
        ]);
        let sliced = full.slice(2, 3);

        let mut builder = ListGroupValueBuilder::<i32>::new(child_field(), make_child());
        for i in 0..sliced.len() {
            builder.append_val(&sliced, i).unwrap();
        }

        let probe = list_array(&[
            Some(vec![Some(1), Some(2)]),
            None,
            Some(vec![Some(3)]),
            Some(vec![Some(99), Some(99)]),
        ]);
        assert!(builder.equal_to(0, &probe, 0));
        assert!(builder.equal_to(1, &probe, 1));
        assert!(builder.equal_to(2, &probe, 2));
        assert!(
            !builder.equal_to(0, &probe, 3),
            "sliced[0]=[1,2] != [99,99]"
        );

        // Equal_to against the SAME sliced array.
        for i in 0..sliced.len() {
            assert!(builder.equal_to(i, &sliced, i));
        }

        let out = Box::new(builder).build();
        let out = out.as_any().downcast_ref::<ListArray>().unwrap();
        assert_eq!(out.len(), 3);
        assert!(out.is_null(1));
        let v0 = out.value(0);
        let v0 = v0.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(v0.values(), &[1, 2]);
        let v2 = out.value(2);
        let v2 = v2.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(v2.values(), &[3]);
    }

    #[test]
    fn handles_sliced_input_large_list_array() {
        let full = large_list_array(&[
            Some(vec![Some(99)]),
            Some(vec![Some(98)]),
            Some(vec![Some(7), Some(8)]),
            None,
        ]);
        let sliced = full.slice(2, 2);

        let mut builder = ListGroupValueBuilder::<i64>::new(child_field(), make_child());
        for i in 0..sliced.len() {
            builder.append_val(&sliced, i).unwrap();
        }

        let probe =
            large_list_array(&[Some(vec![Some(7), Some(8)]), None, Some(vec![Some(99)])]);
        assert!(builder.equal_to(0, &probe, 0));
        assert!(builder.equal_to(1, &probe, 1));
        assert!(!builder.equal_to(0, &probe, 2), "sliced[0]=[7,8] != [99]");
    }

    #[test]
    fn take_n_zero_and_full() {
        let input = list_array(&[Some(vec![Some(1), Some(2)]), Some(vec![Some(3)])]);
        let mut builder = ListGroupValueBuilder::<i32>::new(child_field(), make_child());
        for i in 0..input.len() {
            builder.append_val(&input, i).unwrap();
        }

        let none = builder.take_n(0);
        assert_eq!(none.len(), 0);
        assert_eq!(builder.len(), 2, "remainder unchanged after take_n(0)");

        let all = builder.take_n(2);
        let all = all.as_any().downcast_ref::<ListArray>().unwrap();
        assert_eq!(all.len(), 2);
        assert_eq!(builder.len(), 0, "builder drained after take_n(len)");
    }

    #[test]
    fn take_n_with_nulls_and_empty_rows() {
        let input = list_array(&[
            Some(vec![Some(1), Some(2)]),
            None,
            Some(vec![]),
            Some(vec![Some(3), Some(4), Some(5)]),
        ]);
        let mut builder = ListGroupValueBuilder::<i32>::new(child_field(), make_child());
        for i in 0..input.len() {
            builder.append_val(&input, i).unwrap();
        }

        // Take the first three (one normal + null + empty).
        let three = builder.take_n(3);
        let three = three.as_any().downcast_ref::<ListArray>().unwrap();
        assert_eq!(three.len(), 3);
        assert!(!three.is_null(0));
        assert!(three.is_null(1), "null carried over");
        assert!(!three.is_null(2));
        assert_eq!(three.value_length(2), 0, "empty list carried over");

        // Remaining = row 3 only.
        let rest = Box::new(builder).build();
        let rest = rest.as_any().downcast_ref::<ListArray>().unwrap();
        assert_eq!(rest.len(), 1);
        let v = rest.value(0);
        let v = v.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(v.values(), &[3, 4, 5]);
    }

    #[test]
    fn vectorized_methods_match_per_row() {
        let input = list_array(&[
            Some(vec![Some(1), Some(2)]),
            None,
            Some(vec![Some(3)]),
            Some(vec![Some(1), Some(2)]),
        ]);

        let mut per_row = ListGroupValueBuilder::<i32>::new(child_field(), make_child());
        for i in 0..input.len() {
            per_row.append_val(&input, i).unwrap();
        }

        let mut vec_b = ListGroupValueBuilder::<i32>::new(child_field(), make_child());
        vec_b.vectorized_append(&input, &[0, 1, 2, 3]).unwrap();
        assert_eq!(vec_b.len(), per_row.len());

        let lhs = vec![0usize, 1, 2, 3];
        let rhs = vec![0usize, 1, 2, 0];
        let mut bb = BooleanBufferBuilder::new(4);
        bb.append_n(4, true);
        vec_b.vectorized_equal_to(&lhs, &input, &rhs, &mut bb);
        for idx in 0..4 {
            assert_eq!(
                bb.get_bit(idx),
                per_row.equal_to(lhs[idx], &input, rhs[idx]),
                "row {idx}"
            );
        }

        // Pre-set false bit must not flip back to true.
        let mut bb = BooleanBufferBuilder::new(4);
        bb.append_n(4, true);
        bb.set_bit(0, false);
        vec_b.vectorized_equal_to(&lhs, &input, &rhs, &mut bb);
        assert!(!bb.get_bit(0), "pre-set false stays false");
    }

    #[test]
    fn size_grows_with_appends() {
        let input = list_array(&[Some(vec![Some(1), Some(2), Some(3)])]);
        let mut builder = ListGroupValueBuilder::<i32>::new(child_field(), make_child());
        let s0 = builder.size();
        for _ in 0..16 {
            builder.append_val(&input, 0).unwrap();
        }
        let s1 = builder.size();
        assert!(s1 > s0, "size should grow ({s0} -> {s1})");
    }

    #[test]
    fn build_empty_builder_returns_empty_list() {
        let builder = ListGroupValueBuilder::<i32>::new(child_field(), make_child());
        let out = Box::new(builder).build();
        let out = out.as_any().downcast_ref::<ListArray>().unwrap();
        assert_eq!(out.len(), 0);
    }

    #[test]
    fn nested_list_of_list_int32() {
        // List<List<Int32>>: outer list whose elements are themselves lists of
        // ints. Exercises a recursive child GroupColumn built via the
        // dispatcher.
        use crate::aggregates::group_values::new_group_values;
        use crate::aggregates::order::GroupOrdering;
        use arrow::array::{ListArray, builder::Int32Builder, builder::ListBuilder};
        use arrow::datatypes::Schema;
        use datafusion_expr::EmitTo;

        let inner_field = Arc::new(Field::new("item", DataType::Int32, true));
        let outer_field = Arc::new(Field::new(
            "item",
            DataType::List(Arc::clone(&inner_field)),
            true,
        ));
        let schema = Arc::new(Schema::new(vec![Field::new(
            "matrix",
            DataType::List(Arc::clone(&outer_field)),
            true,
        )]));

        // Helper: produce a List<List<Int32>> from
        //   Option<Vec<Option<Vec<Option<i32>>>>>
        let mk = |rows: &[Option<Vec<Option<Vec<Option<i32>>>>>]| -> ArrayRef {
            let inner = ListBuilder::new(Int32Builder::new())
                .with_field(Arc::clone(&inner_field));
            let mut outer = ListBuilder::new(inner).with_field(Arc::clone(&outer_field));
            for row in rows {
                match row {
                    None => outer.append(false),
                    Some(sublists) => {
                        for sub in sublists {
                            match sub {
                                None => outer.values().append(false),
                                Some(items) => {
                                    for v in items {
                                        outer.values().values().append_option(*v);
                                    }
                                    outer.values().append(true);
                                }
                            }
                        }
                        outer.append(true);
                    }
                }
            }
            Arc::new(outer.finish())
        };

        let mut gv = new_group_values(schema, &GroupOrdering::None).unwrap();

        // Three groups: [[1,2],[3]], its duplicate, a distinct value, and a null.
        let batch = mk(&[
            Some(vec![Some(vec![Some(1), Some(2)]), Some(vec![Some(3)])]),
            Some(vec![Some(vec![Some(1), Some(2)]), Some(vec![Some(3)])]),
            Some(vec![Some(vec![Some(1), Some(2)]), Some(vec![Some(4)])]),
            None,
            Some(vec![Some(vec![Some(1), Some(2)]), None]),
        ]);
        let mut groups = Vec::new();
        gv.intern(&[batch], &mut groups).unwrap();
        assert_eq!(
            groups,
            vec![0, 0, 1, 2, 3],
            "List<List<Int32>> dedup across rows"
        );

        let out = gv.emit(EmitTo::All).unwrap();
        let ll = out[0].as_any().downcast_ref::<ListArray>().unwrap();
        assert_eq!(ll.len(), 4);
        assert!(ll.is_null(2), "row 2 was the null outer group");

        // Inspect row 0: should be [[1,2],[3]].
        let row0 = ll.value(0);
        let row0 = row0.as_any().downcast_ref::<ListArray>().unwrap();
        assert_eq!(row0.len(), 2);
        let r0_0 = row0.value(0);
        let r0_0 = r0_0.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(r0_0.values(), &[1, 2]);
        let r0_1 = row0.value(1);
        let r0_1 = r0_1.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(r0_1.values(), &[3]);

        // Row 3 had an inner-null sublist: [[1,2], null].
        let row3 = ll.value(3);
        let row3 = row3.as_any().downcast_ref::<ListArray>().unwrap();
        assert_eq!(row3.len(), 2);
        assert!(!row3.is_null(0));
        assert!(row3.is_null(1), "inner null sublist preserved");
    }
}
