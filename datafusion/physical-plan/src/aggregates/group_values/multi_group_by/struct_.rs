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

//! [`GroupColumn`] implementation for `Struct<...>` group keys.

use crate::aggregates::group_values::multi_group_by::{GroupColumn, nulls_equal_to};
use crate::aggregates::group_values::null_builder::MaybeNullBufferBuilder;

use arrow::array::{Array, ArrayRef, BooleanBufferBuilder, StructArray};
use arrow::datatypes::Fields;
use datafusion_common::Result;
use datafusion_execution::memory_pool::proxy::VecAllocExt;
use std::sync::Arc;

/// A [`GroupColumn`] for `Struct<...>` whose children are themselves
/// `GroupColumn`-supported.
///
/// Each child builder stores values for a single struct field. Per Arrow
/// semantics, child slots exist even when the outer struct row is null, so
/// `append_val` always advances every child once per outer row regardless of
/// the outer null bit.
pub struct StructGroupValueBuilder {
    fields: Fields,
    children: Vec<Box<dyn GroupColumn>>,
    outer_nulls: MaybeNullBufferBuilder,
    outer_len: usize,
}

impl StructGroupValueBuilder {
    pub fn new(fields: Fields, children: Vec<Box<dyn GroupColumn>>) -> Self {
        assert_eq!(
            fields.len(),
            children.len(),
            "StructGroupValueBuilder: field count must match child column count"
        );
        Self {
            fields,
            children,
            outer_nulls: MaybeNullBufferBuilder::new(),
            outer_len: 0,
        }
    }
}

impl GroupColumn for StructGroupValueBuilder {
    fn equal_to(&self, lhs_row: usize, array: &ArrayRef, rhs_row: usize) -> bool {
        let lhs_null = self.outer_nulls.is_null(lhs_row);
        let rhs_null = array.is_null(rhs_row);
        if let Some(result) = nulls_equal_to(lhs_null, rhs_null) {
            return result;
        }

        let s = array
            .as_any()
            .downcast_ref::<StructArray>()
            .expect("StructGroupValueBuilder called with non-Struct array");
        for (i, child) in self.children.iter().enumerate() {
            let child_array = s.column(i);
            if !child.equal_to(lhs_row, child_array, rhs_row) {
                return false;
            }
        }
        true
    }

    fn append_val(&mut self, array: &ArrayRef, row: usize) -> Result<()> {
        let s = array
            .as_any()
            .downcast_ref::<StructArray>()
            .expect("StructGroupValueBuilder called with non-Struct array");
        self.outer_nulls.append(s.is_null(row));
        for (i, child) in self.children.iter_mut().enumerate() {
            let child_array = s.column(i);
            child.append_val(child_array, row)?;
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
        self.outer_nulls.allocated_size()
            + self.children.allocated_size()
            + self.children.iter().map(|c| c.size()).sum::<usize>()
    }

    fn build(self: Box<Self>) -> ArrayRef {
        let Self {
            fields,
            children,
            mut outer_nulls,
            outer_len: _,
        } = *self;
        let outer_nulls =
            std::mem::replace(&mut outer_nulls, MaybeNullBufferBuilder::new()).build();
        let child_arrays: Vec<ArrayRef> =
            children.into_iter().map(|c| c.build()).collect();
        Arc::new(StructArray::new(fields, child_arrays, outer_nulls))
    }

    fn take_n(&mut self, n: usize) -> ArrayRef {
        let first_n_outer_nulls = self.outer_nulls.take_n(n);
        let first_n_children: Vec<ArrayRef> =
            self.children.iter_mut().map(|c| c.take_n(n)).collect();
        self.outer_len -= n;
        Arc::new(StructArray::new(
            self.fields.clone(),
            first_n_children,
            first_n_outer_nulls,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::aggregates::group_values::multi_group_by::bytes::ByteGroupValueBuilder;
    use crate::aggregates::group_values::multi_group_by::primitive::PrimitiveGroupValueBuilder;
    use arrow::array::{Int32Array, StringArray, StructArray};
    use arrow::datatypes::{DataType, Field, Int32Type};
    use datafusion_physical_expr::binary_map::OutputType;
    use std::sync::Arc;

    fn struct_fields() -> Fields {
        Fields::from(vec![
            Field::new("id", DataType::Utf8, true),
            Field::new("n", DataType::Int32, true),
        ])
    }

    fn make_builder() -> StructGroupValueBuilder {
        let children: Vec<Box<dyn GroupColumn>> = vec![
            Box::new(ByteGroupValueBuilder::<i32>::new(OutputType::Utf8)),
            Box::new(PrimitiveGroupValueBuilder::<Int32Type, true>::new(
                DataType::Int32,
            )),
        ];
        StructGroupValueBuilder::new(struct_fields(), children)
    }

    fn struct_array(rows: &[Option<(Option<&str>, Option<i32>)>]) -> ArrayRef {
        let ids: Vec<Option<&str>> = rows
            .iter()
            .map(|row| match row {
                None => None,
                Some((id, _)) => *id,
            })
            .collect();
        let ns: Vec<Option<i32>> = rows
            .iter()
            .map(|row| match row {
                None => None,
                Some((_, n)) => *n,
            })
            .collect();
        let nulls: Vec<bool> = rows.iter().map(|r| r.is_none()).collect();
        let id_arr: ArrayRef = Arc::new(StringArray::from(ids));
        let n_arr: ArrayRef = Arc::new(Int32Array::from(ns));
        // Null buffer: bit set means valid.
        let null_buffer = arrow::buffer::NullBuffer::from(
            nulls.iter().map(|n| !*n).collect::<Vec<bool>>(),
        );
        Arc::new(StructArray::new(
            struct_fields(),
            vec![id_arr, n_arr],
            Some(null_buffer),
        ))
    }

    #[test]
    fn append_equal_take_for_struct() {
        let input = struct_array(&[
            Some((Some("a"), Some(1))),
            Some((Some("b"), Some(2))),
            Some((Some("a"), Some(1))),
            None,
            Some((Some("a"), Some(1))),
        ]);
        let mut builder = make_builder();
        for i in 0..input.len() {
            builder.append_val(&input, i).unwrap();
        }
        assert_eq!(builder.len(), 5);

        // equal_to: row 0 == row 2 == row 4, row 1 distinct, row 3 null
        let probe = struct_array(&[
            Some((Some("a"), Some(1))),
            Some((Some("b"), Some(2))),
            None,
            Some((Some("a"), Some(2))), // same id, different n
        ]);
        assert!(builder.equal_to(0, &probe, 0));
        assert!(builder.equal_to(1, &probe, 1));
        assert!(builder.equal_to(3, &probe, 2), "null == null");
        assert!(!builder.equal_to(0, &probe, 3), "(a,1) != (a,2)");
        assert!(!builder.equal_to(0, &probe, 2), "(a,1) != null");

        // build
        let out = Box::new(builder).build();
        let out_struct = out.as_any().downcast_ref::<StructArray>().unwrap();
        assert_eq!(out_struct.len(), 5);
        assert!(out_struct.is_null(3));
        let ids = out_struct
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(ids.value(0), "a");
        assert_eq!(ids.value(2), "a");
        let ns = out_struct
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(ns.value(0), 1);
        assert_eq!(ns.value(2), 1);
    }

    #[test]
    fn struct_dispatcher_round_trip() {
        use crate::aggregates::group_values::new_group_values;
        use crate::aggregates::order::GroupOrdering;
        use arrow::datatypes::Schema;
        use datafusion_expr::EmitTo;

        let field = Field::new("row_payload", DataType::Struct(struct_fields()), true);
        let schema = Arc::new(Schema::new(vec![field]));
        let mut gv = new_group_values(schema, &GroupOrdering::None).unwrap();

        let batch: ArrayRef = struct_array(&[
            Some((Some("a"), Some(1))),
            Some((Some("b"), Some(2))),
            Some((Some("a"), Some(1))),
            None,
            None,
        ]);
        let mut groups = Vec::new();
        gv.intern(&[batch], &mut groups).unwrap();
        assert_eq!(
            groups,
            vec![0, 1, 0, 2, 2],
            "null struct dedups against null"
        );

        let out = gv.emit(EmitTo::All).unwrap();
        let out_struct = out[0].as_any().downcast_ref::<StructArray>().unwrap();
        assert_eq!(out_struct.len(), 3);
        assert!(!out_struct.is_null(0));
        assert!(!out_struct.is_null(1));
        assert!(out_struct.is_null(2));
    }

    #[test]
    fn handles_sliced_input_struct_array() {
        // Build a 5-row StructArray, slice off the first 2 rows, and verify
        // that append_val + equal_to read the correct logical positions.
        let full = struct_array(&[
            Some((Some("X"), Some(99))), // sliced off
            Some((Some("Y"), Some(98))), // sliced off
            Some((Some("a"), Some(1))),
            None,
            Some((Some("b"), Some(2))),
        ]);
        let sliced = full.slice(2, 3);

        let mut builder = make_builder();
        for i in 0..sliced.len() {
            builder.append_val(&sliced, i).unwrap();
        }

        let probe = struct_array(&[
            Some((Some("a"), Some(1))),
            None,
            Some((Some("b"), Some(2))),
            Some((Some("X"), Some(99))),
        ]);
        assert!(builder.equal_to(0, &probe, 0), "sliced[0] == (a,1)");
        assert!(builder.equal_to(1, &probe, 1), "sliced[1] == null");
        assert!(builder.equal_to(2, &probe, 2), "sliced[2] == (b,2)");
        assert!(
            !builder.equal_to(0, &probe, 3),
            "sliced[0] (a,1) != (X,99) (the sliced-off prefix value)"
        );

        // Equal_to against sliced rhs too.
        for i in 0..sliced.len() {
            assert!(builder.equal_to(i, &sliced, i));
        }

        let out = Box::new(builder).build();
        let out_struct = out.as_any().downcast_ref::<StructArray>().unwrap();
        assert_eq!(out_struct.len(), 3);
        assert!(out_struct.is_null(1));
        let ids = out_struct
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let ns = out_struct
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(ids.value(0), "a");
        assert_eq!(ns.value(0), 1);
        assert_eq!(ids.value(2), "b");
        assert_eq!(ns.value(2), 2);
    }

    #[test]
    fn take_n_zero_full_and_with_nulls() {
        let input =
            struct_array(&[Some((Some("a"), Some(1))), None, Some((Some("b"), Some(2)))]);
        let mut builder = make_builder();
        for i in 0..input.len() {
            builder.append_val(&input, i).unwrap();
        }

        // take_n(0): empty prefix, remainder unchanged.
        let none = builder.take_n(0);
        assert_eq!(none.len(), 0);
        assert_eq!(builder.len(), 3);

        // take_n(2): prefix contains the null row.
        let two = builder.take_n(2);
        let two = two.as_any().downcast_ref::<StructArray>().unwrap();
        assert_eq!(two.len(), 2);
        assert!(!two.is_null(0));
        assert!(two.is_null(1), "null row carried into prefix");
        assert_eq!(builder.len(), 1);

        // Remaining = row 2 (b, 2).
        let rest = Box::new(builder).build();
        let rest = rest.as_any().downcast_ref::<StructArray>().unwrap();
        assert_eq!(rest.len(), 1);
        let ids = rest
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(ids.value(0), "b");
    }

    #[test]
    fn take_n_full_drains_builder() {
        let input = struct_array(&[Some((Some("a"), Some(1))), None]);
        let mut builder = make_builder();
        for i in 0..input.len() {
            builder.append_val(&input, i).unwrap();
        }
        let all = builder.take_n(2);
        assert_eq!(all.len(), 2);
        assert_eq!(builder.len(), 0);
    }

    #[test]
    fn vectorized_methods_match_per_row() {
        let input = struct_array(&[
            Some((Some("a"), Some(1))),
            None,
            Some((Some("b"), Some(2))),
            Some((Some("a"), Some(1))),
        ]);

        let mut per_row = make_builder();
        for i in 0..input.len() {
            per_row.append_val(&input, i).unwrap();
        }

        let mut vec_b = make_builder();
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
    }

    #[test]
    fn size_grows_with_appends() {
        let input = struct_array(&[Some((Some("hello world"), Some(7)))]);
        let mut builder = make_builder();
        let s0 = builder.size();
        for _ in 0..16 {
            builder.append_val(&input, 0).unwrap();
        }
        let s1 = builder.size();
        assert!(s1 > s0, "size should grow ({s0} -> {s1})");
    }

    #[test]
    fn build_empty_builder_returns_empty_struct() {
        let builder = make_builder();
        let out = Box::new(builder).build();
        let out = out.as_any().downcast_ref::<StructArray>().unwrap();
        assert_eq!(out.len(), 0);
        assert_eq!(out.num_columns(), 2);
    }

    #[test]
    fn nested_struct_of_struct() {
        // Build Struct<Struct<Utf8, Int32>, Int32> using the recursive factory.
        use crate::aggregates::group_values::multi_group_by::supported_schema;
        use crate::aggregates::group_values::new_group_values;
        use crate::aggregates::order::GroupOrdering;
        use arrow::array::StructArray;
        use arrow::datatypes::Schema;
        use datafusion_expr::EmitTo;

        let inner_fields = Fields::from(vec![
            Field::new("id", DataType::Utf8, true),
            Field::new("n", DataType::Int32, true),
        ]);
        let outer_fields = Fields::from(vec![
            Field::new("inner", DataType::Struct(inner_fields.clone()), true),
            Field::new("tag", DataType::Int32, true),
        ]);
        let schema = Arc::new(Schema::new(vec![Field::new(
            "row",
            DataType::Struct(outer_fields.clone()),
            true,
        )]));
        assert!(
            supported_schema(schema.as_ref()),
            "Struct<Struct<...>> must be supported_schema=true"
        );

        // Build a Struct<Struct<id: Utf8, n: Int32>, tag: Int32> array with
        // four rows: two identical, one distinct, one null outer.
        //
        // OuterRow models one outer struct row: outer null is `None`,
        // otherwise `Some((inner, tag))` where `inner = (id, n)`.
        type OuterRow<'a> = Option<((Option<&'a str>, Option<i32>), Option<i32>)>;
        let mk_array = |rows: &[OuterRow<'_>]| -> ArrayRef {
            // Inner struct children
            let mut inner_ids: Vec<Option<&str>> = Vec::with_capacity(rows.len());
            let mut inner_ns: Vec<Option<i32>> = Vec::with_capacity(rows.len());
            let mut tags: Vec<Option<i32>> = Vec::with_capacity(rows.len());
            let mut outer_validity: Vec<bool> = Vec::with_capacity(rows.len());
            let mut inner_validity: Vec<bool> = Vec::with_capacity(rows.len());
            for row in rows {
                match row {
                    None => {
                        inner_ids.push(None);
                        inner_ns.push(None);
                        tags.push(None);
                        outer_validity.push(false);
                        inner_validity.push(false);
                    }
                    Some(((id, n), tag)) => {
                        inner_ids.push(*id);
                        inner_ns.push(*n);
                        tags.push(*tag);
                        outer_validity.push(true);
                        inner_validity.push(true);
                    }
                }
            }
            let inner_id_arr: ArrayRef = Arc::new(StringArray::from(inner_ids));
            let inner_n_arr: ArrayRef = Arc::new(Int32Array::from(inner_ns));
            let inner_null = arrow::buffer::NullBuffer::from(inner_validity);
            let inner = Arc::new(StructArray::new(
                inner_fields.clone(),
                vec![inner_id_arr, inner_n_arr],
                Some(inner_null),
            )) as ArrayRef;
            let tag_arr: ArrayRef = Arc::new(Int32Array::from(tags));
            let outer_null = arrow::buffer::NullBuffer::from(outer_validity);
            Arc::new(StructArray::new(
                outer_fields.clone(),
                vec![inner, tag_arr],
                Some(outer_null),
            ))
        };

        let mut gv = new_group_values(schema, &GroupOrdering::None).unwrap();
        let batch = mk_array(&[
            Some(((Some("a"), Some(1)), Some(7))), // 0
            Some(((Some("a"), Some(1)), Some(7))), // dup of 0
            Some(((Some("a"), Some(1)), Some(8))), // distinct (outer tag differs)
            None,                                  // 2
            Some(((Some("b"), Some(2)), Some(7))), // distinct (inner differs)
        ]);
        let mut groups = Vec::new();
        gv.intern(&[batch], &mut groups).unwrap();
        assert_eq!(
            groups,
            vec![0, 0, 1, 2, 3],
            "nested struct dedup tracks inner+outer equality"
        );
        let out = gv.emit(EmitTo::All).unwrap();
        let s = out[0].as_any().downcast_ref::<StructArray>().unwrap();
        assert_eq!(s.len(), 4);
        assert!(s.is_null(2), "row 2 was the null outer group");
    }
}
