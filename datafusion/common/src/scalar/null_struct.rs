// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use std::sync::Arc;

use arrow::array::{ArrayRef, StructArray, new_null_array};
use arrow::buffer::NullBuffer;
use arrow::datatypes::{DataType, FieldRef, Fields};
use arrow::error::ArrowError;
use hashbrown::HashMap;

/// Returns a single-row, null [`StructArray`] of the given fields.
///
/// Equal to `new_null_array(&DataType::Struct(fields), 1)`, but children of
/// equal type share one array instead of each allocating its own. A null row
/// never reads its children, so any null array of the right type will do.
/// Planning builds these for wide structs (as unbounded interval endpoints),
/// where one allocation per field dominates planning time.
pub(super) fn new_null_struct(fields: &Fields) -> Result<StructArray, ArrowError> {
    NullStructBuilder::default().build(fields)
}

/// Keys are chosen so that a lookup costs O(1) regardless of type size:
/// hashing a nested [`DataType`] walks all of its fields, which would make
/// lookups as expensive as the construction they replace.
#[derive(Default)]
struct NullStructBuilder<'a> {
    by_type: HashMap<&'a DataType, ArrayRef>,
    /// Keyed by the address of the [`Fields`] allocation. Sound because every
    /// key is borrowed from the input for `'a`, so no address can be freed and
    /// reused while it is a key.
    by_fields: HashMap<*const FieldRef, ArrayRef>,
    nulls: Option<NullBuffer>,
}

impl<'a> NullStructBuilder<'a> {
    fn build(&mut self, fields: &'a Fields) -> Result<StructArray, ArrowError> {
        let children = fields
            .iter()
            .map(|f| self.child(f.data_type()))
            .collect::<Result<_, _>>()?;
        let nulls = self.nulls.get_or_insert_with(|| NullBuffer::new_null(1));
        StructArray::try_new_with_length(fields.clone(), children, Some(nulls.clone()), 1)
    }

    fn child(&mut self, data_type: &'a DataType) -> Result<ArrayRef, ArrowError> {
        match data_type {
            DataType::Struct(fields) => {
                if let Some(array) = self.by_fields.get(&fields.as_ptr()) {
                    return Ok(Arc::clone(array));
                }
                let array: ArrayRef = Arc::new(self.build(fields)?);
                self.by_fields.insert(fields.as_ptr(), Arc::clone(&array));
                Ok(array)
            }
            _ => Ok(Arc::clone(
                self.by_type
                    .entry(data_type)
                    .or_insert_with(|| new_null_array(data_type, 1)),
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Array, AsArray};
    use arrow::datatypes::Field;

    fn assert_matches_arrow(fields: Fields) {
        let expected = new_null_array(&DataType::Struct(fields.clone()), 1);
        let actual = new_null_struct(&fields).unwrap();
        assert_eq!(actual.to_data(), expected.to_data());
        actual.to_data().validate_full().unwrap();
    }

    #[test]
    fn matches_arrow_new_null_array() {
        let leaf: Fields = vec![
            Field::new("s", DataType::Utf8, true),
            Field::new("i", DataType::Int64, false),
            Field::new("b", DataType::Boolean, true),
        ]
        .into();
        let shared = DataType::Struct(leaf.clone());
        // Equal to `shared`, but a separate `Fields` allocation.
        let copy = DataType::Struct(leaf.iter().map(|f| f.as_ref().clone()).collect());
        let list = DataType::new_list(DataType::Struct(leaf.clone()), true);

        assert_matches_arrow(Fields::empty());
        assert_matches_arrow(leaf.clone());
        assert_matches_arrow(
            vec![
                Field::new("a", shared.clone(), true),
                Field::new("b", shared, false),
                Field::new("c", copy, true),
                Field::new("d", list.clone(), true),
                Field::new("e", list, false),
                Field::new("f", DataType::Utf8, false),
                Field::new("g", DataType::Struct(Fields::empty()), true),
            ]
            .into(),
        );
    }

    #[test]
    fn shares_children_of_equal_type() {
        let inner: Fields = vec![Field::new("v", DataType::Utf8, true)].into();
        let fields: Fields = vec![
            Field::new("a", DataType::Utf8, true),
            Field::new("b", DataType::Utf8, false),
            Field::new("c", DataType::Struct(inner.clone()), true),
            Field::new("d", DataType::Struct(inner), true),
        ]
        .into();
        let array = new_null_struct(&fields).unwrap();
        assert!(Arc::ptr_eq(array.column(0), array.column(1)));
        assert!(Arc::ptr_eq(array.column(2), array.column(3)));
        assert!(Arc::ptr_eq(
            array.column(0),
            array.column(2).as_struct().column(0)
        ));
    }
}
