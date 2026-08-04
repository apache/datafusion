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

//! Filters for `Utf8View` and `BinaryView` `IN` lists whose non-null values are
//! at most 12 bytes.
//!
//! Arrow stores such values directly in a `u128` view: the low 32 bits contain
//! the length and the remaining bits contain zero-padded bytes. Comparing two
//! inline views compares their complete values. Long views contain a prefix and
//! buffer location instead, so a list containing one uses the generic filter.
//!
//! Lists with up to four non-null values use the existing 128-bit branchless
//! filter. Larger lists use the primitive hash-set filter over the same 128
//! bits. `Decimal128Type` is only a carrier; no decimal operations are used.
//! A long input value cannot match an inline list value because its length is
//! part of the view.

use std::marker::PhantomData;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, AsArray, BooleanArray, GenericByteViewArray, MAX_INLINE_VIEW_LEN,
    PrimitiveArray,
};
use arrow::buffer::ScalarBuffer;
use arrow::datatypes::{
    BinaryViewType, ByteViewType, DataType, Decimal128Type, StringViewType,
};
use arrow::util::bit_iterator::BitIndexIterator;
use datafusion_common::{Result, exec_datafusion_err, internal_datafusion_err};

use super::primitive_filter::instantiate_primitive_filter;
use super::static_filter::{StaticFilter, StaticFilterRef};

fn view_len(view: u128) -> u32 {
    view as u32
}

fn downcast_byte_view<T: ByteViewType>(
    array: &dyn Array,
) -> Result<&GenericByteViewArray<T>> {
    array.as_byte_view_opt::<T>().ok_or_else(|| {
        exec_datafusion_err!(
            "Expected concrete {} array, got {}",
            T::DATA_TYPE,
            array.data_type()
        )
    })
}

fn all_inline<T: ByteViewType>(array: &GenericByteViewArray<T>) -> bool {
    let is_inline = |idx: usize| view_len(array.views()[idx]) <= MAX_INLINE_VIEW_LEN;
    match array.nulls() {
        Some(nulls) => {
            BitIndexIterator::new(nulls.validity(), nulls.offset(), nulls.len())
                .all(is_inline)
        }
        None => (0..array.len()).all(is_inline),
    }
}

fn as_decimal128<T: ByteViewType>(
    array: &GenericByteViewArray<T>,
) -> PrimitiveArray<Decimal128Type> {
    let views = array.views();
    // `views.inner()` is already sliced to the array's offset.
    let values = ScalarBuffer::<i128>::new(views.inner().clone(), 0, views.len());
    PrimitiveArray::<Decimal128Type>::new(values, array.nulls().cloned())
}

/// Adapts the selected primitive filter to the original byte-view type.
///
/// Arrow requires unused inline bytes to be zero, so equal values have equal
/// `u128` views.
struct ByteViewFilter<T: ByteViewType> {
    inner: StaticFilterRef,
    _marker: PhantomData<T>,
}

impl<T: ByteViewType> StaticFilter for ByteViewFilter<T> {
    fn null_count(&self) -> usize {
        self.inner.null_count()
    }

    fn contains(&self, v: &dyn Array, negated: bool) -> Result<BooleanArray> {
        let array = downcast_byte_view::<T>(v)?;
        self.inner.contains(&as_decimal128(array), negated)
    }
}

fn instantiate_typed_filter<T: ByteViewType>(
    in_array: &ArrayRef,
) -> Result<Option<StaticFilterRef>> {
    let array = downcast_byte_view::<T>(in_array.as_ref())?;
    if !all_inline(array) {
        return Ok(None);
    }

    let primitive: ArrayRef = Arc::new(as_decimal128(array));
    let inner = instantiate_primitive_filter(&primitive)?.ok_or_else(|| {
        internal_datafusion_err!(
            "Byte view filter: no primitive filter for {}",
            primitive.data_type()
        )
    })?;
    Ok(Some(Arc::new(ByteViewFilter::<T> {
        inner,
        _marker: PhantomData,
    })))
}

/// Returns a filter when every non-null byte view is inline.
pub(super) fn instantiate_byte_view_filter(
    in_array: &ArrayRef,
) -> Result<Option<StaticFilterRef>> {
    match in_array.data_type() {
        DataType::Utf8View => instantiate_typed_filter::<StringViewType>(in_array),
        DataType::BinaryView => instantiate_typed_filter::<BinaryViewType>(in_array),
        _ => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{BinaryViewArray, StringViewArray};

    fn assert_contains(
        filter: &dyn StaticFilter,
        needles: &dyn Array,
        expected: Vec<Option<bool>>,
    ) -> Result<()> {
        assert_eq!(
            filter.contains(needles, false)?,
            BooleanArray::from(expected)
        );
        Ok(())
    }

    #[test]
    fn large_list_nulls_and_slices() -> Result<()> {
        let haystack: ArrayRef = Arc::new(
            StringViewArray::from(vec![
                Some("outside"),
                Some("a"),
                Some("b"),
                None,
                Some("c"),
                Some("d"),
                Some("e"),
                Some("tail"),
            ])
            .slice(1, 6),
        );
        let filter = instantiate_byte_view_filter(&haystack)?.unwrap();
        let needles = StringViewArray::from(vec![
            Some("outside"),
            Some("b"),
            Some("missing"),
            None,
            Some("e"),
            Some("tail"),
        ])
        .slice(1, 4);

        assert_contains(&*filter, &needles, vec![Some(true), None, None, Some(true)])?;
        assert_eq!(
            filter.contains(&needles, true)?,
            BooleanArray::from(vec![Some(false), None, None, Some(false)])
        );
        Ok(())
    }

    #[test]
    fn routing_and_boundaries() -> Result<()> {
        let inline = "abcdefghijkl";
        let long = "abcdefghijklm";
        let needles = StringViewArray::from(vec![inline, long, "missing"]);

        // Covers both sides of the shared primitive-filter cutoff.
        for values in [
            vec![inline, "one", "two", "three"],
            vec![inline, "one", "two", "three", "four"],
        ] {
            let haystack = Arc::new(StringViewArray::from(values)) as ArrayRef;
            let filter = instantiate_byte_view_filter(&haystack)?.unwrap();
            assert_contains(
                &*filter,
                &needles,
                vec![Some(true), Some(false), Some(false)],
            )?;
        }

        let mixed: ArrayRef = Arc::new(StringViewArray::from(vec!["short", long]));
        assert!(instantiate_byte_view_filter(&mixed)?.is_none());

        let binary: ArrayRef = Arc::new(BinaryViewArray::from(vec![
            b"a".as_slice(),
            b"b".as_slice(),
            b"c".as_slice(),
            b"d".as_slice(),
            b"e".as_slice(),
        ]));
        let filter = instantiate_byte_view_filter(&binary)?.unwrap();
        let needles = BinaryViewArray::from(vec![b"a".as_slice(), b"z".as_slice()]);
        assert_contains(&*filter, &needles, vec![Some(true), Some(false)])?;
        Ok(())
    }
}
