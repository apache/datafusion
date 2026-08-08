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
//! the length and the remaining bits contain zero-padded bytes. Equality of the
//! views is therefore equality of the values. Long views contain a prefix and
//! buffer location instead, so a list containing one uses the generic filter.
//!
//! Lists with up to four non-null values use the existing 128-bit branchless
//! filter. Larger lists use a `HashSet<u128>`. `Decimal128Type` only lets the
//! branchless filter compare the same 128 bits; no decimal operations are used.
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
use datafusion_common::{HashSet, Result, exec_datafusion_err};

use super::branchless_filter::{BranchlessFilter, BranchlessFilterType};
use super::result::build_in_list_result;
use super::static_filter::{StaticFilter, handle_dictionary};

#[inline(always)]
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

/// A byte-view array whose non-null values all use Arrow's inline layout.
struct InlineViews<'a, T: ByteViewType> {
    array: &'a GenericByteViewArray<T>,
}

impl<'a, T: ByteViewType> InlineViews<'a, T> {
    fn try_new(array: &'a ArrayRef) -> Result<Option<Self>> {
        let array = downcast_byte_view::<T>(array.as_ref())?;
        Ok(all_inline(array).then_some(Self { array }))
    }
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

fn as_decimal128<T: ByteViewType>(inline: &InlineViews<'_, T>) -> ArrayRef {
    let array = inline.array;
    let views = array.views();
    // `views.inner()` is already sliced to the array's offset.
    let values = ScalarBuffer::<i128>::new(views.inner().clone(), 0, views.len());
    Arc::new(PrimitiveArray::<Decimal128Type>::new(
        values,
        array.nulls().cloned(),
    ))
}

fn branchless_filter<T: ByteViewType>(
    inline: &InlineViews<'_, T>,
) -> Result<Arc<dyn StaticFilter + Send + Sync>> {
    let values = as_decimal128(inline);

    Ok(Arc::new(ByteViewBranchless::<T> {
        inner: BranchlessFilter::<Decimal128Type>::try_new(&values)?,
        _marker: PhantomData,
    }))
}

struct ByteViewBranchless<T: ByteViewType> {
    inner: BranchlessFilter<Decimal128Type>,
    _marker: PhantomData<T>,
}

/// Exact set membership for inline views.
///
/// Arrow requires unused inline bytes to be zero, so equal values have equal
/// `u128` views.
struct ByteViewHashSet<T: ByteViewType> {
    set: HashSet<u128>,
    null_count: usize,
    _marker: PhantomData<T>,
}

impl<T: ByteViewType> ByteViewHashSet<T> {
    fn new(inline: &InlineViews<'_, T>) -> Self {
        let array = inline.array;
        let mut set = HashSet::with_capacity(array.len() - array.null_count());
        match array.nulls() {
            Some(nulls) => {
                BitIndexIterator::new(nulls.validity(), nulls.offset(), nulls.len())
                    .for_each(|idx| {
                        set.insert(array.views()[idx]);
                    });
            }
            None => set.extend(array.views().iter().copied()),
        }

        Self {
            set,
            null_count: array.null_count(),
            _marker: PhantomData,
        }
    }
}

impl<T: ByteViewType> StaticFilter for ByteViewHashSet<T> {
    fn null_count(&self) -> usize {
        self.null_count
    }

    fn contains(&self, v: &dyn Array, negated: bool) -> Result<BooleanArray> {
        handle_dictionary!(self, v, negated);
        let array = downcast_byte_view::<T>(v)?;
        let views = array.views();
        Ok(build_in_list_result(
            array.len(),
            array.nulls(),
            self.null_count > 0,
            negated,
            |idx| {
                // SAFETY: `build_in_list_result` visits indices in `0..array.len()`.
                self.set.contains(unsafe { views.get_unchecked(idx) })
            },
        ))
    }
}

impl<T: ByteViewType> StaticFilter for ByteViewBranchless<T> {
    fn null_count(&self) -> usize {
        self.inner.null_count()
    }

    fn contains(&self, v: &dyn Array, negated: bool) -> Result<BooleanArray> {
        handle_dictionary!(self, v, negated);
        let array = downcast_byte_view::<T>(v)?;
        let values: &[i128] = array.views().inner().typed_data();
        Ok(self
            .inner
            .contains_raw_values(values, array.nulls(), negated))
    }
}

fn use_branchless(count: usize) -> bool {
    count <= <Decimal128Type as BranchlessFilterType>::MAX_LIST_LEN
}

fn instantiate_typed_filter<T: ByteViewType>(
    in_array: &ArrayRef,
) -> Result<Option<Arc<dyn StaticFilter + Send + Sync>>> {
    let Some(inline) = InlineViews::<T>::try_new(in_array)? else {
        return Ok(None);
    };

    let count = inline.array.len() - inline.array.null_count();
    if use_branchless(count) {
        branchless_filter(&inline).map(Some)
    } else {
        Ok(Some(Arc::new(ByteViewHashSet::new(&inline))))
    }
}

/// Returns a filter when every non-null byte view is inline.
pub(super) fn instantiate_byte_view_filter(
    in_array: &ArrayRef,
) -> Result<Option<Arc<dyn StaticFilter + Send + Sync>>> {
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
    use arrow::datatypes::StringViewType;

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
    fn hash_set_nulls_and_slices() -> Result<()> {
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
        let inline = InlineViews::<StringViewType>::try_new(&haystack)?
            .expect("all non-null values are inline");
        let filter = ByteViewHashSet::new(&inline);
        let needles =
            StringViewArray::from(vec![Some("b"), Some("missing"), None, Some("e")]);

        assert_contains(&filter, &needles, vec![Some(true), None, None, Some(true)])?;
        assert_eq!(
            filter.contains(&needles, true)?,
            BooleanArray::from(vec![Some(false), None, None, Some(false)])
        );
        Ok(())
    }

    #[test]
    fn routing_and_boundaries() -> Result<()> {
        assert!(use_branchless(4));
        assert!(!use_branchless(5));

        let inline = "abcdefghijkl";
        let long = "abcdefghijklm";
        let values = StringViewArray::from(vec![
            "outside", inline, "one", "two", "three", "four", "tail",
        ]);
        let needles = StringViewArray::from(vec![inline, long, "missing"]);

        // Covers the branchless/hash-set cutoff and a non-zero view offset.
        for haystack in [
            Arc::new(values.slice(1, 4)) as ArrayRef,
            Arc::new(values.slice(1, 5)) as ArrayRef,
        ] {
            let filter = instantiate_byte_view_filter(&haystack)?.unwrap();
            assert_contains(
                &*filter,
                &needles,
                vec![Some(true), Some(false), Some(false)],
            )?;
        }

        let mixed: ArrayRef = Arc::new(StringViewArray::from(vec!["short", long]));
        assert!(instantiate_byte_view_filter(&mixed)?.is_none());

        let all_null: ArrayRef = Arc::new(StringViewArray::from(vec![None::<&str>]));
        let filter = instantiate_byte_view_filter(&all_null)?.unwrap();
        let needle = StringViewArray::from(vec!["missing"]);
        assert_eq!(filter.contains(&needle, false)?, BooleanArray::new_null(1));
        assert_eq!(filter.contains(&needle, true)?, BooleanArray::new_null(1));

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
