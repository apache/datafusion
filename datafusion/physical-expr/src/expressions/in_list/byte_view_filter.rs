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

//! Optimized filters for Utf8View and BinaryView IN lists.

use std::hash::BuildHasher;
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
use datafusion_common::hash_utils::RandomState;
use datafusion_common::{Result, exec_datafusion_err};
use hashbrown::{DefaultHashBuilder, HashTable};

use super::frozen_set::{FrozenSet, FrozenSetHash};
use super::primitive_filter::{BranchlessFilter, PrimitiveFilterType};
use super::result::{build_in_list_result, build_in_list_result_with_null_shortcircuit};
use super::static_filter::{StaticFilter, handle_dictionary};

#[inline(always)]
fn view_len(view: u128) -> u32 {
    view as u32
}

/// The low 64 bits of a long view contain its length and four-byte prefix.
#[inline(always)]
fn long_key(view: u128) -> u64 {
    view as u64
}

#[inline(always)]
fn view_key(view: u128) -> u128 {
    if view_len(view) <= MAX_INLINE_VIEW_LEN {
        view
    } else {
        long_key(view) as u128
    }
}

fn downcast_byte_view<T: ByteViewType>(
    array: &dyn Array,
) -> Result<&GenericByteViewArray<T>> {
    array
        .as_byte_view_opt::<T>()
        .ok_or_else(|| exec_datafusion_err!("Expected concrete {} array", T::DATA_TYPE))
}

/// Keyed folded-multiply hash specialized for Arrow's inline view encoding.
struct InlineViewHash {
    key: [u64; 2],
}

impl Default for InlineViewHash {
    fn default() -> Self {
        let state = DefaultHashBuilder::default();
        Self {
            key: [
                BuildHasher::hash_one(&state, 0_u8),
                BuildHasher::hash_one(&state, 1_u8),
            ],
        }
    }
}

impl FrozenSetHash<u128> for InlineViewHash {
    #[inline(always)]
    fn hash_one(&self, value: u128) -> u64 {
        let lo = value as u64 ^ self.key[0];
        let hi = (value >> 64) as u64 ^ self.key[1];
        let product = lo as u128 * hi as u128;
        product as u64 ^ (product >> 64) as u64
    }
}

#[derive(Debug, PartialEq, Eq)]
enum ViewComposition {
    Inline,
    Long,
    Mixed {
        inline_count: usize,
        long_count: usize,
    },
}

fn view_composition<T: ByteViewType>(array: &ArrayRef) -> Result<ViewComposition> {
    let array = downcast_byte_view::<T>(array.as_ref())?;
    let mut inline_count = 0;
    let mut long_count = 0;
    let mut visit = |idx: usize| {
        if view_len(array.views()[idx]) <= MAX_INLINE_VIEW_LEN {
            inline_count += 1;
        } else {
            long_count += 1;
        }
    };

    match array.nulls() {
        Some(nulls) => {
            BitIndexIterator::new(nulls.validity(), nulls.offset(), nulls.len())
                .for_each(&mut visit);
        }
        None => (0..array.len()).for_each(&mut visit),
    }

    Ok(match (inline_count, long_count) {
        (inline_count, long_count) if inline_count > 0 && long_count > 0 => {
            ViewComposition::Mixed {
                inline_count,
                long_count,
            }
        }
        (0, long_count) if long_count > 0 => ViewComposition::Long,
        _ => ViewComposition::Inline,
    })
}

fn reinterpret_byte_view_as_decimal128<T: ByteViewType>(
    array: &dyn Array,
) -> Result<ArrayRef> {
    let array = downcast_byte_view::<T>(array)?;
    let views = array.views();
    let values = ScalarBuffer::<i128>::new(views.inner().clone(), 0, views.len());
    Ok(Arc::new(PrimitiveArray::<Decimal128Type>::new(
        values,
        array.nulls().cloned(),
    )))
}

fn make_byte_view_branchless_filter<T: ByteViewType>(
    in_array: &ArrayRef,
) -> Result<Arc<dyn StaticFilter + Send + Sync>> {
    let values = reinterpret_byte_view_as_decimal128::<T>(in_array.as_ref())?;
    let non_null_count = in_array.len() - in_array.null_count();

    macro_rules! filter {
        ($n:literal) => {
            Arc::new(ByteViewBranchless::<T, $n> {
                inner: BranchlessFilter::<Decimal128Type, $n>::try_new(&values)?,
                _marker: PhantomData,
            }) as Arc<dyn StaticFilter + Send + Sync>
        };
    }

    Ok(match non_null_count {
        0 => filter!(0),
        1 => filter!(1),
        2 => filter!(2),
        3 => filter!(3),
        4 => filter!(4),
        _ => unreachable!("validated inline byte-view list length"),
    })
}

struct ByteViewBranchless<T: ByteViewType, const N: usize> {
    inner: BranchlessFilter<Decimal128Type, N>,
    _marker: PhantomData<T>,
}

struct InlineByteViewFilter<T: ByteViewType> {
    set: FrozenSet<u128, InlineViewHash>,
    null_count: usize,
    _marker: PhantomData<T>,
}

impl<T: ByteViewType> InlineByteViewFilter<T> {
    fn try_new(in_array: &ArrayRef) -> Result<Self> {
        if in_array.data_type() != &T::DATA_TYPE {
            return Err(exec_datafusion_err!(
                "InlineByteViewFilter: expected {} array, got {}",
                T::DATA_TYPE,
                in_array.data_type()
            ));
        }

        let array = downcast_byte_view::<T>(in_array.as_ref())?;
        let mut values = Vec::with_capacity(array.len() - array.null_count());
        match array.nulls() {
            Some(nulls) => {
                BitIndexIterator::new(nulls.validity(), nulls.offset(), nulls.len())
                    .for_each(|idx| values.push(array.views()[idx]));
            }
            None => values.extend_from_slice(array.views()),
        }

        Ok(Self {
            set: FrozenSet::try_new_with_hasher(&values, InlineViewHash::default())?,
            null_count: array.null_count(),
            _marker: PhantomData,
        })
    }
}

impl<T: ByteViewType> StaticFilter for InlineByteViewFilter<T> {
    fn null_count(&self) -> usize {
        self.null_count
    }

    fn contains(&self, v: &dyn Array, negated: bool) -> Result<BooleanArray> {
        handle_dictionary!(self, v, negated);

        if v.data_type() != &T::DATA_TYPE {
            return Err(exec_datafusion_err!(
                "InlineByteViewFilter: expected {} array, got {}",
                T::DATA_TYPE,
                v.data_type()
            ));
        }

        let array = downcast_byte_view::<T>(v)?;
        let views = array.views();
        Ok(build_in_list_result(
            array.len(),
            array.nulls(),
            self.null_count > 0,
            negated,
            |idx| {
                // SAFETY: `build_in_list_result` visits indices in `0..array.len()`.
                self.set.contains(unsafe { *views.get_unchecked(idx) })
            },
        ))
    }
}

impl<T: ByteViewType, const N: usize> StaticFilter for ByteViewBranchless<T, N> {
    fn null_count(&self) -> usize {
        self.inner.null_count()
    }

    fn contains(&self, v: &dyn Array, negated: bool) -> Result<BooleanArray> {
        handle_dictionary!(self, v, negated);

        if v.data_type() != &T::DATA_TYPE {
            return Err(exec_datafusion_err!(
                "ByteViewBranchless: expected {} array, got {}",
                T::DATA_TYPE,
                v.data_type()
            ));
        }

        let array = downcast_byte_view::<T>(v)?;
        let values: &[i128] = array.views().inner().typed_data();
        Ok(self.inner.contains_slice(values, array.nulls(), negated))
    }
}

/// Two-stage filter for inline and mixed Utf8View and BinaryView arrays.
///
/// Inline values are represented completely by their 128-bit view. For long
/// values, the length and prefix reject impossible matches before an exact
/// full-byte lookup.
struct ByteViewFilter<T: ByteViewType> {
    in_array: ArrayRef,
    view_keys: FrozenSet<u128, InlineViewHash>,
    long_value_table: HashTable<usize>,
    state: RandomState,
    _marker: PhantomData<T>,
}

impl<T: ByteViewType> ByteViewFilter<T> {
    fn try_new(in_array: ArrayRef) -> Result<Self> {
        if in_array.data_type() != &T::DATA_TYPE {
            return Err(exec_datafusion_err!(
                "ByteViewFilter: expected {} array, got {}",
                T::DATA_TYPE,
                in_array.data_type()
            ));
        }

        let array = downcast_byte_view::<T>(in_array.as_ref())?;
        let mut view_keys = Vec::new();
        let mut long_value_table = HashTable::new();
        let state = RandomState::default();

        let mut insert = |idx: usize| {
            let view = array.views()[idx];
            view_keys.push(view_key(view));
            if view_len(view) <= MAX_INLINE_VIEW_LEN {
                return;
            }

            // SAFETY: idx is produced from this array's bounds or validity bitmap.
            let value: &[u8] = unsafe { array.value_unchecked(idx) }.as_ref();
            let hash = state.hash_one(value);
            if long_value_table
                .find(hash, |&stored_idx| {
                    // SAFETY: stored_idx was inserted from this array.
                    let stored: &[u8] =
                        unsafe { array.value_unchecked(stored_idx) }.as_ref();
                    stored == value
                })
                .is_none()
            {
                long_value_table.insert_unique(hash, idx, |&stored_idx| {
                    // SAFETY: stored_idx was inserted from this array.
                    let stored: &[u8] =
                        unsafe { array.value_unchecked(stored_idx) }.as_ref();
                    state.hash_one(stored)
                });
            }
        };

        match array.nulls() {
            Some(nulls) => {
                BitIndexIterator::new(nulls.validity(), nulls.offset(), nulls.len())
                    .for_each(&mut insert);
            }
            None => (0..array.len()).for_each(&mut insert),
        }

        view_keys.sort_unstable();
        view_keys.dedup();

        Ok(Self {
            in_array,
            view_keys: FrozenSet::try_new_with_hasher(
                &view_keys,
                InlineViewHash::default(),
            )?,
            long_value_table,
            state,
            _marker: PhantomData,
        })
    }
}

impl<T: ByteViewType> StaticFilter for ByteViewFilter<T> {
    fn null_count(&self) -> usize {
        self.in_array.null_count()
    }

    fn contains(&self, v: &dyn Array, negated: bool) -> Result<BooleanArray> {
        handle_dictionary!(self, v, negated);

        if v.data_type() != &T::DATA_TYPE {
            return Err(exec_datafusion_err!(
                "ByteViewFilter: expected {} array, got {}",
                T::DATA_TYPE,
                v.data_type()
            ));
        }

        let needles = downcast_byte_view::<T>(v)?;
        let haystack = downcast_byte_view::<T>(self.in_array.as_ref())?;
        Ok(build_in_list_result_with_null_shortcircuit(
            needles.len(),
            needles.nulls(),
            self.in_array.null_count() > 0,
            negated,
            |i| {
                let view = needles.views()[i];
                if !self.view_keys.contains(view_key(view)) {
                    return false;
                }
                if view_len(view) <= MAX_INLINE_VIEW_LEN {
                    return true;
                }

                // SAFETY: i is in bounds and null indices are skipped.
                let needle: &[u8] = unsafe { needles.value_unchecked(i) }.as_ref();
                let hash = self.state.hash_one(needle);
                self.long_value_table
                    .find(hash, |&idx| {
                        // SAFETY: idx was inserted from self.in_array.
                        let value: &[u8] =
                            unsafe { haystack.value_unchecked(idx) }.as_ref();
                        value == needle
                    })
                    .is_some()
            },
        ))
    }
}

fn make_byte_view_filter<T: ByteViewType>(
    in_array: &ArrayRef,
) -> Result<Arc<dyn StaticFilter + Send + Sync>> {
    Ok(Arc::new(ByteViewFilter::<T>::try_new(Arc::clone(
        in_array,
    ))?))
}

fn make_inline_byte_view_filter<T: ByteViewType>(
    in_array: &ArrayRef,
) -> Result<Arc<dyn StaticFilter + Send + Sync>> {
    Ok(Arc::new(InlineByteViewFilter::<T>::try_new(in_array)?))
}

fn instantiate_typed_byte_view_filter<T: ByteViewType>(
    in_array: &ArrayRef,
) -> Result<Option<Arc<dyn StaticFilter + Send + Sync>>> {
    let non_null_count = in_array.len() - in_array.null_count();
    match view_composition::<T>(in_array)? {
        ViewComposition::Inline
            if non_null_count
                <= <Decimal128Type as PrimitiveFilterType>::BRANCHLESS_MAX_LIST_LEN =>
        {
            make_byte_view_branchless_filter::<T>(in_array).map(Some)
        }
        ViewComposition::Inline => make_inline_byte_view_filter::<T>(in_array).map(Some),
        ViewComposition::Mixed {
            inline_count,
            long_count,
        } if inline_count <= long_count => make_byte_view_filter::<T>(in_array).map(Some),
        ViewComposition::Mixed { .. } => Ok(None),
        ViewComposition::Long => Ok(None),
    }
}

/// Creates an optimized byte-view filter when its measured specialization wins.
///
/// Inline lists use direct view comparisons or a frozen set. Long-dominant mixed
/// lists add length/prefix rejection and exact long-value verification. Other
/// compositions stay on the generic filter when this extra stage does not win.
pub(super) fn instantiate_byte_view_filter(
    in_array: &ArrayRef,
) -> Result<Option<Arc<dyn StaticFilter + Send + Sync>>> {
    match in_array.data_type() {
        DataType::Utf8View => {
            instantiate_typed_byte_view_filter::<StringViewType>(in_array)
        }
        DataType::BinaryView => {
            instantiate_typed_byte_view_filter::<BinaryViewType>(in_array)
        }
        _ => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{BinaryViewArray, DictionaryArray, Int8Array, StringViewArray};
    use arrow::datatypes::{BinaryViewType, StringViewType};

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
    fn frozen_filter_handles_inline_slices() -> Result<()> {
        let haystack: ArrayRef = Arc::new(
            StringViewArray::from(vec![
                Some("outside"),
                Some("a"),
                Some("b"),
                Some("c"),
                Some("d"),
                Some("e"),
                Some("tail"),
            ])
            .slice(1, 5),
        );
        let filter = ByteViewFilter::<StringViewType>::try_new(haystack)?;
        let needles =
            StringViewArray::from(vec![Some("outside"), Some("b"), Some("z"), Some("e")])
                .slice(1, 3);

        assert_contains(&filter, &needles, vec![Some(true), Some(false), Some(true)])
    }

    #[test]
    fn inline_filter_handles_slices_nulls_and_not_in() -> Result<()> {
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
        let filter = InlineByteViewFilter::<StringViewType>::try_new(&haystack)?;
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
    fn frozen_filter_verifies_long_prefix_collisions_and_nulls() -> Result<()> {
        let haystack: ArrayRef = Arc::new(
            StringViewArray::from(vec![
                Some("outside-long-value"),
                Some("abcdefghijklmn1"),
                None,
                Some("short"),
                Some("tail-long-value"),
            ])
            .slice(1, 3),
        );
        let filter = ByteViewFilter::<StringViewType>::try_new(haystack)?;
        let needles = StringViewArray::from(vec![
            Some("abcdefghijklmn1"),
            Some("abcdefghijklmn2"),
            Some("short"),
            None,
        ]);

        assert_contains(&filter, &needles, vec![Some(true), None, Some(true), None])?;
        assert_eq!(
            filter.contains(&needles, true)?,
            BooleanArray::from(vec![Some(false), None, Some(false), None])
        );
        Ok(())
    }

    #[test]
    fn frozen_filter_handles_binary_views() -> Result<()> {
        let haystack: ArrayRef = Arc::new(BinaryViewArray::from(vec![
            Some([0xff, 0x00].as_slice()),
            Some(b"abcdefghijklmn1".as_slice()),
        ]));
        let filter = ByteViewFilter::<BinaryViewType>::try_new(haystack)?;
        let needles = BinaryViewArray::from(vec![
            Some([0xff, 0x00].as_slice()),
            Some(b"abcdefghijklmn1".as_slice()),
            Some(b"abcdefghijklmn2".as_slice()),
        ]);

        assert_contains(&filter, &needles, vec![Some(true), Some(true), Some(false)])
    }

    #[test]
    fn frozen_filter_handles_dictionary_needles() -> Result<()> {
        let haystack: ArrayRef = Arc::new(StringViewArray::from(vec![
            Some("short"),
            Some("abcdefghijklmn1"),
            None,
        ]));
        let filter = ByteViewFilter::<StringViewType>::try_new(haystack)?;
        let keys = Int8Array::from(vec![Some(0), Some(1), None, Some(2)]);
        let values = Arc::new(StringViewArray::from(vec![
            Some("short"),
            Some("abcdefghijklmn1"),
            Some("missing"),
        ]));
        let needles = DictionaryArray::try_new(keys, values)?;

        assert_contains(&filter, &needles, vec![Some(true), Some(true), None, None])
    }

    #[test]
    fn branchless_filter_handles_utf8_and_binary_views() -> Result<()> {
        let utf8_haystack: ArrayRef =
            Arc::new(StringViewArray::from(vec![Some("one"), None, Some("two")]));
        let utf8_filter =
            make_byte_view_branchless_filter::<StringViewType>(&utf8_haystack)?;
        let utf8_needles = StringViewArray::from(vec![Some("two"), Some("three"), None]);
        assert_contains(&*utf8_filter, &utf8_needles, vec![Some(true), None, None])?;

        let binary_haystack: ArrayRef = Arc::new(BinaryViewArray::from(vec![
            Some([0xff].as_slice()),
            Some([0x00].as_slice()),
        ]));
        let binary_filter =
            make_byte_view_branchless_filter::<BinaryViewType>(&binary_haystack)?;
        let binary_needles =
            BinaryViewArray::from(vec![Some([0x00].as_slice()), Some([0x01].as_slice())]);
        assert_contains(
            &*binary_filter,
            &binary_needles,
            vec![Some(true), Some(false)],
        )
    }

    #[test]
    fn byte_view_filters_reject_other_types() -> Result<()> {
        let haystack: ArrayRef = Arc::new(StringViewArray::from(vec![
            Some("short"),
            Some("abcdefghijklmn1"),
        ]));
        let frozen = ByteViewFilter::<StringViewType>::try_new(Arc::clone(&haystack))?;
        let branchless_haystack: ArrayRef =
            Arc::new(StringViewArray::from(vec![Some("short")]));
        let branchless =
            make_byte_view_branchless_filter::<StringViewType>(&branchless_haystack)?;
        let needles = BinaryViewArray::from(vec![Some(b"short".as_slice())]);

        assert!(frozen.contains(&needles, false).is_err());
        assert!(branchless.contains(&needles, false).is_err());
        Ok(())
    }

    #[test]
    fn byte_view_routing_only_selects_measured_wins() -> Result<()> {
        let inline_four: ArrayRef = Arc::new(StringViewArray::from(vec![
            Some("a"),
            None,
            Some("b"),
            Some("c"),
            Some("d"),
        ]));
        assert!(instantiate_byte_view_filter(&inline_four)?.is_some());

        let inline_five: ArrayRef = Arc::new(StringViewArray::from(vec![
            Some("a"),
            Some("b"),
            Some("c"),
            Some("d"),
            Some("e"),
        ]));
        assert!(instantiate_byte_view_filter(&inline_five)?.is_some());

        let all_null: ArrayRef =
            Arc::new(StringViewArray::from(vec![None::<&str>, None::<&str>]));
        assert!(instantiate_byte_view_filter(&all_null)?.is_some());

        let all_long: ArrayRef = Arc::new(StringViewArray::from(vec![
            Some("abcdefghijklmn1"),
            Some("abcdefghijklmn2"),
        ]));
        assert!(instantiate_byte_view_filter(&all_long)?.is_none());

        let mixed: ArrayRef = Arc::new(StringViewArray::from(vec![
            Some("short"),
            Some("abcdefghijklmn1"),
        ]));
        assert!(instantiate_byte_view_filter(&mixed)?.is_some());

        let inline_dominant_mixed: ArrayRef = Arc::new(StringViewArray::from(vec![
            Some("a"),
            Some("b"),
            Some("c"),
            Some("d"),
            Some("e"),
            Some("abcdefghijklmn1"),
        ]));
        assert!(instantiate_byte_view_filter(&inline_dominant_mixed)?.is_none());

        let long_dominant_mixed: ArrayRef = Arc::new(StringViewArray::from(vec![
            Some("short"),
            Some("abcdefghijklmn1"),
            Some("abcdefghijklmn2"),
        ]));
        assert!(instantiate_byte_view_filter(&long_dominant_mixed)?.is_some());

        let inline_binary: ArrayRef = Arc::new(BinaryViewArray::from(vec![
            Some(b"a".as_slice()),
            Some(b"b".as_slice()),
            Some(b"c".as_slice()),
            Some(b"d".as_slice()),
            Some(b"e".as_slice()),
        ]));
        assert!(instantiate_byte_view_filter(&inline_binary)?.is_some());

        let mixed_binary: ArrayRef = Arc::new(BinaryViewArray::from(vec![
            Some([0xff].as_slice()),
            Some(b"abcdefghijklmn1".as_slice()),
        ]));
        assert!(instantiate_byte_view_filter(&mixed_binary)?.is_some());

        Ok(())
    }
}
