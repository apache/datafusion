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

//! Sort kernels over several arrays at once.
//!
//! The rows of all arrays ("blocks") are sorted together without concatenating or
//! slicing them. A row is addressed by an [`ArrayRowIndex`], `(array index, row index)`,
//! and every comparison reads the value through `get_unchecked` on the matching array.
//!
//! Adapted from `arrow_ord::sort`.

use std::cmp::Ordering;
use std::sync::Arc;

use arrow::array::{
    Array, ArrayRef, ArrowNativeTypeOp, ArrowPrimitiveType, BooleanArray, ByteView,
    DynComparator, FixedSizeBinaryArray, GenericByteArray, GenericByteViewArray,
    PrimitiveArray, make_comparator,
};
use arrow::compute::{SortOptions, interleave};
use arrow::datatypes::{
    BinaryType, BinaryViewType, ByteArrayType, ByteViewType, DataType, LargeBinaryType,
    LargeUtf8Type, StringViewType, Utf8Type,
};
use arrow::downcast_primitive_array;
use arrow::error::ArrowError;
use arrow::row::{RowConverter, Rows, SortField};

/// `(array index, row index in that array)`, the shape [`interleave`] takes
pub type ArrayRowIndex = (usize, usize);

/// Views up to this many bytes are stored inline
const MAX_INLINE_VIEW_LEN: u32 = 12;

/// One column to be used in lexicographical sort, made of one array per block
#[derive(Clone, Debug)]
pub struct SortColumn {
    /// The arrays of the column, all of the same data type
    pub values: Vec<ArrayRef>,
    /// Sort options for this column
    pub options: Option<SortOptions>,
}

/// Sorts the rows of `arrays` together and gathers them into new arrays with the same
/// lengths as the inputs. Nulls are ordered according to `nulls_first`, floats by IEEE
/// 754 totalOrder. Unstable: equal elements may not keep their order.
pub fn sort(arrays: &[ArrayRef], options: Option<SortOptions>) -> Result<Vec<ArrayRef>, ArrowError> {
    let indices = sort_to_indices(arrays, options, None)?;
    gather(arrays, &indices)
}

/// Like [`sort`] but keeps only the first `limit` rows of the sort order, still chunked
/// by the input lengths (the trailing chunks come out shorter or are omitted)
pub fn sort_limit(
    arrays: &[ArrayRef],
    options: Option<SortOptions>,
    limit: Option<usize>,
) -> Result<Vec<ArrayRef>, ArrowError> {
    let indices = sort_to_indices(arrays, options, limit)?;
    gather(arrays, &indices)
}

/// Gathers `indices` out of `arrays` into new arrays chunked like `arrays`: output `i`
/// has the length of input `i` (shorter when `indices` runs out)
pub fn gather(arrays: &[ArrayRef], indices: &[ArrayRowIndex]) -> Result<Vec<ArrayRef>, ArrowError> {
    let refs: Vec<&dyn Array> = arrays.iter().map(|array| array.as_ref()).collect();
    let mut out = Vec::with_capacity(arrays.len());
    let mut start = 0;
    for array in arrays {
        if start >= indices.len() {
            break;
        }
        let end = (start + array.len()).min(indices.len());
        out.push(interleave(&refs, &indices[start..end])?);
        start = end;
    }
    Ok(out)
}

/// Sorts the rows of `arrays` together and returns their positions in sort order.
/// `limit` keeps only the first `limit` positions ([`partial_sort`]).
pub fn sort_to_indices(
    arrays: &[ArrayRef],
    options: Option<SortOptions>,
    limit: Option<usize>,
) -> Result<Vec<ArrayRowIndex>, ArrowError> {
    let Some(first) = arrays.first() else {
        return Ok(vec![]);
    };
    if let Some(other) = arrays.iter().find(|array| array.data_type() != first.data_type()) {
        return Err(ArrowError::ComputeError(format!(
            "sort arrays have different data types: {} and {}",
            first.data_type(),
            other.data_type()
        )));
    }
    let total: usize = arrays.iter().map(|array| array.len()).sum();
    if total == 0 || limit == Some(0) {
        return Ok(vec![]);
    }

    let options = options.unwrap_or_default();
    let first = first.as_ref();

    Ok(downcast_primitive_array! {
        first => sort_primitive_like(first, arrays, options, limit),
        DataType::Boolean => sort_boolean(&downcast_all::<BooleanArray>(arrays), options, limit),
        DataType::Utf8 => sort_bytes(&downcast_all::<GenericByteArray<Utf8Type>>(arrays), options, limit),
        DataType::LargeUtf8 => sort_bytes(&downcast_all::<GenericByteArray<LargeUtf8Type>>(arrays), options, limit),
        DataType::Binary => sort_bytes(&downcast_all::<GenericByteArray<BinaryType>>(arrays), options, limit),
        DataType::LargeBinary => sort_bytes(&downcast_all::<GenericByteArray<LargeBinaryType>>(arrays), options, limit),
        DataType::Utf8View => sort_byte_view(&downcast_all::<GenericByteViewArray<StringViewType>>(arrays), options, limit),
        DataType::BinaryView => sort_byte_view(&downcast_all::<GenericByteViewArray<BinaryViewType>>(arrays), options, limit),
        DataType::FixedSizeBinary(_) => sort_fixed_size_binary(&downcast_all::<FixedSizeBinaryArray>(arrays), options, limit),
        // Dictionaries, lists, runs, structs...: compared through arrow's comparators
        _ => sort_by_comparators(&[SortColumn { values: arrays.to_vec(), options: Some(options) }], limit)?,
    })
}

/// Sorts lexicographically by every column and returns the positions in sort order
pub fn lexsort_to_indices(
    columns: &[SortColumn],
    limit: Option<usize>,
) -> Result<Vec<ArrayRowIndex>, ArrowError> {
    let Some(first) = columns.first() else {
        return Err(ArrowError::InvalidArgumentError(
            "Sort requires at least one column".to_string(),
        ));
    };
    for column in columns {
        if column.values.len() != first.values.len()
            || column
                .values
                .iter()
                .zip(&first.values)
                .any(|(array, other)| array.len() != other.len())
        {
            return Err(ArrowError::ComputeError(
                "lexical sort columns have different row counts".to_string(),
            ));
        }
    }
    if first.values.is_empty() {
        return Ok(Vec::new());
    }
    if columns.len() == 1 {
        return sort_to_indices(&first.values, first.options, limit);
    }
    let fields = columns
        .iter()
        .map(|column| {
            SortField::new_with_options(
                column.values[0].data_type().clone(),
                column.options.unwrap_or_default(),
            )
        })
        .collect::<Vec<_>>();
    if RowConverter::supports_fields(&fields) {
        return sort_by_rows(columns, fields, limit);
    }
    sort_by_comparators(columns, limit)
}

/// Sorts lexicographically by every column and gathers each column like [`gather`]
pub fn lexsort(
    columns: &[SortColumn],
    limit: Option<usize>,
) -> Result<Vec<Vec<ArrayRef>>, ArrowError> {
    let indices = lexsort_to_indices(columns, limit)?;
    columns
        .iter()
        .map(|column| gather(&column.values, &indices))
        .collect()
}

/// The arrays typed as `A`, they were checked to share one data type
fn downcast_all<A: Array + 'static>(arrays: &[ArrayRef]) -> Vec<&A> {
    arrays
        .iter()
        .map(|array| {
            array
                .as_any()
                .downcast_ref::<A>()
                .expect("all arrays were checked to have the same data type")
        })
        .collect()
}

/// Positions of the non null rows and of the null rows, both in array order
fn partition_validity<A: Array>(arrays: &[&A]) -> (Vec<ArrayRowIndex>, Vec<ArrayRowIndex>) {
    let null_count: usize = arrays.iter().map(|array| array.null_count()).sum();
    let total: usize = arrays.iter().map(|array| array.len()).sum();
    let mut valids = Vec::with_capacity(total - null_count);
    let mut nulls = Vec::with_capacity(null_count);
    for (array_index, array) in arrays.iter().enumerate() {
        match array.nulls().filter(|nulls| nulls.null_count() > 0) {
            None => valids.extend((0..array.len()).map(|row| (array_index, row))),
            Some(validity) => {
                for row in 0..array.len() {
                    if validity.is_valid(row) {
                        valids.push((array_index, row));
                    } else {
                        nulls.push((array_index, row));
                    }
                }
            }
        }
    }
    (valids, nulls)
}

/// `T` is inferred from the first array, which the downcast macro hands over typed
fn sort_primitive_like<T: ArrowPrimitiveType>(
    _first: &PrimitiveArray<T>,
    arrays: &[ArrayRef],
    options: SortOptions,
    limit: Option<usize>,
) -> Vec<ArrayRowIndex> {
    let arrays = downcast_all::<PrimitiveArray<T>>(arrays);
    let (valids, nulls) = partition_validity(&arrays);
    let values: Vec<&[T::Native]> = arrays.iter().map(|array| array.values().as_ref()).collect();
    let mut valids: Vec<(ArrayRowIndex, T::Native)> = valids
        .into_iter()
        // SAFETY: the positions come from the arrays themselves
        .map(|position| (position, unsafe { *values.get_unchecked(position.0).get_unchecked(position.1) }))
        .collect();
    sort_impl(options, &mut valids, &nulls, limit, |a, b| a.1.compare(b.1))
}

fn sort_boolean(
    arrays: &[&BooleanArray],
    options: SortOptions,
    limit: Option<usize>,
) -> Vec<ArrayRowIndex> {
    let (valids, nulls) = partition_validity(arrays);
    let mut valids: Vec<(ArrayRowIndex, bool)> = valids
        .into_iter()
        // SAFETY: the positions come from the arrays themselves
        .map(|position| (position, unsafe { arrays.get_unchecked(position.0).value_unchecked(position.1) }))
        .collect();
    sort_impl(options, &mut valids, &nulls, limit, |a, b| a.1.cmp(&b.1))
}

fn sort_bytes<T: ByteArrayType>(
    arrays: &[&GenericByteArray<T>],
    options: SortOptions,
    limit: Option<usize>,
) -> Vec<ArrayRowIndex> {
    let (valids, nulls) = partition_validity(arrays);
    // SAFETY: the positions come from the arrays themselves
    let bytes = |position: ArrayRowIndex| -> &[u8] {
        unsafe { arrays.get_unchecked(position.0).value_unchecked(position.1).as_ref() }
    };
    // Most byte sequences differ in their first bytes: a 4 byte big endian prefix compared as
    // one u32 (left padded when shorter) decides nearly every comparison without touching
    // the full values
    let mut valids: Vec<(ArrayRowIndex, (u32, u64))> = valids
        .into_iter()
        .map(|position| (position, prefix_and_len(bytes(position))))
        .collect();
    sort_impl(options, &mut valids, &nulls, limit, |a, b| {
        compare_prefixed(a.1, b.1).unwrap_or_else(|| bytes(a.0).cmp(bytes(b.0)))
    })
}

/// `(4 byte big endian prefix, len)` of `slice`, shorter slices are left padded
fn prefix_and_len(slice: &[u8]) -> (u32, u64) {
    let prefix = if slice.len() >= 4 {
        // SAFETY: at least 4 readable bytes
        u32::from_be(unsafe { std::ptr::read_unaligned(slice.as_ptr().cast::<u32>()) })
    } else if slice.is_empty() {
        0
    } else {
        let mut prefix = 0u32;
        for &byte in slice {
            prefix = (prefix << 8) | byte as u32;
        }
        // len is in [1, 3] so the shift is in [8, 24]
        prefix << (8 * (4 - slice.len()))
    };
    (prefix, slice.len() as u64)
}

/// The order decided by the prefixes alone, `None` when the full values must be compared
fn compare_prefixed(a: (u32, u64), b: (u32, u64)) -> Option<Ordering> {
    let ord = a.0.cmp(&b.0);
    if ord != Ordering::Equal {
        return Some(ord);
    }
    // padded prefixes are equal only when both are complete and equal
    if a.1 < 4 || b.1 < 4 {
        let ord = a.1.cmp(&b.1);
        if ord != Ordering::Equal {
            return Some(ord);
        }
    }
    None
}

fn sort_byte_view<T: ByteViewType>(
    arrays: &[&GenericByteViewArray<T>],
    options: SortOptions,
    limit: Option<usize>,
) -> Vec<ArrayRowIndex> {
    let (valids, nulls) = partition_validity(arrays);
    let mut valids: Vec<(ArrayRowIndex, u128)> = valids
        .into_iter()
        // SAFETY: the positions come from the arrays themselves
        .map(|position| (position, unsafe { *arrays.get_unchecked(position.0).views().get_unchecked(position.1) }))
        .collect();

    if arrays.iter().all(|array| array.data_buffers().is_empty()) {
        // every view is inline, the key is the view itself
        return sort_impl(options, &mut valids, &nulls, limit, |a, b| {
            GenericByteViewArray::<T>::inline_key_fast(a.1)
                .cmp(&GenericByteViewArray::<T>::inline_key_fast(b.1))
        });
    }

    sort_impl(options, &mut valids, &nulls, limit, |a, b| {
        let (raw_a, raw_b) = (a.1, b.1);
        if (raw_a as u32) <= MAX_INLINE_VIEW_LEN && (raw_b as u32) <= MAX_INLINE_VIEW_LEN {
            return GenericByteViewArray::<T>::inline_key_fast(raw_a)
                .cmp(&GenericByteViewArray::<T>::inline_key_fast(raw_b));
        }
        let prefix_a = ByteView::from(raw_a).prefix.swap_bytes();
        let prefix_b = ByteView::from(raw_b).prefix.swap_bytes();
        if prefix_a != prefix_b {
            return prefix_a.cmp(&prefix_b);
        }
        // SAFETY: the positions come from the arrays themselves
        let full_a: &[u8] = unsafe { arrays.get_unchecked(a.0.0).value_unchecked(a.0.1).as_ref() };
        let full_b: &[u8] = unsafe { arrays.get_unchecked(b.0.0).value_unchecked(b.0.1).as_ref() };
        full_a.cmp(full_b)
    })
}

fn sort_fixed_size_binary(
    arrays: &[&FixedSizeBinaryArray],
    options: SortOptions,
    limit: Option<usize>,
) -> Vec<ArrayRowIndex> {
    let (valids, nulls) = partition_validity(arrays);
    let mut valids: Vec<(ArrayRowIndex, &[u8])> = valids
        .into_iter()
        // SAFETY: the positions come from the arrays themselves
        .map(|position| (position, unsafe { arrays.get_unchecked(position.0).value_unchecked(position.1) }))
        .collect();
    sort_impl(options, &mut valids, &nulls, limit, |a, b| a.1.cmp(b.1))
}

/// Sorts `valids` by `cmp` (reversed when descending), then lays out nulls and valids
/// according to `nulls_first`, keeping only `limit` positions
#[inline(never)]
fn sort_impl<K: Copy>(
    options: SortOptions,
    valids: &mut [(ArrayRowIndex, K)],
    nulls: &[ArrayRowIndex],
    limit: Option<usize>,
    mut cmp: impl FnMut(&(ArrayRowIndex, K), &(ArrayRowIndex, K)) -> Ordering,
) -> Vec<ArrayRowIndex> {
    let valid_limit = match (limit, options.nulls_first) {
        (Some(limit), true) => limit.saturating_sub(nulls.len()).min(valids.len()),
        _ => valids.len(),
    };
    if options.descending {
        sort_unstable_by(valids, valid_limit, |a, b| cmp(a, b).reverse());
    } else {
        sort_unstable_by(valids, valid_limit, cmp);
    }

    let len = valids.len() + nulls.len();
    let limit = limit.unwrap_or(len).min(len);
    let mut out = Vec::with_capacity(limit);
    if options.nulls_first {
        out.extend_from_slice(&nulls[..nulls.len().min(limit)]);
        let remaining = limit - out.len();
        out.extend(valids.iter().map(|(position, _)| *position).take(remaining));
    } else {
        out.extend(valids.iter().map(|(position, _)| *position).take(limit));
        let remaining = limit - out.len();
        out.extend_from_slice(&nulls[..remaining]);
    }
    out
}

/// Sorts every row of `columns` with one arrow comparator per column and pair of arrays,
/// the fallback for types without a typed path and for multi column sorts
/// Multi column sort through the row format: every array is encoded to rows on its own
/// (nothing is concatenated) and rows compare as plain bytes, which is far cheaper than
/// a chain of dynamic comparators per column
fn sort_by_rows(
    columns: &[SortColumn],
    fields: Vec<SortField>,
    limit: Option<usize>,
) -> Result<Vec<ArrayRowIndex>, ArrowError> {
    let converter = RowConverter::new(fields)?;
    let rows = (0..columns[0].values.len())
        .map(|array_index| {
            let arrays = columns
                .iter()
                .map(|column| Arc::clone(&column.values[array_index]))
                .collect::<Vec<_>>();
            converter.convert_columns(&arrays)
        })
        .collect::<Result<Vec<_>, _>>()?;

    // Fixed width rows are compared as one or two big endian integers carried next to
    // the index, which beats a `memcmp` through two pointer chases per comparison
    let fixed_width = columns
        .iter()
        .map(|column| match column.values[0].data_type() {
            DataType::Boolean => Some(1 + 1),
            data_type => data_type.primitive_width().map(|width| 1 + width),
        })
        .sum::<Option<usize>>();
    match fixed_width {
        Some(width) if width <= 16 => {
            return Ok(sort_by_fixed_rows(&rows, limit, |row| {
                let mut key = [0u8; 16];
                key[..width].copy_from_slice(row);
                u128::from_be_bytes(key)
            }));
        }
        Some(width) if width <= 32 => {
            return Ok(sort_by_fixed_rows(&rows, limit, |row| {
                let mut key = [0u8; 32];
                key[..width].copy_from_slice(row);
                let (high, low) = key.split_at(16);
                (
                    u128::from_be_bytes(high.try_into().unwrap()),
                    u128::from_be_bytes(low.try_into().unwrap()),
                )
            }));
        }
        _ => {}
    }

    let mut indices: Vec<ArrayRowIndex> = rows
        .iter()
        .enumerate()
        .flat_map(|(array_index, rows)| (0..rows.num_rows()).map(move |row| (array_index, row)))
        .collect();
    let len = limit.unwrap_or(indices.len()).min(indices.len());
    sort_unstable_by(&mut indices, len, |a, b| {
        // SAFETY: the indices were built from the row counts of these very `rows`
        unsafe {
            rows.get_unchecked(a.0)
                .row_unchecked(a.1)
                .cmp(&rows.get_unchecked(b.0).row_unchecked(b.1))
        }
    });
    indices.truncate(len);
    Ok(indices)
}

/// Sorts rows of one fixed width by the integer key `make_key` builds from their bytes
fn sort_by_fixed_rows<K: Ord + Copy>(
    rows: &[Rows],
    limit: Option<usize>,
    make_key: impl Fn(&[u8]) -> K,
) -> Vec<ArrayRowIndex> {
    let make_key = &make_key;
    let mut keyed: Vec<(K, ArrayRowIndex)> = rows
        .iter()
        .enumerate()
        .flat_map(|(array_index, rows)| {
            rows.iter()
                .enumerate()
                .map(move |(row_index, row)| (make_key(row.as_ref()), (array_index, row_index)))
        })
        .collect();
    let len = limit.unwrap_or(keyed.len()).min(keyed.len());
    sort_unstable_by(&mut keyed, len, |a, b| a.0.cmp(&b.0));
    keyed.into_iter().take(len).map(|(_, index)| index).collect()
}

fn sort_by_comparators(
    columns: &[SortColumn],
    limit: Option<usize>,
) -> Result<Vec<ArrayRowIndex>, ArrowError> {
    let arrays_count = columns[0].values.len();
    // comparators[column][left array][right array]
    let comparators: Vec<Vec<Vec<DynComparator>>> = columns
        .iter()
        .map(|column| {
            let options = column.options.unwrap_or_default();
            (0..arrays_count)
                .map(|left| {
                    (0..arrays_count)
                        .map(|right| {
                            make_comparator(
                                column.values[left].as_ref(),
                                column.values[right].as_ref(),
                                options,
                            )
                        })
                        .collect::<Result<Vec<_>, _>>()
                })
                .collect::<Result<Vec<_>, _>>()
        })
        .collect::<Result<Vec<_>, _>>()?;

    let mut indices: Vec<ArrayRowIndex> = columns[0]
        .values
        .iter()
        .enumerate()
        .flat_map(|(array_index, array)| (0..array.len()).map(move |row| (array_index, row)))
        .collect();
    let len = limit.unwrap_or(indices.len()).min(indices.len());
    sort_unstable_by(&mut indices, len, |a, b| {
        for column in &comparators {
            // SAFETY: array indices come from `columns[0].values`, every column has as many
            let ord = unsafe { column.get_unchecked(a.0).get_unchecked(b.0) }(a.1, b.1);
            if ord != Ordering::Equal {
                return ord;
            }
        }
        Ordering::Equal
    });
    indices.truncate(len);
    Ok(indices)
}

#[inline]
fn sort_unstable_by<T, F>(array: &mut [T], limit: usize, cmp: F)
where
    F: FnMut(&T, &T) -> Ordering,
{
    if array.len() == limit {
        array.sort_unstable_by(cmp);
    } else {
        partial_sort(array, limit, cmp);
    }
}

/// Sorts only the first `limit` elements into place. Unstable.
pub fn partial_sort<T, F>(v: &mut [T], limit: usize, mut is_less: F)
where
    F: FnMut(&T, &T) -> Ordering,
{
    if let Some(n) = limit.checked_sub(1) {
        let (before, _mid, _after) = v.select_nth_unstable_by(n, &mut is_less);
        before.sort_unstable_by(is_less);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{
        ArrayRef, BooleanArray, DictionaryArray, Int32Array, Int64Array, StringArray,
        StringViewArray,
    };
    use arrow::compute::{
        SortColumn as ArrowSortColumn, cast, concat, lexsort_to_indices as arrow_lexsort_to_indices,
        sort_to_indices as arrow_sort_to_indices, take,
    };
    use arrow::datatypes::Int32Type;
    use std::sync::Arc;

    /// Deterministic values without depending on the rand API
    struct Lcg(u64);

    impl Lcg {
        fn next(&mut self) -> u64 {
            self.0 = self.0.wrapping_mul(6364136223846793005).wrapping_add(1442695040888963407);
            self.0 >> 33
        }

        fn below(&mut self, n: u64) -> u64 {
            self.next() % n
        }

        fn maybe<T>(&mut self, value: T) -> Option<T> {
            (self.below(10) != 0).then_some(value)
        }
    }

    const LENGTHS: [usize; 5] = [0, 13, 1, 50, 20];

    fn ints(rng: &mut Lcg) -> Vec<ArrayRef> {
        LENGTHS
            .iter()
            .map(|&len| {
                let values = (0..len).map(|_| {
                    let value = rng.below(40) as i32 - 20;
                    rng.maybe(value)
                });
                Arc::new(Int32Array::from(values.collect::<Vec<_>>())) as ArrayRef
            })
            .collect()
    }

    fn word(rng: &mut Lcg) -> String {
        let len = rng.below(20) as usize;
        (0..len).map(|_| (b'a' + rng.below(4) as u8) as char).collect()
    }

    fn strings(rng: &mut Lcg) -> Vec<ArrayRef> {
        LENGTHS
            .iter()
            .map(|&len| {
                let values = (0..len).map(|_| {
                    let value = word(rng);
                    rng.maybe(value)
                });
                Arc::new(StringArray::from(values.collect::<Vec<_>>())) as ArrayRef
            })
            .collect()
    }

    fn string_views(rng: &mut Lcg) -> Vec<ArrayRef> {
        LENGTHS
            .iter()
            .map(|&len| {
                let values = (0..len).map(|_| {
                    let value = word(rng);
                    rng.maybe(value)
                });
                Arc::new(StringViewArray::from(values.collect::<Vec<_>>())) as ArrayRef
            })
            .collect()
    }

    fn booleans(rng: &mut Lcg) -> Vec<ArrayRef> {
        LENGTHS
            .iter()
            .map(|&len| {
                let values = (0..len).map(|_| {
                    let value = rng.below(2) == 0;
                    rng.maybe(value)
                });
                Arc::new(BooleanArray::from(values.collect::<Vec<_>>())) as ArrayRef
            })
            .collect()
    }

    fn dictionaries(rng: &mut Lcg) -> Vec<ArrayRef> {
        LENGTHS
            .iter()
            .map(|&len| {
                let values: Vec<Option<String>> = (0..len)
                    .map(|_| {
                        let value = word(rng);
                        rng.maybe(value)
                    })
                    .collect();
                Arc::new(
                    values
                        .iter()
                        .map(|value| value.as_deref())
                        .collect::<DictionaryArray<Int32Type>>(),
                ) as ArrayRef
            })
            .collect()
    }

    fn all_options() -> Vec<Option<SortOptions>> {
        let mut options = vec![None];
        for descending in [false, true] {
            for nulls_first in [false, true] {
                options.push(Some(SortOptions { descending, nulls_first }));
            }
        }
        options
    }

    /// Values in our sort order must equal arrow's sort of the concatenated arrays
    fn check_single(arrays: &[ArrayRef]) {
        let refs: Vec<&dyn Array> = arrays.iter().map(|array| array.as_ref()).collect();
        let whole = concat(&refs).unwrap();
        for options in all_options() {
            for limit in [None, Some(1), Some(7), Some(1000)] {
                let ours = sort_to_indices(arrays, options, limit).unwrap();
                let ours = interleave(&refs, &ours).unwrap();
                let expected = arrow_sort_to_indices(&whole, options, limit).unwrap();
                let expected = take(&whole, &expected, None).unwrap();
                // dictionaries may end up with different dictionaries but the same values
                let (ours, expected) = if let DataType::Dictionary(_, _) = whole.data_type() {
                    (cast(&ours, &DataType::Utf8).unwrap(), cast(&expected, &DataType::Utf8).unwrap())
                } else {
                    (ours, expected)
                };
                assert_eq!(&ours, &expected, "options {options:?} limit {limit:?}");
            }
        }
    }

    #[test]
    fn matches_arrow_on_concatenated_input() {
        let mut rng = Lcg(7);
        check_single(&ints(&mut rng));
        check_single(&strings(&mut rng));
        check_single(&string_views(&mut rng));
        check_single(&booleans(&mut rng));
        check_single(&dictionaries(&mut rng));
    }

    #[test]
    fn lexsort_matches_arrow_on_concatenated_input() {
        let mut rng = Lcg(11);
        // few distinct ints so the second column decides often
        let ints: Vec<ArrayRef> = LENGTHS
            .iter()
            .map(|&len| {
                let values = (0..len).map(|_| {
                    let value = rng.below(3) as i32;
                    rng.maybe(value)
                });
                Arc::new(Int32Array::from(values.collect::<Vec<_>>())) as ArrayRef
            })
            .collect();
        let views = string_views(&mut rng);
        let int_refs: Vec<&dyn Array> = ints.iter().map(|array| array.as_ref()).collect();
        let view_refs: Vec<&dyn Array> = views.iter().map(|array| array.as_ref()).collect();
        let whole_ints = concat(&int_refs).unwrap();
        let whole_views = concat(&view_refs).unwrap();

        for int_options in all_options() {
            for view_options in all_options() {
                for limit in [None, Some(5)] {
                    let columns = [
                        SortColumn { values: ints.clone(), options: int_options },
                        SortColumn { values: views.clone(), options: view_options },
                    ];
                    let ours = lexsort_to_indices(&columns, limit).unwrap();
                    let expected = arrow_lexsort_to_indices(
                        &[
                            ArrowSortColumn { values: Arc::clone(&whole_ints), options: int_options },
                            ArrowSortColumn { values: Arc::clone(&whole_views), options: view_options },
                        ],
                        limit,
                    )
                    .unwrap();
                    assert_eq!(
                        &interleave(&int_refs, &ours).unwrap(),
                        &take(&whole_ints, &expected, None).unwrap()
                    );
                    assert_eq!(
                        &interleave(&view_refs, &ours).unwrap(),
                        &take(&whole_views, &expected, None).unwrap()
                    );

                    // `lexsort` gathers the same order, chunked like the inputs
                    let sorted = lexsort(&columns, limit).unwrap();
                    let gathered: Vec<&dyn Array> = sorted[1].iter().map(|array| array.as_ref()).collect();
                    assert_eq!(
                        &concat(&gathered).unwrap(),
                        &take(&whole_views, &expected, None).unwrap()
                    );
                }
            }
        }
    }

    /// Fixed width columns take the integer key paths (one `u128` up to 16 bytes of row,
    /// two above that), check both against arrow on the concatenated input
    #[test]
    fn fixed_width_lexsort_matches_arrow_on_concatenated_input() {
        let mut rng = Lcg(29);
        let mut column = |distinct: u64, wide: bool| -> Vec<ArrayRef> {
            LENGTHS
                .iter()
                .map(|&len| {
                    if wide {
                        let values = (0..len).map(|_| {
                            let value = rng.below(distinct) as i64 - 1;
                            rng.maybe(value)
                        });
                        Arc::new(Int64Array::from(values.collect::<Vec<_>>())) as ArrayRef
                    } else {
                        let values = (0..len).map(|_| {
                            let value = rng.below(distinct) as i32 - 1;
                            rng.maybe(value)
                        });
                        Arc::new(Int32Array::from(values.collect::<Vec<_>>())) as ArrayRef
                    }
                })
                .collect()
        };
        // 5 + 9 = 14 bytes per row, and 5 + 9 + 9 = 23 bytes per row
        let two = vec![column(3, false), column(4, true)];
        let three = vec![column(3, false), column(3, true), column(50, true)];
        for columns in [two, three] {
            let whole: Vec<ArrayRef> = columns
                .iter()
                .map(|arrays| {
                    let refs: Vec<&dyn Array> = arrays.iter().map(|a| a.as_ref()).collect();
                    concat(&refs).unwrap()
                })
                .collect();
            for first_options in all_options() {
                for rest_options in all_options() {
                    for limit in [None, Some(7)] {
                        let options = |i: usize| if i == 0 { first_options } else { rest_options };
                        let sort_columns: Vec<SortColumn> = columns
                            .iter()
                            .enumerate()
                            .map(|(i, values)| SortColumn { values: values.clone(), options: options(i) })
                            .collect();
                        let ours = lexsort_to_indices(&sort_columns, limit).unwrap();
                        let arrow_columns: Vec<ArrowSortColumn> = whole
                            .iter()
                            .enumerate()
                            .map(|(i, values)| ArrowSortColumn { values: Arc::clone(values), options: options(i) })
                            .collect();
                        let expected = arrow_lexsort_to_indices(&arrow_columns, limit).unwrap();
                        for (arrays, whole) in columns.iter().zip(&whole) {
                            let refs: Vec<&dyn Array> = arrays.iter().map(|a| a.as_ref()).collect();
                            assert_eq!(
                                &interleave(&refs, &ours).unwrap(),
                                &take(whole, &expected, None).unwrap()
                            );
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn sort_keeps_input_chunking() {
        let mut rng = Lcg(3);
        let arrays = strings(&mut rng);
        let sorted = sort(&arrays, None).unwrap();
        assert_eq!(sorted.iter().map(|array| array.len()).collect::<Vec<_>>(), LENGTHS);
        let limited = sort_limit(&arrays, None, Some(15)).unwrap();
        assert_eq!(limited.iter().map(|array| array.len()).collect::<Vec<_>>(), vec![0, 13, 1, 1]);
    }

    #[test]
    fn rejects_mixed_types_and_row_counts() {
        let ints: ArrayRef = Arc::new(Int32Array::from(vec![1]));
        let strings: ArrayRef = Arc::new(StringArray::from(vec!["a"]));
        assert!(sort_to_indices(&[Arc::clone(&ints), strings], None, None).is_err());
        assert!(
            lexsort_to_indices(
                &[
                    SortColumn { values: vec![Arc::clone(&ints)], options: None },
                    SortColumn { values: vec![Arc::clone(&ints), ints], options: None },
                ],
                None
            )
            .is_err()
        );
    }
}
