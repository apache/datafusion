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

// COPIED FROM https://github.com/apache/arrow-rs/blob/master/arrow-select/src/filter.rs
// The idea is to expand the API
// to allow multiple input arrays to be filtered into a single output array

//! Defines filter kernels

use arrow::array::{ArrayData, ArrayDataBuilder, MutableArrayData};
use arrow_array::builder::BooleanBufferBuilder;
use arrow_array::cast::AsArray;
use arrow_array::types::{
    ArrowDictionaryKeyType, ArrowPrimitiveType, ByteArrayType, ByteViewType,
    RunEndIndexType,
};
use arrow_array::*;
use arrow_buffer::bit_iterator::{BitIndexIterator, BitSliceIterator};
use arrow_buffer::{bit_util, ArrowNativeType, BooleanBuffer, NullBuffer, RunEndBuffer};
use arrow_buffer::{Buffer, MutableBuffer};
use arrow_schema::*;
use std::ops::AddAssign;
use std::sync::Arc;

/// If the filter selects more than this fraction of rows, use
/// [`SlicesIterator`] to copy ranges of values. Otherwise iterate
/// over individual rows using [`IndexIterator`]
///
/// Threshold of 0.8 chosen based on <https://dl.acm.org/doi/abs/10.1145/3465998.3466009>
///
const FILTER_SLICES_SELECTIVITY_THRESHOLD: f64 = 0.8;

/// An iterator of `(usize, usize)` each representing an interval
/// `[start, end)` whose slots of a bitmap [Buffer] are true. Each
/// interval corresponds to a contiguous region of memory to be
/// "taken" from an array to be filtered.
///
/// ## Notes:
///
/// 1. Ignores the validity bitmap (ignores nulls)
///
/// 2. Only performant for filters that copy across long contiguous runs
#[derive(Debug)]
pub struct SlicesIterator<'a>(BitSliceIterator<'a>);

impl<'a> SlicesIterator<'a> {
    pub fn new(filter: &'a BooleanArray) -> Self {
        Self(filter.values().set_slices())
    }
}

impl<'a> Iterator for SlicesIterator<'a> {
    type Item = (usize, usize);

    fn next(&mut self) -> Option<Self::Item> {
        self.0.next()
    }
}

/// An iterator of `usize` whose index in [`BooleanArray`] is true
///
/// This provides the best performance on most predicates, apart from those which keep
/// large runs and therefore favour [`SlicesIterator`]
struct IndexIterator<'a> {
    remaining: usize,
    iter: BitIndexIterator<'a>,
}

impl<'a> IndexIterator<'a> {
    fn new(filter: &'a BooleanArray, remaining: usize) -> Self {
        assert_eq!(filter.null_count(), 0);
        let iter = filter.values().set_indices();
        Self { remaining, iter }
    }
}

impl<'a> Iterator for IndexIterator<'a> {
    type Item = usize;

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining != 0 {
            // Fascinatingly swapping these two lines around results in a 50%
            // performance regression for some benchmarks
            let next = self.iter.next().expect("IndexIterator exhausted early");
            self.remaining -= 1;
            // Must panic if exhausted early as trusted length iterator
            return Some(next);
        }
        None
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.remaining, Some(self.remaining))
    }
}

/// Counts the number of set bits in `filter`
fn filter_count(filter: &BooleanArray) -> usize {
    filter.values().count_set_bits()
}

/// Function that can filter arbitrary arrays
///
/// Deprecated: Use [`FilterPredicate`] instead
#[deprecated]
pub type Filter<'a> = Box<dyn Fn(&ArrayData) -> ArrayData + 'a>;

/// Returns a prepared function optimized to filter multiple arrays.
/// Creating this function requires time, but using it is faster than [arrow::compute::filter] when the
/// same filter needs to be applied to multiple arrays (e.g. a multi-column `RecordBatch`).
/// WARNING: the nulls of `filter` are ignored and the value on its slot is considered.
/// Therefore, it is considered undefined behavior to pass `filter` with null values.
///
/// Deprecated: Use [`FilterBuilder`] instead
#[deprecated]
#[allow(deprecated)]
pub fn build_filter(filter: &BooleanArray) -> Result<arrow::compute::Filter, ArrowError> {
    let iter = SlicesIterator::new(filter);
    let filter_count = filter_count(filter);
    let chunks = iter.collect::<Vec<_>>();

    Ok(Box::new(move |array: &ArrayData| {
        match filter_count {
            // return all
            len if len == array.len() => array.clone(),
            0 => ArrayData::new_empty(array.data_type()),
            _ => {
                let mut mutable = MutableArrayData::new(vec![array], false, filter_count);
                chunks
                    .iter()
                    .for_each(|(start, end)| mutable.extend(0, *start, *end));
                mutable.freeze()
            }
        }
    }))
}

/// Remove null values by do a bitmask AND operation with null bits and the boolean bits.
pub fn prep_null_mask_filter(filter: &BooleanArray) -> BooleanArray {
    let nulls = filter.nulls().unwrap();
    let mask = filter.values() & nulls.inner();
    BooleanArray::new(mask, None)
}

/// Filters an [Array], returning elements matching the filter (i.e. where the values are true).
///
/// # Example
/// ```rust
/// # use arrow_array::{Int32Array, BooleanArray};
/// # use arrow_select::filter::filter;
/// let array = Int32Array::from(vec![5, 6, 7, 8, 9]);
/// let filter_array = BooleanArray::from(vec![true, false, false, true, false]);
/// let c = filter(&array, &filter_array).unwrap();
/// let c = c.as_any().downcast_ref::<Int32Array>().unwrap();
/// assert_eq!(c, &Int32Array::from(vec![5, 8]));
/// ```
pub fn filter(
    values: &dyn Array,
    predicate: &BooleanArray,
) -> Result<ArrayRef, ArrowError> {
    let predicate = FilterBuilder::new(predicate).build();
    filter_array(values, &predicate)
}

/// Returns a new [RecordBatch] with arrays containing only values matching the filter.
pub fn filter_record_batch(
    record_batch: &RecordBatch,
    predicate: &BooleanArray,
) -> Result<RecordBatch, ArrowError> {
    let mut filter_builder = FilterBuilder::new(predicate);
    if record_batch.num_columns() > 1 {
        // Only optimize if filtering more than one column
        filter_builder = filter_builder.optimize();
    }
    let filter = filter_builder.build();

    let filtered_arrays = record_batch
        .columns()
        .iter()
        .map(|a| filter_array(a, &filter))
        .collect::<Result<Vec<_>, _>>()?;
    let options = RecordBatchOptions::default().with_row_count(Some(filter.count()));
    RecordBatch::try_new_with_options(record_batch.schema(), filtered_arrays, &options)
}

/// A builder to construct [`FilterPredicate`]
#[derive(Debug)]
pub struct FilterBuilder {
    filter: BooleanArray,
    count: usize,
    strategy: IterationStrategy,
}

impl FilterBuilder {
    /// Create a new [`FilterBuilder`] that can be used to construct a [`FilterPredicate`]
    pub fn new(filter: &BooleanArray) -> Self {
        let filter = match filter.null_count() {
            0 => filter.clone(),
            _ => arrow::compute::prep_null_mask_filter(filter),
        };

        let count = filter_count(&filter);
        let strategy = IterationStrategy::default_strategy(filter.len(), count);

        Self {
            filter,
            count,
            strategy,
        }
    }

    /// Compute an optimised representation of the provided `filter` mask that can be
    /// applied to an array more quickly.
    ///
    /// Note: There is limited benefit to calling this to then filter a single array
    /// Note: This will likely have a larger memory footprint than the original mask
    pub fn optimize(mut self) -> Self {
        match self.strategy {
            IterationStrategy::SlicesIterator => {
                let slices = SlicesIterator::new(&self.filter).collect();
                self.strategy = IterationStrategy::Slices(slices)
            }
            IterationStrategy::IndexIterator => {
                let indices = IndexIterator::new(&self.filter, self.count).collect();
                self.strategy = IterationStrategy::Indices(indices)
            }
            _ => {}
        }
        self
    }

    /// Construct the final `FilterPredicate`
    pub fn build(self) -> FilterPredicate {
        FilterPredicate {
            filter: self.filter,
            count: self.count,
            strategy: self.strategy,
        }
    }
}

/// The iteration strategy used to evaluate [`FilterPredicate`]
#[derive(Debug)]
enum IterationStrategy {
    /// A lazily evaluated iterator of ranges
    SlicesIterator,
    /// A lazily evaluated iterator of indices
    IndexIterator,
    /// A precomputed list of indices
    Indices(Vec<usize>),
    /// A precomputed array of ranges
    Slices(Vec<(usize, usize)>),
    /// Select all rows
    All,
    /// Select no rows
    None,
}

impl IterationStrategy {
    /// The default [`IterationStrategy`] for a filter of length `filter_length`
    /// and selecting `filter_count` rows
    fn default_strategy(filter_length: usize, filter_count: usize) -> Self {
        if filter_length == 0 || filter_count == 0 {
            return IterationStrategy::None;
        }

        if filter_count == filter_length {
            return IterationStrategy::All;
        }

        // Compute the selectivity of the predicate by dividing the number of true
        // bits in the predicate by the predicate's total length
        //
        // This can then be used as a heuristic for the optimal iteration strategy
        let selectivity_frac = filter_count as f64 / filter_length as f64;
        if selectivity_frac > FILTER_SLICES_SELECTIVITY_THRESHOLD {
            return IterationStrategy::SlicesIterator;
        }
        IterationStrategy::IndexIterator
    }
}

/// A filtering predicate that can be applied to an [`Array`]
#[derive(Debug)]
pub struct FilterPredicate {
    filter: BooleanArray,
    count: usize,
    strategy: IterationStrategy,
}

impl FilterPredicate {
    /// Selects rows from `values` based on this [`FilterPredicate`]
    pub fn filter(&self, values: &dyn Array) -> Result<ArrayRef, ArrowError> {
        filter_array(values, self)
    }

    /// Number of rows being selected based on this [`FilterPredicate`]
    pub fn count(&self) -> usize {
        self.count
    }
}

pub fn filter_array(
    values: &dyn Array,
    predicate: &FilterPredicate,
) -> Result<ArrayRef, ArrowError> {
    if predicate.filter.len() > values.len() {
        return Err(ArrowError::InvalidArgumentError(format!(
            "Filter predicate of length {} is larger than target array of length {}",
            predicate.filter.len(),
            values.len()
        )));
    }

    match predicate.strategy {
        IterationStrategy::None => Ok(new_empty_array(values.data_type())),
        IterationStrategy::All => Ok(values.slice(0, predicate.count)),
        // actually filter
        _ => downcast_primitive_array! {
            values => Ok(Arc::new(filter_primitive(values, predicate))),
            DataType::Boolean => {
                let values = values.as_any().downcast_ref::<BooleanArray>().unwrap();
                Ok(Arc::new(filter_boolean(values, predicate)))
            }
            DataType::Utf8 => {
                Ok(Arc::new(filter_bytes(values.as_string::<i32>(), predicate)))
            }
            DataType::LargeUtf8 => {
                Ok(Arc::new(filter_bytes(values.as_string::<i64>(), predicate)))
            }
            DataType::Utf8View => {
                Ok(Arc::new(filter_byte_view(values.as_string_view(), predicate)))
            }
            DataType::Binary => {
                Ok(Arc::new(filter_bytes(values.as_binary::<i32>(), predicate)))
            }
            DataType::LargeBinary => {
                Ok(Arc::new(filter_bytes(values.as_binary::<i64>(), predicate)))
            }
            DataType::BinaryView => {
                Ok(Arc::new(filter_byte_view(values.as_binary_view(), predicate)))
            }
            DataType::RunEndEncoded(_, _) => {
                downcast_run_array!{
                    values => Ok(Arc::new(filter_run_end_array(values, predicate)?)),
                    t => unimplemented!("Filter not supported for RunEndEncoded type {:?}", t)
                }
            }
            DataType::Dictionary(_, _) => downcast_dictionary_array! {
                values => Ok(Arc::new(filter_dict(values, predicate))),
                t => unimplemented!("Filter not supported for dictionary type {:?}", t)
            }
            _ => {
                let data = values.to_data();
                // fallback to using MutableArrayData
                let mut mutable = MutableArrayData::new(
                    vec![&data],
                    false,
                    predicate.count,
                );

                match &predicate.strategy {
                    IterationStrategy::Slices(slices) => {
                        slices
                            .iter()
                            .for_each(|(start, end)| mutable.extend(0, *start, *end));
                    }
                    _ => {
                        let iter = SlicesIterator::new(&predicate.filter);
                        iter.for_each(|(start, end)| mutable.extend(0, start, end));
                    }
                }

                let data = mutable.freeze();
                Ok(make_array(data))
            }
        },
    }
}

/// Filter any supported [`RunArray`] based on a [`FilterPredicate`]
fn filter_run_end_array<R: RunEndIndexType>(
    re_arr: &RunArray<R>,
    pred: &FilterPredicate,
) -> Result<RunArray<R>, ArrowError>
where
    R::Native: Into<i64> + From<bool>,
    R::Native: AddAssign,
{
    let run_ends: &RunEndBuffer<R::Native> = re_arr.run_ends();
    let mut values_filter = BooleanBufferBuilder::new(run_ends.len());
    let mut new_run_ends = vec![R::default_value(); run_ends.len()];

    let mut start = 0i64;
    let mut i = 0;
    let filter_values = pred.filter.values();
    let mut count = R::default_value();

    for end in run_ends.inner().into_iter().map(|i| (*i).into()) {
        let mut keep = false;
        // in filter_array the predicate array is checked to have the same len as the run end array
        // this means the largest value in the run_ends is == to pred.len()
        // so we're always within bounds when calling value_unchecked
        for pred in
            (start..end).map(|i| unsafe { filter_values.value_unchecked(i as usize) })
        {
            count += R::Native::from(pred);
            keep |= pred
        }
        // this is to avoid branching
        new_run_ends[i] = count;
        i += keep as usize;

        values_filter.append(keep);
        start = end;
    }

    new_run_ends.truncate(i);

    if values_filter.is_empty() {
        new_run_ends.clear();
    }

    let values = re_arr.values();
    let pred = BooleanArray::new(values_filter.finish(), None);
    let values = arrow::compute::filter(&values, &pred)?;

    let run_ends = PrimitiveArray::<R>::new(new_run_ends.into(), None);
    RunArray::try_new(&run_ends, &values)
}

/// Computes a new null mask for `data` based on `predicate`
///
/// If the predicate selected no null-rows, returns `None`, otherwise returns
/// `Some((null_count, null_buffer))` where `null_count` is the number of nulls
/// in the filtered output, and `null_buffer` is the filtered null buffer
///
fn filter_null_mask(
    nulls: Option<&NullBuffer>,
    predicate: &FilterPredicate,
) -> Option<(usize, Buffer)> {
    let nulls = nulls?;
    if nulls.null_count() == 0 {
        return None;
    }

    let nulls = filter_bits(nulls.inner(), predicate);
    // The filtered `nulls` has a length of `predicate.count` bits and
    // therefore the null count is this minus the number of valid bits
    let null_count = predicate.count - nulls.count_set_bits_offset(0, predicate.count);

    if null_count == 0 {
        return None;
    }

    Some((null_count, nulls))
}

/// Filter the packed bitmask `buffer`, with `predicate` starting at bit offset `offset`
fn filter_bits(buffer: &BooleanBuffer, predicate: &FilterPredicate) -> Buffer {
    let src = buffer.values();
    let offset = buffer.offset();

    match &predicate.strategy {
        IterationStrategy::IndexIterator => {
            let bits = IndexIterator::new(&predicate.filter, predicate.count)
                .map(|src_idx| bit_util::get_bit(src, src_idx + offset));

            // SAFETY: `IndexIterator` reports its size correctly
            unsafe { MutableBuffer::from_trusted_len_iter_bool(bits).into() }
        }
        IterationStrategy::Indices(indices) => {
            let bits = indices
                .iter()
                .map(|src_idx| bit_util::get_bit(src, *src_idx + offset));

            // SAFETY: `Vec::iter()` reports its size correctly
            unsafe { MutableBuffer::from_trusted_len_iter_bool(bits).into() }
        }
        IterationStrategy::SlicesIterator => {
            let mut builder =
                BooleanBufferBuilder::new(bit_util::ceil(predicate.count, 8));
            for (start, end) in SlicesIterator::new(&predicate.filter) {
                builder.append_packed_range(start + offset..end + offset, src)
            }
            builder.into()
        }
        IterationStrategy::Slices(slices) => {
            let mut builder =
                BooleanBufferBuilder::new(bit_util::ceil(predicate.count, 8));
            for (start, end) in slices {
                builder.append_packed_range(*start + offset..*end + offset, src)
            }
            builder.into()
        }
        IterationStrategy::All | IterationStrategy::None => unreachable!(),
    }
}

/// `filter` implementation for boolean buffers
fn filter_boolean(array: &BooleanArray, predicate: &FilterPredicate) -> BooleanArray {
    let values = filter_bits(array.values(), predicate);

    let mut builder = ArrayDataBuilder::new(DataType::Boolean)
        .len(predicate.count)
        .add_buffer(values);

    if let Some((null_count, nulls)) = filter_null_mask(array.nulls(), predicate) {
        builder = builder.null_count(null_count).null_bit_buffer(Some(nulls));
    }

    let data = unsafe { builder.build_unchecked() };
    BooleanArray::from(data)
}

#[inline(never)]
fn filter_native<T: ArrowNativeType>(
    values: &[T],
    predicate: &FilterPredicate,
) -> Buffer {
    assert!(values.len() >= predicate.filter.len());

    let buffer = match &predicate.strategy {
        IterationStrategy::SlicesIterator => {
            let mut buffer =
                MutableBuffer::with_capacity(predicate.count * T::get_byte_width());
            for (start, end) in SlicesIterator::new(&predicate.filter) {
                buffer.extend_from_slice(&values[start..end]);
            }
            buffer
        }
        IterationStrategy::Slices(slices) => {
            let mut buffer =
                MutableBuffer::with_capacity(predicate.count * T::get_byte_width());
            for (start, end) in slices {
                buffer.extend_from_slice(&values[*start..*end]);
            }
            buffer
        }
        IterationStrategy::IndexIterator => {
            let iter =
                IndexIterator::new(&predicate.filter, predicate.count).map(|x| values[x]);

            // SAFETY: IndexIterator is trusted length
            unsafe { MutableBuffer::from_trusted_len_iter(iter) }
        }
        IterationStrategy::Indices(indices) => {
            let iter = indices.iter().map(|x| values[*x]);

            // SAFETY: `Vec::iter` is trusted length
            unsafe { MutableBuffer::from_trusted_len_iter(iter) }
        }
        IterationStrategy::All | IterationStrategy::None => unreachable!(),
    };

    buffer.into()
}

/// `filter` implementation for primitive arrays
pub(crate) fn filter_primitive<T>(
    array: &PrimitiveArray<T>,
    predicate: &FilterPredicate,
) -> PrimitiveArray<T>
where
    T: ArrowPrimitiveType,
{
    let values = array.values();
    let buffer = filter_native(values, predicate);
    let mut builder = ArrayDataBuilder::new(array.data_type().clone())
        .len(predicate.count)
        .add_buffer(buffer);

    if let Some((null_count, nulls)) = filter_null_mask(array.nulls(), predicate) {
        builder = builder.null_count(null_count).null_bit_buffer(Some(nulls));
    }

    let data = unsafe { builder.build_unchecked() };
    PrimitiveArray::from(data)
}

/// [`FilterBytes`] is created from a source [`GenericByteArray`] and can be
/// used to build a new [`GenericByteArray`] by copying values from the source
///
/// TODO(raphael): Could this be used for the take kernel as well?
struct FilterBytes<'a, OffsetSize> {
    src_offsets: &'a [OffsetSize],
    src_values: &'a [u8],
    dst_offsets: MutableBuffer,
    dst_values: MutableBuffer,
    cur_offset: OffsetSize,
}

impl<'a, OffsetSize> FilterBytes<'a, OffsetSize>
where
    OffsetSize: OffsetSizeTrait,
{
    fn new<T>(capacity: usize, array: &'a GenericByteArray<T>) -> Self
    where
        T: ByteArrayType<Offset = OffsetSize>,
    {
        let num_offsets_bytes = (capacity + 1) * std::mem::size_of::<OffsetSize>();
        let mut dst_offsets = MutableBuffer::new(num_offsets_bytes);
        let dst_values = MutableBuffer::new(0);
        let cur_offset = OffsetSize::from_usize(0).unwrap();
        dst_offsets.push(cur_offset);

        Self {
            src_offsets: array.value_offsets(),
            src_values: array.value_data(),
            dst_offsets,
            dst_values,
            cur_offset,
        }
    }

    /// Returns the byte offset at `idx`
    #[inline]
    fn get_value_offset(&self, idx: usize) -> usize {
        self.src_offsets[idx].as_usize()
    }

    /// Returns the start and end of the value at index `idx` along with its length
    #[inline]
    fn get_value_range(&self, idx: usize) -> (usize, usize, OffsetSize) {
        // These can only fail if `array` contains invalid data
        let start = self.get_value_offset(idx);
        let end = self.get_value_offset(idx + 1);
        let len = OffsetSize::from_usize(end - start).expect("illegal offset range");
        (start, end, len)
    }

    /// Extends the in-progress array by the indexes in the provided iterator
    fn extend_idx(&mut self, iter: impl Iterator<Item = usize>) {
        for idx in iter {
            let (start, end, len) = self.get_value_range(idx);
            self.cur_offset += len;
            self.dst_offsets.push(self.cur_offset);
            self.dst_values
                .extend_from_slice(&self.src_values[start..end]);
        }
    }

    /// Extends the in-progress array by the ranges in the provided iterator
    fn extend_slices(&mut self, iter: impl Iterator<Item = (usize, usize)>) {
        for (start, end) in iter {
            // These can only fail if `array` contains invalid data
            for idx in start..end {
                let (_, _, len) = self.get_value_range(idx);
                self.cur_offset += len;
                self.dst_offsets.push(self.cur_offset); // push_unchecked?
            }

            let value_start = self.get_value_offset(start);
            let value_end = self.get_value_offset(end);
            self.dst_values
                .extend_from_slice(&self.src_values[value_start..value_end]);
        }
    }
}

/// `filter` implementation for byte arrays
///
/// Note: NULLs with a non-zero slot length in `array` will have the corresponding
/// data copied across. This allows handling the null mask separately from the data
fn filter_bytes<T>(
    array: &GenericByteArray<T>,
    predicate: &FilterPredicate,
) -> GenericByteArray<T>
where
    T: ByteArrayType,
{
    let mut filter = FilterBytes::new(predicate.count, array);

    match &predicate.strategy {
        IterationStrategy::SlicesIterator => {
            filter.extend_slices(SlicesIterator::new(&predicate.filter))
        }
        IterationStrategy::Slices(slices) => filter.extend_slices(slices.iter().cloned()),
        IterationStrategy::IndexIterator => {
            filter.extend_idx(IndexIterator::new(&predicate.filter, predicate.count))
        }
        IterationStrategy::Indices(indices) => filter.extend_idx(indices.iter().cloned()),
        IterationStrategy::All | IterationStrategy::None => unreachable!(),
    }

    let mut builder = ArrayDataBuilder::new(T::DATA_TYPE)
        .len(predicate.count)
        .add_buffer(filter.dst_offsets.into())
        .add_buffer(filter.dst_values.into());

    if let Some((null_count, nulls)) = filter_null_mask(array.nulls(), predicate) {
        builder = builder.null_count(null_count).null_bit_buffer(Some(nulls));
    }

    let data = unsafe { builder.build_unchecked() };
    GenericByteArray::from(data)
}

/// `filter` implementation for byte view arrays.
fn filter_byte_view<T: ByteViewType>(
    array: &GenericByteViewArray<T>,
    predicate: &FilterPredicate,
) -> GenericByteViewArray<T> {
    let new_view_buffer = filter_native(array.views(), predicate);

    let mut builder = ArrayDataBuilder::new(T::DATA_TYPE)
        .len(predicate.count)
        .add_buffer(new_view_buffer)
        .add_buffers(array.data_buffers().to_vec());

    if let Some((null_count, nulls)) = filter_null_mask(array.nulls(), predicate) {
        builder = builder.null_count(null_count).null_bit_buffer(Some(nulls));
    }

    GenericByteViewArray::from(unsafe { builder.build_unchecked() })
}

/// `filter` implementation for dictionaries
fn filter_dict<T>(
    array: &DictionaryArray<T>,
    predicate: &FilterPredicate,
) -> DictionaryArray<T>
where
    T: ArrowDictionaryKeyType,
    T::Native: num::Num,
{
    let builder = filter_primitive::<T>(array.keys(), predicate)
        .into_data()
        .into_builder()
        .data_type(array.data_type().clone())
        .child_data(vec![array.values().to_data()]);

    // SAFETY:
    // Keys were valid before, filtered subset is therefore still valid
    DictionaryArray::from(unsafe { builder.build_unchecked() })
}
