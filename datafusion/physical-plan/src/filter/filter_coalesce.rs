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

mod bytes;

use std::sync::Arc;

use arrow_schema::{DataType, Schema};
pub(crate) use bytes::ByteFilterBuilder;

use arrow::{
    array::AsArray,
    compute::{prep_null_mask_filter, SlicesIterator},
};
use arrow_array::{
    new_empty_array, Array, ArrayRef, ArrowPrimitiveType, BooleanArray, PrimitiveArray,
};
use arrow_buffer::{bit_iterator::BitIndexIterator, ScalarBuffer};
use datafusion_common::Result;
use datafusion_expr::sqlparser::keywords::NULL;

use crate::null_builder::MaybeNullBufferBuilder;

/// If the filter selects more than this fraction of rows, use
/// [`SlicesIterator`] to copy ranges of values. Otherwise iterate
/// over individual rows using [`IndexIterator`]
///
/// Threshold of 0.8 chosen based on <https://dl.acm.org/doi/abs/10.1145/3465998.3466009>
///
const FILTER_SLICES_SELECTIVITY_THRESHOLD: f64 = 0.8;

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
    // /// Selects rows from `values` based on this [`FilterPredicate`]
    // pub fn filter(&self, values: &dyn Array) -> Result<ArrayRef> {
    //     filter_array(values, self)
    // }

    /// Number of rows being selected based on this [`FilterPredicate`]
    pub fn count(&self) -> usize {
        self.count
    }
}

pub trait FilterCoalescer: Send + Sync {
    fn append_filtered_array(
        &mut self,
        array: &ArrayRef,
        predicate: &FilterPredicate,
    ) -> Result<()>;

    fn row_count(&self) -> usize;
    fn build(self: Box<Self>) -> ArrayRef;
}

#[derive(Debug)]
pub struct PrimitiveFilterBuilder<T: ArrowPrimitiveType, const NULLABLE: bool> {
    filter_values: Vec<T::Native>,
    nulls: MaybeNullBufferBuilder,
}

impl<T, const NULLABLE: bool> PrimitiveFilterBuilder<T, NULLABLE>
where
    T: ArrowPrimitiveType,
{
    pub fn new() -> Self {
        Self {
            filter_values: vec![],
            nulls: MaybeNullBufferBuilder::new(),
        }
    }
}

impl<T, const NULLABLE: bool> FilterCoalescer for PrimitiveFilterBuilder<T, NULLABLE>
where
    T: ArrowPrimitiveType,
{
    fn append_filtered_array(
        &mut self,
        array: &ArrayRef,
        predicate: &FilterPredicate,
    ) -> Result<()> {
        let arr = array.as_primitive::<T>();
        let values = arr.values();

        match &predicate.strategy {
            IterationStrategy::SlicesIterator => {
                if NULLABLE {
                    for (start, end) in SlicesIterator::new(&predicate.filter) {
                        for row in start..end {
                            if arr.is_valid(row) {
                                self.filter_values.push(values[row]);
                                self.nulls.append(false);
                            } else {
                                self.filter_values.push(T::default_value());
                                self.nulls.append(true);
                            }
                        }
                    }
                } else {
                    for (start, end) in SlicesIterator::new(&predicate.filter) {
                        self.filter_values.extend(&values[start..end]);
                    }
                }
            }
            IterationStrategy::Slices(slices) => {
                if NULLABLE {
                    for (start, end) in slices {
                        let start = *start;
                        let end = *end;
                        for row in start..end {
                            if arr.is_valid(row) {
                                self.filter_values.push(values[row]);
                                self.nulls.append(false);
                            } else {
                                self.filter_values.push(T::default_value());
                                self.nulls.append(true);
                            }
                        }
                    }
                } else {
                    for (start, end) in SlicesIterator::new(&predicate.filter) {
                        self.filter_values.extend(&values[start..end]);
                    }
                }
            }
            IterationStrategy::IndexIterator => {
                if NULLABLE {
                    for row in IndexIterator::new(&predicate.filter, predicate.count) {
                        if arr.is_valid(row) {
                            self.filter_values.push(values[row]);
                            self.nulls.append(false);
                        } else {
                            self.filter_values.push(T::default_value());
                            self.nulls.append(true);
                        }
                    }
                } else {
                    for row in IndexIterator::new(&predicate.filter, predicate.count) {
                        self.filter_values.push(values[row]);
                    }
                }
            }
            IterationStrategy::Indices(indices) => {
                if NULLABLE {
                    for row in indices {
                        if arr.is_valid(*row) {
                            self.filter_values.push(values[*row]);
                            self.nulls.append(false);
                        } else {
                            self.filter_values.push(T::default_value());
                            self.nulls.append(true);
                        }
                    }
                } else {
                    let iter = indices.iter().map(|x| values[*x]);
                    self.filter_values.extend(iter);
                }
            }
            IterationStrategy::None => {}
            IterationStrategy::All => {
                for v in arr.iter() {
                    if let Some(v) = v {
                        self.filter_values.push(v);
                        self.nulls.append(false);
                    } else {
                        self.filter_values.push(T::default_value());
                        self.nulls.append(true);
                    }
                }
            }
        }

        Ok(())
    }

    fn row_count(&self) -> usize {
        self.filter_values.len()
    }

    fn build(self: Box<Self>) -> ArrayRef {
        let Self {
            filter_values,
            nulls,
        } = *self;

        let nulls = nulls.build();
        if !NULLABLE {
            assert!(nulls.is_none(), "unexpected nulls in non nullable input");
        }

        Arc::new(PrimitiveArray::<T>::new(
            ScalarBuffer::from(filter_values),
            nulls,
        ))
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

impl Iterator for IndexIterator<'_> {
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

/// Returns true if [`GroupValuesColumn`] supported for the specified schema
pub fn supported_schema(schema: &Schema) -> bool {
    schema
        .fields()
        .iter()
        .map(|f| f.data_type())
        .all(supported_type)
}

/// Returns true if the specified data type is supported by [`GroupValuesColumn`]
///
/// In order to be supported, there must be a specialized implementation of
/// [`GroupColumn`] for the data type, instantiated in [`GroupValuesColumn::intern`]
fn supported_type(data_type: &DataType) -> bool {
    matches!(
        *data_type,
        DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float32
            | DataType::Float64
            | DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Binary
            | DataType::LargeBinary
            | DataType::Date32
            | DataType::Date64 // | DataType::Utf8View
                               // | DataType::BinaryView
    )
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
            _ => prep_null_mask_filter(filter),
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

/// Counts the number of set bits in `filter`
fn filter_count(filter: &BooleanArray) -> usize {
    filter.values().count_set_bits()
}
