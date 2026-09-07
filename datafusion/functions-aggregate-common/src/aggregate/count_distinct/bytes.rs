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

//! [`BytesDistinctCountAccumulator`] for Utf8/LargeUtf8/Binary/LargeBinary values

use arrow::array::{ArrayRef, OffsetSizeTrait};
use datafusion_common::ScalarValue;
use datafusion_common::cast::as_list_array;
use datafusion_common::utils::SingleRowListArrayBuilder;
use datafusion_expr_common::accumulator::Accumulator;
use datafusion_physical_expr_common::binary_map::{ArrowBytesSet, OutputType};
use datafusion_physical_expr_common::binary_view_map::ArrowBytesViewSet;
use std::fmt::Debug;
use std::mem::size_of_val;

/// Specialized implementation of
/// `COUNT DISTINCT` for [`StringArray`] [`LargeStringArray`],
/// [`BinaryArray`] and [`LargeBinaryArray`].
///
/// [`StringArray`]: arrow::array::StringArray
/// [`LargeStringArray`]: arrow::array::LargeStringArray
/// [`BinaryArray`]: arrow::array::BinaryArray
/// [`LargeBinaryArray`]: arrow::array::LargeBinaryArray
#[derive(Debug)]
pub struct BytesDistinctCountAccumulator<O: OffsetSizeTrait>(ArrowBytesSet<O>);

impl<O: OffsetSizeTrait> BytesDistinctCountAccumulator<O> {
    /// The set deliberately does not pre-allocate. `GroupsAccumulatorAdapter`
    /// creates one accumulator per group, so a grouped `COUNT(DISTINCT)` over a
    /// high cardinality key holds hundreds of thousands of these at once and
    /// most of them see only a handful of values.
    ///
    /// The ungrouped path builds one of these and grows it to hold every
    /// distinct value in the input, so it is the caller that had a use for the
    /// warm up. It loses nothing here: the set grows into exactly the
    /// capacities a pre-allocated one reaches, which
    /// `ungrouped_utf8_accumulator_is_never_worse_than_a_pre_allocated_set`
    /// pins. That is why this constructor needs no signal distinguishing the
    /// two callers.
    pub fn new(output_type: OutputType) -> Self {
        Self(ArrowBytesSet::new(output_type))
    }
}

impl<O: OffsetSizeTrait> Accumulator for BytesDistinctCountAccumulator<O> {
    fn state(&mut self) -> datafusion_common::Result<Vec<ScalarValue>> {
        let set = self.0.take();
        let arr = set.into_state();
        Ok(vec![
            SingleRowListArrayBuilder::new(arr).build_list_scalar(),
        ])
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> datafusion_common::Result<()> {
        if values.is_empty() {
            return Ok(());
        }

        self.0.insert(&values[0]);

        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> datafusion_common::Result<()> {
        if states.is_empty() {
            return Ok(());
        }
        assert_eq!(
            states.len(),
            1,
            "count_distinct states must be single array"
        );

        let arr = as_list_array(&states[0])?;
        arr.iter().try_for_each(|maybe_list| {
            if let Some(list) = maybe_list {
                self.0.insert(&list);
            }
            Ok(())
        })
    }

    fn evaluate(&mut self) -> datafusion_common::Result<ScalarValue> {
        Ok(ScalarValue::Int64(Some(self.0.non_null_len() as i64)))
    }

    fn size(&self) -> usize {
        size_of_val(self) + self.0.size()
    }
}

/// Specialized implementation of
/// `COUNT DISTINCT` for [`StringViewArray`] and [`BinaryViewArray`].
///
/// [`StringViewArray`]: arrow::array::StringViewArray
/// [`BinaryViewArray`]: arrow::array::BinaryViewArray
#[derive(Debug)]
pub struct BytesViewDistinctCountAccumulator(ArrowBytesViewSet);

impl BytesViewDistinctCountAccumulator {
    /// See [`BytesDistinctCountAccumulator::new`] for why the set does not
    /// pre-allocate.
    pub fn new(output_type: OutputType) -> Self {
        Self(ArrowBytesViewSet::new(output_type))
    }
}

impl Accumulator for BytesViewDistinctCountAccumulator {
    fn state(&mut self) -> datafusion_common::Result<Vec<ScalarValue>> {
        let set = self.0.take();
        let arr = set.into_state();
        Ok(vec![
            SingleRowListArrayBuilder::new(arr).build_list_scalar(),
        ])
    }

    fn update_batch(&mut self, values: &[ArrayRef]) -> datafusion_common::Result<()> {
        if values.is_empty() {
            return Ok(());
        }

        self.0.insert(&values[0]);

        Ok(())
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> datafusion_common::Result<()> {
        if states.is_empty() {
            return Ok(());
        }
        assert_eq!(
            states.len(),
            1,
            "count_distinct states must be single array"
        );

        let arr = as_list_array(&states[0])?;
        arr.iter().try_for_each(|maybe_list| {
            if let Some(list) = maybe_list {
                self.0.insert(&list);
            }
            Ok(())
        })
    }

    fn evaluate(&mut self) -> datafusion_common::Result<ScalarValue> {
        Ok(ScalarValue::Int64(Some(self.0.non_null_len() as i64)))
    }

    fn size(&self) -> usize {
        size_of_val(self) + self.0.size()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{StringArray, StringViewArray};
    use datafusion_physical_expr_common::binary_map::INITIAL_MAP_CAPACITY;
    use datafusion_physical_expr_common::binary_view_map::INITIAL_MAP_CAPACITY as INITIAL_VIEW_MAP_CAPACITY;
    use std::sync::Arc;

    /// The batch size the aggregate stream drives an ungrouped accumulator with.
    const BATCH_SIZE: usize = 8192;

    /// Distinct value counts spanning both sides of the warm up capacity, up to
    /// ones where the two constructors have converged.
    const CARDINALITIES: [usize; 7] = [0, 1, 100, 1_000, 10_000, 100_000, 500_000];

    /// Cardinalities small enough that the warm up dominates what the set
    /// holds. This is the per group population, where `GroupsAccumulatorAdapter`
    /// holds one accumulator per group and most see a handful of values.
    const PER_GROUP_SCALE: usize = 100;

    /// Cardinalities at which a lazily built set has grown into exactly the
    /// capacities a pre-allocated one reaches. This is the ungrouped
    /// population, one set holding every distinct value in the input.
    const UNGROUPED_SCALE: usize = 10_000;

    /// Longer than the map's inline value length, so the value lands in the
    /// value buffer rather than inside the hash table entry.
    fn distinct_value(i: usize) -> String {
        format!("distinct value number {i}")
    }

    fn batches(distinct_values: usize, view: bool) -> Vec<ArrayRef> {
        (0..distinct_values)
            .step_by(BATCH_SIZE)
            .map(|start| {
                let values = (start..(start + BATCH_SIZE).min(distinct_values))
                    .map(distinct_value);
                if view {
                    Arc::new(StringViewArray::from_iter_values(values)) as ArrayRef
                } else {
                    Arc::new(StringArray::from_iter_values(values)) as ArrayRef
                }
            })
            .collect()
    }

    /// The property that decides whether the ungrouped path can afford to share
    /// the lazy constructor with the per group path.
    fn assert_lazy_is_not_worse(
        distinct_values: usize,
        lazy_size: usize,
        pre_allocated_size: usize,
    ) {
        // The guarantee that lets both paths share one lazy constructor.
        assert!(
            lazy_size <= pre_allocated_size,
            "at {distinct_values} distinct values the lazy set reported \
             {lazy_size} bytes against the {pre_allocated_size} bytes the \
             pre-allocated one reported"
        );

        if distinct_values <= PER_GROUP_SCALE {
            // The warm up is pure overhead here, and removing it is the whole
            // point of the change.
            assert!(
                lazy_size < pre_allocated_size,
                "at {distinct_values} distinct values the lazy set should be \
                 strictly cheaper, but reported {lazy_size} bytes against \
                 {pre_allocated_size}"
            );
        }

        if distinct_values >= UNGROUPED_SCALE {
            // The hash table's bucket count is a power of two fixed by the
            // number of entries, and the value buffer grows on a power of two
            // ladder, so a set that starts empty lands on exactly the
            // capacities a pre-allocated one reaches. The ungrouped path gives
            // up nothing by starting empty, which is why these accumulators
            // need no signal telling them apart from the per group ones.
            assert_eq!(
                lazy_size, pre_allocated_size,
                "at {distinct_values} distinct values the lazy and pre-allocated \
                 sets should have converged"
            );
        }
    }

    /// An ungrouped `COUNT(DISTINCT <string>)` builds a single accumulator that
    /// grows to hold every distinct value in its input, which is the population
    /// the warm up existed for. It must be no more expensive without one.
    #[test]
    fn ungrouped_utf8_accumulator_is_never_worse_than_a_pre_allocated_set() {
        for distinct_values in CARDINALITIES {
            let mut accumulator =
                BytesDistinctCountAccumulator::<i32>::new(OutputType::Utf8);
            let mut pre_allocated = ArrowBytesSet::<i32>::with_capacity(
                OutputType::Utf8,
                INITIAL_MAP_CAPACITY,
            );

            for batch in batches(distinct_values, false) {
                accumulator.update_batch(&[Arc::clone(&batch)]).unwrap();
                pre_allocated.insert(&batch);
            }

            assert_eq!(accumulator.0.non_null_len(), distinct_values);
            assert_lazy_is_not_worse(
                distinct_values,
                accumulator.0.size(),
                pre_allocated.size(),
            );
        }
    }

    /// The `Utf8View` counterpart of
    /// [`ungrouped_utf8_accumulator_is_never_worse_than_a_pre_allocated_set`].
    #[test]
    fn ungrouped_utf8_view_accumulator_is_never_worse_than_a_pre_allocated_set() {
        for distinct_values in CARDINALITIES {
            let mut accumulator =
                BytesViewDistinctCountAccumulator::new(OutputType::Utf8View);
            let mut pre_allocated = ArrowBytesViewSet::with_capacity(
                OutputType::Utf8View,
                INITIAL_VIEW_MAP_CAPACITY,
            );

            for batch in batches(distinct_values, true) {
                accumulator.update_batch(&[Arc::clone(&batch)]).unwrap();
                pre_allocated.insert(&batch);
            }

            assert_eq!(accumulator.0.non_null_len(), distinct_values);
            assert_lazy_is_not_worse(
                distinct_values,
                accumulator.0.size(),
                pre_allocated.size(),
            );
        }
    }
}
