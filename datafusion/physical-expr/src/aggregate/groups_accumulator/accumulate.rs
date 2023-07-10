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

//! [`GroupsAccumulator`] helpers: [`NullState`] and [`accumulate_indices`]
//!
//! [`GroupsAccumulator`]: crate::GroupsAccumulator

use arrow::datatypes::ArrowPrimitiveType;
use arrow_array::{Array, BooleanArray, PrimitiveArray};
use arrow_buffer::{BooleanBufferBuilder, NullBuffer};

/// Track the accumulator null state per row: if any values for that
/// group were null and if any values have been seen at all for that group.
///
/// This is part of the inner loop for many [`GroupsAccumulator`]s,
/// and thus the performance is critical and so there are multiple
/// specialized implementations, invoked depending on the specific
/// combinations of the input.
///
/// Typically there are 4 potential combinations of inputs must be
/// special caseed for performance:
///
/// * With / Without filter
/// * With / Without nulls in the input
///
/// If the input has nulls, then the accumulator must potentially
/// handle each input null value specially (e.g. for `SUM` to mark the
/// corresponding sum as null)
///
/// If there are filters present, `NullState` tracks if it has seen
/// *any* value for that group (as some values may be filtered
/// out). Without a filter, the accumulator is only passed groups that
/// had at least one value to accumulate so they do not need to track
/// if they have seen values for a particular group.
#[derive(Debug)]
pub struct NullState {
    /// Tracks if a null input value has been seen for `group_index`,
    /// if there were any nulls in the input.
    ///
    /// If `null_inputs[i]` is true, have not seen any null values for
    /// group `i`, or have not seen any vaues
    ///
    /// If `null_inputs[i]` is false, saw at least one null value for
    /// group `i`
    null_inputs: Option<BooleanBufferBuilder>,

    /// If there has been an `opt_filter`, has it seen any
    /// non-filtered input values for `group_index`?
    ///
    /// If `seen_values[i]` is true, have seen at least one non null
    /// value for group `i`
    ///
    /// If `seen_values[i]` is false, have not seen any values that
    /// pass the filter yet for group `i`
    seen_values: Option<BooleanBufferBuilder>,
}

impl NullState {
    pub fn new() -> Self {
        Self {
            null_inputs: None,
            seen_values: None,
        }
    }

    /// return the size of all buffers allocated by this null state, not including self
    pub fn size(&self) -> usize {
        builder_size(self.null_inputs.as_ref()) + builder_size(self.seen_values.as_ref())
    }

    /// Invokes `value_fn(group_index, value)` for each non null, non
    /// filtered value of `value`, while tracking which groups have
    /// seen null inputs and which groups have seen any inputs if necessary
    //
    /// # Arguments:
    ///
    /// * `values`: the input arguments to the accumulator
    /// * `group_indices`:  To which groups do the rows in `values` belong, (aka group_index)
    /// * `opt_filter`: if present, only rows for which is Some(true) are included
    /// * `value_fn`: function invoked for  (group_index, value) where value is non null
    ///
    /// # Example
    ///
    /// ```text
    ///  ┌─────────┐   ┌─────────┐   ┌ ─ ─ ─ ─ ┐
    ///  │ ┌─────┐ │   │ ┌─────┐ │     ┌─────┐
    ///  │ │  2  │ │   │ │ 200 │ │   │ │  t  │ │
    ///  │ ├─────┤ │   │ ├─────┤ │     ├─────┤
    ///  │ │  2  │ │   │ │ 100 │ │   │ │  f  │ │
    ///  │ ├─────┤ │   │ ├─────┤ │     ├─────┤
    ///  │ │  0  │ │   │ │ 200 │ │   │ │  t  │ │
    ///  │ ├─────┤ │   │ ├─────┤ │     ├─────┤
    ///  │ │  1  │ │   │ │ 200 │ │   │ │NULL │ │
    ///  │ ├─────┤ │   │ ├─────┤ │     ├─────┤
    ///  │ │  0  │ │   │ │ 300 │ │   │ │  t  │ │
    ///  │ └─────┘ │   │ └─────┘ │     └─────┘
    ///  └─────────┘   └─────────┘   └ ─ ─ ─ ─ ┘
    ///
    /// group_indices   values        opt_filter
    /// ```
    ///
    /// In the example above, `value_fn` is invoked for each (group_index,
    /// value) pair where `opt_filter[i]` is true and values is non null
    ///
    /// ```text
    /// value_fn(2, 200)
    /// value_fn(0, 200)
    /// value_fn(0, 300)
    /// ```
    ///
    /// It also sets
    ///
    /// 1. `self.seen_values[group_index]` to true for all rows that had a value if there is a filter
    ///
    /// 2. `self.null_inputs[group_index]` to true for all rows that had a null in input
    pub fn accumulate<T, F>(
        &mut self,
        group_indices: &[usize],
        values: &PrimitiveArray<T>,
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
        mut value_fn: F,
    ) where
        T: ArrowPrimitiveType + Send,
        F: FnMut(usize, T::Native) + Send,
    {
        let data: &[T::Native] = values.values();
        assert_eq!(data.len(), group_indices.len());

        match (values.null_count() > 0, opt_filter) {
            // no nulls, no filter,
            (false, None) => {
                // if we have previously seen nulls, ensure the null
                // buffer is big enough (start everything at valid)
                if self.null_inputs.is_some() {
                    initialize_builder(&mut self.null_inputs, total_num_groups, true);
                }
                let iter = group_indices.iter().zip(data.iter());
                for (&group_index, &new_value) in iter {
                    value_fn(group_index, new_value)
                }
            }
            // nulls, no filter
            (true, None) => {
                let nulls = values.nulls().unwrap();
                // All groups start as valid (true), and are set to
                // null if we see a null in the input)
                let null_inputs =
                    initialize_builder(&mut self.null_inputs, total_num_groups, true);

                // This is based on (ahem, COPY/PASTA) arrow::compute::aggregate::sum
                // iterate over in chunks of 64 bits for more efficient null checking
                let group_indices_chunks = group_indices.chunks_exact(64);
                let data_chunks = data.chunks_exact(64);
                let bit_chunks = nulls.inner().bit_chunks();

                let group_indices_remainder = group_indices_chunks.remainder();
                let data_remainder = data_chunks.remainder();

                group_indices_chunks
                    .zip(data_chunks)
                    .zip(bit_chunks.iter())
                    .for_each(|((group_index_chunk, data_chunk), mask)| {
                        // index_mask has value 1 << i in the loop
                        let mut index_mask = 1;
                        group_index_chunk.iter().zip(data_chunk.iter()).for_each(
                            |(&group_index, &new_value)| {
                                // valid bit was set, real vale
                                let is_valid = (mask & index_mask) != 0;
                                if is_valid {
                                    value_fn(group_index, new_value);
                                } else {
                                    // input null means this group is now null
                                    null_inputs.set_bit(group_index, false);
                                }
                                index_mask <<= 1;
                            },
                        )
                    });

                // handle any remaining bits (after the intial 64)
                let remainder_bits = bit_chunks.remainder_bits();
                group_indices_remainder
                    .iter()
                    .zip(data_remainder.iter())
                    .enumerate()
                    .for_each(|(i, (&group_index, &new_value))| {
                        let is_valid = remainder_bits & (1 << i) != 0;
                        if is_valid {
                            value_fn(group_index, new_value);
                        } else {
                            // input null means this group is now null
                            null_inputs.set_bit(group_index, false);
                        }
                    });
            }
            // no nulls, but a filter
            (false, Some(filter)) => {
                assert_eq!(filter.len(), group_indices.len());

                // default seen to false (we fill it in as we go)
                let seen_values =
                    initialize_builder(&mut self.seen_values, total_num_groups, false);
                // The performance with a filter could be improved by
                // iterating over the filter in chunks, rather than a single
                // iterator. TODO file a ticket
                group_indices
                    .iter()
                    .zip(data.iter())
                    .zip(filter.iter())
                    .for_each(|((&group_index, &new_value), filter_value)| {
                        if let Some(true) = filter_value {
                            value_fn(group_index, new_value);
                            // remember we have seen a value for this index
                            seen_values.set_bit(group_index, true);
                        }
                    })
            }
            // both null values and filters
            (true, Some(filter)) => {
                let null_inputs =
                    initialize_builder(&mut self.null_inputs, total_num_groups, true);
                let seen_values =
                    initialize_builder(&mut self.seen_values, total_num_groups, false);

                assert_eq!(filter.len(), group_indices.len());
                // The performance with a filter could be improved by
                // iterating over the filter in chunks, rather than using
                // iterators. TODO file a ticket
                filter
                    .iter()
                    .zip(group_indices.iter())
                    .zip(values.iter())
                    .for_each(|((filter_value, group_index), new_value)| {
                        if let Some(true) = filter_value {
                            if let Some(new_value) = new_value {
                                value_fn(*group_index, new_value)
                            } else {
                                // input null means this group is now null
                                null_inputs.set_bit(*group_index, false);
                            }
                            // remember we have seen a value for this index
                            seen_values.set_bit(*group_index, true);
                        }
                    })
            }
        }
    }

    /// Invokes `value_fn(group_index, value)` for each non null, non
    /// filtered value in `values`, while tracking which groups have
    /// seen null inputs and which groups have seen any inputs, for
    /// [`BooleanArray`]s.
    ///
    /// Since `BooleanArray` is not a [`PrimitiveArray`] it must be
    /// handled specially.
    ///
    /// See [`Self::accumulate`], which handles `PrimitiveArray`s, for
    /// more details on other arguments.
    pub fn accumulate_boolean<F>(
        &mut self,
        group_indices: &[usize],
        values: &BooleanArray,
        opt_filter: Option<&BooleanArray>,
        total_num_groups: usize,
        mut value_fn: F,
    ) where
        F: FnMut(usize, bool) + Send,
    {
        let data = values.values();
        assert_eq!(data.len(), group_indices.len());

        // These could be made more performant by iterating in chunks of 64 bits at a time
        match (values.null_count() > 0, opt_filter) {
            // no nulls, no filter,
            (false, None) => {
                // if we have previously seen nulls, ensure the null
                // buffer is big enough (start everything at valid)
                if self.null_inputs.is_some() {
                    initialize_builder(&mut self.null_inputs, total_num_groups, true);
                }
                group_indices.iter().zip(data.iter()).for_each(
                    |(&group_index, new_value)| value_fn(group_index, new_value),
                )
            }
            // nulls, no filter
            (true, None) => {
                let nulls = values.nulls().unwrap();
                // All groups start as valid (true), and are set to
                // null if we see a null in the input)
                let null_inputs =
                    initialize_builder(&mut self.null_inputs, total_num_groups, true);

                group_indices
                    .iter()
                    .zip(data.iter())
                    .zip(nulls.iter())
                    .for_each(|((&group_index, new_value), is_valid)| {
                        if is_valid {
                            value_fn(group_index, new_value);
                        } else {
                            // input null means this group is now null
                            null_inputs.set_bit(group_index, false);
                        }
                    })
            }
            // no nulls, but a filter
            (false, Some(filter)) => {
                assert_eq!(filter.len(), group_indices.len());

                // default seen to false (we fill it in as we go)
                let seen_values =
                    initialize_builder(&mut self.seen_values, total_num_groups, false);

                group_indices
                    .iter()
                    .zip(data.iter())
                    .zip(filter.iter())
                    .for_each(|((&group_index, new_value), filter_value)| {
                        if let Some(true) = filter_value {
                            value_fn(group_index, new_value);
                            // remember we have seen a value for this index
                            seen_values.set_bit(group_index, true);
                        }
                    })
            }
            // both null values and filters
            (true, Some(filter)) => {
                let null_inputs =
                    initialize_builder(&mut self.null_inputs, total_num_groups, true);
                let seen_values =
                    initialize_builder(&mut self.seen_values, total_num_groups, false);

                assert_eq!(filter.len(), group_indices.len());
                filter
                    .iter()
                    .zip(group_indices.iter())
                    .zip(values.iter())
                    .for_each(|((filter_value, group_index), new_value)| {
                        if let Some(true) = filter_value {
                            if let Some(new_value) = new_value {
                                value_fn(*group_index, new_value)
                            } else {
                                // input null means this group is now null
                                null_inputs.set_bit(*group_index, false);
                            }
                            // remember we have seen a value for this index
                            seen_values.set_bit(*group_index, true);
                        }
                    })
            }
        }
    }

    /// Creates the final [`NullBuffer`] representing which
    /// group_indices should have null values (because they saw a null
    /// input, or because they never saw any values)
    ///
    /// resets the internal state to empty
    pub fn build(&mut self) -> Option<NullBuffer> {
        // nulls (validity) set false for any group that saw a null
        //
        // seen_values (validity) set true for any group that saw a value
        let nulls = self
            .null_inputs
            .as_mut()
            .map(|null_inputs| NullBuffer::new(null_inputs.finish()))
            .and_then(|nulls| {
                if nulls.null_count() > 0 {
                    Some(nulls)
                } else {
                    None
                }
            });

        // if we had filters, some groups may never have seen a value
        // so they are only non-null if we have seen values
        let seen_values = self
            .seen_values
            .as_mut()
            .map(|seen_values| NullBuffer::new(seen_values.finish()));

        match (nulls, seen_values) {
            (None, None) => None,
            (Some(nulls), None) => Some(nulls),
            (None, Some(seen_values)) => Some(seen_values),
            (Some(seen_values), Some(nulls)) => {
                NullBuffer::union(Some(&seen_values), Some(&nulls))
            }
        }
    }
}

/// This function is called to update the accumulator state per row
/// when the value is not needed (e.g. COUNT)
///
/// `F`: Invoked like `value_fn(group_index) for all non null values
/// passing the filter. Note that no tracking is done for null inputs
/// or which groups have seen any values
///
/// See [`NullState::accumulate`], for more details on other
/// arguments.
pub fn accumulate_indices<F>(
    group_indices: &[usize],
    nulls: Option<&NullBuffer>,
    opt_filter: Option<&BooleanArray>,
    mut index_fn: F,
) where
    F: FnMut(usize) + Send,
{
    match (nulls, opt_filter) {
        (None, None) => {
            for &group_index in group_indices.iter() {
                index_fn(group_index)
            }
        }
        (None, Some(filter)) => {
            assert_eq!(filter.len(), group_indices.len());
            // The performance with a filter could be improved by
            // iterating over the filter in chunks, rather than a single
            // iterator. TODO file a ticket
            let iter = group_indices.iter().zip(filter.iter());
            for (&group_index, filter_value) in iter {
                if let Some(true) = filter_value {
                    index_fn(group_index)
                }
            }
        }
        (Some(valids), None) => {
            assert_eq!(valids.len(), group_indices.len());
            // This is based on (ahem, COPY/PASTA) arrow::compute::aggregate::sum
            // iterate over in chunks of 64 bits for more efficient null checking
            let group_indices_chunks = group_indices.chunks_exact(64);
            let bit_chunks = valids.inner().bit_chunks();

            let group_indices_remainder = group_indices_chunks.remainder();

            group_indices_chunks.zip(bit_chunks.iter()).for_each(
                |(group_index_chunk, mask)| {
                    // index_mask has value 1 << i in the loop
                    let mut index_mask = 1;
                    group_index_chunk.iter().for_each(|&group_index| {
                        // valid bit was set, real vale
                        let is_valid = (mask & index_mask) != 0;
                        if is_valid {
                            index_fn(group_index);
                        }
                        index_mask <<= 1;
                    })
                },
            );

            // handle any remaining bits (after the intial 64)
            let remainder_bits = bit_chunks.remainder_bits();
            group_indices_remainder
                .iter()
                .enumerate()
                .for_each(|(i, &group_index)| {
                    let is_valid = remainder_bits & (1 << i) != 0;
                    if is_valid {
                        index_fn(group_index)
                    }
                });
        }

        (Some(valids), Some(filter)) => {
            assert_eq!(filter.len(), group_indices.len());
            assert_eq!(valids.len(), group_indices.len());
            // The performance with a filter could likely be improved by
            // iterating over the filter in chunks, rather than using
            // iterators. TODO file a ticket
            filter
                .iter()
                .zip(group_indices.iter())
                .zip(valids.iter())
                .for_each(|((filter_value, &group_index), is_valid)| {
                    if let (Some(true), true) = (filter_value, is_valid) {
                        index_fn(group_index)
                    }
                })
        }
    }
}

/// Enures that `builder` contains a `BooleanBufferBuilder with at
/// least `total_num_groups`.
///
/// All new entries are initialized to `default_value`
fn initialize_builder(
    builder: &mut Option<BooleanBufferBuilder>,
    total_num_groups: usize,
    default_value: bool,
) -> &mut BooleanBufferBuilder {
    if builder.is_none() {
        *builder = Some(BooleanBufferBuilder::new(total_num_groups));
    }
    let builder = builder.as_mut().unwrap();

    if builder.len() < total_num_groups {
        let new_groups = total_num_groups - builder.len();
        builder.append_n(new_groups, default_value);
    }
    builder
}

fn builder_size(builder: Option<&BooleanBufferBuilder>) -> usize {
    builder
        .map(|null_inputs| {
            // capacity is in bits, so convert to bytes
            null_inputs.capacity() / 8
        })
        .unwrap_or(0)
}

#[cfg(test)]
mod test {
    use super::*;

    use arrow_array::UInt32Array;
    use hashbrown::HashSet;
    use rand::{rngs::ThreadRng, Rng};

    #[test]
    fn accumulate() {
        let group_indices = (0..100).collect();
        let values = (0..100).map(|i| (i + 1) * 10).collect();
        let values_with_nulls = (0..100)
            .map(|i| if i % 3 == 0 { None } else { Some((i + 1) * 10) })
            .collect();

        // default to every fifth value being false, every even
        // being null
        let filter: BooleanArray = (0..100)
            .map(|i| {
                let is_even = i % 2 == 0;
                let is_fifth = i % 5 == 0;
                if is_even {
                    None
                } else if is_fifth {
                    Some(false)
                } else {
                    Some(true)
                }
            })
            .collect();

        Fixture {
            group_indices,
            values,
            values_with_nulls,
            filter,
        }
        .run()
    }

    #[test]
    fn accumulate_fuzz() {
        let mut rng = rand::thread_rng();
        for _ in 0..100 {
            Fixture::new_random(&mut rng).run();
        }
    }

    /// Values for testing (there are enough values to exercise the 64 bit chunks
    struct Fixture {
        /// 100..0
        group_indices: Vec<usize>,

        /// 10, 20, ... 1010
        values: Vec<u32>,

        /// same as values, but every third is null:
        /// None, Some(20), Some(30), None ...
        values_with_nulls: Vec<Option<u32>>,

        /// filter (defaults to None)
        filter: BooleanArray,
    }

    impl Fixture {
        fn new_random(rng: &mut ThreadRng) -> Self {
            // Number of input values in a batch
            let num_values: usize = rng.gen_range(1..200);
            // number of distinct groups
            let num_groups: usize = rng.gen_range(2..1000);
            let max_group = num_groups - 1;

            let group_indices: Vec<usize> = (0..num_values)
                .map(|_| rng.gen_range(0..max_group))
                .collect();

            let values: Vec<u32> = (0..num_values).map(|_| rng.gen()).collect();

            // 10% chance of false
            // 10% change of null
            // 80% chance of true
            let filter: BooleanArray = (0..num_values)
                .map(|_| {
                    let filter_value = rng.gen_range(0.0..1.0);
                    if filter_value < 0.1 {
                        Some(false)
                    } else if filter_value < 0.2 {
                        None
                    } else {
                        Some(true)
                    }
                })
                .collect();

            // random values with random number and location of nulls
            // random null percentage
            let null_pct: f32 = rng.gen_range(0.0..1.0);
            let values_with_nulls: Vec<Option<u32>> = (0..num_values)
                .map(|_| {
                    let is_null = null_pct < rng.gen_range(0.0..1.0);
                    if is_null {
                        None
                    } else {
                        Some(rng.gen())
                    }
                })
                .collect();

            Self {
                group_indices,
                values,
                values_with_nulls,
                filter,
            }
        }

        /// returns `Self::values` an Array
        fn values_array(&self) -> UInt32Array {
            UInt32Array::from(self.values.clone())
        }

        /// returns `Self::values_with_nulls` as an Array
        fn values_with_nulls_array(&self) -> UInt32Array {
            UInt32Array::from(self.values_with_nulls.clone())
        }

        /// Calls `NullState::accumulate` and `accumulate_indices`
        /// with all combinations of nulls and filter values
        fn run(&self) {
            let total_num_groups = *self.group_indices.iter().max().unwrap() + 1;

            let group_indices = &self.group_indices;
            let values_array = self.values_array();
            let values_with_nulls_array = self.values_with_nulls_array();
            let filter = &self.filter;

            // no null, no filters
            Self::accumulate_test(group_indices, &values_array, None, total_num_groups);

            // nulls, no filters
            Self::accumulate_test(
                group_indices,
                &values_with_nulls_array,
                None,
                total_num_groups,
            );

            // no nulls, filters
            Self::accumulate_test(
                group_indices,
                &values_array,
                Some(filter),
                total_num_groups,
            );

            // nulls, filters
            Self::accumulate_test(
                group_indices,
                &values_with_nulls_array,
                Some(filter),
                total_num_groups,
            );
        }

        /// Calls `NullState::accumulate` and `accumulate_indices` to
        /// ensure it generates the correct values.
        ///
        fn accumulate_test(
            group_indices: &[usize],
            values: &UInt32Array,
            opt_filter: Option<&BooleanArray>,
            total_num_groups: usize,
        ) {
            Self::accumulate_values_test(
                group_indices,
                values,
                opt_filter,
                total_num_groups,
            );
            Self::accumulate_indices_test(group_indices, values.nulls(), opt_filter);

            // Convert values into a boolean array (using anything above the average)
            //let avg = values.iter().filter_map(|
        }

        /// This is effectively a different implementation of
        /// accumulate that we compare with the above implementation
        fn accumulate_values_test(
            group_indices: &[usize],
            values: &UInt32Array,
            opt_filter: Option<&BooleanArray>,
            total_num_groups: usize,
        ) {
            let mut accumulated_values = vec![];
            let mut null_state = NullState::new();

            null_state.accumulate(
                group_indices,
                values,
                opt_filter,
                total_num_groups,
                |group_index, value| {
                    accumulated_values.push((group_index, value));
                },
            );

            // Figure out the expected values
            let mut expected_values = vec![];
            let mut expected_null_input = HashSet::new();
            let mut expected_seen_values = HashSet::new();

            match opt_filter {
                None => group_indices.iter().zip(values.iter()).for_each(
                    |(&group_index, value)| {
                        expected_seen_values.insert(group_index);
                        if let Some(value) = value {
                            expected_values.push((group_index, value));
                        } else {
                            expected_null_input.insert(group_index);
                        }
                    },
                ),
                Some(filter) => {
                    group_indices
                        .iter()
                        .zip(values.iter())
                        .zip(filter.iter())
                        .for_each(|((&group_index, value), is_included)| {
                            // if value passed filter
                            if let Some(true) = is_included {
                                expected_seen_values.insert(group_index);
                                if let Some(value) = value {
                                    expected_values.push((group_index, value));
                                } else {
                                    expected_null_input.insert(group_index);
                                }
                            }
                        });
                }
            }

            assert_eq!(accumulated_values, expected_values,
                       "\n\naccumulated_values:{accumulated_values:#?}\n\nexpected_values:{expected_values:#?}");

            // validate null state
            if values.null_count() > 0 {
                let null_inputs =
                    null_state.null_inputs.as_ref().unwrap().finish_cloned();
                for (group_index, is_valid) in null_inputs.iter().enumerate() {
                    let expected_valid = !expected_null_input.contains(&group_index);
                    assert_eq!(
                        expected_valid, is_valid,
                        "mismatch at for group {group_index}"
                    );
                }
            }

            // validate seen_values

            if opt_filter.is_some() {
                let seen_values =
                    null_state.seen_values.as_ref().unwrap().finish_cloned();
                for (group_index, is_seen) in seen_values.iter().enumerate() {
                    let expected_seen = expected_seen_values.contains(&group_index);
                    assert_eq!(
                        expected_seen, is_seen,
                        "mismatch at for group {group_index}"
                    );
                }
            }

            // Validate the final buffer (one value per group)
            let expected_null_buffer =
                match (values.null_count() > 0, opt_filter.is_some()) {
                    (false, false) => None,
                    // only nulls
                    (true, false) => {
                        let null_buffer: NullBuffer = (0..total_num_groups)
                            .map(|group_index| {
                                // there was and no null inputs
                                !expected_null_input.contains(&group_index)
                            })
                            .collect();
                        Some(null_buffer)
                    }
                    // only filter
                    (false, true) => {
                        let null_buffer: NullBuffer = (0..total_num_groups)
                            .map(|group_index| {
                                // we saw a value
                                expected_seen_values.contains(&group_index)
                            })
                            .collect();
                        Some(null_buffer)
                    }
                    // nulls and filter
                    (true, true) => {
                        let null_buffer: NullBuffer = (0..total_num_groups)
                            .map(|group_index| {
                                // output is valid if there was at least one
                                // input value and no null inputs
                                expected_seen_values.contains(&group_index)
                                    && !expected_null_input.contains(&group_index)
                            })
                            .collect();
                        Some(null_buffer)
                    }
                };

            let null_buffer = null_state.build();

            assert_eq!(null_buffer, expected_null_buffer);
        }

        // Calls `accumulate_indices`
        // and opt_filter and ensures it calls the right values
        fn accumulate_indices_test(
            group_indices: &[usize],
            nulls: Option<&NullBuffer>,
            opt_filter: Option<&BooleanArray>,
        ) {
            let mut accumulated_values = vec![];

            accumulate_indices(group_indices, nulls, opt_filter, |group_index| {
                accumulated_values.push(group_index);
            });

            // Figure out the expected values
            let mut expected_values = vec![];

            match (nulls, opt_filter) {
                (None, None) => group_indices.iter().for_each(|&group_index| {
                    expected_values.push(group_index);
                }),
                (Some(nulls), None) => group_indices.iter().zip(nulls.iter()).for_each(
                    |(&group_index, is_valid)| {
                        if is_valid {
                            expected_values.push(group_index);
                        }
                    },
                ),
                (None, Some(filter)) => group_indices.iter().zip(filter.iter()).for_each(
                    |(&group_index, is_included)| {
                        if let Some(true) = is_included {
                            expected_values.push(group_index);
                        }
                    },
                ),
                (Some(nulls), Some(filter)) => {
                    group_indices
                        .iter()
                        .zip(nulls.iter())
                        .zip(filter.iter())
                        .for_each(|((&group_index, is_valid), is_included)| {
                            // if value passed filter
                            if let (true, Some(true)) = (is_valid, is_included) {
                                expected_values.push(group_index);
                            }
                        });
                }
            }

            assert_eq!(accumulated_values, expected_values,
                       "\n\naccumulated_values:{accumulated_values:#?}\n\nexpected_values:{expected_values:#?}");
        }
    }
}
