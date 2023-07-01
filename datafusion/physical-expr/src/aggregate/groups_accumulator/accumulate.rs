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

//! Vectorized [`accumulate`] and [`accumulate_nullable`] functions

use arrow_array::{Array, ArrowNumericType, BooleanArray, PrimitiveArray};

/// This function is used to update the accumulator state per row,
/// for a `PrimitiveArray<T>` with no nulls. It is the inner loop for
/// many GroupsAccumulators and thus performance critical.
///
/// # Arguments:
///
/// * `values`: the input arguments to the accumulator
/// * `group_indices`:  To which groups do the rows in `values` belong, group id)
/// * `opt_filter`: if present, invoke value_fn if opt_filter[i] is true
/// * `value_fn`: function invoked for each (group_index, value) pair.
///
/// `F`: Invoked for each input row like `value_fn(group_index, value)
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
/// value) pair where `opt_filter[i]` is true
///
/// ```text
/// value_fn(2, 200)
/// value_fn(0, 200)
/// value_fn(0, 300)
/// ```
///
/// I couldn't find any way to combine this with
/// accumulate_all_nullable without having to pass in a is_null on
/// every row.
///
pub fn accumulate_all<T, F>(
    group_indicies: &[usize],
    values: &PrimitiveArray<T>,
    opt_filter: Option<&BooleanArray>,
    mut value_fn: F,
) where
    T: ArrowNumericType + Send,
    F: FnMut(usize, T::Native) + Send,
{
    // Given performance is critical, assert if the wrong flavor is called
    assert_eq!(
        values.null_count(), 0,
        "Called accumulate_all with nullable array (call accumulate_all_nullable instead)"
    );

    // AAL TODO handle filter values

    let data: &[T::Native] = values.values();
    assert_eq!(data.len(), group_indicies.len());

    let iter = group_indicies.iter().zip(data.iter());
    for (&group_index, &new_value) in iter {
        value_fn(group_index, new_value)
    }
}

/// This function is called to update the accumulator state per row,
/// for a `PrimitiveArray<T>` that can have nulls. See
/// [`accumulate_all`] for more detail and example
///
/// `F`: Invoked like `value_fn(group_index, value, is_valid).
///
/// NOTE the parameter is true when the value is VALID (not when it is
/// NULL).
pub fn accumulate_all_nullable<T, F>(
    group_indicies: &[usize],
    values: &PrimitiveArray<T>,
    opt_filter: Option<&BooleanArray>,
    mut value_fn: F,
) where
    T: ArrowNumericType + Send,
    F: FnMut(usize, T::Native, bool) + Send,
{
    // AAL TODO handle filter values

    // Given performance is critical, assert if the wrong flavor is called
    let valids = values
        .nulls()
        .expect("Called accumulate_all_nullable with non-nullable array (call accumulate_all instead)");

    // This is based on (ahem, COPY/PASTA) arrow::compute::aggregate::sum
    let data: &[T::Native] = values.values();
    assert_eq!(data.len(), group_indicies.len());

    let group_indices_chunks = group_indicies.chunks_exact(64);
    let data_chunks = data.chunks_exact(64);
    let bit_chunks = valids.inner().bit_chunks();

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
                    value_fn(group_index, new_value, is_valid);
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
            value_fn(group_index, new_value, is_valid)
        });
}

#[cfg(test)]
mod test {
    use super::*;

    use arrow_array::UInt32Array;
    use rand::{rngs::ThreadRng, Rng};

    #[test]
    fn accumulate_no_filter() {
        Fixture::new().accumulate_all_test()
    }

    #[test]
    #[should_panic(
        expected = "assertion failed: `(left == right)`\n  left: `34`,\n right: `0`: Called accumulate_all with nullable array (call accumulate_all_nullable instead)"
    )]
    fn accumulate_with_nullable_panics() {
        let fixture = Fixture::new();
        // call with an array that has nulls should panic
        accumulate_all(
            &fixture.group_indices,
            &fixture.values_with_nulls_array(),
            fixture.opt_filter(),
            |_, _| {},
        );
    }

    #[test]
    fn accumulate_nullable_no_filter() {
        Fixture::new().accumulate_all_nullable_test()
    }

    #[test]
    #[should_panic(
        expected = "Called accumulate_all_nullable with non-nullable array (call accumulate_all instead)"
    )]
    fn accumulate_nullable_with_non_nullable_panics() {
        let fixture = Fixture::new();
        // call with an array that has nulls should panic
        accumulate_all_nullable(
            &fixture.group_indices,
            &fixture.values_array(),
            fixture.opt_filter(),
            |_, _, _| {},
        );
    }

    // TODO: filter testing with/without null

    #[test]
    fn accumulate_fuzz() {
        let mut rng = rand::thread_rng();
        for _ in 0..100 {
            Fixture::new_random(&mut rng).accumulate_all_test();
        }
    }

    #[test]
    fn accumulate_nullable_fuzz() {
        let mut rng = rand::thread_rng();
        let mut nullable_called = false;
        for _ in 0..100 {
            let fixture = Fixture::new_random(&mut rng);
            // sometimes the random generator will create an array
            // with no nulls so avoid panic'ing in tests
            if fixture.values_with_nulls.iter().any(|v| v.is_none()) {
                nullable_called = true;
                fixture.accumulate_all_nullable_test();
            } else {
                fixture.accumulate_all_test();
            }
            assert!(nullable_called);
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

        /// Optional filter (defaults to None)
        opt_filter: Option<BooleanArray>,
    }

    impl Fixture {
        fn new() -> Self {
            Self {
                group_indices: (0..100).collect(),
                values: (0..100).map(|i| (i + 1) * 10).collect(),
                values_with_nulls: (0..100)
                    .map(|i| if i % 3 == 0 { None } else { Some((i + 1) * 10) })
                    .collect(),
                opt_filter: None,
            }
        }

        fn new_random(rng: &mut ThreadRng) -> Self {
            let num_groups: usize = rng.gen_range(0..1000);
            let group_indices: Vec<usize> = (0..num_groups).map(|_| rng.gen()).collect();

            let values: Vec<u32> = (0..num_groups).map(|_| rng.gen()).collect();

            // random values with random number and location of nulls
            // random null percentage
            let null_pct: f32 = rng.gen_range(0.0..1.0);
            let values_with_nulls: Vec<Option<u32>> = (0..num_groups)
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
                opt_filter: None,
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

        fn opt_filter(&self) -> Option<&BooleanArray> {
            self.opt_filter.as_ref()
        }

        // Calls `accumulate_all` with group_indices, values, and
        // opt_filter and ensures it calls the right values
        fn accumulate_all_test(&self) {
            let mut accumulated = vec![];
            accumulate_all(
                &self.group_indices,
                &self.values_array(),
                self.opt_filter(),
                |group_index, value| accumulated.push((group_index, value)),
            );

            // Should have see all indexes and values in order
            accumulated
                .into_iter()
                .enumerate()
                .for_each(|(i, (group_index, value))| {
                    assert_eq!(group_index, self.group_indices[i]);
                    assert_eq!(value, self.values[i]);
                })
        }

        // Calls `accumulate_all_nullable` with group_indices, values,
        // and opt_filter and ensures it calls the right values
        fn accumulate_all_nullable_test(&self) {
            let mut accumulated = vec![];

            accumulate_all_nullable(
                &self.group_indices,
                &self.values_with_nulls_array(),
                self.opt_filter(),
                |group_index, value, is_valid| {
                    let value = if is_valid { Some(value) } else { None };
                    accumulated.push((group_index, value));
                },
            );

            // Should have see all indexes and values in order
            accumulated
                .into_iter()
                .enumerate()
                .for_each(|(i, (group_index, value))| {
                    assert_eq!(group_index, self.group_indices[i]);
                    assert_eq!(value, self.values_with_nulls[i]);
                })
        }
    }
}
