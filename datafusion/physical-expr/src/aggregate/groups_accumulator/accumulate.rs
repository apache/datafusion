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

use arrow_array::{Array, ArrowNumericType, PrimitiveArray};

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
/// ```
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
    opt_filter: Option<&arrow_array::BooleanArray>,
    mut value_fn: F,
) where
    T: ArrowNumericType + Send,
    F: FnMut(usize, T::Native) + Send,
{
    assert_eq!(
        values.null_count(), 0,
        "Called accumulate_all with nullable array (call accumulate_all_nullable instead)"
    );

    // AAL TODO handle filter values

    let data: &[T::Native] = values.values();
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
    opt_filter: Option<&arrow_array::BooleanArray>,
    mut value_fn: F,
) where
    T: ArrowNumericType + Send,
    F: FnMut(usize, T::Native, bool) + Send,
{
    // AAL TODO handle filter values
    // TODO combine the null mask from values and opt_filter
    let valids = values
        .nulls()
        .expect("Called accumulate_all_nullable with non-nullable array (call accumulate_all instead)");

    // This is based on (ahem, COPY/PASTA) arrow::compute::aggregate::sum
    let data: &[T::Native] = values.values();

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

    #[test]
    fn basic() {}
}
