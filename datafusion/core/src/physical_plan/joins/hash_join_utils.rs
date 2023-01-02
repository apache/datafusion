use std::{fmt, usize};

use ahash::RandomState;
use arrow::record_batch::RecordBatch;
use arrow::{
    array::{
        Array, ArrayData, ArrayRef, BooleanArray, Date32Array, Date64Array,
        Decimal128Array, DictionaryArray, Float32Array, Float64Array, Int16Array,
        Int32Array, Int64Array, Int8Array, LargeStringArray, PrimitiveArray, StringArray,
        Time32MillisecondArray, Time32SecondArray, Time64MicrosecondArray,
        Time64NanosecondArray, TimestampMicrosecondArray, TimestampMillisecondArray,
        TimestampNanosecondArray, TimestampSecondArray, UInt16Array, UInt32Array,
        UInt32BufferBuilder, UInt64Array, UInt64BufferBuilder, UInt8Array,
    },
    datatypes::{
        ArrowNativeType, DataType, Int16Type, Int32Type, Int64Type, Int8Type, TimeUnit,
        UInt16Type, UInt32Type, UInt64Type, UInt8Type,
    },
};
use hashbrown::raw::RawTable;
use smallvec::{smallvec, SmallVec};

use datafusion_common::cast::{as_dictionary_array, as_string_array};
use datafusion_common::DataFusionError;
use datafusion_physical_expr::expressions::Column;
use datafusion_physical_expr::hash_utils::create_hashes;
use datafusion_physical_expr::PhysicalExpr;

use crate::physical_plan::joins::utils::{
    apply_join_filter_to_indices, JoinFilter, JoinSide,
};

/// Updates `hash` with new entries from [RecordBatch] evaluated against the expressions `on`,
/// assuming that the [RecordBatch] corresponds to the `index`th
pub fn update_hash(
    on: &[Column],
    batch: &RecordBatch,
    hash_map: &mut JoinHashMap,
    offset: usize,
    random_state: &RandomState,
    hashes_buffer: &mut Vec<u64>,
) -> datafusion_common::Result<()> {
    // evaluate the keys
    let keys_values = on
        .iter()
        .map(|c| Ok(c.evaluate(batch)?.into_array(batch.num_rows())))
        .collect::<datafusion_common::Result<Vec<_>>>()?;

    // calculate the hash values
    let hash_values = create_hashes(&keys_values, random_state, hashes_buffer)?;

    // insert hashes to key of the hashmap
    for (row, hash_value) in hash_values.iter().enumerate() {
        let item = hash_map
            .0
            .get_mut(*hash_value, |(hash, _)| *hash_value == *hash);
        if let Some((_, indices)) = item {
            indices.push((row + offset) as u64);
        } else {
            hash_map.0.insert(
                *hash_value,
                (*hash_value, smallvec![(row + offset) as u64]),
                |(hash, _)| *hash,
            );
        }
    }
    Ok(())
}

// Get left and right indices which is satisfies the on condition (include equal_conditon and filter_in_join) in the Join
#[allow(clippy::too_many_arguments)]
pub fn build_join_indices(
    probe_batch: &RecordBatch,
    build_hashmap: &JoinHashMap,
    build_input_buffer: &RecordBatch,
    on_build: &[Column],
    on_probe: &[Column],
    filter: Option<&JoinFilter>,
    random_state: &RandomState,
    null_equals_null: &bool,
    hashes_buffer: &mut Vec<u64>,
    offset: Option<usize>,
    build_side: JoinSide,
) -> datafusion_common::Result<(UInt64Array, UInt32Array)> {
    // Get the indices which is satisfies the equal join condition, like `left.a1 = right.a2`
    let (build_indices, probe_indices) = build_equal_condition_join_indices(
        build_hashmap,
        build_input_buffer,
        probe_batch,
        on_build,
        on_probe,
        random_state,
        null_equals_null,
        hashes_buffer,
        offset,
    )?;
    if let Some(filter) = filter {
        // Filter the indices which is satisfies the non-equal join condition, like `left.b1 = 10`
        apply_join_filter_to_indices(
            build_input_buffer,
            probe_batch,
            build_indices,
            probe_indices,
            filter,
            build_side,
        )
    } else {
        Ok((build_indices, probe_indices))
    }
}

macro_rules! equal_rows_elem {
    ($array_type:ident, $l: ident, $r: ident, $left: ident, $right: ident, $null_equals_null: ident) => {{
        let left_array = $l.as_any().downcast_ref::<$array_type>().unwrap();
        let right_array = $r.as_any().downcast_ref::<$array_type>().unwrap();

        match (left_array.is_null($left), right_array.is_null($right)) {
            (false, false) => left_array.value($left) == right_array.value($right),
            (true, true) => $null_equals_null,
            _ => false,
        }
    }};
}

macro_rules! equal_rows_elem_with_string_dict {
    ($key_array_type:ident, $l: ident, $r: ident, $left: ident, $right: ident, $null_equals_null: ident) => {{
        let left_array: &DictionaryArray<$key_array_type> =
            as_dictionary_array::<$key_array_type>($l).unwrap();
        let right_array: &DictionaryArray<$key_array_type> =
            as_dictionary_array::<$key_array_type>($r).unwrap();

        let (left_values, left_values_index) = {
            let keys_col = left_array.keys();
            if keys_col.is_valid($left) {
                let values_index = keys_col
                    .value($left)
                    .to_usize()
                    .expect("Can not convert index to usize in dictionary");

                (
                    as_string_array(left_array.values()).unwrap(),
                    Some(values_index),
                )
            } else {
                (as_string_array(left_array.values()).unwrap(), None)
            }
        };
        let (right_values, right_values_index) = {
            let keys_col = right_array.keys();
            if keys_col.is_valid($right) {
                let values_index = keys_col
                    .value($right)
                    .to_usize()
                    .expect("Can not convert index to usize in dictionary");

                (
                    as_string_array(right_array.values()).unwrap(),
                    Some(values_index),
                )
            } else {
                (as_string_array(right_array.values()).unwrap(), None)
            }
        };

        match (left_values_index, right_values_index) {
            (Some(left_values_index), Some(right_values_index)) => {
                left_values.value(left_values_index)
                    == right_values.value(right_values_index)
            }
            (None, None) => $null_equals_null,
            _ => false,
        }
    }};
}

/// Left and right row have equal values
/// If more data types are supported here, please also add the data types in can_hash function
/// to generate hash join logical plan.
pub fn equal_rows(
    left: usize,
    right: usize,
    left_arrays: &[ArrayRef],
    right_arrays: &[ArrayRef],
    null_equals_null: bool,
) -> datafusion_common::Result<bool> {
    let mut err = None;
    let res = left_arrays
        .iter()
        .zip(right_arrays)
        .all(|(l, r)| match l.data_type() {
            DataType::Null => {
                // lhs and rhs are both `DataType::Null`, so the equal result
                // is dependent on `null_equals_null`
                null_equals_null
            }
            DataType::Boolean => {
                equal_rows_elem!(BooleanArray, l, r, left, right, null_equals_null)
            }
            DataType::Int8 => {
                equal_rows_elem!(Int8Array, l, r, left, right, null_equals_null)
            }
            DataType::Int16 => {
                equal_rows_elem!(Int16Array, l, r, left, right, null_equals_null)
            }
            DataType::Int32 => {
                equal_rows_elem!(Int32Array, l, r, left, right, null_equals_null)
            }
            DataType::Int64 => {
                equal_rows_elem!(Int64Array, l, r, left, right, null_equals_null)
            }
            DataType::UInt8 => {
                equal_rows_elem!(UInt8Array, l, r, left, right, null_equals_null)
            }
            DataType::UInt16 => {
                equal_rows_elem!(UInt16Array, l, r, left, right, null_equals_null)
            }
            DataType::UInt32 => {
                equal_rows_elem!(UInt32Array, l, r, left, right, null_equals_null)
            }
            DataType::UInt64 => {
                equal_rows_elem!(UInt64Array, l, r, left, right, null_equals_null)
            }
            DataType::Float32 => {
                equal_rows_elem!(Float32Array, l, r, left, right, null_equals_null)
            }
            DataType::Float64 => {
                equal_rows_elem!(Float64Array, l, r, left, right, null_equals_null)
            }
            DataType::Date32 => {
                equal_rows_elem!(Date32Array, l, r, left, right, null_equals_null)
            }
            DataType::Date64 => {
                equal_rows_elem!(Date64Array, l, r, left, right, null_equals_null)
            }
            DataType::Time32(time_unit) => match time_unit {
                TimeUnit::Second => {
                    equal_rows_elem!(Time32SecondArray, l, r, left, right, null_equals_null)
                }
                TimeUnit::Millisecond => {
                    equal_rows_elem!(Time32MillisecondArray, l, r, left, right, null_equals_null)
                }
                _ => {
                    err = Some(Err(DataFusionError::Internal(
                        "Unsupported data type in hasher".to_string(),
                    )));
                    false
                }
            }
            DataType::Time64(time_unit) => match time_unit {
                TimeUnit::Microsecond => {
                    equal_rows_elem!(Time64MicrosecondArray, l, r, left, right, null_equals_null)
                }
                TimeUnit::Nanosecond => {
                    equal_rows_elem!(Time64NanosecondArray, l, r, left, right, null_equals_null)
                }
                _ => {
                    err = Some(Err(DataFusionError::Internal(
                        "Unsupported data type in hasher".to_string(),
                    )));
                    false
                }
            }
            DataType::Timestamp(time_unit, None) => match time_unit {
                TimeUnit::Second => {
                    equal_rows_elem!(
                        TimestampSecondArray,
                        l,
                        r,
                        left,
                        right,
                        null_equals_null
                    )
                }
                TimeUnit::Millisecond => {
                    equal_rows_elem!(
                        TimestampMillisecondArray,
                        l,
                        r,
                        left,
                        right,
                        null_equals_null
                    )
                }
                TimeUnit::Microsecond => {
                    equal_rows_elem!(
                        TimestampMicrosecondArray,
                        l,
                        r,
                        left,
                        right,
                        null_equals_null
                    )
                }
                TimeUnit::Nanosecond => {
                    equal_rows_elem!(
                        TimestampNanosecondArray,
                        l,
                        r,
                        left,
                        right,
                        null_equals_null
                    )
                }
            },
            DataType::Utf8 => {
                equal_rows_elem!(StringArray, l, r, left, right, null_equals_null)
            }
            DataType::LargeUtf8 => {
                equal_rows_elem!(LargeStringArray, l, r, left, right, null_equals_null)
            }
            DataType::Decimal128(_, lscale) => match r.data_type() {
                DataType::Decimal128(_, rscale) => {
                    if lscale == rscale {
                        equal_rows_elem!(
                            Decimal128Array,
                            l,
                            r,
                            left,
                            right,
                            null_equals_null
                        )
                    } else {
                        err = Some(Err(DataFusionError::Internal(
                            "Inconsistent Decimal data type in hasher, the scale should be same".to_string(),
                        )));
                        false
                    }
                }
                _ => {
                    err = Some(Err(DataFusionError::Internal(
                        "Unsupported data type in hasher".to_string(),
                    )));
                    false
                }
            },
            DataType::Dictionary(key_type, value_type)
            if *value_type.as_ref() == DataType::Utf8 =>
                {
                    match key_type.as_ref() {
                        DataType::Int8 => {
                            equal_rows_elem_with_string_dict!(
                            Int8Type,
                            l,
                            r,
                            left,
                            right,
                            null_equals_null
                        )
                        }
                        DataType::Int16 => {
                            equal_rows_elem_with_string_dict!(
                            Int16Type,
                            l,
                            r,
                            left,
                            right,
                            null_equals_null
                        )
                        }
                        DataType::Int32 => {
                            equal_rows_elem_with_string_dict!(
                            Int32Type,
                            l,
                            r,
                            left,
                            right,
                            null_equals_null
                        )
                        }
                        DataType::Int64 => {
                            equal_rows_elem_with_string_dict!(
                            Int64Type,
                            l,
                            r,
                            left,
                            right,
                            null_equals_null
                        )
                        }
                        DataType::UInt8 => {
                            equal_rows_elem_with_string_dict!(
                            UInt8Type,
                            l,
                            r,
                            left,
                            right,
                            null_equals_null
                        )
                        }
                        DataType::UInt16 => {
                            equal_rows_elem_with_string_dict!(
                            UInt16Type,
                            l,
                            r,
                            left,
                            right,
                            null_equals_null
                        )
                        }
                        DataType::UInt32 => {
                            equal_rows_elem_with_string_dict!(
                            UInt32Type,
                            l,
                            r,
                            left,
                            right,
                            null_equals_null
                        )
                        }
                        DataType::UInt64 => {
                            equal_rows_elem_with_string_dict!(
                            UInt64Type,
                            l,
                            r,
                            left,
                            right,
                            null_equals_null
                        )
                        }
                        _ => {
                            // should not happen
                            err = Some(Err(DataFusionError::Internal(
                                "Unsupported data type in hasher".to_string(),
                            )));
                            false
                        }
                    }
                }
            other => {
                // This is internal because we should have caught this before.
                err = Some(Err(DataFusionError::Internal(format!(
                    "Unsupported data type in hasher: {other}"
                ))));
                false
            }
        });

    err.unwrap_or(Ok(res))
}

// Returns the index of equal condition join result: left_indices and right_indices
// On LEFT.b1 = RIGHT.b2
// LEFT Table:
//  a1  b1  c1
//  1   1   10
//  3   3   30
//  5   5   50
//  7   7   70
//  9   8   90
//  11  8   110
// 13   10  130
// RIGHT Table:
//  a2   b2  c2
//  2    2   20
//  4    4   40
//  6    6   60
//  8    8   80
// 10   10  100
// 12   10  120
// The result is
// "+----+----+-----+----+----+-----+",
// "| a1 | b1 | c1  | a2 | b2 | c2  |",
// "+----+----+-----+----+----+-----+",
// "| 11 | 8  | 110 | 8  | 8  | 80  |",
// "| 13 | 10 | 130 | 10 | 10 | 100 |",
// "| 13 | 10 | 130 | 12 | 10 | 120 |",
// "| 9  | 8  | 90  | 8  | 8  | 80  |",
// "+----+----+-----+----+----+-----+"
// And the result of left and right indices
// left indices:  5, 6, 6, 4
// right indices: 3, 4, 5, 3
#[allow(clippy::too_many_arguments)]
pub fn build_equal_condition_join_indices(
    build_hashmap: &JoinHashMap,
    build_input_buffer: &RecordBatch,
    probe_batch: &RecordBatch,
    build_on: &[Column],
    probe_on: &[Column],
    random_state: &RandomState,
    null_equals_null: &bool,
    hashes_buffer: &mut Vec<u64>,
    offset: Option<usize>,
) -> datafusion_common::Result<(UInt64Array, UInt32Array)> {
    let keys_values = probe_on
        .iter()
        .map(|c| Ok(c.evaluate(probe_batch)?.into_array(probe_batch.num_rows())))
        .collect::<datafusion_common::Result<Vec<_>>>()?;
    let build_join_values = build_on
        .iter()
        .map(|c| {
            Ok(c.evaluate(build_input_buffer)?
                .into_array(build_input_buffer.num_rows()))
        })
        .collect::<datafusion_common::Result<Vec<_>>>()?;
    hashes_buffer.clear();
    hashes_buffer.resize(probe_batch.num_rows(), 0);
    let hash_values = create_hashes(&keys_values, random_state, hashes_buffer)?;
    // Using a buffer builder to avoid slower normal builder
    let mut left_indices = UInt64BufferBuilder::new(0);
    let mut right_indices = UInt32BufferBuilder::new(0);
    let offset_value = offset.unwrap_or(0);
    // Visit all of the right rows
    for (row, hash_value) in hash_values.iter().enumerate() {
        // Get the hash and find it in the build index

        // For every item on the left and right we check if it matches
        // This possibly contains rows with hash collisions,
        // So we have to check here whether rows are equal or not
        if let Some((_, indices)) = build_hashmap
            .0
            .get(*hash_value, |(hash, _)| *hash_value == *hash)
        {
            for &i in indices {
                // Check hash collisions
                let offset_build_index = i as usize - offset_value;
                // Check hash collisions
                if equal_rows(
                    offset_build_index,
                    row,
                    &build_join_values,
                    &keys_values,
                    *null_equals_null,
                )? {
                    left_indices.append(offset_build_index as u64);
                    right_indices.append(row as u32);
                }
            }
        }
    }
    let left = ArrayData::builder(DataType::UInt64)
        .len(left_indices.len())
        .add_buffer(left_indices.finish())
        .build()
        .unwrap();
    let right = ArrayData::builder(DataType::UInt32)
        .len(right_indices.len())
        .add_buffer(right_indices.finish())
        .build()
        .unwrap();

    Ok((
        PrimitiveArray::<UInt64Type>::from(left),
        PrimitiveArray::<UInt32Type>::from(right),
    ))
}

// Maps a `u64` hash value based on the left ["on" values] to a list of indices with this key's value.
//
// Note that the `u64` keys are not stored in the hashmap (hence the `()` as key), but are only used
// to put the indices in a certain bucket.
// By allocating a `HashMap` with capacity for *at least* the number of rows for entries at the left side,
// we make sure that we don't have to re-hash the hashmap, which needs access to the key (the hash in this case) value.
// E.g. 1 -> [3, 6, 8] indicates that the column values map to rows 3, 6 and 8 for hash value 1
// As the key is a hash value, we need to check possible hash collisions in the probe stage
// During this stage it might be the case that a row is contained the same hashmap value,
// but the values don't match. Those are checked in the [equal_rows] macro
// TODO: speed up collision check and move away from using a hashbrown HashMap
// https://github.com/apache/arrow-datafusion/issues/50
pub struct JoinHashMap(pub RawTable<(u64, SmallVec<[u64; 1]>)>);

impl fmt::Debug for JoinHashMap {
    fn fmt(&self, _f: &mut fmt::Formatter) -> fmt::Result {
        Ok(())
    }
}
