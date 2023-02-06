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

//! Hash Join related functionality used both on logical and physical plans
//!
use std::{fmt, usize};

use ahash::RandomState;
use arrow::datatypes::SchemaRef;
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

use crate::common::Result;
use arrow::compute::CastOptions;
use datafusion_common::cast::{as_dictionary_array, as_string_array};
use datafusion_common::{DataFusionError, ScalarValue};
use datafusion_expr::Operator;
use datafusion_physical_expr::expressions::{BinaryExpr, CastExpr, Column, Literal};
use datafusion_physical_expr::hash_utils::create_hashes;
use datafusion_physical_expr::intervals::interval_aritmetics::Interval;
use datafusion_physical_expr::physical_expr_visitor::{
    PhysicalExprVisitable, PhysicalExpressionVisitor, Recursion,
};
use datafusion_physical_expr::rewrite::TreeNodeRewritable;
use datafusion_physical_expr::{PhysicalExpr, PhysicalSortExpr};
use std::collections::HashMap;
use std::sync::Arc;

use crate::physical_plan::joins::utils::{
    apply_join_filter_to_indices, JoinFilter, JoinSide,
};
use crate::physical_plan::sorts::sort::SortOptions;

/// Updates `hash` with new entries from [RecordBatch] evaluated against the expressions `on`,
/// assuming that the [RecordBatch] corresponds to the `index`th
pub fn update_hash(
    on: &[Column],
    batch: &RecordBatch,
    hash_map: &mut JoinHashMap,
    offset: usize,
    random_state: &RandomState,
    hashes_buffer: &mut Vec<u64>,
) -> Result<()> {
    // evaluate the keys
    let keys_values = on
        .iter()
        .map(|c| Ok(c.evaluate(batch)?.into_array(batch.num_rows())))
        .collect::<Result<Vec<_>>>()?;

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

// Get build and probe indices which satisfies the on condition (include equal_conditon and filter_in_join) in the Join
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
) -> Result<(UInt64Array, UInt32Array)> {
    // Get the indices which satisfies the equal join condition, like `left.a1 = right.a2`
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
        // Filter the indices which satisfies the non-equal join condition, like `left.b1 = 10`
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
) -> Result<bool> {
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

// Returns the index of equal condition join result: build_indices and probe_indices
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
// And the result of build and probe indices
// build indices:  5, 6, 6, 4
// probe indices: 3, 4, 5, 3
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
) -> Result<(UInt64Array, UInt32Array)> {
    let keys_values = probe_on
        .iter()
        .map(|c| Ok(c.evaluate(probe_batch)?.into_array(probe_batch.num_rows())))
        .collect::<Result<Vec<_>>>()?;
    let build_join_values = build_on
        .iter()
        .map(|c| {
            Ok(c.evaluate(build_input_buffer)?
                .into_array(build_input_buffer.num_rows()))
        })
        .collect::<Result<Vec<_>>>()?;
    hashes_buffer.clear();
    hashes_buffer.resize(probe_batch.num_rows(), 0);
    let hash_values = create_hashes(&keys_values, random_state, hashes_buffer)?;
    // Using a buffer builder to avoid slower normal builder
    let mut build_indices = UInt64BufferBuilder::new(0);
    let mut probe_indices = UInt32BufferBuilder::new(0);
    let offset_value = offset.unwrap_or(0);
    // Visit all of the probe rows
    for (row, hash_value) in hash_values.iter().enumerate() {
        // Get the hash and find it in the build index

        // For every item on the build and probe we check if it matches
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
                    build_indices.append(offset_build_index as u64);
                    probe_indices.append(row as u32);
                }
            }
        }
    }
    let build = ArrayData::builder(DataType::UInt64)
        .len(build_indices.len())
        .add_buffer(build_indices.finish())
        .build()
        .unwrap();
    let probe = ArrayData::builder(DataType::UInt32)
        .len(probe_indices.len())
        .add_buffer(probe_indices.finish())
        .build()
        .unwrap();

    Ok((
        PrimitiveArray::<UInt64Type>::from(build),
        PrimitiveArray::<UInt32Type>::from(probe),
    ))
}

// Maps a `u64` hash value based on the build ["on" values] to a list of indices with this key's value.
//
// Note that the `u64` keys are not stored in the hashmap (hence the `()` as key), but are only used
// to put the indices in a certain bucket.
// By allocating a `HashMap` with capacity for *at least* the number of rows for entries at the build side,
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

#[derive(Debug)]
pub struct CheckFilterExprContainsSortInformation<'a> {
    /// Supported state
    pub contains: &'a mut bool,
    pub checked_expr: Arc<dyn PhysicalExpr>,
}

impl<'a> CheckFilterExprContainsSortInformation<'a> {
    pub fn new(contains: &'a mut bool, checked_expr: Arc<dyn PhysicalExpr>) -> Self {
        Self {
            contains,
            checked_expr,
        }
    }
}

impl PhysicalExpressionVisitor for CheckFilterExprContainsSortInformation<'_> {
    fn pre_visit(self, expr: Arc<dyn PhysicalExpr>) -> Result<Recursion<Self>> {
        *self.contains = *self.contains || self.checked_expr.eq(&expr);
        if *self.contains {
            Ok(Recursion::Stop(self))
        } else {
            Ok(Recursion::Continue(self))
        }
    }
}

#[derive(Debug)]
pub struct PhysicalExprColumnCollector<'a> {
    pub columns: &'a mut Vec<Column>,
}

impl<'a> PhysicalExprColumnCollector<'a> {
    pub fn new(columns: &'a mut Vec<Column>) -> Self {
        Self { columns }
    }
}

impl PhysicalExpressionVisitor for PhysicalExprColumnCollector<'_> {
    fn pre_visit(self, expr: Arc<dyn PhysicalExpr>) -> Result<Recursion<Self>> {
        if let Some(column) = expr.as_any().downcast_ref::<Column>() {
            self.columns.push(column.clone())
        }
        Ok(Recursion::Continue(self))
    }
}

/// Create main_col -> filter_col one to one mapping from filter column indices.
/// A column index looks like
///            ColumnIndex {
//                 index: 0, -> field index in main schema
//                 side: JoinSide::Left, -> child side
//             },
pub fn map_origin_col_to_filter_col(
    filter: &JoinFilter,
    schema: SchemaRef,
    side: &JoinSide,
) -> Result<HashMap<Column, Column>> {
    let mut col_to_col_map: HashMap<Column, Column> = HashMap::new();
    for (filter_schema_index, index) in filter.column_indices().iter().enumerate() {
        if index.side.eq(side) {
            // Get main field from column index
            let main_field = schema.field(index.index);
            // Create a column PhysicalExpr
            let main_col = Column::new_with_schema(main_field.name(), schema.as_ref())?;
            // Since the filter.column_indices() order directly same with intermediate schema fields, we can
            // get the column.
            let filter_field = filter.schema().field(filter_schema_index);
            let filter_col = Column::new(filter_field.name(), filter_schema_index);
            // Insert mapping
            col_to_col_map.insert(main_col, filter_col);
        }
    }
    Ok(col_to_col_map)
}

/// This function plays an important role in the expression graph traversal process. It is necessary to analyze the `PhysicalSortExpr`
/// because the sorting of expressions is required for join filter expressions.
///
/// The method works as follows:
/// 1. Maps the original columns to the filter columns using the `map_origin_col_to_filter_col` function.
/// 2. Collects all columns in the sort expression using the `PhysicalExprColumnCollector` visitor.
/// 3. Checks if all columns are included in the `column_mapping_information` map.
/// 4. If all columns are included, the sort expression is converted into a filter expression using the `transform_up` and `convert_filter_columns` functions.
/// 5. Searches the converted filter expression in the filter expression using the `CheckFilterExprContainsSortInformation` visitor.
/// 6. If an exact match is encountered, returns the converted filter expression as `Some(Arc<dyn PhysicalExpr>)`.
/// 7. If all columns are not included or the exact match is not encountered, returns `None`.
///
/// Use Cases:
/// Consider the filter expression "a + b > c + 10 AND a + b < c + 100".
/// 1. If the expression "a@ + d@" is sorted, it will not be accepted since the "d@" column is not part of the filter.
/// 2. If the expression "d@" is sorted, it will not be accepted since the "d@" column is not part of the filter.
/// 3. If the expression "a@ + b@ + c@" is sorted, all columns are represented in the filter expression. However,
///    there is no exact match, so this expression does not indicate pruning.
///
pub fn convert_sort_expr_with_filter_schema(
    side: &JoinSide,
    filter: &JoinFilter,
    schema: SchemaRef,
    sort_expr: &PhysicalSortExpr,
) -> Result<Option<Arc<dyn PhysicalExpr>>> {
    let column_mapping_information: HashMap<Column, Column> =
        map_origin_col_to_filter_col(filter, schema, side)?;
    let expr = sort_expr.expr.clone();
    // Get main schema columns
    let mut expr_columns = vec![];
    expr.clone()
        .accept(PhysicalExprColumnCollector::new(&mut expr_columns))?;
    // Calculation is possible with 'column_mapping_information' since sort exprs belong to a child.
    let all_columns_are_included = expr_columns
        .iter()
        .all(|col| column_mapping_information.contains_key(col));
    if all_columns_are_included {
        // Since we are sure that one to one column mapping includes all columns, we convert
        // the sort expression into a filter expression.
        let converted_filter_expr = expr
            .transform_up(&|p| convert_filter_columns(p, &column_mapping_information))?;
        let mut contains = false;
        // Search converted PhysicalExpr in filter expression
        filter.expression().clone().accept(
            CheckFilterExprContainsSortInformation::new(
                &mut contains,
                converted_filter_expr.clone(),
            ),
        )?;
        // If the exact match is encountered, use this sorted expression in graph traversals.
        if contains {
            Ok(Some(converted_filter_expr))
        } else {
            Ok(None)
        }
    } else {
        Ok(None)
    }
}

/// This function is used to build the filter expression based on the sort order of input columns.
///
/// It first calls the convert_sort_expr_with_filter_schema method to determine if the sort
/// order of columns can be used in the filter expression.
/// If it returns a Some value, the method wraps the result in a SortedFilterExpr
/// instance with the original sort expression, converted filter expression, and sort options.
/// If it returns a None value, this function returns None.
///
/// The SortedFilterExpr instance contains information about the sort order of columns
/// that can be used in the filter expression, which can be used to optimize the query execution process.
pub fn build_filter_input_order_v2(
    side: JoinSide,
    filter: &JoinFilter,
    schema: SchemaRef,
    order: &PhysicalSortExpr,
) -> Result<Option<SortedFilterExpr>> {
    match convert_sort_expr_with_filter_schema(&side, filter, schema, order)? {
        Some(expr) => Ok(Some(SortedFilterExpr::new(
            side,
            order.expr.clone(),
            expr,
            order.options,
        ))),
        None => Ok(None),
    }
}

/// Convert a physical expression into a filter expression using a column mapping information.
fn convert_filter_columns(
    input: Arc<dyn PhysicalExpr>,
    column_mapping_information: &HashMap<Column, Column>,
) -> Result<Option<Arc<dyn PhysicalExpr>>> {
    // Attempt to downcast the input expression to a Column type.
    if let Some(col) = input.as_any().downcast_ref::<Column>() {
        // If the downcast is successful, retrieve the corresponding filter column.
        let filter_col = column_mapping_information.get(col).unwrap().clone();
        // Return the filter column as an Arc wrapped in an Option.
        Ok(Some(Arc::new(filter_col)))
    } else {
        // If the downcast is not successful, return the input expression as is.
        Ok(Some(input))
    }
}

#[derive(Debug, Clone)]
/// The SortedFilterExpr struct is used to represent a sorted filter expression in the
/// [SymmetricHashJoinExec] struct. It contains information about the join side, the origin
/// expression, the filter expression, and the sort option.
/// The struct has several methods to access and modify its fields.
pub struct SortedFilterExpr {
    /// Column side
    pub join_side: JoinSide,
    /// Sorted expr from a particular join side (child)
    pub origin_expr: Arc<dyn PhysicalExpr>,
    /// For interval calculations, one to one mapping of the columns according to filter expression,
    /// and column indices.
    pub filter_expr: Arc<dyn PhysicalExpr>,
    /// Sort option
    pub sort_option: SortOptions,
    /// Interval
    pub interval: Interval,
    /// NodeIndex in Graph
    pub node_index: usize,
}

impl SortedFilterExpr {
    /// Constructor
    pub fn new(
        join_side: JoinSide,
        origin_expr: Arc<dyn PhysicalExpr>,
        filter_expr: Arc<dyn PhysicalExpr>,
        sort_option: SortOptions,
    ) -> Self {
        Self {
            join_side,
            origin_expr,
            filter_expr,
            sort_option,
            interval: Interval::default(),
            node_index: 0,
        }
    }
    /// Get origin expr information
    pub fn origin_expr(&self) -> Arc<dyn PhysicalExpr> {
        self.origin_expr.clone()
    }
    /// Get filter expr information
    pub fn filter_expr(&self) -> Arc<dyn PhysicalExpr> {
        self.filter_expr.clone()
    }
    /// Get sort information
    pub fn sort_option(&self) -> SortOptions {
        self.sort_option
    }
    /// Get interval information
    pub fn interval(&self) -> &Interval {
        &self.interval
    }
    /// Sets interval
    pub fn set_interval(&mut self, interval: Interval) {
        self.interval = interval;
    }
    /// Node index in ExprIntervalGraph
    pub fn node_index(&self) -> usize {
        self.node_index
    }
    /// Node index setter in ExprIntervalGraph
    pub fn set_node_index(&mut self, node_index: usize) {
        self.node_index = node_index;
    }
}
/// Filter expr for a + b > c + 10 AND a + b < c + 100
#[allow(dead_code)]
pub(crate) fn complicated_filter() -> Arc<dyn PhysicalExpr> {
    let left_expr = BinaryExpr::new(
        Arc::new(CastExpr::new(
            Arc::new(BinaryExpr::new(
                Arc::new(Column::new("0", 0)),
                Operator::Plus,
                Arc::new(Column::new("1", 1)),
            )),
            DataType::Int64,
            CastOptions { safe: false },
        )),
        Operator::Gt,
        Arc::new(BinaryExpr::new(
            Arc::new(CastExpr::new(
                Arc::new(Column::new("2", 2)),
                DataType::Int64,
                CastOptions { safe: false },
            )),
            Operator::Plus,
            Arc::new(Literal::new(ScalarValue::Int64(Some(10)))),
        )),
    );

    let right_expr = BinaryExpr::new(
        Arc::new(CastExpr::new(
            Arc::new(BinaryExpr::new(
                Arc::new(Column::new("0", 0)),
                Operator::Plus,
                Arc::new(Column::new("1", 1)),
            )),
            DataType::Int64,
            CastOptions { safe: false },
        )),
        Operator::Lt,
        Arc::new(BinaryExpr::new(
            Arc::new(CastExpr::new(
                Arc::new(Column::new("2", 2)),
                DataType::Int64,
                CastOptions { safe: false },
            )),
            Operator::Plus,
            Arc::new(Literal::new(ScalarValue::Int64(Some(100)))),
        )),
    );

    Arc::new(BinaryExpr::new(
        Arc::new(left_expr),
        Operator::And,
        Arc::new(right_expr),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::physical_plan::{
        expressions::Column,
        expressions::PhysicalSortExpr,
        joins::utils::{ColumnIndex, JoinFilter, JoinSide},
    };
    use arrow::compute::{CastOptions, SortOptions};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;
    #[test]
    fn find_expr_inside_expr() -> Result<()> {
        let filter_expr = complicated_filter();

        let mut contains1 = false;
        let expr_1 = Arc::new(Column::new("gnz", 0));
        filter_expr
            .clone()
            .accept(CheckFilterExprContainsSortInformation::new(
                &mut contains1,
                expr_1,
            ))?;
        assert!(!contains1);

        let mut contains2 = false;
        let expr_2 = Arc::new(Column::new("1", 1));
        filter_expr
            .clone()
            .accept(CheckFilterExprContainsSortInformation::new(
                &mut contains2,
                expr_2,
            ))?;
        assert!(contains2);

        let mut contains3 = false;
        let expr_3 = Arc::new(CastExpr::new(
            Arc::new(BinaryExpr::new(
                Arc::new(Column::new("0", 0)),
                Operator::Plus,
                Arc::new(Column::new("1", 1)),
            )),
            DataType::Int64,
            CastOptions { safe: false },
        ));
        filter_expr
            .clone()
            .accept(CheckFilterExprContainsSortInformation::new(
                &mut contains3,
                expr_3,
            ))?;
        assert!(contains3);

        let mut contains4 = false;
        let expr_4 = Arc::new(Column::new("1", 42));
        filter_expr
            .clone()
            .accept(CheckFilterExprContainsSortInformation::new(
                &mut contains4,
                expr_4,
            ))?;
        assert!(!contains4);

        Ok(())
    }

    #[test]
    fn build_sorted_expr() -> Result<()> {
        let left_schema = Arc::new(Schema::new(vec![
            Field::new("la1", DataType::Int32, false),
            Field::new("lb1", DataType::Int32, false),
            Field::new("lc1", DataType::Int32, false),
            Field::new("lt1", DataType::Int32, false),
            Field::new("la2", DataType::Int32, false),
            Field::new("la1_des", DataType::Int32, false),
        ]));

        let right_schema = Arc::new(Schema::new(vec![
            Field::new("ra1", DataType::Int32, false),
            Field::new("rb1", DataType::Int32, false),
            Field::new("rc1", DataType::Int32, false),
            Field::new("rt1", DataType::Int32, false),
            Field::new("ra2", DataType::Int32, false),
            Field::new("ra1_des", DataType::Int32, false),
        ]));

        let filter_col_0 = Arc::new(Column::new("0", 0));
        let filter_col_1 = Arc::new(Column::new("1", 1));
        let filter_col_2 = Arc::new(Column::new("2", 2));

        let column_indices = vec![
            ColumnIndex {
                index: 0,
                side: JoinSide::Left,
            },
            ColumnIndex {
                index: 4,
                side: JoinSide::Left,
            },
            ColumnIndex {
                index: 0,
                side: JoinSide::Right,
            },
        ];
        let intermediate_schema = Schema::new(vec![
            Field::new(filter_col_0.name(), DataType::Int32, true),
            Field::new(filter_col_1.name(), DataType::Int32, true),
            Field::new(filter_col_2.name(), DataType::Int32, true),
        ]);

        let filter_expr = complicated_filter();

        let filter = JoinFilter::new(filter_expr, column_indices, intermediate_schema);

        assert!(build_filter_input_order_v2(
            JoinSide::Left,
            &filter,
            left_schema.clone(),
            &PhysicalSortExpr {
                expr: Arc::new(Column::new("la1", 0)),
                options: SortOptions::default(),
            }
        )?
        .is_some());
        assert!(build_filter_input_order_v2(
            JoinSide::Left,
            &filter,
            left_schema,
            &PhysicalSortExpr {
                expr: Arc::new(Column::new("lt1", 3)),
                options: SortOptions::default(),
            }
        )?
        .is_none());
        assert!(build_filter_input_order_v2(
            JoinSide::Right,
            &filter,
            right_schema.clone(),
            &PhysicalSortExpr {
                expr: Arc::new(Column::new("ra1", 0)),
                options: SortOptions::default(),
            }
        )?
        .is_some());
        assert!(build_filter_input_order_v2(
            JoinSide::Right,
            &filter,
            right_schema,
            &PhysicalSortExpr {
                expr: Arc::new(Column::new("rb1", 1)),
                options: SortOptions::default(),
            }
        )?
        .is_none());

        Ok(())
    }
    // if one side is sorted by ORDER BY (a+b), and join filter condition includes (a-b).
    #[test]
    fn sorted_filter_expr_build() -> Result<()> {
        let filter_col_0 = Arc::new(Column::new("0", 0));
        let filter_col_1 = Arc::new(Column::new("1", 1));

        let column_indices = vec![
            ColumnIndex {
                index: 0,
                side: JoinSide::Left,
            },
            ColumnIndex {
                index: 1,
                side: JoinSide::Left,
            },
        ];
        let intermediate_schema = Schema::new(vec![
            Field::new(filter_col_0.name(), DataType::Int32, true),
            Field::new(filter_col_1.name(), DataType::Int32, true),
        ]);

        let filter_expr = Arc::new(BinaryExpr::new(
            Arc::new(Column::new("0", 0)),
            Operator::Minus,
            Arc::new(Column::new("1", 1)),
        ));

        let filter = JoinFilter::new(filter_expr, column_indices, intermediate_schema);

        let schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int64, false),
        ]));

        let sorted = PhysicalSortExpr {
            expr: Arc::new(BinaryExpr::new(
                Arc::new(Column::new("a", 0)),
                Operator::Plus,
                Arc::new(Column::new("b", 1)),
            )),
            options: SortOptions::default(),
        };

        let res = convert_sort_expr_with_filter_schema(
            &JoinSide::Left,
            &filter,
            schema,
            &sorted,
        )?;
        assert!(res.is_none());
        Ok(())
    }
}
