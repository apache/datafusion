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

//! Routers for range partitioning.

use std::cmp::Ordering;
use std::sync::Arc;

use arrow::array::*;
use arrow::compute::SortOptions;
use arrow::datatypes::*;
use arrow::row::{Row, RowConverter, Rows, SortField};
use datafusion_common::{
    DataFusionError, Result, ScalarValue, exec_err, not_impl_err, plan_err,
    validate_range_split_points,
};
use datafusion_physical_expr::SplitPoint;

/// A router for assigning rows to range partitions.
#[derive(Debug, Clone)]
pub(crate) struct RangeRouter {
    data_types: Vec<DataType>,
    split_points: Vec<SplitPoint>,
    sort_options: Vec<SortOptions>,
    inner: RangeRouterInner,
}

#[derive(Debug, Clone)]
enum RangeRouterInner {
    /// Specialized fast path for a single primitive column with non-null split points.
    Primitive(PrimitiveRangeRouter),
    /// Universal fast path using Arrow's RowConverter for arbitrary types and composite keys.
    Row(RowConverterRangeRouter),
}

impl RangeRouter {
    /// Constructs the best router for the given sort options and split points,
    /// inferring key data types from the split points.
    pub(crate) fn try_new(
        sort_options: &[SortOptions],
        split_points: &[SplitPoint],
    ) -> Result<Self> {
        // Pass None so `try_new_with_optional_data_types` runs `validate_range_split_points`
        // before indexing into split point columns, avoiding a panic on width mismatch.
        Self::try_new_with_optional_data_types(sort_options, split_points, None)
    }

    /// Constructs the best router for the given sort options, split points, and target data types,
    /// coercing split point values to the target data types.
    pub(crate) fn try_new_with_data_types(
        sort_options: &[SortOptions],
        split_points: &[SplitPoint],
        data_types: &[DataType],
    ) -> Result<Self> {
        Self::try_new_with_optional_data_types(
            sort_options,
            split_points,
            Some(data_types),
        )
    }

    fn try_new_with_optional_data_types(
        sort_options: &[SortOptions],
        split_points: &[SplitPoint],
        data_types: Option<&[DataType]>,
    ) -> Result<Self> {
        validate_range_split_points(split_points, sort_options)?;

        let (data_types, split_points) = if let Some(target_dts) = data_types {
            if target_dts.len() != sort_options.len() {
                return plan_err!(
                    "Range partitioning expected {} data types for sort options, but got {}",
                    sort_options.len(),
                    target_dts.len()
                );
            }
            let coerced_split_points = split_points
                .iter()
                .map(|sp| {
                    let vals = sp
                        .values()
                        .iter()
                        .zip(target_dts)
                        .map(|(val, target_dt)| {
                            if val.data_type() == *target_dt {
                                Ok(val.clone())
                            } else {
                                val.cast_to(target_dt)
                            }
                        })
                        .collect::<Result<Vec<_>>>()?;
                    Ok(SplitPoint::new(vals))
                })
                .collect::<Result<Vec<_>>>()?;
            (target_dts.to_vec(), coerced_split_points)
        } else if !split_points.is_empty() {
            let dts: Vec<DataType> = (0..sort_options.len())
                .map(|col_idx| split_points[0].values()[col_idx].data_type())
                .collect();
            (dts, split_points.to_vec())
        } else {
            (vec![], vec![])
        };

        // Try single-column primitive fast path
        if data_types.len() == 1
            && !sort_options.is_empty()
            && let Some(primitive_router) =
                PrimitiveRangeRouter::try_new(&split_points, sort_options[0])
        {
            return Ok(Self {
                data_types,
                split_points,
                sort_options: sort_options.to_vec(),
                inner: RangeRouterInner::Primitive(primitive_router),
            });
        }

        // Try RowConverter path
        let row_router =
            RowConverterRangeRouter::try_new(&data_types, sort_options, &split_points)?;
        Ok(Self {
            data_types,
            split_points,
            sort_options: sort_options.to_vec(),
            inner: RangeRouterInner::Row(row_router),
        })
    }

    /// Data types configured in this router.
    #[cfg(test)]
    pub(crate) fn data_types(&self) -> &[DataType] {
        &self.data_types
    }

    /// Split points configured in this router.
    pub(crate) fn split_points(&self) -> &[SplitPoint] {
        &self.split_points
    }

    /// Sort options configured in this router.
    pub(crate) fn sort_options(&self) -> &[SortOptions] {
        &self.sort_options
    }

    /// Number of split points configured in this router.
    pub(crate) fn num_split_points(&self) -> usize {
        self.split_points.len()
    }

    /// Generic routing entry point that calls `emit(row_idx, partition)` for every row.
    pub(crate) fn route_with<E>(&self, arrays: &[ArrayRef], mut emit: E) -> Result<()>
    where
        E: FnMut(usize, usize),
    {
        if self.split_points.is_empty() {
            let num_rows = arrays.first().map(|a| a.len()).unwrap_or(0);
            for row_idx in 0..num_rows {
                emit(row_idx, 0);
            }
            return Ok(());
        }

        if arrays.len() != self.data_types.len() {
            return exec_err!(
                "Range partitioning expected {} columns, but got {}",
                self.data_types.len(),
                arrays.len()
            );
        }

        for (i, (arr, expected_dt)) in arrays.iter().zip(&self.data_types).enumerate() {
            if arr.data_type() != expected_dt {
                return exec_err!(
                    "Range partitioning expected column {i} to be of type {expected_dt:?}, but got {:?}",
                    arr.data_type()
                );
            }
        }

        match &self.inner {
            RangeRouterInner::Primitive(r) => {
                if let Some(first_col) = arrays.first() {
                    r.route_with(first_col.as_ref(), emit)
                } else {
                    Ok(())
                }
            }
            RangeRouterInner::Row(r) => r.route_with(arrays, emit),
        }
    }

    /// Groups row indices from `arrays` into partition index buckets.
    pub(crate) fn route_indices(
        &self,
        arrays: &[ArrayRef],
        indices: &mut [Vec<u32>],
    ) -> Result<()> {
        self.route_with(arrays, |row_idx, partition| {
            indices[partition].push(row_idx as u32);
        })
    }

    /// Appends output partition IDs to `partition_ids`.
    pub(crate) fn route_partition_ids(
        &self,
        arrays: &[ArrayRef],
        partition_ids: &mut Vec<u64>,
    ) -> Result<()> {
        let num_rows = arrays.first().map(|a| a.len()).unwrap_or(0);
        partition_ids
            .try_reserve(num_rows)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        self.route_with(arrays, |_row_idx, partition| {
            partition_ids.push(partition as u64);
        })
    }
}

macro_rules! define_primitive_router {
    ($( ($variant:ident, $type:ty, $arrow_type:ty, $array:ident) ),* $(,)?) => {
        /// Specialized router for primitive scalar types.
        #[derive(Debug, Clone)]
        enum PrimitiveRangeRouter {
            $( $variant(PrimitiveValuesRouter<$type>), )*
            Float32(FloatValuesRouter<f32>),
            Float64(FloatValuesRouter<f64>),
        }

        impl PrimitiveRangeRouter {
            fn try_new(split_points: &[SplitPoint], sort_options: SortOptions) -> Option<Self> {
                if split_points.is_empty() {
                    return None;
                }

                let scalars = split_points.iter().map(|sp| sp.values()[0].clone());
                let split_array = ScalarValue::iter_to_array(scalars).ok()?;
                if split_array.null_count() > 0 {
                    return None;
                }

                macro_rules! make_primitive {
                    ($target_arrow_type:ty, $target_variant:ident) => {{
                        let arr = split_array
                            .as_any()
                            .downcast_ref::<PrimitiveArray<$target_arrow_type>>()?;
                        let vals = arr.values().to_vec();
                        Some(Self::$target_variant(PrimitiveValuesRouter::new(vals, sort_options)))
                    }};
                }

                match split_array.data_type() {
                    DataType::Int8 => make_primitive!(Int8Type, Int8),
                    DataType::Int16 => make_primitive!(Int16Type, Int16),
                    DataType::Int32 => make_primitive!(Int32Type, Int32),
                    DataType::Int64 => make_primitive!(Int64Type, Int64),
                    DataType::UInt8 => make_primitive!(UInt8Type, UInt8),
                    DataType::UInt16 => make_primitive!(UInt16Type, UInt16),
                    DataType::UInt32 => make_primitive!(UInt32Type, UInt32),
                    DataType::UInt64 => make_primitive!(UInt64Type, UInt64),
                    DataType::Date32 => make_primitive!(Date32Type, Date32),
                    DataType::Date64 => make_primitive!(Date64Type, Date64),
                    DataType::Time32(TimeUnit::Second) => make_primitive!(Time32SecondType, Time32Second),
                    DataType::Time32(TimeUnit::Millisecond) => make_primitive!(Time32MillisecondType, Time32Millisecond),
                    DataType::Time64(TimeUnit::Microsecond) => make_primitive!(Time64MicrosecondType, Time64Microsecond),
                    DataType::Time64(TimeUnit::Nanosecond) => make_primitive!(Time64NanosecondType, Time64Nanosecond),
                    DataType::Timestamp(TimeUnit::Second, _) => make_primitive!(TimestampSecondType, TimestampSecond),
                    DataType::Timestamp(TimeUnit::Millisecond, _) => make_primitive!(TimestampMillisecondType, TimestampMillisecond),
                    DataType::Timestamp(TimeUnit::Microsecond, _) => make_primitive!(TimestampMicrosecondType, TimestampMicrosecond),
                    DataType::Timestamp(TimeUnit::Nanosecond, _) => make_primitive!(TimestampNanosecondType, TimestampNanosecond),
                    DataType::Float32 => {
                        let arr = split_array.as_any().downcast_ref::<Float32Array>()?;
                        let vals = arr.values().to_vec();
                        Some(Self::Float32(FloatValuesRouter::new(vals, sort_options)))
                    }
                    DataType::Float64 => {
                        let arr = split_array.as_any().downcast_ref::<Float64Array>()?;
                        let vals = arr.values().to_vec();
                        Some(Self::Float64(FloatValuesRouter::new(vals, sort_options)))
                    }
                    _ => None,
                }
            }

            fn route_with<E>(&self, array: &dyn Array, emit: E) -> Result<()>
            where
                E: FnMut(usize, usize),
            {
                match self {
                    $(
                        Self::$variant(r) => {
                            let arr = array.as_any().downcast_ref::<$array>().ok_or_else(|| {
                                DataFusionError::Internal(format!("Expected {}", stringify!($array)))
                            })?;
                            r.route_with(arr, emit);
                            Ok(())
                        }
                    )*
                    Self::Float32(r) => {
                        let arr = array.as_any().downcast_ref::<Float32Array>().ok_or_else(|| {
                            DataFusionError::Internal("Expected Float32Array".to_string())
                        })?;
                        r.route_with(arr, emit);
                        Ok(())
                    }
                    Self::Float64(r) => {
                        let arr = array.as_any().downcast_ref::<Float64Array>().ok_or_else(|| {
                            DataFusionError::Internal("Expected Float64Array".to_string())
                        })?;
                        r.route_with(arr, emit);
                        Ok(())
                    }
                }
            }
        }
    };
}

define_primitive_router!(
    (Int8, i8, Int8Type, Int8Array),
    (Int16, i16, Int16Type, Int16Array),
    (Int32, i32, Int32Type, Int32Array),
    (Int64, i64, Int64Type, Int64Array),
    (UInt8, u8, UInt8Type, UInt8Array),
    (UInt16, u16, UInt16Type, UInt16Array),
    (UInt32, u32, UInt32Type, UInt32Array),
    (UInt64, u64, UInt64Type, UInt64Array),
    (Date32, i32, Date32Type, Date32Array),
    (Date64, i64, Date64Type, Date64Array),
    (Time32Second, i32, Time32SecondType, Time32SecondArray),
    (
        Time32Millisecond,
        i32,
        Time32MillisecondType,
        Time32MillisecondArray
    ),
    (
        Time64Microsecond,
        i64,
        Time64MicrosecondType,
        Time64MicrosecondArray
    ),
    (
        Time64Nanosecond,
        i64,
        Time64NanosecondType,
        Time64NanosecondArray
    ),
    (
        TimestampSecond,
        i64,
        TimestampSecondType,
        TimestampSecondArray
    ),
    (
        TimestampMillisecond,
        i64,
        TimestampMillisecondType,
        TimestampMillisecondArray
    ),
    (
        TimestampMicrosecond,
        i64,
        TimestampMicrosecondType,
        TimestampMicrosecondArray
    ),
    (
        TimestampNanosecond,
        i64,
        TimestampNanosecondType,
        TimestampNanosecondArray
    ),
);

/// Generic router for primitive integer and temporal types.
#[derive(Debug, Clone)]
struct PrimitiveValuesRouter<T: ArrowNativeTypeOp + Ord + Copy + Send + Sync + 'static> {
    split_points: Vec<T>,
    sort_options: SortOptions,
}

impl<T: ArrowNativeTypeOp + Ord + Copy + Send + Sync + 'static> PrimitiveValuesRouter<T> {
    fn new(split_points: Vec<T>, sort_options: SortOptions) -> Self {
        Self {
            split_points,
            sort_options,
        }
    }

    fn route_with<A: ArrowPrimitiveType<Native = T>, E: FnMut(usize, usize)>(
        &self,
        array: &PrimitiveArray<A>,
        mut emit: E,
    ) {
        let split_points = &self.split_points;
        let descending = self.sort_options.descending;
        let nulls_first = self.sort_options.nulls_first;

        if array.null_count() == 0 {
            let values = array.values().as_ref();
            if !descending {
                for (idx, &val) in values.iter().enumerate() {
                    let p = split_points.partition_point(|&sp| sp <= val);
                    emit(idx, p);
                }
            } else {
                for (idx, &val) in values.iter().enumerate() {
                    let p = split_points.partition_point(|&sp| sp >= val);
                    emit(idx, p);
                }
            }
        } else {
            let null_partition = if nulls_first { 0 } else { split_points.len() };
            for idx in 0..array.len() {
                if array.is_null(idx) {
                    emit(idx, null_partition);
                } else {
                    let val = array.value(idx);
                    let p = if !descending {
                        split_points.partition_point(|&sp| sp <= val)
                    } else {
                        split_points.partition_point(|&sp| sp >= val)
                    };
                    emit(idx, p);
                }
            }
        }
    }
}

/// Generic router for floating point values using total ordering.
#[derive(Debug, Clone)]
struct FloatValuesRouter<T: Copy + Send + Sync + 'static> {
    split_points: Vec<T>,
    sort_options: SortOptions,
}

impl<T: Copy + Send + Sync + 'static> FloatValuesRouter<T> {
    fn new(split_points: Vec<T>, sort_options: SortOptions) -> Self {
        Self {
            split_points,
            sort_options,
        }
    }
}

macro_rules! impl_float_values_router {
    ($t:ty, $arr:ty) => {
        impl FloatValuesRouter<$t> {
            fn route_with<E: FnMut(usize, usize)>(&self, array: &$arr, mut emit: E) {
                let split_points = &self.split_points;
                let descending = self.sort_options.descending;
                let nulls_first = self.sort_options.nulls_first;

                if array.null_count() == 0 {
                    let values = array.values().as_ref();
                    if !descending {
                        for (idx, &val) in values.iter().enumerate() {
                            let p = split_points.partition_point(|&sp| {
                                sp.total_cmp(&val) != Ordering::Greater
                            });
                            emit(idx, p);
                        }
                    } else {
                        for (idx, &val) in values.iter().enumerate() {
                            let p = split_points.partition_point(|&sp| {
                                sp.total_cmp(&val) != Ordering::Less
                            });
                            emit(idx, p);
                        }
                    }
                } else {
                    let null_partition = if nulls_first { 0 } else { split_points.len() };
                    for idx in 0..array.len() {
                        if array.is_null(idx) {
                            emit(idx, null_partition);
                        } else {
                            let val = array.value(idx);
                            let p = if !descending {
                                split_points.partition_point(|&sp| {
                                    sp.total_cmp(&val) != Ordering::Greater
                                })
                            } else {
                                split_points.partition_point(|&sp| {
                                    sp.total_cmp(&val) != Ordering::Less
                                })
                            };
                            emit(idx, p);
                        }
                    }
                }
            }
        }
    };
}

impl_float_values_router!(f32, Float32Array);
impl_float_values_router!(f64, Float64Array);

/// Router backed by Arrow's RowConverter.
#[derive(Debug, Clone)]
struct RowConverterRangeRouter {
    converter: Arc<RowConverter>,
    split_point_rows: Option<Rows>,
}

impl RowConverterRangeRouter {
    fn try_new(
        data_types: &[DataType],
        sort_options: &[SortOptions],
        split_points: &[SplitPoint],
    ) -> Result<Self> {
        let sort_fields = data_types
            .iter()
            .zip(sort_options)
            .map(|(dt, opt)| SortField::new_with_options(dt.clone(), *opt))
            .collect::<Vec<_>>();

        if !RowConverter::supports_fields(&sort_fields) {
            return not_impl_err!(
                "Range partitioning is not supported for data types: {:?}",
                data_types
            );
        }

        let row_converter = RowConverter::new(sort_fields)?;
        let num_cols = data_types.len();

        let split_point_rows = if split_points.is_empty() {
            None
        } else {
            let split_point_arrays = (0..num_cols)
                .map(|col_idx| {
                    let col_scalars =
                        split_points.iter().map(|sp| sp.values()[col_idx].clone());
                    ScalarValue::iter_to_array(col_scalars)
                })
                .collect::<Result<Vec<_>>>()?;

            Some(row_converter.convert_columns(&split_point_arrays)?)
        };

        Ok(Self {
            converter: Arc::new(row_converter),
            split_point_rows,
        })
    }

    fn route_with<E: FnMut(usize, usize)>(
        &self,
        arrays: &[ArrayRef],
        mut emit: E,
    ) -> Result<()> {
        let rows = self.converter.convert_columns(arrays)?;
        if let Some(sp_rows) = &self.split_point_rows {
            for (row_idx, row) in rows.iter().enumerate() {
                let partition = partition_point_rows(sp_rows, &row);
                emit(row_idx, partition);
            }
        } else {
            for row_idx in 0..rows.num_rows() {
                emit(row_idx, 0);
            }
        }
        Ok(())
    }
}

#[inline]
fn partition_point_rows(sp_rows: &Rows, row: &Row<'_>) -> usize {
    let mut left = 0;
    let mut right = sp_rows.num_rows();
    while left < right {
        let mid = left + (right - left) / 2;
        if sp_rows.row(mid) <= *row {
            left = mid + 1;
        } else {
            right = mid;
        }
    }
    left
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    fn make_split_points_1d(scalars: Vec<ScalarValue>) -> Vec<SplitPoint> {
        scalars
            .into_iter()
            .map(|s| SplitPoint::new(vec![s]))
            .collect()
    }

    fn assert_routing(
        router: &RangeRouter,
        arrays: &[ArrayRef],
        expected_partition_ids: &[u64],
        expected_indices: Option<&[Vec<u32>]>,
    ) -> Result<()> {
        let mut partition_ids = Vec::new();
        router.route_partition_ids(arrays, &mut partition_ids)?;
        assert_eq!(partition_ids, expected_partition_ids);

        if let Some(expected) = expected_indices {
            let mut indices = vec![vec![]; expected.len()];
            router.route_indices(arrays, &mut indices)?;
            assert_eq!(indices, expected);
        }

        Ok(())
    }

    #[test]
    fn test_primitive_router_i64_asc() -> Result<()> {
        let split_points = make_split_points_1d(vec![
            ScalarValue::Int64(Some(10)),
            ScalarValue::Int64(Some(20)),
            ScalarValue::Int64(Some(30)),
        ]);
        let sort_options = vec![SortOptions {
            descending: false,
            nulls_first: true,
        }];

        let router = RangeRouter::try_new(&sort_options, &split_points)?;
        assert!(matches!(router.inner, RangeRouterInner::Primitive(_)));

        let input = Arc::new(Int64Array::from(vec![
            Some(5),
            Some(10),
            Some(15),
            Some(20),
            Some(25),
            Some(30),
            Some(35),
            None,
        ])) as ArrayRef;

        assert_routing(
            &router,
            &[input],
            &[0, 1, 1, 2, 2, 3, 3, 0],
            Some(&[vec![0, 7], vec![1, 2], vec![3, 4], vec![5, 6]]),
        )
    }

    #[test]
    fn test_primitive_router_i64_desc() -> Result<()> {
        let split_points = make_split_points_1d(vec![
            ScalarValue::Int64(Some(30)),
            ScalarValue::Int64(Some(20)),
            ScalarValue::Int64(Some(10)),
        ]);
        let sort_options = vec![SortOptions {
            descending: true,
            nulls_first: false,
        }];

        let router = RangeRouter::try_new(&sort_options, &split_points)?;
        assert!(matches!(router.inner, RangeRouterInner::Primitive(_)));

        let input = Arc::new(Int64Array::from(vec![
            Some(35),
            Some(30),
            Some(25),
            Some(20),
            Some(15),
            Some(10),
            Some(5),
            None,
        ])) as ArrayRef;

        assert_routing(
            &router,
            &[input],
            &[0, 1, 1, 2, 2, 3, 3, 3],
            Some(&[vec![0], vec![1, 2], vec![3, 4], vec![5, 6, 7]]),
        )
    }

    #[test]
    fn test_float_router() -> Result<()> {
        let split_points = make_split_points_1d(vec![
            ScalarValue::Float64(Some(0.0)),
            ScalarValue::Float64(Some(100.0)),
        ]);
        let sort_options = vec![SortOptions {
            descending: false,
            nulls_first: false,
        }];

        let router = RangeRouter::try_new(&sort_options, &split_points)?;
        assert!(matches!(router.inner, RangeRouterInner::Primitive(_)));

        let input = Arc::new(Float64Array::from(vec![
            Some(-10.0),
            Some(0.0),
            Some(50.0),
            Some(100.0),
            Some(200.0),
            None,
        ])) as ArrayRef;

        assert_routing(
            &router,
            &[input],
            &[0, 1, 1, 2, 2, 2],
            Some(&[vec![0], vec![1, 2], vec![3, 4, 5]]),
        )
    }

    #[test]
    fn test_row_converter_strings() -> Result<()> {
        let split_points = make_split_points_1d(vec![
            ScalarValue::Utf8(Some("d".to_string())),
            ScalarValue::Utf8(Some("m".to_string())),
            ScalarValue::Utf8(Some("s".to_string())),
        ]);
        let sort_options = vec![SortOptions {
            descending: false,
            nulls_first: true,
        }];

        let router = RangeRouter::try_new(&sort_options, &split_points)?;
        assert!(matches!(router.inner, RangeRouterInner::Row(_)));

        let input = Arc::new(StringArray::from(vec![
            Some("apple"),
            Some("d"),
            Some("frog"),
            Some("m"),
            Some("orange"),
            Some("s"),
            Some("zebra"),
            None,
        ])) as ArrayRef;

        assert_routing(
            &router,
            &[input],
            &[0, 1, 1, 2, 2, 3, 3, 0],
            Some(&[vec![0, 7], vec![1, 2], vec![3, 4], vec![5, 6]]),
        )
    }

    #[test]
    fn test_row_converter_composite_keys() -> Result<()> {
        let split_points = vec![
            SplitPoint::new(vec![
                ScalarValue::Int64(Some(1)),
                ScalarValue::Utf8(Some("b".to_string())),
            ]),
            SplitPoint::new(vec![
                ScalarValue::Int64(Some(1)),
                ScalarValue::Utf8(Some("d".to_string())),
            ]),
            SplitPoint::new(vec![
                ScalarValue::Int64(Some(2)),
                ScalarValue::Utf8(Some("a".to_string())),
            ]),
        ];
        let sort_options = vec![
            SortOptions {
                descending: false,
                nulls_first: false,
            },
            SortOptions {
                descending: false,
                nulls_first: false,
            },
        ];

        let router = RangeRouter::try_new(&sort_options, &split_points)?;
        assert!(matches!(router.inner, RangeRouterInner::Row(_)));

        let col1 = Arc::new(Int64Array::from(vec![1, 1, 1, 1, 2, 2, 3])) as ArrayRef;
        let col2 = Arc::new(StringArray::from(vec!["a", "b", "c", "d", "a", "z", "a"]))
            as ArrayRef;

        assert_routing(
            &router,
            &[col1, col2],
            &[0, 1, 1, 2, 3, 3, 3],
            Some(&[vec![0], vec![1, 2], vec![3], vec![4, 5, 6]]),
        )
    }

    #[test]
    fn test_router_empty_split_points() -> Result<()> {
        let split_points = vec![];
        let sort_options = vec![SortOptions::default()];

        let router = RangeRouter::try_new(&sort_options, &split_points)?;
        assert_eq!(router.num_split_points(), 0);

        let input = Arc::new(Int64Array::from(vec![10, 20, 30])) as ArrayRef;
        assert_routing(&router, &[input], &[0, 0, 0], Some(&[vec![0, 1, 2]]))
    }

    #[test]
    fn test_router_decimal_precision_widening() -> Result<()> {
        // Split point defined with precision 10, scale 2
        let split_points = vec![SplitPoint::new(vec![ScalarValue::Decimal128(
            Some(1000),
            10,
            2,
        )])];
        let sort_options = vec![SortOptions::default()];
        let target_data_types = vec![DataType::Decimal128(20, 2)];

        let router = RangeRouter::try_new_with_data_types(
            &sort_options,
            &split_points,
            &target_data_types,
        )?;
        assert_eq!(router.data_types(), &target_data_types);

        // Column array is Decimal128(20, 2)
        let array = Arc::new(
            Decimal128Array::from(vec![Some(500i128), Some(1000i128), Some(2000i128)])
                .with_precision_and_scale(20, 2)?,
        ) as ArrayRef;

        assert_routing(&router, &[array], &[0, 1, 1], Some(&[vec![0], vec![1, 2]]))
    }

    #[test]
    fn test_router_timestamp_timezone_coercion() -> Result<()> {
        // Split point defined without timezone
        let split_points = vec![SplitPoint::new(vec![
            ScalarValue::TimestampNanosecond(Some(100), None),
            ScalarValue::Int64(Some(0)),
        ])];
        let sort_options = vec![SortOptions::default(), SortOptions::default()];
        let target_data_types = vec![
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
            DataType::Int64,
        ];

        let router = RangeRouter::try_new_with_data_types(
            &sort_options,
            &split_points,
            &target_data_types,
        )?;
        assert_eq!(router.data_types(), &target_data_types);

        let col1 = Arc::new(
            TimestampNanosecondArray::from(vec![Some(50i64), Some(200i64)])
                .with_timezone("UTC"),
        ) as ArrayRef;
        let col2 = Arc::new(Int64Array::from(vec![Some(1i64), Some(2i64)])) as ArrayRef;

        assert_routing(&router, &[col1, col2], &[0, 1], Some(&[vec![0], vec![1]]))
    }

    #[test]
    fn test_router_type_mismatch_error() -> Result<()> {
        let split_points = make_split_points_1d(vec![ScalarValue::Int64(Some(10))]);
        let sort_options = vec![SortOptions::default()];

        let router = RangeRouter::try_new(&sort_options, &split_points)?;

        // Pass Float64Array instead of Int64Array
        let invalid_input = Arc::new(Float64Array::from(vec![5.0, 15.0])) as ArrayRef;
        let err = router.route_with(&[invalid_input], |_, _| {}).unwrap_err();
        assert!(
            err.to_string()
                .contains("Range partitioning expected column 0 to be of type Int64")
        );

        // Pass wrong column count
        let err = router.route_with(&[], |_, _| {}).unwrap_err();
        assert!(
            err.to_string()
                .contains("Range partitioning expected 1 columns, but got 0")
        );

        Ok(())
    }

    #[test]
    fn test_router_width_mismatch_error() {
        let split_points = vec![SplitPoint::new(vec![ScalarValue::Int64(Some(10))])];
        // 2 sort options but split point only has 1 column
        let sort_options = vec![SortOptions::default(), SortOptions::default()];

        let err = RangeRouter::try_new(&sort_options, &split_points).unwrap_err();
        assert!(err.to_string().contains(
            "Range partitioning split point 0 has width 1, but ordering has width 2"
        ));
    }
}
