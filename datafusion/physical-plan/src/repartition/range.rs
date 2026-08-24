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
use arrow::row::{OwnedRow, RowConverter, SortField};
use datafusion_common::{DataFusionError, Result, ScalarValue, not_impl_err};
use datafusion_physical_expr::SplitPoint;

/// An router for assigning rows to range partitions.
#[derive(Debug, Clone)]
pub(crate) struct RangeRouter {
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
    /// Constructs the best router for the given key types, split points, and sort options.
    pub(crate) fn try_new(
        data_types: &[DataType],
        sort_options: &[SortOptions],
        split_points: &[SplitPoint],
    ) -> Result<Self> {
        // Try single-column primitive fast path
        if data_types.len() == 1
            && !sort_options.is_empty()
            && let Some(primitive_router) =
                PrimitiveRangeRouter::try_new(split_points, sort_options[0])
        {
            return Ok(Self {
                inner: RangeRouterInner::Primitive(primitive_router),
            });
        }

        // Try RowConverter path
        let row_router =
            RowConverterRangeRouter::try_new(data_types, sort_options, split_points)?;
        Ok(Self {
            inner: RangeRouterInner::Row(row_router),
        })
    }

    /// Number of split points configured in this router.
    pub(crate) fn num_split_points(&self) -> usize {
        match &self.inner {
            RangeRouterInner::Primitive(r) => r.num_split_points(),
            RangeRouterInner::Row(r) => r.num_split_points(),
        }
    }

    /// Groups row indices from `arrays` into partition index buckets.
    pub(crate) fn route_indices(
        &self,
        arrays: &[ArrayRef],
        indices: &mut [Vec<u32>],
    ) -> Result<()> {
        match &self.inner {
            RangeRouterInner::Primitive(r) => {
                if let Some(first_col) = arrays.first() {
                    r.route_indices(first_col.as_ref(), indices)
                } else {
                    Ok(())
                }
            }
            RangeRouterInner::Row(r) => r.route_indices(arrays, indices),
        }
    }

    /// Appends output partition IDs to `partition_ids`.
    pub(crate) fn route_partition_ids(
        &self,
        arrays: &[ArrayRef],
        partition_ids: &mut Vec<u64>,
    ) -> Result<()> {
        match &self.inner {
            RangeRouterInner::Primitive(r) => {
                if let Some(first_col) = arrays.first() {
                    r.route_partition_ids(first_col.as_ref(), partition_ids)
                } else {
                    Ok(())
                }
            }
            RangeRouterInner::Row(r) => r.route_partition_ids(arrays, partition_ids),
        }
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

            fn num_split_points(&self) -> usize {
                match self {
                    $( Self::$variant(r) => r.num_split_points(), )*
                    Self::Float32(r) => r.num_split_points(),
                    Self::Float64(r) => r.num_split_points(),
                }
            }

            fn route_indices(&self, array: &dyn Array, indices: &mut [Vec<u32>]) -> Result<()> {
                match self {
                    $(
                        Self::$variant(r) => {
                            let arr = array.as_any().downcast_ref::<$array>().ok_or_else(|| {
                                DataFusionError::Internal(format!("Expected {}", stringify!($array)))
                            })?;
                            r.route_indices(arr, indices);
                            Ok(())
                        }
                    )*
                    Self::Float32(r) => {
                        let arr = array.as_any().downcast_ref::<Float32Array>().ok_or_else(|| {
                            DataFusionError::Internal("Expected Float32Array".to_string())
                        })?;
                        r.route_indices(arr, indices);
                        Ok(())
                    }
                    Self::Float64(r) => {
                        let arr = array.as_any().downcast_ref::<Float64Array>().ok_or_else(|| {
                            DataFusionError::Internal("Expected Float64Array".to_string())
                        })?;
                        r.route_indices(arr, indices);
                        Ok(())
                    }
                }
            }

            fn route_partition_ids(&self, array: &dyn Array, partition_ids: &mut Vec<u64>) -> Result<()> {
                match self {
                    $(
                        Self::$variant(r) => {
                            let arr = array.as_any().downcast_ref::<$array>().ok_or_else(|| {
                                DataFusionError::Internal(format!("Expected {}", stringify!($array)))
                            })?;
                            r.route_partition_ids(arr, partition_ids);
                            Ok(())
                        }
                    )*
                    Self::Float32(r) => {
                        let arr = array.as_any().downcast_ref::<Float32Array>().ok_or_else(|| {
                            DataFusionError::Internal("Expected Float32Array".to_string())
                        })?;
                        r.route_partition_ids(arr, partition_ids);
                        Ok(())
                    }
                    Self::Float64(r) => {
                        let arr = array.as_any().downcast_ref::<Float64Array>().ok_or_else(|| {
                            DataFusionError::Internal("Expected Float64Array".to_string())
                        })?;
                        r.route_partition_ids(arr, partition_ids);
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

    fn num_split_points(&self) -> usize {
        self.split_points.len()
    }

    fn route_indices<A: ArrowPrimitiveType<Native = T>>(
        &self,
        array: &PrimitiveArray<A>,
        indices: &mut [Vec<u32>],
    ) {
        let split_points = &self.split_points;
        let descending = self.sort_options.descending;
        let nulls_first = self.sort_options.nulls_first;

        if array.null_count() == 0 {
            let values = array.values().as_ref();
            if !descending {
                for (idx, &val) in values.iter().enumerate() {
                    let p = split_points.partition_point(|&sp| sp <= val);
                    indices[p].push(idx as u32);
                }
            } else {
                for (idx, &val) in values.iter().enumerate() {
                    let p = split_points.partition_point(|&sp| sp >= val);
                    indices[p].push(idx as u32);
                }
            }
        } else {
            let null_partition = if nulls_first { 0 } else { split_points.len() };
            for idx in 0..array.len() {
                if array.is_null(idx) {
                    indices[null_partition].push(idx as u32);
                } else {
                    let val = array.value(idx);
                    let p = if !descending {
                        split_points.partition_point(|&sp| sp <= val)
                    } else {
                        split_points.partition_point(|&sp| sp >= val)
                    };
                    indices[p].push(idx as u32);
                }
            }
        }
    }

    fn route_partition_ids<A: ArrowPrimitiveType<Native = T>>(
        &self,
        array: &PrimitiveArray<A>,
        partition_ids: &mut Vec<u64>,
    ) {
        let split_points = &self.split_points;
        let descending = self.sort_options.descending;
        let nulls_first = self.sort_options.nulls_first;

        if array.null_count() == 0 {
            let values = array.values().as_ref();
            if !descending {
                partition_ids.extend(
                    values
                        .iter()
                        .map(|&val| split_points.partition_point(|&sp| sp <= val) as u64),
                );
            } else {
                partition_ids.extend(
                    values
                        .iter()
                        .map(|&val| split_points.partition_point(|&sp| sp >= val) as u64),
                );
            }
        } else {
            let null_partition =
                (if nulls_first { 0 } else { split_points.len() }) as u64;
            partition_ids.extend((0..array.len()).map(|idx| {
                if array.is_null(idx) {
                    null_partition
                } else {
                    let val = array.value(idx);
                    let p = if !descending {
                        split_points.partition_point(|&sp| sp <= val)
                    } else {
                        split_points.partition_point(|&sp| sp >= val)
                    };
                    p as u64
                }
            }));
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

    fn num_split_points(&self) -> usize {
        self.split_points.len()
    }
}

macro_rules! impl_float_values_router {
    ($t:ty, $arr:ty) => {
        impl FloatValuesRouter<$t> {
            fn route_indices(&self, array: &$arr, indices: &mut [Vec<u32>]) {
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
                            indices[p].push(idx as u32);
                        }
                    } else {
                        for (idx, &val) in values.iter().enumerate() {
                            let p = split_points.partition_point(|&sp| {
                                sp.total_cmp(&val) != Ordering::Less
                            });
                            indices[p].push(idx as u32);
                        }
                    }
                } else {
                    let null_partition = if nulls_first { 0 } else { split_points.len() };
                    for idx in 0..array.len() {
                        if array.is_null(idx) {
                            indices[null_partition].push(idx as u32);
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
                            indices[p].push(idx as u32);
                        }
                    }
                }
            }

            fn route_partition_ids(&self, array: &$arr, partition_ids: &mut Vec<u64>) {
                let split_points = &self.split_points;
                let descending = self.sort_options.descending;
                let nulls_first = self.sort_options.nulls_first;

                if array.null_count() == 0 {
                    let values = array.values().as_ref();
                    if !descending {
                        partition_ids.extend(values.iter().map(|&val| {
                            split_points.partition_point(|&sp| {
                                sp.total_cmp(&val) != Ordering::Greater
                            }) as u64
                        }));
                    } else {
                        partition_ids.extend(values.iter().map(|&val| {
                            split_points.partition_point(|&sp| {
                                sp.total_cmp(&val) != Ordering::Less
                            }) as u64
                        }));
                    }
                } else {
                    let null_partition =
                        (if nulls_first { 0 } else { split_points.len() }) as u64;
                    partition_ids.extend((0..array.len()).map(|idx| {
                        if array.is_null(idx) {
                            null_partition
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
                            p as u64
                        }
                    }));
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
    split_point_rows: Vec<OwnedRow>,
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
            vec![]
        } else {
            let split_point_arrays = (0..num_cols)
                .map(|col_idx| {
                    let col_scalars =
                        split_points.iter().map(|sp| sp.values()[col_idx].clone());
                    ScalarValue::iter_to_array(col_scalars)
                })
                .collect::<Result<Vec<_>>>()?;

            row_converter
                .convert_columns(&split_point_arrays)?
                .iter()
                .map(|r| r.owned())
                .collect()
        };

        Ok(Self {
            converter: Arc::new(row_converter),
            split_point_rows,
        })
    }

    fn num_split_points(&self) -> usize {
        self.split_point_rows.len()
    }

    fn route_indices(&self, arrays: &[ArrayRef], indices: &mut [Vec<u32>]) -> Result<()> {
        let rows = self.converter.convert_columns(arrays)?;
        let sp_rows = &self.split_point_rows;

        for (row_idx, row) in rows.iter().enumerate() {
            let partition = sp_rows.partition_point(|sp| sp.as_ref() <= row.as_ref());
            indices[partition].push(row_idx as u32);
        }
        Ok(())
    }

    fn route_partition_ids(
        &self,
        arrays: &[ArrayRef],
        partition_ids: &mut Vec<u64>,
    ) -> Result<()> {
        let rows = self.converter.convert_columns(arrays)?;
        let sp_rows = &self.split_point_rows;

        partition_ids.extend(
            rows.iter().map(|row| {
                sp_rows.partition_point(|sp| sp.as_ref() <= row.as_ref()) as u64
            }),
        );
        Ok(())
    }
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
        let data_types = vec![DataType::Int64];

        let router = RangeRouter::try_new(&data_types, &sort_options, &split_points)?;
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
        let data_types = vec![DataType::Int64];

        let router = RangeRouter::try_new(&data_types, &sort_options, &split_points)?;
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
        let data_types = vec![DataType::Float64];

        let router = RangeRouter::try_new(&data_types, &sort_options, &split_points)?;
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
        let data_types = vec![DataType::Utf8];

        let router = RangeRouter::try_new(&data_types, &sort_options, &split_points)?;
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
        let data_types = vec![DataType::Int64, DataType::Utf8];

        let router = RangeRouter::try_new(&data_types, &sort_options, &split_points)?;
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
        let data_types = vec![DataType::Int64];

        let router = RangeRouter::try_new(&data_types, &sort_options, &split_points)?;
        assert_eq!(router.num_split_points(), 0);

        let input = Arc::new(Int64Array::from(vec![10, 20, 30])) as ArrayRef;
        assert_routing(&router, &[input], &[0, 0, 0], Some(&[vec![0, 1, 2]]))
    }
}
