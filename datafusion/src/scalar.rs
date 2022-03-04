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

//! ScalarValue reimported from datafusion-common

pub use datafusion_common::{
    ScalarValue, MAX_PRECISION_FOR_DECIMAL128, MAX_SCALE_FOR_DECIMAL128,
};

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::types::days_ms;
    use arrow::{array::*, datatypes::*};
    use datafusion_common::field_util::struct_array_from;
    use std::cmp::Ordering;
    use std::sync::Arc;

    type StringArray = Utf8Array<i32>;
    type LargeStringArray = Utf8Array<i64>;
    type SmallBinaryArray = BinaryArray<i32>;
    type LargeBinaryArray = BinaryArray<i64>;

    #[test]
    fn scalar_decimal_test() {
        let decimal_value = ScalarValue::Decimal128(Some(123), 10, 1);
        assert_eq!(DataType::Decimal(10, 1), decimal_value.get_datatype());
        let try_into_value: i128 = decimal_value.clone().try_into().unwrap();
        assert_eq!(123_i128, try_into_value);
        assert!(!decimal_value.is_null());
        let neg_decimal_value = decimal_value.arithmetic_negate();
        match neg_decimal_value {
            ScalarValue::Decimal128(v, _, _) => {
                assert_eq!(-123, v.unwrap());
            }
            _ => {
                unreachable!();
            }
        }

        // decimal scalar to array
        let array = decimal_value.to_array();
        let array = array.as_any().downcast_ref::<Int128Array>().unwrap();
        assert_eq!(1, array.len());
        assert_eq!(DataType::Decimal(10, 1), array.data_type().clone());
        assert_eq!(123i128, array.value(0));

        // decimal scalar to array with size
        let array = decimal_value.to_array_of_size(10);
        let array_decimal = array.as_any().downcast_ref::<Int128Array>().unwrap();
        assert_eq!(10, array.len());
        assert_eq!(DataType::Decimal(10, 1), array.data_type().clone());
        assert_eq!(123i128, array_decimal.value(0));
        assert_eq!(123i128, array_decimal.value(9));
        // test eq array
        assert!(decimal_value.eq_array(&array, 1));
        assert!(decimal_value.eq_array(&array, 5));
        // test try from array
        assert_eq!(
            decimal_value,
            ScalarValue::try_from_array(&array, 5).unwrap()
        );

        assert_eq!(
            decimal_value,
            ScalarValue::try_new_decimal128(123, 10, 1).unwrap()
        );

        // test compare
        let left = ScalarValue::Decimal128(Some(123), 10, 2);
        let right = ScalarValue::Decimal128(Some(124), 10, 2);
        assert!(!left.eq(&right));
        let result = left < right;
        assert!(result);
        let result = left <= right;
        assert!(result);
        let right = ScalarValue::Decimal128(Some(124), 10, 3);
        // make sure that two decimals with diff datatype can't be compared.
        let result = left.partial_cmp(&right);
        assert_eq!(None, result);

        let decimal_vec = vec![
            ScalarValue::Decimal128(Some(1), 10, 2),
            ScalarValue::Decimal128(Some(2), 10, 2),
            ScalarValue::Decimal128(Some(3), 10, 2),
        ];
        // convert the vec to decimal array and check the result
        let array = ScalarValue::iter_to_array(decimal_vec.into_iter()).unwrap();
        assert_eq!(3, array.len());
        assert_eq!(DataType::Decimal(10, 2), array.data_type().clone());

        let decimal_vec = vec![
            ScalarValue::Decimal128(Some(1), 10, 2),
            ScalarValue::Decimal128(Some(2), 10, 2),
            ScalarValue::Decimal128(Some(3), 10, 2),
            ScalarValue::Decimal128(None, 10, 2),
        ];
        let array: ArrayRef =
            ScalarValue::iter_to_array(decimal_vec.into_iter()).unwrap();
        assert_eq!(4, array.len());
        assert_eq!(DataType::Decimal(10, 2), array.data_type().clone());

        assert!(ScalarValue::try_new_decimal128(1, 10, 2)
            .unwrap()
            .eq_array(&array, 0));
        assert!(ScalarValue::try_new_decimal128(2, 10, 2)
            .unwrap()
            .eq_array(&array, 1));
        assert!(ScalarValue::try_new_decimal128(3, 10, 2)
            .unwrap()
            .eq_array(&array, 2));
        assert_eq!(
            ScalarValue::Decimal128(None, 10, 2),
            ScalarValue::try_from_array(&array, 3).unwrap()
        );
        assert_eq!(
            ScalarValue::Decimal128(None, 10, 2),
            ScalarValue::try_from_array(&array, 4).unwrap()
        );
    }

    #[test]
    fn scalar_value_to_array_u64() {
        let value = ScalarValue::UInt64(Some(13u64));
        let array = value.to_array();
        let array = array.as_any().downcast_ref::<UInt64Array>().unwrap();
        assert_eq!(array.len(), 1);
        assert!(!array.is_null(0));
        assert_eq!(array.value(0), 13);

        let value = ScalarValue::UInt64(None);
        let array = value.to_array();
        let array = array.as_any().downcast_ref::<UInt64Array>().unwrap();
        assert_eq!(array.len(), 1);
        assert!(array.is_null(0));
    }

    #[test]
    fn scalar_value_to_array_u32() {
        let value = ScalarValue::UInt32(Some(13u32));
        let array = value.to_array();
        let array = array.as_any().downcast_ref::<UInt32Array>().unwrap();
        assert_eq!(array.len(), 1);
        assert!(!array.is_null(0));
        assert_eq!(array.value(0), 13);

        let value = ScalarValue::UInt32(None);
        let array = value.to_array();
        let array = array.as_any().downcast_ref::<UInt32Array>().unwrap();
        assert_eq!(array.len(), 1);
        assert!(array.is_null(0));
    }

    #[test]
    fn scalar_list_null_to_array() {
        let list_array_ref =
            ScalarValue::List(None, Box::new(DataType::UInt64)).to_array();
        let list_array = list_array_ref
            .as_any()
            .downcast_ref::<ListArray<i32>>()
            .unwrap();

        assert!(list_array.is_null(0));
        assert_eq!(list_array.len(), 1);
        assert_eq!(list_array.values().len(), 0);
    }

    #[test]
    fn scalar_list_to_array() {
        let list_array_ref = ScalarValue::List(
            Some(Box::new(vec![
                ScalarValue::UInt64(Some(100)),
                ScalarValue::UInt64(None),
                ScalarValue::UInt64(Some(101)),
            ])),
            Box::new(DataType::UInt64),
        )
        .to_array();

        let list_array = list_array_ref
            .as_any()
            .downcast_ref::<ListArray<i32>>()
            .unwrap();
        assert_eq!(list_array.len(), 1);
        assert_eq!(list_array.values().len(), 3);

        let prim_array_ref = list_array.value(0);
        let prim_array = prim_array_ref
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap();
        assert_eq!(prim_array.len(), 3);
        assert_eq!(prim_array.value(0), 100);
        assert!(prim_array.is_null(1));
        assert_eq!(prim_array.value(2), 101);
    }

    /// Creates array directly and via ScalarValue and ensures they are the same
    macro_rules! check_scalar_iter {
        ($SCALAR_T:ident, $ARRAYTYPE:ident, $INPUT:expr) => {{
            let scalars: Vec<_> =
                $INPUT.iter().map(|v| ScalarValue::$SCALAR_T(*v)).collect();

            let array = ScalarValue::iter_to_array(scalars.into_iter()).unwrap();

            let expected = $ARRAYTYPE::from($INPUT).as_arc();

            assert_eq!(&array, &expected);
        }};
    }

    /// Creates array directly and via ScalarValue and ensures they are the same
    /// but for variants that carry a timezone field.
    macro_rules! check_scalar_iter_tz {
        ($SCALAR_T:ident, $INPUT:expr) => {{
            let scalars: Vec<_> = $INPUT
                .iter()
                .map(|v| ScalarValue::$SCALAR_T(*v, None))
                .collect();

            let array = ScalarValue::iter_to_array(scalars.into_iter()).unwrap();

            let expected: Arc<dyn Array> = Arc::new(Int64Array::from($INPUT));

            assert_eq!(&array, &expected);
        }};
    }

    /// Creates array directly and via ScalarValue and ensures they
    /// are the same, for string  arrays
    macro_rules! check_scalar_iter_string {
        ($SCALAR_T:ident, $ARRAYTYPE:ident, $INPUT:expr) => {{
            let scalars: Vec<_> = $INPUT
                .iter()
                .map(|v| ScalarValue::$SCALAR_T(v.map(|v| v.to_string())))
                .collect();

            let array = ScalarValue::iter_to_array(scalars.into_iter()).unwrap();

            let expected: Arc<dyn Array> = Arc::new($ARRAYTYPE::from($INPUT));

            assert_eq!(&array, &expected);
        }};
    }

    /// Creates array directly and via ScalarValue and ensures they
    /// are the same, for binary arrays
    macro_rules! check_scalar_iter_binary {
        ($SCALAR_T:ident, $ARRAYTYPE:ident, $INPUT:expr) => {{
            let scalars: Vec<_> = $INPUT
                .iter()
                .map(|v| ScalarValue::$SCALAR_T(v.map(|v| v.to_vec())))
                .collect();

            let array = ScalarValue::iter_to_array(scalars.into_iter()).unwrap();

            let expected: $ARRAYTYPE =
                $INPUT.iter().map(|v| v.map(|v| v.to_vec())).collect();

            let expected: Arc<dyn Array> = Arc::new(expected);

            assert_eq!(&array, &expected);
        }};
    }

    #[test]
    fn scalar_iter_to_array_boolean() {
        check_scalar_iter!(
            Boolean,
            MutableBooleanArray,
            vec![Some(true), None, Some(false)]
        );
        check_scalar_iter!(Float32, Float32Vec, vec![Some(1.9), None, Some(-2.1)]);
        check_scalar_iter!(Float64, Float64Vec, vec![Some(1.9), None, Some(-2.1)]);

        check_scalar_iter!(Int8, Int8Vec, vec![Some(1), None, Some(3)]);
        check_scalar_iter!(Int16, Int16Vec, vec![Some(1), None, Some(3)]);
        check_scalar_iter!(Int32, Int32Vec, vec![Some(1), None, Some(3)]);
        check_scalar_iter!(Int64, Int64Vec, vec![Some(1), None, Some(3)]);

        check_scalar_iter!(UInt8, UInt8Vec, vec![Some(1), None, Some(3)]);
        check_scalar_iter!(UInt16, UInt16Vec, vec![Some(1), None, Some(3)]);
        check_scalar_iter!(UInt32, UInt32Vec, vec![Some(1), None, Some(3)]);
        check_scalar_iter!(UInt64, UInt64Vec, vec![Some(1), None, Some(3)]);

        check_scalar_iter_tz!(TimestampSecond, vec![Some(1), None, Some(3)]);
        check_scalar_iter_tz!(TimestampMillisecond, vec![Some(1), None, Some(3)]);
        check_scalar_iter_tz!(TimestampMicrosecond, vec![Some(1), None, Some(3)]);
        check_scalar_iter_tz!(TimestampNanosecond, vec![Some(1), None, Some(3)]);

        check_scalar_iter_string!(
            Utf8,
            StringArray,
            vec![Some("foo"), None, Some("bar")]
        );
        check_scalar_iter_string!(
            LargeUtf8,
            LargeStringArray,
            vec![Some("foo"), None, Some("bar")]
        );
        check_scalar_iter_binary!(
            Binary,
            SmallBinaryArray,
            vec![Some(b"foo"), None, Some(b"bar")]
        );
        check_scalar_iter_binary!(
            LargeBinary,
            LargeBinaryArray,
            vec![Some(b"foo"), None, Some(b"bar")]
        );
    }

    #[test]
    fn scalar_iter_to_array_empty() {
        let scalars = vec![] as Vec<ScalarValue>;

        let result = ScalarValue::iter_to_array(scalars.into_iter()).unwrap_err();
        assert!(
            result
                .to_string()
                .contains("Empty iterator passed to ScalarValue::iter_to_array"),
            "{}",
            result
        );
    }

    #[test]
    fn scalar_iter_to_array_mismatched_types() {
        use ScalarValue::*;
        // If the scalar values are not all the correct type, error here
        let scalars: Vec<ScalarValue> = vec![Boolean(Some(true)), Int32(Some(5))];

        let result = ScalarValue::iter_to_array(scalars.into_iter()).unwrap_err();
        assert!(result.to_string().contains("Inconsistent types in ScalarValue::iter_to_array. Expected Boolean, got Int32(5)"),
                "{}", result);
    }

    #[test]
    fn scalar_try_from_array_null() {
        let array = vec![Some(33), None].into_iter().collect::<Int64Array>();
        let array: ArrayRef = Arc::new(array);

        assert_eq!(
            ScalarValue::Int64(Some(33)),
            ScalarValue::try_from_array(&array, 0).unwrap()
        );
        assert_eq!(
            ScalarValue::Int64(None),
            ScalarValue::try_from_array(&array, 1).unwrap()
        );
    }

    #[test]
    fn scalar_try_from_dict_datatype() {
        let data_type =
            DataType::Dictionary(IntegerType::Int8, Box::new(DataType::Utf8), false);
        let data_type = &data_type;
        assert_eq!(ScalarValue::Utf8(None), data_type.try_into().unwrap())
    }

    #[test]
    fn size_of_scalar() {
        // Since ScalarValues are used in a non trivial number of places,
        // making it larger means significant more memory consumption
        // per distinct value.
        #[cfg(target_arch = "aarch64")]
        assert_eq!(std::mem::size_of::<ScalarValue>(), 64);

        #[cfg(target_arch = "amd64")]
        assert_eq!(std::mem::size_of::<ScalarValue>(), 48);
    }

    #[test]
    fn scalar_eq_array() {
        // Validate that eq_array has the same semantics as ScalarValue::eq
        macro_rules! make_typed_vec {
            ($INPUT:expr, $TYPE:ident) => {{
                $INPUT
                    .iter()
                    .map(|v| v.map(|v| v as $TYPE))
                    .collect::<Vec<_>>()
            }};
        }

        let bool_vals = vec![Some(true), None, Some(false)];
        let f32_vals = vec![Some(-1.0), None, Some(1.0)];
        let f64_vals = make_typed_vec!(f32_vals, f64);

        let i8_vals = vec![Some(-1), None, Some(1)];
        let i16_vals = make_typed_vec!(i8_vals, i16);
        let i32_vals = make_typed_vec!(i8_vals, i32);
        let i64_vals = make_typed_vec!(i8_vals, i64);
        let days_ms_vals = &[Some(days_ms::new(1, 2)), None, Some(days_ms::new(10, 0))];

        let u8_vals = vec![Some(0), None, Some(1)];
        let u16_vals = make_typed_vec!(u8_vals, u16);
        let u32_vals = make_typed_vec!(u8_vals, u32);
        let u64_vals = make_typed_vec!(u8_vals, u64);

        let str_vals = &[Some("foo"), None, Some("bar")];

        /// Test each value in `scalar` with the corresponding element
        /// at `array`. Assumes each element is unique (aka not equal
        /// with all other indexes)
        struct TestCase {
            array: ArrayRef,
            scalars: Vec<ScalarValue>,
        }

        /// Create a test case for casing the input to the specified array type
        macro_rules! make_test_case {
            ($INPUT:expr, $ARRAY_TY:ident, $SCALAR_TY:ident) => {{
                TestCase {
                    array: Arc::new($INPUT.iter().collect::<$ARRAY_TY>()),
                    scalars: $INPUT.iter().map(|v| ScalarValue::$SCALAR_TY(*v)).collect(),
                }
            }};

            ($INPUT:expr, $ARRAY_TY:ident, $SCALAR_TY:ident, $TZ:expr) => {{
                let tz = $TZ;
                TestCase {
                    array: Arc::new($INPUT.iter().collect::<$ARRAY_TY>()),
                    scalars: $INPUT
                        .iter()
                        .map(|v| ScalarValue::$SCALAR_TY(*v, tz.clone()))
                        .collect(),
                }
            }};
        }

        macro_rules! make_date_test_case {
            ($INPUT:expr, $ARRAY_TY:ident, $SCALAR_TY:ident) => {{
                TestCase {
                    array: Arc::new($ARRAY_TY::from($INPUT).to(DataType::$SCALAR_TY)),
                    scalars: $INPUT.iter().map(|v| ScalarValue::$SCALAR_TY(*v)).collect(),
                }
            }};
        }

        macro_rules! make_ts_test_case {
            ($INPUT:expr, $ARROW_TU:ident, $SCALAR_TY:ident, $TZ:expr) => {{
                TestCase {
                    array: Arc::new(
                        Int64Array::from($INPUT)
                            .to(DataType::Timestamp(TimeUnit::$ARROW_TU, $TZ)),
                    ),
                    scalars: $INPUT
                        .iter()
                        .map(|v| ScalarValue::$SCALAR_TY(*v, $TZ))
                        .collect(),
                }
            }};
        }

        macro_rules! make_temporal_test_case {
            ($INPUT:expr, $ARRAY_TY:ident, $ARROW_TU:ident, $SCALAR_TY:ident) => {{
                TestCase {
                    array: Arc::new(
                        $ARRAY_TY::from($INPUT)
                            .to(DataType::Interval(IntervalUnit::$ARROW_TU)),
                    ),
                    scalars: $INPUT.iter().map(|v| ScalarValue::$SCALAR_TY(*v)).collect(),
                }
            }};
        }

        macro_rules! make_str_test_case {
            ($INPUT:expr, $ARRAY_TY:ident, $SCALAR_TY:ident) => {{
                TestCase {
                    array: Arc::new($INPUT.iter().cloned().collect::<$ARRAY_TY>()),
                    scalars: $INPUT
                        .iter()
                        .map(|v| ScalarValue::$SCALAR_TY(v.map(|v| v.to_string())))
                        .collect(),
                }
            }};
        }

        macro_rules! make_binary_test_case {
            ($INPUT:expr, $ARRAY_TY:ident, $SCALAR_TY:ident) => {{
                TestCase {
                    array: Arc::new($INPUT.iter().cloned().collect::<$ARRAY_TY>()),
                    scalars: $INPUT
                        .iter()
                        .map(|v| {
                            ScalarValue::$SCALAR_TY(v.map(|v| v.as_bytes().to_vec()))
                        })
                        .collect(),
                }
            }};
        }

        /// create a test case for DictionaryArray<$INDEX_TY>
        macro_rules! make_str_dict_test_case {
            ($INPUT:expr, $INDEX_TY:ty, $SCALAR_TY:ident) => {{
                TestCase {
                    array: {
                        let mut array = MutableDictionaryArray::<
                            $INDEX_TY,
                            MutableUtf8Array<i32>,
                        >::new();
                        array.try_extend(*($INPUT)).unwrap();
                        let array: DictionaryArray<$INDEX_TY> = array.into();
                        Arc::new(array)
                    },
                    scalars: $INPUT
                        .iter()
                        .map(|v| ScalarValue::$SCALAR_TY(v.map(|v| v.to_string())))
                        .collect(),
                }
            }};
        }
        let utc_tz = Some("UTC".to_owned());
        let cases = vec![
            make_test_case!(bool_vals, BooleanArray, Boolean),
            make_test_case!(f32_vals, Float32Array, Float32),
            make_test_case!(f64_vals, Float64Array, Float64),
            make_test_case!(i8_vals, Int8Array, Int8),
            make_test_case!(i16_vals, Int16Array, Int16),
            make_test_case!(i32_vals, Int32Array, Int32),
            make_test_case!(i64_vals, Int64Array, Int64),
            make_test_case!(u8_vals, UInt8Array, UInt8),
            make_test_case!(u16_vals, UInt16Array, UInt16),
            make_test_case!(u32_vals, UInt32Array, UInt32),
            make_test_case!(u64_vals, UInt64Array, UInt64),
            make_str_test_case!(str_vals, StringArray, Utf8),
            make_str_test_case!(str_vals, LargeStringArray, LargeUtf8),
            make_binary_test_case!(str_vals, SmallBinaryArray, Binary),
            make_binary_test_case!(str_vals, LargeBinaryArray, LargeBinary),
            make_date_test_case!(&i32_vals, Int32Array, Date32),
            make_date_test_case!(&i64_vals, Int64Array, Date64),
            make_ts_test_case!(&i64_vals, Second, TimestampSecond, utc_tz.clone()),
            make_ts_test_case!(
                &i64_vals,
                Millisecond,
                TimestampMillisecond,
                utc_tz.clone()
            ),
            make_ts_test_case!(
                &i64_vals,
                Microsecond,
                TimestampMicrosecond,
                utc_tz.clone()
            ),
            make_ts_test_case!(
                &i64_vals,
                Nanosecond,
                TimestampNanosecond,
                utc_tz.clone()
            ),
            make_ts_test_case!(&i64_vals, Second, TimestampSecond, None),
            make_ts_test_case!(&i64_vals, Millisecond, TimestampMillisecond, None),
            make_ts_test_case!(&i64_vals, Microsecond, TimestampMicrosecond, None),
            make_ts_test_case!(&i64_vals, Nanosecond, TimestampNanosecond, None),
            make_temporal_test_case!(&i32_vals, Int32Array, YearMonth, IntervalYearMonth),
            make_temporal_test_case!(days_ms_vals, DaysMsArray, DayTime, IntervalDayTime),
            make_str_dict_test_case!(str_vals, i8, Utf8),
            make_str_dict_test_case!(str_vals, i16, Utf8),
            make_str_dict_test_case!(str_vals, i32, Utf8),
            make_str_dict_test_case!(str_vals, i64, Utf8),
            make_str_dict_test_case!(str_vals, u8, Utf8),
            make_str_dict_test_case!(str_vals, u16, Utf8),
            make_str_dict_test_case!(str_vals, u32, Utf8),
            make_str_dict_test_case!(str_vals, u64, Utf8),
        ];

        for case in cases {
            let TestCase { array, scalars } = case;
            assert_eq!(array.len(), scalars.len());

            for (index, scalar) in scalars.into_iter().enumerate() {
                assert!(
                    scalar.eq_array(&array, index),
                    "Expected {:?} to be equal to {:?} at index {}",
                    scalar,
                    array,
                    index
                );

                // test that all other elements are *not* equal
                for other_index in 0..array.len() {
                    if index != other_index {
                        assert!(
                            !scalar.eq_array(&array, other_index),
                            "Expected {:?} to be NOT equal to {:?} at index {}",
                            scalar,
                            array,
                            other_index
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn scalar_partial_ordering() {
        use ScalarValue::*;

        assert_eq!(
            Int64(Some(33)).partial_cmp(&Int64(Some(0))),
            Some(Ordering::Greater)
        );
        assert_eq!(
            Int64(Some(0)).partial_cmp(&Int64(Some(33))),
            Some(Ordering::Less)
        );
        assert_eq!(
            Int64(Some(33)).partial_cmp(&Int64(Some(33))),
            Some(Ordering::Equal)
        );
        // For different data type, `partial_cmp` returns None.
        assert_eq!(Int64(Some(33)).partial_cmp(&Int32(Some(33))), None);
        assert_eq!(Int32(Some(33)).partial_cmp(&Int64(Some(33))), None);

        assert_eq!(
            List(
                Some(Box::new(vec![Int32(Some(1)), Int32(Some(5))])),
                Box::new(DataType::Int32),
            )
            .partial_cmp(&List(
                Some(Box::new(vec![Int32(Some(1)), Int32(Some(5))])),
                Box::new(DataType::Int32),
            )),
            Some(Ordering::Equal)
        );

        assert_eq!(
            List(
                Some(Box::new(vec![Int32(Some(10)), Int32(Some(5))])),
                Box::new(DataType::Int32),
            )
            .partial_cmp(&List(
                Some(Box::new(vec![Int32(Some(1)), Int32(Some(5))])),
                Box::new(DataType::Int32),
            )),
            Some(Ordering::Greater)
        );

        assert_eq!(
            List(
                Some(Box::new(vec![Int32(Some(1)), Int32(Some(5))])),
                Box::new(DataType::Int32),
            )
            .partial_cmp(&List(
                Some(Box::new(vec![Int32(Some(10)), Int32(Some(5))])),
                Box::new(DataType::Int32),
            )),
            Some(Ordering::Less)
        );

        // For different data type, `partial_cmp` returns None.
        assert_eq!(
            List(
                Some(Box::new(vec![Int64(Some(1)), Int64(Some(5))])),
                Box::new(DataType::Int64),
            )
            .partial_cmp(&List(
                Some(Box::new(vec![Int32(Some(1)), Int32(Some(5))])),
                Box::new(DataType::Int32),
            )),
            None
        );

        assert_eq!(
            ScalarValue::from(vec![
                ("A", ScalarValue::from(1.0)),
                ("B", ScalarValue::from("Z")),
            ])
            .partial_cmp(&ScalarValue::from(vec![
                ("A", ScalarValue::from(2.0)),
                ("B", ScalarValue::from("A")),
            ])),
            Some(Ordering::Less)
        );

        // For different struct fields, `partial_cmp` returns None.
        assert_eq!(
            ScalarValue::from(vec![
                ("A", ScalarValue::from(1.0)),
                ("B", ScalarValue::from("Z")),
            ])
            .partial_cmp(&ScalarValue::from(vec![
                ("a", ScalarValue::from(2.0)),
                ("b", ScalarValue::from("A")),
            ])),
            None
        );
    }

    #[test]
    fn test_scalar_struct() {
        let field_a = Field::new("A", DataType::Int32, false);
        let field_b = Field::new("B", DataType::Boolean, false);
        let field_c = Field::new("C", DataType::Utf8, false);

        let field_e = Field::new("e", DataType::Int16, false);
        let field_f = Field::new("f", DataType::Int64, false);
        let field_d = Field::new(
            "D",
            DataType::Struct(vec![field_e.clone(), field_f.clone()]),
            false,
        );

        let scalar = ScalarValue::Struct(
            Some(Box::new(vec![
                ScalarValue::Int32(Some(23)),
                ScalarValue::Boolean(Some(false)),
                ScalarValue::Utf8(Some("Hello".to_string())),
                ScalarValue::from(vec![
                    ("e", ScalarValue::from(2i16)),
                    ("f", ScalarValue::from(3i64)),
                ]),
            ])),
            Box::new(vec![
                field_a.clone(),
                field_b.clone(),
                field_c.clone(),
                field_d.clone(),
            ]),
        );
        let _dt = scalar.get_datatype();
        let _sub_dt = field_d.data_type.clone();

        // Check Display
        assert_eq!(
            format!("{}", scalar),
            String::from("{A:23,B:false,C:Hello,D:{e:2,f:3}}")
        );

        // Check Debug
        assert_eq!(
            format!("{:?}", scalar),
            String::from(
                r#"Struct({A:Int32(23),B:Boolean(false),C:Utf8("Hello"),D:Struct({e:Int16(2),f:Int64(3)})})"#
            )
        );

        // Convert to length-2 array
        let array = scalar.to_array_of_size(2);
        let expected_vals = vec![
            (field_a.clone(), Int32Vec::from_slice(&[23, 23]).as_arc()),
            (
                field_b.clone(),
                Arc::new(BooleanArray::from_slice(&vec![false, false])) as ArrayRef,
            ),
            (
                field_c.clone(),
                Arc::new(StringArray::from_slice(&vec!["Hello", "Hello"])) as ArrayRef,
            ),
            (
                field_d.clone(),
                Arc::new(StructArray::from_data(
                    DataType::Struct(vec![field_e.clone(), field_f.clone()]),
                    vec![
                        Int16Vec::from_slice(&[2, 2]).as_arc(),
                        Int64Vec::from_slice(&[3, 3]).as_arc(),
                    ],
                    None,
                )) as ArrayRef,
            ),
        ];

        let expected = Arc::new(struct_array_from(expected_vals)) as ArrayRef;
        assert_eq!(&array, &expected);

        // Construct from second element of ArrayRef
        let constructed = ScalarValue::try_from_array(&expected, 1).unwrap();
        assert_eq!(constructed, scalar);

        // None version
        let none_scalar = ScalarValue::try_from(array.data_type()).unwrap();
        assert!(none_scalar.is_null());
        assert_eq!(format!("{:?}", none_scalar), String::from("Struct(NULL)"));

        // Construct with convenience From<Vec<(&str, ScalarValue)>>
        let constructed = ScalarValue::from(vec![
            ("A", ScalarValue::from(23i32)),
            ("B", ScalarValue::from(false)),
            ("C", ScalarValue::from("Hello")),
            (
                "D",
                ScalarValue::from(vec![
                    ("e", ScalarValue::from(2i16)),
                    ("f", ScalarValue::from(3i64)),
                ]),
            ),
        ]);
        assert_eq!(constructed, scalar);

        // Build Array from Vec of structs
        let scalars = vec![
            ScalarValue::from(vec![
                ("A", ScalarValue::from(23i32)),
                ("B", ScalarValue::from(false)),
                ("C", ScalarValue::from("Hello")),
                (
                    "D",
                    ScalarValue::from(vec![
                        ("e", ScalarValue::from(2i16)),
                        ("f", ScalarValue::from(3i64)),
                    ]),
                ),
            ]),
            ScalarValue::from(vec![
                ("A", ScalarValue::from(7i32)),
                ("B", ScalarValue::from(true)),
                ("C", ScalarValue::from("World")),
                (
                    "D",
                    ScalarValue::from(vec![
                        ("e", ScalarValue::from(4i16)),
                        ("f", ScalarValue::from(5i64)),
                    ]),
                ),
            ]),
            ScalarValue::from(vec![
                ("A", ScalarValue::from(-1000i32)),
                ("B", ScalarValue::from(true)),
                ("C", ScalarValue::from("!!!!!")),
                (
                    "D",
                    ScalarValue::from(vec![
                        ("e", ScalarValue::from(6i16)),
                        ("f", ScalarValue::from(7i64)),
                    ]),
                ),
            ]),
        ];
        let array: ArrayRef = ScalarValue::iter_to_array(scalars).unwrap();

        let expected = Arc::new(struct_array_from(vec![
            (field_a, Int32Vec::from_slice(&[23, 7, -1000]).as_arc()),
            (
                field_b,
                Arc::new(BooleanArray::from_slice(&vec![false, true, true])) as ArrayRef,
            ),
            (
                field_c,
                Arc::new(StringArray::from_slice(&vec!["Hello", "World", "!!!!!"]))
                    as ArrayRef,
            ),
            (
                field_d,
                Arc::new(StructArray::from_data(
                    DataType::Struct(vec![field_e, field_f]),
                    vec![
                        Int16Vec::from_slice(&[2, 4, 6]).as_arc(),
                        Int64Vec::from_slice(&[3, 5, 7]).as_arc(),
                    ],
                    None,
                )) as ArrayRef,
            ),
        ])) as ArrayRef;

        assert_eq!(&array, &expected);
    }

    #[test]
    fn test_lists_in_struct() {
        let field_a = Field::new("A", DataType::Utf8, false);
        let field_primitive_list = Field::new(
            "primitive_list",
            DataType::List(Box::new(Field::new("item", DataType::Int32, true))),
            false,
        );

        // Define primitive list scalars
        let l0 = ScalarValue::List(
            Some(Box::new(vec![
                ScalarValue::from(1i32),
                ScalarValue::from(2i32),
                ScalarValue::from(3i32),
            ])),
            Box::new(DataType::Int32),
        );

        let l1 = ScalarValue::List(
            Some(Box::new(vec![
                ScalarValue::from(4i32),
                ScalarValue::from(5i32),
            ])),
            Box::new(DataType::Int32),
        );

        let l2 = ScalarValue::List(
            Some(Box::new(vec![ScalarValue::from(6i32)])),
            Box::new(DataType::Int32),
        );

        // Define struct scalars
        let s0 = ScalarValue::from(vec![
            ("A", ScalarValue::Utf8(Some(String::from("First")))),
            ("primitive_list", l0),
        ]);

        let s1 = ScalarValue::from(vec![
            ("A", ScalarValue::Utf8(Some(String::from("Second")))),
            ("primitive_list", l1),
        ]);

        let s2 = ScalarValue::from(vec![
            ("A", ScalarValue::Utf8(Some(String::from("Third")))),
            ("primitive_list", l2),
        ]);

        // iter_to_array for struct scalars
        let array =
            ScalarValue::iter_to_array(vec![s0.clone(), s1.clone(), s2.clone()]).unwrap();
        let array = array.as_any().downcast_ref::<StructArray>().unwrap();

        let mut list_array =
            MutableListArray::<i32, Int32Vec>::new_with_capacity(Int32Vec::new(), 5);
        list_array
            .try_extend(vec![
                Some(vec![Some(1), Some(2), Some(3)]),
                Some(vec![Some(4), Some(5)]),
                Some(vec![Some(6)]),
            ])
            .unwrap();
        let expected = struct_array_from(vec![
            (
                field_a.clone(),
                Arc::new(StringArray::from_slice(&vec!["First", "Second", "Third"]))
                    as ArrayRef,
            ),
            (field_primitive_list.clone(), list_array.as_arc()),
        ]);

        assert_eq!(array, &expected);

        // Define list-of-structs scalars
        let nl0 = ScalarValue::List(
            Some(Box::new(vec![s0.clone(), s1.clone()])),
            Box::new(s0.get_datatype()),
        );

        let nl1 =
            ScalarValue::List(Some(Box::new(vec![s2])), Box::new(s0.get_datatype()));

        let nl2 =
            ScalarValue::List(Some(Box::new(vec![s1])), Box::new(s0.get_datatype()));

        // iter_to_array for list-of-struct
        let array = ScalarValue::iter_to_array(vec![nl0, nl1, nl2]).unwrap();
        let array = array.as_any().downcast_ref::<ListArray<i32>>().unwrap();

        // Construct expected array with array builders
        let field_a_builder =
            Utf8Array::<i32>::from_slice(&vec!["First", "Second", "Third", "Second"]);
        let primitive_value_builder = Int32Vec::with_capacity(5);
        let mut field_primitive_list_builder =
            MutableListArray::<i32, Int32Vec>::new_with_capacity(
                primitive_value_builder,
                0,
            );
        field_primitive_list_builder
            .try_push(Some(vec![1, 2, 3].into_iter().map(Option::Some)))
            .unwrap();
        field_primitive_list_builder
            .try_push(Some(vec![4, 5].into_iter().map(Option::Some)))
            .unwrap();
        field_primitive_list_builder
            .try_push(Some(vec![6].into_iter().map(Option::Some)))
            .unwrap();
        field_primitive_list_builder
            .try_push(Some(vec![4, 5].into_iter().map(Option::Some)))
            .unwrap();
        let _element_builder = StructArray::from_data(
            DataType::Struct(vec![field_a, field_primitive_list]),
            vec![
                Arc::new(field_a_builder),
                field_primitive_list_builder.as_arc(),
            ],
            None,
        );
        //let expected = ListArray::(element_builder, 5);
        eprintln!("array = {:?}", array);
        //assert_eq!(array, &expected);
    }

    #[test]
    fn test_nested_lists() {
        // Define inner list scalars
        let l1 = ScalarValue::List(
            Some(Box::new(vec![
                ScalarValue::List(
                    Some(Box::new(vec![
                        ScalarValue::from(1i32),
                        ScalarValue::from(2i32),
                        ScalarValue::from(3i32),
                    ])),
                    Box::new(DataType::Int32),
                ),
                ScalarValue::List(
                    Some(Box::new(vec![
                        ScalarValue::from(4i32),
                        ScalarValue::from(5i32),
                    ])),
                    Box::new(DataType::Int32),
                ),
            ])),
            Box::new(DataType::List(Box::new(Field::new(
                "item",
                DataType::Int32,
                true,
            )))),
        );

        let l2 = ScalarValue::List(
            Some(Box::new(vec![
                ScalarValue::List(
                    Some(Box::new(vec![ScalarValue::from(6i32)])),
                    Box::new(DataType::Int32),
                ),
                ScalarValue::List(
                    Some(Box::new(vec![
                        ScalarValue::from(7i32),
                        ScalarValue::from(8i32),
                    ])),
                    Box::new(DataType::Int32),
                ),
            ])),
            Box::new(DataType::List(Box::new(Field::new(
                "item",
                DataType::Int32,
                true,
            )))),
        );

        let l3 = ScalarValue::List(
            Some(Box::new(vec![ScalarValue::List(
                Some(Box::new(vec![ScalarValue::from(9i32)])),
                Box::new(DataType::Int32),
            )])),
            Box::new(DataType::List(Box::new(Field::new(
                "item",
                DataType::Int32,
                true,
            )))),
        );

        let array = ScalarValue::iter_to_array(vec![l1, l2, l3]).unwrap();

        // Construct expected array with array builders
        let inner_builder = Int32Vec::with_capacity(8);
        let middle_builder =
            MutableListArray::<i32, Int32Vec>::new_with_capacity(inner_builder, 0);
        let mut outer_builder =
            MutableListArray::<i32, MutableListArray<i32, Int32Vec>>::new_with_capacity(
                middle_builder,
                0,
            );
        outer_builder
            .try_push(Some(vec![
                Some(vec![Some(1), Some(2), Some(3)]),
                Some(vec![Some(4), Some(5)]),
            ]))
            .unwrap();
        outer_builder
            .try_push(Some(vec![
                Some(vec![Some(6)]),
                Some(vec![Some(7), Some(8)]),
            ]))
            .unwrap();
        outer_builder
            .try_push(Some(vec![Some(vec![Some(9)])]))
            .unwrap();

        let expected = outer_builder.as_arc();

        assert_eq!(&array, &expected);
    }

    #[test]
    fn scalar_timestamp_ns_utc_timezone() {
        let scalar = ScalarValue::TimestampNanosecond(
            Some(1599566400000000000),
            Some("UTC".to_owned()),
        );

        assert_eq!(
            scalar.get_datatype(),
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".to_owned()))
        );

        let array = scalar.to_array();
        assert_eq!(array.len(), 1);
        assert_eq!(
            array.data_type(),
            &DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".to_owned()))
        );

        let newscalar = ScalarValue::try_from_array(&array, 0).unwrap();
        assert_eq!(
            newscalar.get_datatype(),
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".to_owned()))
        );
    }
}
