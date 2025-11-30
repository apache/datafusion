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

use super::*;

#[test]
fn test_decimal_binary_comparison_coercion() -> Result<()> {
    let input_decimal = DataType::Decimal128(20, 3);
    let input_types = [
        DataType::Int8,
        DataType::Int16,
        DataType::Int32,
        DataType::Int64,
        DataType::Float32,
        DataType::Float64,
        DataType::Decimal128(38, 10),
        DataType::Decimal128(20, 8),
        DataType::Null,
    ];
    let result_types = [
        DataType::Decimal128(20, 3),
        DataType::Decimal128(20, 3),
        DataType::Decimal128(20, 3),
        DataType::Decimal128(23, 3),
        DataType::Decimal128(24, 7),
        DataType::Decimal128(32, 15),
        DataType::Decimal128(38, 10),
        DataType::Decimal128(25, 8),
        DataType::Decimal128(20, 3),
    ];
    let comparison_op_types = [
        Operator::NotEq,
        Operator::Eq,
        Operator::Gt,
        Operator::GtEq,
        Operator::Lt,
        Operator::LtEq,
    ];
    for (i, input_type) in input_types.iter().enumerate() {
        let expect_type = &result_types[i];
        for op in comparison_op_types {
            let (lhs, rhs) = BinaryTypeCoercer::new(&input_decimal, &op, input_type)
                .get_input_types()?;
            assert_eq!(expect_type, &lhs);
            assert_eq!(expect_type, &rhs);
        }
    }
    // negative test
    let result_type =
        BinaryTypeCoercer::new(&input_decimal, &Operator::Eq, &DataType::Boolean)
            .get_input_types();
    assert!(result_type.is_err());
    Ok(())
}

#[test]
fn test_like_coercion() {
    // string coerce to strings
    test_like_rule!(DataType::Utf8, DataType::Utf8, Some(DataType::Utf8));
    test_like_rule!(
        DataType::LargeUtf8,
        DataType::Utf8,
        Some(DataType::LargeUtf8)
    );
    test_like_rule!(
        DataType::Utf8,
        DataType::LargeUtf8,
        Some(DataType::LargeUtf8)
    );
    test_like_rule!(
        DataType::LargeUtf8,
        DataType::LargeUtf8,
        Some(DataType::LargeUtf8)
    );

    // Also coerce binary to strings
    test_like_rule!(DataType::Binary, DataType::Utf8, Some(DataType::Utf8));
    test_like_rule!(
        DataType::LargeBinary,
        DataType::Utf8,
        Some(DataType::LargeUtf8)
    );
    test_like_rule!(
        DataType::Binary,
        DataType::LargeUtf8,
        Some(DataType::LargeUtf8)
    );
    test_like_rule!(
        DataType::LargeBinary,
        DataType::LargeUtf8,
        Some(DataType::LargeUtf8)
    );
}

#[test]
fn test_type_coercion() -> Result<()> {
    test_coercion_binary_rule!(
        DataType::Utf8,
        DataType::Date32,
        Operator::Eq,
        DataType::Date32
    );
    test_coercion_binary_rule!(
        DataType::Utf8,
        DataType::Date64,
        Operator::Lt,
        DataType::Date64
    );
    test_coercion_binary_rule!(
        DataType::Utf8,
        DataType::Time32(TimeUnit::Second),
        Operator::Eq,
        DataType::Time32(TimeUnit::Second)
    );
    test_coercion_binary_rule!(
        DataType::Utf8,
        DataType::Time32(TimeUnit::Millisecond),
        Operator::Eq,
        DataType::Time32(TimeUnit::Millisecond)
    );
    test_coercion_binary_rule!(
        DataType::Utf8,
        DataType::Time64(TimeUnit::Microsecond),
        Operator::Eq,
        DataType::Time64(TimeUnit::Microsecond)
    );
    test_coercion_binary_rule!(
        DataType::Utf8,
        DataType::Time64(TimeUnit::Nanosecond),
        Operator::Eq,
        DataType::Time64(TimeUnit::Nanosecond)
    );
    test_coercion_binary_rule!(
        DataType::Utf8,
        DataType::Timestamp(TimeUnit::Second, None),
        Operator::Lt,
        DataType::Timestamp(TimeUnit::Nanosecond, None)
    );
    test_coercion_binary_rule!(
        DataType::Utf8,
        DataType::Timestamp(TimeUnit::Millisecond, None),
        Operator::Lt,
        DataType::Timestamp(TimeUnit::Nanosecond, None)
    );
    test_coercion_binary_rule!(
        DataType::Utf8,
        DataType::Timestamp(TimeUnit::Microsecond, None),
        Operator::Lt,
        DataType::Timestamp(TimeUnit::Nanosecond, None)
    );
    test_coercion_binary_rule!(
        DataType::Utf8,
        DataType::Timestamp(TimeUnit::Nanosecond, None),
        Operator::Lt,
        DataType::Timestamp(TimeUnit::Nanosecond, None)
    );
    test_coercion_binary_rule!(
        DataType::Utf8,
        DataType::Utf8,
        Operator::RegexMatch,
        DataType::Utf8
    );
    test_coercion_binary_rule!(
        DataType::Utf8,
        DataType::Utf8View,
        Operator::RegexMatch,
        DataType::Utf8View
    );
    test_coercion_binary_rule!(
        DataType::Utf8View,
        DataType::Utf8,
        Operator::RegexMatch,
        DataType::Utf8View
    );
    test_coercion_binary_rule!(
        DataType::Utf8View,
        DataType::Utf8View,
        Operator::RegexMatch,
        DataType::Utf8View
    );
    test_coercion_binary_rule!(
        DataType::Utf8,
        DataType::Utf8,
        Operator::RegexNotMatch,
        DataType::Utf8
    );
    test_coercion_binary_rule!(
        DataType::Utf8View,
        DataType::Utf8,
        Operator::RegexNotMatch,
        DataType::Utf8View
    );
    test_coercion_binary_rule!(
        DataType::Utf8,
        DataType::Utf8View,
        Operator::RegexNotMatch,
        DataType::Utf8View
    );
    test_coercion_binary_rule!(
        DataType::Utf8View,
        DataType::Utf8View,
        Operator::RegexNotMatch,
        DataType::Utf8View
    );
    test_coercion_binary_rule!(
        DataType::Utf8,
        DataType::Utf8,
        Operator::RegexNotIMatch,
        DataType::Utf8
    );
    test_coercion_binary_rule!(
        DataType::Utf8View,
        DataType::Utf8,
        Operator::RegexNotIMatch,
        DataType::Utf8View
    );
    test_coercion_binary_rule!(
        DataType::Utf8,
        DataType::Utf8View,
        Operator::RegexNotIMatch,
        DataType::Utf8View
    );
    test_coercion_binary_rule!(
        DataType::Utf8View,
        DataType::Utf8View,
        Operator::RegexNotIMatch,
        DataType::Utf8View
    );
    test_coercion_binary_rule!(
        DataType::Dictionary(DataType::Int32.into(), DataType::Utf8.into()),
        DataType::Utf8,
        Operator::RegexMatch,
        DataType::Utf8
    );
    test_coercion_binary_rule!(
        DataType::Dictionary(DataType::Int32.into(), DataType::Utf8.into()),
        DataType::Utf8View,
        Operator::RegexMatch,
        DataType::Utf8View
    );
    test_coercion_binary_rule!(
        DataType::Dictionary(DataType::Int32.into(), DataType::Utf8View.into()),
        DataType::Utf8,
        Operator::RegexMatch,
        DataType::Utf8View
    );
    test_coercion_binary_rule!(
        DataType::Dictionary(DataType::Int32.into(), DataType::Utf8View.into()),
        DataType::Utf8View,
        Operator::RegexMatch,
        DataType::Utf8View
    );
    test_coercion_binary_rule!(
        DataType::Dictionary(DataType::Int32.into(), DataType::Utf8.into()),
        DataType::Utf8,
        Operator::RegexIMatch,
        DataType::Utf8
    );
    test_coercion_binary_rule!(
        DataType::Dictionary(DataType::Int32.into(), DataType::Utf8View.into()),
        DataType::Utf8,
        Operator::RegexIMatch,
        DataType::Utf8View
    );
    test_coercion_binary_rule!(
        DataType::Dictionary(DataType::Int32.into(), DataType::Utf8.into()),
        DataType::Utf8View,
        Operator::RegexIMatch,
        DataType::Utf8View
    );
    test_coercion_binary_rule!(
        DataType::Dictionary(DataType::Int32.into(), DataType::Utf8View.into()),
        DataType::Utf8View,
        Operator::RegexIMatch,
        DataType::Utf8View
    );
    test_coercion_binary_rule!(
        DataType::Dictionary(DataType::Int32.into(), DataType::Utf8.into()),
        DataType::Utf8,
        Operator::RegexNotMatch,
        DataType::Utf8
    );
    test_coercion_binary_rule!(
        DataType::Dictionary(DataType::Int32.into(), DataType::Utf8.into()),
        DataType::Utf8View,
        Operator::RegexNotMatch,
        DataType::Utf8View
    );
    test_coercion_binary_rule!(
        DataType::Dictionary(DataType::Int32.into(), DataType::Utf8View.into()),
        DataType::Utf8,
        Operator::RegexNotMatch,
        DataType::Utf8View
    );
    test_coercion_binary_rule!(
        DataType::Dictionary(DataType::Int32.into(), DataType::Utf8.into()),
        DataType::Utf8View,
        Operator::RegexNotMatch,
        DataType::Utf8View
    );
    test_coercion_binary_rule!(
        DataType::Dictionary(DataType::Int32.into(), DataType::Utf8.into()),
        DataType::Utf8,
        Operator::RegexNotIMatch,
        DataType::Utf8
    );
    test_coercion_binary_rule!(
        DataType::Dictionary(DataType::Int32.into(), DataType::Utf8View.into()),
        DataType::Utf8,
        Operator::RegexNotIMatch,
        DataType::Utf8View
    );
    test_coercion_binary_rule!(
        DataType::Dictionary(DataType::Int32.into(), DataType::Utf8.into()),
        DataType::Utf8View,
        Operator::RegexNotIMatch,
        DataType::Utf8View
    );
    test_coercion_binary_rule!(
        DataType::Dictionary(DataType::Int32.into(), DataType::Utf8View.into()),
        DataType::Utf8View,
        Operator::RegexNotIMatch,
        DataType::Utf8View
    );
    test_coercion_binary_rule!(
        DataType::Int16,
        DataType::Int64,
        Operator::BitwiseAnd,
        DataType::Int64
    );
    test_coercion_binary_rule!(
        DataType::UInt64,
        DataType::UInt64,
        Operator::BitwiseAnd,
        DataType::UInt64
    );
    test_coercion_binary_rule!(
        DataType::Int8,
        DataType::UInt32,
        Operator::BitwiseAnd,
        DataType::Int64
    );
    test_coercion_binary_rule!(
        DataType::UInt32,
        DataType::Int32,
        Operator::BitwiseAnd,
        DataType::Int64
    );
    test_coercion_binary_rule!(
        DataType::UInt16,
        DataType::Int16,
        Operator::BitwiseAnd,
        DataType::Int32
    );
    test_coercion_binary_rule!(
        DataType::UInt32,
        DataType::UInt32,
        Operator::BitwiseAnd,
        DataType::UInt32
    );
    test_coercion_binary_rule!(
        DataType::UInt16,
        DataType::UInt32,
        Operator::BitwiseAnd,
        DataType::UInt32
    );
    Ok(())
}

#[test]
fn test_type_coercion_compare() -> Result<()> {
    // boolean
    test_coercion_binary_rule!(
        DataType::Boolean,
        DataType::Boolean,
        Operator::Eq,
        DataType::Boolean
    );
    // float
    test_coercion_binary_rule!(
        DataType::Float16,
        DataType::Int64,
        Operator::Eq,
        DataType::Float16
    );
    test_coercion_binary_rule!(
        DataType::Float16,
        DataType::Float64,
        Operator::Eq,
        DataType::Float64
    );
    test_coercion_binary_rule!(
        DataType::Float32,
        DataType::Int64,
        Operator::Eq,
        DataType::Float32
    );
    test_coercion_binary_rule!(
        DataType::Float32,
        DataType::Float64,
        Operator::GtEq,
        DataType::Float64
    );
    // signed integer
    test_coercion_binary_rule!(
        DataType::Int8,
        DataType::Int32,
        Operator::LtEq,
        DataType::Int32
    );
    test_coercion_binary_rule!(
        DataType::Int64,
        DataType::Int32,
        Operator::LtEq,
        DataType::Int64
    );
    // unsigned integer
    test_coercion_binary_rule!(
        DataType::UInt32,
        DataType::UInt8,
        Operator::Gt,
        DataType::UInt32
    );
    test_coercion_binary_rule!(
        DataType::UInt64,
        DataType::UInt8,
        Operator::Eq,
        DataType::UInt64
    );
    test_coercion_binary_rule!(
        DataType::UInt64,
        DataType::Int64,
        Operator::Eq,
        DataType::Decimal128(20, 0)
    );
    // numeric/decimal
    test_coercion_binary_rule!(
        DataType::Int64,
        DataType::Decimal128(10, 0),
        Operator::Eq,
        DataType::Decimal128(20, 0)
    );
    test_coercion_binary_rule!(
        DataType::Int64,
        DataType::Decimal128(10, 2),
        Operator::Lt,
        DataType::Decimal128(22, 2)
    );
    test_coercion_binary_rule!(
        DataType::Float64,
        DataType::Decimal128(10, 3),
        Operator::Gt,
        DataType::Decimal128(30, 15)
    );
    test_coercion_binary_rule!(
        DataType::Int64,
        DataType::Decimal128(10, 0),
        Operator::Eq,
        DataType::Decimal128(20, 0)
    );
    test_coercion_binary_rule!(
        DataType::Decimal128(14, 2),
        DataType::Decimal128(10, 3),
        Operator::GtEq,
        DataType::Decimal128(15, 3)
    );
    test_coercion_binary_rule!(
        DataType::UInt64,
        DataType::Decimal128(20, 0),
        Operator::Eq,
        DataType::Decimal128(20, 0)
    );

    // Binary
    test_coercion_binary_rule!(
        DataType::Binary,
        DataType::Binary,
        Operator::Eq,
        DataType::Binary
    );
    test_coercion_binary_rule!(
        DataType::Utf8,
        DataType::Binary,
        Operator::Eq,
        DataType::Binary
    );
    test_coercion_binary_rule!(
        DataType::Binary,
        DataType::Utf8,
        Operator::Eq,
        DataType::Binary
    );

    // LargeBinary
    test_coercion_binary_rule!(
        DataType::LargeBinary,
        DataType::LargeBinary,
        Operator::Eq,
        DataType::LargeBinary
    );
    test_coercion_binary_rule!(
        DataType::Binary,
        DataType::LargeBinary,
        Operator::Eq,
        DataType::LargeBinary
    );
    test_coercion_binary_rule!(
        DataType::LargeBinary,
        DataType::Binary,
        Operator::Eq,
        DataType::LargeBinary
    );
    test_coercion_binary_rule!(
        DataType::Utf8,
        DataType::LargeBinary,
        Operator::Eq,
        DataType::LargeBinary
    );
    test_coercion_binary_rule!(
        DataType::LargeBinary,
        DataType::Utf8,
        Operator::Eq,
        DataType::LargeBinary
    );
    test_coercion_binary_rule!(
        DataType::LargeUtf8,
        DataType::LargeBinary,
        Operator::Eq,
        DataType::LargeBinary
    );
    test_coercion_binary_rule!(
        DataType::LargeBinary,
        DataType::LargeUtf8,
        Operator::Eq,
        DataType::LargeBinary
    );

    // Timestamps
    let utc: Option<Arc<str>> = Some("UTC".into());
    test_coercion_binary_rule!(
        DataType::Timestamp(TimeUnit::Second, utc.clone()),
        DataType::Timestamp(TimeUnit::Second, utc.clone()),
        Operator::Eq,
        DataType::Timestamp(TimeUnit::Second, utc.clone())
    );
    test_coercion_binary_rule!(
        DataType::Timestamp(TimeUnit::Second, utc.clone()),
        DataType::Timestamp(TimeUnit::Second, Some("Europe/Brussels".into())),
        Operator::Eq,
        DataType::Timestamp(TimeUnit::Second, utc.clone())
    );
    test_coercion_binary_rule!(
        DataType::Timestamp(TimeUnit::Second, Some("America/New_York".into())),
        DataType::Timestamp(TimeUnit::Second, Some("Europe/Brussels".into())),
        Operator::Eq,
        DataType::Timestamp(TimeUnit::Second, Some("America/New_York".into()))
    );
    test_coercion_binary_rule!(
        DataType::Timestamp(TimeUnit::Second, Some("Europe/Brussels".into())),
        DataType::Timestamp(TimeUnit::Second, utc),
        Operator::Eq,
        DataType::Timestamp(TimeUnit::Second, Some("Europe/Brussels".into()))
    );

    // list
    let inner_field = Arc::new(Field::new_list_field(DataType::Int64, true));
    test_coercion_binary_rule!(
        DataType::List(Arc::clone(&inner_field)),
        DataType::List(Arc::clone(&inner_field)),
        Operator::Eq,
        DataType::List(Arc::clone(&inner_field))
    );
    test_coercion_binary_rule!(
        DataType::List(Arc::clone(&inner_field)),
        DataType::LargeList(Arc::clone(&inner_field)),
        Operator::Eq,
        DataType::LargeList(Arc::clone(&inner_field))
    );
    test_coercion_binary_rule!(
        DataType::LargeList(Arc::clone(&inner_field)),
        DataType::List(Arc::clone(&inner_field)),
        Operator::Eq,
        DataType::LargeList(Arc::clone(&inner_field))
    );
    test_coercion_binary_rule!(
        DataType::LargeList(Arc::clone(&inner_field)),
        DataType::LargeList(Arc::clone(&inner_field)),
        Operator::Eq,
        DataType::LargeList(Arc::clone(&inner_field))
    );
    test_coercion_binary_rule!(
        DataType::FixedSizeList(Arc::clone(&inner_field), 10),
        DataType::FixedSizeList(Arc::clone(&inner_field), 10),
        Operator::Eq,
        DataType::FixedSizeList(Arc::clone(&inner_field), 10)
    );
    test_coercion_binary_rule!(
        DataType::FixedSizeList(Arc::clone(&inner_field), 10),
        DataType::LargeList(Arc::clone(&inner_field)),
        Operator::Eq,
        DataType::LargeList(Arc::clone(&inner_field))
    );
    test_coercion_binary_rule!(
        DataType::LargeList(Arc::clone(&inner_field)),
        DataType::FixedSizeList(Arc::clone(&inner_field), 10),
        Operator::Eq,
        DataType::LargeList(Arc::clone(&inner_field))
    );
    test_coercion_binary_rule!(
        DataType::List(Arc::clone(&inner_field)),
        DataType::FixedSizeList(Arc::clone(&inner_field), 10),
        Operator::Eq,
        DataType::List(Arc::clone(&inner_field))
    );
    test_coercion_binary_rule!(
        DataType::FixedSizeList(Arc::clone(&inner_field), 10),
        DataType::List(Arc::clone(&inner_field)),
        Operator::Eq,
        DataType::List(Arc::clone(&inner_field))
    );

    let inner_timestamp_field = Arc::new(Field::new_list_field(
        DataType::Timestamp(TimeUnit::Microsecond, None),
        true,
    ));
    let result_type = BinaryTypeCoercer::new(
        &DataType::List(Arc::clone(&inner_field)),
        &Operator::Eq,
        &DataType::List(Arc::clone(&inner_timestamp_field)),
    )
    .get_input_types();
    assert!(result_type.is_err());

    Ok(())
}

#[test]
fn test_list_coercion() {
    let lhs_type = DataType::List(Arc::new(Field::new("lhs", DataType::Int8, false)));

    let rhs_type = DataType::List(Arc::new(Field::new("rhs", DataType::Int64, true)));

    let coerced_type = list_coercion(&lhs_type, &rhs_type).unwrap();
    assert_eq!(
        coerced_type,
        DataType::List(Arc::new(Field::new("lhs", DataType::Int64, true)))
    );
}

#[test]
fn test_map_coercion() -> Result<()> {
    let lhs = Field::new_map(
        "lhs",
        "entries",
        Arc::new(Field::new("keys", DataType::Utf8, false)),
        Arc::new(Field::new("values", DataType::LargeUtf8, false)),
        true,
        false,
    );
    let rhs = Field::new_map(
        "rhs",
        "kvp",
        Arc::new(Field::new("k", DataType::Utf8, false)),
        Arc::new(Field::new("v", DataType::Utf8, true)),
        false,
        true,
    );

    let expected = Field::new_map(
        "expected",
        "entries",
        Arc::new(Field::new("keys", DataType::Utf8, false)),
        Arc::new(Field::new("values", DataType::LargeUtf8, true)),
        false,
        true,
    );

    test_coercion_binary_rule!(
        lhs.data_type(),
        rhs.data_type(),
        Operator::Eq,
        expected.data_type().clone()
    );
    Ok(())
}

#[test]
fn test_decimal_cross_variant_comparison_coercion() -> Result<()> {
    let test_cases = [
        // (lhs, rhs, expected_result)
        (
            DataType::Decimal32(5, 2),
            DataType::Decimal64(10, 3),
            DataType::Decimal64(10, 3),
        ),
        (
            DataType::Decimal32(7, 1),
            DataType::Decimal128(15, 4),
            DataType::Decimal128(15, 4),
        ),
        (
            DataType::Decimal32(9, 0),
            DataType::Decimal256(20, 5),
            DataType::Decimal256(20, 5),
        ),
        (
            DataType::Decimal64(12, 3),
            DataType::Decimal128(18, 2),
            DataType::Decimal128(19, 3),
        ),
        (
            DataType::Decimal64(15, 4),
            DataType::Decimal256(25, 6),
            DataType::Decimal256(25, 6),
        ),
        (
            DataType::Decimal128(20, 5),
            DataType::Decimal256(30, 8),
            DataType::Decimal256(30, 8),
        ),
        // Reverse order cases
        (
            DataType::Decimal64(10, 3),
            DataType::Decimal32(5, 2),
            DataType::Decimal64(10, 3),
        ),
        (
            DataType::Decimal128(15, 4),
            DataType::Decimal32(7, 1),
            DataType::Decimal128(15, 4),
        ),
        (
            DataType::Decimal256(20, 5),
            DataType::Decimal32(9, 0),
            DataType::Decimal256(20, 5),
        ),
        (
            DataType::Decimal128(18, 2),
            DataType::Decimal64(12, 3),
            DataType::Decimal128(19, 3),
        ),
        (
            DataType::Decimal256(25, 6),
            DataType::Decimal64(15, 4),
            DataType::Decimal256(25, 6),
        ),
        (
            DataType::Decimal256(30, 8),
            DataType::Decimal128(20, 5),
            DataType::Decimal256(30, 8),
        ),
    ];

    let comparison_op_types = [
        Operator::NotEq,
        Operator::Eq,
        Operator::Gt,
        Operator::GtEq,
        Operator::Lt,
        Operator::LtEq,
    ];

    for (lhs_type, rhs_type, expected_type) in test_cases {
        for op in comparison_op_types {
            let (lhs, rhs) =
                BinaryTypeCoercer::new(&lhs_type, &op, &rhs_type).get_input_types()?;
            assert_eq!(
                expected_type, lhs,
                "Coercion of type {lhs_type:?} with {rhs_type:?} resulted in unexpected type: {lhs:?}"
            );
            assert_eq!(
                expected_type, rhs,
                "Coercion of type {rhs_type:?} with {lhs_type:?} resulted in unexpected type: {rhs:?}"
            );
        }
    }

    Ok(())
}
