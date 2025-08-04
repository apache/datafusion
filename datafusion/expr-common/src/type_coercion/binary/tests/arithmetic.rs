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
use datafusion_common::assert_contains;

#[test]
fn test_coercion_error() -> Result<()> {
    let coercer =
        BinaryTypeCoercer::new(&DataType::Float32, &Operator::Plus, &DataType::Utf8);
    let result_type = coercer.get_input_types();

    let e = result_type.unwrap_err();
    assert_eq!(e.strip_backtrace(), "Error during planning: Cannot coerce arithmetic expression Float32 + Utf8 to valid types");
    Ok(())
}

#[test]
fn test_date_timestamp_arithmetic_error() -> Result<()> {
    let (lhs, rhs) = BinaryTypeCoercer::new(
        &DataType::Timestamp(TimeUnit::Nanosecond, None),
        &Operator::Minus,
        &DataType::Timestamp(TimeUnit::Millisecond, None),
    )
    .get_input_types()?;
    assert_eq!(lhs.to_string(), "Timestamp(Millisecond, None)");
    assert_eq!(rhs.to_string(), "Timestamp(Millisecond, None)");

    let err =
        BinaryTypeCoercer::new(&DataType::Date32, &Operator::Plus, &DataType::Date64)
            .get_input_types()
            .unwrap_err()
            .to_string();

    assert_contains!(
        &err,
        "Cannot get result type for temporal operation Date64 + Date64"
    );

    Ok(())
}

#[test]
fn test_decimal_mathematics_op_type() {
    assert_eq!(
        coerce_numeric_type_to_decimal(&DataType::Int8).unwrap(),
        DataType::Decimal128(3, 0)
    );
    assert_eq!(
        coerce_numeric_type_to_decimal(&DataType::Int16).unwrap(),
        DataType::Decimal128(5, 0)
    );
    assert_eq!(
        coerce_numeric_type_to_decimal(&DataType::Int32).unwrap(),
        DataType::Decimal128(10, 0)
    );
    assert_eq!(
        coerce_numeric_type_to_decimal(&DataType::Int64).unwrap(),
        DataType::Decimal128(20, 0)
    );
    assert_eq!(
        coerce_numeric_type_to_decimal(&DataType::Float16).unwrap(),
        DataType::Decimal128(6, 3)
    );
    assert_eq!(
        coerce_numeric_type_to_decimal(&DataType::Float32).unwrap(),
        DataType::Decimal128(14, 7)
    );
    assert_eq!(
        coerce_numeric_type_to_decimal(&DataType::Float64).unwrap(),
        DataType::Decimal128(30, 15)
    );
}

#[test]
fn test_type_coercion_arithmetic() -> Result<()> {
    use DataType::*;

    // (Float64, _) | (_, Float64) => Some(Float64)
    test_coercion_binary_rule_multiple!(
        Float64,
        [
            Float64, Float32, Float16, Int64, UInt64, Int32, UInt32, Int16, UInt16, Int8,
            UInt8
        ],
        Operator::Plus,
        Float64
    );
    // (_, Float32) | (Float32, _) => Some(Float32)
    test_coercion_binary_rule_multiple!(
        Float32,
        [Float32, Float16, Int64, UInt64, Int32, UInt32, Int16, UInt16, Int8, UInt8],
        Operator::Plus,
        Float32
    );
    // (_, Float16) | (Float16, _) => Some(Float16)
    test_coercion_binary_rule_multiple!(
        Float16,
        [Float16, Int64, UInt64, Int32, UInt32, Int16, UInt16, Int8, UInt8],
        Operator::Plus,
        Float16
    );
    // (UInt64, Int64 | Int32 | Int16 | Int8) | (Int64 | Int32 | Int16 | Int8, UInt64)  => Some(Decimal128(20, 0))
    test_coercion_binary_rule_multiple!(
        UInt64,
        [Int64, Int32, Int16, Int8],
        Operator::Divide,
        Decimal128(20, 0)
    );
    // (UInt64, _) | (_, UInt64) => Some(UInt64)
    test_coercion_binary_rule_multiple!(
        UInt64,
        [UInt64, UInt32, UInt16, UInt8],
        Operator::Modulo,
        UInt64
    );
    // (Int64, _) | (_, Int64) => Some(Int64)
    test_coercion_binary_rule_multiple!(
        Int64,
        [Int64, Int32, UInt32, Int16, UInt16, Int8, UInt8],
        Operator::Modulo,
        Int64
    );
    // (UInt32, Int32 | Int16 | Int8) | (Int32 | Int16 | Int8, UInt32) => Some(Int64)
    test_coercion_binary_rule_multiple!(
        UInt32,
        [Int32, Int16, Int8],
        Operator::Modulo,
        Int64
    );
    // (UInt32, _) | (_, UInt32) => Some(UInt32)
    test_coercion_binary_rule_multiple!(
        UInt32,
        [UInt32, UInt16, UInt8],
        Operator::Modulo,
        UInt32
    );
    // (Int32, _) | (_, Int32) => Some(Int32)
    test_coercion_binary_rule_multiple!(
        Int32,
        [Int32, Int16, Int8],
        Operator::Modulo,
        Int32
    );
    // (UInt16, Int16 | Int8) | (Int16 | Int8, UInt16) => Some(Int32)
    test_coercion_binary_rule_multiple!(UInt16, [Int16, Int8], Operator::Minus, Int32);
    // (UInt16, _) | (_, UInt16) => Some(UInt16)
    test_coercion_binary_rule_multiple!(
        UInt16,
        [UInt16, UInt8, UInt8],
        Operator::Plus,
        UInt16
    );
    // (Int16, _) | (_, Int16) => Some(Int16)
    test_coercion_binary_rule_multiple!(Int16, [Int16, Int8], Operator::Plus, Int16);
    // (UInt8, Int8) | (Int8, UInt8) => Some(Int16)
    test_coercion_binary_rule!(Int8, UInt8, Operator::Minus, Int16);
    test_coercion_binary_rule!(UInt8, Int8, Operator::Multiply, Int16);
    // (UInt8, _) | (_, UInt8) => Some(UInt8)
    test_coercion_binary_rule!(UInt8, UInt8, Operator::Minus, UInt8);
    // (Int8, _) | (_, Int8) => Some(Int8)
    test_coercion_binary_rule!(Int8, Int8, Operator::Plus, Int8);

    Ok(())
}

fn test_math_decimal_coercion_rule(
    lhs_type: DataType,
    rhs_type: DataType,
    expected_lhs_type: DataType,
    expected_rhs_type: DataType,
) {
    let (lhs_type, rhs_type) = math_decimal_coercion(&lhs_type, &rhs_type).unwrap();
    assert_eq!(lhs_type, expected_lhs_type);
    assert_eq!(rhs_type, expected_rhs_type);
}

#[test]
fn test_coercion_arithmetic_decimal() -> Result<()> {
    test_math_decimal_coercion_rule(
        DataType::Decimal128(10, 2),
        DataType::Decimal128(10, 2),
        DataType::Decimal128(10, 2),
        DataType::Decimal128(10, 2),
    );

    test_math_decimal_coercion_rule(
        DataType::Int32,
        DataType::Decimal128(10, 2),
        DataType::Decimal128(10, 0),
        DataType::Decimal128(10, 2),
    );

    test_math_decimal_coercion_rule(
        DataType::Int32,
        DataType::Decimal128(10, 2),
        DataType::Decimal128(10, 0),
        DataType::Decimal128(10, 2),
    );

    test_math_decimal_coercion_rule(
        DataType::Int32,
        DataType::Decimal128(10, 2),
        DataType::Decimal128(10, 0),
        DataType::Decimal128(10, 2),
    );

    test_math_decimal_coercion_rule(
        DataType::Int32,
        DataType::Decimal128(10, 2),
        DataType::Decimal128(10, 0),
        DataType::Decimal128(10, 2),
    );

    test_math_decimal_coercion_rule(
        DataType::Int32,
        DataType::Decimal128(10, 2),
        DataType::Decimal128(10, 0),
        DataType::Decimal128(10, 2),
    );

    test_math_decimal_coercion_rule(
        DataType::UInt32,
        DataType::Decimal128(10, 2),
        DataType::Decimal128(10, 0),
        DataType::Decimal128(10, 2),
    );
    test_math_decimal_coercion_rule(
        DataType::Decimal128(10, 2),
        DataType::UInt32,
        DataType::Decimal128(10, 2),
        DataType::Decimal128(10, 0),
    );

    Ok(())
}
