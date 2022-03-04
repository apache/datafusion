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

//! Coercion rules for matching argument types for binary operators

use arrow::datatypes::DataType;
use datafusion_common::DataFusionError;
use datafusion_common::Result;
use datafusion_common::{MAX_PRECISION_FOR_DECIMAL128, MAX_SCALE_FOR_DECIMAL128};
use datafusion_expr::Operator;

/// Coercion rules for all binary operators. Returns the output type
/// of applying `op` to an argument of `lhs_type` and `rhs_type`.
pub(crate) fn coerce_types(
    lhs_type: &DataType,
    op: &Operator,
    rhs_type: &DataType,
) -> Result<DataType> {
    // This result MUST be compatible with `binary_coerce`
    let result = match op {
        Operator::BitwiseAnd | Operator::BitwiseOr => {
            bitwise_coercion(lhs_type, rhs_type)
        }
        Operator::And | Operator::Or => match (lhs_type, rhs_type) {
            // logical binary boolean operators can only be evaluated in bools
            (DataType::Boolean, DataType::Boolean) => Some(DataType::Boolean),
            _ => None,
        },
        // logical equality operators have their own rules, and always return a boolean
        Operator::Eq | Operator::NotEq => comparison_eq_coercion(lhs_type, rhs_type),
        // order-comparison operators have their own rules
        Operator::Lt | Operator::Gt | Operator::GtEq | Operator::LtEq => {
            comparison_order_coercion(lhs_type, rhs_type)
        }
        // "like" operators operate on strings and always return a boolean
        Operator::Like | Operator::NotLike => like_coercion(lhs_type, rhs_type),
        // for math expressions, the final value of the coercion is also the return type
        // because coercion favours higher information types
        Operator::Plus
        | Operator::Minus
        | Operator::Modulo
        | Operator::Divide
        | Operator::Multiply => mathematics_numerical_coercion(op, lhs_type, rhs_type),
        Operator::RegexMatch
        | Operator::RegexIMatch
        | Operator::RegexNotMatch
        | Operator::RegexNotIMatch => string_coercion(lhs_type, rhs_type),
        Operator::IsDistinctFrom | Operator::IsNotDistinctFrom => {
            eq_coercion(lhs_type, rhs_type)
        }
    };

    // re-write the error message of failed coercions to include the operator's information
    match result {
        None => Err(DataFusionError::Plan(
            format!(
                "'{:?} {} {:?}' can't be evaluated because there isn't a common type to coerce the types to",
                lhs_type, op, rhs_type
            ),
        )),
        Some(t) => Ok(t)
    }
}

fn bitwise_coercion(left_type: &DataType, right_type: &DataType) -> Option<DataType> {
    use arrow::datatypes::DataType::*;

    if !is_numeric(left_type) || !is_numeric(right_type) {
        return None;
    }
    if left_type == right_type && !is_dictionary(left_type) {
        return Some(left_type.clone());
    }
    // TODO support other data type
    match (left_type, right_type) {
        (Int64, _) | (_, Int64) => Some(Int64),
        (Int32, _) | (_, Int32) => Some(Int32),
        (Int16, _) | (_, Int16) => Some(Int16),
        (Int8, _) | (_, Int8) => Some(Int8),
        _ => None,
    }
}

fn comparison_eq_coercion(lhs_type: &DataType, rhs_type: &DataType) -> Option<DataType> {
    // can't compare dictionaries directly due to
    // https://github.com/apache/arrow-rs/issues/1201
    if lhs_type == rhs_type && !is_dictionary(lhs_type) {
        // same type => equality is possible
        return Some(lhs_type.clone());
    }
    comparison_binary_numeric_coercion(lhs_type, rhs_type)
        .or_else(|| dictionary_coercion(lhs_type, rhs_type))
        .or_else(|| temporal_coercion(lhs_type, rhs_type))
}

fn comparison_order_coercion(
    lhs_type: &DataType,
    rhs_type: &DataType,
) -> Option<DataType> {
    // can't compare dictionaries directly due to
    // https://github.com/apache/arrow-rs/issues/1201
    if lhs_type == rhs_type && !is_dictionary(lhs_type) {
        // same type => all good
        return Some(lhs_type.clone());
    }
    comparison_binary_numeric_coercion(lhs_type, rhs_type)
        .or_else(|| string_coercion(lhs_type, rhs_type))
        .or_else(|| dictionary_coercion(lhs_type, rhs_type))
        .or_else(|| temporal_coercion(lhs_type, rhs_type))
}

fn comparison_binary_numeric_coercion(
    lhs_type: &DataType,
    rhs_type: &DataType,
) -> Option<DataType> {
    use arrow::datatypes::DataType::*;
    if !is_numeric(lhs_type) || !is_numeric(rhs_type) {
        return None;
    };

    // same type => all good
    if lhs_type == rhs_type {
        return Some(lhs_type.clone());
    }

    // these are ordered from most informative to least informative so
    // that the coercion removes the least amount of information
    match (lhs_type, rhs_type) {
        // support decimal data type for comparison operation
        (Decimal(p1, s1), Decimal(p2, s2)) => Some(Decimal(*p1.max(p2), *s1.max(s2))),
        (Decimal(_, _), _) => get_comparison_common_decimal_type(lhs_type, rhs_type),
        (_, Decimal(_, _)) => get_comparison_common_decimal_type(rhs_type, lhs_type),
        (Float64, _) | (_, Float64) => Some(Float64),
        (_, Float32) | (Float32, _) => Some(Float32),
        (Int64, _) | (_, Int64) => Some(Int64),
        (Int32, _) | (_, Int32) => Some(Int32),
        (Int16, _) | (_, Int16) => Some(Int16),
        (Int8, _) | (_, Int8) => Some(Int8),
        (UInt64, _) | (_, UInt64) => Some(UInt64),
        (UInt32, _) | (_, UInt32) => Some(UInt32),
        (UInt16, _) | (_, UInt16) => Some(UInt16),
        (UInt8, _) | (_, UInt8) => Some(UInt8),
        _ => None,
    }
}

fn get_comparison_common_decimal_type(
    decimal_type: &DataType,
    other_type: &DataType,
) -> Option<DataType> {
    let other_decimal_type = &match other_type {
        // This conversion rule is from spark
        // https://github.com/apache/spark/blob/1c81ad20296d34f137238dadd67cc6ae405944eb/sql/catalyst/src/main/scala/org/apache/spark/sql/types/DecimalType.scala#L127
        DataType::Int8 => DataType::Decimal(3, 0),
        DataType::Int16 => DataType::Decimal(5, 0),
        DataType::Int32 => DataType::Decimal(10, 0),
        DataType::Int64 => DataType::Decimal(20, 0),
        DataType::Float32 => DataType::Decimal(14, 7),
        DataType::Float64 => DataType::Decimal(30, 15),
        _ => {
            return None;
        }
    };
    match (decimal_type, &other_decimal_type) {
        (DataType::Decimal(p1, s1), DataType::Decimal(p2, s2)) => {
            let new_precision = p1.max(p2);
            let new_scale = s1.max(s2);
            Some(DataType::Decimal(*new_precision, *new_scale))
        }
        _ => None,
    }
}

// Convert the numeric data type to the decimal data type.
// Now, we just support the signed integer type and floating-point type.
fn coerce_numeric_type_to_decimal(numeric_type: &DataType) -> Option<DataType> {
    match numeric_type {
        DataType::Int8 => Some(DataType::Decimal(3, 0)),
        DataType::Int16 => Some(DataType::Decimal(5, 0)),
        DataType::Int32 => Some(DataType::Decimal(10, 0)),
        DataType::Int64 => Some(DataType::Decimal(20, 0)),
        // TODO if we convert the floating-point data to the decimal type, it maybe overflow.
        DataType::Float32 => Some(DataType::Decimal(14, 7)),
        DataType::Float64 => Some(DataType::Decimal(30, 15)),
        _ => None,
    }
}

fn mathematics_numerical_coercion(
    mathematics_op: &Operator,
    lhs_type: &DataType,
    rhs_type: &DataType,
) -> Option<DataType> {
    use arrow::datatypes::DataType::*;

    // error on any non-numeric type
    if !is_numeric(lhs_type) || !is_numeric(rhs_type) {
        return None;
    };

    // same type => all good
    if lhs_type == rhs_type {
        return Some(lhs_type.clone());
    }

    // these are ordered from most informative to least informative so
    // that the coercion removes the least amount of information
    match (lhs_type, rhs_type) {
        (Decimal(_, _), Decimal(_, _)) => {
            coercion_decimal_mathematics_type(mathematics_op, lhs_type, rhs_type)
        }
        (Decimal(_, _), _) => {
            let converted_decimal_type = coerce_numeric_type_to_decimal(rhs_type);
            match converted_decimal_type {
                None => None,
                Some(right_decimal_type) => coercion_decimal_mathematics_type(
                    mathematics_op,
                    lhs_type,
                    &right_decimal_type,
                ),
            }
        }
        (_, Decimal(_, _)) => {
            let converted_decimal_type = coerce_numeric_type_to_decimal(lhs_type);
            match converted_decimal_type {
                None => None,
                Some(left_decimal_type) => coercion_decimal_mathematics_type(
                    mathematics_op,
                    &left_decimal_type,
                    rhs_type,
                ),
            }
        }
        (Float64, _) | (_, Float64) => Some(Float64),
        (_, Float32) | (Float32, _) => Some(Float32),
        (Int64, _) | (_, Int64) => Some(Int64),
        (Int32, _) | (_, Int32) => Some(Int32),
        (Int16, _) | (_, Int16) => Some(Int16),
        (Int8, _) | (_, Int8) => Some(Int8),
        (UInt64, _) | (_, UInt64) => Some(UInt64),
        (UInt32, _) | (_, UInt32) => Some(UInt32),
        (UInt16, _) | (_, UInt16) => Some(UInt16),
        (UInt8, _) | (_, UInt8) => Some(UInt8),
        _ => None,
    }
}

fn create_decimal_type(precision: usize, scale: usize) -> DataType {
    DataType::Decimal(
        MAX_PRECISION_FOR_DECIMAL128.min(precision),
        MAX_SCALE_FOR_DECIMAL128.min(scale),
    )
}

fn coercion_decimal_mathematics_type(
    mathematics_op: &Operator,
    left_decimal_type: &DataType,
    right_decimal_type: &DataType,
) -> Option<DataType> {
    use arrow::datatypes::DataType::*;
    match (left_decimal_type, right_decimal_type) {
        // The coercion rule from spark
        // https://github.com/apache/spark/blob/c20af535803a7250fef047c2bf0fe30be242369d/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/analysis/DecimalPrecision.scala#L35
        (Decimal(p1, s1), Decimal(p2, s2)) => {
            match mathematics_op {
                Operator::Plus | Operator::Minus => {
                    // max(s1, s2)
                    let result_scale = *s1.max(s2);
                    // max(s1, s2) + max(p1-s1, p2-s2) + 1
                    let result_precision = result_scale + (*p1 - *s1).max(*p2 - *s2) + 1;
                    Some(create_decimal_type(result_precision, result_scale))
                }
                Operator::Multiply => {
                    // s1 + s2
                    let result_scale = *s1 + *s2;
                    // p1 + p2 + 1
                    let result_precision = *p1 + *p2 + 1;
                    Some(create_decimal_type(result_precision, result_scale))
                }
                Operator::Divide => {
                    // max(6, s1 + p2 + 1)
                    let result_scale = 6.max(*s1 + *p2 + 1);
                    // p1 - s1 + s2 + max(6, s1 + p2 + 1)
                    let result_precision = result_scale + *p1 - *s1 + *s2;
                    Some(create_decimal_type(result_precision, result_scale))
                }
                Operator::Modulo => {
                    // max(s1, s2)
                    let result_scale = *s1.max(s2);
                    // min(p1-s1, p2-s2) + max(s1, s2)
                    let result_precision = result_scale + (*p1 - *s1).min(*p2 - *s2);
                    Some(create_decimal_type(result_precision, result_scale))
                }
                _ => unreachable!(),
            }
        }
        _ => unreachable!(),
    }
}

/// Determine if a DataType is signed numeric or not
pub fn is_signed_numeric(dt: &DataType) -> bool {
    matches!(
        dt,
        DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::Float16
            | DataType::Float32
            | DataType::Float64
            | DataType::Decimal(_, _)
    )
}

/// Determine if a DataType is numeric or not
pub fn is_numeric(dt: &DataType) -> bool {
    is_signed_numeric(dt)
        || match dt {
            DataType::UInt8 | DataType::UInt16 | DataType::UInt32 | DataType::UInt64 => {
                true
            }
            _ => false,
        }
}

/// Coercion rules for dictionary values (aka the type of the  dictionary itself)
fn dictionary_value_coercion(
    lhs_type: &DataType,
    rhs_type: &DataType,
) -> Option<DataType> {
    numerical_coercion(lhs_type, rhs_type).or_else(|| string_coercion(lhs_type, rhs_type))
}

/// Coercion rules for Dictionaries: the type that both lhs and rhs
/// can be casted to for the purpose of a computation.
///
/// It would likely be preferable to cast primitive values to
/// dictionaries, and thus avoid unpacking dictionary as well as doing
/// faster comparisons. However, the arrow compute kernels (e.g. eq)
/// don't have DictionaryArray support yet, so fall back to unpacking
/// the dictionaries
fn dictionary_coercion(lhs_type: &DataType, rhs_type: &DataType) -> Option<DataType> {
    match (lhs_type, rhs_type) {
        (
            DataType::Dictionary(_lhs_index_type, lhs_value_type),
            DataType::Dictionary(_rhs_index_type, rhs_value_type),
        ) => dictionary_value_coercion(lhs_value_type, rhs_value_type),
        (DataType::Dictionary(_index_type, value_type), _) => {
            dictionary_value_coercion(value_type, rhs_type)
        }
        (_, DataType::Dictionary(_index_type, value_type)) => {
            dictionary_value_coercion(lhs_type, value_type)
        }
        _ => None,
    }
}

/// Coercion rules for Strings: the type that both lhs and rhs can be
/// casted to for the purpose of a string computation
fn string_coercion(lhs_type: &DataType, rhs_type: &DataType) -> Option<DataType> {
    use arrow::datatypes::DataType::*;
    match (lhs_type, rhs_type) {
        (Utf8, Utf8) => Some(Utf8),
        (LargeUtf8, Utf8) => Some(LargeUtf8),
        (Utf8, LargeUtf8) => Some(LargeUtf8),
        (LargeUtf8, LargeUtf8) => Some(LargeUtf8),
        _ => None,
    }
}

/// coercion rules for like operations.
/// This is a union of string coercion rules and dictionary coercion rules
fn like_coercion(lhs_type: &DataType, rhs_type: &DataType) -> Option<DataType> {
    string_coercion(lhs_type, rhs_type)
        .or_else(|| dictionary_coercion(lhs_type, rhs_type))
}

/// Coercion rules for Temporal columns: the type that both lhs and rhs can be
/// casted to for the purpose of a date computation
fn temporal_coercion(lhs_type: &DataType, rhs_type: &DataType) -> Option<DataType> {
    use arrow::datatypes::DataType::*;
    use arrow::datatypes::TimeUnit;
    match (lhs_type, rhs_type) {
        (Utf8, Date32) => Some(Date32),
        (Date32, Utf8) => Some(Date32),
        (Utf8, Date64) => Some(Date64),
        (Date64, Utf8) => Some(Date64),
        (Timestamp(lhs_unit, lhs_tz), Timestamp(rhs_unit, rhs_tz)) => {
            let tz = match (lhs_tz, rhs_tz) {
                // can't cast across timezones
                (Some(lhs_tz), Some(rhs_tz)) => {
                    if lhs_tz != rhs_tz {
                        return None;
                    } else {
                        Some(lhs_tz.clone())
                    }
                }
                (Some(lhs_tz), None) => Some(lhs_tz.clone()),
                (None, Some(rhs_tz)) => Some(rhs_tz.clone()),
                (None, None) => None,
            };

            let unit = match (lhs_unit, rhs_unit) {
                (TimeUnit::Second, TimeUnit::Millisecond) => TimeUnit::Second,
                (TimeUnit::Second, TimeUnit::Microsecond) => TimeUnit::Second,
                (TimeUnit::Second, TimeUnit::Nanosecond) => TimeUnit::Second,
                (TimeUnit::Millisecond, TimeUnit::Second) => TimeUnit::Second,
                (TimeUnit::Millisecond, TimeUnit::Microsecond) => TimeUnit::Millisecond,
                (TimeUnit::Millisecond, TimeUnit::Nanosecond) => TimeUnit::Millisecond,
                (TimeUnit::Microsecond, TimeUnit::Second) => TimeUnit::Second,
                (TimeUnit::Microsecond, TimeUnit::Millisecond) => TimeUnit::Millisecond,
                (TimeUnit::Microsecond, TimeUnit::Nanosecond) => TimeUnit::Microsecond,
                (TimeUnit::Nanosecond, TimeUnit::Second) => TimeUnit::Second,
                (TimeUnit::Nanosecond, TimeUnit::Millisecond) => TimeUnit::Millisecond,
                (TimeUnit::Nanosecond, TimeUnit::Microsecond) => TimeUnit::Microsecond,
                (l, r) => {
                    assert_eq!(l, r);
                    l.clone()
                }
            };

            Some(Timestamp(unit, tz))
        }
        _ => None,
    }
}

pub(crate) fn is_dictionary(t: &DataType) -> bool {
    matches!(t, DataType::Dictionary(_, _))
}

/// Coercion rule for numerical types: The type that both lhs and rhs
/// can be casted to for numerical calculation, while maintaining
/// maximum precision
fn numerical_coercion(lhs_type: &DataType, rhs_type: &DataType) -> Option<DataType> {
    use arrow::datatypes::DataType::*;

    // error on any non-numeric type
    if !is_numeric(lhs_type) || !is_numeric(rhs_type) {
        return None;
    };

    // can't compare dictionaries directly due to
    // https://github.com/apache/arrow-rs/issues/1201
    if lhs_type == rhs_type && !is_dictionary(lhs_type) {
        // same type => all good
        return Some(lhs_type.clone());
    }

    // these are ordered from most informative to least informative so
    // that the coercion removes the least amount of information
    match (lhs_type, rhs_type) {
        (Float64, _) | (_, Float64) => Some(Float64),
        (_, Float32) | (Float32, _) => Some(Float32),
        (Int64, _) | (_, Int64) => Some(Int64),
        (Int32, _) | (_, Int32) => Some(Int32),
        (Int16, _) | (_, Int16) => Some(Int16),
        (Int8, _) | (_, Int8) => Some(Int8),
        (UInt64, _) | (_, UInt64) => Some(UInt64),
        (UInt32, _) | (_, UInt32) => Some(UInt32),
        (UInt16, _) | (_, UInt16) => Some(UInt16),
        (UInt8, _) | (_, UInt8) => Some(UInt8),
        _ => None,
    }
}

/// coercion rules for equality operations. This is a superset of all numerical coercion rules.
fn eq_coercion(lhs_type: &DataType, rhs_type: &DataType) -> Option<DataType> {
    // can't compare dictionaries directly due to
    // https://github.com/apache/arrow-rs/issues/1201
    if lhs_type == rhs_type && !is_dictionary(lhs_type) {
        // same type => equality is possible
        return Some(lhs_type.clone());
    }
    numerical_coercion(lhs_type, rhs_type)
        .or_else(|| dictionary_coercion(lhs_type, rhs_type))
        .or_else(|| temporal_coercion(lhs_type, rhs_type))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::DataType;
    use datafusion_common::DataFusionError;
    use datafusion_common::Result;
    use datafusion_expr::Operator;

    #[test]

    fn test_coercion_error() -> Result<()> {
        let result_type =
            coerce_types(&DataType::Float32, &Operator::Plus, &DataType::Utf8);

        if let Err(DataFusionError::Plan(e)) = result_type {
            assert_eq!(e, "'Float32 + Utf8' can't be evaluated because there isn't a common type to coerce the types to");
            Ok(())
        } else {
            Err(DataFusionError::Internal(
                "Coercion should have returned an DataFusionError::Internal".to_string(),
            ))
        }
    }

    #[test]
    fn test_decimal_binary_comparison_coercion() -> Result<()> {
        let input_decimal = DataType::Decimal(20, 3);
        let input_types = [
            DataType::Int8,
            DataType::Int16,
            DataType::Int32,
            DataType::Int64,
            DataType::Float32,
            DataType::Float64,
            DataType::Decimal(38, 10),
        ];
        let result_types = [
            DataType::Decimal(20, 3),
            DataType::Decimal(20, 3),
            DataType::Decimal(20, 3),
            DataType::Decimal(20, 3),
            DataType::Decimal(20, 7),
            DataType::Decimal(30, 15),
            DataType::Decimal(38, 10),
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
                let result_type = coerce_types(&input_decimal, &op, input_type)?;
                assert_eq!(expect_type, &result_type);
            }
        }
        // negative test
        let result_type = coerce_types(&input_decimal, &Operator::Eq, &DataType::Boolean);
        assert!(result_type.is_err());
        Ok(())
    }

    #[test]
    fn test_decimal_mathematics_op_type() {
        assert_eq!(
            coerce_numeric_type_to_decimal(&DataType::Int8).unwrap(),
            DataType::Decimal(3, 0)
        );
        assert_eq!(
            coerce_numeric_type_to_decimal(&DataType::Int16).unwrap(),
            DataType::Decimal(5, 0)
        );
        assert_eq!(
            coerce_numeric_type_to_decimal(&DataType::Int32).unwrap(),
            DataType::Decimal(10, 0)
        );
        assert_eq!(
            coerce_numeric_type_to_decimal(&DataType::Int64).unwrap(),
            DataType::Decimal(20, 0)
        );
        assert_eq!(
            coerce_numeric_type_to_decimal(&DataType::Float32).unwrap(),
            DataType::Decimal(14, 7)
        );
        assert_eq!(
            coerce_numeric_type_to_decimal(&DataType::Float64).unwrap(),
            DataType::Decimal(30, 15)
        );

        let op = Operator::Plus;
        let left_decimal_type = DataType::Decimal(10, 3);
        let right_decimal_type = DataType::Decimal(20, 4);
        let result = coercion_decimal_mathematics_type(
            &op,
            &left_decimal_type,
            &right_decimal_type,
        );
        assert_eq!(DataType::Decimal(21, 4), result.unwrap());
        let op = Operator::Minus;
        let result = coercion_decimal_mathematics_type(
            &op,
            &left_decimal_type,
            &right_decimal_type,
        );
        assert_eq!(DataType::Decimal(21, 4), result.unwrap());
        let op = Operator::Multiply;
        let result = coercion_decimal_mathematics_type(
            &op,
            &left_decimal_type,
            &right_decimal_type,
        );
        assert_eq!(DataType::Decimal(31, 7), result.unwrap());
        let op = Operator::Divide;
        let result = coercion_decimal_mathematics_type(
            &op,
            &left_decimal_type,
            &right_decimal_type,
        );
        assert_eq!(DataType::Decimal(35, 24), result.unwrap());
        let op = Operator::Modulo;
        let result = coercion_decimal_mathematics_type(
            &op,
            &left_decimal_type,
            &right_decimal_type,
        );
        assert_eq!(DataType::Decimal(11, 4), result.unwrap());
    }

    #[test]
    fn test_dictionary_type_coersion() {
        use DataType::*;

        // TODO: In the future, this would ideally return Dictionary types and avoid unpacking
        let lhs_type = Dictionary(Box::new(Int8), Box::new(Int32));
        let rhs_type = Dictionary(Box::new(Int8), Box::new(Int16));
        assert_eq!(dictionary_coercion(&lhs_type, &rhs_type), Some(Int32));

        let lhs_type = Dictionary(Box::new(Int8), Box::new(Utf8));
        let rhs_type = Dictionary(Box::new(Int8), Box::new(Int16));
        assert_eq!(dictionary_coercion(&lhs_type, &rhs_type), None);

        let lhs_type = Dictionary(Box::new(Int8), Box::new(Utf8));
        let rhs_type = Utf8;
        assert_eq!(dictionary_coercion(&lhs_type, &rhs_type), Some(Utf8));

        let lhs_type = Utf8;
        let rhs_type = Dictionary(Box::new(Int8), Box::new(Utf8));
        assert_eq!(dictionary_coercion(&lhs_type, &rhs_type), Some(Utf8));
    }
}
