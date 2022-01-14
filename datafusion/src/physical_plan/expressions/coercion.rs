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

//! Coercion rules used to coerce types to match existing expressions' implementations

use arrow::datatypes::DataType;

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
pub fn dictionary_coercion(lhs_type: &DataType, rhs_type: &DataType) -> Option<DataType> {
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
pub fn string_coercion(lhs_type: &DataType, rhs_type: &DataType) -> Option<DataType> {
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
pub fn like_coercion(lhs_type: &DataType, rhs_type: &DataType) -> Option<DataType> {
    string_coercion(lhs_type, rhs_type)
        .or_else(|| dictionary_coercion(lhs_type, rhs_type))
}

/// Coercion rules for Temporal columns: the type that both lhs and rhs can be
/// casted to for the purpose of a date computation
pub fn temporal_coercion(lhs_type: &DataType, rhs_type: &DataType) -> Option<DataType> {
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

/// Coercion rule for numerical types: The type that both lhs and rhs
/// can be casted to for numerical calculation, while maintaining
/// maximum precision
pub fn numerical_coercion(lhs_type: &DataType, rhs_type: &DataType) -> Option<DataType> {
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

// coercion rules for equality operations. This is a superset of all numerical coercion rules.
pub fn eq_coercion(lhs_type: &DataType, rhs_type: &DataType) -> Option<DataType> {
    if lhs_type == rhs_type {
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
