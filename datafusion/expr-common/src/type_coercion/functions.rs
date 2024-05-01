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

use std::sync::Arc;

use arrow::{
    compute::can_cast_types,
    datatypes::{DataType, TimeUnit},
};
use datafusion_common::utils::list_ndims;

use crate::{
    signature::{FIXED_SIZE_LIST_WILDCARD, TIMEZONE_WILDCARD},
    type_coercion::binary::comparison_binary_numeric_coercion,
};

/// Return true if a value of type `type_from` can be coerced
/// (losslessly converted) into a value of `type_to`
///
/// See the module level documentation for more detail on coercion.
pub fn can_coerce_from(type_into: &DataType, type_from: &DataType) -> bool {
    if type_into == type_from {
        return true;
    }
    if let Some(coerced) = coerced_from(type_into, type_from) {
        return coerced == *type_into;
    }
    false
}

fn coerced_from<'a>(
    type_into: &'a DataType,
    type_from: &'a DataType,
) -> Option<DataType> {
    use self::DataType::*;

    // match Dictionary first
    match (type_into, type_from) {
        // coerced dictionary first
        (_, Dictionary(_, value_type))
            if coerced_from(type_into, value_type).is_some() =>
        {
            Some(type_into.clone())
        }
        (Dictionary(_, value_type), _)
            if coerced_from(value_type, type_from).is_some() =>
        {
            Some(type_into.clone())
        }
        // coerced into type_into
        (Int8, _) if matches!(type_from, Null | Int8) => Some(type_into.clone()),
        (Int16, _) if matches!(type_from, Null | Int8 | Int16 | UInt8) => {
            Some(type_into.clone())
        }
        (Int32, _)
            if matches!(type_from, Null | Int8 | Int16 | Int32 | UInt8 | UInt16) =>
        {
            Some(type_into.clone())
        }
        (Int64, _)
            if matches!(
                type_from,
                Null | Int8 | Int16 | Int32 | Int64 | UInt8 | UInt16 | UInt32
            ) =>
        {
            Some(type_into.clone())
        }
        (UInt8, _) if matches!(type_from, Null | UInt8) => Some(type_into.clone()),
        (UInt16, _) if matches!(type_from, Null | UInt8 | UInt16) => {
            Some(type_into.clone())
        }
        (UInt32, _) if matches!(type_from, Null | UInt8 | UInt16 | UInt32) => {
            Some(type_into.clone())
        }
        (UInt64, _) if matches!(type_from, Null | UInt8 | UInt16 | UInt32 | UInt64) => {
            Some(type_into.clone())
        }
        (Float32, _)
            if matches!(
                type_from,
                Null | Int8
                    | Int16
                    | Int32
                    | Int64
                    | UInt8
                    | UInt16
                    | UInt32
                    | UInt64
                    | Float32
            ) =>
        {
            Some(type_into.clone())
        }
        (Float64, _)
            if matches!(
                type_from,
                Null | Int8
                    | Int16
                    | Int32
                    | Int64
                    | UInt8
                    | UInt16
                    | UInt32
                    | UInt64
                    | Float32
                    | Float64
                    | Decimal128(_, _)
            ) =>
        {
            Some(type_into.clone())
        }
        (Timestamp(TimeUnit::Nanosecond, None), _)
            if matches!(
                type_from,
                Null | Timestamp(_, None) | Date32 | Utf8 | LargeUtf8
            ) =>
        {
            Some(type_into.clone())
        }
        (Interval(_), _) if matches!(type_from, Utf8 | LargeUtf8) => {
            Some(type_into.clone())
        }
        // Any type can be coerced into strings
        (Utf8 | LargeUtf8, _) => Some(type_into.clone()),
        (Null, _) if can_cast_types(type_from, type_into) => Some(type_into.clone()),

        (List(_), _) if matches!(type_from, FixedSizeList(_, _)) => {
            Some(type_into.clone())
        }

        // Only accept list and largelist with the same number of dimensions unless the type is Null.
        // List or LargeList with different dimensions should be handled in TypeSignature or other places before this
        (List(_) | LargeList(_), _)
            if datafusion_common::utils::base_type(type_from).eq(&Null)
                || list_ndims(type_from) == list_ndims(type_into) =>
        {
            Some(type_into.clone())
        }
        // should be able to coerce wildcard fixed size list to non wildcard fixed size list
        (FixedSizeList(f_into, FIXED_SIZE_LIST_WILDCARD), _) => match type_from {
            FixedSizeList(f_from, size_from) => {
                match coerced_from(f_into.data_type(), f_from.data_type()) {
                    Some(data_type) if &data_type != f_into.data_type() => {
                        let new_field =
                            Arc::new(f_into.as_ref().clone().with_data_type(data_type));
                        Some(FixedSizeList(new_field, *size_from))
                    }
                    Some(_) => Some(FixedSizeList(f_into.clone(), *size_from)),
                    _ => None,
                }
            }
            _ => None,
        },

        (Timestamp(unit, Some(tz)), _) if tz.as_ref() == TIMEZONE_WILDCARD => {
            match type_from {
                Timestamp(_, Some(from_tz)) => {
                    Some(Timestamp(unit.clone(), Some(from_tz.clone())))
                }
                Null | Date32 | Utf8 | LargeUtf8 | Timestamp(_, None) => {
                    // In the absence of any other information assume the time zone is "+00" (UTC).
                    Some(Timestamp(unit.clone(), Some("+00".into())))
                }
                _ => None,
            }
        }
        (Timestamp(_, Some(_)), _)
            if matches!(
                type_from,
                Null | Timestamp(_, _) | Date32 | Utf8 | LargeUtf8
            ) =>
        {
            Some(type_into.clone())
        }
        // More coerce rules.
        // Note that not all rules in `comparison_coercion` can be reused here.
        // For example, all numeric types can be coerced into Utf8 for comparison,
        // but not for function arguments.
        _ => comparison_binary_numeric_coercion(type_into, type_from).and_then(
            |coerced_type| {
                if *type_into == coerced_type {
                    Some(coerced_type)
                } else {
                    None
                }
            },
        ),
    }
}
