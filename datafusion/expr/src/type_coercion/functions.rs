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

use crate::{Signature, TypeSignature};
use arrow::{
    compute::can_cast_types,
    datatypes::{DataType, TimeUnit},
};
use datafusion_common::{DataFusionError, Result};

/// Performs type coercion for functions Returns the data types that
/// each argument must be coerced to match `signature`.
///
/// For more details on coercion in general, please see the
/// [`type_coercion`](datafusion_expr::type_coercion) module.
pub fn data_types(
    current_types: &[DataType],
    signature: &Signature,
) -> Result<Vec<DataType>> {
    if current_types.is_empty() {
        return Ok(vec![]);
    }
    let valid_types = get_valid_types(&signature.type_signature, current_types)?;

    if valid_types
        .iter()
        .any(|data_type| data_type == current_types)
    {
        return Ok(current_types.to_vec());
    }

    for valid_types in valid_types {
        if let Some(types) = maybe_data_types(&valid_types, current_types) {
            return Ok(types);
        }
    }

    // none possible -> Error
    Err(DataFusionError::Plan(format!(
        "Coercion from {:?} to the signature {:?} failed.",
        current_types, &signature.type_signature
    )))
}

fn get_valid_types(
    signature: &TypeSignature,
    current_types: &[DataType],
) -> Result<Vec<Vec<DataType>>> {
    let valid_types = match signature {
        TypeSignature::Variadic(valid_types) => valid_types
            .iter()
            .map(|valid_type| current_types.iter().map(|_| valid_type.clone()).collect())
            .collect(),
        TypeSignature::Uniform(number, valid_types) => valid_types
            .iter()
            .map(|valid_type| (0..*number).map(|_| valid_type.clone()).collect())
            .collect(),
        TypeSignature::VariadicEqual => {
            // one entry with the same len as current_types, whose type is `current_types[0]`.
            vec![current_types
                .iter()
                .map(|_| current_types[0].clone())
                .collect()]
        }
        TypeSignature::Exact(valid_types) => vec![valid_types.clone()],
        TypeSignature::Any(number) => {
            if current_types.len() != *number {
                return Err(DataFusionError::Plan(format!(
                    "The function expected {} arguments but received {}",
                    number,
                    current_types.len()
                )));
            }
            vec![(0..*number).map(|i| current_types[i].clone()).collect()]
        }
        TypeSignature::OneOf(types) => types
            .iter()
            .filter_map(|t| get_valid_types(t, current_types).ok())
            .flatten()
            .collect::<Vec<_>>(),
    };

    Ok(valid_types)
}

/// Try to coerce current_types into valid_types.
fn maybe_data_types(
    valid_types: &[DataType],
    current_types: &[DataType],
) -> Option<Vec<DataType>> {
    if valid_types.len() != current_types.len() {
        return None;
    }

    let mut new_type = Vec::with_capacity(valid_types.len());
    for (i, valid_type) in valid_types.iter().enumerate() {
        let current_type = &current_types[i];

        if current_type == valid_type {
            new_type.push(current_type.clone())
        } else {
            // attempt to coerce
            if can_coerce_from(valid_type, current_type) {
                new_type.push(valid_type.clone())
            } else {
                // not possible
                return None;
            }
        }
    }
    Some(new_type)
}

/// Return true if a value of type `type_from` can be coerced
/// (losslessly converted) into a value of `type_to`
///
/// See the module level documentation for more detail on coercion.
pub fn can_coerce_from(type_into: &DataType, type_from: &DataType) -> bool {
    use self::DataType::*;
    // Null can convert to most of types
    match type_into {
        Int8 => matches!(type_from, Null | Int8),
        Int16 => matches!(type_from, Null | Int8 | Int16 | UInt8),
        Int32 => matches!(type_from, Null | Int8 | Int16 | Int32 | UInt8 | UInt16),
        Int64 => matches!(
            type_from,
            Null | Int8 | Int16 | Int32 | Int64 | UInt8 | UInt16 | UInt32
        ),
        UInt8 => matches!(type_from, Null | UInt8),
        UInt16 => matches!(type_from, Null | UInt8 | UInt16),
        UInt32 => matches!(type_from, Null | UInt8 | UInt16 | UInt32),
        UInt64 => matches!(type_from, Null | UInt8 | UInt16 | UInt32 | UInt64),
        Float32 => matches!(
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
        ),
        Float64 => matches!(
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
        ),
        Timestamp(TimeUnit::Nanosecond, None) => {
            matches!(type_from, Null | Timestamp(_, None))
        }
        Utf8 | LargeUtf8 => true,
        Null => can_cast_types(type_from, type_into),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::DataType;

    #[test]
    fn test_maybe_data_types() {
        // this vec contains: arg1, arg2, expected result
        let cases = vec![
            // 2 entries, same values
            (
                vec![DataType::UInt8, DataType::UInt16],
                vec![DataType::UInt8, DataType::UInt16],
                Some(vec![DataType::UInt8, DataType::UInt16]),
            ),
            // 2 entries, can coerse values
            (
                vec![DataType::UInt16, DataType::UInt16],
                vec![DataType::UInt8, DataType::UInt16],
                Some(vec![DataType::UInt16, DataType::UInt16]),
            ),
            // 0 entries, all good
            (vec![], vec![], Some(vec![])),
            // 2 entries, can't coerce
            (
                vec![DataType::Boolean, DataType::UInt16],
                vec![DataType::UInt8, DataType::UInt16],
                None,
            ),
            // u32 -> u16 is possible
            (
                vec![DataType::Boolean, DataType::UInt32],
                vec![DataType::Boolean, DataType::UInt16],
                Some(vec![DataType::Boolean, DataType::UInt32]),
            ),
        ];

        for case in cases {
            assert_eq!(maybe_data_types(&case.0, &case.1), case.2)
        }
    }

    #[test]
    fn test_get_valid_types_one_of() -> Result<()> {
        let signature =
            TypeSignature::OneOf(vec![TypeSignature::Any(1), TypeSignature::Any(2)]);

        let invalid_types = get_valid_types(
            &signature,
            &[DataType::Int32, DataType::Int32, DataType::Int32],
        )?;
        assert_eq!(invalid_types.len(), 0);

        let args = vec![DataType::Int32, DataType::Int32];
        let valid_types = get_valid_types(&signature, &args)?;
        assert_eq!(valid_types.len(), 1);
        assert_eq!(valid_types[0], args);

        let args = vec![DataType::Int32];
        let valid_types = get_valid_types(&signature, &args)?;
        assert_eq!(valid_types.len(), 1);
        assert_eq!(valid_types[0], args);

        Ok(())
    }
}
