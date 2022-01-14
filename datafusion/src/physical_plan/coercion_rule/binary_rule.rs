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

//! Support the coercion rule for binary operation

use crate::arrow::datatypes::DataType;
use crate::error::{DataFusionError, Result};
use crate::logical_plan::Operator;
use crate::physical_plan::expressions::coercion::{
    dictionary_coercion, eq_coercion, is_numeric, like_coercion, numerical_coercion,
    string_coercion, temporal_coercion,
};

/// Coercion rules for all binary operators. Returns the output type
/// of applying `op` to an argument of `lhs_type` and `rhs_type`.
pub(crate) fn coerce_types(
    lhs_type: &DataType,
    op: &Operator,
    rhs_type: &DataType,
) -> Result<DataType> {
    // This result MUST be compatible with `binary_coerce`
    let result = match op {
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
        // TODO: support decimal data type
        Operator::Plus
        | Operator::Minus
        | Operator::Modulo
        | Operator::Divide
        | Operator::Multiply => numerical_coercion(lhs_type, rhs_type),
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

fn comparison_eq_coercion(lhs_type: &DataType, rhs_type: &DataType) -> Option<DataType> {
    if lhs_type == rhs_type {
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
    if lhs_type == rhs_type {
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

#[cfg(test)]
mod tests {
    use crate::arrow::datatypes::DataType;
    use crate::error::{DataFusionError, Result};
    use crate::logical_plan::Operator;
    use crate::physical_plan::coercion_rule::binary_rule::coerce_types;

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
}
