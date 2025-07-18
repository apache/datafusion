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

// Common test macros

/// Tests that coercion for a binary operator between two types yields the expected result type for both sides.
///
/// Usage: test_coercion_binary_rule!(lhs_type, rhs_type, op, expected_type)
/// - lhs_type: The left-hand side data type
/// - rhs_type: The right-hand side data type
/// - op: The binary operator (e.g., "+", "-", etc.)
/// - expected_type: The type both sides should be coerced to
macro_rules! test_coercion_binary_rule {
    ($LHS_TYPE:expr, $RHS_TYPE:expr, $OP:expr, $RESULT_TYPE:expr) => {{
        let (lhs, rhs) =
            BinaryTypeCoercer::new(&$LHS_TYPE, &$OP, &$RHS_TYPE).get_input_types()?;
        assert_eq!(lhs, $RESULT_TYPE);
        assert_eq!(rhs, $RESULT_TYPE);
    }};
}

/// Tests that coercion for a binary operator between one type and multiple right-hand side types
/// yields the expected result type for both sides, in both lhs/rhs and rhs/lhs order.
///
/// Usage: test_coercion_binary_rule_multiple!(lhs_type, rhs_types, op, expected_type)
/// - lhs_type: The left-hand side data type
/// - rhs_types: An iterable of right-hand side data types
/// - op: The binary operator
/// - expected_type: The type both sides should be coerced to
macro_rules! test_coercion_binary_rule_multiple {
    ($LHS_TYPE:expr, $RHS_TYPES:expr, $OP:expr, $RESULT_TYPE:expr) => {{
        for rh_type in $RHS_TYPES {
            let (lhs, rhs) =
                BinaryTypeCoercer::new(&$LHS_TYPE, &$OP, &rh_type).get_input_types()?;
            assert_eq!(lhs, $RESULT_TYPE);
            assert_eq!(rhs, $RESULT_TYPE);

            BinaryTypeCoercer::new(&rh_type, &$OP, &$LHS_TYPE).get_input_types()?;
            assert_eq!(lhs, $RESULT_TYPE);
            assert_eq!(rhs, $RESULT_TYPE);
        }
    }};
}

/// Tests that the like_coercion function returns the expected result type for both lhs/rhs and rhs/lhs order.
///
/// Usage: test_like_rule!(lhs_type, rhs_type, expected_type)
/// - lhs_type: The left-hand side data type
/// - rhs_type: The right-hand side data type
/// - expected_type: The expected result type from like_coercion
macro_rules! test_like_rule {
    ($LHS_TYPE:expr, $RHS_TYPE:expr, $RESULT_TYPE:expr) => {{
        let result = like_coercion(&$LHS_TYPE, &$RHS_TYPE);
        assert_eq!(result, $RESULT_TYPE);
        let result = like_coercion(&$RHS_TYPE, &$LHS_TYPE);
        assert_eq!(result, $RESULT_TYPE);
    }};
}

mod arithmetic;
mod comparison;
mod dictionary;
mod null_coercion;
