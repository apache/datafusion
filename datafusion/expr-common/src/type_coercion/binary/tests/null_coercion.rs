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
fn test_type_coercion_logical_op() -> Result<()> {
    test_coercion_binary_rule!(
        DataType::Boolean,
        DataType::Boolean,
        Operator::And,
        DataType::Boolean
    );

    test_coercion_binary_rule!(
        DataType::Boolean,
        DataType::Boolean,
        Operator::Or,
        DataType::Boolean
    );
    test_coercion_binary_rule!(
        DataType::Boolean,
        DataType::Null,
        Operator::And,
        DataType::Boolean
    );
    test_coercion_binary_rule!(
        DataType::Boolean,
        DataType::Null,
        Operator::Or,
        DataType::Boolean
    );
    test_coercion_binary_rule!(
        DataType::Null,
        DataType::Null,
        Operator::Or,
        DataType::Boolean
    );
    test_coercion_binary_rule!(
        DataType::Null,
        DataType::Null,
        Operator::And,
        DataType::Boolean
    );
    test_coercion_binary_rule!(
        DataType::Null,
        DataType::Boolean,
        Operator::And,
        DataType::Boolean
    );
    test_coercion_binary_rule!(
        DataType::Null,
        DataType::Boolean,
        Operator::Or,
        DataType::Boolean
    );
    Ok(())
}
