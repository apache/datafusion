// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

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
