// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Utility functions for the interval arithmetic library

use std::sync::Arc;

use crate::{
    expressions::{BinaryExpr, CastExpr, Column, Literal, NegativeExpr},
    PhysicalExpr,
};

use arrow_schema::{DataType, SchemaRef};
use datafusion_expr::Operator;

/// Indicates whether interval arithmetic is supported for the given expression.
/// Currently, we do not support all [`PhysicalExpr`]s for interval calculations.
/// We do not support every type of [`Operator`]s either. Over time, this check
/// will relax as more types of `PhysicalExpr`s and `Operator`s are supported.
/// Currently, [`CastExpr`], [`NegativeExpr`], [`BinaryExpr`], [`Column`] and [`Literal`] are supported.
pub fn check_support(expr: &Arc<dyn PhysicalExpr>, schema: &SchemaRef) -> bool {
    let expr_any = expr.as_any();
    if let Some(binary_expr) = expr_any.downcast_ref::<BinaryExpr>() {
        is_operator_supported(binary_expr.op())
            && check_support(binary_expr.left(), schema)
            && check_support(binary_expr.right(), schema)
    } else if let Some(column) = expr_any.downcast_ref::<Column>() {
        if let Ok(field) = schema.field_with_name(column.name()) {
            is_datatype_supported(field.data_type())
        } else {
            return false;
        }
    } else if let Some(literal) = expr_any.downcast_ref::<Literal>() {
        if let Ok(dt) = literal.data_type(schema) {
            is_datatype_supported(&dt)
        } else {
            return false;
        }
    } else if let Some(cast) = expr_any.downcast_ref::<CastExpr>() {
        check_support(cast.expr(), schema)
    } else if let Some(negative) = expr_any.downcast_ref::<NegativeExpr>() {
        check_support(negative.arg(), schema)
    } else {
        false
    }
}

/// Indicates whether interval arithmetic is supported for the given operator.
pub fn is_operator_supported(op: &Operator) -> bool {
    matches!(
        op,
        &Operator::Plus
            | &Operator::Minus
            | &Operator::And
            | &Operator::Gt
            | &Operator::GtEq
            | &Operator::Lt
            | &Operator::LtEq
            | &Operator::Eq
            | &Operator::Multiply
            | &Operator::Divide
    )
}

/// Indicates whether interval arithmetic is supported for the given data type.
pub fn is_datatype_supported(data_type: &DataType) -> bool {
    matches!(
        data_type,
        &DataType::Int64
            | &DataType::Int32
            | &DataType::Int16
            | &DataType::Int8
            | &DataType::UInt64
            | &DataType::UInt32
            | &DataType::UInt16
            | &DataType::UInt8
            | &DataType::Float64
            | &DataType::Float32
    )
}
