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

use datafusion_common::arrow::datatypes::DataType;
use datafusion_common::{exec_err, DataFusionError, ScalarValue};
use datafusion_physical_expr::expressions::Literal;
use datafusion_physical_expr_common::physical_expr::PhysicalExpr;
use std::sync::Arc;

pub(crate) fn get_signed_integer(value: ScalarValue) -> datafusion_common::Result<i64> {
    if value.is_null() {
        return Ok(0);
    }

    if !value.data_type().is_integer() {
        return exec_err!("Expected an integer value");
    }

    value.cast_to(&DataType::Int64)?.try_into()
}

pub(crate) fn get_casted_value(
    default_value: Option<ScalarValue>,
    dtype: &DataType,
) -> datafusion_common::Result<ScalarValue> {
    match default_value {
        Some(v) if !v.data_type().is_null() => v.cast_to(dtype),
        // If None or Null datatype
        _ => ScalarValue::try_from(dtype),
    }
}

/// Rewrites the NULL expression (1st argument) with an expression
/// which is the same data type as the default value (3rd argument).
/// Also rewrites the return type with the same data type as the
/// default value.
///
/// If a default value is not provided, or it is NULL the original
/// expression (1st argument) and return type is returned without
/// any modifications.
pub(crate) fn rewrite_null_expr_and_data_type(
    args: &[Arc<dyn PhysicalExpr>],
    expr_type: &DataType,
) -> datafusion_common::Result<(Arc<dyn PhysicalExpr>, DataType)> {
    assert!(!args.is_empty());
    let expr = Arc::clone(&args[0]);

    // The input expression and the return is type is unchanged
    // when the input expression is not NULL.
    if !expr_type.is_null() {
        return Ok((expr, expr_type.clone()));
    }

    get_scalar_value_from_args(args, 2)?
        .and_then(|value| {
            ScalarValue::try_from(value.data_type().clone())
                .map(|sv| {
                    Ok((
                        Arc::new(Literal::new(sv)) as Arc<dyn PhysicalExpr>,
                        value.data_type().clone(),
                    ))
                })
                .ok()
        })
        .unwrap_or(Ok((expr, expr_type.clone())))
}

pub(crate) fn get_scalar_value_from_args(
    args: &[Arc<dyn PhysicalExpr>],
    index: usize,
) -> datafusion_common::Result<Option<ScalarValue>> {
    Ok(if let Some(field) = args.get(index) {
        let tmp = field
            .as_any()
            .downcast_ref::<Literal>()
            .ok_or_else(|| DataFusionError::NotImplemented(
                format!("There is only support Literal types for field at idx: {index} in Window Function"),
            ))?
            .value()
            .clone();
        Some(tmp)
    } else {
        None
    })
}
