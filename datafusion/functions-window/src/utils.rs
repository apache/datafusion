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
use datafusion_common::{exec_err, DataFusionError, Result, ScalarValue};
use datafusion_physical_expr::expressions::Literal;
use datafusion_physical_expr_common::physical_expr::PhysicalExpr;
use std::sync::Arc;

pub(crate) fn get_signed_integer(value: ScalarValue) -> Result<i64> {
    if value.is_null() {
        return Ok(0);
    }

    if !value.data_type().is_integer() {
        return exec_err!("Expected an integer value");
    }

    value.cast_to(&DataType::Int64)?.try_into()
}

pub(crate) fn get_scalar_value_from_args(
    args: &[Arc<dyn PhysicalExpr>],
    index: usize,
) -> Result<Option<ScalarValue>> {
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

pub(crate) fn get_unsigned_integer(value: ScalarValue) -> Result<u64> {
    if value.is_null() {
        return Ok(0);
    }

    if !value.data_type().is_integer() {
        return exec_err!("Expected an integer value");
    }

    value.cast_to(&DataType::UInt64)?.try_into()
}
