// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use crate::function::error_utils::unsupported_data_type_exec_err;
use arrow::array::{ArrayRef, AsArray};
use arrow::datatypes::{DataType, Float64Type};
use datafusion_common::utils::take_function_args;
use datafusion_common::{Result, ScalarValue};
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use std::sync::Arc;

/// <https://spark.apache.org/docs/latest/api/sql/index.html#expm1>
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkExpm1 {
    signature: Signature,
}

impl Default for SparkExpm1 {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkExpm1 {
    pub fn new() -> Self {
        Self {
            signature: Signature::exact(vec![DataType::Float64], Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SparkExpm1 {
    fn name(&self) -> &str {
        "expm1"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let [arg] = take_function_args(self.name(), args.args)?;
        match arg {
            ColumnarValue::Scalar(ScalarValue::Float64(value)) => Ok(
                ColumnarValue::Scalar(ScalarValue::Float64(value.map(|x| x.exp_m1()))),
            ),
            ColumnarValue::Array(array) => match array.data_type() {
                DataType::Float64 => Ok(ColumnarValue::Array(Arc::new(
                    array
                        .as_primitive::<Float64Type>()
                        .unary::<_, Float64Type>(|x| x.exp_m1()),
                )
                    as ArrayRef)),
                other => Err(unsupported_data_type_exec_err(
                    "expm1",
                    format!("{}", DataType::Float64).as_str(),
                    other,
                )),
            },
            other => Err(unsupported_data_type_exec_err(
                "expm1",
                format!("{}", DataType::Float64).as_str(),
                &other.data_type(),
            )),
        }
    }
}
