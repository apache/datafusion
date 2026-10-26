// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use std::sync::Arc;

use arrow::array::{ArrayRef, AsArray, Float64Array};
use arrow::compute::kernels::arity::binary;
use arrow::datatypes::{DataType, Float64Type};
use datafusion_common::Result;
use datafusion_common::utils::take_function_args;
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use datafusion_functions::utils::make_scalar_function;

/// Spark-compatible `hypot` function.
///
/// <https://spark.apache.org/docs/latest/api/sql/index.html#hypot>
///
/// Returns `sqrt(expr1^2 + expr2^2)` computed without intermediate overflow or
/// underflow, matching Spark's use of `java.lang.Math.hypot`.
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkHypot {
    signature: Signature,
}

impl Default for SparkHypot {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkHypot {
    pub fn new() -> Self {
        Self {
            // Spark only defines hypot over doubles
            signature: Signature::exact(
                vec![DataType::Float64, DataType::Float64],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for SparkHypot {
    fn name(&self) -> &str {
        "hypot"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Float64)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        make_scalar_function(spark_hypot, vec![])(&args.args)
    }
}

fn spark_hypot(args: &[ArrayRef]) -> Result<ArrayRef> {
    let [x, y] = take_function_args("hypot", args)?;

    let x = x.as_primitive::<Float64Type>();
    let y = y.as_primitive::<Float64Type>();
    let result: Float64Array = binary(x, y, |a, b| a.hypot(b))?;
    Ok(Arc::new(result))
}
