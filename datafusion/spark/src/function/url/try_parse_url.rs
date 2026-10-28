// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use crate::function::url::parse_url::{ParseUrl, spark_handled_parse_url};
use arrow::array::ArrayRef;
use arrow::datatypes::DataType;
use datafusion_common::Result;
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, TypeSignature,
    Volatility,
};
use datafusion_functions::utils::make_scalar_function;

/// TRY_PARSE_URL function for tolerant URL component extraction (never errors; returns NULL on invalid or missing parts).
/// <https://spark.apache.org/docs/latest/api/sql/index.html#try_parse_url>
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct TryParseUrl {
    signature: Signature,
}

impl Default for TryParseUrl {
    fn default() -> Self {
        Self::new()
    }
}

impl TryParseUrl {
    pub fn new() -> Self {
        Self {
            signature: Signature::one_of(
                vec![TypeSignature::String(2), TypeSignature::String(3)],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for TryParseUrl {
    fn name(&self) -> &str {
        "try_parse_url"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        let parse_url: ParseUrl = ParseUrl::new();
        parse_url.return_type(arg_types)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;
        make_scalar_function(spark_try_parse_url, vec![])(&args)
    }
}

fn spark_try_parse_url(args: &[ArrayRef]) -> Result<ArrayRef> {
    spark_handled_parse_url(args, |x| match x {
        Err(_) => Ok(None),
        result => result,
    })
}
