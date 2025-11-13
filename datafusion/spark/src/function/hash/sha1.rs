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

use std::any::Any;
use std::fmt::Write;
use std::sync::Arc;

use arrow::array::{ArrayRef, StringArray};
use arrow::datatypes::DataType;
use datafusion_common::cast::{
    as_binary_array, as_binary_view_array, as_large_binary_array,
};
use datafusion_common::{assert_eq_or_internal_err, exec_err, DataFusionError, Result};
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use datafusion_functions::utils::make_scalar_function;
use sha1::{Digest, Sha1};

/// <https://spark.apache.org/docs/latest/api/sql/index.html#sha1>
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkSha1 {
    signature: Signature,
    aliases: Vec<String>,
}

impl Default for SparkSha1 {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkSha1 {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
            aliases: vec!["sha".to_string()],
        }
    }
}

impl ScalarUDFImpl for SparkSha1 {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "sha1"
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Utf8)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        make_scalar_function(spark_sha1, vec![])(&args.args)
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.len() != 1 {
            return exec_err!(
                "`sha1` function requires 1 argument, got {}",
                arg_types.len()
            );
        }
        match arg_types[0] {
            DataType::Binary | DataType::LargeBinary | DataType::BinaryView => {
                Ok(vec![arg_types[0].clone()])
            }
            DataType::Utf8 | DataType::Utf8View => Ok(vec![DataType::Binary]),
            DataType::LargeUtf8 => Ok(vec![DataType::LargeBinary]),
            DataType::Null => Ok(vec![DataType::Binary]),
            _ => exec_err!("`sha1` function does not support type {}", arg_types[0]),
        }
    }
}

fn spark_sha1_digest(value: &[u8]) -> String {
    let result = Sha1::digest(value);
    let mut s = String::with_capacity(result.len() * 2);
    #[allow(deprecated)]
    for b in result.as_slice() {
        #[allow(clippy::unwrap_used)]
        write!(&mut s, "{b:02x}").unwrap();
    }
    s
}

fn spark_sha1_impl<'a>(input: impl Iterator<Item = Option<&'a [u8]>>) -> ArrayRef {
    let result = input
        .map(|value| value.map(spark_sha1_digest))
        .collect::<StringArray>();
    Arc::new(result)
}

fn spark_sha1(args: &[ArrayRef]) -> Result<ArrayRef> {
    let [input] = args else {
        assert_eq_or_internal_err!(
            args.len(),
            1,
            "Spark `sha1` function requires 1 argument"
        );
        unreachable!()
    };

    match input.data_type() {
        DataType::Binary => {
            let input = as_binary_array(input)?;
            Ok(spark_sha1_impl(input.iter()))
        }
        DataType::LargeBinary => {
            let input = as_large_binary_array(input)?;
            Ok(spark_sha1_impl(input.iter()))
        }
        DataType::BinaryView => {
            let input = as_binary_view_array(input)?;
            Ok(spark_sha1_impl(input.iter()))
        }
        _ => {
            exec_err!(
                "Spark `sha1` function: argument must be binary or large binary, got {:?}",
                input.data_type()
            )
        }
    }
}
