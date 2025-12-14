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
    as_binary_array, as_binary_view_array, as_fixed_size_binary_array,
    as_large_binary_array,
};
use datafusion_common::types::{NativeType, logical_string};
use datafusion_common::utils::take_function_args;
use datafusion_common::{Result, internal_err};
use datafusion_expr::{
    Coercion, ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    TypeSignatureClass, Volatility,
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
            signature: Signature::coercible(
                vec![Coercion::new_implicit(
                    TypeSignatureClass::Binary,
                    vec![TypeSignatureClass::Native(logical_string())],
                    NativeType::Binary,
                )],
                Volatility::Immutable,
            ),
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
}

fn spark_sha1_digest(value: &[u8]) -> String {
    let result = Sha1::digest(value);
    let mut s = String::with_capacity(result.len() * 2);
    for b in result.as_slice() {
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
    let [input] = take_function_args("sha1", args)?;

    match input.data_type() {
        DataType::Null => Ok(Arc::new(StringArray::new_null(input.len()))),
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
        DataType::FixedSizeBinary(_) => {
            let input = as_fixed_size_binary_array(input)?;
            Ok(spark_sha1_impl(input.iter()))
        }
        dt => {
            internal_err!("Unsupported data type for sha1: {dt}")
        }
    }
}
