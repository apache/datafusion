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

use arrow::array::{ArrayRef, OffsetSizeTrait, StringArray};
use arrow::datatypes::DataType;
use datafusion::logical_expr::{Coercion, ColumnarValue, Signature, TypeSignatureClass};
use datafusion_common::cast::{as_generic_string_array, as_string_view_array};
use datafusion_common::types::{NativeType, logical_string};
use datafusion_common::utils::take_function_args;
use datafusion_common::{Result, exec_err};
use datafusion_expr::{ScalarFunctionArgs, ScalarUDFImpl, Volatility};
use datafusion_functions::utils::make_scalar_function;

use std::sync::Arc;

/// Spark-compatible `quote` expression
/// <https://spark.apache.org/docs/latest/api/sql/index.html#quote>
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkQuote {
    signature: Signature,
}

impl Default for SparkQuote {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkQuote {
    pub fn new() -> Self {
        let str_coercion = Coercion::new_implicit(
            TypeSignatureClass::Native(logical_string()),
            vec![TypeSignatureClass::Any],
            NativeType::String,
        );
        Self {
            signature: Signature::coercible(vec![str_coercion], Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SparkQuote {
    fn name(&self) -> &str {
        "quote"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        match &arg_types[0] {
            DataType::LargeUtf8 => Ok(DataType::LargeUtf8),
            _ => Ok(DataType::Utf8),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        make_scalar_function(spark_quote_inner, vec![])(&args.args)
    }
}

fn spark_quote_inner(arg: &[ArrayRef]) -> Result<ArrayRef> {
    let [array] = take_function_args("quote", arg)?;
    match &array.data_type() {
        DataType::Utf8 => quote_array::<i32>(array),
        DataType::LargeUtf8 => quote_array::<i64>(array),
        DataType::Utf8View => quote_view(array),
        other => {
            exec_err!("unsupported data type {other:?} for function `quote`")
        }
    }
}

fn quote_array<T: OffsetSizeTrait>(array: &ArrayRef) -> Result<ArrayRef> {
    let str_array = as_generic_string_array::<T>(array)?;
    let result = str_array
        .iter()
        .map(|s| s.map(compute_quote))
        .collect::<StringArray>();
    Ok(Arc::new(result))
}

fn quote_view(str_view: &ArrayRef) -> Result<ArrayRef> {
    let str_array = as_string_view_array(str_view)?;
    let result = str_array
        .iter()
        .map(|opt_str| opt_str.map(compute_quote))
        .collect::<StringArray>();
    Ok(Arc::new(result) as ArrayRef)
}

const QUOTE_CHAR: char = '\'';
const ESCAPE_CHAR: char = '\\';

fn compute_quote(s: &str) -> String {
    let mut quoted = String::with_capacity(s.len() + 2);
    quoted.push(QUOTE_CHAR);
    for c in s.chars() {
        if c == QUOTE_CHAR {
            quoted.push(ESCAPE_CHAR);
        }
        quoted.push(c);
    }
    quoted.push(QUOTE_CHAR);
    quoted
}
