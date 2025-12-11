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

use arrow::array::ArrayRef;
use arrow::datatypes::DataType;

use datafusion_common::Result;
use datafusion_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use datafusion_functions::utils::make_scalar_function;

use crate::function::url::url_decode::{spark_handled_url_decode, UrlDecode};

#[derive(Debug, PartialEq, Eq, Hash)]
pub struct TryUrlDecode {
    signature: Signature,
    url_decoder: UrlDecode,
}

impl Default for TryUrlDecode {
    fn default() -> Self {
        Self::new()
    }
}

impl TryUrlDecode {
    pub fn new() -> Self {
        Self {
            signature: Signature::string(1, Volatility::Immutable),
            url_decoder: UrlDecode::new(),
        }
    }
}

impl ScalarUDFImpl for TryUrlDecode {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "try_url_decode"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        self.url_decoder.return_type(arg_types)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let ScalarFunctionArgs { args, .. } = args;
        make_scalar_function(spark_try_url_decode, vec![])(&args)
    }
}

fn spark_try_url_decode(args: &[ArrayRef]) -> Result<ArrayRef> {
    spark_handled_url_decode(args, |x| match x {
        Err(_) => Ok(None),
        result => result,
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::StringArray;
    use datafusion_common::{cast::as_string_array, Result};

    use super::*;

    #[test]
    fn test_try_decode_error_handled() -> Result<()> {
        let input = Arc::new(StringArray::from(vec![
            Some("http%3A%2F%2spark.apache.org"), // '%2s' is not a valid percent encoded character
            // Valid cases
            Some("https%3A%2F%2Fspark.apache.org"),
            None,
        ]));

        let expected =
            StringArray::from(vec![None, Some("https://spark.apache.org"), None]);

        let result = spark_try_url_decode(&[input as ArrayRef])?;
        let result = as_string_array(&result)?;

        assert_eq!(&expected, result);
        Ok(())
    }
}
