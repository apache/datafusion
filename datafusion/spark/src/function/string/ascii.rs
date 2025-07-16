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

use arrow::array::{ArrayAccessor, ArrayIter, ArrayRef, AsArray, Int32Array};
use arrow::datatypes::DataType;
use arrow::error::ArrowError;
use datafusion_common::{internal_err, plan_err, Result};
use datafusion_expr::ColumnarValue;
use datafusion_expr::{ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};
use datafusion_functions::utils::make_scalar_function;
use std::any::Any;
use std::sync::Arc;

/// <https://spark.apache.org/docs/latest/api/sql/index.html#ascii>
#[derive(Debug)]
pub struct SparkAscii {
    signature: Signature,
    aliases: Vec<String>,
}

impl Default for SparkAscii {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkAscii {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
            aliases: vec![],
        }
    }
}

impl ScalarUDFImpl for SparkAscii {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "ascii"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Int32)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        make_scalar_function(ascii, vec![])(&args.args)
    }

    fn aliases(&self) -> &[String] {
        &self.aliases
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.len() != 1 {
            return plan_err!(
                "The {} function requires 1 argument, but got {}.",
                self.name(),
                arg_types.len()
            );
        }
        Ok(vec![DataType::Utf8])
    }
}

fn calculate_ascii<'a, V>(array: V) -> Result<ArrayRef, ArrowError>
where
    V: ArrayAccessor<Item = &'a str>,
{
    let iter = ArrayIter::new(array);
    let result = iter
        .map(|string| {
            string.map(|s| {
                let mut chars = s.chars();
                chars.next().map_or(0, |v| v as i32)
            })
        })
        .collect::<Int32Array>();

    Ok(Arc::new(result) as ArrayRef)
}

/// Returns the numeric code of the first character of the argument.
pub fn ascii(args: &[ArrayRef]) -> Result<ArrayRef> {
    match args[0].data_type() {
        DataType::Utf8 => {
            let string_array = args[0].as_string::<i32>();
            Ok(calculate_ascii(string_array)?)
        }
        DataType::LargeUtf8 => {
            let string_array = args[0].as_string::<i64>();
            Ok(calculate_ascii(string_array)?)
        }
        DataType::Utf8View => {
            let string_array = args[0].as_string_view();
            Ok(calculate_ascii(string_array)?)
        }
        _ => internal_err!("Unsupported data type"),
    }
}

#[cfg(test)]
mod tests {
    use crate::function::string::ascii::SparkAscii;
    use crate::function::utils::test::test_scalar_function;
    use arrow::array::{Array, Int32Array};
    use arrow::datatypes::DataType::Int32;
    use datafusion_common::{Result, ScalarValue};
    use datafusion_expr::{ColumnarValue, ScalarUDFImpl};

    macro_rules! test_ascii_string_invoke {
        ($INPUT:expr, $EXPECTED:expr) => {
            test_scalar_function!(
                SparkAscii::new(),
                vec![ColumnarValue::Scalar(ScalarValue::Utf8($INPUT))],
                $EXPECTED,
                i32,
                Int32,
                Int32Array
            );

            test_scalar_function!(
                SparkAscii::new(),
                vec![ColumnarValue::Scalar(ScalarValue::LargeUtf8($INPUT))],
                $EXPECTED,
                i32,
                Int32,
                Int32Array
            );

            test_scalar_function!(
                SparkAscii::new(),
                vec![ColumnarValue::Scalar(ScalarValue::Utf8View($INPUT))],
                $EXPECTED,
                i32,
                Int32,
                Int32Array
            );
        };
    }

    #[test]
    fn test_ascii_invoke() -> Result<()> {
        test_ascii_string_invoke!(Some(String::from("x")), Ok(Some(120)));
        test_ascii_string_invoke!(Some(String::from("a")), Ok(Some(97)));
        test_ascii_string_invoke!(Some(String::from("")), Ok(Some(0)));
        test_ascii_string_invoke!(Some(String::from("\n")), Ok(Some(10)));
        test_ascii_string_invoke!(Some(String::from("\t")), Ok(Some(9)));
        test_ascii_string_invoke!(None, Ok(None));

        Ok(())
    }
}
