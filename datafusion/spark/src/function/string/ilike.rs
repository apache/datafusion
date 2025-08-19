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

use arrow::array::ArrayRef;
use arrow::compute::ilike;
use arrow::datatypes::DataType;
use datafusion_common::{exec_err, Result};
use datafusion_expr::ColumnarValue;
use datafusion_expr::{ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility};
use datafusion_functions::utils::make_scalar_function;
use std::any::Any;
use std::sync::Arc;

/// ILIKE function for case-insensitive pattern matching
/// <https://spark.apache.org/docs/latest/api/sql/index.html#ilike>
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkILike {
    signature: Signature,
}

impl Default for SparkILike {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkILike {
    pub fn new() -> Self {
        Self {
            signature: Signature::string(2, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for SparkILike {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "ilike"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        make_scalar_function(spark_ilike, vec![])(&args.args)
    }
}

/// Returns true if str matches pattern (case insensitive).
pub fn spark_ilike(args: &[ArrayRef]) -> Result<ArrayRef> {
    if args.len() != 2 {
        return exec_err!("ilike function requires exactly 2 arguments");
    }

    let result = ilike(&args[0], &args[1])?;
    Ok(Arc::new(result))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::function::utils::test::test_scalar_function;
    use arrow::array::{Array, BooleanArray};
    use arrow::datatypes::DataType::Boolean;
    use datafusion_common::{Result, ScalarValue};
    use datafusion_expr::{ColumnarValue, ScalarUDFImpl};

    macro_rules! test_ilike_string_invoke {
        ($INPUT1:expr, $INPUT2:expr, $EXPECTED:expr) => {
            test_scalar_function!(
                SparkILike::new(),
                vec![
                    ColumnarValue::Scalar(ScalarValue::Utf8($INPUT1)),
                    ColumnarValue::Scalar(ScalarValue::Utf8($INPUT2))
                ],
                $EXPECTED,
                bool,
                Boolean,
                BooleanArray
            );

            test_scalar_function!(
                SparkILike::new(),
                vec![
                    ColumnarValue::Scalar(ScalarValue::LargeUtf8($INPUT1)),
                    ColumnarValue::Scalar(ScalarValue::LargeUtf8($INPUT2))
                ],
                $EXPECTED,
                bool,
                Boolean,
                BooleanArray
            );

            test_scalar_function!(
                SparkILike::new(),
                vec![
                    ColumnarValue::Scalar(ScalarValue::Utf8View($INPUT1)),
                    ColumnarValue::Scalar(ScalarValue::Utf8View($INPUT2))
                ],
                $EXPECTED,
                bool,
                Boolean,
                BooleanArray
            );
        };
    }

    #[test]
    fn test_ilike_invoke() -> Result<()> {
        test_ilike_string_invoke!(
            Some(String::from("Spark")),
            Some(String::from("_park")),
            Ok(Some(true))
        );
        test_ilike_string_invoke!(
            Some(String::from("Spark")),
            Some(String::from("_PARK")),
            Ok(Some(true))
        );
        test_ilike_string_invoke!(
            Some(String::from("SPARK")),
            Some(String::from("_park")),
            Ok(Some(true))
        );
        test_ilike_string_invoke!(
            Some(String::from("Spark")),
            Some(String::from("sp%")),
            Ok(Some(true))
        );
        test_ilike_string_invoke!(
            Some(String::from("Spark")),
            Some(String::from("SP%")),
            Ok(Some(true))
        );
        test_ilike_string_invoke!(
            Some(String::from("Spark")),
            Some(String::from("%ARK")),
            Ok(Some(true))
        );
        test_ilike_string_invoke!(
            Some(String::from("Spark")),
            Some(String::from("xyz")),
            Ok(Some(false))
        );
        test_ilike_string_invoke!(None, Some(String::from("_park")), Ok(None));
        test_ilike_string_invoke!(Some(String::from("Spark")), None, Ok(None));
        test_ilike_string_invoke!(None, None, Ok(None));

        Ok(())
    }
}
