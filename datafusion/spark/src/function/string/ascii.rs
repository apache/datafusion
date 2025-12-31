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

use std::sync::Arc;

use arrow::datatypes::{DataType, Field, FieldRef};
use datafusion_common::types::{NativeType, logical_string};
use datafusion_common::{Result, internal_err};
use datafusion_expr::{
    Coercion, ColumnarValue, ReturnFieldArgs, ScalarFunctionArgs, ScalarUDFImpl,
    Signature, TypeSignatureClass, Volatility,
};
use datafusion_functions::string::ascii::ascii;
use datafusion_functions::utils::make_scalar_function;
use std::any::Any;

/// Spark compatible version of the [ascii] function. Differs from the [default ascii function]
/// in that it is more permissive of input types, for example casting numeric input to string
/// before executing the function (default version doesn't allow numeric input).
///
/// [ascii]: https://spark.apache.org/docs/latest/api/sql/index.html#ascii
/// [default ascii function]: datafusion_functions::string::ascii::AsciiFunc
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct SparkAscii {
    signature: Signature,
}

impl Default for SparkAscii {
    fn default() -> Self {
        Self::new()
    }
}

impl SparkAscii {
    pub fn new() -> Self {
        // Spark's ascii uses ImplicitCastInputTypes with StringType,
        // which allows numeric types to be implicitly cast to String.
        // See: https://github.com/apache/spark/blob/master/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/stringExpressions.scala
        let string_coercion = Coercion::new_implicit(
            TypeSignatureClass::Native(logical_string()),
            vec![TypeSignatureClass::Numeric],
            NativeType::String,
        );

        Self {
            signature: Signature::coercible(vec![string_coercion], Volatility::Immutable),
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
        internal_err!("return_field_from_args should be used instead")
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        // ascii returns an Int32 value
        // The result is nullable only if any of the input arguments is nullable
        let nullable = args.arg_fields.iter().any(|f| f.is_nullable());
        Ok(Arc::new(Field::new("ascii", DataType::Int32, nullable)))
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        make_scalar_function(ascii, vec![])(&args.args)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion_expr::ReturnFieldArgs;

    #[test]
    fn test_return_field_nullable_input() {
        let ascii_func = SparkAscii::new();
        let nullable_field = Arc::new(Field::new("input", DataType::Utf8, true));

        let result = ascii_func
            .return_field_from_args(ReturnFieldArgs {
                arg_fields: &[nullable_field],
                scalar_arguments: &[],
            })
            .unwrap();

        assert_eq!(result.data_type(), &DataType::Int32);
        assert!(
            result.is_nullable(),
            "Output should be nullable when input is nullable"
        );
    }

    #[test]
    fn test_return_field_non_nullable_input() {
        let ascii_func = SparkAscii::new();
        let non_nullable_field = Arc::new(Field::new("input", DataType::Utf8, false));

        let result = ascii_func
            .return_field_from_args(ReturnFieldArgs {
                arg_fields: &[non_nullable_field],
                scalar_arguments: &[],
            })
            .unwrap();

        assert_eq!(result.data_type(), &DataType::Int32);
        assert!(
            !result.is_nullable(),
            "Output should not be nullable when input is not nullable"
        );
    }
}
