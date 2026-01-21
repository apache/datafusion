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

//! Math function: `isnan()`.

use arrow::datatypes::{DataType, Float16Type, Float32Type, Float64Type};
use datafusion_common::types::NativeType;
use datafusion_common::{Result, ScalarValue, exec_err};
use datafusion_expr::{Coercion, ColumnarValue, ScalarFunctionArgs, TypeSignatureClass};

use arrow::array::{ArrayRef, AsArray, BooleanArray};
use datafusion_expr::{Documentation, ScalarUDFImpl, Signature, Volatility};
use datafusion_macros::user_doc;
use std::any::Any;
use std::sync::Arc;

#[user_doc(
    doc_section(label = "Math Functions"),
    description = "Returns true if a given number is +NaN or -NaN otherwise returns false.",
    syntax_example = "isnan(numeric_expression)",
    sql_example = r#"```sql
> SELECT isnan(1);
+----------+
| isnan(1) |
+----------+
| false    |
+----------+
```"#,
    standard_argument(name = "numeric_expression", prefix = "Numeric")
)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct IsNanFunc {
    signature: Signature,
}

impl Default for IsNanFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl IsNanFunc {
    pub fn new() -> Self {
        // Accept any numeric type and coerce to float
        let float = Coercion::new_implicit(
            TypeSignatureClass::Float,
            vec![TypeSignatureClass::Numeric],
            NativeType::Float64,
        );
        Self {
            signature: Signature::coercible(vec![float], Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for IsNanFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "isnan"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        // Handle NULL input
        if args.args[0].data_type().is_null() {
            return Ok(ColumnarValue::Scalar(ScalarValue::Boolean(None)));
        }

        let args = ColumnarValue::values_to_arrays(&args.args)?;

        let arr: ArrayRef = match args[0].data_type() {
            DataType::Float64 => Arc::new(BooleanArray::from_unary(
                args[0].as_primitive::<Float64Type>(),
                f64::is_nan,
            )) as ArrayRef,

            DataType::Float32 => Arc::new(BooleanArray::from_unary(
                args[0].as_primitive::<Float32Type>(),
                f32::is_nan,
            )) as ArrayRef,

            DataType::Float16 => Arc::new(BooleanArray::from_unary(
                args[0].as_primitive::<Float16Type>(),
                |x| x.is_nan(),
            )) as ArrayRef,
            other => {
                return exec_err!(
                    "Unsupported data type {other:?} for function {}",
                    self.name()
                );
            }
        };
        Ok(ColumnarValue::Array(arr))
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}
