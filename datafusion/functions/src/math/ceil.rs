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
use std::sync::Arc;

use arrow::array::{ArrayRef, AsArray};
use arrow::compute::cast;
use arrow::datatypes::{
    DataType, Decimal128Type, Decimal256Type, Decimal32Type, Decimal64Type, Float32Type,
    Float64Type,
};
use datafusion_common::{exec_err, Result};
use datafusion_expr::interval_arithmetic::Interval;
use datafusion_expr::sort_properties::{ExprProperties, SortProperties};
use datafusion_expr::{
    Coercion, ColumnarValue, Documentation, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    TypeSignature, TypeSignatureClass, Volatility,
};
use datafusion_macros::user_doc;

use super::decimal::{apply_decimal_op, ceil_decimal_value};

#[user_doc(
    doc_section(label = "Math Functions"),
    description = "Returns the nearest integer greater than or equal to a number.",
    syntax_example = "ceil(numeric_expression)",
    standard_argument(name = "numeric_expression", prefix = "Numeric"),
    sql_example = r#"```sql
> SELECT ceil(3.14);
+------------+
| ceil(3.14) |
+------------+
| 4.0        |
+------------+
```"#
)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct CeilFunc {
    signature: Signature,
}

impl Default for CeilFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl CeilFunc {
    pub fn new() -> Self {
        let decimal_sig = Coercion::new_exact(TypeSignatureClass::Decimal);
        Self {
            signature: Signature::one_of(
                vec![
                    TypeSignature::Coercible(vec![decimal_sig]),
                    TypeSignature::Uniform(1, vec![DataType::Float64, DataType::Float32]),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for CeilFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "ceil"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        match arg_types[0] {
            DataType::Float32 => Ok(DataType::Float32),
            DataType::Decimal32(precision, scale) => {
                Ok(DataType::Decimal32(precision, scale))
            }
            DataType::Decimal64(precision, scale) => {
                Ok(DataType::Decimal64(precision, scale))
            }
            DataType::Decimal128(precision, scale) => {
                Ok(DataType::Decimal128(precision, scale))
            }
            DataType::Decimal256(precision, scale) => {
                Ok(DataType::Decimal256(precision, scale))
            }
            _ => Ok(DataType::Float64),
        }
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let args = ColumnarValue::values_to_arrays(&args.args)?;
        let value = &args[0];

        let result: ArrayRef = match value.data_type() {
            DataType::Float64 => Arc::new(
                value
                    .as_primitive::<Float64Type>()
                    .unary::<_, Float64Type>(f64::ceil),
            ),
            DataType::Float32 => Arc::new(
                value
                    .as_primitive::<Float32Type>()
                    .unary::<_, Float32Type>(f32::ceil),
            ),
            DataType::Null => cast(value.as_ref(), &DataType::Float64)?,
            DataType::Decimal32(_, scale) => apply_decimal_op::<Decimal32Type, _>(
                value,
                *scale,
                self.name(),
                ceil_decimal_value,
            )?,
            DataType::Decimal64(_, scale) => apply_decimal_op::<Decimal64Type, _>(
                value,
                *scale,
                self.name(),
                ceil_decimal_value,
            )?,
            DataType::Decimal128(_, scale) => apply_decimal_op::<Decimal128Type, _>(
                value,
                *scale,
                self.name(),
                ceil_decimal_value,
            )?,
            DataType::Decimal256(_, scale) => apply_decimal_op::<Decimal256Type, _>(
                value,
                *scale,
                self.name(),
                ceil_decimal_value,
            )?,
            other => {
                return exec_err!(
                    "Unsupported data type {other:?} for function {}",
                    self.name()
                )
            }
        };

        Ok(ColumnarValue::Array(result))
    }

    fn output_ordering(&self, input: &[ExprProperties]) -> Result<SortProperties> {
        Ok(input[0].sort_properties)
    }

    fn evaluate_bounds(&self, inputs: &[&Interval]) -> Result<Interval> {
        let data_type = inputs[0].data_type();
        Interval::make_unbounded(&data_type)
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}
