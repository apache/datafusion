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

use arrow::array::{ArrayRef, ArrowNativeTypeOp, AsArray, BooleanArray};
use arrow::datatypes::DataType::{Boolean, Float16, Float32, Float64};
use arrow::datatypes::{DataType, Float16Type, Float32Type, Float64Type};

use datafusion_common::types::NativeType;
use datafusion_common::{Result, ScalarValue, exec_err};
use datafusion_expr::{Coercion, TypeSignatureClass};
use datafusion_expr::{
    ColumnarValue, Documentation, ScalarFunctionArgs, ScalarUDFImpl, Signature,
    Volatility,
};
use datafusion_macros::user_doc;

use crate::utils::make_scalar_function;

#[user_doc(
    doc_section(label = "Math Functions"),
    description = "Returns true if a given number is +0.0 or -0.0 otherwise returns false.",
    syntax_example = "iszero(numeric_expression)",
    sql_example = r#"```sql
> SELECT iszero(0);
+------------+
| iszero(0)  |
+------------+
| true       |
+------------+
```"#,
    standard_argument(name = "numeric_expression", prefix = "Numeric")
)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct IsZeroFunc {
    signature: Signature,
}

impl Default for IsZeroFunc {
    fn default() -> Self {
        IsZeroFunc::new()
    }
}

impl IsZeroFunc {
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

impl ScalarUDFImpl for IsZeroFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "iszero"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(Boolean)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        // Handle NULL input
        if args.args[0].data_type().is_null() {
            return Ok(ColumnarValue::Scalar(ScalarValue::Boolean(None)));
        }
        make_scalar_function(iszero, vec![])(&args.args)
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}

/// Iszero SQL function
fn iszero(args: &[ArrayRef]) -> Result<ArrayRef> {
    match args[0].data_type() {
        Float64 => Ok(Arc::new(BooleanArray::from_unary(
            args[0].as_primitive::<Float64Type>(),
            |x| x == 0.0,
        )) as ArrayRef),

        Float32 => Ok(Arc::new(BooleanArray::from_unary(
            args[0].as_primitive::<Float32Type>(),
            |x| x == 0.0,
        )) as ArrayRef),

        Float16 => Ok(Arc::new(BooleanArray::from_unary(
            args[0].as_primitive::<Float16Type>(),
            |x| x.is_zero(),
        )) as ArrayRef),

        other => exec_err!("Unsupported data type {other:?} for function iszero"),
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use arrow::array::{ArrayRef, Float32Array, Float64Array};

    use datafusion_common::cast::as_boolean_array;

    use crate::math::iszero::iszero;

    #[test]
    fn test_iszero_f64() {
        let args: Vec<ArrayRef> =
            vec![Arc::new(Float64Array::from(vec![1.0, 0.0, 3.0, -0.0]))];

        let result = iszero(&args).expect("failed to initialize function iszero");
        let booleans =
            as_boolean_array(&result).expect("failed to initialize function iszero");

        assert_eq!(booleans.len(), 4);
        assert!(!booleans.value(0));
        assert!(booleans.value(1));
        assert!(!booleans.value(2));
        assert!(booleans.value(3));
    }

    #[test]
    fn test_iszero_f32() {
        let args: Vec<ArrayRef> =
            vec![Arc::new(Float32Array::from(vec![1.0, 0.0, 3.0, -0.0]))];

        let result = iszero(&args).expect("failed to initialize function iszero");
        let booleans =
            as_boolean_array(&result).expect("failed to initialize function iszero");

        assert_eq!(booleans.len(), 4);
        assert!(!booleans.value(0));
        assert!(booleans.value(1));
        assert!(!booleans.value(2));
        assert!(booleans.value(3));
    }
}
