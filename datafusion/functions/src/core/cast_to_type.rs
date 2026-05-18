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

//! [`CastToTypeFunc`]: Implementation of the `cast_to_type` function

use arrow::datatypes::{DataType, Field, FieldRef};
use datafusion_common::{Result, internal_err, utils::take_function_args};
use datafusion_expr::simplify::{ExprSimplifyResult, SimplifyContext};
use datafusion_expr::{
    Coercion, ColumnarValue, Documentation, Expr, ReturnFieldArgs, ScalarFunctionArgs,
    ScalarUDFImpl, Signature, TypeSignatureClass, Volatility,
};
use datafusion_macros::user_doc;

/// Casts the first argument to the data type of the second argument.
///
/// Only the type of the second argument is used; its value is ignored.
/// This is useful in macros or generic SQL where you need to preserve
/// or match types dynamically.
///
/// For example:
/// ```sql
/// select cast_to_type('42', NULL::INTEGER);
/// ```
#[user_doc(
    doc_section(label = "Other Functions"),
    description = "Casts the first argument to the data type of the second argument. Only the type of the second argument is used; its value is ignored.",
    syntax_example = "cast_to_type(expression, reference)",
    sql_example = r#"```sql
> select cast_to_type('42', NULL::INTEGER) as a;
+----+
| a  |
+----+
| 42 |
+----+

> select cast_to_type(1 + 2, NULL::DOUBLE) as b;
+-----+
| b   |
+-----+
| 3.0 |
+-----+
```"#,
    argument(
        name = "expression",
        description = "The expression to cast. It can be a constant, column, or function, and any combination of operators."
    ),
    argument(
        name = "reference",
        description = "Reference expression whose data type determines the target cast type. The value is ignored."
    )
)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct CastToTypeFunc {
    signature: Signature,
}

impl Default for CastToTypeFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl CastToTypeFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::coercible(
                vec![
                    Coercion::new_exact(TypeSignatureClass::Any),
                    Coercion::new_exact(TypeSignatureClass::Any),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for CastToTypeFunc {
    fn name(&self) -> &str {
        "cast_to_type"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        internal_err!("return_field_from_args should be called instead")
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let [source_field, reference_field] =
            take_function_args(self.name(), args.arg_fields)?;
        let target_type = reference_field.data_type().clone();
        // Nullability is inherited only from the first argument (the value
        // being cast).  The second argument is used solely for its type, so
        // its own nullability is irrelevant.  The one exception is when the
        // target type is Null – that type is inherently nullable.
        let nullable = source_field.is_nullable() || target_type == DataType::Null;
        Ok(Field::new(self.name(), target_type, nullable).into())
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        internal_err!("cast_to_type should have been simplified to cast")
    }

    fn simplify(
        &self,
        args: Vec<Expr>,
        info: &SimplifyContext,
    ) -> Result<ExprSimplifyResult> {
        let [source_arg, type_arg] = take_function_args(self.name(), args)?;
        let target_type = info.get_data_type(&type_arg)?;
        let source_type = info.get_data_type(&source_arg)?;
        let new_expr = if source_type == target_type {
            // the argument's data type is already the correct type
            source_arg
        } else {
            let nullable = info.nullable(&source_arg)? || target_type == DataType::Null;
            // Use an actual cast to get the correct type
            Expr::Cast(datafusion_expr::Cast {
                expr: Box::new(source_arg),
                field: Field::new("", target_type, nullable).into(),
            })
        };
        Ok(ExprSimplifyResult::Simplified(new_expr))
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}
