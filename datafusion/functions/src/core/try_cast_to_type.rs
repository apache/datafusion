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

//! [`TryCastToTypeFunc`]: Implementation of the `try_cast_to_type` function

use arrow::datatypes::{DataType, Field, FieldRef};
use datafusion_common::{
    Result, datatype::DataTypeExt, internal_err, utils::take_function_args,
};
use datafusion_expr::simplify::{ExprSimplifyResult, SimplifyContext};
use datafusion_expr::{
    Coercion, ColumnarValue, Documentation, Expr, ReturnFieldArgs, ScalarFunctionArgs,
    ScalarUDFImpl, Signature, TypeSignatureClass, Volatility,
};
use datafusion_macros::user_doc;

/// Like [`cast_to_type`](super::cast_to_type::CastToTypeFunc) but returns NULL
/// on cast failure instead of erroring.
///
/// This is implemented by simplifying `try_cast_to_type(expr, ref)` into
/// `Expr::TryCast` during optimization.
#[user_doc(
    doc_section(label = "Other Functions"),
    description = "Casts the first argument to the data type of the second argument, returning NULL if the cast fails. Only the type of the second argument is used; its value is ignored.",
    syntax_example = "try_cast_to_type(expression, reference)",
    sql_example = r#"```sql
> select try_cast_to_type('123', NULL::INTEGER) as a,
         try_cast_to_type('not_a_number', NULL::INTEGER) as b;

+-----+------+
| a   | b    |
+-----+------+
| 123 | NULL |
+-----+------+
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
pub struct TryCastToTypeFunc {
    signature: Signature,
}

impl Default for TryCastToTypeFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl TryCastToTypeFunc {
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

impl ScalarUDFImpl for TryCastToTypeFunc {
    fn name(&self) -> &str {
        "try_cast_to_type"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        internal_err!("return_field_from_args should be called instead")
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        // TryCast can always return NULL (on cast failure), so always nullable
        let [_, reference_field] = take_function_args(self.name(), args.arg_fields)?;
        let target_type = reference_field.data_type().clone();
        Ok(Field::new(self.name(), target_type, true).into())
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        internal_err!("try_cast_to_type should have been simplified to try_cast")
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
            source_arg
        } else {
            Expr::TryCast(datafusion_expr::TryCast {
                expr: Box::new(source_arg),
                field: target_type.into_nullable_field_ref(),
            })
        };
        Ok(ExprSimplifyResult::Simplified(new_expr))
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}
