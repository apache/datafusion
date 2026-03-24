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

//! [`ArrowTryCastFunc`]: Implementation of the `arrow_try_cast`

use arrow::datatypes::{DataType, Field, FieldRef};
use arrow::error::ArrowError;
use datafusion_common::{
    Result, arrow_datafusion_err, datatype::DataTypeExt, exec_datafusion_err, exec_err,
    internal_err, types::logical_string, utils::take_function_args,
};
use std::any::Any;

use datafusion_expr::simplify::{ExprSimplifyResult, SimplifyContext};
use datafusion_expr::{
    Coercion, ColumnarValue, Documentation, Expr, ReturnFieldArgs, ScalarFunctionArgs,
    ScalarUDFImpl, Signature, TypeSignatureClass, Volatility,
};
use datafusion_macros::user_doc;

use super::arrow_cast::data_type_from_args;

/// Like [`arrow_cast`](super::arrow_cast::ArrowCastFunc) but returns NULL on cast failure instead of erroring.
///
/// This is implemented by simplifying `arrow_try_cast(expr, 'Type')` into
/// `Expr::TryCast` during optimization.
#[user_doc(
    doc_section(label = "Other Functions"),
    description = "Casts a value to a specific Arrow data type, returning NULL if the cast fails.",
    syntax_example = "arrow_try_cast(expression, datatype)",
    sql_example = r#"```sql
> select arrow_try_cast('123', 'Int64') as a,
         arrow_try_cast('not_a_number', 'Int64') as b;

+-----+------+
| a   | b    |
+-----+------+
| 123 | NULL |
+-----+------+
```"#,
    argument(
        name = "expression",
        description = "Expression to cast. The expression can be a constant, column, or function, and any combination of operators."
    ),
    argument(
        name = "datatype",
        description = "[Arrow data type](https://docs.rs/arrow/latest/arrow/datatypes/enum.DataType.html) name to cast to, as a string. The format is the same as that returned by [`arrow_typeof`]"
    )
)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct ArrowTryCastFunc {
    signature: Signature,
}

impl Default for ArrowTryCastFunc {
    fn default() -> Self {
        Self::new()
    }
}

impl ArrowTryCastFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::coercible(
                vec![
                    Coercion::new_exact(TypeSignatureClass::Any),
                    Coercion::new_exact(TypeSignatureClass::Native(logical_string())),
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl ScalarUDFImpl for ArrowTryCastFunc {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        "arrow_try_cast"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        internal_err!("return_field_from_args should be called instead")
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        // TryCast can always return NULL (on cast failure), so always nullable
        let [_, type_arg] = take_function_args(self.name(), args.scalar_arguments)?;

        type_arg
            .and_then(|sv| sv.try_as_str().flatten().filter(|s| !s.is_empty()))
            .map_or_else(
                || {
                    exec_err!(
                        "{} requires its second argument to be a non-empty constant string",
                        self.name()
                    )
                },
                |casted_type| match casted_type.parse::<DataType>() {
                    Ok(data_type) => {
                        Ok(Field::new(self.name(), data_type, true).into())
                    }
                    Err(ArrowError::ParseError(e)) => Err(exec_datafusion_err!("{e}")),
                    Err(e) => Err(arrow_datafusion_err!(e)),
                },
            )
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        internal_err!("arrow_try_cast should have been simplified to try_cast")
    }

    fn simplify(
        &self,
        mut args: Vec<Expr>,
        info: &SimplifyContext,
    ) -> Result<ExprSimplifyResult> {
        let target_type = data_type_from_args(self.name(), &args)?;
        // remove second (type) argument
        args.pop().unwrap();
        let arg = args.pop().unwrap();

        let source_type = info.get_data_type(&arg)?;
        let new_expr = if source_type == target_type {
            arg
        } else {
            Expr::TryCast(datafusion_expr::TryCast {
                expr: Box::new(arg),
                field: target_type.into_nullable_field_ref(),
            })
        };
        Ok(ExprSimplifyResult::Simplified(new_expr))
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}
