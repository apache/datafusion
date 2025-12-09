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

use arrow::datatypes::{DataType, Field, FieldRef};
use datafusion_common::{Result, internal_err, utils::take_function_args};
use datafusion_expr::{
    ColumnarValue, Documentation, Expr, ReturnFieldArgs, ScalarFunctionArgs,
    ScalarUDFImpl, Signature, Volatility,
    conditional_expressions::CaseBuilder,
    simplify::{ExprSimplifyResult, SimplifyInfo},
    type_coercion::binary::comparison_coercion,
};
use datafusion_macros::user_doc;

#[user_doc(
    doc_section(label = "Conditional Functions"),
    description = "Returns _expression2_ if _expression1_ is not NULL; otherwise it returns _expression3_.",
    syntax_example = "nvl2(expression1, expression2, expression3)",
    sql_example = r#"```sql
> select nvl2(null, 'a', 'b');
+--------------------------------+
| nvl2(NULL,Utf8("a"),Utf8("b")) |
+--------------------------------+
| b                              |
+--------------------------------+
> select nvl2('data', 'a', 'b');
+----------------------------------------+
| nvl2(Utf8("data"),Utf8("a"),Utf8("b")) |
+----------------------------------------+
| a                                      |
+----------------------------------------+
```
"#,
    argument(
        name = "expression1",
        description = "Expression to test for null. Can be a constant, column, or function, and any combination of operators."
    ),
    argument(
        name = "expression2",
        description = "Expression to return if expr1 is not null. Can be a constant, column, or function, and any combination of operators."
    ),
    argument(
        name = "expression3",
        description = "Expression to return if expr1 is null. Can be a constant, column, or function, and any combination of operators."
    )
)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct NVL2Func {
    signature: Signature,
}

impl Default for NVL2Func {
    fn default() -> Self {
        Self::new()
    }
}

impl NVL2Func {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for NVL2Func {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn name(&self) -> &str {
        "nvl2"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, arg_types: &[DataType]) -> Result<DataType> {
        Ok(arg_types[1].clone())
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        let nullable =
            args.arg_fields[1].is_nullable() || args.arg_fields[2].is_nullable();
        let return_type = args.arg_fields[1].data_type().clone();
        Ok(Field::new(self.name(), return_type, nullable).into())
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        internal_err!("nvl2 should have been simplified to case")
    }

    fn simplify(
        &self,
        args: Vec<Expr>,
        _info: &dyn SimplifyInfo,
    ) -> Result<ExprSimplifyResult> {
        let [test, if_non_null, if_null] = take_function_args(self.name(), args)?;

        let expr = CaseBuilder::new(
            None,
            vec![test.is_not_null()],
            vec![if_non_null],
            Some(Box::new(if_null)),
        )
        .end()?;

        Ok(ExprSimplifyResult::Simplified(expr))
    }

    fn short_circuits(&self) -> bool {
        true
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        let [tested, if_non_null, if_null] = take_function_args(self.name(), arg_types)?;
        let new_type =
            [if_non_null, if_null]
                .iter()
                .try_fold(tested.clone(), |acc, x| {
                    // The coerced types found by `comparison_coercion` are not guaranteed to be
                    // coercible for the arguments. `comparison_coercion` returns more loose
                    // types that can be coerced to both `acc` and `x` for comparison purpose.
                    // See `maybe_data_types` for the actual coercion.
                    let coerced_type = comparison_coercion(&acc, x);
                    if let Some(coerced_type) = coerced_type {
                        Ok(coerced_type)
                    } else {
                        internal_err!("Coercion from {acc} to {x} failed.")
                    }
                })?;
        Ok(vec![new_type; arg_types.len()])
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}
