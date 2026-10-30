// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use arrow::datatypes::{DataType, Field, FieldRef};
use datafusion_common::metadata::coerced_fields_metadata;
use datafusion_common::{Result, internal_err, plan_err, utils::take_function_args};
use datafusion_expr::{
    ColumnarValue, Documentation, Expr, ReturnFieldArgs, ScalarFunctionArgs,
    ScalarUDFImpl, Signature, Volatility,
    conditional_expressions::CaseBuilder,
    simplify::{ExprSimplifyResult, SimplifyContext},
    type_coercion::binary::type_union_coercion,
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
        // The result value comes from the second or third argument; propagate
        // field metadata (e.g. Arrow extension types) from them. The first
        // argument is only tested for NULL and does not contribute a value.
        let metadata =
            coerced_fields_metadata(args.arg_fields[1..3].iter().map(|f| f.as_ref()))?;
        Ok(metadata
            .add_to_field(Field::new(self.name(), return_type, nullable))
            .into())
    }

    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        internal_err!("nvl2 should have been simplified to case")
    }

    fn simplify(
        &self,
        args: Vec<Expr>,
        _info: &SimplifyContext,
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
        // `type_union_coercion` is order-sensitive (e.g. for timezones); start from
        // `if_null` to match `CASE`, which coerces starting from the ELSE type.
        let Some(new_type) = type_union_coercion(if_null, if_non_null) else {
            return plan_err!(
                "nvl2 cannot coerce {if_non_null} and {if_null} to a common type"
            );
        };

        Ok(vec![tested.clone(), new_type.clone(), new_type])
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.doc()
    }
}
