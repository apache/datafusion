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

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::Arc;

    const EXTENSION_KEY: &str = "ARROW:extension:name";

    #[test]
    fn propagates_metadata_from_value_arguments() {
        // The first argument is only tested for NULL; its metadata must not
        // leak into the result even when the value arguments agree.
        let fields = [
            Arc::new(Field::new("test", DataType::Binary, true).with_metadata(
                HashMap::from([(
                    EXTENSION_KEY.to_string(),
                    "unrelated.type".to_string(),
                )]),
            )),
            Arc::new(
                Field::new("if_non_null", DataType::Binary, true).with_metadata(
                    HashMap::from([(
                        EXTENSION_KEY.to_string(),
                        "geoarrow.wkb".to_string(),
                    )]),
                ),
            ),
            Arc::new(Field::new("if_null", DataType::Binary, true).with_metadata(
                HashMap::from([(EXTENSION_KEY.to_string(), "geoarrow.wkb".to_string())]),
            )),
        ];
        let ret = NVL2Func::new()
            .return_field_from_args(ReturnFieldArgs {
                arg_fields: &fields,
                scalar_arguments: &[None, None, None],
            })
            .unwrap();
        assert_eq!(ret.data_type(), &DataType::Binary);
        assert_eq!(
            ret.metadata().get(EXTENSION_KEY).map(String::as_str),
            Some("geoarrow.wkb")
        );
    }

    #[test]
    fn errors_when_value_arguments_have_conflicting_extension_types() {
        let fields = [
            Arc::new(Field::new("test", DataType::Boolean, true)),
            Arc::new(
                Field::new("if_non_null", DataType::Binary, true).with_metadata(
                    HashMap::from([(
                        EXTENSION_KEY.to_string(),
                        "geoarrow.wkb".to_string(),
                    )]),
                ),
            ),
            Arc::new(Field::new("if_null", DataType::Binary, true).with_metadata(
                HashMap::from([(
                    EXTENSION_KEY.to_string(),
                    "unrelated.type".to_string(),
                )]),
            )),
        ];
        let err = NVL2Func::new()
            .return_field_from_args(ReturnFieldArgs {
                arg_fields: &fields,
                scalar_arguments: &[None, None, None],
            })
            .unwrap_err();
        assert!(err.to_string().contains("geoarrow.wkb"), "{err}");
        assert!(err.to_string().contains("unrelated.type"), "{err}");
    }
}
