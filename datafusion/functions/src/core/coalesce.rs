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

use crate::utils::unanimous_metadata;
use arrow::datatypes::{DataType, Field, FieldRef};
use datafusion_common::{Result, exec_err, internal_err, plan_err};
use datafusion_expr::binary::try_type_union_resolution;
use datafusion_expr::conditional_expressions::CaseBuilder;
use datafusion_expr::simplify::{ExprSimplifyResult, SimplifyContext};
use datafusion_expr::{
    ColumnarValue, Documentation, Expr, ReturnFieldArgs, ScalarFunctionArgs,
};
use datafusion_expr::{ScalarUDFImpl, Signature, Volatility};
use datafusion_macros::user_doc;
use itertools::Itertools;

#[user_doc(
    doc_section(label = "Conditional Functions"),
    description = "Returns the first of its arguments that is not _null_. Returns _null_ if all arguments are _null_. This function is often used to substitute a default value for _null_ values.",
    syntax_example = "coalesce(expression1[, ..., expression_n])",
    sql_example = r#"```sql
> select coalesce(null, null, 'datafusion');
+----------------------------------------+
| coalesce(NULL,NULL,Utf8("datafusion")) |
+----------------------------------------+
| datafusion                             |
+----------------------------------------+
```"#,
    argument(
        name = "expression1, expression_n",
        description = "Expression to use if previous expressions are _null_. Can be a constant, column, or function, and any combination of arithmetic operators. Pass as many expression arguments as necessary."
    )
)]
#[derive(Debug, PartialEq, Eq, Hash)]
pub struct CoalesceFunc {
    pub(super) signature: Signature,
}

impl Default for CoalesceFunc {
    fn default() -> Self {
        CoalesceFunc::new()
    }
}

impl CoalesceFunc {
    pub fn new() -> Self {
        Self {
            signature: Signature::user_defined(Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for CoalesceFunc {
    fn name(&self) -> &str {
        "coalesce"
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        internal_err!("return_field_from_args should be called instead")
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> Result<FieldRef> {
        // If any the arguments in coalesce is non-null, the result is non-null
        let nullable = args.arg_fields.iter().all(|f| f.is_nullable());
        let return_type = args
            .arg_fields
            .iter()
            .map(|f| f.data_type())
            .find_or_first(|d| !d.is_null())
            .unwrap()
            .clone();
        // Propagate field metadata (e.g. Arrow extension types) only when the
        // arguments that can supply a value agree on it.
        let metadata = unanimous_metadata(args.arg_fields.iter().map(|f| f.as_ref()));
        Ok(Field::new(self.name(), return_type, nullable)
            .with_metadata(metadata)
            .into())
    }

    fn simplify(
        &self,
        args: Vec<Expr>,
        _info: &SimplifyContext,
    ) -> Result<ExprSimplifyResult> {
        if args.is_empty() {
            return plan_err!("coalesce must have at least one argument");
        }
        if args.len() == 1 {
            return Ok(ExprSimplifyResult::Simplified(
                args.into_iter().next().unwrap(),
            ));
        }

        let n = args.len();
        let (init, last_elem) = args.split_at(n - 1);
        let whens = init
            .iter()
            .map(|x| x.clone().is_not_null())
            .collect::<Vec<_>>();
        let cases = init.to_vec();
        Ok(ExprSimplifyResult::Simplified(
            CaseBuilder::new(None, whens, cases, Some(Box::new(last_elem[0].clone())))
                .end()?,
        ))
    }

    /// coalesce evaluates to the first value which is not NULL
    fn invoke_with_args(&self, _args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        internal_err!("coalesce should have been simplified to case")
    }

    fn conditional_arguments<'a>(
        &self,
        args: &'a [Expr],
    ) -> Option<(Vec<&'a Expr>, Vec<&'a Expr>)> {
        let eager = vec![&args[0]];
        let lazy = args[1..].iter().collect();
        Some((eager, lazy))
    }

    fn short_circuits(&self) -> bool {
        true
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> Result<Vec<DataType>> {
        if arg_types.is_empty() {
            return exec_err!("coalesce must have at least one argument");
        }

        try_type_union_resolution(arg_types)
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

    fn field(name: &str, dt: DataType, nullable: bool) -> FieldRef {
        Arc::new(Field::new(name, dt, nullable))
    }

    fn ext_field(name: &str, dt: DataType, extension_name: &str) -> FieldRef {
        Arc::new(Field::new(name, dt, true).with_metadata(HashMap::from([(
            EXTENSION_KEY.to_string(),
            extension_name.to_string(),
        )])))
    }

    fn return_field(arg_fields: &[FieldRef]) -> FieldRef {
        let scalars = vec![None; arg_fields.len()];
        CoalesceFunc::new()
            .return_field_from_args(ReturnFieldArgs {
                arg_fields,
                scalar_arguments: &scalars,
            })
            .unwrap()
    }

    #[test]
    fn propagates_metadata_when_arguments_agree() {
        let ret = return_field(&[
            ext_field("a", DataType::Binary, "geoarrow.wkb"),
            ext_field("b", DataType::Binary, "geoarrow.wkb"),
        ]);
        assert_eq!(ret.data_type(), &DataType::Binary);
        assert!(ret.is_nullable());
        assert_eq!(
            ret.metadata().get(EXTENSION_KEY).map(String::as_str),
            Some("geoarrow.wkb")
        );
    }

    #[test]
    fn null_literal_argument_does_not_block_metadata() {
        // An untyped NULL argument carries no metadata but cannot contribute
        // a typed value either, so it does not participate in the agreement.
        let ret = return_field(&[
            field("null", DataType::Null, true),
            ext_field("b", DataType::Binary, "geoarrow.wkb"),
        ]);
        assert_eq!(ret.data_type(), &DataType::Binary);
        assert_eq!(
            ret.metadata().get(EXTENSION_KEY).map(String::as_str),
            Some("geoarrow.wkb")
        );
    }

    #[test]
    fn drops_metadata_when_arguments_disagree() {
        let ret = return_field(&[
            ext_field("a", DataType::Binary, "geoarrow.wkb"),
            field("plain", DataType::Binary, true),
        ]);
        assert_eq!(ret.data_type(), &DataType::Binary);
        assert!(ret.metadata().is_empty());
    }

    #[test]
    fn no_metadata_when_all_arguments_are_null() {
        let ret = return_field(&[
            field("a", DataType::Null, true),
            field("b", DataType::Null, true),
        ]);
        assert_eq!(ret.data_type(), &DataType::Null);
        assert!(ret.metadata().is_empty());
    }

    #[test]
    fn nullability_is_unchanged_by_metadata_propagation() {
        let ret = return_field(&[
            ext_field("a", DataType::Binary, "geoarrow.wkb"),
            Arc::new(Field::new("b", DataType::Binary, false).with_metadata(
                HashMap::from([(EXTENSION_KEY.to_string(), "geoarrow.wkb".to_string())]),
            )),
        ]);
        assert!(!ret.is_nullable());
        assert_eq!(
            ret.metadata().get(EXTENSION_KEY).map(String::as_str),
            Some("geoarrow.wkb")
        );
    }
}
