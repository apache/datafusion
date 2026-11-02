// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

use arrow::array::{BooleanArray, new_null_array};
use arrow::compute::kernels::zip::zip;
use arrow::compute::{and, is_not_null, is_null};
use arrow::datatypes::{DataType, Field, FieldRef};
use datafusion_common::metadata::coerced_fields_metadata;
use datafusion_common::{Result, exec_err, internal_err, plan_err};
use datafusion_expr::binary::try_type_union_resolution;
use datafusion_expr::conditional_expressions::CaseBuilder;
use datafusion_expr::expr::ScalarFunction;
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
        // Propagate field metadata (e.g. Arrow extension types) from the
        // arguments that can supply a value; see `coerced_fields_metadata`
        // for the merge semantics.
        let metadata =
            coerced_fields_metadata(args.arg_fields.iter().map(|f| f.as_ref()))?;
        Ok(metadata
            .add_to_field(Field::new(self.name(), return_type, nullable))
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

        // `coalesce(a, b)` is rewritten to the (lazy, fast)
        // `CASE WHEN a IS NOT NULL THEN a ELSE b END`, which names every argument
        // but the last one twice. A volatile argument named twice is evaluated
        // twice, so the rewrite stops at the first volatile non-final argument
        // and the rest is left to the `invoke_with_args` kernel, which evaluates
        // each argument once. See https://github.com/apache/datafusion/issues/25477
        let (init, last) = args.split_at(args.len() - 1);
        let (prefix, rest) = match init.iter().position(|arg| arg.is_volatile()) {
            None => (init, last[0].clone()),
            Some(0) => return Ok(ExprSimplifyResult::Original(args)),
            Some(i) => (
                &init[..i],
                Expr::ScalarFunction(ScalarFunction::new_udf(
                    super::coalesce(),
                    args[i..].to_vec(),
                )),
            ),
        };

        let whens = prefix.iter().map(|x| x.clone().is_not_null()).collect();
        Ok(ExprSimplifyResult::Simplified(
            CaseBuilder::new(None, whens, prefix.to_vec(), Some(Box::new(rest))).end()?,
        ))
    }

    /// coalesce evaluates to the first value which is not NULL
    ///
    /// Only reached when [`Self::simplify`] keeps the call because an argument
    /// is volatile; evaluates each argument exactly once.
    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> Result<ColumnarValue> {
        let args = args.args;
        // do not accept 0 arguments.
        if args.is_empty() {
            return exec_err!(
                "coalesce was called with {} arguments. It requires at least 1.",
                args.len()
            );
        }

        let return_type = args[0].data_type();
        let mut return_array = args.iter().filter_map(|x| match x {
            ColumnarValue::Array(array) => Some(array.len()),
            _ => None,
        });

        if let Some(size) = return_array.next() {
            // start with nulls as default output
            let mut current_value = new_null_array(&return_type, size);
            let mut remainder = BooleanArray::from(vec![true; size]);

            for arg in args {
                match arg {
                    ColumnarValue::Array(ref array) => {
                        let to_apply = and(&remainder, &is_not_null(array.as_ref())?)?;
                        current_value = zip(&to_apply, array, &current_value)?;
                        remainder = and(&remainder, &is_null(array)?)?;
                    }
                    ColumnarValue::Scalar(value) => {
                        if value.is_null() {
                            continue;
                        } else {
                            let last_value = value.to_scalar()?;
                            current_value = zip(&remainder, &last_value, &current_value)?;
                            break;
                        }
                    }
                }
                if remainder.true_count() == 0 {
                    break;
                }
            }
            Ok(ColumnarValue::Array(current_value))
        } else {
            let result = args
                .iter()
                .find_map(|x| match x {
                    ColumnarValue::Scalar(s) if !s.is_null() => Some(x.clone()),
                    _ => None,
                })
                .unwrap_or_else(|| args[0].clone());
            Ok(result)
        }
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
