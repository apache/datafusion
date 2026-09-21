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

use arrow::array::{BooleanArray, new_null_array};
use arrow::compute::kernels::zip::zip;
use arrow::compute::{and, is_not_null, is_null};
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
        Ok(Field::new(self.name(), return_type, nullable).into())
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

        // The `CASE WHEN a IS NOT NULL THEN a ELSE b END` rewrite below mentions
        // every non-final argument *twice* (once in the `WHEN` predicate and once
        // in the `THEN` result). That is fine for deterministic arguments, but for
        // a volatile argument the two mentions are two independent draws, so
        // `coalesce(random_nullable_expr, default)` can take the `THEN` branch
        // after the `WHEN` draw was non-null and still produce NULL.
        //
        // See https://github.com/apache/datafusion/issues/25477. For volatile
        // arguments we keep `coalesce` intact and let the runtime kernel in
        // `invoke_with_args` evaluate each argument exactly once.
        //
        // Only the non-final arguments are duplicated: the last argument becomes
        // the `ELSE` branch, which names it once, so a volatile last argument is
        // safe to rewrite and keeps its laziness.
        if args[..n - 1].iter().any(|arg| arg.is_volatile()) {
            return Ok(ExprSimplifyResult::Original(args));
        }

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
    ///
    /// This kernel is only reached when [`Self::simplify`] declined to rewrite the
    /// call into a `CASE` expression, which today only happens when one of the
    /// arguments is volatile. It evaluates each argument exactly once.
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
                if remainder.iter().all(|x| x == Some(false)) {
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Array, ArrayRef, Int32Array, StringArray};
    use arrow::datatypes::DataType;
    use datafusion_common::ScalarValue;
    use datafusion_common::config::ConfigOptions;
    use datafusion_expr::{ColumnarValue, Expr, ScalarUDFImpl, lit};

    use super::*;

    fn invoke(args: Vec<ColumnarValue>, number_rows: usize) -> ColumnarValue {
        let return_type = args[0].data_type();
        let arg_fields = args
            .iter()
            .enumerate()
            .map(|(i, a)| Field::new(format!("a{i}"), a.data_type(), true).into())
            .collect();
        CoalesceFunc::new()
            .invoke_with_args(ScalarFunctionArgs {
                args,
                arg_fields,
                number_rows,
                return_field: Field::new("f", return_type, true).into(),
                config_options: Arc::new(ConfigOptions::default()),
            })
            .expect("coalesce kernel failed")
    }

    /// The restored runtime kernel picks the first non-null value per row.
    #[test]
    fn coalesce_kernel_arrays() {
        let a: ArrayRef = Arc::new(Int32Array::from(vec![Some(1), None, None, None]));
        let b: ArrayRef = Arc::new(Int32Array::from(vec![Some(9), Some(2), None, None]));
        let c: ArrayRef =
            Arc::new(Int32Array::from(vec![Some(8), Some(8), Some(3), None]));

        let result = invoke(
            vec![
                ColumnarValue::Array(a),
                ColumnarValue::Array(b),
                ColumnarValue::Array(c),
            ],
            4,
        )
        .into_array(4)
        .unwrap();

        let expected: ArrayRef =
            Arc::new(Int32Array::from(vec![Some(1), Some(2), Some(3), None]));
        assert_eq!(&result, &expected);
    }

    /// A trailing non-null scalar fills every remaining row, so the output has no nulls.
    #[test]
    fn coalesce_kernel_array_with_scalar_fallback() {
        let a: ArrayRef = Arc::new(Int32Array::from(vec![Some(1), None, Some(3), None]));

        let result = invoke(
            vec![
                ColumnarValue::Array(a),
                ColumnarValue::Scalar(ScalarValue::Int32(Some(-1))),
            ],
            4,
        )
        .into_array(4)
        .unwrap();

        let expected: ArrayRef =
            Arc::new(Int32Array::from(vec![Some(1), Some(-1), Some(3), Some(-1)]));
        assert_eq!(&result, &expected);
        assert_eq!(result.null_count(), 0);
    }

    /// All-scalar input short-circuits to the first non-null scalar.
    #[test]
    fn coalesce_kernel_all_scalars() {
        let result = invoke(
            vec![
                ColumnarValue::Scalar(ScalarValue::Utf8(None)),
                ColumnarValue::Scalar(ScalarValue::Utf8(Some("datafusion".into()))),
                ColumnarValue::Scalar(ScalarValue::Utf8(Some("unused".into()))),
            ],
            1,
        );
        match result {
            ColumnarValue::Scalar(ScalarValue::Utf8(Some(s))) => {
                assert_eq!(s, "datafusion")
            }
            other => panic!("expected Utf8 scalar, got {other:?}"),
        }
    }

    /// All-null scalars produce a null scalar rather than an error.
    #[test]
    fn coalesce_kernel_all_null_scalars() {
        let result = invoke(
            vec![
                ColumnarValue::Scalar(ScalarValue::Int32(None)),
                ColumnarValue::Scalar(ScalarValue::Int32(None)),
            ],
            1,
        );
        match result {
            ColumnarValue::Scalar(v) => assert!(v.is_null()),
            other => panic!("expected null scalar, got {other:?}"),
        }
    }

    /// All-null arrays produce an all-null array of the right type and length.
    #[test]
    fn coalesce_kernel_all_null_arrays() {
        let a: ArrayRef = Arc::new(StringArray::from(vec![None as Option<&str>, None]));
        let b: ArrayRef = Arc::new(StringArray::from(vec![None as Option<&str>, None]));

        let result = invoke(vec![ColumnarValue::Array(a), ColumnarValue::Array(b)], 2)
            .into_array(2)
            .unwrap();

        assert_eq!(result.len(), 2);
        assert_eq!(result.null_count(), 2);
        assert_eq!(result.data_type(), &DataType::Utf8);
    }

    #[test]
    fn coalesce_kernel_rejects_empty_args() {
        let err = CoalesceFunc::new()
            .invoke_with_args(ScalarFunctionArgs {
                args: vec![],
                arg_fields: vec![],
                number_rows: 1,
                return_field: Field::new("f", DataType::Int32, true).into(),
                config_options: Arc::new(ConfigOptions::default()),
            })
            .unwrap_err();
        assert!(err.to_string().contains("It requires at least 1"));
    }

    fn simplify(args: Vec<Expr>) -> ExprSimplifyResult {
        CoalesceFunc::new()
            .simplify(args, &SimplifyContext::default())
            .expect("simplify failed")
    }

    /// Non-volatile arguments keep the lazy `CASE` rewrite.
    #[test]
    fn simplify_non_volatile_expands_to_case() {
        let result = simplify(vec![lit(1i64), lit(2i64)]);
        match result {
            ExprSimplifyResult::Simplified(Expr::Case(_)) => {}
            other => panic!("expected a CASE expression, got {other:?}"),
        }
    }

    /// A volatile argument must NOT be expanded, because the `CASE` rewrite would
    /// name it twice and evaluate it twice. See issue #25477.
    #[test]
    fn simplify_volatile_is_left_intact() {
        let random =
            datafusion_expr::expr::ScalarFunction::new_udf(crate::math::random(), vec![]);
        let args = vec![Expr::ScalarFunction(random), lit(-1i64)];
        match simplify(args.clone()) {
            ExprSimplifyResult::Original(original) => assert_eq!(original, args),
            other => panic!("expected the original coalesce args, got {other:?}"),
        }
    }

    /// A volatile argument nested inside a larger expression is also caught.
    #[test]
    fn simplify_nested_volatile_is_left_intact() {
        let random = Expr::ScalarFunction(
            datafusion_expr::expr::ScalarFunction::new_udf(crate::math::random(), vec![]),
        );
        let args = vec![random + lit(1.0f64), lit(-1.0f64)];
        match simplify(args.clone()) {
            ExprSimplifyResult::Original(original) => assert_eq!(original, args),
            other => panic!("expected the original coalesce args, got {other:?}"),
        }
    }

    /// A volatile *last* argument is named once (it becomes the `ELSE` branch),
    /// so the rewrite is safe and must still happen -- otherwise the other
    /// arguments needlessly lose their laziness.
    #[test]
    fn simplify_volatile_last_arg_still_expands_to_case() {
        let random = Expr::ScalarFunction(
            datafusion_expr::expr::ScalarFunction::new_udf(crate::math::random(), vec![]),
        );
        match simplify(vec![lit(1.0f64), random]) {
            ExprSimplifyResult::Simplified(Expr::Case(_)) => {}
            other => panic!("expected a CASE expression, got {other:?}"),
        }
    }

    /// A single argument is still unwrapped, volatile or not (it is named once).
    #[test]
    fn simplify_single_volatile_arg_is_unwrapped() {
        let random = Expr::ScalarFunction(
            datafusion_expr::expr::ScalarFunction::new_udf(crate::math::random(), vec![]),
        );
        match simplify(vec![random.clone()]) {
            ExprSimplifyResult::Simplified(e) => assert_eq!(e, random),
            other => panic!("expected the argument itself, got {other:?}"),
        }
    }
}
