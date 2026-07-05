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

//! Analyzer-time rewrite that captures each `arrays_zip` argument's natural
//! field name and pins it onto the UDF via
//! [`SparkArraysZip::with_field_names`]. Running here — before optimizer passes
//! that may rename `arg_fields` — is what keeps the names stable through
//! planning.

use std::any::Any;
use std::sync::Arc;

use datafusion::optimizer::analyzer::AnalyzerRule;
use datafusion_common::Result;
use datafusion_common::config::ConfigOptions;
use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_expr::expr::ScalarFunction;
use datafusion_expr::{Expr, LogicalPlan, ScalarUDF};

use super::arrays_zip::SparkArraysZip;

/// Rewrites `arrays_zip(arg1, arg2, ...)` so the result struct's field names
/// follow Spark semantics, by pinning them onto a
/// [`SparkArraysZip::with_field_names`] instance.
///
/// Only the crate's own SQL session needs this: a programmatic caller (e.g. a
/// Spark plan translator) already knows the names and calls `with_field_names`
/// directly. Here the names must be recovered from the SQL arguments instead.
///
/// Running as an analyzer rule — before any optimizer pass that renames
/// `arg_fields` — is what keeps the names stable: once pinned, the UDF reports
/// the same schema in planning and execution regardless of later renames.
///
/// `name_i` derivation:
/// * `Expr::Column(c)` / `Expr::OuterReferenceColumn(_, c)` → `c.name`
/// * `Expr::Alias { name, .. }` → `name`
/// * otherwise → the 0-based ordinal (`"0"`, `"1"`, ...)
///
/// Duplicate names are kept as-is, matching Spark (`arrays_zip(a, a)` →
/// `struct<a, a>`, whose fields are then inaccessible by name).
#[derive(Default, Debug)]
pub struct SparkArraysZipRewrite;

impl AnalyzerRule for SparkArraysZipRewrite {
    fn name(&self) -> &str {
        "spark_arrays_zip_rewrite"
    }

    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> Result<LogicalPlan> {
        plan.transform_up_with_subqueries(|plan| {
            let plan = plan.map_expressions(|expr| expr.transform_up(rewrite_expr))?;
            if plan.transformed {
                plan.data.recompute_schema().map(Transformed::yes)
            } else {
                Ok(plan)
            }
        })
        .map(|res| res.data)
    }
}

/// Pin Spark field names onto a single `arrays_zip` call. Any other expression
/// passes through unchanged.
fn rewrite_expr(expr: Expr) -> Result<Transformed<Expr>> {
    let Expr::ScalarFunction(sf) = &expr else {
        return Ok(Transformed::no(expr));
    };
    let any: &dyn Any = sf.func.inner().as_ref();
    let Some(spark) = any.downcast_ref::<SparkArraysZip>() else {
        return Ok(Transformed::no(expr));
    };
    if spark.field_names().is_some() {
        return Ok(Transformed::no(expr));
    }

    let Expr::ScalarFunction(sf) = expr else {
        unreachable!()
    };
    let names: Vec<String> = sf
        .args
        .iter()
        .enumerate()
        .map(|(i, e)| display_name_for(e, i))
        .collect();

    let func = Arc::new(ScalarUDF::new_from_impl(SparkArraysZip::with_field_names(
        names,
    )));
    Ok(Transformed::yes(Expr::ScalarFunction(
        ScalarFunction::new_udf(func, sf.args),
    )))
}

fn display_name_for(expr: &Expr, ordinal: usize) -> String {
    match expr {
        Expr::Column(c) | Expr::OuterReferenceColumn(_, c) => c.name.clone(),
        Expr::Alias(a) => a.name.clone(),
        _ => ordinal.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::function::array::arrays_zip;
    use arrow::datatypes::DataType;
    use datafusion_common::Column;
    use datafusion_expr::{ScalarUDF, ScalarUDFImpl, Signature, Volatility, lit};
    use datafusion_functions_nested::arrays_zip::ArraysZip;
    use std::sync::Arc;

    #[test]
    fn rewrites_column_refs_to_pinned_names() {
        let original = arrays_zip().call(vec![
            Expr::Column(Column::from_name("a")),
            Expr::Column(Column::from_name("b")),
        ]);

        let rewritten = run_rewrite(original).expect("transform succeeded");
        assert!(rewritten.transformed, "rewriter should have fired");

        assert_eq!(pinned_names(&rewritten.data), vec!["a", "b"]);
        let Expr::ScalarFunction(sf) = &rewritten.data else {
            panic!("expected ScalarFunction, got {:?}", rewritten.data);
        };
        assert_eq!(sf.args.len(), 2, "args must be left untouched");
    }

    #[test]
    fn falls_back_to_ordinals_for_expressions() {
        let original =
            arrays_zip().call(vec![lit(1i64), Expr::Column(Column::from_name("b"))]);

        let rewritten = run_rewrite(original).expect("transform succeeded");
        assert_eq!(pinned_names(&rewritten.data), vec!["0", "b"]);
    }

    #[test]
    fn skips_when_names_already_pinned() {
        let pinned = Arc::new(ScalarUDF::new_from_impl(
            SparkArraysZip::with_field_names(vec!["a".into(), "b".into()]),
        ));
        let already = Expr::ScalarFunction(ScalarFunction::new_udf(
            pinned,
            vec![
                Expr::Column(Column::from_name("a")),
                Expr::Column(Column::from_name("b")),
            ],
        ));

        let rewritten = run_rewrite(already).expect("transform succeeded");
        assert!(!rewritten.transformed, "second pass must not re-pin names");
    }

    #[test]
    fn ignores_other_functions() {
        let other = lit(1i64) + lit(2i64);
        let rewritten = run_rewrite(other.clone()).expect("transform succeeded");
        assert!(!rewritten.transformed);
        assert_eq!(rewritten.data, other);
    }

    #[test]
    fn ignores_inner_arrays_zip_with_same_name() {
        let inner_udf = Arc::new(ScalarUDF::new_from_impl(ArraysZip::new()));
        assert_eq!(inner_udf.name(), "arrays_zip");

        let inner_call = Expr::ScalarFunction(ScalarFunction::new_udf(
            inner_udf,
            vec![Expr::Column(Column::from_name("a"))],
        ));

        let rewritten = run_rewrite(inner_call.clone()).expect("transform succeeded");
        assert!(
            !rewritten.transformed,
            "must skip same-named UDF that is not our singleton"
        );
        assert_eq!(rewritten.data, inner_call);
    }

    #[test]
    fn ignores_foreign_udf_with_colliding_name() {
        #[derive(Debug, PartialEq, Eq, Hash)]
        struct ForeignArraysZip(Signature);
        impl ScalarUDFImpl for ForeignArraysZip {
            fn name(&self) -> &str {
                "arrays_zip"
            }
            fn signature(&self) -> &Signature {
                &self.0
            }
            fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
                Ok(DataType::Null)
            }
            fn invoke_with_args(
                &self,
                _args: datafusion_expr::ScalarFunctionArgs,
            ) -> Result<datafusion_expr::ColumnarValue> {
                unreachable!("not called in test")
            }
        }

        let foreign = Arc::new(ScalarUDF::new_from_impl(ForeignArraysZip(
            Signature::variadic_any(Volatility::Immutable),
        )));
        let call = Expr::ScalarFunction(ScalarFunction::new_udf(
            foreign,
            vec![Expr::Column(Column::from_name("a"))],
        ));

        let rewritten = run_rewrite(call.clone()).expect("transform succeeded");
        assert!(!rewritten.transformed);
        assert_eq!(rewritten.data, call);
    }

    /// Repeated columns keep their duplicate names, matching Spark — the
    /// rewrite does not de-duplicate.
    #[test]
    fn keeps_repeated_column_names() {
        let original = arrays_zip().call(vec![
            Expr::Column(Column::from_name("a")),
            Expr::Column(Column::from_name("a")),
            Expr::Column(Column::from_name("b")),
            Expr::Column(Column::from_name("a")),
        ]);

        let rewritten = run_rewrite(original).expect("transform succeeded");
        assert_eq!(pinned_names(&rewritten.data), vec!["a", "a", "b", "a"]);
    }

    fn run_rewrite(expr: Expr) -> Result<Transformed<Expr>> {
        expr.transform_up(rewrite_expr)
    }

    fn pinned_names(expr: &Expr) -> Vec<String> {
        let Expr::ScalarFunction(sf) = expr else {
            panic!("expected ScalarFunction, got {expr:?}");
        };
        let any: &dyn Any = sf.func.inner().as_ref();
        let spark = any
            .downcast_ref::<SparkArraysZip>()
            .expect("rewritten func must be SparkArraysZip");
        spark
            .field_names()
            .expect("names must be pinned after rewrite")
            .to_vec()
    }
}
