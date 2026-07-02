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
//! field name and embeds it as a marked `List<Utf8>` literal first argument.
//! Running here — before optimizer passes that may rename `arg_fields` —
//! is what keeps the names stable through planning.

use std::any::Any;
use std::collections::{BTreeMap, HashMap};

use arrow::datatypes::DataType;
use datafusion_common::config::ConfigOptions;
use datafusion_common::metadata::FieldMetadata;
use datafusion_common::tree_node::Transformed;
use datafusion_common::{DFSchema, Result, ScalarValue};
use datafusion_expr::Expr;
use datafusion_expr::expr_rewriter::FunctionRewrite;

use super::arrays_zip::SparkArraysZip;

/// Metadata key stamped on the injected names literal so we can recognize it.
pub(crate) const ARRAYS_ZIP_NAMES_KEY: &str = "datafusion.spark.arrays_zip.names";

/// Rewrites `arrays_zip(arg1, arg2, ...)` into
/// `arrays_zip([name1, ...], arg1, arg2, ...)` with the names literal marked.
///
/// `name_i` derivation:
/// * `Expr::Column(c)` / `Expr::OuterReferenceColumn(_, c)` → `c.name`
/// * `Expr::Alias { name, .. }` → `name`
/// * otherwise → the 0-based ordinal (`"0"`, `"1"`, ...)
///
/// Duplicates are then disambiguated with `_<n>` suffixes (`a, a` → `a, a_1`).
#[derive(Default, Debug)]
pub struct SparkArraysZipRewrite;

impl FunctionRewrite for SparkArraysZipRewrite {
    fn name(&self) -> &str {
        "spark_arrays_zip_rewrite"
    }

    fn rewrite(
        &self,
        expr: Expr,
        _schema: &DFSchema,
        _config: &ConfigOptions,
    ) -> Result<Transformed<Expr>> {
        let Expr::ScalarFunction(sf) = &expr else {
            return Ok(Transformed::no(expr));
        };
        if !is_spark_arrays_zip(sf.func.inner().as_ref()) {
            return Ok(Transformed::no(expr));
        }
        if first_arg_is_names_list(&sf.args) {
            return Ok(Transformed::no(expr));
        }

        let Expr::ScalarFunction(sf) = expr else {
            unreachable!()
        };
        let raw_names: Vec<String> = sf
            .args
            .iter()
            .enumerate()
            .map(|(i, e)| display_name_for(e, i))
            .collect();
        let names = dedup_names(raw_names);

        let mut new_args = Vec::with_capacity(sf.args.len() + 1);
        new_args.push(names_literal(&names));
        new_args.extend(sf.args);

        Ok(Transformed::yes(Expr::ScalarFunction(
            datafusion_expr::expr::ScalarFunction::new_udf(sf.func, new_args),
        )))
    }
}

/// Identity check via [`Any`] downcast. `Arc::ptr_eq` would fail for callers
/// that go through [`datafusion_expr::ScalarUDF::call`], which clones the UDF
/// into a fresh `Arc` instead of reusing the session-registry singleton.
fn is_spark_arrays_zip(udf: &dyn datafusion_expr::ScalarUDFImpl) -> bool {
    let any: &dyn Any = udf;
    any.downcast_ref::<SparkArraysZip>().is_some()
}

fn display_name_for(expr: &Expr, ordinal: usize) -> String {
    match expr {
        Expr::Column(c) | Expr::OuterReferenceColumn(_, c) => c.name.clone(),
        Expr::Alias(a) => a.name.clone(),
        _ => ordinal.to_string(),
    }
}

/// Disambiguate repeats with `_<n>` suffixes.
/// `["a", "a", "b", "a"]` → `["a", "a_1", "b", "a_2"]`.
pub(crate) fn dedup_names(names: Vec<String>) -> Vec<String> {
    let mut seen: HashMap<String, usize> = HashMap::new();
    names
        .into_iter()
        .map(|name| {
            let count = seen.entry(name.clone()).or_insert(0);
            let unique = if *count == 0 {
                name
            } else {
                format!("{name}_{count}")
            };
            *count += 1;
            unique
        })
        .collect()
}

/// True iff `args[0]` is a literal carrying our marker metadata. The metadata
/// check (not the value's type) prevents collision with a user-supplied
/// `List<Utf8>` first argument.
fn first_arg_is_names_list(args: &[Expr]) -> bool {
    let Some(Expr::Literal(_, Some(metadata))) = args.first() else {
        return false;
    };
    metadata.inner().contains_key(ARRAYS_ZIP_NAMES_KEY)
}

fn names_literal(names: &[String]) -> Expr {
    let scalars: Vec<ScalarValue> = names
        .iter()
        .map(|n| ScalarValue::Utf8(Some(n.clone())))
        .collect();
    let list =
        ScalarValue::new_list_from_iter(scalars.into_iter(), &DataType::Utf8, false);
    let metadata = FieldMetadata::new(BTreeMap::from([(
        ARRAYS_ZIP_NAMES_KEY.to_string(),
        "1".into(),
    )]));
    Expr::Literal(ScalarValue::List(list), Some(metadata))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::function::array::arrays_zip;
    use datafusion_common::Column;
    use datafusion_expr::{ScalarUDF, ScalarUDFImpl, Signature, Volatility, lit};
    use datafusion_functions_nested::arrays_zip::ArraysZip;
    use std::sync::Arc;

    /// `arrays_zip(a, b)` → 3 args, first is our marked names literal,
    /// names derived from column refs.
    #[test]
    fn rewrites_column_refs_to_marked_names() {
        let original = arrays_zip().call(vec![
            Expr::Column(Column::from_name("a")),
            Expr::Column(Column::from_name("b")),
        ]);

        let rewritten = run_rewrite(original).expect("transform succeeded");
        assert!(rewritten.transformed, "rewriter should have fired");

        let names = extract_first_arg_names(&rewritten.data);
        assert_eq!(names, vec!["a", "b"]);
        assert_eq!(arg_count(&rewritten.data), 3);
    }

    /// Non-column args fall back to ordinals (`"0"`, `"1"`, ...).
    #[test]
    fn falls_back_to_ordinals_for_expressions() {
        let original =
            arrays_zip().call(vec![lit(1i64), Expr::Column(Column::from_name("b"))]);

        let rewritten = run_rewrite(original).expect("transform succeeded");
        let names = extract_first_arg_names(&rewritten.data);
        assert_eq!(names, vec!["0", "b"]);
    }

    /// Already-marked input is left alone (idempotency).
    #[test]
    fn skips_when_first_arg_already_marked() {
        let already = arrays_zip().call(vec![
            names_literal(&["a".into(), "b".into()]),
            Expr::Column(Column::from_name("a")),
            Expr::Column(Column::from_name("b")),
        ]);

        let rewritten = run_rewrite(already).expect("transform succeeded");
        assert!(
            !rewritten.transformed,
            "second pass must not stack another names literal"
        );
        assert_eq!(arg_count(&rewritten.data), 3);
    }

    /// A user-supplied `List<Utf8>` literal in the first slot has *no* marker
    /// and must be treated as data: the rewriter still prepends a marker and
    /// shifts the user's literal to position 1.
    #[test]
    fn user_supplied_list_utf8_without_marker_is_data() {
        let user_data = Expr::Literal(
            ScalarValue::List(ScalarValue::new_list_from_iter(
                vec![
                    ScalarValue::Utf8(Some("hello".into())),
                    ScalarValue::Utf8(Some("world".into())),
                ]
                .into_iter(),
                &DataType::Utf8,
                false,
            )),
            None, // ← no marker metadata
        );
        let original = arrays_zip().call(vec![
            user_data.clone(),
            Expr::Column(Column::from_name("b")),
        ]);

        let rewritten = run_rewrite(original).expect("transform succeeded");
        assert!(rewritten.transformed, "must rewrite, must not skip");

        let args = scalar_function_args(&rewritten.data);
        assert_eq!(args.len(), 3);

        // Position 0: our marked names literal with ordinal "0" and column "b".
        let names = extract_first_arg_names(&rewritten.data);
        assert_eq!(names, vec!["0", "b"]);

        // Position 1: the user's untouched unmarked list literal.
        let Expr::Literal(_, marker) = &args[1] else {
            panic!("expected user data preserved as Literal at position 1");
        };
        assert!(
            marker.is_none(),
            "user data must not have gained our marker"
        );
    }

    /// A different scalar function call is passed through unchanged.
    #[test]
    fn ignores_other_functions() {
        let other = lit(1i64) + lit(2i64);
        let rewritten = run_rewrite(other.clone()).expect("transform succeeded");
        assert!(!rewritten.transformed);
        assert_eq!(rewritten.data, other);
    }

    /// The inner DataFusion `arrays_zip` (reachable under its `list_zip`
    /// alias) shares the `name()` `"arrays_zip"` — identity dispatch must
    /// reject it so we don't poison its argument list with a marker.
    #[test]
    fn ignores_inner_arrays_zip_with_same_name() {
        let inner_udf = Arc::new(ScalarUDF::new_from_impl(ArraysZip::new()));
        assert_eq!(inner_udf.name(), "arrays_zip");

        let inner_call =
            Expr::ScalarFunction(datafusion_expr::expr::ScalarFunction::new_udf(
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

    /// A foreign UDF that just happens to be named `arrays_zip` must not be
    /// touched. Uses identity check, not name match.
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
        let call = Expr::ScalarFunction(datafusion_expr::expr::ScalarFunction::new_udf(
            foreign,
            vec![Expr::Column(Column::from_name("a"))],
        ));

        let rewritten = run_rewrite(call.clone()).expect("transform succeeded");
        assert!(!rewritten.transformed);
        assert_eq!(rewritten.data, call);
    }

    /// Duplicate names produced by the same column being passed twice get
    /// `_n` suffixes so the result struct has unique field names.
    #[test]
    fn dedups_repeated_column_names() {
        let original = arrays_zip().call(vec![
            Expr::Column(Column::from_name("a")),
            Expr::Column(Column::from_name("a")),
            Expr::Column(Column::from_name("b")),
            Expr::Column(Column::from_name("a")),
        ]);

        let rewritten = run_rewrite(original).expect("transform succeeded");
        let names = extract_first_arg_names(&rewritten.data);
        assert_eq!(names, vec!["a", "a_1", "b", "a_2"]);
    }

    #[test]
    fn dedup_helper_suffix_pattern() {
        let out = dedup_names(vec![
            "x".into(),
            "y".into(),
            "x".into(),
            "x".into(),
            "y".into(),
        ]);
        assert_eq!(out, vec!["x", "y", "x_1", "x_2", "y_1"]);
    }

    fn run_rewrite(expr: Expr) -> Result<Transformed<Expr>> {
        let schema = DFSchema::empty();
        let config = ConfigOptions::new();
        SparkArraysZipRewrite.rewrite(expr, &schema, &config)
    }

    fn scalar_function_args(expr: &Expr) -> &[Expr] {
        let Expr::ScalarFunction(sf) = expr else {
            panic!("expected ScalarFunction, got {expr:?}");
        };
        &sf.args
    }

    fn arg_count(expr: &Expr) -> usize {
        scalar_function_args(expr).len()
    }

    fn extract_first_arg_names(expr: &Expr) -> Vec<String> {
        let args = scalar_function_args(expr);
        let Expr::Literal(ScalarValue::List(list), Some(metadata)) = &args[0] else {
            panic!(
                "first arg should be marked Literal(List), got {:?}",
                args[0]
            );
        };
        assert!(
            metadata.inner().contains_key(ARRAYS_ZIP_NAMES_KEY),
            "first arg literal must carry the marker stamp"
        );
        use arrow::array::{Array, StringArray};
        let values = list.value(0);
        let strs = values
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("names list element type must be Utf8");
        (0..strs.len()).map(|i| strs.value(i).to_string()).collect()
    }
}
