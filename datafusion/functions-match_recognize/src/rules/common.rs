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

use std::sync::Arc;

use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_common::{plan_err, Result, ScalarValue};
use datafusion_expr::expr::{
    AggregateFunction, ScalarFunction, WindowFunction, WindowFunctionParams,
};
use datafusion_expr::match_recognize::columns::classifier_bits_col_name;
use datafusion_expr::match_recognize::columns::MrMetadataColumn;
use datafusion_expr::utils::COUNT_STAR_EXPANSION;
use datafusion_expr::{col, expr::Sort};
use datafusion_expr::{AggregateUDF, Expr, WindowFunctionDefinition};

// Lightweight types to clarify intent
pub(super) type FilterExpr = Option<Box<Expr>>;

#[derive(Clone)]
pub(super) struct ArgsFilter {
    pub args: Vec<Expr>,
    pub filter: FilterExpr,
}

pub(super) const VALUE_ARG_INDEX: usize = 0;
pub(super) const OFFSET_ARG_INDEX: usize = 1;
pub(super) const DEFAULT_ARG_INDEX: usize = 2;
pub(super) const SYMBOL_ARG_INDEX: usize = 1;

pub(super) type RewriteResult = Result<Option<Expr>>;
pub(super) type RewriteArm<T> = fn(&T) -> RewriteResult;

pub(super) fn apply_first_rewrite<T>(input: &T, arms: &[RewriteArm<T>]) -> RewriteResult {
    for arm in arms {
        if let Some(res) = arm(input)? {
            return Ok(Some(res));
        }
    }
    Ok(None)
}

pub(super) fn strip_mr_symbol(expr: Expr) -> Result<(Expr, Option<String>)> {
    let mut found_symbol: Option<String> = None;

    let transformed = expr.transform_up(|node| match node {
        Expr::ScalarFunction(f) if is_mr_symbol(&f) => {
            let args = &f.args;
            if let [value_expr, Expr::Literal(ScalarValue::Utf8(Some(sym)), _)] =
                args.as_slice()
            {
                let sym_upper = sym.to_uppercase();
                if let Some(existing) = &found_symbol {
                    if existing != &sym_upper {
                        return plan_err!(
                            "Expression can reference at most one symbol predicate, found '{}' and '{}'",
                            existing,
                            sym_upper
                        );
                    }
                } else {
                    found_symbol = Some(sym_upper);
                }

                Ok(Transformed::yes(value_expr.clone()))
            } else {
                plan_err!(
                    "mr_symbol function call has unexpected arguments. Expected (value_expr, Utf8('SYM')), got: {:?}",
                    args
                )
            }
        }
        _ => Ok(Transformed::no(node)),
    })?;

    Ok((transformed.data, found_symbol))
}

pub(super) fn strip_symbol_from_first_arg(
    args: &[Expr],
) -> Result<(Vec<Expr>, Option<String>)> {
    if let Some(value_expr) = args.get(VALUE_ARG_INDEX) {
        let (clean_expr, symbol_opt) = strip_mr_symbol(value_expr.clone())?;
        let mut new_args = args.to_vec();
        new_args[VALUE_ARG_INDEX] = clean_expr;
        Ok((new_args, symbol_opt))
    } else {
        Ok((args.to_vec(), None))
    }
}

pub(super) fn combine_filter(
    existing: FilterExpr,
    classifier_condition: Expr,
) -> FilterExpr {
    Some(Box::new(match existing {
        Some(existing_expr) => (*existing_expr).and(classifier_condition),
        None => classifier_condition,
    }))
}

pub(super) fn inject_symbol_filter(
    existing: FilterExpr,
    symbol: impl AsRef<str>,
) -> FilterExpr {
    let cond = col(classifier_bits_col_name(symbol.as_ref()));
    combine_filter(existing, cond)
}

pub(super) fn mr_sequence_sort() -> Sort {
    Sort::new(
        col(MrMetadataColumn::MatchSequenceNumber.as_ref()),
        true,
        false,
    )
}

pub(super) fn with_mr_sequence_order_by(order_by: &[Sort]) -> Vec<Sort> {
    let mut out = order_by.to_vec();
    out.push(mr_sequence_sort());
    out
}

pub(super) fn with_value_fn_sequence_order_by<F: HasFunctionName>(
    fun: &F,
    order_by: &[Sort],
) -> Vec<Sort> {
    if is_first_value(fun) || is_last_value(fun) {
        with_mr_sequence_order_by(order_by)
    } else {
        order_by.to_vec()
    }
}

pub(super) fn symbol_mask_col(symbol: &str) -> Expr {
    col(classifier_bits_col_name(symbol))
}

pub(super) fn count_star_expr() -> Expr {
    Expr::Literal(COUNT_STAR_EXPANSION, None)
}

pub(super) trait HasFunctionName {
    fn name_str(&self) -> &str;
}

impl HasFunctionName for WindowFunctionDefinition {
    fn name_str(&self) -> &str {
        self.name()
    }
}

impl HasFunctionName for Arc<AggregateUDF> {
    fn name_str(&self) -> &str {
        self.name()
    }
}

pub(crate) fn is_mr_symbol(f: &ScalarFunction) -> bool {
    f.name() == "mr_symbol"
}

pub(super) fn is_count(udaf: &Arc<AggregateUDF>) -> bool {
    udaf.name() == "count"
}

pub(super) fn is_first<F: HasFunctionName>(fun: &F) -> bool {
    fun.name_str() == "first"
}

pub(super) fn is_last<F: HasFunctionName>(fun: &F) -> bool {
    fun.name_str() == "last"
}

pub(super) fn is_prev(wfd: &WindowFunctionDefinition) -> bool {
    wfd.name() == "prev"
}

pub(super) fn is_next(wfd: &WindowFunctionDefinition) -> bool {
    wfd.name() == "next"
}

pub(super) fn is_first_value<F: HasFunctionName>(fun: &F) -> bool {
    fun.name_str() == "first_value"
}

pub(super) fn is_last_value<F: HasFunctionName>(fun: &F) -> bool {
    fun.name_str() == "last_value"
}

pub(super) fn is_lag(wfd: &WindowFunctionDefinition) -> bool {
    wfd.name() == "lag"
}

pub(super) fn is_lead(wfd: &WindowFunctionDefinition) -> bool {
    wfd.name() == "lead"
}

pub(super) fn is_navigation(wfd: &WindowFunctionDefinition) -> bool {
    is_first(wfd) || is_last(wfd) || is_prev(wfd) || is_next(wfd)
}

pub(super) fn is_first_or_last_value(wfd: &WindowFunctionDefinition) -> bool {
    is_first_value(wfd) || is_last_value(wfd)
}

pub(super) fn metadata_fn_to_virtual_col(name: &str) -> Option<MrMetadataColumn> {
    if name == MrMetadataColumn::Classifier.measure_function_name() {
        Some(MrMetadataColumn::Classifier)
    } else if name == MrMetadataColumn::MatchNumber.measure_function_name() {
        Some(MrMetadataColumn::MatchNumber)
    } else if name == MrMetadataColumn::MatchSequenceNumber.measure_function_name() {
        Some(MrMetadataColumn::MatchSequenceNumber)
    } else {
        None
    }
}

pub(super) fn rebuild_aggregate_with(
    af: &AggregateFunction,
    udaf: Arc<AggregateUDF>,
    new_args: Vec<Expr>,
    new_filter: Option<Box<Expr>>,
    new_order_by: Vec<Sort>,
) -> Expr {
    Expr::AggregateFunction(AggregateFunction::new_udf(
        udaf,
        new_args,
        af.params.distinct,
        new_filter,
        new_order_by,
        af.params.null_treatment,
    ))
}

pub(super) fn rebuild_aggregate_with_same_udaf(
    af: &AggregateFunction,
    new_args: Vec<Expr>,
    new_filter: Option<Box<Expr>>,
    new_order_by: Vec<Sort>,
) -> Expr {
    rebuild_aggregate_with(af, Arc::clone(&af.func), new_args, new_filter, new_order_by)
}

pub(super) fn extract_optional_i64_literal(ex: &Expr) -> Option<i64> {
    match ex {
        Expr::Literal(ScalarValue::Int64(Some(v)), _) => Some(*v),
        Expr::Literal(ScalarValue::UInt64(Some(v)), _) => Some(*v as i64),
        _ => None,
    }
}

pub(super) fn extract_optional_scalar_literal(ex: &Expr) -> Option<ScalarValue> {
    match ex {
        Expr::Literal(sv, _) => Some(sv.clone()),
        _ => None,
    }
}

pub(super) fn rebuild_window_with_args(
    wf: &WindowFunction,
    new_args: Vec<Expr>,
) -> Result<Expr> {
    let params = WindowFunctionParams {
        args: new_args,
        partition_by: wf.params.partition_by.clone(),
        order_by: wf.params.order_by.clone(),
        window_frame: wf.params.window_frame.clone(),
        filter: wf.params.filter.clone(),
        null_treatment: wf.params.null_treatment,
        distinct: wf.params.distinct,
    };
    Ok(Expr::WindowFunction(Box::new(WindowFunction {
        fun: wf.fun.clone(),
        params,
    })))
}

pub(super) fn rebuild_window_with_args_and_filter(
    wf: &WindowFunction,
    new_args: Vec<Expr>,
    new_filter: FilterExpr,
) -> Result<Expr> {
    let params = WindowFunctionParams {
        args: new_args,
        partition_by: wf.params.partition_by.clone(),
        order_by: wf.params.order_by.clone(),
        window_frame: wf.params.window_frame.clone(),
        filter: new_filter.or_else(|| wf.params.filter.clone()),
        null_treatment: wf.params.null_treatment,
        distinct: wf.params.distinct,
    };
    Ok(Expr::WindowFunction(Box::new(WindowFunction {
        fun: wf.fun.clone(),
        params,
    })))
}

pub(super) fn validate_no_symbol_wildcard_in_non_count(
    first_arg: Option<&Expr>,
    is_count_fn: bool,
    func_name: &str,
) -> Result<()> {
    if let Some(Expr::ScalarFunction(f)) = first_arg {
        if is_mr_symbol(f) && !is_count_fn {
            #[allow(deprecated)]
            if let [Expr::Wildcard { .. }, _] = f.args.as_slice() {
                return plan_err!(
                    "Symbol-qualified wildcard is only supported for COUNT in MATCH_RECOGNIZE; found '{}'",
                    func_name
                );
            }
        }
    }
    Ok(())
}

pub(super) fn count_symbol_star_new_args_and_filter(
    first_arg: Option<&Expr>,
    existing_filter: FilterExpr,
) -> Option<ArgsFilter> {
    if let Some(Expr::ScalarFunction(f)) = first_arg {
        if is_mr_symbol(f) {
            #[allow(deprecated)]
            if let [Expr::Wildcard { .. }, Expr::Literal(ScalarValue::Utf8(Some(symbol)), _)] =
                f.args.as_slice()
            {
                let new_args = vec![count_star_expr()];
                let new_filter = inject_symbol_filter(existing_filter, symbol);
                return Some(ArgsFilter {
                    args: new_args,
                    filter: new_filter,
                });
            }
        }
    }
    None
}

pub(super) fn apply_symbol_filter_on_first_arg(
    args: &[Expr],
    existing_filter: FilterExpr,
) -> Result<Option<ArgsFilter>> {
    let (new_args, symbol_opt) = strip_symbol_from_first_arg(args)?;
    if let Some(symbol) = symbol_opt {
        let new_filter = inject_symbol_filter(existing_filter, &symbol);
        Ok(Some(ArgsFilter {
            args: new_args,
            filter: new_filter,
        }))
    } else {
        Ok(None)
    }
}
