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

//! SQL planning extensions like [`MatchRecognizeFunctionPlanner`]

use std::sync::Arc;

use datafusion_common::tree_node::{Transformed, TreeNode};
use datafusion_common::{plan_err, Result, ScalarValue};
use datafusion_expr::expr::{AggregateFunction, ScalarFunction, WindowFunctionParams};
use datafusion_expr::match_recognize::columns::classifier_bits_col_name;
use datafusion_expr::match_recognize::columns::MrMetadataColumn;
use datafusion_expr::{col, expr::Sort};
use datafusion_expr::{
    expr::WindowFunction,
    planner::{ExprPlanner, PlannerResult},
    utils::COUNT_STAR_EXPANSION,
    Expr,
};
use datafusion_expr::{AggregateUDF, Literal, WindowFunctionDefinition};

use crate::windows::{mr_first_udwf, mr_last_udwf, mr_next_udwf, mr_prev_udwf};

/// Argument positions for common function shapes
/// - VALUE_ARG_INDEX: first argument (the value expression)
/// - OFFSET_ARG_INDEX: `lag/lead` offset argument position
/// - DEFAULT_ARG_INDEX: `lag/lead` default value argument position
/// - SYMBOL_ARG_INDEX: `mr_symbol` symbol argument position
const VALUE_ARG_INDEX: usize = 0;
const OFFSET_ARG_INDEX: usize = 1;
const DEFAULT_ARG_INDEX: usize = 2;
const SYMBOL_ARG_INDEX: usize = 1;

/// Planner that rewrites MATCH_RECOGNIZE-specific expressions into
/// executable equivalents used by the rest of DataFusion.
///
/// Invariants and behavior:
/// - At most one symbol predicate may be referenced within a single expression.
/// - Symbol names are normalized to uppercase.
/// - Some functions (e.g. first/last) require deterministic ordering and thus
///   enforce `ORDER BY __mr_match_sequence_number ASC NULLS LAST`.
#[derive(Debug)]
pub struct MatchRecognizeFunctionPlanner;

impl ExprPlanner for MatchRecognizeFunctionPlanner {
    fn plan_match_recognize(
        &self,
        expr: Expr,
        aggregate_context: bool,
    ) -> Result<PlannerResult<Expr>> {
        let rewritten = Self::transform_match_recognize_expr(expr, aggregate_context)?;
        if rewritten.transformed {
            Ok(PlannerResult::Planned(rewritten.data))
        } else {
            Ok(PlannerResult::Original(rewritten.data))
        }
    }
}

impl MatchRecognizeFunctionPlanner {
    // Recursively transform MATCH_RECOGNIZE expressions to their execution equivalents
    fn transform_match_recognize_expr(
        e: Expr,
        aggregate_context: bool,
    ) -> Result<Transformed<Expr>> {
        match &e {
            // Rewrite nullary scalar calls to pass-through virtual metadata columns
            Expr::ScalarFunction(f) if f.args.is_empty() => {
                if let Some(new_expr) = Self::rewrite_nullary_scalar(f) {
                    return Ok(Transformed::yes(new_expr));
                }
                Ok(Transformed::no(e))
            }
            // Bare symbol column: represented as mr_symbol(col, 'SYM').
            // Rewrite to last_value(col) with FILTER (__mr_classifier = 'SYM') and
            // ORDER BY __mr_match_sequence_number ASC NULLS LAST; drop the mr_symbol wrapper.
            Expr::ScalarFunction(f) if is_mr_symbol(f) => {
                let new_expr = Self::rewrite_mr_symbol(f, aggregate_context)?;
                Ok(Transformed::yes(new_expr))
            }

            Expr::WindowFunction(wf) => {
                if let Some(new_expr) = Self::rewrite_window_expr(wf)? {
                    return Ok(Transformed::yes(new_expr));
                }
                // Stop recursion inside window function: do not traverse inside
                Ok(Transformed::no(e))
            }

            Expr::AggregateFunction(af) => {
                if let Some(new_expr) = Self::rewrite_aggregate_expr(af)? {
                    return Ok(Transformed::yes(new_expr));
                }
                // Stop recursion inside aggregate function: do not traverse inside
                Ok(Transformed::no(e))
            }

            _ => e.map_children(|child| {
                Self::transform_match_recognize_expr(child, aggregate_context)
            }),
        }
    }

    /// Handle nullary scalar function calls (metadata passthrough)
    fn rewrite_nullary_scalar(f: &ScalarFunction) -> Option<Expr> {
        metadata_fn_to_virtual_col(f.name()).map(|col_kind| col(col_kind.as_ref()))
    }

    /// Handle window function expressions, including MR navigation and
    /// aggregate window functions that operate on symbol predicates.
    fn rewrite_window_expr(wf: &WindowFunction) -> Result<Option<Expr>> {
        apply_first_rewrite(
            wf,
            &[
                Self::rewrite_window_nav,
                Self::rewrite_window_lag_lead,
                Self::rewrite_window_first_last_value,
                Self::rewrite_window_aggregate_udaf,
            ],
        )
    }

    /// Handle aggregate function expressions, including MR aggregates that
    /// operate on symbol predicates.
    fn rewrite_aggregate_expr(af: &AggregateFunction) -> Result<Option<Expr>> {
        Self::check_agg_disallow_symbol_wildcard_non_count(af)?;
        apply_first_rewrite(
            af,
            &[
                Self::rewrite_agg_first_last,
                Self::rewrite_agg_count_symbol_star,
                Self::rewrite_agg_general_strip_filter,
                Self::rewrite_agg_first_last_value_ordering,
            ],
        )
    }

    /// Handle scalar function calls to `mr_symbol(value, 'SYM')`.
    ///
    /// If `aggregate_context` is true, produce an aggregate `last_value(value)`
    /// with `FILTER (__mr_classifier = 'SYM')` and enforced MR sequence ordering.
    /// Otherwise, rewrite to the corresponding MR window function variant.
    fn rewrite_mr_symbol(f: &ScalarFunction, aggregate_context: bool) -> Result<Expr> {
        // Assume mr_symbol(value_expr, Utf8('SYM'))
        let value_expr = &f.args[VALUE_ARG_INDEX];
        let Expr::Literal(ScalarValue::Utf8(Some(sym)), _) = &f.args[SYMBOL_ARG_INDEX]
        else {
            return plan_err!(
                "mr_symbol second argument must be Utf8('SYM'), got: {:?}",
                &f.args[SYMBOL_ARG_INDEX]
            );
        };

        let new_expr = match aggregate_context {
            true => {
                // Build last_value(value_expr) with FILTER (__mr_classifier = 'SYM')
                // and enforce ORDER BY __mr_match_sequence_number ASC NULLS LAST
                let order_by = vec![mr_sequence_sort()];
                Expr::AggregateFunction(AggregateFunction::new_udf(
                    datafusion_functions_aggregate::first_last::last_value_udaf(),
                    vec![value_expr.clone()],
                    false,
                    inject_symbol_filter(None, sym),
                    order_by,
                    None,
                ))
            }
            false => mr_last_udwf()
                .call(vec![value_expr.clone(), col(classifier_bits_col_name(sym))]),
        };
        Ok(new_expr)
    }

    /// Window arm: navigation functions (FIRST/LAST/PREV/NEXT) with potential mr_symbol wrapper on first arg
    fn rewrite_window_nav(wf: &WindowFunction) -> Result<Option<Expr>> {
        if is_navigation(&wf.fun) {
            let (new_args, symbol_opt) = strip_symbol_from_first_arg(&wf.params.args)?;
            if symbol_opt.is_some() {
                let new_wf = rebuild_window_with_args(wf, new_args)?;
                return Ok(Some(new_wf));
            }
        }
        Ok(None)
    }

    /// Window arm: LAG/LEAD with optional offset/default literals and mr_symbol on first arg
    fn rewrite_window_lag_lead(wf: &WindowFunction) -> Result<Option<Expr>> {
        if is_lag(&wf.fun) || is_lead(&wf.fun) {
            let offset_val = wf
                .params
                .args
                .get(OFFSET_ARG_INDEX)
                .and_then(extract_optional_i64_literal);

            let default_val = wf
                .params
                .args
                .get(DEFAULT_ARG_INDEX)
                .and_then(extract_optional_scalar_literal);

            let (new_args, symbol_opt) = strip_symbol_from_first_arg(&wf.params.args)?;
            if let Some(symbol) = symbol_opt {
                let clean_expr = new_args[VALUE_ARG_INDEX].clone();
                let new_expr = build_prev_next_udwf_expr(
                    is_lag(&wf.fun),
                    clean_expr,
                    symbol,
                    offset_val,
                    default_val,
                );
                return Ok(Some(new_expr));
            }
        }
        Ok(None)
    }

    /// Window arm: FIRST_VALUE/LAST_VALUE referencing symbol columns in first arg
    fn rewrite_window_first_last_value(wf: &WindowFunction) -> Result<Option<Expr>> {
        if is_first_last_value(&wf.fun) {
            let (new_args, symbol_opt) = strip_symbol_from_first_arg(&wf.params.args)?;
            if let Some(symbol) = symbol_opt {
                let clean_expr = new_args[VALUE_ARG_INDEX].clone();
                let mask_col = col(classifier_bits_col_name(&symbol));
                let new_expr = if is_first_value(&wf.fun) {
                    mr_first_udwf().call(vec![clean_expr, mask_col])
                } else {
                    mr_last_udwf().call(vec![clean_expr, mask_col])
                };
                return Ok(Some(new_expr));
            }
        }
        Ok(None)
    }

    /// Window arm: Aggregate UDAF window functions operating on symbol predicates
    fn rewrite_window_aggregate_udaf(wf: &WindowFunction) -> Result<Option<Expr>> {
        if let WindowFunctionDefinition::AggregateUDF(udaf) = &wf.fun {
            if let Some(Expr::ScalarFunction(f)) = wf.params.args.get(VALUE_ARG_INDEX) {
                if is_mr_symbol(f) && !is_count(udaf) {
                    #[allow(deprecated)]
                    if let [Expr::Wildcard { .. }, _] = f.args.as_slice() {
                        return plan_err!(
                            "Symbol-qualified wildcard is only supported for COUNT in MATCH_RECOGNIZE; found '{}'",
                            udaf.name()
                        );
                    }
                }
            }

            if is_count(udaf) {
                if let Some(Expr::ScalarFunction(f)) = wf.params.args.get(VALUE_ARG_INDEX)
                {
                    if is_mr_symbol(f) {
                        #[allow(deprecated)]
                        if let [Expr::Wildcard { .. }, Expr::Literal(ScalarValue::Utf8(Some(symbol)), _)] =
                            f.args.as_slice()
                        {
                            let new_args =
                                vec![Expr::Literal(COUNT_STAR_EXPANSION, None)];
                            let new_wf = rebuild_window_with_args_and_filter(
                                wf,
                                new_args,
                                inject_symbol_filter(wf.params.filter.clone(), symbol),
                            )?;
                            return Ok(Some(new_wf));
                        }
                    }
                }
            }

            let (new_args, symbol_opt) = strip_symbol_from_first_arg(&wf.params.args)?;
            if let Some(symbol) = symbol_opt {
                let new_wf = rebuild_window_with_args_and_filter(
                    wf,
                    new_args,
                    inject_symbol_filter(wf.params.filter.clone(), &symbol),
                )?;
                return Ok(Some(new_wf));
            }
        }
        Ok(None)
    }

    /// Aggregate arm: Disallow symbol-qualified wildcard for non-COUNT aggregates
    fn check_agg_disallow_symbol_wildcard_non_count(
        af: &AggregateFunction,
    ) -> Result<()> {
        if let Some(Expr::ScalarFunction(f)) = af.params.args.get(VALUE_ARG_INDEX) {
            if is_mr_symbol(f) && !is_count(&af.func) {
                #[allow(deprecated)]
                if let [Expr::Wildcard { .. }, _] = f.args.as_slice() {
                    return plan_err!(
                        "Symbol-qualified wildcard is only supported for COUNT in MATCH_RECOGNIZE; found '{}'",
                        af.func.name()
                    );
                }
            }
        }
        Ok(())
    }

    /// Aggregate arm: Rewrite MR 'first'/'last' into 'first_value'/'last_value' and enforce ordering
    fn rewrite_agg_first_last(af: &AggregateFunction) -> Result<Option<Expr>> {
        if is_first(&af.func) || is_last(&af.func) {
            let (new_args, new_filter) =
                if let Some(value_expr) = af.params.args.get(VALUE_ARG_INDEX) {
                    let (clean_expr, symbol_opt) = strip_mr_symbol(value_expr.clone())?;
                    let mut args = af.params.args.clone();
                    args[VALUE_ARG_INDEX] = clean_expr;

                    let filter = symbol_opt
                        .map(|s| inject_symbol_filter(af.params.filter.clone(), s))
                        .unwrap_or_else(|| af.params.filter.clone());
                    (args, filter)
                } else {
                    (af.params.args.clone(), af.params.filter.clone())
                };

            let mut new_order_by = af.params.order_by.clone();
            append_mr_sequence_order_by(&mut new_order_by);

            let new_udaf = if is_first(&af.func) {
                datafusion_functions_aggregate::first_last::first_value_udaf()
            } else {
                datafusion_functions_aggregate::first_last::last_value_udaf()
            };

            let new_af =
                rebuild_aggregate_with(af, new_udaf, new_args, new_filter, new_order_by);

            return Ok(Some(new_af));
        }
        Ok(None)
    }

    /// Aggregate arm: Special case COUNT(symbol.*)
    fn rewrite_agg_count_symbol_star(af: &AggregateFunction) -> Result<Option<Expr>> {
        if is_count(&af.func) {
            if let Some(Expr::ScalarFunction(f)) = af.params.args.get(VALUE_ARG_INDEX) {
                if is_mr_symbol(f) {
                    #[allow(deprecated)]
                    if let [Expr::Wildcard { .. }, Expr::Literal(ScalarValue::Utf8(Some(symbol)), _)] =
                        f.args.as_slice()
                    {
                        let new_args = vec![Expr::Literal(COUNT_STAR_EXPANSION, None)];
                        let new_filter =
                            inject_symbol_filter(af.params.filter.clone(), symbol);

                        let new_af = rebuild_aggregate_with_same_udaf(
                            af,
                            new_args,
                            new_filter,
                            af.params.order_by.clone(),
                        );

                        return Ok(Some(new_af));
                    }
                }
            }
        }
        Ok(None)
    }

    /// Aggregate arm: General case, strip mr_symbol from first arg and push classifier filter
    fn rewrite_agg_general_strip_filter(af: &AggregateFunction) -> Result<Option<Expr>> {
        if let Some(value_expr) = af.params.args.get(VALUE_ARG_INDEX) {
            let (clean_expr, symbol_opt) = strip_mr_symbol(value_expr.clone())?;

            if let Some(symbol) = symbol_opt {
                let mut new_args = af.params.args.clone();
                new_args[VALUE_ARG_INDEX] = clean_expr;

                let new_filter = inject_symbol_filter(af.params.filter.clone(), &symbol);

                let mut new_order_by = af.params.order_by.clone();
                if is_first_value(&af.func) || is_last_value(&af.func) {
                    append_mr_sequence_order_by(&mut new_order_by);
                }

                let new_af = rebuild_aggregate_with_same_udaf(
                    af,
                    new_args,
                    new_filter,
                    new_order_by,
                );

                return Ok(Some(new_af));
            }
        }
        Ok(None)
    }

    /// Aggregate arm: Inject deterministic order for first_value/last_value if missing
    fn rewrite_agg_first_last_value_ordering(
        af: &AggregateFunction,
    ) -> Result<Option<Expr>> {
        if is_first_value(&af.func) || is_last_value(&af.func) {
            let mut new_order_by = af.params.order_by.clone();
            append_mr_sequence_order_by(&mut new_order_by);
            let new_af = rebuild_aggregate_with_same_udaf(
                af,
                af.params.args.clone(),
                af.params.filter.clone(),
                new_order_by,
            );
            return Ok(Some(new_af));
        }
        Ok(None)
    }
}

/// Helper: strip all mr_symbol wrappers, ensuring at most one symbol.
/// Returns the cleaned expression and the single symbol (uppercased) if present.
fn strip_mr_symbol(expr: Expr) -> Result<(Expr, Option<String>)> {
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

                Ok(Transformed::yes(
                    value_expr.clone(),
                ))
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

/// Try a list of rewrite functions and return the first successful result
type RewriteResult = Result<Option<Expr>>;
type RewriteArm<T> = fn(&T) -> RewriteResult;

fn apply_first_rewrite<T>(input: &T, arms: &[RewriteArm<T>]) -> RewriteResult {
    for arm in arms {
        if let Some(res) = arm(input)? {
            return Ok(Some(res));
        }
    }
    Ok(None)
}

/// Strip mr_symbol from the first argument of an argument list, returning the
/// updated args and the optional symbol if present.
fn strip_symbol_from_first_arg(args: &[Expr]) -> Result<(Vec<Expr>, Option<String>)> {
    if let Some(value_expr) = args.get(VALUE_ARG_INDEX) {
        let (clean_expr, symbol_opt) = strip_mr_symbol(value_expr.clone())?;
        let mut new_args = args.to_vec();
        new_args[VALUE_ARG_INDEX] = clean_expr;
        Ok((new_args, symbol_opt))
    } else {
        Ok((args.to_vec(), None))
    }
}

// Combine an optional existing FILTER with a classifier condition using logical AND.
// Returns Some(filter) in all cases, as the classifier condition must be applied.
fn combine_filter(
    existing: Option<Box<Expr>>,
    classifier_condition: Expr,
) -> Option<Box<Expr>> {
    Some(Box::new(match existing {
        Some(existing_expr) => (*existing_expr).and(classifier_condition),
        None => classifier_condition,
    }))
}

// Helper: inject classifier filter (WHERE __mr_classifier_'<symbol>'), preserving any existing filter
fn inject_symbol_filter(
    existing: Option<Box<Expr>>,
    symbol: impl AsRef<str>,
) -> Option<Box<Expr>> {
    let cond = col(classifier_bits_col_name(symbol.as_ref()));
    combine_filter(existing, cond)
}

// Append ORDER BY __mr_match_sequence_number ASC NULLS LAST to the provided order_by list
fn append_mr_sequence_order_by(order_by: &mut Vec<Sort>) {
    // Deterministic match order is required for MR semantics.
    order_by.push(mr_sequence_sort());
}

fn mr_sequence_sort() -> Sort {
    // ASC, NULLS LAST
    Sort::new(
        col(MrMetadataColumn::MatchSequenceNumber.as_ref()),
        true,
        false,
    )
}

// ---- Name equality helpers ----
trait HasFunctionName {
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

fn is_mr_symbol(f: &ScalarFunction) -> bool {
    f.name() == "mr_symbol"
}

fn is_count(udaf: &Arc<AggregateUDF>) -> bool {
    udaf.name() == "count"
}

fn is_first<F: HasFunctionName>(fun: &F) -> bool {
    fun.name_str() == "first"
}

fn is_last<F: HasFunctionName>(fun: &F) -> bool {
    fun.name_str() == "last"
}

fn is_prev(wfd: &WindowFunctionDefinition) -> bool {
    wfd.name() == "prev"
}

fn is_next(wfd: &WindowFunctionDefinition) -> bool {
    wfd.name() == "next"
}

fn is_first_value<F: HasFunctionName>(fun: &F) -> bool {
    fun.name_str() == "first_value"
}

fn is_last_value<F: HasFunctionName>(fun: &F) -> bool {
    fun.name_str() == "last_value"
}

fn is_lag(wfd: &WindowFunctionDefinition) -> bool {
    wfd.name() == "lag"
}

fn is_lead(wfd: &WindowFunctionDefinition) -> bool {
    wfd.name() == "lead"
}

/// Grouped helper: navigation window functions FIRST/LAST/PREV/NEXT
fn is_navigation(wfd: &WindowFunctionDefinition) -> bool {
    is_first(wfd) || is_last(wfd) || is_prev(wfd) || is_next(wfd)
}

/// Grouped helper: FIRST_VALUE/LAST_VALUE window functions
fn is_first_last_value(wfd: &WindowFunctionDefinition) -> bool {
    is_first_value(wfd) || is_last_value(wfd)
}

/// Map metadata passthrough scalar functions (no-arg) to their virtual column
fn metadata_fn_to_virtual_col(name: &str) -> Option<MrMetadataColumn> {
    if name == MrMetadataColumn::Classifier.measure_function_name() {
        Some(MrMetadataColumn::Classifier)
    } else if name == MrMetadataColumn::MatchNumber.measure_function_name() {
        Some(MrMetadataColumn::MatchNumber)
    } else if name == MrMetadataColumn::MatchSequenceNumber.measure_function_name() {
        Some(MrMetadataColumn::MatchSequenceNumber)
    } else if name == MrMetadataColumn::IsExcludedRow.measure_function_name() {
        Some(MrMetadataColumn::IsExcludedRow)
    } else {
        None
    }
}

/// Rebuild an AggregateFunction expression with a potentially different UDAF
fn rebuild_aggregate_with(
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

/// Rebuild an AggregateFunction expression keeping the same UDAF
fn rebuild_aggregate_with_same_udaf(
    af: &AggregateFunction,
    new_args: Vec<Expr>,
    new_filter: Option<Box<Expr>>,
    new_order_by: Vec<Sort>,
) -> Expr {
    rebuild_aggregate_with(af, Arc::clone(&af.func), new_args, new_filter, new_order_by)
}

/// Extract optional i64 literal from an expression (supports Int64/UInt64)
fn extract_optional_i64_literal(ex: &Expr) -> Option<i64> {
    match ex {
        Expr::Literal(ScalarValue::Int64(Some(v)), _) => Some(*v),
        Expr::Literal(ScalarValue::UInt64(Some(v)), _) => Some(*v as i64),
        _ => None,
    }
}

/// Extract a scalar literal value if the expression is a literal
fn extract_optional_scalar_literal(ex: &Expr) -> Option<ScalarValue> {
    match ex {
        Expr::Literal(sv, _) => Some(sv.clone()),
        _ => None,
    }
}

// Rebuild a WindowFunction expression with new args, preserving all window params
fn rebuild_window_with_args(wf: &WindowFunction, new_args: Vec<Expr>) -> Result<Expr> {
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

// Rebuild a WindowFunction expression with new args and optional new filter, preserving all other params
fn rebuild_window_with_args_and_filter(
    wf: &WindowFunction,
    new_args: Vec<Expr>,
    new_filter: Option<Box<Expr>>,
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

// Build a prev/next UDWF expression with shared construction for lag/lead
fn build_prev_next_udwf_expr(
    is_prev: bool,
    clean_expr: Expr,
    symbol: String,
    offset_val: Option<i64>,
    default_val: Option<ScalarValue>,
) -> Expr {
    let mask_col = col(classifier_bits_col_name(&symbol));
    let offset_lit = offset_val
        .map(|v| v.lit())
        .unwrap_or(ScalarValue::Null.lit());
    let default_lit = default_val.unwrap_or(ScalarValue::Null).lit();

    if is_prev {
        mr_prev_udwf().call(vec![clean_expr, offset_lit, default_lit, mask_col])
    } else {
        mr_next_udwf().call(vec![clean_expr, offset_lit, default_lit, mask_col])
    }
}
