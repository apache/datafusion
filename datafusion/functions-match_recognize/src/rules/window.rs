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

use datafusion_common::{Result, ScalarValue};
use datafusion_expr::expr::WindowFunction;
use datafusion_expr::{Expr, Literal, WindowFunctionDefinition};

use crate::windows::{mr_first_udwf, mr_last_udwf, mr_next_udwf, mr_prev_udwf};

use super::common::*;

pub fn rewrite_window_expr(wf: &WindowFunction) -> Result<Option<Expr>> {
    const WINDOW_REWRITES: &[RewriteArm<WindowFunction>] = &[
        rewrite_window_nav,
        rewrite_window_lag_lead,
        rewrite_window_first_last_value,
        rewrite_window_aggregate_udaf,
    ];
    apply_first_rewrite(wf, WINDOW_REWRITES)
}

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

fn rewrite_window_first_last_value(wf: &WindowFunction) -> Result<Option<Expr>> {
    if is_first_or_last_value(&wf.fun) {
        let (new_args, symbol_opt) = strip_symbol_from_first_arg(&wf.params.args)?;
        if let Some(symbol) = symbol_opt {
            let clean_expr = new_args[VALUE_ARG_INDEX].clone();
            let mask_col = symbol_mask_col(&symbol);
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

fn rewrite_window_aggregate_udaf(wf: &WindowFunction) -> Result<Option<Expr>> {
    if let WindowFunctionDefinition::AggregateUDF(udaf) = &wf.fun {
        validate_no_symbol_wildcard_in_non_count(
            wf.params.args.get(VALUE_ARG_INDEX),
            is_count(udaf),
            udaf.name(),
        )?;

        if is_count(udaf) {
            if let Some(ArgsFilter {
                args: new_args,
                filter: new_filter,
            }) = count_symbol_star_new_args_and_filter(
                wf.params.args.get(VALUE_ARG_INDEX),
                wf.params.filter.clone(),
            ) {
                let new_wf =
                    rebuild_window_with_args_and_filter(wf, new_args, new_filter)?;
                return Ok(Some(new_wf));
            }
        }

        if let Some(ArgsFilter {
            args: new_args,
            filter: new_filter,
        }) =
            apply_symbol_filter_on_first_arg(&wf.params.args, wf.params.filter.clone())?
        {
            let new_wf = rebuild_window_with_args_and_filter(wf, new_args, new_filter)?;
            return Ok(Some(new_wf));
        }
    }
    Ok(None)
}

fn build_prev_next_udwf_expr(
    is_prev: bool,
    clean_expr: Expr,
    symbol: String,
    offset_val: Option<i64>,
    default_val: Option<ScalarValue>,
) -> Expr {
    let mask_col = symbol_mask_col(&symbol);
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
