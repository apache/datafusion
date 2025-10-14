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

use datafusion_common::Result;
use datafusion_expr::expr::AggregateFunction;
use datafusion_expr::Expr;

use super::common::*;

pub fn rewrite_aggregate_expr(af: &AggregateFunction) -> Result<Option<Expr>> {
    validate_no_symbol_wildcard_in_non_count(
        af.params.args.get(VALUE_ARG_INDEX),
        is_count(&af.func),
        af.func.name(),
    )?;
    const AGG_REWRITES: &[RewriteArm<AggregateFunction>] = &[
        rewrite_first_last_to_value_udaf,
        rewrite_count_symbol_star_filter,
        rewrite_strip_symbol_and_inject_filter,
        rewrite_ensure_value_ordering,
    ];
    apply_first_rewrite(af, AGG_REWRITES)
}

fn rewrite_first_last_to_value_udaf(af: &AggregateFunction) -> Result<Option<Expr>> {
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

        let new_order_by = with_mr_sequence_order_by(&af.params.order_by);

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

fn rewrite_count_symbol_star_filter(af: &AggregateFunction) -> Result<Option<Expr>> {
    if is_count(&af.func) {
        if let Some(ArgsFilter {
            args: new_args,
            filter: new_filter,
        }) = count_symbol_star_new_args_and_filter(
            af.params.args.get(VALUE_ARG_INDEX),
            af.params.filter.clone(),
        ) {
            let new_af = rebuild_aggregate_with_same_udaf(
                af,
                new_args,
                new_filter,
                af.params.order_by.clone(),
            );
            return Ok(Some(new_af));
        }
    }
    Ok(None)
}

fn rewrite_strip_symbol_and_inject_filter(
    af: &AggregateFunction,
) -> Result<Option<Expr>> {
    if let Some(ArgsFilter {
        args: mut new_args,
        filter: new_filter,
    }) = apply_symbol_filter_on_first_arg(&af.params.args, af.params.filter.clone())?
    {
        let new_order_by = with_value_fn_sequence_order_by(&af.func, &af.params.order_by);

        let new_af = rebuild_aggregate_with_same_udaf(
            af,
            std::mem::take(&mut new_args),
            new_filter,
            new_order_by,
        );

        return Ok(Some(new_af));
    }
    Ok(None)
}

fn rewrite_ensure_value_ordering(af: &AggregateFunction) -> Result<Option<Expr>> {
    if is_first_value(&af.func) || is_last_value(&af.func) {
        let new_order_by = with_mr_sequence_order_by(&af.params.order_by);
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
