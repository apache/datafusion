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

use crate::rules::common::{
    inject_symbol_filter, metadata_fn_to_virtual_col, mr_sequence_sort, SYMBOL_ARG_INDEX,
    VALUE_ARG_INDEX,
};
use crate::windows::mr_last_udwf;
use datafusion_common::{plan_err, Result, ScalarValue};
use datafusion_expr::expr::{AggregateFunction, ScalarFunction};
use datafusion_expr::match_recognize::columns::classifier_bits_col_name;
use datafusion_expr::{col, Expr};
use datafusion_functions_aggregate::first_last::last_value_udaf;

pub fn rewrite_nullary_scalar(f: &ScalarFunction) -> Option<Expr> {
    metadata_fn_to_virtual_col(f.name()).map(|col_kind| col(col_kind.as_ref()))
}

pub fn rewrite_mr_symbol(f: &ScalarFunction, aggregate_context: bool) -> Result<Expr> {
    let value_expr = &f.args[VALUE_ARG_INDEX];
    let Expr::Literal(ScalarValue::Utf8(Some(sym)), _) = &f.args[SYMBOL_ARG_INDEX] else {
        return plan_err!(
            "mr_symbol second argument must be Utf8('SYM'), got: {:?}",
            &f.args[SYMBOL_ARG_INDEX]
        );
    };

    let new_expr = if aggregate_context {
        let order_by = vec![mr_sequence_sort()];
        Expr::AggregateFunction(AggregateFunction::new_udf(
            last_value_udaf(),
            vec![value_expr.clone()],
            false,
            inject_symbol_filter(None, sym),
            order_by,
            None,
        ))
    } else {
        mr_last_udwf().call(vec![value_expr.clone(), col(classifier_bits_col_name(sym))])
    };
    Ok(new_expr)
}
