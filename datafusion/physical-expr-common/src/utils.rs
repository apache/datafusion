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

use crate::expressions::literal::Literal;
use crate::expressions::CastExpr;
use crate::sort_expr::PhysicalSortExpr;
use datafusion_common::{exec_err, DFSchema, Result};
use datafusion_expr::expr::Alias;
use datafusion_expr::expressions::column::Column;
use datafusion_expr::physical_expr::PhysicalExpr;
use datafusion_expr::Expr;

/// Reverses the ORDER BY expression, which is useful during equivalent window
/// expression construction. For instance, 'ORDER BY a ASC, NULLS LAST' turns into
/// 'ORDER BY a DESC, NULLS FIRST'.
pub fn reverse_order_bys(order_bys: &[PhysicalSortExpr]) -> Vec<PhysicalSortExpr> {
    order_bys
        .iter()
        .map(|e| PhysicalSortExpr::new(e.expr.clone(), !e.options))
        .collect()
}

/// Converts `datafusion_expr::Expr` into corresponding `Arc<dyn PhysicalExpr>`.
/// If conversion is not supported yet, returns Error.
pub fn limited_convert_logical_expr_to_physical_expr_with_dfschema(
    expr: &Expr,
    dfschema: &DFSchema,
) -> Result<Arc<dyn PhysicalExpr>> {
    match expr {
        Expr::Alias(Alias { expr, .. }) => Ok(
            limited_convert_logical_expr_to_physical_expr_with_dfschema(expr, dfschema)?,
        ),
        Expr::Column(col) => {
            let idx = dfschema.index_of_column(col)?;
            Ok(Arc::new(Column::new(&col.name, idx)))
        }
        Expr::Cast(cast_expr) => Ok(Arc::new(CastExpr::new(
            limited_convert_logical_expr_to_physical_expr_with_dfschema(
                cast_expr.expr.as_ref(),
                dfschema,
            )?,
            cast_expr.data_type.clone(),
            None,
        ))),
        Expr::Literal(value) => Ok(Arc::new(Literal::new(value.clone()))),
        _ => exec_err!(
            "Unsupported expression: {expr} for conversion to Arc<dyn PhysicalExpr>"
        ),
    }
}
