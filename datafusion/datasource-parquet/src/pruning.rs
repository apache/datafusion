// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Shared predicate construction for row-group and page-index pruning.

use std::sync::Arc;

use arrow::datatypes::Schema;
use datafusion_expr::Operator;
use datafusion_physical_expr::expressions::{BinaryExpr, IsNullExpr, NotExpr};
use datafusion_physical_expr::utils::collect_columns;
use datafusion_physical_expr::{PhysicalExpr, PhysicalExprSimplifier};
use datafusion_pruning::{PruningPredicate, PruningPredicateBuilder};

/// Build a null-safe inverse used to prove every row matches `predicate`.
///
/// Rows where a filter evaluates to NULL do not pass it, so nullable referenced
/// columns are included in the inverse. If the inverse can be pruned, every row
/// is guaranteed to satisfy the original predicate.
pub(crate) fn build_inverted_predicate(
    predicate: &PruningPredicate,
    arrow_schema: &Schema,
) -> Option<PruningPredicate> {
    // Some pruning rewrites preserve only whether rows can be TRUE, while
    // full-match inference must distinguish FALSE from UNKNOWN.
    if !predicate.can_be_inverted_for_full_match() {
        return None;
    }
    let mut inverted_expr: Arc<dyn PhysicalExpr> =
        Arc::new(NotExpr::new(Arc::clone(predicate.orig_expr())));

    let mut columns = collect_columns(predicate.orig_expr())
        .into_iter()
        .filter(|column| arrow_schema.field(column.index()).is_nullable())
        .collect::<Vec<_>>();
    columns.sort_by(|a, b| {
        a.index()
            .cmp(&b.index())
            .then_with(|| a.name().cmp(b.name()))
    });

    // A matched expression such as `s['a']` can be NULL when its root column
    // is not nullable, so every matched expression gets its own `IS NULL`.
    let mut null_operands: Vec<Arc<dyn PhysicalExpr>> = columns
        .into_iter()
        .map(|column| Arc::new(column) as Arc<dyn PhysicalExpr>)
        .collect();
    null_operands.extend(predicate.stats_key_exprs().iter().cloned());

    for operand in null_operands {
        inverted_expr = Arc::new(BinaryExpr::new(
            inverted_expr,
            Operator::Or,
            Arc::new(IsNullExpr::new(operand)),
        ));
    }

    let simplifier = PhysicalExprSimplifier::new(arrow_schema);
    let inverted_expr = simplifier.simplify(inverted_expr).ok()?;
    PruningPredicateBuilder::new()
        .with_file_schema(Arc::clone(predicate.schema()))
        .with_max_in_list_size(predicate.max_in_list_size())
        .with_statistics_expr_matcher(predicate.statistics_expr_matcher().cloned())
        .try_build(inverted_expr)
        .ok()
}
