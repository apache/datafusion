// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! This module implements a rule that simplifies the values for `InList`s

use super::THRESHOLD_INLINE_INLIST;

use arrow::datatypes::DataType;
use datafusion_common::tree_node::{Transformed, TreeNodeRewriter};
use datafusion_common::{DFSchema, Result, ScalarValue};
use datafusion_expr::expr::InList;
use datafusion_expr::{Expr, ExprSchemable};

pub(super) struct ShortenInListSimplifier<'a> {
    schema: &'a DFSchema,
}

impl<'a> ShortenInListSimplifier<'a> {
    pub(super) fn new(schema: &'a DFSchema) -> Self {
        Self { schema }
    }

    /// Benchmarks in `in_list_rewrite` favor evaluating short literal lists as
    /// membership expressions over OR/AND chains for these types on batches with
    /// a mix of matches and misses, or no matches. Integer chains can still be
    /// faster when every row matches the first value, allowing the remaining
    /// comparisons to short circuit.
    ///
    /// See [PR #25187](https://github.com/apache/datafusion/pull/25187) for measurements.
    fn should_retain_short_inlist(&self, expr: &Expr, list: &[Expr]) -> bool {
        let Ok(data_type) = expr.get_type(self.schema) else {
            return false;
        };
        let is_float = matches!(data_type, DataType::Float32 | DataType::Float64);
        (is_float
            || matches!(
                data_type,
                DataType::Int32
                    | DataType::Int64
                    | DataType::UInt64
                    | DataType::FixedSizeBinary(1 | 2 | 4 | 8 | 16)
            ))
            && list.iter().all(|expr| match expr.as_literal() {
                // The membership filters compare floats by bit pattern, so
                // -0.0 and +0.0 would not match each other, while `=` treats
                // them as equal. Keep the comparison chain when a zero is in
                // the list so both forms agree.
                Some(ScalarValue::Float32(Some(v))) if is_float => *v != 0.0,
                Some(ScalarValue::Float64(Some(v))) if is_float => *v != 0.0,
                Some(_) => true,
                None => false,
            })
    }
}

impl TreeNodeRewriter for ShortenInListSimplifier<'_> {
    type Node = Expr;

    fn f_up(&mut self, expr: Expr) -> Result<Transformed<Expr>> {
        // Rewrite eligible short lists to left-deep comparison chains:
        // expr IN (A, B, ...) --> (expr = A) OR (expr = B) OR (expr = C)
        if let Expr::InList(InList {
            ref expr,
            ref list,
            negated,
        }) = expr
            && !list.is_empty()
            && (
                // For lists with only 1 value we allow more complex expressions to be simplified
                // e.g SUBSTR(c1, 2, 3) IN ('1') -> SUBSTR(c1, 2, 3) = '1'
                // for more than one we avoid repeating this potentially expensive
                // expressions
                list.len() == 1
                    || list.len() <= THRESHOLD_INLINE_INLIST
                        && expr.try_as_col().is_some()
                        && !self.should_retain_short_inlist(expr, list)
            )
        {
            let first_val = list[0].clone();
            if negated {
                return Ok(Transformed::yes(list.iter().skip(1).cloned().fold(
                    (*expr.clone()).not_eq(first_val),
                    |acc, y| {
                        // Note that `A and B and C and D` is a left-deep tree structure
                        // as such we want to maintain this structure as much as possible
                        // to avoid reordering the expression during each optimization
                        // pass.
                        //
                        // Left-deep tree structure for `A and B and C and D`:
                        // ```
                        //        &
                        //       / \
                        //      &   D
                        //     / \
                        //    &   C
                        //   / \
                        //  A   B
                        // ```
                        //
                        // The code below maintain the left-deep tree structure.
                        acc.and((*expr.clone()).not_eq(y))
                    },
                )));
            } else {
                return Ok(Transformed::yes(list.iter().skip(1).cloned().fold(
                    (*expr.clone()).eq(first_val),
                    |acc, y| {
                        // Same reasoning as above
                        acc.or((*expr.clone()).eq(y))
                    },
                )));
            }
        }

        Ok(Transformed::no(expr))
    }
}
