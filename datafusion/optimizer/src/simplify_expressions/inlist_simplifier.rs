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

//! This module implements a rule that simplifies the values for `InList`s

use super::THRESHOLD_INLINE_INLIST;

use arrow::datatypes::DataType;
use datafusion_common::tree_node::{Transformed, TreeNodeRewriter};
use datafusion_common::{DFSchema, Result};
use datafusion_expr::expr::InList;
use datafusion_expr::{Expr, ExprSchemable};

pub(super) struct ShortenInListSimplifier<'a> {
    schema: &'a DFSchema,
}

impl<'a> ShortenInListSimplifier<'a> {
    pub(super) fn new(schema: &'a DFSchema) -> Self {
        Self { schema }
    }

    /// Keep short literal lists for types with efficient membership filters.
    fn should_retain_short_inlist(&self, expr: &Expr, list: &[Expr]) -> bool {
        let Ok(data_type) = expr.get_type(self.schema) else {
            return false;
        };
        matches!(
            data_type,
            DataType::Int32
                | DataType::Int64
                | DataType::UInt64
                | DataType::Float32
                | DataType::Float64
                | DataType::FixedSizeBinary(1 | 2 | 4 | 8 | 16)
        ) && list.iter().all(|expr| {
            expr.as_literal()
                .is_some_and(|value| value.data_type() == data_type)
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
