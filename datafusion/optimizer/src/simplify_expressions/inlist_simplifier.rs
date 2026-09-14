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

use arrow::array::MAX_INLINE_VIEW_LEN;
use arrow::datatypes::{DataType, IntervalUnit, TimeUnit};
use datafusion_common::tree_node::{Transformed, TreeNodeRewriter};
use datafusion_common::{Result, ScalarValue};
use datafusion_expr::Expr;
use datafusion_expr::expr::InList;
use datafusion_expr::simplify::SimplifyContext;

pub(super) struct ShortenInListSimplifier<'a> {
    info: &'a SimplifyContext,
}

impl<'a> ShortenInListSimplifier<'a> {
    pub(super) fn new(info: &'a SimplifyContext) -> Self {
        Self { info }
    }

    /// Returns true when the physical `IN` expression has a specialized fast
    /// path that is preferable to expanding a short static list into ORs.
    fn has_specialized_static_filter(&self, expr: &Expr, list: &[Expr]) -> bool {
        if !list.iter().all(|expr| matches!(expr, Expr::Literal(_, _))) {
            return false;
        }

        // Valid optimizer inputs have already been type-coerced, so the tested
        // expression determines the physical list representation.
        let Ok(data_type) = self.info.get_data_type(expr) else {
            // Type errors are reported elsewhere. Preserve the existing
            // shortening behavior instead of making simplification fail.
            return false;
        };
        supports_specialized_static_filter(&data_type, list)
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
                        && !self.has_specialized_static_filter(expr, list)
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

/// Mirrors the specialized physical filters used by `InListExpr`.
///
/// Keep this type and representation match synchronized with the primitive,
/// fixed-size-binary, and byte-view selectors under
/// `datafusion/physical-expr/src/expressions/in_list/`.
fn supports_specialized_static_filter(data_type: &DataType, list: &[Expr]) -> bool {
    let data_type = dictionary_value_type(data_type);
    match data_type {
        DataType::Int8
        | DataType::UInt8
        | DataType::Int16
        | DataType::UInt16
        | DataType::Float16
        | DataType::Int32
        | DataType::UInt32
        | DataType::Float32
        | DataType::Date32
        | DataType::Int64
        | DataType::UInt64
        | DataType::Float64
        | DataType::Date64
        | DataType::Timestamp(_, _)
        | DataType::Duration(_)
        | DataType::Decimal128(_, _)
        | DataType::Interval(IntervalUnit::MonthDayNano) => true,
        DataType::Time32(TimeUnit::Second | TimeUnit::Millisecond)
        | DataType::Time64(TimeUnit::Microsecond | TimeUnit::Nanosecond) => true,
        DataType::FixedSizeBinary(width) => matches!(*width, 1 | 2 | 4 | 8 | 16),
        DataType::Utf8View | DataType::BinaryView => {
            list.iter().all(|expr| inline_view_literal(expr, data_type))
        }
        _ => false,
    }
}

fn dictionary_value_type(mut data_type: &DataType) -> &DataType {
    while let DataType::Dictionary(_, value_type) = data_type {
        data_type = value_type;
    }
    data_type
}

fn inline_view_literal(expr: &Expr, data_type: &DataType) -> bool {
    let Expr::Literal(value, _) = expr else {
        return false;
    };
    let value = dictionary_scalar_value(value);
    if value.is_null() {
        return true;
    }

    let len = match (data_type, value) {
        (DataType::Utf8View, ScalarValue::Utf8View(Some(value))) => value.len(),
        (DataType::BinaryView, ScalarValue::BinaryView(Some(value))) => value.len(),
        _ => return false,
    };
    len <= MAX_INLINE_VIEW_LEN as usize
}

fn dictionary_scalar_value(mut value: &ScalarValue) -> &ScalarValue {
    while let ScalarValue::Dictionary(_, dictionary_value) = value {
        value = dictionary_value;
    }
    value
}
