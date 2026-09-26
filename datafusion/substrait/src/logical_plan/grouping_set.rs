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

//! The column a multi-set aggregate ends with, which Substrait and DataFusion
//! fill differently.
//!
//! Substrait gives an [`AggregateRel`] with more than one grouping set a
//! trailing `i32` holding "the zero-based index of the grouping set that
//! yielded the record" ([Aggregate Operation]). DataFusion ends the same
//! aggregate with `__grouping_id`, which packs a bitmask of the columns the set
//! leaves out together with an ordinal separating repeated sets. Both identify
//! the set a row came from, so each side can be written as a map of the other,
//! which is what the consumer and the producer apply.
//!
//! [`AggregateRel`]: substrait::proto::AggregateRel
//! [Aggregate Operation]: https://substrait.io/relations/logical_relations/#aggregate-operation

use datafusion::arrow::datatypes::DataType;
use datafusion::common::{
    Column, ScalarValue, internal_datafusion_err, internal_err, not_impl_err,
};
use datafusion::logical_expr::utils::grouping_set_to_exprlist;
use datafusion::logical_expr::{Aggregate, Case, Expr, GroupingSet, lit};

/// The name the grouping set index is given, which Substrait leaves to the
/// plan's root names.
pub(crate) const GROUPING_SET_INDEX: &str = "grouping_set_index";

/// The `__grouping_id` value DataFusion gives each grouping set, in the order
/// the sets are listed.
///
/// The value is `(ordinal << group_count) | mask`: a bit is set in `mask` for
/// every grouping column the set leaves out, counting from the last column, and
/// `ordinal` counts the sets before this one holding the same columns. Both
/// parts follow from the set alone, so no two sets share a value.
pub(crate) fn grouping_set_ids(
    columns: &[&Expr],
    sets: &[Vec<Expr>],
) -> datafusion::common::Result<Vec<u64>> {
    let group_count = columns.len();
    if group_count > 64 {
        return not_impl_err!(
            "Grouping sets with more than 64 columns are not supported"
        );
    }

    let mut ids = Vec::with_capacity(sets.len());
    let mut masks = Vec::with_capacity(sets.len());
    for set in sets {
        let mut mask = 0u64;
        for (position, column) in columns.iter().enumerate() {
            if !set.contains(column) {
                mask |= 1 << (group_count - 1 - position);
            }
        }
        let ordinal = masks.iter().filter(|seen| **seen == mask).count() as u64;
        masks.push(mask);
        ids.push((ordinal << group_count) | mask);
    }
    Ok(ids)
}

/// The grouping sets of an aggregate DataFusion built from `GROUPING SETS`.
pub(crate) fn grouping_sets_of(
    group_exprs: &[Expr],
) -> datafusion::common::Result<&Vec<Vec<Expr>>> {
    let [Expr::GroupingSet(GroupingSet::GroupingSets(sets))] = group_exprs else {
        return internal_err!(
            "Expected a single GROUPING SETS expression, got {group_exprs:?}"
        );
    };
    Ok(sets)
}

/// The grouping columns, in the order DataFusion's aggregate schema holds them.
pub(crate) fn grouping_set_columns(
    group_exprs: &[Expr],
) -> datafusion::common::Result<Vec<&Expr>> {
    grouping_set_to_exprlist(group_exprs)
}

/// `CASE WHEN <index> = 0 THEN ids[0] ... ELSE ids[last] END`, mapping the
/// grouping set index to DataFusion's `__grouping_id`.
pub(crate) fn grouping_id_from_index(
    index: &Expr,
    grouping_id_type: &DataType,
    ids: &[u64],
) -> datafusion::common::Result<Expr> {
    let ids = ids
        .iter()
        .map(|id| grouping_id_literal(*id, grouping_id_type))
        .collect::<datafusion::common::Result<Vec<_>>>()?;
    let Some((last, rest)) = ids.split_last() else {
        return internal_err!("Grouping set aggregate has no grouping sets");
    };
    case_over(
        rest.iter()
            .enumerate()
            .map(|(position, id)| {
                Ok((index.clone().eq(lit(index_literal(position)?)), id.clone()))
            })
            .collect::<datafusion::common::Result<Vec<_>>>()?,
        last.clone(),
    )
}

/// `CASE WHEN <grouping_id> = ids[0] THEN 0 ... ELSE last END`, mapping
/// DataFusion's `__grouping_id` to the grouping set index.
pub(crate) fn index_from_grouping_id(
    grouping_id: &Expr,
    grouping_id_type: &DataType,
    ids: &[u64],
) -> datafusion::common::Result<Expr> {
    let Some((_, rest)) = ids.split_last() else {
        return internal_err!("Grouping set aggregate has no grouping sets");
    };
    let when_then = rest
        .iter()
        .enumerate()
        .map(|(position, id)| {
            let id = grouping_id_literal(*id, grouping_id_type)?;
            Ok((grouping_id.clone().eq(id), lit(index_literal(position)?)))
        })
        .collect::<datafusion::common::Result<Vec<_>>>()?;
    case_over(when_then, lit(index_literal(rest.len())?))
}

/// The `CASE` both maps are written as. The last arm is the `ELSE`: the values
/// are exhaustive, and an `ELSE` keeps the result non-nullable, which is what
/// Substrait requires of the index and DataFusion of `__grouping_id`.
fn case_over(
    when_then: Vec<(Expr, Expr)>,
    else_expr: Expr,
) -> datafusion::common::Result<Expr> {
    if when_then.is_empty() {
        // A single grouping set carries no index column, so both callers stop
        // before reaching this.
        return Ok(else_expr);
    }
    Ok(Expr::Case(Case {
        expr: None,
        when_then_expr: when_then
            .into_iter()
            .map(|(when, then)| (Box::new(when), Box::new(then)))
            .collect(),
        else_expr: Some(Box::new(else_expr)),
    }))
}

fn index_literal(position: usize) -> datafusion::common::Result<i32> {
    i32::try_from(position).map_err(|_| {
        internal_datafusion_err!("More grouping sets than an i32 index can hold")
    })
}

/// A literal of the integer type [`Aggregate::grouping_id_type`] sized to the
/// number of grouping columns.
fn grouping_id_literal(
    id: u64,
    grouping_id_type: &DataType,
) -> datafusion::common::Result<Expr> {
    let value = match grouping_id_type {
        DataType::UInt8 => ScalarValue::UInt8(Some(id as u8)),
        DataType::UInt16 => ScalarValue::UInt16(Some(id as u16)),
        DataType::UInt32 => ScalarValue::UInt32(Some(id as u32)),
        DataType::UInt64 => ScalarValue::UInt64(Some(id)),
        other => {
            return internal_err!(
                "Unexpected {} type: {other}",
                Aggregate::INTERNAL_GROUPING_ID
            );
        }
    };
    Ok(lit(value))
}

/// The column DataFusion's aggregate schema holds `__grouping_id` in.
pub(crate) fn grouping_id_column(
    schema: &datafusion::common::DFSchema,
) -> datafusion::common::Result<(usize, Column)> {
    let Some(index) =
        schema.index_of_column_by_name(None, Aggregate::INTERNAL_GROUPING_ID)
    else {
        return internal_err!(
            "Grouping set aggregate schema is missing {}",
            Aggregate::INTERNAL_GROUPING_ID
        );
    };
    Ok((index, Column::from(schema.qualified_field(index))))
}
