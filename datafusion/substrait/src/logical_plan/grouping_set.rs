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
    Column, DFSchema, ScalarValue, internal_datafusion_err, internal_err, not_impl_err,
};
use datafusion::logical_expr::utils::grouping_set_to_exprlist;
use datafusion::logical_expr::{Aggregate, Case, Expr, GroupingSet, lit};

/// The preferred name for the grouping set index, which Substrait leaves to
/// the plan's root names. Both the consumer and the producer only use this
/// literal name as the base candidate passed to
/// [`unique_grouping_set_index_name`]; neither hard-codes it as the name a
/// real schema is guaranteed to accept.
const GROUPING_SET_INDEX: &str = "grouping_set_index";

/// A name for the grouping set index column that is not already taken in
/// `schema`.
///
/// `schema` is a real aggregate's output (the consumer) or that aggregate's
/// `DFSchema` sans `__grouping_id` (the producer), so it can legitimately
/// already contain a column named [`GROUPING_SET_INDEX`] - either a user
/// column with that literal name, or (on the producer side specifically) two
/// joined columns that only collide once reduced to this bare, unqualified
/// name. Falling back to the fixed name regardless would make the synthetic
/// column indistinguishable from that real one, or fail schema construction
/// outright with a duplicate-field error.
pub(crate) fn unique_grouping_set_index_name(schema: &DFSchema) -> String {
    if schema
        .index_of_column_by_name(None, GROUPING_SET_INDEX)
        .is_none()
    {
        return GROUPING_SET_INDEX.to_string();
    }
    let mut suffix = 0u32;
    loop {
        let candidate = format!("{GROUPING_SET_INDEX}_{suffix}");
        if schema.index_of_column_by_name(None, &candidate).is_none() {
            return candidate;
        }
        suffix += 1;
    }
}

/// The `__grouping_id` value DataFusion gives each grouping set, in the order
/// the sets are listed.
///
/// The value is `(ordinal << group_count) | mask`: a bit is set in `mask` for
/// every grouping column the set leaves out, counting from the last column, and
/// `ordinal` counts the sets before this one holding the same columns. Both
/// parts follow from the set alone, so no two sets share a value - *as long as
/// `ordinal` fits in the `64 - group_count` bits above the mask*. `ordinal`
/// grows with how many times a set is repeated, not with `group_count`, so a
/// set repeated enough times can overflow those bits well before
/// `group_count` reaches 64 on its own: with `group_count = 63` only one bit
/// is left for the ordinal, so a third occurrence of the same set (ordinal
/// `2`, i.e. `0b10`) would need to shift that single `1` bit out of the `u64`
/// entirely, silently losing it (`2u64 << 63 == 0`, not an out-of-range
/// shift) and colliding with the first occurrence's id. Checking the shift
/// amount alone (`checked_shl`) does not catch this: `2u64.checked_shl(63)`
/// still returns `Some(0)`, because the amount is in range - only the *value*
/// overflows out of the type, which `checked_shl` does not consider.
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
        let id = if ordinal == 0 {
            // No shift at all, so this is always representable, including
            // the `group_count == 64` case where the mask alone already
            // fills every bit and no ordinal bits are available.
            mask
        } else {
            let ordinal_bits = 64 - group_count;
            // `1u64 << 64` would itself be an out-of-range shift; at
            // `ordinal_bits == 64` (`group_count == 0`) every `u64` ordinal
            // is representable anyway, since shifting by 0 loses nothing.
            let max_ordinal = if ordinal_bits >= 64 {
                u64::MAX
            } else {
                (1u64 << ordinal_bits) - 1
            };
            if ordinal > max_ordinal {
                return not_impl_err!(
                    "A grouping set with {group_count} columns cannot be repeated more than {max_ordinal} time(s): the duplicate ordinal does not fit in the {ordinal_bits} bit(s) left above the mask"
                );
            }
            (ordinal << group_count) | mask
        };
        ids.push(id);
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
    schema: &DFSchema,
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

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::logical_expr::col;

    fn distinct_columns(count: usize) -> Vec<Expr> {
        (0..count).map(|i| col(format!("c{i}"))).collect()
    }

    /// A single grouping set naming every one of 64 columns has mask `0` and
    /// is always the first (ordinal `0`) occurrence of that mask, so the
    /// `u64` id is representable without shifting by the full 64-bit width.
    #[test]
    fn grouping_set_ids_at_64_columns_zero_ordinal() -> datafusion::common::Result<()> {
        let exprs = distinct_columns(64);
        let columns: Vec<&Expr> = exprs.iter().collect();
        let sets = vec![exprs.clone()];

        let ids = grouping_set_ids(&columns, &sets)?;

        assert_eq!(ids, vec![0u64]);
        Ok(())
    }

    /// The same 64-column set repeated needs ordinal `1`, which does not fit
    /// in a `u64` alongside a full 64-bit mask. This must be a clean error,
    /// not a shift-overflow panic.
    #[test]
    fn grouping_set_ids_rejects_repeated_set_at_64_columns() {
        let exprs = distinct_columns(64);
        let columns: Vec<&Expr> = exprs.iter().collect();
        let sets = vec![exprs.clone(), exprs.clone()];

        let err = grouping_set_ids(&columns, &sets).unwrap_err();

        assert!(
            err.to_string().contains("cannot be repeated"),
            "unexpected error: {err}"
        );
    }

    /// 65 columns is rejected on its own, independent of any repeated set.
    #[test]
    fn grouping_set_ids_rejects_more_than_64_columns() {
        let exprs = distinct_columns(65);
        let columns: Vec<&Expr> = exprs.iter().collect();
        let sets = vec![exprs.clone()];

        let err = grouping_set_ids(&columns, &sets).unwrap_err();

        assert!(
            err.to_string().contains("more than 64 columns"),
            "unexpected error: {err}"
        );
    }

    /// At 63 columns exactly one bit is left for the ordinal, so a second
    /// occurrence of the same set (ordinal `1`) is representable: both ids
    /// fit in the `u64` without losing any bits.
    #[test]
    fn grouping_set_ids_ordinal_fits_at_63_columns() -> datafusion::common::Result<()> {
        let exprs = distinct_columns(63);
        let columns: Vec<&Expr> = exprs.iter().collect();
        let sets = vec![exprs.clone(), exprs.clone()];

        let ids = grouping_set_ids(&columns, &sets)?;

        assert_eq!(ids, vec![0u64, 1u64 << 63]);
        Ok(())
    }

    /// A third occurrence of the same 63-column set needs ordinal `2`
    /// (`0b10`), which does not fit in the single bit left above the mask:
    /// `2u64 << 63` silently discards the bit instead of producing an
    /// out-of-range-shift panic, so this must be a checked, clean error - not
    /// a value that happens to collide with another set's id.
    #[test]
    fn grouping_set_ids_rejects_ordinal_overflow_at_63_columns() {
        let exprs = distinct_columns(63);
        let columns: Vec<&Expr> = exprs.iter().collect();
        let sets = vec![exprs.clone(), exprs.clone(), exprs.clone()];

        let err = grouping_set_ids(&columns, &sets).unwrap_err();

        assert!(
            err.to_string().contains("cannot be repeated"),
            "unexpected error: {err}"
        );
    }

    /// At 62 columns two bits are left for the ordinal, so up to four
    /// occurrences (ordinals `0..=3`) of the same set are representable.
    #[test]
    fn grouping_set_ids_ordinal_fits_at_62_columns() -> datafusion::common::Result<()> {
        let exprs = distinct_columns(62);
        let columns: Vec<&Expr> = exprs.iter().collect();
        let sets = vec![exprs.clone(), exprs.clone(), exprs.clone(), exprs.clone()];

        let ids = grouping_set_ids(&columns, &sets)?;

        assert_eq!(ids, vec![0u64, 1u64 << 62, 2u64 << 62, 3u64 << 62]);
        Ok(())
    }

    /// A fifth occurrence at 62 columns needs ordinal `4` (`0b100`), which
    /// overflows the two bits available.
    #[test]
    fn grouping_set_ids_rejects_ordinal_overflow_at_62_columns() {
        let exprs = distinct_columns(62);
        let columns: Vec<&Expr> = exprs.iter().collect();
        let sets = vec![
            exprs.clone(),
            exprs.clone(),
            exprs.clone(),
            exprs.clone(),
            exprs.clone(),
        ];

        let err = grouping_set_ids(&columns, &sets).unwrap_err();

        assert!(
            err.to_string().contains("cannot be repeated"),
            "unexpected error: {err}"
        );
    }
}
