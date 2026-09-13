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

//! Reading timezone-naive timestamps in the session time zone when they are
//! *implicitly* converted to a timezone-aware type.
//!
//! # The model
//!
//! A `Timestamp(unit, Some(tz))` value is an instant; `tz` is only a *display
//! label* attached to it. A `Timestamp(unit, None)` value is a wall clock
//! reading with no instant attached. Turning the second into the first requires
//! picking a zone to read the wall clock in, and that choice is what decides
//! which instant you get.
//!
//! PostgreSQL and DuckDB always read it in the *session* time zone, never in
//! the zone that happens to label the other operand. DataFusion's type
//! coercion, by contrast, produces a plain
//! `CAST(naive AS Timestamp(unit, Some(other_tz)))`, and arrow's cast reads the
//! naive value in `other_tz`. [`cast_to_with_session_time_zone`] restores the
//! PostgreSQL behaviour by splitting that cast in two:
//!
//! ```text
//! CAST(ts AS Timestamp(ns, "America/New_York"))
//! -- becomes, with datafusion.execution.time_zone = '+08:00'
//! CAST(CAST(ts AS Timestamp(ns, "+08:00")) AS Timestamp(ns, "America/New_York"))
//! ```
//!
//! The inner cast reads the wall clock in the session zone, fixing the instant.
//! The outer cast is instant-preserving (an aware -> aware cast only changes the
//! display label), so the coerced *type* is unchanged and only the chosen
//! instant differs.
//!
//! # Where it applies
//!
//! Only where the planner or the analyzer *inserts* a cast on the user's
//! behalf: comparisons, arithmetic, `IN`, `BETWEEN`, `CASE`, `UNION`, function
//! arguments, `VALUES`, `INSERT`, ... . Every one of those call sites builds its
//! cast with [`cast_to_with_session_time_zone`] rather than calling
//! [`ExprSchemable::cast_to`] directly. The three library functions that insert
//! a cast into a whole plan instead of into one expression have a sibling that
//! does the same:
//! [`coerce_plan_expr_for_schema_with_session_time_zone`](crate::expr_rewriter::coerce_plan_expr_for_schema_with_session_time_zone),
//! [`cast_subquery_with_session_time_zone`](crate::expr_schema::cast_subquery_with_session_time_zone)
//! and
//! [`LogicalPlanBuilder::values_with_session_time_zone`](crate::LogicalPlanBuilder::values_with_session_time_zone).
//!
//! Splitting the cast where it is inserted, rather than looking for inserted
//! casts afterwards, is what makes the implicit/explicit distinction exact: an
//! expression that already has the target type is never cast, so a cast the user
//! wrote is never mistaken for one DataFusion inserted.
//!
//! # What it does *not* touch
//!
//! Explicit conversions keep arrow's semantics, which is also what PostgreSQL
//! and DuckDB do for their explicit forms:
//!
//! * `ts AT TIME ZONE 'America/New_York'` reads the wall clock in New York,
//!   whatever the session zone is.
//! * `arrow_cast(ts, 'Timestamp(Nanosecond, Some("America/New_York"))')`, the
//!   DataFrame API's `cast_to`/`cast` and Substrait casts all keep meaning
//!   exactly the arrow cast they name.
//! * SQL `CAST(ts AS TIMESTAMPTZ)` is already planned with the session time zone
//!   as its target, so the target zone equals the session zone and there is
//!   nothing to split.
//!
//! # A no-op by default
//!
//! `datafusion.execution.time_zone` is unset by default. With no session zone
//! there is no wall clock zone to prefer, so every function here degrades to a
//! plain [`ExprSchemable::cast_to`] and plans are unchanged.

use std::sync::Arc;

use arrow::datatypes::{DataType, FieldRef};
use datafusion_common::{ExprSchema, Result};

use crate::{Expr, ExprSchemable};

/// Wraps `expr` in a cast to `cast_to_type`, reading a timezone-naive timestamp
/// in `session_time_zone` if the cast makes one timezone-aware.
///
/// This is the [`ExprSchemable::cast_to`] to use for a cast that DataFusion
/// inserts on the user's behalf. It behaves exactly like `cast_to` except when
/// all of the following hold, in which case it emits the two step cast
/// described in the [module documentation](self):
///
/// * `session_time_zone` is set (`datafusion.execution.time_zone`),
/// * the cast turns a timezone-naive timestamp — possibly nested inside a
///   `List`, `LargeList`, `FixedSizeList`, `Struct`, `Map` or `Dictionary` —
///   into a timezone-aware one,
/// * and the target zone is not already the session zone.
///
/// The returned expression always has type `cast_to_type`; only the instant the
/// naive wall clock is read at changes.
///
/// # Errors
///
/// As [`ExprSchemable::cast_to`]: when `expr` cannot be typed against `schema`,
/// or cannot be cast to `cast_to_type`.
pub fn cast_to_with_session_time_zone(
    expr: Expr,
    cast_to_type: &DataType,
    schema: &dyn ExprSchema,
    session_time_zone: Option<&str>,
) -> Result<Expr> {
    let Some(time_zone) = session_time_zone else {
        return expr.cast_to(cast_to_type, schema);
    };
    let source_type = expr.get_type(schema)?;
    let Some(intermediate) = session_zoned_type(&source_type, cast_to_type, time_zone)
    else {
        return expr.cast_to(cast_to_type, schema);
    };
    expr.cast_to(&intermediate, schema)?
        .cast_to(cast_to_type, schema)
}

/// The intermediate cast target: `target` with every timezone-naive leaf of
/// `source` that `target` makes timezone-aware re-zoned to `time_zone`.
///
/// `None` when there is no such leaf, i.e. when the cast already reads the naive
/// value in the session time zone or does not read one at all.
fn session_zoned_type(
    source: &DataType,
    target: &DataType,
    time_zone: &str,
) -> Option<DataType> {
    match (source, target) {
        (DataType::Timestamp(_, None), DataType::Timestamp(unit, Some(target_tz))) => {
            (target_tz.as_ref() != time_zone)
                .then(|| DataType::Timestamp(*unit, Some(Arc::from(time_zone))))
        }
        (DataType::List(source), DataType::List(target)) => {
            Some(DataType::List(rezoned_field(source, target, time_zone)?))
        }
        (DataType::LargeList(source), DataType::LargeList(target)) => Some(
            DataType::LargeList(rezoned_field(source, target, time_zone)?),
        ),
        (DataType::ListView(source), DataType::ListView(target)) => Some(
            DataType::ListView(rezoned_field(source, target, time_zone)?),
        ),
        (DataType::LargeListView(source), DataType::LargeListView(target)) => Some(
            DataType::LargeListView(rezoned_field(source, target, time_zone)?),
        ),
        (
            DataType::FixedSizeList(source, source_len),
            DataType::FixedSizeList(target, target_len),
        ) if source_len == target_len => Some(DataType::FixedSizeList(
            rezoned_field(source, target, time_zone)?,
            *target_len,
        )),
        (DataType::Struct(source), DataType::Struct(target))
            if source.len() == target.len() =>
        {
            let mut changed = false;
            let fields = source
                .iter()
                .zip(target.iter())
                .map(
                    |(source, target)| match rezoned_field(source, target, time_zone) {
                        Some(field) => {
                            changed = true;
                            field
                        }
                        None => Arc::clone(target),
                    },
                )
                .collect::<Vec<_>>();
            changed.then(|| DataType::Struct(fields.into()))
        }
        // A `Map` is a list of a two field (key, value) struct; recursing into
        // that entries field covers both the keys and the values.
        (
            DataType::Map(source_entries, _),
            DataType::Map(target_entries, target_sorted),
        ) => Some(DataType::Map(
            rezoned_field(source_entries, target_entries, time_zone)?,
            *target_sorted,
        )),
        (
            DataType::Dictionary(_, source_value),
            DataType::Dictionary(target_key, target_value),
        ) => Some(DataType::Dictionary(
            target_key.clone(),
            Box::new(session_zoned_type(source_value, target_value, time_zone)?),
        )),
        // Other nested types (`Union`, `RunEndEncoded`, ...) are not rewritten:
        // type coercion does not produce a naive -> aware cast through them, and
        // an unhandled type simply keeps DataFusion's pre-existing behaviour.
        _ => None,
    }
}

fn rezoned_field(
    source: &FieldRef,
    target: &FieldRef,
    time_zone: &str,
) -> Option<FieldRef> {
    let data_type =
        session_zoned_type(source.data_type(), target.data_type(), time_zone)?;
    Some(Arc::new(target.as_ref().clone().with_data_type(data_type)))
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::collections::HashMap;

    use arrow::datatypes::{Field, Fields, TimeUnit};
    use datafusion_common::{DFSchema, ScalarValue};
    use insta::assert_snapshot;

    use crate::expr::Cast;
    use crate::{col, lit};

    const NY: &str = "America/New_York";
    const SESSION: &str = "+08:00";

    fn naive(unit: TimeUnit) -> DataType {
        DataType::Timestamp(unit, None)
    }

    fn aware(unit: TimeUnit, tz: &str) -> DataType {
        DataType::Timestamp(unit, Some(Arc::from(tz)))
    }

    fn schema_with(name: &str, data_type: DataType) -> DFSchema {
        DFSchema::from_unqualified_fields(
            vec![Field::new(name, data_type, true)].into(),
            HashMap::new(),
        )
        .unwrap()
    }

    /// Casts `col("ts")`, typed as `source`, to `target`.
    fn cast(source: DataType, target: &DataType, tz: Option<&str>) -> Expr {
        let schema = schema_with("ts", source);
        cast_to_with_session_time_zone(col("ts"), target, &schema, tz).unwrap()
    }

    /// The data type of the cast nested inside `expr`, which must be a cast of
    /// a cast.
    fn inner_cast_type(expr: &Expr) -> &DataType {
        let Expr::Cast(Cast { expr: inner, .. }) = expr else {
            panic!("expected a cast, got {expr}");
        };
        let Expr::Cast(Cast { field, .. }) = inner.as_ref() else {
            panic!("expected a nested cast, got {inner}");
        };
        field.data_type()
    }

    #[test]
    fn naive_to_aware_cast_is_split() {
        assert_snapshot!(
            cast(naive(TimeUnit::Nanosecond), &aware(TimeUnit::Nanosecond, NY), Some(SESSION)),
            @r#"CAST(CAST(ts AS Timestamp(ns, "+08:00")) AS Timestamp(ns, "America/New_York"))"#
        );
    }

    #[test]
    fn unset_session_time_zone_is_a_no_op() {
        assert_snapshot!(
            cast(naive(TimeUnit::Nanosecond), &aware(TimeUnit::Nanosecond, NY), None),
            @r#"CAST(ts AS Timestamp(ns, "America/New_York"))"#
        );
    }

    #[test]
    fn cast_to_session_zone_is_untouched() {
        assert_snapshot!(
            cast(naive(TimeUnit::Nanosecond), &aware(TimeUnit::Nanosecond, SESSION), Some(SESSION)),
            @r#"CAST(ts AS Timestamp(ns, "+08:00"))"#
        );
    }

    #[test]
    fn aware_to_aware_cast_is_untouched() {
        assert_snapshot!(
            cast(aware(TimeUnit::Nanosecond, "UTC"), &aware(TimeUnit::Nanosecond, NY), Some(SESSION)),
            @r#"CAST(ts AS Timestamp(ns, "America/New_York"))"#
        );
    }

    #[test]
    fn aware_to_naive_cast_is_untouched() {
        assert_snapshot!(
            cast(aware(TimeUnit::Nanosecond, NY), &naive(TimeUnit::Nanosecond), Some(SESSION)),
            @"CAST(ts AS Timestamp(ns))"
        );
    }

    #[test]
    fn non_timestamp_cast_is_untouched() {
        assert_snapshot!(
            cast(DataType::Int32, &DataType::Int64, Some(SESSION)),
            @"CAST(ts AS Int64)"
        );
    }

    /// A cast whose source already has the target type is not a cast at all.
    #[test]
    fn identity_cast_is_untouched() {
        assert_snapshot!(
            cast(aware(TimeUnit::Nanosecond, NY), &aware(TimeUnit::Nanosecond, NY), Some(SESSION)),
            @"ts"
        );
    }

    /// The naive value is truncated to the target unit by the inner cast, as a
    /// single cast would have done.
    #[test]
    fn unit_change_happens_in_the_inner_cast() {
        assert_snapshot!(
            cast(naive(TimeUnit::Nanosecond), &aware(TimeUnit::Second, NY), Some(SESSION)),
            @r#"CAST(CAST(ts AS Timestamp(s, "+08:00")) AS Timestamp(s, "America/New_York"))"#
        );
    }

    #[test]
    fn literal_cast_is_split() {
        let schema = DFSchema::empty();
        let expr = lit(ScalarValue::TimestampNanosecond(Some(0), None));
        assert_snapshot!(
            cast_to_with_session_time_zone(
                expr,
                &aware(TimeUnit::Nanosecond, NY),
                &schema,
                Some(SESSION),
            )
            .unwrap(),
            @r#"CAST(CAST(TimestampNanosecond(0, None) AS Timestamp(ns, "+08:00")) AS Timestamp(ns, "America/New_York"))"#
        );
    }

    #[test]
    fn nested_list_is_split() {
        let list =
            |inner: DataType| DataType::List(Arc::new(Field::new("item", inner, true)));
        assert_snapshot!(
            cast(
                list(naive(TimeUnit::Nanosecond)),
                &list(aware(TimeUnit::Nanosecond, NY)),
                Some(SESSION),
            ),
            @r#"CAST(CAST(ts AS List(Timestamp(ns, "+08:00"))) AS List(Timestamp(ns, "America/New_York")))"#
        );
    }

    #[test]
    fn nested_fixed_size_list_is_split() {
        let list = |inner: DataType| {
            DataType::FixedSizeList(Arc::new(Field::new("item", inner, true)), 2)
        };
        let expr = cast(
            list(naive(TimeUnit::Nanosecond)),
            &list(aware(TimeUnit::Nanosecond, NY)),
            Some(SESSION),
        );
        assert_eq!(
            inner_cast_type(&expr),
            &list(aware(TimeUnit::Nanosecond, SESSION))
        );
    }

    #[test]
    fn nested_struct_is_split() {
        let strukt = |inner: DataType| {
            DataType::Struct(Fields::from(vec![
                Field::new("i", DataType::Int32, true),
                Field::new("ts", inner, true),
            ]))
        };
        let expr = cast(
            strukt(naive(TimeUnit::Nanosecond)),
            &strukt(aware(TimeUnit::Nanosecond, NY)),
            Some(SESSION),
        );
        assert_eq!(
            inner_cast_type(&expr),
            &strukt(aware(TimeUnit::Nanosecond, SESSION))
        );
    }

    #[test]
    fn nested_dictionary_is_split() {
        let dict = |inner: DataType| {
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(inner))
        };
        let expr = cast(
            dict(naive(TimeUnit::Nanosecond)),
            &dict(aware(TimeUnit::Nanosecond, NY)),
            Some(SESSION),
        );
        assert_eq!(
            inner_cast_type(&expr),
            &dict(aware(TimeUnit::Nanosecond, SESSION))
        );
    }

    #[test]
    fn nested_map_is_split() {
        let map = |inner: DataType| {
            DataType::Map(
                Arc::new(Field::new(
                    "entries",
                    DataType::Struct(Fields::from(vec![
                        Field::new("key", DataType::Utf8, false),
                        Field::new("value", inner, true),
                    ])),
                    false,
                )),
                false,
            )
        };
        let expr = cast(
            map(naive(TimeUnit::Nanosecond)),
            &map(aware(TimeUnit::Nanosecond, NY)),
            Some(SESSION),
        );
        assert_eq!(
            inner_cast_type(&expr),
            &map(aware(TimeUnit::Nanosecond, SESSION))
        );
    }
}
