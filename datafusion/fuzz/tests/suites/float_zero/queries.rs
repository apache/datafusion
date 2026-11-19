// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! The queries, and when each is known to give a wrong result.

use super::settings::Setting;

/// A query. `{t}` is replaced by the table name and `{z}` by a zero.
#[derive(Debug)]
pub struct Template {
    pub name: &'static str,
    pub sql: &'static str,
    /// Whether the ORDER BY fixes the order of every row. If not, rows are
    /// compared in any order.
    pub ordered: bool,
    pub known_failure: Option<KnownFailure>,
}

/// When a query is known to give a wrong result. Remove it when the bug is
/// fixed; the fixed-table test fails until then.
#[derive(Debug)]
pub struct KnownFailure {
    pub applies: fn(&Setting) -> bool,
    pub issue: &'static str,
}

impl Template {
    /// The query for `table`, with each zero written as `zero`.
    pub fn render(&self, table: &str, zero: &str) -> String {
        self.sql.replace("{t}", table).replace("{z}", zero)
    }

    /// The issue of the known failure under `setting`, if any.
    pub fn known_issue(&self, setting: &Setting) -> Option<&'static str> {
        self.known_failure
            .as_ref()
            .filter(|known| (known.applies)(setting))
            .map(|known| known.issue)
    }

    const fn fails(self, applies: fn(&Setting) -> bool, issue: &'static str) -> Self {
        Template {
            known_failure: Some(KnownFailure { applies, issue }),
            ..self
        }
    }
}

const fn template(name: &'static str, sql: &'static str, ordered: bool) -> Template {
    Template {
        name,
        sql,
        ordered,
        known_failure: None,
    }
}

fn always(_: &Setting) -> bool {
    true
}

/// A sort-merge join is only used when the data is split up.
fn sort_merge_join(setting: &Setting) -> bool {
    !setting.prefer_hash_join && setting.target_partitions > 1
}

const FLOAT_SEMANTICS: &str = "https://github.com/apache/datafusion/issues/26138";
const WINDOW: &str = "https://github.com/apache/datafusion/issues/25766";
const SORT_MERGE_JOIN: &str = "https://github.com/apache/datafusion/issues/26115";

/// Rules for adding a query:
///
/// - It has exactly one right answer. An ORDER BY that picks or numbers rows
///   (LIMIT, `first_value`, DISTINCT ON, `row_number`) ends with `id` or `b`
///   to break ties. `rank` and RANGE frames keep their ties on purpose.
/// - No math on float columns, which can turn `0.0` into `-0.0`, and no
///   functions that can tell the two apart, like `1 / a`.
/// - `ordered` only when the ORDER BY fixes the order of every row.
pub const TEMPLATES: &[Template] = &[
    // Sort
    template("order_by_a_b", "SELECT a, b FROM {t} ORDER BY a, b", true)
        .fails(always, FLOAT_SEMANTICS),
    template(
        "order_by_a_b_limit",
        "SELECT a, b FROM {t} ORDER BY a, b LIMIT 1",
        true,
    )
    .fails(always, FLOAT_SEMANTICS),
    template("order_by_k", "SELECT id FROM {t} ORDER BY k, id", true)
        .fails(always, FLOAT_SEMANTICS),
    // Window
    template(
        "rank",
        "SELECT id, rank() OVER (ORDER BY a) FROM {t}",
        false,
    )
    .fails(always, WINDOW),
    template(
        "range_frame",
        "SELECT id, sum(b) OVER (ORDER BY a RANGE BETWEEN CURRENT ROW AND CURRENT ROW) FROM {t}",
        false,
    )
    .fails(always, WINDOW),
    template(
        "row_number_partition",
        "SELECT id, row_number() OVER (PARTITION BY a ORDER BY id) FROM {t}",
        false,
    )
    .fails(always, WINDOW),
    template(
        "first_value",
        "SELECT first_value(b ORDER BY a, b) FROM {t}",
        false,
    )
    .fails(always, FLOAT_SEMANTICS),
    template(
        "count_distinct_window",
        "SELECT DISTINCT count(DISTINCT a) OVER () FROM {t}",
        false,
    )
    .fails(always, FLOAT_SEMANTICS),
    // Aggregate
    template("group_by_a", "SELECT count(*) FROM {t} GROUP BY a", false),
    template("count_distinct", "SELECT count(DISTINCT a) FROM {t}", false),
    template(
        "count_distinct_two",
        "SELECT count(DISTINCT a), count(DISTINCT b) FROM {t}",
        false,
    )
    .fails(always, FLOAT_SEMANTICS),
    template(
        "distinct_on",
        "SELECT DISTINCT ON (g) g, b FROM {t} ORDER BY g, a, b",
        true,
    )
    .fails(always, FLOAT_SEMANTICS),
    template("min_max_k", "SELECT min(k), max(k) FROM {t}", false)
        .fails(always, FLOAT_SEMANTICS),
    // Join
    template(
        "join_a",
        "SELECT count(*) FROM {t} x JOIN {t} y ON x.a = y.a",
        false,
    ),
    template(
        "join_k",
        "SELECT count(*) FROM {t} x JOIN {t} y ON x.k = y.k",
        false,
    )
    .fails(sort_merge_join, SORT_MERGE_JOIN),
    // Expression
    template("in_list", "SELECT id, a IN ({z}, 1.0) FROM {t}", false),
    template(
        "in_list_f32",
        "SELECT id, a32 IN ({z}, 1.0) FROM {t}",
        false,
    ),
    template(
        "in_list_long",
        "SELECT id, a IN ({z}, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, \
         11.0, 12.0, 13.0, 14.0, 15.0, 16.0, 17.0, 18.0, 19.0, 20.0, 21.0, 22.0, \
         23.0, 24.0, 25.0) FROM {t}",
        false,
    ),
    template(
        "case_when",
        "SELECT id, CASE a WHEN {z} THEN 'eq' ELSE 'ne' END FROM {t}",
        false,
    )
    .fails(always, FLOAT_SEMANTICS),
    template("nullif", "SELECT id, nullif(a, {z}) FROM {t}", false)
        .fails(always, FLOAT_SEMANTICS),
    template(
        "array_has_make_array",
        "SELECT id, array_has(make_array(a), {z}), \
         array_has_any(make_array(a), make_array({z})), \
         array_has_all(make_array(a), make_array({z})) FROM {t}",
        false,
    )
    .fails(always, FLOAT_SEMANTICS),
    template(
        "array_has_k",
        "SELECT id, array_has(k, {z}) FROM {t}",
        false,
    )
    .fails(always, FLOAT_SEMANTICS),
    template(
        "array_position",
        "SELECT id, array_position(k, {z}) FROM {t}",
        false,
    )
    .fails(always, FLOAT_SEMANTICS),
    template(
        "array_replace",
        "SELECT id, array_replace(k, {z}, 9.0) FROM {t}",
        false,
    )
    .fails(always, FLOAT_SEMANTICS),
    template(
        "array_remove",
        "SELECT id, array_remove(k, {z}) FROM {t}",
        false,
    )
    .fails(always, FLOAT_SEMANTICS),
];
