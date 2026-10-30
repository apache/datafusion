// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! Query templates and the (template, setting) pairs known to be wrong.

use super::settings::Setting;

/// A query to run on both `raw` and `canon`. `{t}` is replaced by the table
/// name and every `{z}` by a zero literal.
#[derive(Debug)]
pub struct Template {
    pub name: &'static str,
    pub sql: &'static str,
    /// Whether the outer ORDER BY decides the whole output order. If not, the
    /// results are compared as multisets of rows.
    pub ordered: bool,
}

impl Template {
    /// The query for `table`, taking each zero literal from `zero`.
    pub fn render(&self, table: &str, mut zero: impl FnMut() -> &'static str) -> String {
        let mut sql = self.sql.replace("{t}", table);
        while let Some(pos) = sql.find("{z}") {
            sql.replace_range(pos..pos + 3, zero());
        }
        sql
    }
}

const fn template(name: &'static str, sql: &'static str, ordered: bool) -> Template {
    Template { name, sql, ordered }
}

/// The templates. Rules for adding one:
///
/// - It has exactly one correct answer. An ORDER BY that picks or numbers rows
///   (LIMIT, `first_value`, DISTINCT ON, `row_number`, `lag`, ROWS frames)
///   ends with a unique column (`id` or `b`). `rank` and RANGE frames don't,
///   because their ties are what is being tested.
/// - No arithmetic on float columns (`-a`, `a * a`), since it can create
///   `-0.0` in `canon`, and no functions that can see the sign (`1 / a`,
///   `atan2`, casts to string).
/// - `ordered` only when the outer ORDER BY decides the whole output order.
pub const TEMPLATES: &[Template] = &[
    // Sort
    template("order_by_a_b", "SELECT a, b FROM {t} ORDER BY a, b", true),
    template(
        "order_by_a_b_limit",
        "SELECT a, b FROM {t} ORDER BY a, b LIMIT 1",
        true,
    ),
    template("order_by_k", "SELECT id FROM {t} ORDER BY k, id", true),
    // Window
    template(
        "rank",
        "SELECT id, rank() OVER (ORDER BY a) FROM {t}",
        false,
    ),
    template(
        "range_frame",
        "SELECT id, sum(b) OVER (ORDER BY a RANGE BETWEEN CURRENT ROW AND CURRENT ROW) FROM {t}",
        false,
    ),
    template(
        "row_number_partition",
        "SELECT id, row_number() OVER (PARTITION BY a ORDER BY id) FROM {t}",
        false,
    ),
    template(
        "first_value",
        "SELECT first_value(b ORDER BY a, b) FROM {t}",
        false,
    ),
    template(
        "count_distinct_window",
        "SELECT DISTINCT count(DISTINCT a) OVER () FROM {t}",
        false,
    ),
    // Aggregate
    template("group_by_a", "SELECT count(*) FROM {t} GROUP BY a", false),
    template("count_distinct", "SELECT count(DISTINCT a) FROM {t}", false),
    template(
        "count_distinct_two",
        "SELECT count(DISTINCT a), count(DISTINCT b) FROM {t}",
        false,
    ),
    template(
        "distinct_on",
        "SELECT DISTINCT ON (g) g, b FROM {t} ORDER BY g, a, b",
        true,
    ),
    template("min_max_k", "SELECT min(k), max(k) FROM {t}", false),
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
    ),
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
    ),
    template("nullif", "SELECT id, nullif(a, {z}) FROM {t}", false),
    template(
        "array_has_make_array",
        "SELECT id, array_has(make_array(a), {z}), \
         array_has_any(make_array(a), make_array({z})), \
         array_has_all(make_array(a), make_array({z})) FROM {t}",
        false,
    ),
    template(
        "array_has_k",
        "SELECT id, array_has(k, {z}) FROM {t}",
        false,
    ),
    template(
        "array_position",
        "SELECT id, array_position(k, {z}) FROM {t}",
        false,
    ),
    template(
        "array_replace",
        "SELECT id, array_replace(k, {z}, 9.0) FROM {t}",
        false,
    ),
    template(
        "array_remove",
        "SELECT id, array_remove(k, {z}) FROM {t}",
        false,
    ),
];

/// A (template, setting) pair that is known to be wrong. The fixed-data test
/// requires every listed pair to still fail, so the PR that fixes one must
/// remove its entry.
pub struct KnownFailure {
    pub template: &'static str,
    pub applies: fn(&Setting) -> bool,
    pub issue: &'static str,
}

impl KnownFailure {
    pub fn matches(template: &Template, setting: &Setting) -> Option<&'static Self> {
        KNOWN_FAILURES
            .iter()
            .find(|f| f.template == template.name && (f.applies)(setting))
    }
}

fn always(_: &Setting) -> bool {
    true
}

/// The planner only picks a sort-merge join when the input is repartitioned.
fn sort_merge_join(setting: &Setting) -> bool {
    !setting.prefer_hash_join && setting.target_partitions > 1
}

const fn known(
    template: &'static str,
    applies: fn(&Setting) -> bool,
    issue: &'static str,
) -> KnownFailure {
    KnownFailure {
        template,
        applies,
        issue,
    }
}

const FLOAT_SEMANTICS: &str = "https://github.com/apache/datafusion/issues/26138";
const IN_LIST: &str = "https://github.com/apache/datafusion/issues/25186";
const WINDOW: &str = "https://github.com/apache/datafusion/issues/25766";
const SORT_MERGE_JOIN: &str = "https://github.com/apache/datafusion/issues/26115";

pub const KNOWN_FAILURES: &[KnownFailure] = &[
    // Sorting orders `-0.0` before `0.0`
    known("order_by_a_b", always, FLOAT_SEMANTICS),
    known("order_by_a_b_limit", always, FLOAT_SEMANTICS),
    known("order_by_k", always, FLOAT_SEMANTICS),
    known("first_value", always, FLOAT_SEMANTICS),
    known("distinct_on", always, FLOAT_SEMANTICS),
    known("min_max_k", always, FLOAT_SEMANTICS),
    // Window partitions and peers
    known("rank", always, WINDOW),
    known("range_frame", always, WINDOW),
    known("row_number_partition", always, WINDOW),
    // `count(DISTINCT)` when not planned as a GROUP BY
    known("count_distinct_two", always, FLOAT_SEMANTICS),
    known("count_distinct_window", always, FLOAT_SEMANTICS),
    // Expressions
    known("in_list", always, IN_LIST),
    known("in_list_f32", always, IN_LIST),
    known("in_list_long", always, IN_LIST),
    known("case_when", always, FLOAT_SEMANTICS),
    known("nullif", always, FLOAT_SEMANTICS),
    known("array_has_make_array", always, FLOAT_SEMANTICS),
    known("array_has_k", always, FLOAT_SEMANTICS),
    known("array_position", always, FLOAT_SEMANTICS),
    known("array_replace", always, FLOAT_SEMANTICS),
    known("array_remove", always, FLOAT_SEMANTICS),
    // Joins
    known("join_k", sort_merge_join, SORT_MERGE_JOIN),
];
