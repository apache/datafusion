// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: See the NOTICE file distributed with this work for additional information regarding copyright ownership
// SPDX-FileContributor: Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements

//! The queries, and when each is known to give a wrong result.

use super::settings::Setting;

/// A query. `{t}` is replaced by the table name and `{z}` by a zero.
#[derive(Debug)]
pub struct Query {
    pub name: &'static str,
    pub sql: &'static str,
    /// Whether the ORDER BY fixes the order of every row. If not, rows are
    /// compared in any order.
    pub ordered: bool,
    pub known_failure: Option<KnownFailure>,
}

/// When a query is known to give a wrong result. Remove it when the bug is
/// fixed; the fixed-table test fails until then. This suite runs only in the
/// merge queue, so a PR that fixes one of these bugs should run
/// `cargo test -p datafusion-fuzz --features extended_tests --test fuzz float_zero_fixed`.
#[derive(Debug)]
pub struct KnownFailure {
    pub applies: fn(&Setting) -> bool,
    pub issue: &'static str,
}

impl Query {
    pub fn render(&self, table: &str, zero: &str) -> String {
        self.sql.replace("{t}", table).replace("{z}", zero)
    }

    pub fn known_issue(&self, setting: &Setting) -> Option<&'static str> {
        self.known_failure
            .as_ref()
            .filter(|known| (known.applies)(setting))
            .map(|known| known.issue)
    }

    const fn ordered(self) -> Self {
        Query {
            ordered: true,
            ..self
        }
    }

    const fn fails(self, issue: &'static str) -> Self {
        self.fails_when(always, issue)
    }

    const fn fails_when(
        self,
        applies: fn(&Setting) -> bool,
        issue: &'static str,
    ) -> Self {
        Query {
            known_failure: Some(KnownFailure { applies, issue }),
            ..self
        }
    }
}

const fn query(name: &'static str, sql: &'static str) -> Query {
    Query {
        name,
        sql,
        ordered: false,
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
/// - `.ordered()` only when the ORDER BY fixes the order of every row.
/// - Numbers compared with `a32` or `a16` are written with `arrow_cast`.
///   Written as plain `0.0`, they would turn the column into a bigger float
///   type, and the query would no longer test that column's own type.
pub const QUERIES: &[Query] = &[
    // Sort
    query("order_by_a_b", "SELECT a, b FROM {t} ORDER BY a, b")
        .ordered()
        .fails(FLOAT_SEMANTICS),
    query("order_by_a_b_limit", "SELECT a, b FROM {t} ORDER BY a, b LIMIT 1")
        .ordered()
        .fails(FLOAT_SEMANTICS),
    query("order_by_k", "SELECT id FROM {t} ORDER BY k, id")
        .ordered()
        .fails(FLOAT_SEMANTICS),
    query("order_by_a32_b", "SELECT a32, b FROM {t} ORDER BY a32, b")
        .ordered()
        .fails(FLOAT_SEMANTICS),
    query("order_by_a16_b", "SELECT a16, b FROM {t} ORDER BY a16, b")
        .ordered()
        .fails(FLOAT_SEMANTICS),
    // Window
    query("rank", "SELECT id, rank() OVER (ORDER BY a) FROM {t}").fails(WINDOW),
    query(
        "range_frame",
        "SELECT id, sum(b) OVER (ORDER BY a RANGE BETWEEN CURRENT ROW AND CURRENT ROW) FROM {t}",
    )
    .fails(WINDOW),
    query(
        "row_number_partition",
        "SELECT id, row_number() OVER (PARTITION BY a ORDER BY id) FROM {t}",
    )
    .fails(WINDOW),
    query("first_value", "SELECT first_value(b ORDER BY a, b) FROM {t}")
        .fails(FLOAT_SEMANTICS),
    query(
        "count_distinct_window",
        "SELECT DISTINCT count(DISTINCT a) OVER () FROM {t}",
    )
    .fails(FLOAT_SEMANTICS),
    // Aggregate
    query("group_by_a", "SELECT count(*) FROM {t} GROUP BY a"),
    query("count_distinct", "SELECT count(DISTINCT a) FROM {t}"),
    query("group_by_a32", "SELECT count(*) FROM {t} GROUP BY a32"),
    query("group_by_a32_g", "SELECT g, count(*) FROM {t} GROUP BY a32, g"),
    query("count_distinct_a32", "SELECT count(DISTINCT a32) FROM {t}"),
    query("group_by_a16", "SELECT count(*) FROM {t} GROUP BY a16"),
    query("group_by_a16_g", "SELECT g, count(*) FROM {t} GROUP BY a16, g"),
    query("count_distinct_a16", "SELECT count(DISTINCT a16) FROM {t}"),
    query(
        "count_distinct_two",
        "SELECT count(DISTINCT a), count(DISTINCT b) FROM {t}",
    )
    .fails(FLOAT_SEMANTICS),
    query(
        "distinct_on",
        "SELECT DISTINCT ON (g) g, b FROM {t} ORDER BY g, a, b",
    )
    .ordered()
    .fails(FLOAT_SEMANTICS),
    query("min_max_k", "SELECT min(k), max(k) FROM {t}").fails(FLOAT_SEMANTICS),
    // Join
    query("join_a", "SELECT count(*) FROM {t} x JOIN {t} y ON x.a = y.a"),
    query("join_a32", "SELECT count(*) FROM {t} x JOIN {t} y ON x.a32 = y.a32"),
    query("join_a16", "SELECT count(*) FROM {t} x JOIN {t} y ON x.a16 = y.a16"),
    query("join_k", "SELECT count(*) FROM {t} x JOIN {t} y ON x.k = y.k")
        .fails_when(sort_merge_join, SORT_MERGE_JOIN),
    // Expression
    query("in_list", "SELECT id, a IN ({z}, 1.0) FROM {t}"),
    query(
        "in_list_f32",
        "SELECT id, a32 IN (arrow_cast({z}, 'Float32'), arrow_cast(1.0, 'Float32'), \
         arrow_cast(2.0, 'Float32'), arrow_cast(3.0, 'Float32')) FROM {t}",
    ),
    query(
        "in_list_long",
        "SELECT id, a IN ({z}, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, \
         11.0, 12.0, 13.0, 14.0, 15.0, 16.0, 17.0, 18.0, 19.0, 20.0, 21.0, 22.0, \
         23.0, 24.0, 25.0) FROM {t}",
    ),
    query(
        "in_list_f16",
        "SELECT id, a16 IN (arrow_cast({z}, 'Float16'), arrow_cast(1.0, 'Float16'), \
         arrow_cast(2.0, 'Float16'), arrow_cast(3.0, 'Float16')) FROM {t}",
    ),
    query(
        "in_list_f16_long",
        "SELECT id, a16 IN (arrow_cast({z}, 'Float16'), arrow_cast(1.0, 'Float16'), \
         arrow_cast(2.0, 'Float16'), arrow_cast(3.0, 'Float16'), \
         arrow_cast(4.0, 'Float16'), arrow_cast(5.0, 'Float16'), \
         arrow_cast(6.0, 'Float16'), arrow_cast(7.0, 'Float16'), \
         arrow_cast(8.0, 'Float16'), arrow_cast(9.0, 'Float16'), \
         arrow_cast(10.0, 'Float16'), arrow_cast(11.0, 'Float16')) FROM {t}",
    ),
    query(
        "case_when",
        "SELECT id, CASE a WHEN {z} THEN 'eq' ELSE 'ne' END FROM {t}",
    )
    .fails(FLOAT_SEMANTICS),
    query(
        "case_when_f32",
        "SELECT id, CASE a32 WHEN arrow_cast({z}, 'Float32') THEN 'zero' \
         WHEN arrow_cast(1.0, 'Float32') THEN 'one' ELSE 'other' END FROM {t}",
    )
    .fails(FLOAT_SEMANTICS),
    query(
        "case_when_f16",
        "SELECT id, CASE a16 WHEN arrow_cast({z}, 'Float16') THEN 'zero' \
         WHEN arrow_cast(1.0, 'Float16') THEN 'one' ELSE 'other' END FROM {t}",
    )
    .fails(FLOAT_SEMANTICS),
    query("nullif", "SELECT id, nullif(a, {z}) FROM {t}").fails(FLOAT_SEMANTICS),
    query(
        "array_has_make_array",
        "SELECT id, array_has(make_array(a), {z}), \
         array_has_any(make_array(a), make_array({z})), \
         array_has_all(make_array(a), make_array({z})) FROM {t}",
    )
    .fails(FLOAT_SEMANTICS),
    query("array_has_k", "SELECT id, array_has(k, {z}) FROM {t}").fails(FLOAT_SEMANTICS),
    query("array_position", "SELECT id, array_position(k, {z}) FROM {t}")
        .fails(FLOAT_SEMANTICS),
    query("array_replace", "SELECT id, array_replace(k, {z}, 9.0) FROM {t}")
        .fails(FLOAT_SEMANTICS),
    query("array_remove", "SELECT id, array_remove(k, {z}) FROM {t}")
        .fails(FLOAT_SEMANTICS),
];
