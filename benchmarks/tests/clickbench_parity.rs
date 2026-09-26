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

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};

const ROOT: &str = env!("CARGO_MANIFEST_DIR");

struct IntentionalDifference {
    suite: &'static str,
    id: u32,
    reason: &'static str,
}

const INTENTIONAL_DIFFERENCES: &[IntentionalDifference] = &[];

#[test]
fn clickbench_queries_stay_in_sync() {
    check_suite("clickbench", "queries", "clickbench.slt");
    check_suite("clickbench_extended", "extended", "clickbench_extended.slt");
}

fn check_suite(suite: &str, legacy_directory: &str, slt_file: &str) {
    let benchmark_directory = Path::new(ROOT)
        .join("sql_benchmarks")
        .join(suite)
        .join("benchmarks");
    let benchmarks = read_benchmarks(&benchmark_directory);
    let slt_path = Path::new(ROOT)
        .join("../datafusion/sqllogictest/test_files")
        .join(slt_file);
    let slt_queries = read_slt_queries(&slt_path);

    for (id, benchmark) in &benchmarks {
        if is_intentional_difference(suite, *id) {
            continue;
        }
        assert!(
            slt_queries.contains(&benchmark.sql),
            "ClickBench query {id} differs between {} and {}",
            benchmark.path.display(),
            slt_path.display(),
        );
    }

    let legacy_directory = Path::new(ROOT)
        .join("queries/clickbench")
        .join(legacy_directory);
    let legacy_queries = read_legacy_queries(&legacy_directory);
    assert_same_ids(
        suite,
        &benchmark_directory,
        &benchmarks,
        &legacy_directory,
        &legacy_queries,
    );

    for (id, benchmark) in benchmarks {
        if is_intentional_difference(suite, id) {
            continue;
        }
        let legacy = &legacy_queries[&id];
        assert_eq!(
            benchmark.sql,
            legacy.sql,
            "ClickBench query {id} differs between {} and {}",
            benchmark.path.display(),
            legacy.path.display(),
        );
    }
}

fn is_intentional_difference(suite: &str, id: u32) -> bool {
    INTENTIONAL_DIFFERENCES.iter().any(|difference| {
        difference.suite == suite && difference.id == id && !difference.reason.is_empty()
    })
}

fn assert_same_ids(
    suite: &str,
    benchmark_directory: &Path,
    benchmarks: &BTreeMap<u32, Query>,
    legacy_directory: &Path,
    legacy_queries: &BTreeMap<u32, Query>,
) {
    let benchmark_ids = benchmarks.keys().copied().collect::<BTreeSet<_>>();
    let legacy_ids = legacy_queries.keys().copied().collect::<BTreeSet<_>>();
    let missing_benchmarks = legacy_ids
        .difference(&benchmark_ids)
        .map(|id| legacy_queries[id].path.display().to_string())
        .collect::<Vec<_>>();
    let missing_legacy = benchmark_ids
        .difference(&legacy_ids)
        .map(|id| benchmarks[id].path.display().to_string())
        .collect::<Vec<_>>();

    assert!(
        missing_benchmarks.is_empty() && missing_legacy.is_empty(),
        "ClickBench {suite} query IDs differ between {} and {}. Missing benchmarks: {missing_benchmarks:?}. Missing legacy queries: {missing_legacy:?}",
        benchmark_directory.display(),
        legacy_directory.display(),
    );
}

struct Query {
    path: PathBuf,
    sql: String,
}

fn read_benchmarks(directory: &Path) -> BTreeMap<u32, Query> {
    read_queries(directory, "benchmark", |contents| {
        let (_, sql) = contents
            .split_once("\nrun\n")
            .expect("benchmark must contain a run block");
        sql.split_once("\nresult ")
            .expect("benchmark run block must be followed by result")
            .0
    })
}

fn read_legacy_queries(directory: &Path) -> BTreeMap<u32, Query> {
    read_queries(directory, "sql", |contents| contents)
}

fn read_queries(
    directory: &Path,
    extension: &str,
    extract_sql: impl Fn(&str) -> &str,
) -> BTreeMap<u32, Query> {
    let mut queries = BTreeMap::new();
    for entry in fs::read_dir(directory).unwrap() {
        let path = entry.unwrap().path();
        if path.extension().and_then(|extension| extension.to_str()) != Some(extension) {
            continue;
        }

        let stem = path.file_stem().unwrap().to_str().unwrap();
        let id = stem.strip_prefix('q').unwrap().parse().unwrap();
        let contents = fs::read_to_string(&path).unwrap();
        let old = queries.insert(
            id,
            Query {
                path,
                sql: normalize_sql(extract_sql(&contents)),
            },
        );
        assert!(old.is_none(), "duplicate ClickBench query ID {id}");
    }
    queries
}

fn read_slt_queries(path: &Path) -> BTreeSet<String> {
    let contents = fs::read_to_string(path).unwrap();
    contents
        .split("\nquery ")
        .skip(1)
        .map(|query| {
            let (_, sql) = query
                .split_once('\n')
                .expect("query directive must be followed by SQL");
            let (sql, _) = sql
                .split_once("\n----")
                .expect("query SQL must be followed by expected output");
            normalize_sql(sql)
        })
        .collect()
}

fn normalize_sql(sql: &str) -> String {
    let mut without_comments = String::with_capacity(sql.len());
    let mut in_single_quote = false;
    let mut in_double_quote = false;

    for line in sql.lines() {
        let mut characters = line.chars().peekable();
        while let Some(character) = characters.next() {
            match character {
                '\'' if !in_double_quote && characters.peek() == Some(&'\'') => {
                    without_comments.push(character);
                    without_comments.push(characters.next().unwrap());
                }
                '\'' if !in_double_quote => {
                    in_single_quote = !in_single_quote;
                    without_comments.push(character);
                }
                '"' if !in_single_quote && characters.peek() == Some(&'"') => {
                    without_comments.push(character);
                    without_comments.push(characters.next().unwrap());
                }
                '"' if !in_single_quote => {
                    in_double_quote = !in_double_quote;
                    without_comments.push(character);
                }
                '-' if !in_single_quote
                    && !in_double_quote
                    && characters.peek() == Some(&'-') =>
                {
                    break;
                }
                _ => without_comments.push(character),
            }
        }
        without_comments.push(' ');
    }

    without_comments
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

#[cfg(test)]
mod tests {
    use super::normalize_sql;

    #[test]
    fn normalizes_comments_and_whitespace_only() {
        assert_eq!(
            normalize_sql("-- comment\nSELECT  a -- comment\nFROM\tb;"),
            "SELECT a FROM b;"
        );
    }

    #[test]
    fn preserves_comment_markers_in_string_literals() {
        assert_eq!(
            normalize_sql("SELECT 'it''s -- a value' FROM \"a\"\"-- table\"; -- comment"),
            "SELECT 'it''s -- a value' FROM \"a\"\"-- table\";"
        );
    }
}
