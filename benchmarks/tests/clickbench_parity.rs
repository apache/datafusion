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
    let mut queries = BTreeSet::new();
    let mut offset = 0;

    while let Some(directive_offset) = find_query_directive(&contents[offset..]) {
        let directive = offset + directive_offset;
        let sql_offset = contents[directive..]
            .find('\n')
            .map(|offset| directive + offset + 1)
            .expect("query directive must be followed by SQL");
        let boundary_offset = find_slt_boundary(&contents[sql_offset..])
            .expect("query SQL must be followed by expected output");
        queries.insert(normalize_sql(
            &contents[sql_offset..sql_offset + boundary_offset],
        ));
        offset = sql_offset + boundary_offset + "\n----".len();
        offset += skip_expected_output(&contents[offset..]);
    }

    queries
}

fn skip_expected_output(contents: &str) -> usize {
    contents
        .find("\n\n")
        .map(|offset| offset + 2)
        .unwrap_or(contents.len())
}

fn find_query_directive(contents: &str) -> Option<usize> {
    contents
        .strip_prefix("query ")
        .map(|_| 0)
        .or_else(|| contents.find("\nquery ").map(|offset| offset + 1))
}

fn find_slt_boundary(sql: &str) -> Option<usize> {
    let mut index = 0;

    while index < sql.len() {
        let remaining = &sql[index..];
        let character = remaining.chars().next().unwrap();

        if remaining.starts_with("\n----") {
            return Some(index);
        }
        if matches!(character, '\'' | '"') {
            index += quoted_end(
                remaining,
                character,
                character == '\'' && is_escaped_string_start(sql, index),
            );
        } else if let Some(delimiter) = dollar_quote_delimiter(remaining) {
            let content = &remaining[delimiter.len()..];
            index += content
                .find(delimiter)
                .map(|end| delimiter.len() + end + delimiter.len())
                .unwrap_or(remaining.len());
        } else {
            index += character.len_utf8();
        }
    }

    None
}

fn normalize_sql(sql: &str) -> String {
    let mut normalized = String::with_capacity(sql.len());
    let mut pending_whitespace = false;
    let mut index = 0;

    while index < sql.len() {
        let remaining = &sql[index..];
        let character = remaining.chars().next().unwrap();

        if character.is_whitespace() {
            pending_whitespace = true;
            index += character.len_utf8();
        } else if remaining.starts_with("--") {
            index += remaining.find('\n').unwrap_or(remaining.len());
            pending_whitespace = true;
        } else if matches!(character, '\'' | '"') {
            push_pending_whitespace(&mut normalized, &mut pending_whitespace);
            let end = quoted_end(
                remaining,
                character,
                character == '\'' && is_escaped_string_start(sql, index),
            );
            normalized.push_str(&remaining[..end]);
            index += end;
        } else if let Some(delimiter) = dollar_quote_delimiter(remaining) {
            push_pending_whitespace(&mut normalized, &mut pending_whitespace);
            let content = &remaining[delimiter.len()..];
            let end = content
                .find(delimiter)
                .map(|end| delimiter.len() + end + delimiter.len())
                .unwrap_or(remaining.len());
            normalized.push_str(&remaining[..end]);
            index += end;
        } else {
            push_pending_whitespace(&mut normalized, &mut pending_whitespace);
            normalized.push(character);
            index += character.len_utf8();
        }
    }

    normalized
}

fn push_pending_whitespace(output: &mut String, pending_whitespace: &mut bool) {
    if *pending_whitespace && !output.is_empty() {
        output.push(' ');
    }
    *pending_whitespace = false;
}

fn is_escaped_string_start(sql: &str, quote_index: usize) -> bool {
    let Some(prefix) = quote_index
        .checked_sub(1)
        .and_then(|index| sql.as_bytes().get(index))
    else {
        return false;
    };

    matches!(prefix, b'e' | b'E')
        && quote_index
            .checked_sub(2)
            .and_then(|index| sql.as_bytes().get(index))
            .is_none_or(|character| {
                !character.is_ascii_alphanumeric() && *character != b'_'
            })
}

fn quoted_end(sql: &str, quote: char, backslash_escapes: bool) -> usize {
    let mut index = quote.len_utf8();

    while index < sql.len() {
        let remaining = &sql[index..];
        let character = remaining.chars().next().unwrap();
        index += character.len_utf8();

        if character == '\\' && backslash_escapes {
            if let Some(escaped) = sql[index..].chars().next() {
                index += escaped.len_utf8();
            }
        } else if character == quote {
            if remaining[character.len_utf8()..].starts_with(quote) {
                index += quote.len_utf8();
            } else {
                return index;
            }
        }
    }

    sql.len()
}

fn dollar_quote_delimiter(sql: &str) -> Option<&str> {
    let mut characters = sql.char_indices();
    if characters.next()?.1 != '$' {
        return None;
    }

    let (first_index, first) = characters.next()?;
    if first == '$' {
        return Some(&sql[..first_index + first.len_utf8()]);
    }
    if !matches!(first, 'a'..='z' | 'A'..='Z' | '_') {
        return None;
    }

    for (index, character) in characters {
        if character == '$' {
            return Some(&sql[..index + character.len_utf8()]);
        }
        if !matches!(character, 'a'..='z' | 'A'..='Z' | '0'..='9' | '_') {
            return None;
        }
    }

    None
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;
    use std::fs;

    use super::{normalize_sql, read_slt_queries};

    #[test]
    fn reads_interleaved_and_multiline_slt_queries() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("queries.slt");
        fs::write(
            &path,
            "query I\nSELECT unrelated;\n----\n1\n\nquery I\nSELECT\n    \"ClickBenchColumn\"\nFROM hits;\n----\n1\n",
        )
        .unwrap();

        assert_eq!(
            read_slt_queries(&path),
            BTreeSet::from([
                "SELECT unrelated;".to_string(),
                "SELECT \"ClickBenchColumn\" FROM hits;".to_string(),
            ])
        );
    }

    #[test]
    fn ignores_slt_directives_and_boundaries_in_string_literals() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("queries.slt");
        fs::write(
            &path,
            "query T\nSELECT 'first line\nquery I\n----\nlast line';\n----\nfirst line\\nquery I\\n----\\nlast line\n",
        )
        .unwrap();

        assert_eq!(
            read_slt_queries(&path),
            BTreeSet::from(
                ["SELECT 'first line\nquery I\n----\nlast line';".to_string()]
            )
        );
    }

    #[test]
    fn ignores_query_directives_in_expected_output() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("queries.slt");
        fs::write(
            &path,
            "query I\nSELECT 1;\n----\nquery I\n\nquery I\nSELECT 2;\n----\n2\n",
        )
        .unwrap();

        assert_eq!(
            read_slt_queries(&path),
            BTreeSet::from(["SELECT 1;".to_string(), "SELECT 2;".to_string()])
        );
    }

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

    #[test]
    fn preserves_quoted_content() {
        assert_ne!(
            normalize_sql("SELECT 'a  b';"),
            normalize_sql("SELECT 'a b';")
        );
        assert_ne!(
            normalize_sql("SELECT \"a  b\";"),
            normalize_sql("SELECT \"a b\";")
        );
        assert_ne!(
            normalize_sql("SELECT 'a\nb';"),
            normalize_sql("SELECT 'a b';")
        );
        assert_eq!(
            normalize_sql(
                "SELECT E'a\\'  b -- value', $tag$c  d -- value$tag$; -- comment"
            ),
            "SELECT E'a\\'  b -- value', $tag$c  d -- value$tag$;"
        );
    }
}
