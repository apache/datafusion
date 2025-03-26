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

use datafusion_functions::string;
use std::{collections::HashMap, sync::Arc};

use datafusion_common::{Diagnostic, Location, Result, Span};
use datafusion_sql::planner::{ParserOptions, SqlToRel};
use regex::Regex;
use sqlparser::{dialect::GenericDialect, parser::Parser};

use crate::{MockContextProvider, MockSessionState};

fn do_query(sql: &'static str) -> Diagnostic {
    let dialect = GenericDialect {};
    let statement = Parser::new(&dialect)
        .try_with_sql(sql)
        .expect("unable to create parser")
        .parse_statement()
        .expect("unable to parse query");
    let options = ParserOptions {
        collect_spans: true,
        ..ParserOptions::default()
    };
    let state = MockSessionState::default()
        .with_scalar_function(Arc::new(string::concat().as_ref().clone()));
    let context = MockContextProvider { state };
    let sql_to_rel = SqlToRel::new_with_options(&context, options);
    match sql_to_rel.sql_statement_to_plan(statement) {
        Ok(_) => panic!("expected error"),
        Err(err) => match err.diagnostic() {
            Some(diag) => diag.clone(),
            None => panic!("expected diagnostic"),
        },
    }
}

/// Given a query that contains tag delimited spans, returns a mapping from the
/// span name to the [`Span`]. Tags are comments of the form `/*tag*/`. In case
/// you want the same location to open two spans, or close open and open
/// another, you can use '+' to separate the names (the order matters). The
/// names of spans can only contain letters, digits, underscores, and dashes.
///
///
/// ## Example
///
/// ```rust
/// let spans = get_spans("SELECT /*myspan*/car/*myspan*/ FROM cars");
/// // myspan is                            ^^^
/// dbg!(&spans["myspan"]);
/// ```
///
/// ## Example
///
/// ```rust
/// let spans = get_spans("SELECT /*whole+left*/speed/*left*/ + /*right*/10/*right+whole*/ FROM cars");
/// // whole is                                 ^^^^^^^^^^^^^^^^^^^^^^^^^^^
/// // left is                                  ^^^^^
/// // right is                                                          ^^
/// dbg!(&spans["whole"]);
/// dbg!(&spans["left"]);
/// dbg!(&spans["right"]);
/// ```
fn get_spans(query: &'static str) -> HashMap<String, Span> {
    let mut spans = HashMap::new();

    let mut bytes_per_line = vec![];
    for line in query.lines() {
        bytes_per_line.push(line.len());
    }

    let byte_offset_to_loc = |s: &str, byte_offset: usize| -> Location {
        let mut line = 1;
        let mut column = 1;
        for (i, c) in s.chars().enumerate() {
            if i == byte_offset {
                return Location { line, column };
            }
            if c == '\n' {
                line += 1;
                column = 1;
            } else {
                column += 1;
            }
        }
        Location { line, column }
    };

    let re = Regex::new(r#"/\*([\w\d\+_-]+)\*/"#).unwrap();
    let mut stack: Vec<(String, usize)> = vec![];
    for c in re.captures_iter(query) {
        let m = c.get(0).unwrap();
        let tags = c.get(1).unwrap().as_str().split("+").collect::<Vec<_>>();

        for tag in tags {
            if stack.last().map(|(top_tag, _)| top_tag.as_str()) == Some(tag) {
                let (_, start) = stack.pop().unwrap();
                let end = m.start();

                spans.insert(
                    tag.to_string(),
                    Span::new(
                        byte_offset_to_loc(query, start),
                        byte_offset_to_loc(query, end),
                    ),
                );
            } else {
                stack.push((tag.to_string(), m.end()));
            }
        }
    }

    if !stack.is_empty() {
        panic!("unbalanced tags");
    }

    spans
}

#[test]
fn test_table_not_found() -> Result<()> {
    let query = "SELECT * FROM /*a*/personx/*a*/";
    let spans = get_spans(query);
    let diag = do_query(query);
    assert_eq!(diag.message, "table 'personx' not found");
    assert_eq!(diag.span, Some(spans["a"]));
    Ok(())
}

#[test]
fn test_unqualified_column_not_found() -> Result<()> {
    let query = "SELECT /*a*/first_namex/*a*/ FROM person";
    let spans = get_spans(query);
    let diag = do_query(query);
    assert_eq!(diag.message, "column 'first_namex' not found");
    assert_eq!(diag.span, Some(spans["a"]));
    Ok(())
}

#[test]
fn test_qualified_column_not_found() -> Result<()> {
    let query = "SELECT /*a*/person.first_namex/*a*/ FROM person";
    let spans = get_spans(query);
    let diag = do_query(query);
    assert_eq!(diag.message, "column 'first_namex' not found in 'person'");
    assert_eq!(diag.span, Some(spans["a"]));
    Ok(())
}

#[test]
fn test_union_wrong_number_of_columns() -> Result<()> {
    let query = "/*whole+left*/SELECT first_name FROM person/*left*/ UNION ALL /*right*/SELECT first_name, last_name FROM person/*right+whole*/";
    let spans = get_spans(query);
    let diag = do_query(query);
    assert_eq!(
        diag.message,
        "UNION queries have different number of columns"
    );
    assert_eq!(diag.span, Some(spans["whole"]));
    assert_eq!(diag.notes[0].message, "this side has 1 fields");
    assert_eq!(diag.notes[0].span, Some(spans["left"]));
    assert_eq!(diag.notes[1].message, "this side has 2 fields");
    assert_eq!(diag.notes[1].span, Some(spans["right"]));
    Ok(())
}

#[test]
fn test_missing_non_aggregate_in_group_by() -> Result<()> {
    let query = "SELECT id, /*a*/first_name/*a*/ FROM person GROUP BY id";
    let spans = get_spans(query);
    let diag = do_query(query);
    assert_eq!(
        diag.message,
        "'person.first_name' must appear in GROUP BY clause because it's not an aggregate expression"
    );
    assert_eq!(diag.span, Some(spans["a"]));
    assert_eq!(
        diag.helps[0].message,
        "Either add 'person.first_name' to GROUP BY clause, or use an aggregare function like ANY_VALUE(person.first_name)"
    );
    Ok(())
}

#[test]
fn test_ambiguous_reference() -> Result<()> {
    let query = "SELECT /*a*/first_name/*a*/ FROM person a, person b";
    let spans = get_spans(query);
    let diag = do_query(query);
    assert_eq!(diag.message, "column 'first_name' is ambiguous");
    assert_eq!(diag.span, Some(spans["a"]));
    assert_eq!(diag.notes[0].message, "possible column a.first_name");
    assert_eq!(diag.notes[1].message, "possible column b.first_name");
    Ok(())
}

#[test]
fn test_incompatible_types_binary_arithmetic() -> Result<()> {
    let query =
        "SELECT /*whole+left*/id/*left*/ + /*right*/first_name/*right+whole*/ FROM person";
    let spans = get_spans(query);
    let diag = do_query(query);
    assert_eq!(diag.message, "expressions have incompatible types");
    assert_eq!(diag.span, Some(spans["whole"]));
    assert_eq!(diag.notes[0].message, "has type UInt32");
    assert_eq!(diag.notes[0].span, Some(spans["left"]));
    assert_eq!(diag.notes[1].message, "has type Utf8");
    assert_eq!(diag.notes[1].span, Some(spans["right"]));
    Ok(())
}

#[test]
fn test_field_not_found_suggestion() -> Result<()> {
    let query = "SELECT /*whole*/first_na/*whole*/ FROM person";
    let spans = get_spans(query);
    let diag = do_query(query);
    assert_eq!(diag.message, "column 'first_na' not found");
    assert_eq!(diag.span, Some(spans["whole"]));
    assert_eq!(diag.notes.len(), 1);

    let mut suggested_fields: Vec<String> = diag
        .notes
        .iter()
        .filter_map(|note| {
            if note.message.starts_with("possible column") {
                Some(note.message.replace("possible column ", ""))
            } else {
                None
            }
        })
        .collect();
    suggested_fields.sort();
    assert_eq!(suggested_fields[0], "person.first_name");
    Ok(())
}

#[test]
fn test_ambiguous_column_suggestion() -> Result<()> {
    let query = "SELECT /*whole*/id/*whole*/ FROM test_decimal, person";
    let spans = get_spans(query);
    let diag = do_query(query);

    assert_eq!(diag.message, "column 'id' is ambiguous");
    assert_eq!(diag.span, Some(spans["whole"]));

    assert_eq!(diag.notes.len(), 2);

    let mut suggested_fields: Vec<String> = diag
        .notes
        .iter()
        .filter_map(|note| {
            if note.message.starts_with("possible column") {
                Some(note.message.replace("possible column ", ""))
            } else {
                None
            }
        })
        .collect();

    suggested_fields.sort();
    assert_eq!(suggested_fields, vec!["person.id", "test_decimal.id"]);

    Ok(())
}

#[test]
fn test_invalid_function() -> Result<()> {
    let query = "SELECT /*whole*/concat_not_exist/*whole*/()";
    let spans = get_spans(query);
    let diag = do_query(query);
    assert_eq!(diag.message, "Invalid function 'concat_not_exist'");
    assert_eq!(diag.notes[0].message, "Possible function 'concat'");
    assert_eq!(diag.span, Some(spans["whole"]));
    Ok(())
}
#[test]
fn test_scalar_subquery_multiple_columns() -> Result<(), Box<dyn std::error::Error>> {
    let query = "SELECT (SELECT 1 AS /*x*/x/*x*/, 2 AS /*y*/y/*y*/) AS col";
    let spans = get_spans(query);
    let diag = do_query(query);

    assert_eq!(
        diag.message,
        "Too many columns! The subquery should only return one column"
    );

    let expected_span = Some(Span {
        start: spans["x"].start,
        end: spans["y"].end,
    });
    assert_eq!(diag.span, expected_span);
    assert_eq!(
        diag.notes
            .iter()
            .map(|n| (n.message.as_str(), n.span))
            .collect::<Vec<_>>(),
        vec![("Extra column 1", Some(spans["y"]))]
    );
    assert_eq!(
        diag.helps
            .iter()
            .map(|h| h.message.as_str())
            .collect::<Vec<_>>(),
        vec!["Select only one column in the subquery"]
    );

    Ok(())
}

#[test]
fn test_in_subquery_multiple_columns() -> Result<(), Box<dyn std::error::Error>> {
    // This query uses an IN subquery with multiple columns - this should trigger an error
    let query = "SELECT * FROM person WHERE id IN (SELECT /*id*/id/*id*/, /*first*/first_name/*first*/ FROM person)";
    let spans = get_spans(query);
    let diag = do_query(query);

    assert_eq!(
        diag.message,
        "Too many columns! The subquery should only return one column"
    );

    let expected_span = Some(Span {
        start: spans["id"].start,
        end: spans["first"].end,
    });
    assert_eq!(diag.span, expected_span);
    assert_eq!(
        diag.notes
            .iter()
            .map(|n| (n.message.as_str(), n.span))
            .collect::<Vec<_>>(),
        vec![("Extra column 1", Some(spans["first"]))]
    );
    assert_eq!(
        diag.helps
            .iter()
            .map(|h| h.message.as_str())
            .collect::<Vec<_>>(),
        vec!["Select only one column in the subquery"]
    );
    Ok(())
}

#[test]
fn test_unary_op_plus_with_column() -> Result<()> {
    // Test with a direct query that references a column with an incompatible type
    let query = "SELECT +/*whole*/first_name/*whole*/ FROM person";
    let spans = get_spans(query);
    let diag = do_query(query);
    assert_eq!(diag.message, "+ cannot be used with Utf8");
    assert_eq!(diag.span, Some(spans["whole"]));
    assert_eq!(
        diag.notes[0].message,
        "+ can only be used with numbers, intervals, and timestamps"
    );
    assert_eq!(
        diag.helps[0].message,
        "perhaps you need to cast person.first_name"
    );
    Ok(())
}

#[test]
fn test_unary_op_plus_with_non_column() -> Result<()> {
    // create a table with a column of type varchar
    let query = "SELECT +'a'";
    let diag = do_query(query);
    assert_eq!(diag.message, "+ cannot be used with Utf8");
    assert_eq!(
        diag.notes[0].message,
        "+ can only be used with numbers, intervals, and timestamps"
    );
    assert_eq!(diag.notes[0].span, None);
    assert_eq!(
        diag.helps[0].message,
        "perhaps you need to cast Utf8(\"a\")"
    );
    assert_eq!(diag.helps[0].span, None);
    assert_eq!(diag.span, None);
    Ok(())
}
