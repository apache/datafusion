use std::collections::HashMap;

use arrow_schema::{DataType, Field, Schema};
use datafusion::error::Result;
use datafusion::prelude::*;
use datafusion_common::Diagnostic;
use regex::Regex;
use sqlparser::tokenizer::{Location, Span};

async fn cars_query(sql: &'static str) -> Diagnostic {
    let config =
        SessionConfig::new().set_bool("datafusion.sql_parser.collect_spans", true);
    let ctx = SessionContext::new_with_config(config);
    let csv_path = "../../datafusion/core/tests/data/cars.csv".to_string();
    let schema = Schema::new(vec![
        Field::new("car", DataType::Utf8, false),
        Field::new("speed", DataType::Float32, false),
        Field::new("time", DataType::Utf8, false),
    ]);
    let read_options = CsvReadOptions::default().has_header(true).schema(&schema);
    ctx.register_csv("cars", &csv_path, read_options)
        .await
        .expect("error registering cars.csv");
    match ctx.sql(sql).await {
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

#[cfg(test)]
#[tokio::test]
pub async fn test_table_not_found() -> Result<()> {
    let query = "SELECT * FROM /*a*/carz/*a*/";
    let spans = get_spans(query);
    let diag = cars_query(query).await;
    assert_eq!(diag.message, "table 'carz' not found");
    assert_eq!(diag.span, spans["a"]);
    Ok(())
}

#[cfg(test)]
#[tokio::test]
pub async fn test_unqualified_column_not_found() -> Result<()> {
    let query = "SELECT /*a*/speedz/*a*/ FROM cars";
    let spans = get_spans(query);
    let diag = cars_query(query).await;
    assert_eq!(diag.message, "column 'speedz' not found");
    assert_eq!(diag.span, spans["a"]);
    Ok(())
}

#[cfg(test)]
#[tokio::test]
pub async fn test_qualified_column_not_found() -> Result<()> {
    let query = "SELECT /*a*/cars.speedz/*a*/ FROM cars";
    let spans = get_spans(query);
    let diag = cars_query(query).await;
    assert_eq!(diag.message, "column 'speedz' not found in 'cars'");
    assert_eq!(diag.span, spans["a"]);
    Ok(())
}

#[cfg(test)]
#[tokio::test]
pub async fn test_union_wrong_number_of_columns() -> Result<()> {
    let query = "/*whole+left*/SELECT speed FROM cars/*left*/ UNION ALL /*right*/SELECT speed, time FROM cars/*right+whole*/";
    let spans = get_spans(query);
    let diag = cars_query(query).await;
    assert_eq!(
        diag.message,
        "UNION queries have different number of columns"
    );
    assert_eq!(diag.span, spans["whole"]);
    assert_eq!(diag.notes[0].message, "this side has 1 fields");
    assert_eq!(diag.notes[0].span, spans["left"]);
    assert_eq!(diag.notes[1].message, "this side has 2 fields");
    assert_eq!(diag.notes[1].span, spans["right"]);
    Ok(())
}

#[cfg(test)]
#[tokio::test]
pub async fn test_missing_non_aggregate_in_group_by() -> Result<()> {
    let query = "SELECT car, /*a*/speed/*a*/ FROM cars GROUP BY car";
    let spans = get_spans(query);
    let diag = cars_query(query).await;
    assert_eq!(
        diag.message,
        "'cars.speed' must appear in GROUP BY clause because it's not an aggregate expression"
    );
    assert_eq!(diag.span, spans["a"]);
    assert_eq!(diag.helps[0].message, "add 'cars.speed' to GROUP BY clause");
    Ok(())
}

#[cfg(test)]
#[tokio::test]
pub async fn test_ambiguous_reference() -> Result<()> {
    let query = "SELECT /*a*/car/*a*/ FROM cars a, cars b";
    let spans = get_spans(query);
    let diag = cars_query(query).await;
    assert_eq!(diag.message, "column 'car' is ambiguous");
    assert_eq!(diag.span, spans["a"]);
    assert_eq!(
        diag.notes[0].message,
        "possible reference to 'car' in table 'a'"
    );
    assert_eq!(
        diag.notes[1].message,
        "possible reference to 'car' in table 'b'"
    );
    Ok(())
}

#[cfg(test)]
#[tokio::test]
pub async fn test_incompatible_types_binary_arithmetic() -> Result<()> {
    let query =
        "SELECT /*whole+left*/car/*left*/ + /*right*/speed/*right+whole*/ FROM cars";
    let spans = get_spans(query);
    let diag = cars_query(query).await;
    assert_eq!(diag.message, "expressions have incompatible types");
    assert_eq!(diag.span, spans["whole"]);
    assert_eq!(diag.notes[0].message, "has type Utf8");
    assert_eq!(diag.notes[0].span, spans["left"]);
    assert_eq!(diag.notes[1].message, "has type Float32");
    assert_eq!(diag.notes[1].span, spans["right"]);
    Ok(())
}
