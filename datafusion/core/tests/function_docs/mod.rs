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

//! Checks the ASCII tables inside `sql_example` docs. They are copied verbatim onto
//! the generated pages, so a table that does not line up here does not line up there.

use datafusion::execution::SessionStateDefaults;
use datafusion::logical_expr::Documentation;
use std::fmt::Write as _;
use unicode_width::UnicodeWidthStr;

fn is_border(line: &str) -> bool {
    line.len() > 2
        && line.starts_with('+')
        && line.ends_with('+')
        && line.bytes().all(|b| matches!(b, b'-' | b'+'))
}

/// Markdown's header rule, `| --- | :---: |`, which `arrow` never emits.
fn is_separator(line: &str) -> bool {
    line.trim_matches('|').split('|').all(|cell| {
        let cell = cell.trim();
        cell.len() >= 3 && cell.bytes().all(|b| matches!(b, b'-' | b':' | b' '))
    })
}

fn fault(line: &str) -> Option<&'static str> {
    if line.starts_with('+') {
        (!is_border(line)).then_some("not a border")
    } else if is_separator(line) {
        Some("markdown separator row")
    } else if !(line.starts_with("| ") && line.ends_with(" |")) {
        Some("cell is not padded as `| value |`")
    } else {
        None
    }
}

fn misaligned_tables(name: &str, example: &str) -> Vec<String> {
    let lines: Vec<&str> = example.lines().map(str::trim_end).collect();
    let mut reports = vec![];
    let mut i = 0;
    while i < lines.len() {
        if !is_border(lines[i].trim_start()) {
            i += 1;
            continue;
        }
        let start = i;
        while i < lines.len() && lines[i].trim_start().starts_with(['+', '|']) {
            i += 1;
        }
        let table = &lines[start..i];
        let faults: Vec<Option<&str>> =
            table.iter().map(|l| fault(l.trim_start())).collect();
        let widths: Vec<usize> = table.iter().map(|l| l.width()).collect();
        if faults.iter().all(Option::is_none) && widths.iter().all(|w| *w == widths[0]) {
            continue;
        }
        let mut report = format!("{name}, table at line {} of the example:", start + 1);
        for ((line, width), fault) in table.iter().zip(&widths).zip(&faults) {
            write!(report, "\n  {width:>4}  {line}").ok();
            if let Some(fault) = fault {
                write!(report, "    <- {fault}").ok();
            }
        }
        reports.push(report);
    }
    reports
}

#[test]
fn sql_examples_are_well_formed_tables() {
    let mut reports = vec![];
    let mut check = |name: &str, doc: Option<&Documentation>| {
        if let Some(example) = doc.and_then(|d| d.sql_example.as_deref()) {
            reports.extend(misaligned_tables(name, example));
        }
    };

    for f in SessionStateDefaults::default_scalar_functions() {
        check(f.name(), f.documentation());
    }
    for f in SessionStateDefaults::default_higher_order_functions() {
        check(f.name(), f.documentation());
    }
    for f in SessionStateDefaults::default_aggregate_functions() {
        check(f.name(), f.documentation());
    }
    for f in SessionStateDefaults::default_window_functions() {
        check(f.name(), f.documentation());
    }

    assert!(
        reports.is_empty(),
        "{} `sql_example` table(s) are not what `arrow` prints. Every line of a table \
         has the same display width and every cell is padded as `| value |`.\n\n{}",
        reports.len(),
        reports.join("\n\n")
    );
}

#[test]
fn wide_characters_are_measured_by_display_width() {
    let example = r#"+-------------------+
| ascii(Utf8("🚀")) |
+-------------------+"#;
    assert!(misaligned_tables("ascii", example).is_empty());
}

#[test]
fn short_row_is_reported() {
    let example = "+-------+\n| 1   |\n+-------+";
    assert_eq!(misaligned_tables("f", example).len(), 1);
}

#[test]
fn unpadded_cell_is_reported() {
    let example = "+------+\n| value|\n+------+";
    assert_eq!(misaligned_tables("f", example).len(), 1);
}

#[test]
fn markdown_separator_row_is_reported() {
    let example = "+-----+\n| --- |\n+-----+";
    assert_eq!(misaligned_tables("f", example).len(), 1);
}
