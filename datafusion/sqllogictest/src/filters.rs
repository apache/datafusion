use datafusion::sql::parser::{DFParserBuilder, Statement};
use sqllogictest::{AsyncDB, Record};
use sqlparser::ast::{SetExpr, Statement as SqlStatement};
use std::path::Path;
use std::str::FromStr;

/// Filter specification that determines whether a certain sqllogictest record in
/// a certain file should be filtered. In order for a [`Filter`] to match a record,
/// it needs to:
/// - Belong to a file whose absolute path contains the `file_substring` substring.
/// - If a `line_number` is specific, be declared in that same line number.
///
/// If a [`Filter`] matches a specific record, then the record is executed, if there's
/// no match, the record is skipped.
#[derive(Debug, Clone)]
pub struct Filter {
    file_substring: String,
    line_number: Option<u32>,
}

impl FromStr for Filter {
    type Err = String;

    fn from_str(s: &str) -> datafusion::common::Result<Self, Self::Err> {
        let parts: Vec<&str> = s.rsplitn(2, ':').collect();
        if parts.len() == 2 {
            match parts[0].parse::<u32>() {
                Ok(line) => Ok(Filter {
                    file_substring: parts[1].to_string(),
                    line_number: Some(line),
                }),
                Err(_) => Err(format!("Cannot parse line number from '{s}'")),
            }
        } else {
            Ok(Filter {
                file_substring: s.to_string(),
                line_number: None,
            })
        }
    }
}

/// Given a list of [`Filter`]s, determines if the whole file in the provided
/// path can be skipped.
///
/// - If there's at least 1 filter whose file name is a substring of the provided path,
///   it returns true.
/// - If the provided filter list is empty, it returns false.
pub fn should_skip_file(path: &Path, filters: &[Filter]) -> bool {
    if filters.is_empty() {
        return false;
    }

    let path_string = path.to_string_lossy();
    for filter in filters {
        if path_string.contains(&filter.file_substring) {
            return false;
        }
    }
    true
}

/// Determines whether a certain sqllogictest record should be skipped given the provided
/// filters.
///
/// If there's at least 1 matching filter, or the filter list is empty, it returns false.
///
/// There are certain records that will never be skipped even if they are not matched
/// by any filters, like CREATE TABLE, INSERT INTO, DROP or SELECT * INTO statements,
/// as they populate tables necessary for other tests to work.
pub fn should_skip_record<D: AsyncDB>(
    record: &Record<D::ColumnType>,
    filters: &[Filter],
) -> bool {
    if filters.is_empty() {
        return false;
    }

    let (sql, loc) = match record {
        Record::Statement { sql, loc, .. } => (sql, loc),
        Record::Query { sql, loc, .. } => (sql, loc),
        _ => return false,
    };

    let Ok(Ok(Some(statement))) = DFParserBuilder::new(sql)
        .build()
        .map(|mut v| v.parse_statements().map(|mut v| v.pop_front()))
    else {
        return false;
    };

    if matches!(statement, Statement::CreateExternalTable(_)) {
        // Cannot skip CREATE EXTERNAL TABLE statements, as they crate tables
        // used in the next tests.
        return false;
    } else if let Statement::Statement(sql_stmt) = statement {
        // Cannot skip SELECT INTO statements, as they can also create tables
        // that further test cases will use.
        if let SqlStatement::Query(v) = sql_stmt.as_ref() {
            if let SetExpr::Select(v) = v.body.as_ref() {
                if v.into.is_some() {
                    return false;
                }
            }
        }

        // Only SELECT and EXPLAIN statements can be skipped, as any other
        // statement might be populating tables that future test cases will use.
        if !matches!(
            sql_stmt.as_ref(),
            SqlStatement::Query(_) | SqlStatement::Explain { .. }
        ) {
            return false;
        }
    }

    for filter in filters {
        if !loc.file().contains(&filter.file_substring) {
            continue;
        }
        if let Some(line_num) = filter.line_number {
            if loc.line() != line_num {
                continue;
            }
        }

        // This filter matches both file name substring and the exact
        //  line number (if one was provided), so don't skip it.
        return false;
    }

    true
}
