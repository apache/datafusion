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

//! Helper that helps with interactive editing, including multi-line parsing and validation,
//! and auto-completion for file name during creating external table.

use datafusion::error::DataFusionError;
use datafusion::sql::parser::{DFParser, Statement};
use datafusion::sql::sqlparser::parser::ParserError;
use rustyline::completion::Completer;
use rustyline::completion::FilenameCompleter;
use rustyline::completion::Pair;
use rustyline::error::ReadlineError;
use rustyline::highlight::Highlighter;
use rustyline::hint::Hinter;
use rustyline::validate::ValidationContext;
use rustyline::validate::ValidationResult;
use rustyline::validate::Validator;
use rustyline::Context;
use rustyline::Helper;
use rustyline::Result;

#[derive(Default)]
pub struct CliHelper {
    completer: FilenameCompleter,
}

impl CliHelper {
    fn validate_input(&self, input: &str) -> Result<ValidationResult> {
        if let Some(sql) = input.strip_suffix(';') {
            let sql = match unescape_input(sql) {
                Ok(sql) => sql,
                Err(err) => {
                    return Ok(ValidationResult::Invalid(Some(format!(
                        "  🤔 Invalid statement: {err}",
                    ))))
                }
            };
            match DFParser::parse_sql(&sql) {
                Ok(statements) if statements.is_empty() => Ok(ValidationResult::Invalid(
                    Some("  🤔 You entered an empty statement".to_string()),
                )),
                Ok(statements) if statements.len() > 1 => Ok(ValidationResult::Invalid(
                    Some("  🤔 You entered more than one statement".to_string()),
                )),
                Ok(_statements) => Ok(ValidationResult::Valid(None)),
                Err(err) => Ok(ValidationResult::Invalid(Some(format!(
                    "  🤔 Invalid statement: {err}",
                )))),
            }
        } else if input.starts_with('\\') {
            // command
            Ok(ValidationResult::Valid(None))
        } else {
            Ok(ValidationResult::Incomplete)
        }
    }
}

impl Highlighter for CliHelper {}

impl Hinter for CliHelper {
    type Hint = String;
}

/// returns true if the current position is after the open quote for
/// creating an external table.
fn is_open_quote_for_location(line: &str, pos: usize) -> bool {
    let mut sql = line[..pos].to_string();
    sql.push('\'');
    if let Ok(stmts) = DFParser::parse_sql(&sql) {
        if let Some(Statement::CreateExternalTable(_)) = stmts.back() {
            return true;
        }
    }
    false
}

impl Completer for CliHelper {
    type Candidate = Pair;

    fn complete(
        &self,
        line: &str,
        pos: usize,
        ctx: &Context<'_>,
    ) -> std::result::Result<(usize, Vec<Pair>), ReadlineError> {
        if is_open_quote_for_location(line, pos) {
            self.completer.complete(line, pos, ctx)
        } else {
            Ok((0, Vec::with_capacity(0)))
        }
    }
}

impl Validator for CliHelper {
    fn validate(&self, ctx: &mut ValidationContext<'_>) -> Result<ValidationResult> {
        let input = ctx.input().trim_end();
        self.validate_input(input)
    }
}

impl Helper for CliHelper {}

/// Unescape input string from readline.
///
/// The data read from stdio will be escaped, so we need to unescape the input before executing the input
pub fn unescape_input(input: &str) -> datafusion::error::Result<String> {
    let mut chars = input.chars();

    let mut result = String::with_capacity(input.len());
    while let Some(char) = chars.next() {
        if char == '\\' {
            if let Some(next_char) = chars.next() {
                // https://static.rust-lang.org/doc/master/reference.html#literals
                result.push(match next_char {
                    '0' => '\0',
                    'n' => '\n',
                    'r' => '\r',
                    't' => '\t',
                    '\\' => '\\',
                    _ => {
                        return Err(DataFusionError::SQL(ParserError::TokenizerError(
                            format!("unsupported escape char: '\\{}'", next_char),
                        )))
                    }
                });
            }
        } else {
            result.push(char);
        }
    }

    Ok(result)
}

#[cfg(test)]
mod tests {
    use std::io::{BufRead, Cursor};

    use super::*;

    fn readline_direct(
        mut reader: impl BufRead,
        validator: &CliHelper,
    ) -> Result<ValidationResult> {
        let mut input = String::new();

        if reader.read_line(&mut input)? == 0 {
            return Err(ReadlineError::Eof);
        }

        validator.validate_input(&input)
    }

    #[test]
    fn unescape_readline_input() -> Result<()> {
        let validator = CliHelper::default();

        // shoule be valid
        let result = readline_direct(
            Cursor::new(r"create external table test stored as csv location 'data.csv' delimiter ',';".as_bytes()),
            &validator,
        )?;
        assert!(matches!(result, ValidationResult::Valid(None)));

        let result = readline_direct(
            Cursor::new(r"create external table test stored as csv location 'data.csv' delimiter '\0';".as_bytes()),
            &validator,
        )?;
        assert!(matches!(result, ValidationResult::Valid(None)));

        let result = readline_direct(
            Cursor::new(r"create external table test stored as csv location 'data.csv' delimiter '\n';".as_bytes()),
            &validator,
        )?;
        assert!(matches!(result, ValidationResult::Valid(None)));

        let result = readline_direct(
            Cursor::new(r"create external table test stored as csv location 'data.csv' delimiter '\r';".as_bytes()),
            &validator,
        )?;
        assert!(matches!(result, ValidationResult::Valid(None)));

        let result = readline_direct(
            Cursor::new(r"create external table test stored as csv location 'data.csv' delimiter '\t';".as_bytes()),
            &validator,
        )?;
        assert!(matches!(result, ValidationResult::Valid(None)));

        let result = readline_direct(
            Cursor::new(r"create external table test stored as csv location 'data.csv' delimiter '\\';".as_bytes()),
            &validator,
        )?;
        assert!(matches!(result, ValidationResult::Valid(None)));

        // should be invalid
        let result = readline_direct(
            Cursor::new(r"create external table test stored as csv location 'data.csv' delimiter ',,';".as_bytes()),
            &validator,
        )?;
        assert!(matches!(result, ValidationResult::Invalid(Some(_))));

        let result = readline_direct(
            Cursor::new(r"create external table test stored as csv location 'data.csv' delimiter '\u{07}';".as_bytes()),
            &validator,
        )?;
        assert!(matches!(result, ValidationResult::Invalid(Some(_))));

        Ok(())
    }
}
