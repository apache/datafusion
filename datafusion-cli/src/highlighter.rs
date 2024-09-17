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

//! The syntax highlighter.

use std::{
    borrow::Cow::{self, Borrowed},
    fmt::Display,
};

use datafusion::sql::sqlparser::{
    dialect::{dialect_from_str, Dialect, GenericDialect},
    keywords::Keyword,
    tokenizer::{Token, Tokenizer},
};
use rustyline::highlight::Highlighter;

/// The syntax highlighter.
#[derive(Debug)]
pub struct SyntaxHighlighter {
    dialect: Box<dyn Dialect>,
}

impl SyntaxHighlighter {
    pub fn new(dialect: &str) -> Self {
        let dialect = dialect_from_str(dialect).unwrap_or(Box::new(GenericDialect {}));
        Self { dialect }
    }
}

pub struct NoSyntaxHighlighter {}

impl Highlighter for NoSyntaxHighlighter {}

impl Highlighter for SyntaxHighlighter {
    fn highlight<'l>(&self, line: &'l str, _: usize) -> Cow<'l, str> {
        let mut out_line = String::new();

        // `with_unescape(false)` since we want to rebuild the original string.
        let mut tokenizer =
            Tokenizer::new(self.dialect.as_ref(), line).with_unescape(false);
        let tokens = tokenizer.tokenize();
        match tokens {
            Ok(tokens) => {
                for token in tokens.iter() {
                    match token {
                        Token::Word(w) if w.keyword != Keyword::NoKeyword => {
                            out_line.push_str(&Color::red(token));
                        }
                        Token::SingleQuotedString(_) => {
                            out_line.push_str(&Color::green(token));
                        }
                        other => out_line.push_str(&format!("{other}")),
                    }
                }
                out_line.into()
            }
            Err(_) => Borrowed(line),
        }
    }

    fn highlight_char(&self, line: &str, _pos: usize, _forced: bool) -> bool {
        !line.is_empty()
    }
}

/// Convenient utility to return strings with [ANSI color](https://gist.github.com/JBlond/2fea43a3049b38287e5e9cefc87b2124).
struct Color {}

impl Color {
    fn green(s: impl Display) -> String {
        format!("\x1b[92m{s}\x1b[0m")
    }

    fn red(s: impl Display) -> String {
        format!("\x1b[91m{s}\x1b[0m")
    }
}

#[cfg(test)]
mod tests {
    use super::SyntaxHighlighter;
    use rustyline::highlight::Highlighter;

    #[test]
    fn highlighter_valid() {
        let s = "SElect col_a from tab_1;";
        let highlighter = SyntaxHighlighter::new("generic");
        let out = highlighter.highlight(s, s.len());
        assert_eq!(
            "\u{1b}[91mSElect\u{1b}[0m col_a \u{1b}[91mfrom\u{1b}[0m tab_1;",
            out
        );
    }

    #[test]
    fn highlighter_valid_with_new_line() {
        let s = "SElect col_a from tab_1\n WHERE col_b = 'なにか';";
        let highlighter = SyntaxHighlighter::new("generic");
        let out = highlighter.highlight(s, s.len());
        assert_eq!(
            "\u{1b}[91mSElect\u{1b}[0m col_a \u{1b}[91mfrom\u{1b}[0m tab_1\n \u{1b}[91mWHERE\u{1b}[0m col_b = \u{1b}[92m'なにか'\u{1b}[0m;",
            out
        );
    }

    #[test]
    fn highlighter_invalid() {
        let s = "SElect col_a from tab_1 WHERE col_b = ';";
        let highlighter = SyntaxHighlighter::new("generic");
        let out = highlighter.highlight(s, s.len());
        assert_eq!("SElect col_a from tab_1 WHERE col_b = ';", out);
    }
}
