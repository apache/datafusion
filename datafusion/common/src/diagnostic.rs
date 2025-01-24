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

use sqlparser::tokenizer::Span;

/// Additional contextual information intended for end users, to help them
/// understand what went wrong by providing human-readable messages, and
/// locations in the source query that relate to the error in some way.
///
/// You can think of a single [`Diagnostic`] as a single "block" of output from
/// rustc. i.e. either an error or a warning, optionally with some notes and
/// help messages.
///
/// If the diagnostic, a note, or a help message doesn't need to point to a
/// specific location in the original SQL query (or the [`Span`] is not
/// available), use [`Span::empty`].
///
/// Example:
///
/// ```rust
/// # use datafusion_common::Diagnostic;
/// # use sqlparser::tokenizer::Span;
/// # let span = Span::empty();
/// let diagnostic = Diagnostic::new_error("Something went wrong", span)
///     .with_help("Have you tried turning it on and off again?", Span::empty());
/// ```
#[derive(Debug, Clone)]
pub struct Diagnostic {
    pub kind: DiagnosticKind,
    pub message: String,
    pub span: Span,
    pub notes: Vec<DiagnosticNote>,
    pub helps: Vec<DiagnosticHelp>,
}

#[derive(Debug, Clone)]
pub struct DiagnosticNote {
    pub message: String,
    pub span: Span,
}

#[derive(Debug, Clone)]
pub struct DiagnosticHelp {
    pub message: String,
    pub span: Span,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DiagnosticKind {
    Error,
    Warning,
}

impl Diagnostic {
    pub fn new_error(message: impl Into<String>, span: Span) -> Self {
        Self {
            kind: DiagnosticKind::Error,
            message: message.into(),
            span,
            notes: Vec::new(),
            helps: Vec::new(),
        }
    }

    pub fn new_warning(message: impl Into<String>, span: Span) -> Self {
        Self {
            kind: DiagnosticKind::Warning,
            message: message.into(),
            span,
            notes: Vec::new(),
            helps: Vec::new(),
        }
    }

    pub fn add_note(&mut self, message: impl Into<String>, span: Span) {
        self.notes.push(DiagnosticNote { message: message.into(), span });
    }

    pub fn add_help(&mut self, message: impl Into<String>, span: Span) {
        self.helps.push(DiagnosticHelp { message: message.into(), span });
    }

    pub fn with_note(mut self, message: impl Into<String>, span: Span) -> Self {
        self.add_note(message.into(), span);
        self
    }

    pub fn with_help(mut self, message: impl Into<String>, span: Span) -> Self {
        self.add_help(message.into(), span);
        self
    }
}
