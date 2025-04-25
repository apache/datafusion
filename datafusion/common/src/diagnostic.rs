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

use crate::Span;

/// Additional contextual information intended for end users, to help them
/// understand what went wrong by providing human-readable messages, and
/// locations in the source query that relate to the error in some way.
///
/// You can think of a single [`Diagnostic`] as a single "block" of output from
/// rustc. i.e. either an error or a warning, optionally with some notes and
/// help messages.
///
/// Example:
///
/// ```rust
/// # use datafusion_common::{Location, Span, Diagnostic};
/// let span = Some(Span {
///     start: Location{ line: 2, column: 1 },
///     end: Location{ line: 4, column: 15 }
/// });
/// let diagnostic = Diagnostic::new_error("Something went wrong", span)
///     .with_help("Have you tried turning it on and off again?", None);
/// ```
#[derive(Debug, Clone)]
pub struct Diagnostic {
    pub kind: DiagnosticKind,
    pub message: String,
    pub span: Option<Span>,
    pub notes: Vec<DiagnosticNote>,
    pub helps: Vec<DiagnosticHelp>,
}

/// A note enriches a [`Diagnostic`] with extra information, possibly referring
/// to different locations in the original SQL query, that helps contextualize
/// the error and helps the end user understand why it occurred.
///
/// Example:
/// SELECT id, name FROM users GROUP BY id
/// Note:      ^^^^ 'name' is not in the GROUP BY clause
#[derive(Debug, Clone)]
pub struct DiagnosticNote {
    pub message: String,
    pub span: Option<Span>,
}

/// A "help" enriches a [`Diagnostic`] with extra information, possibly
/// referring to different locations in the original SQL query, that helps the
/// user understand how they might fix the error or warning.
///
/// Example:
/// SELECT id, name FROM users GROUP BY id
/// Help: Add 'name' here                 ^^^^
#[derive(Debug, Clone)]
pub struct DiagnosticHelp {
    pub message: String,
    pub span: Option<Span>,
}

/// A [`Diagnostic`] can either be a hard error that prevents the query from
/// being planned and executed, or a warning that indicates potential issues,
/// performance problems, or causes for unexpected results, but is non-fatal.
/// This enum expresses these two possibilities.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DiagnosticKind {
    Error,
    Warning,
}

impl Diagnostic {
    /// Creates a new [`Diagnostic`] for a fatal error that prevents the SQL
    /// query from being planned and executed. Optionally takes in a [`Span`] to
    /// describe the location in the source code that caused the error, should
    /// be provided when available.
    pub fn new_error(message: impl Into<String>, span: Option<Span>) -> Self {
        Self {
            kind: DiagnosticKind::Error,
            message: message.into(),
            span,
            notes: Vec::new(),
            helps: Vec::new(),
        }
    }

    /// Creates a new [`Diagnostic`] for a NON-fatal warning, such as a
    /// performance problem, or possible cause for undesired results. Optionally
    /// takes in a [`Span`] to describe the location in the source code that
    /// caused the error, should be provided when available.
    pub fn new_warning(message: impl Into<String>, span: Option<Span>) -> Self {
        Self {
            kind: DiagnosticKind::Warning,
            message: message.into(),
            span,
            notes: Vec::new(),
            helps: Vec::new(),
        }
    }

    /// Adds a "note" to the [`Diagnostic`], which can have zero or many. A "note"
    /// helps contextualize the error and helps the end user understand why it
    /// occurred. It can refer to an arbitrary location in the SQL query, or to
    /// no location.
    pub fn add_note(&mut self, message: impl Into<String>, span: Option<Span>) {
        self.notes.push(DiagnosticNote {
            message: message.into(),
            span,
        });
    }

    /// Adds a "help" to the [`Diagnostic`], which can have zero or many. A
    /// "help" helps the user understand how they might fix the error or
    /// warning. It can refer to an arbitrary location in the SQL query, or to
    /// no location.
    pub fn add_help(&mut self, message: impl Into<String>, span: Option<Span>) {
        self.helps.push(DiagnosticHelp {
            message: message.into(),
            span,
        });
    }

    /// Like [`Diagnostic::add_note`], but returns `self` to allow chaining.
    pub fn with_note(mut self, message: impl Into<String>, span: Option<Span>) -> Self {
        self.add_note(message.into(), span);
        self
    }

    /// Like [`Diagnostic::add_help`], but returns `self` to allow chaining.
    pub fn with_help(mut self, message: impl Into<String>, span: Option<Span>) -> Self {
        self.add_help(message.into(), span);
        self
    }
}
