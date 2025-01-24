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

    pub fn add_note(&mut self, message: String, span: Span) {
        self.notes.push(DiagnosticNote { message, span });
    }

    pub fn add_help(&mut self, message: String, span: Span) {
        self.helps.push(DiagnosticHelp { message, span });
    }

    pub fn with_note(mut self, message: String, span: Span) -> Self {
        self.add_note(message, span);
        self
    }

    pub fn with_help(mut self, message: String, span: Span) -> Self {
        self.add_help(message, span);
        self
    }
}
